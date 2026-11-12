// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

// Package direct is the engine of direct storage (TEEPIN-STORAGE-DESIGN.md sections 17, 19, 22 and 25): bytes go
// straight between this device and the storage network, encrypted and verified here, with the control plane only
// signing requests and keeping the catalog.
//
// It is a port of the Python SDK's engine (_engine.py, _reads.py) by way of the Node SDK's (src/direct), whose single
// driver suits Go's blocking calls: Go has goroutines and contexts, so the engine is plain blocking code and needs no
// sans-I/O split (section 25.2). Cancellation is the context.Context every call takes: it ends a call promptly, closes
// its responses and is never retried.
//
// This file holds the rules that don't depend on how bytes are sent: what goes into a pack, how many requests may be
// in flight, when to retry, and what a storage network's answer means. common.go holds the plumbing every operation
// shares, formats.go the stored formats, and reads.go and blocks.go the reads.
package direct

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

const (
	// PackObjectLimit is the largest object that goes into a pack; larger ones are uploaded in parts.
	PackObjectLimit = 16 << 20
	// StartConcurrency is where the adaptive limit of packs and parts in flight starts.
	StartConcurrency = 4
	// MaxConcurrency is the adaptive limit's ceiling, unless the client sets a lower one.
	MaxConcurrency = 32
	// DefaultUploadWait is how long an upload waits for an unavailable storage network or control plane (17.4).
	DefaultUploadWait = 10 * time.Minute
	// SignMargin: a signature closer than this to its expiry is renewed before it is used.
	SignMargin = time.Minute
	// ResendAfterSent and ResendAfterLost: a pack's bytes are sent again only this long after an upload that answered
	// success, or after one whose answer was lost (section 28.18). Sending 20 MiB again costs far more than asking.
	ResendAfterSent = 30 * time.Second
	ResendAfterLost = 5 * time.Second
	// VerifyEvery is the wait between a commit's looks again once VerifyDelays are spent.
	VerifyEvery = 2 * time.Second
	// MaxRebuilds is how many times a pack's files are reserved again (conflict, bad bytes) before they fail.
	MaxRebuilds = 3
	// CleanupCap bounds each call of the cleanup after a cancel (section 22.13), so a stop stays bounded.
	CleanupCap = 5 * time.Second
)

// A large object's parts (section 21.2): 16 MiB of plaintext by default, at least 5 MiB, a multiple of TWS1's 64 KiB
// segment, and at most 3,000 of them, each at most 5 GiB as stored.
const (
	DefaultPartBytes = 16 << 20
	MinPartBytes     = 5 << 20
	MaxDirectParts   = 3000
	MaxPartStored    = 5 << 30
	// StreamPartBytes is a stream's part size when neither its size nor a part size is given: about 187 GiB at
	// most, in 3,000 parts.
	StreamPartBytes = 64 << 20
	// StreamConcurrency is a stream's parts in flight unless the caller says: each holds its ciphertext, so with the
	// reader's parts a stream holds at most about (StreamConcurrency + 3) times its part size.
	StreamConcurrency = 4
)

// PartRules are the part-size rules DirectPartSize and StreamPartSize apply. An upload engine keeps its own copy, so
// tests can shrink them.
type PartRules struct {
	DefaultBytes int64 // a part's plaintext bytes unless asked for (DefaultPartBytes)
	MinBytes     int64 // the fewest plaintext bytes a part may hold (MinPartBytes)
	MaxParts     int64 // the most parts an object may have (MaxDirectParts)
	StreamBytes  int64 // a stream's part size with no hint (StreamPartBytes)
}

// DefaultPartRules are the rules of the control plane and the storage network.
var DefaultPartRules = PartRules{
	DefaultBytes: DefaultPartBytes,
	MinBytes:     MinPartBytes,
	MaxParts:     MaxDirectParts,
	StreamBytes:  StreamPartBytes,
}

// DirectPartSize is the part size for a large object of size bytes when want (0 for the default) is asked for: at
// least MinBytes, rounded up to whole segments, doubled until the parts fit in MaxParts. An object whose parts would
// pass MaxPartStored as stored is refused.
//
// A slow link (section 22.7): with link measured (nil before anything is), a part carries at most link.Budget of it,
// rounded down to whole segments, whether the part size is the default or asked for. When that cap is under MinBytes,
// or the parts can't fit in MaxParts under it, the error is TooSlow's (413 connection_speed), before anything is
// created or sent. name is for that error.
func DirectPartSize(size, want int64, rules PartRules, link *LinkSpeed, name string) (int64, error) {
	return partSize(size, size, want, rules, link, name)
}

// partSize is the rule of DirectPartSize and StreamPartSize; declared is the size the error names (0: a stream).
func partSize(size, declared, want int64, rules PartRules, link *LinkSpeed, name string) (int64, error) {
	const seg = tws1.SegmentSize
	p := rules.DefaultBytes
	if want > 0 {
		p = want
	}
	p = max(p, rules.MinBytes)
	p = (p + seg - 1) / seg * seg
	var limit int64
	if link != nil {
		limit = link.PartCap()
		if limit < rules.MinBytes {
			return 0, TooSlow(name, declared, *link, limit, rules)
		}
		p = min(p, limit)
	}
	for (size+p-1)/p > rules.MaxParts {
		p *= 2
	}
	if link != nil && p > limit {
		// Doubling passed the cap: the smallest whole-segment size that fits, if the cap allows it (over the size
		// before doubling, so over MinBytes).
		fit := ((size+rules.MaxParts-1)/rules.MaxParts + seg - 1) / seg * seg
		if fit > limit {
			return 0, TooSlow(name, declared, *link, limit, rules)
		}
		p = fit
	}
	first, err := tws1.PartOf(max(size, 1), p, 1)
	if err != nil {
		return 0, err
	}
	if first.StoredSize > MaxPartStored {
		return 0, fmt.Errorf("teepin: %d bytes needs parts over %d bytes; it is too large to upload", size, int64(MaxPartStored))
	}
	return p, nil
}

// StreamPartSize is the part size for a stream whose size isn't known (section 22.1, E2 b). It fixes the largest
// stream: MaxParts of it. expectedSize, a hint, chooses it as for a known size; with neither hint nor want,
// StreamBytes. A measured link caps it as for a known size.
func StreamPartSize(expectedSize, want int64, rules PartRules, link *LinkSpeed, name string) (int64, error) {
	if expectedSize > 0 {
		return partSize(expectedSize, expectedSize, want, rules, link, name)
	}
	if want <= 0 {
		want = rules.StreamBytes
	}
	return partSize(1, 0, want, rules, link, name)
}

// --- slow links (section 22.7) ------------------------------------------------------------------------------------

const (
	// RequestBudget is about the longest one request to the storage network should take at the measured speed.
	// Packs, and the parts of uploads created from then on, shrink to fit it.
	RequestBudget = 240 * time.Second
	// SpeedSampleBytes: a PUT smaller than this isn't measured; its time is mostly the round trip, not the link.
	SpeedSampleBytes = 256 << 10
	// SpeedWeight is how far each measured PUT moves the estimate (moving averages of bytes and of time).
	SpeedWeight = 0.3
	// PutTimeoutFloor and PutTimeoutSpeed: a storage PUT waits for its answer at least this long, and at least its
	// bytes' time at this speed (bytes per second).
	PutTimeoutFloor = 30 * time.Second
	PutTimeoutSpeed = 64 << 10
)

// LinkSpeed is a measured speed, for the size rules: bytes per second, and how long one request may take.
type LinkSpeed struct {
	BytesPerSecond float64
	Budget         time.Duration
}

// RequestBytes is the most bytes one request carries within the budget at this speed.
func (l LinkSpeed) RequestBytes() int64 { return int64(l.BytesPerSecond * l.Budget.Seconds()) }

// PartCap is the largest part this speed allows: RequestBytes rounded down to whole segments.
func (l LinkSpeed) PartCap() int64 { return l.RequestBytes() / tws1.SegmentSize * tws1.SegmentSize }

// SpeedMeter is the upload speed to the storage network, measured from its PUTs (section 22.7): a moving estimate
// per client, of bytes over time, so a long transfer counts for more than a short one. A PUT that succeeded is a
// sample; one that timed out on the engine's own timer is an upper bound; any other failure, and a PUT under the
// sample size, is never measured. Until something is, Link is nil and the default sizes hold. Safe for concurrent
// use.
type SpeedMeter struct {
	mu       sync.Mutex
	budget   time.Duration
	minBytes int64
	bytes    float64
	secs     float64
	fixed    float64 // > 0: Set's speed
}

// NewSpeedMeter is a meter with the default budget (RequestBudget) and sample size (SpeedSampleBytes).
func NewSpeedMeter() *SpeedMeter {
	return &SpeedMeter{budget: RequestBudget, minBytes: SpeedSampleBytes}
}

// Observe takes a PUT of n bytes that succeeded in elapsed.
func (m *SpeedMeter) Observe(n int64, elapsed time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n < m.minBytes || elapsed <= 0 {
		return
	}
	m.sample(float64(n), elapsed.Seconds())
}

// ObserveTimeout takes a PUT of n bytes that the engine's own timer ended after elapsed, with no answer: evidence of a
// slow link, measured from a cold start too. It is an upper bound, weighted like a completed PUT: at most n bytes
// over elapsed, and never above the speed measured so far, so it can only lower the estimate. n is the request's
// size: how much of it was written isn't known.
func (m *SpeedMeter) ObserveTimeout(n int64, elapsed time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n < m.minBytes || elapsed <= 0 {
		return
	}
	b, s := float64(n), elapsed.Seconds()
	if m.secs > 0 {
		b = min(b, m.bytes/m.secs*s)
	}
	m.sample(b, s)
}

func (m *SpeedMeter) sample(b, s float64) {
	if m.secs <= 0 {
		m.bytes, m.secs = b, s
		return
	}
	m.bytes += SpeedWeight * (b - m.bytes)
	m.secs += SpeedWeight * (s - m.secs)
}

// Set fixes the speed in bytes per second (a test hook); 0 goes back to measuring.
func (m *SpeedMeter) Set(bytesPerSecond float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fixed = max(bytesPerSecond, 0)
}

// SetBudget sets how long one request may take (a test hook; RequestBudget by default).
func (m *SpeedMeter) SetBudget(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.budget = d
}

// SetSampleBytes sets the smallest PUT measured (a test hook; SpeedSampleBytes by default).
func (m *SpeedMeter) SetSampleBytes(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.minBytes = n
}

// BytesPerSecond is the measured speed; false before anything is measured.
func (m *SpeedMeter) BytesPerSecond() (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bps()
}

func (m *SpeedMeter) bps() (float64, bool) {
	if m.fixed > 0 {
		return m.fixed, true
	}
	if m.secs > 0 {
		return m.bytes / m.secs, true
	}
	return 0, false
}

// Link is the speed and budget for the size rules; nil before anything is measured.
func (m *SpeedMeter) Link() *LinkSpeed {
	m.mu.Lock()
	defer m.mu.Unlock()
	bps, ok := m.bps()
	if !ok {
		return nil
	}
	return &LinkSpeed{BytesPerSecond: bps, Budget: m.budget}
}

// PackObjectLimitAt is the largest object that goes into a pack; a larger one goes in parts. limit, or on a slow link
// the bytes one request carries at its speed, but never under minPart: an object a pack request couldn't carry in
// time goes in parts, which the part rule sizes for the link. A nil link (nothing measured) keeps limit.
func PackObjectLimitAt(link *LinkSpeed, limit, minPart int64) int64 {
	if link == nil {
		return limit
	}
	return min(limit, max(link.RequestBytes(), minPart))
}

// PutTimeout is how long a storage PUT of n bytes waits for its answer: at least floor, n bytes at floorSpeed bytes per
// second, and on a measured link twice n's time at its speed.
func PutTimeout(n int64, floor time.Duration, floorSpeed float64, link *LinkSpeed) time.Duration {
	secs := max(floor.Seconds(), float64(n)/floorSpeed)
	if link != nil && link.BytesPerSecond > 0 {
		secs = max(secs, 2*float64(n)/link.BytesPerSecond)
	}
	return time.Duration(secs * float64(time.Second))
}

// TooSlow is the error of an upload the measured link can't carry: 413 with code connection_speed, which errors.Is
// reports as errs.ErrConnectionSpeed. name is the object (size bytes; 0 for a stream of unknown size), and partCap
// the largest part the link allows.
func TooSlow(name string, size int64, link LinkSpeed, partCap int64, rules PartRules) error {
	var why string
	if partCap < rules.MinBytes {
		why = fmt.Sprintf("even a %s part would take over %s minutes to send", mibString(rules.MinBytes),
			strconv.FormatFloat(math.Round(link.Budget.Minutes()*100)/100, 'f', -1, 64))
	} else {
		why = fmt.Sprintf("parts can be at most %s, and it would need more than %s of them", mibString(partCap),
			thousands(rules.MaxParts))
	}
	what := "this stream"
	if size > 0 {
		what = fmt.Sprintf("%d bytes", size)
	}
	if name == "" {
		name = "the upload"
	}
	return &errs.APIError{StatusCode: http.StatusRequestEntityTooLarge, Code: errs.CodeConnectionSpeed,
		Message: fmt.Sprintf("%s: the connection to storage measured %s; at that speed %s, so %s is too large to send "+
			"over this connection", name, FormatSpeed(link.BytesPerSecond), why, what)}
}

// FormatSpeed is a speed for a person: "20.0 KiB/s", "1.5 MiB/s".
func FormatSpeed(bytesPerSecond float64) string {
	if bytesPerSecond >= 1<<20 {
		return fmt.Sprintf("%.1f MiB/s", bytesPerSecond/(1<<20))
	}
	return fmt.Sprintf("%.1f KiB/s", bytesPerSecond/(1<<10))
}

// mibString is n bytes in MiB, to four places at most: "5 MiB", "9.375 MiB".
func mibString(n int64) string {
	return strconv.FormatFloat(math.Round(float64(n)/(1<<20)*1e4)/1e4, 'f', -1, 64) + " MiB"
}

// thousands writes n with commas: 3,000.
func thousands(n int64) string {
	if n < 0 {
		return "-" + thousands(-n)
	}
	s := strconv.FormatInt(n, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// PackedSize is a pack's size: each object's needle and TWS1 bytes.
func PackedSize(sizes []int64) int64 {
	var n int64
	for _, s := range sizes {
		n += tws1.NeedleHeaderSize + tws1.StoredSize(s)
	}
	return n
}

// Outcome is how a slot of the adaptive limit ended.
type Outcome int

const (
	// OutcomeOK: the request went through; the limit grows.
	OutcomeOK Outcome = iota
	// OutcomeThrottled: a throttle or a timeout; the limit halves.
	OutcomeThrottled
	// OutcomeFailed: any other failure; the limit stays.
	OutcomeFailed
)

// AdaptiveLimit is how many packs or parts may be in flight (section 17.8): it starts at StartConcurrency, adds about
// one per round of successes, halves on a throttle or a timeout, and never passes its ceiling.
//
// Priority is set while PutObject calls wait for a slot: a run's Acquire then waits too, so a freed slot goes to them
// (TryAcquire) before the run's next pack, and a long run never makes a PutObject wait for its end.
type AdaptiveLimit struct {
	mu       sync.Mutex
	ceiling  float64
	limit    float64
	inFlight int
	priority bool
	wake     chan struct{} // closed, and replaced, to wake every waiter
}

// NewAdaptiveLimit starts at start, never passing ceiling (each at least 1).
func NewAdaptiveLimit(start, ceiling int) *AdaptiveLimit {
	c := float64(max(1, ceiling))
	return &AdaptiveLimit{ceiling: c, limit: math.Min(float64(max(1, start)), c), wake: make(chan struct{})}
}

// Limit is the current limit (a slot is free while fewer than its whole part are in flight).
func (l *AdaptiveLimit) Limit() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}

// Ceiling is the most the limit may grow to.
func (l *AdaptiveLimit) Ceiling() int { return int(l.ceiling) }

// InFlight is how many slots are taken.
func (l *AdaptiveLimit) InFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inFlight
}

func (l *AdaptiveLimit) free() bool { return l.inFlight < int(l.limit) }

// TryAcquire takes a slot now if one is free: PutObject's dispatch, which Priority doesn't hold back.
func (l *AdaptiveLimit) TryAcquire() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.free() {
		return false
	}
	l.inFlight++
	return true
}

// Acquire waits for a slot that PutObject's callers aren't waiting for. A cancelled ctx ends the wait, taking none.
func (l *AdaptiveLimit) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		l.mu.Lock()
		if l.free() && !l.priority {
			l.inFlight++
			l.mu.Unlock()
			return nil
		}
		wake := l.wake
		l.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// SetPriority is set while PutObject calls wait; turning it off wakes the run's waiting Acquire calls.
func (l *AdaptiveLimit) SetPriority(on bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	changed := l.priority != on
	l.priority = on
	if changed && !on {
		l.wakeAll()
	}
}

// Release gives a slot back, adjusting the limit by how it ended, and wakes the waiters.
func (l *AdaptiveLimit) Release(o Outcome) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inFlight = max(0, l.inFlight-1)
	switch o {
	case OutcomeOK:
		l.limit = math.Min(l.ceiling, l.limit+1/l.limit)
	case OutcomeThrottled:
		l.limit = math.Max(1, l.limit/2)
	}
	l.wakeAll()
}

func (l *AdaptiveLimit) wakeAll() {
	close(l.wake)
	l.wake = make(chan struct{})
}

// RetryBudget is a token bucket that caps retries at about a tenth of requests, plus a burst, so an incident doesn't
// become a retry storm (section 17.6).
type RetryBudget struct {
	mu     sync.Mutex
	ratio  float64
	burst  float64
	tokens float64
}

// NewRetryBudget allows ratio retries per request, with a burst of burst.
func NewRetryBudget(ratio, burst float64) *RetryBudget {
	return &RetryBudget{ratio: ratio, burst: burst, tokens: burst}
}

// OnRequest earns a part of a retry.
func (b *RetryBudget) OnRequest() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens = math.Min(b.burst, b.tokens+b.ratio)
}

// TryRetry spends a retry if one is left.
func (b *RetryBudget) TryRetry() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// VerifyDelays are the waits before a commit that answered "verifying" is asked again, then VerifyEvery (28.18).
var VerifyDelays = []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second}

// A pack's sign opens every wrapped key, and its commit writes a row per object, so their timeouts grow with the object
// count (TEEPIN-STORAGE-DESIGN.md 29.15): on one core, the drill measured reserve at 3.3 s for 137,070 objects, sign at
// 1.4 s for 10,000 and 18.7 s for 137,070, commit at 5.3 s for 137,070. ScaledBase covers the round trip;
// TimeoutMargin is on top of the server's time. A reserve or sign body of that many objects is tens of MiB, so its
// sending time is added (BodyTransfer).
const (
	ReservePerObject = 25 * time.Microsecond
	SignPerObject    = 140 * time.Microsecond
	CommitPerObject  = 40 * time.Microsecond
	ScaledBase       = time.Second
	TimeoutMargin    = 3
)

// ScaledTimeout is a call's timeout for objects objects at perObject each: TimeoutMargin times the server's measured
// time, plus transfer (sending its body: BodyTransfer), or floor when that is longer (the client's own timeout, or
// the commit's).
func ScaledTimeout(objects int, perObject, floor, transfer time.Duration) time.Duration {
	return max(floor, TimeoutMargin*(ScaledBase+time.Duration(objects)*perObject)+transfer)
}

// BodyTransfer is the time to send n bytes of a request body: twice their time at the measured speed (bps > 0), as a
// storage PUT's timer allows (PutTimeout), or their time at floorSpeed before anything is measured.
func BodyTransfer(n int64, bps, floorSpeed float64) time.Duration {
	secs := float64(n) / floorSpeed
	if bps > 0 {
		secs = 2 * float64(n) / bps
	}
	return time.Duration(secs * float64(time.Second))
}

// VerifyDelay is the wait before a commit's n-th look again (0 first).
func VerifyDelay(n int) time.Duration {
	if n < len(VerifyDelays) {
		return VerifyDelays[n]
	}
	return VerifyEvery
}

// WaitDelay is the wait before the next round when storage or the control plane is unavailable: from 5 s, doubling
// to 5 minutes, with jitter; a server's Retry-After when it sent one (section 17.6).
func WaitDelay(round int, retryAfter time.Duration) time.Duration {
	const most = 5 * time.Minute
	if retryAfter > 0 {
		return min(retryAfter, most)
	}
	base := most
	if round < 10 {
		base = min(5*time.Second<<round, most)
	}
	return time.Duration(float64(base) * (0.75 + rand.Float64()*0.25))
}

// StorageOutcome is what a request to the storage network ended in, as the engine acts on it.
type StorageOutcome string

const (
	// Sent: the bytes are there (200), or an earlier attempt put them there (412).
	Sent StorageOutcome = "sent"
	// Resign: the signature was refused (expired, or another value): sign again.
	Resign StorageOutcome = "resign"
	// Bad: refused as not the bytes signed (400 BadDigest).
	Bad StorageOutcome = "bad"
	// Busy: wait and try again (429, 5xx).
	Busy StorageOutcome = "busy"
	// Unsent: never sent, safe to send again at once.
	Unsent StorageOutcome = "unsent"
	// Unknown: sent, but no answer came: commit first (section 17.1, rule 5).
	Unknown StorageOutcome = "unknown"
	// Refused: anything else, a bug or an unsupported request, never retried in a loop.
	Refused StorageOutcome = "refused"
)

// OutcomeOf reads what a PUT to the storage network ended in (nil: success).
func OutcomeOf(err error) StorageOutcome {
	if err == nil {
		return Sent
	}
	var ce *errs.ConnError
	if errors.As(err, &ce) {
		if ce.Unsent {
			return Unsent
		}
		return Unknown
	}
	var ae *errs.APIError
	if errors.As(err, &ae) {
		s := ae.StatusCode
		switch {
		case s == http.StatusPreconditionFailed:
			return Sent
		case s == http.StatusUnauthorized || s == http.StatusForbidden:
			return Resign
		case s == http.StatusBadRequest && (ae.Code == "BadDigest" || strings.Contains(ae.Message, "BadDigest")):
			return Bad
		case s == http.StatusTooManyRequests || s >= 500:
			return Busy
		}
	}
	return Refused
}

// IsThrottle reports a throttle (429, or 503), on which the adaptive limit halves.
func IsThrottle(err error) bool {
	var ae *errs.APIError
	return errors.As(err, &ae) && (ae.StatusCode == http.StatusTooManyRequests || ae.StatusCode == http.StatusServiceUnavailable)
}

// IsPassing reports a failure worth waiting out: the network, or a busy or failing server (the client has already
// retried it).
func IsPassing(err error) bool {
	var ce *errs.ConnError
	if errors.As(err, &ce) {
		return true
	}
	var ae *errs.APIError
	return errors.As(err, &ae) && (ae.StatusCode == http.StatusTooManyRequests || ae.StatusCode >= 500)
}

// IsUnsupported reports a deployment that can't serve direct storage: 501 (direct uploads off), or a 404 with none
// of the API's codes on a direct route (a deployment from before the routes existed; a missing bucket or pack always
// names itself).
func IsUnsupported(err error) bool {
	var ae *errs.APIError
	if !errors.As(err, &ae) {
		return false
	}
	return ae.Code == "direct_upload_unsupported" || ae.StatusCode == http.StatusNotImplemented ||
		(ae.StatusCode == http.StatusNotFound && (ae.Code == "" || ae.Code == "not_found"))
}

// Unsupported is the error for a deployment that can't serve direct storage (E4): there is no other path to fall
// back to, so it says so plainly.
func Unsupported(err error) error {
	out := &errs.APIError{StatusCode: http.StatusNotImplemented, Code: "direct_upload_unsupported",
		Message: "this deployment doesn't accept direct uploads, the only way this SDK stores objects; " +
			"it needs a deployment with direct storage on"}
	var ae *errs.APIError
	if errors.As(err, &ae) {
		out.StatusCode, out.RequestID = ae.StatusCode, ae.RequestID
		if ae.Code != "" {
			out.Code = ae.Code
		}
	}
	return out
}
