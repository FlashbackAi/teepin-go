// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
	"github.com/FlashbackAi/teepin-go/teepin/internal/journal"
	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// Direct uploads (TEEPIN-STORAGE-DESIGN.md sections 17, 22.4, 22.6 and 25), a port of the Python SDK's _direct.py by
// way of the Node SDK's uploads.ts, with the same rules: files are encrypted on this device, packed, and sent straight
// to the storage network with requests the control plane signs. No file byte passes through Teepin's API.
//
// A pack goes through these states, each written to its store (a run's journal, or memory) before the action that
// needs it (section 17.1, rule 3):
//
//	new → reserved → built → signed → uploading → sent → committing → committed
//
//   - new: its idempotency key is journaled, so a lost reserve answer is asked for again and gets the same
//     reservation.
//   - reserved: object ids and the account key are known.
//   - built: each object encrypted with a fresh data key, its source hash checked again, the pack written, every
//     object decrypted back and checked, and the data keys wrapped. The wraps are journaled before the first sign:
//     wrapping is randomized, so wrapping again would change the manifest.
//   - signed: the pack's one PUT is signed. Its bytes never change from here (section 17.1, rule 4).
//   - uploading, then sent (200, or 412 when an earlier attempt landed), or unknown when no answer came: then the pack
//     is committed first, and sent again only if the storage network doesn't show it.
//   - conflict (the reservation expired) and bad (the bytes stored aren't the bytes signed) start again under a new
//     reservation with fresh keys.
//
// PutObject shares packs between calls waiting at the same time (group commit): a pack forms whenever a send slot is
// free, from whatever is waiting, so a lone call goes at once and packs fill only under load. A run (UploadFiles)
// packs the files it was given, journaled, so ResumeUploads continues a run that was interrupted.
//
// Cancellation is the context (section 22.13's rules, as in section 25.2). A cancel stops every goroutine of the step
// it reaches, which are waited for (up to CleanupCap) before the step returns the context's error, whatever else was
// met on the way. A run's journal holds every state before its action, so a cancelled run resumes like a crashed one;
// it is released (its journal closed, its lock let go) by whichever ends last, the run's own step or its last pack,
// never while a pack might still commit. Cleanup never waits out an outage: one call, best effort.
//
// Large uploads in parts and streams are in large.go, runs in runs.go.

const (
	// RunExpiry is how long an unfinished run stays untouched before it is shown as expired (its files not uploaded).
	RunExpiry = 7 * 24 * time.Hour
	// StateWarnEnv names the environment variable that sets the state directory's size that earns a warning.
	StateWarnEnv = "TEEPIN_STATE_WARN_BYTES"
	// StateWarnBytes is that size unless the variable says (section 17.12).
	StateWarnBytes = 1 << 30
	// commitTimeout bounds a commit or a complete, which verify what storage holds.
	commitTimeout = 70 * time.Second
	// maxResigns is how many times a refused signature is signed again before the request fails.
	maxResigns = 3
)

// Object is an object as the API describes it; package teepin turns it into its own Object.
type Object struct {
	ID             string            `json:"id"`
	Key            string            `json:"key"`
	Size           int64             `json:"size_bytes"`
	ContentType    string            `json:"content_type,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	ChecksumSHA256 string            `json:"checksum_sha256,omitempty"`
	ETag           string            `json:"etag,omitempty"`
	PartCount      int               `json:"part_count,omitempty"`
	Status         string            `json:"status"`
	Backend        string            `json:"backend"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	UploadedAt     *time.Time        `json:"uploaded_at,omitempty"`
}

// WriteOptions are the details and conditions of an object being written.
type WriteOptions struct {
	ContentType string
	Metadata    map[string]string
	// IfMatch writes only over an object with this ETag ("*": any object).
	IfMatch string
	// IfNoneMatch writes only if no object is at the key.
	IfNoneMatch bool
}

// ifMatch is an If-Match value as the control plane takes it: unquoted, or "*".
func (w WriteOptions) ifMatch() string {
	if w.IfMatch == "*" {
		return "*"
	}
	return unquoteETag(w.IfMatch)
}

// Sizes are the sizes an upload engine works with; tests shrink them, as the Python tests patch _engine's constants.
type Sizes struct {
	PartRules
	// PackObjectLimit: objects up to this size are packed; larger ones go in parts. A pack's own sizes and timings are
	// section 28's (Uploads.Limits).
	PackObjectLimit int64
	// StreamConcurrency is a stream's parts in flight unless the caller says.
	StreamConcurrency int
}

// DefaultSizes are the control plane's and the storage network's.
var DefaultSizes = Sizes{PartRules: DefaultPartRules, PackObjectLimit: PackObjectLimit, StreamConcurrency: StreamConcurrency}

// UploadSettings are the client's settings for uploads.
type UploadSettings struct {
	// Wait is how long an upload waits for the storage network or the control plane to come back; 0 fails at once.
	Wait time.Duration
	// Concurrency is the ceiling of packs and parts in flight.
	Concurrency int
	// StateDir is where runs keep their journals, and the account key's pins.
	StateDir string
}

// UploadHooks let tests watch or change an upload's steps, as the Python and Node tests patch methods. Nil fields do
// nothing; set them before any upload starts.
type UploadHooks struct {
	// Saved is told of each state a run journals, right after the write and before the action it is for: a pack's
	// ("pack"), a large upload's ("large") and a part's ("part", "hashed" or "sent").
	Saved func(kind, state string)
	// NewDataKey makes each data key (tws1.NewDataKey when nil).
	NewDataKey func() ([]byte, error)
	// Encrypt encrypts each packed object (tws1.Encrypt when nil).
	Encrypt func(dek, plain []byte) ([]byte, error)
	// ExpiresSoon decides whether a signature may still be used; soon is what its expiry says.
	ExpiresSoon func(soon bool) bool
	// BeforeSend runs before a pack's PUT, given its first key; it may hold it, deaf to the context, as a request in
	// flight may be.
	BeforeSend func(key string)
	// Hashed is told of each file hashed as it is queued, or checked again on resume.
	Hashed func(path string)
	// StreamHeld is told, as a stream reads each part, how many of its parts are held in memory then: the reader's
	// and those in flight.
	StreamHeld func(parts int)
	// StreamSealed runs as a stream has sealed a part, before its plaintext is let go: the moment it holds the most
	// (the parts in flight, the plaintext sealed and its ciphertext, and the next part read). The reader waits for it.
	StreamSealed func()
	// Flight is told of each phase of a PutObject pack in flight (section 28.25): "add", "uploading", "sent" and
	// "remove" (committed says whether it went into its grace time).
	Flight func(phase, id string, committed bool)
}

// Uploads is direct uploads for one client: the account key, PutObject's shared packs, large uploads and streams,
// upload runs, resume and cleanup. Safe for concurrent use.
type Uploads struct {
	api        API
	opts       Options
	uploadWait time.Duration
	stateDir   string
	pins       *journal.Pins

	// Limit is how many packs and parts are in flight.
	Limit *AdaptiveLimit
	// Budget caps the storage network's retries.
	Budget *RetryBudget
	// Sizes are the sizes it works with.
	Sizes Sizes
	// Speed is the measured upload speed to the storage network: on a slow link it shrinks packs, and the parts of
	// uploads created from then on (section 22.7). Tests fix it with Speed.Set.
	Speed *SpeedMeter
	// PutTimeoutFloor and PutTimeoutSpeed bound a storage PUT's own timer from below (PutTimeoutFloor and
	// PutTimeoutSpeed by default; tests shorten them): see PutTimeout.
	PutTimeoutFloor time.Duration
	PutTimeoutSpeed float64
	// CleanupCap bounds how long a stopping step waits for its goroutines, and a cleanup call may take.
	CleanupCap time.Duration
	// Hooks are for tests.
	Hooks UploadHooks
	// Warn is where warnings go (a run removed by CleanUploads, a large state directory); slog's default logger when
	// nil.
	Warn func(msg string)

	// Limits are the pack sizes and timings (section 28): built in, then the server's from each reservation.
	Limits PackLimits
	// Clock is the timing rule's monotonic clock (tests replace it).
	Clock func() time.Duration

	mu          sync.Mutex
	accountID   string
	waiting     map[string]*Arrivals[*packItem] // PutObject's waiting files, per bucket (section 28.17)
	next        time.Duration                   // when the timing rule next needs a decision, if hasNext
	hasNext     bool
	inflight    int64     // PutObject's pack bytes in flight, against MemoryCap
	flights     *InFlight // PutObject's packs in flight and when each is expected back (section 28.25)
	slotWaiters []*signal
}

// monotonic is a clock that counts from its creation on the monotonic clock.
func monotonic() func() time.Duration {
	start := time.Now()
	return func() time.Duration { return time.Since(start) }
}

// NewUploads is the upload engine of a client.
func NewUploads(api API, opts Options, s UploadSettings) *Uploads {
	return &Uploads{api: api, opts: opts, uploadWait: max(s.Wait, 0), stateDir: s.StateDir,
		pins: journal.NewPins(s.StateDir), Limit: NewAdaptiveLimit(StartConcurrency, max(1, s.Concurrency)),
		Budget: NewRetryBudget(0.1, 10), Sizes: DefaultSizes, CleanupCap: CleanupCap, Speed: NewSpeedMeter(),
		PutTimeoutFloor: PutTimeoutFloor, PutTimeoutSpeed: PutTimeoutSpeed, Limits: DefaultLimits, Clock: monotonic(),
		waiting: map[string]*Arrivals[*packItem]{}, flights: NewInFlight()}
}

// flight tells the packs in flight of one of a pack's phases (section 28.25); a run's packs are not followed, so
// their phases change nothing.
func (u *Uploads) flight(phase string, p *pack, committed bool) {
	u.mu.Lock()
	now := u.Clock()
	followed := u.flights.Has(p.id)
	switch phase {
	case "uploading":
		u.flights.Uploading(p.id, now)
	case "sent":
		u.flights.Sent(p.id, p.sentAt)
	case "remove":
		u.flights.Remove(p.id, now, committed)
	}
	h := u.Hooks.Flight
	u.mu.Unlock()
	if h != nil && followed {
		h(phase, p.id, committed)
	}
}

// limits are the pack limits now (the server may change them with any reservation).
func (u *Uploads) limits() PackLimits {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.Limits
}

// callTimeout is a pack call's timeout (29.15): the server's time for objects objects, scaled, plus sending body (nil
// for none) at the speed measured so far; never under floor.
func (u *Uploads) callTimeout(objects int, perObject, floor time.Duration, body any) time.Duration {
	var n int64
	if body != nil {
		if b, err := json.Marshal(body); err == nil {
			n = int64(len(b))
		}
	}
	bps, _ := u.Speed.BytesPerSecond()
	return ScaledTimeout(objects, perObject, floor, BodyTransfer(n, bps, u.PutTimeoutSpeed))
}

// requestBytes is what one request carries at the speed measured so far, or -1 before anything is measured.
func (u *Uploads) requestBytes() int64 {
	if link := u.Speed.Link(); link != nil {
		return link.RequestBytes()
	}
	return -1
}

// PackObjectLimit is the pack-or-parts choice of every way in (PutObject, Upload, UploadFiles, CopyObject): an object
// over it goes in parts. Sizes.PackObjectLimit, or less at the speed measured so far (section 22.7).
func (u *Uploads) PackObjectLimit() int64 {
	return PackObjectLimitAt(u.Speed.Link(), u.Sizes.PackObjectLimit, u.Sizes.MinBytes)
}

// partSize is a new large upload's part size for size bytes, at the speed measured so far: TooSlow's error when the
// link can't carry it. name is for the error.
func (u *Uploads) partSize(size, want int64, name string) (int64, error) {
	return DirectPartSize(size, want, u.Sizes.PartRules, u.Speed.Link(), name)
}

func (u *Uploads) warn(msg string) {
	if u.Warn != nil {
		u.Warn(msg)
		return
	}
	slog.Warn(msg)
}

func (u *Uploads) saved(kind, state string) {
	if h := u.Hooks.Saved; h != nil {
		h(kind, state)
	}
}

func (u *Uploads) newDataKey() ([]byte, error) {
	if h := u.Hooks.NewDataKey; h != nil {
		return h()
	}
	return tws1.NewDataKey()
}

// --- what the steps share ---------------------------------------------------------------------------------------

// signal is a wake-up that can be given more than once.
type signal struct {
	ch   chan struct{}
	once sync.Once
}

func newSignal() *signal { return &signal{ch: make(chan struct{})} }

func (s *signal) fire() { s.once.Do(func() { close(s.ch) }) }

// waits are what a pack, a large upload or a stream has waited for storage to come back: Wait bounds each.
type waits struct {
	mu        sync.Mutex
	waited    time.Duration
	throttled bool
}

func (w *waits) wasThrottled() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.throttled
}

// stop is a pack that can't go on: its items fail with err (final false keeps them journaled for ResumeUploads).
type stop struct {
	err   error
	final bool
}

// Error is the error that stopped the pack.
func (s *stop) Error() string { return s.err.Error() }

// Unwrap is the error that stopped the pack.
func (s *stop) Unwrap() error { return s.err }

// restart is a large upload that can't go on: it starts again under a new create, with a fresh key.
type restart struct{ why string }

// Error says why the upload starts again.
func (r *restart) Error() string { return "teepin: the upload must start again: " + r.why }

// stateError is a write to the local state that failed (the state directory unwritable, a disk full): it stops the
// whole run, whose journal still holds every state written before it.
type stateError struct{ err error }

// Error says which write of the local state failed.
func (e *stateError) Error() string { return "teepin: the upload state: " + e.err.Error() }

// Unwrap is the write's own error.
func (e *stateError) Unwrap() error { return e.err }

// codeOf is an API error's code ("" for any other error).
func codeOf(err error) string {
	var ae *errs.APIError
	if errors.As(err, &ae) {
		return ae.Code
	}
	return ""
}

func statusOf(err error) int {
	var ae *errs.APIError
	if errors.As(err, &ae) {
		return ae.StatusCode
	}
	return 0
}

// stopsRun reports an error that stops a whole run, with its files kept for ResumeUploads: the key revoked (401), no
// permission (403), no credit (402).
func stopsRun(err error) bool {
	switch statusOf(err) {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusPaymentRequired:
		return true
	}
	return false
}

func isUnavailable(err error) bool {
	var su *errs.StorageUnavailableError
	return errors.As(err, &su)
}

// keep reports an error after which an upload is kept, journaled, for ResumeUploads.
func keep(err error) bool { return isUnavailable(err) || stopsRun(err) }

// describe is what an error says, for a journal: its code, and its message.
func describe(err error) string {
	if err == nil {
		return ""
	}
	var ae *errs.APIError
	if errors.As(err, &ae) && ae.Code != "" {
		return ae.Code + ": " + strings.TrimPrefix(err.Error(), "teepin: ")
	}
	return strings.TrimPrefix(err.Error(), "teepin: ")
}

// recorded is an error read back from a journal.
type recorded struct{ msg string }

// Error is the error as the journal recorded it.
func (r *recorded) Error() string { return "teepin: " + r.msg }

func sha256hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func md5hex(b []byte) string {
	s := md5.Sum(b)
	return hex.EncodeToString(s[:])
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// control is a control-plane call (each is idempotent, so the client retries it as safe), with rounds of waiting
// while the control plane is busy or failing, up to the upload's wait (w's; with no w, none). It returns the status.
func (u *Uploads) control(ctx context.Context, method, path string, body, out any, w *waits, timeout time.Duration) (int, error) {
	for round := 0; ; round++ {
		status, err := u.api.Call(ctx, &Call{Method: method, Path: path, Body: body, Out: out, Timeout: timeout})
		if err == nil {
			return status, nil
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if IsUnsupported(err) {
			return 0, Unsupported(err)
		}
		if !IsPassing(err) || w == nil {
			return 0, err
		}
		if err := u.wait(ctx, w, round, err); err != nil {
			return 0, err
		}
	}
}

// wait is one round of waiting for a service to come back; a StorageUnavailableError once w has waited as long as it
// may.
func (u *Uploads) wait(ctx context.Context, w *waits, round int, err error) error {
	var ra time.Duration
	var ae *errs.APIError
	if errors.As(err, &ae) {
		ra = ae.RetryAfter
	}
	delay := WaitDelay(round, ra)
	w.mu.Lock()
	if IsThrottle(err) {
		w.throttled = true
	}
	if w.waited+delay > u.uploadWait {
		w.mu.Unlock()
		return &errs.StorageUnavailableError{Message: fmt.Sprintf("storage didn't come back within %s: %v",
			u.uploadWait, strings.TrimPrefix(err.Error(), "teepin: ")), Err: err}
	}
	w.waited += delay
	w.mu.Unlock()
	if serr := u.opts.sleep(ctx, delay); serr != nil {
		return Stopped(ctx, serr)
	}
	return nil
}

// accountKeyJSON is the account key as the API answers it.
type accountKeyJSON struct {
	AccountID string `json:"account_id"`
	Version   int    `json:"version"`
	PublicKey string `json:"public_key"`
	Suite     string `json:"suite"`
}

// accountKey is the account key, checked: the account id's bytes, the public key and its version.
type accountKey struct {
	account [16]byte
	public  []byte
	version int
}

// checkKey checks an answer's account key against the suite, this client's account and the pin (section 11.4).
func (u *Uploads) checkKey(ak accountKeyJSON) (accountKey, error) {
	var out accountKey
	if ak.Suite != tws1.Suite {
		return out, errs.Integrity("account_key", "unsupported key suite %q", ak.Suite)
	}
	if ak.AccountID == "" {
		return out, errs.Integrity("account_key", "the API didn't name the account its key belongs to")
	}
	account, err := ObjectIDBytes(ak.AccountID)
	if err != nil {
		return out, errs.Integrity("account_key", "the API named account %q, which is not a UUID", ak.AccountID)
	}
	public, err := base64.StdEncoding.DecodeString(ak.PublicKey)
	if err != nil || len(public) != tws1.PublicKeySize {
		return out, errs.Integrity("account_key", "account %s's public key is not a key", ak.AccountID)
	}
	u.mu.Lock()
	if u.accountID == "" {
		u.accountID = ak.AccountID
	}
	other := u.accountID != ak.AccountID
	first := u.accountID
	u.mu.Unlock()
	if other {
		return out, errs.Integrity("account_key", "the API named account %s, not %s, in one client", ak.AccountID, first)
	}
	if !u.pins.Check(ak.AccountID, ak.Version, public) {
		return out, errs.Integrity("account_key", "account %s's key version %d changed; refusing to encrypt to it",
			ak.AccountID, ak.Version)
	}
	return accountKey{account: account, public: public, version: ak.Version}, nil
}

// expiresSoon reports a signature too close to its expiry to use.
func (u *Uploads) expiresSoon(expiresAt string) bool {
	t := when(expiresAt)
	soon := t.IsZero() || time.Until(t) < SignMargin
	if h := u.Hooks.ExpiresSoon; h != nil {
		return h(soon)
	}
	return soon
}

// signedPut is a signed PUT, as sign answers it.
type signedPut struct {
	Method    string         `json:"method"`
	URL       string         `json:"url"`
	Headers   map[string]any `json:"headers"`
	ExpiresAt string         `json:"expires_at"`
}

// putTimedOut is a storage PUT its own timer ended with no answer.
type putTimedOut struct{ limit time.Duration }

func (e *putTimedOut) Error() string {
	return fmt.Sprintf("no answer from the storage network in %s", e.limit.Round(time.Millisecond))
}

// putSigned sends one signed PUT to the storage network, with exactly the signed headers and never the API key, and
// returns its ETag, unquoted. Every pack and part goes through it, measured (section 22.7): one that succeeds is a
// sample of the link's speed; one its own timer ended with no answer is an upper bound (so a slow link is measured
// from a cold start too); any other failure (an error status, a refused connection, a cancel) is never measured. It
// waits for the answer as long as PutTimeout says: at least 30 s, a second per 64 KiB, and on a measured link twice
// the bytes' time at its speed.
func (u *Uploads) putSigned(ctx context.Context, s signedPut, data []byte) (string, error) {
	n := int64(len(data))
	limit := PutTimeout(n, u.PutTimeoutFloor, u.PutTimeoutSpeed, u.Speed.Link())
	start := time.Now()
	rctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	h := make(map[string]string, len(s.Headers))
	for k, v := range s.Headers {
		h[k] = headerValue(v)
	}
	method := s.Method
	if method == "" {
		method = http.MethodPut
	}
	resp, err := u.api.SendSigned(rctx, &SignedRequest{Method: method, URL: s.URL, Header: h,
		Body: bytes.NewReader(data), Size: int64(len(data))})
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if rctx.Err() != nil {
			u.Speed.ObserveTimeout(n, time.Since(start))
			return "", &errs.ConnError{Err: &putTimedOut{limit: limit}}
		}
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	u.Speed.Observe(n, time.Since(start))
	return unquoteETag(resp.Header.Get("ETag")), nil
}

// abortUpload cancels a large upload: one call (the client's own retries, none while stopping), bounded by
// CleanupCap even after a cancel, best effort, since the upload sweeper ends what it misses.
func (u *Uploads) abortUpload(ctx context.Context, bucket, uploadID string, stopping bool) {
	if uploadID == "" {
		return
	}
	_, _ = u.api.Call(context.WithoutCancel(ctx), &Call{Method: http.MethodDelete,
		Path: BucketPath(bucket) + "/uploads/" + uploadID, Timeout: u.CleanupCap, NoRetry: stopping})
}

// --- items and packs ----------------------------------------------------------------------------------------------

// packItem is one object on its way into a pack.
type packItem struct {
	key         string
	size        int64
	sha256      string
	read        func() ([]byte, error)
	contentType string
	metadata    map[string]string
	ifNoneMatch bool
	ifMatch     string
	path        string // the source file, for a run's
	fileID      string // its row in a run's journal

	objectID string
	result   *Object
	err      error
	final    bool // false when the error leaves it journaled for ResumeUploads
	settled  bool
	// journaled is the pack and object id its file row last journaled.
	journaled string

	// PutObject's: picked when a pack takes it (guarded by Uploads.mu), done when its result is in, wake to wake its
	// waiting caller.
	picked   bool
	done     chan struct{}
	doneOnce sync.Once
	wake     *signal

	// order is a run's place for the file (items given back to the planner keep it); rebuilds the rebuilds of the
	// packs it was in, carried when a pack gives it back, so MaxRebuilds holds; reqBytes its reserve-request bytes.
	order    int
	rebuilds int
	reqBytes int64
}

// stored is the item's bytes in a pack: its needle and TWS1 bytes.
func (it *packItem) stored() int64 { return tws1.NeedleHeaderSize + tws1.StoredSize(it.size) }

// request is the bytes the item adds to a reserve body (section 28.17): its JSON object and a separator.
func (it *packItem) request() int64 {
	if it.reqBytes == 0 {
		b, _ := json.Marshal(it.reserveJSON())
		it.reqBytes = int64(len(b)) + 2
	}
	return it.reqBytes
}

func itemStored(it *packItem) int64  { return it.stored() }
func itemKey(it *packItem) string    { return it.key }
func itemRequest(it *packItem) int64 { return it.request() }

func (it *packItem) finish() {
	if it.done != nil {
		it.doneOnce.Do(func() { close(it.done) })
	}
}

func (it *packItem) reserveJSON() map[string]any {
	o := map[string]any{"key": it.key, "size": it.size, "sha256": it.sha256}
	if it.contentType != "" {
		o["content_type"] = it.contentType
	}
	if len(it.metadata) > 0 {
		o["metadata"] = it.metadata
	}
	if it.ifNoneMatch {
		o["if_none_match"] = true
	}
	if it.ifMatch != "" {
		o["if_match"] = it.ifMatch
	}
	return o
}

// reservation is reserve's answer.
type reservation struct {
	Pack       string         `json:"pack"`
	ExpiresAt  string         `json:"expires_at,omitempty"`
	AccountKey accountKeyJSON `json:"account_key"`
	Objects    []struct {
		Key      string `json:"key"`
		ObjectID string `json:"object_id"`
	} `json:"objects,omitempty"`
	// Limits are the server's pack limits (section 28.15); absent from a server before section 28.
	Limits json.RawMessage `json:"limits,omitempty"`
}

// packObject is one object of a pack's manifest.
type packObject struct {
	ObjectID   string `json:"object_id"`
	Offset     int64  `json:"offset"`
	StoredSize int64  `json:"stored_size"`
	WrappedKey string `json:"wrapped_key"`
	KeyVersion int    `json:"key_version"`
}

// signBody is a pack's manifest, as sign takes it: the same every time it is signed.
type signBody struct {
	Size    int64        `json:"size"`
	MD5     string       `json:"md5"`
	SHA256  string       `json:"sha256"`
	Objects []packObject `json:"objects"`
}

// pack is one pack's state, kept in its store (a run's journal, or memory).
type pack struct {
	id       string
	bucket   string
	items    []*packItem
	store    packStore
	state    string
	idem     string
	res      reservation
	body     signBody
	signed   signedPut
	size     int64
	md5      string
	sha256   string
	fileName string
	data     []byte
	attempts int
	rebuilds int
	verifies int
	resigns  int
	waits
	lastError string

	// underFloor is the reason for a pack under the floor (section 28.9), sent with its reserve; "" at or over it.
	underFloor string
	// inflight is its bytes counted against MemoryCap while it is in flight (PutObject's packs).
	inflight int64
	// sentAt is when its upload ended (in memory only), sentUnknown whether that answer was lost, verifyWaited the
	// checking waits since: the bytes are sent again only 30 s (5 s) after it (section 28.18).
	sentAt       time.Duration
	hasSent      bool
	sentUnknown  bool
	verifyWaited time.Duration
	// giveback takes a run's pack's files back to its planner instead of renewing under the floor (28.5).
	giveback func([]*packItem)
	// dissolve: resumed, unsigned and under the floor (28.22 point 5): its idempotency key is asked first.
	dissolve bool
}

func newPack(bucket string, items []*packItem, store packStore, id string) *pack {
	if id == "" {
		id = UUID7()
	}
	return &pack{id: id, bucket: bucket, items: items, store: store, state: "new", idem: UUID7()}
}

func (p *pack) open() []*packItem {
	var out []*packItem
	for _, it := range p.items {
		if !it.settled {
			out = append(out, it)
		}
	}
	return out
}

// doneItem is an item's result, delivered with others in one journal write.
type doneItem struct {
	it    *packItem
	obj   *Object
	err   error
	final bool
}

// packStore is where a pack's state and bytes are kept.
type packStore interface {
	savePack(p *pack) error
	putBytes(p *pack, data []byte) error
	getBytes(p *pack) []byte
	dropBytes(p *pack)
	itemsDone(done []doneItem) error
}

// memoryStore keeps PutObject's packs in memory: nothing could resume them after a crash.
type memoryStore struct{}

func (memoryStore) savePack(*pack) error { return nil }

func (memoryStore) putBytes(p *pack, data []byte) error {
	p.data = data
	return nil
}

func (memoryStore) getBytes(p *pack) []byte { return p.data }

func (memoryStore) dropBytes(p *pack) { p.data = nil }

func (memoryStore) itemsDone(done []doneItem) error {
	for _, d := range done {
		setDone(d)
	}
	return nil
}

func setDone(d doneItem) {
	d.it.result, d.it.err, d.it.final, d.it.settled = d.obj, d.err, d.final, true
}

// done records items' results together, then wakes their callers.
func (u *Uploads) done(store packStore, done ...doneItem) error {
	if len(done) == 0 {
		return nil
	}
	err := store.itemsDone(done)
	for _, d := range done {
		d.it.finish()
	}
	return err
}

// --- a pack's life ------------------------------------------------------------------------------------------------

// drive takes a pack to committed, delivering every item's result. It returns only an error that stops a whole run
// (auth, permission, credit), after recording it on the pack's items; a failed write of the run's state; or the
// context's error, recording nothing (the store holds the state for resume).
func (u *Uploads) drive(ctx context.Context, p *pack) error {
	err := u.steps(ctx, p)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var s *stop
	var se *stateError
	switch {
	case errors.As(err, &s):
		return u.fail(p, s.err, s.final)
	case errors.As(err, &se):
		return err
	case stopsRun(err):
		if ferr := u.fail(p, err, false); ferr != nil {
			return ferr
		}
		return err
	case isUnavailable(err):
		return u.fail(p, err, false)
	}
	return u.fail(p, err, true)
}

func (u *Uploads) steps(ctx context.Context, p *pack) error {
	if p.dissolve {
		if err := u.dissolvePack(ctx, p); err != nil {
			return err
		}
	}
	for p.state != "committed" && p.state != "failed" && p.state != "returned" {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		switch p.state {
		case "new":
			err = u.reserve(ctx, p)
		case "reserved":
			err = u.build(p)
		case "built":
			err = u.sign(ctx, p)
		case "signed":
			err = u.send(ctx, p)
		case "conflict", "bad":
			err = u.renew(p)
		default:
			err = u.commit(ctx, p)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// fail fails a pack's open items with err; final false keeps them journaled for ResumeUploads.
func (u *Uploads) fail(p *pack, err error, final bool) error {
	p.lastError = describe(err)
	if final {
		p.state = "failed"
		p.store.dropBytes(p)
	}
	serr := p.store.savePack(p)
	var done []doneItem
	for _, it := range p.open() {
		done = append(done, doneItem{it: it, err: err, final: final})
	}
	if derr := u.done(p.store, done...); serr == nil {
		serr = derr
	}
	return serr
}

func (u *Uploads) reserve(ctx context.Context, p *pack) error {
	if err := p.store.savePack(p); err != nil { // the idempotency key, before the call
		return err
	}
	objects := make([]map[string]any, 0, len(p.items))
	for _, it := range p.items {
		objects = append(objects, it.reserveJSON())
	}
	var res reservation
	body := map[string]any{"idempotency_key": p.idem, "objects": objects}
	if p.underFloor != "" {
		body["under_floor"] = p.underFloor
	}
	_, err := u.control(ctx, http.MethodPost, BucketPath(p.bucket)+"/packs", body, &res, &p.waits,
		u.callTimeout(len(p.items), ReservePerObject, u.opts.Timeout, body))
	if err != nil {
		switch codeOf(err) {
		case "reservation_abandoned":
			p.state = "conflict"
			return p.store.savePack(p)
		case "pack_committed":
			// Committed earlier, its answer lost: commit again for the stored result.
			var ae *errs.APIError
			errors.As(err, &ae)
			p.res = reservation{Pack: ae.Detail("pack")}
			p.state = "committing"
			return p.store.savePack(p)
		}
		return err
	}
	if _, err := u.checkKey(res.AccountKey); err != nil {
		return err
	}
	if l, ok := LimitsFromJSON(res.Limits); ok {
		u.mu.Lock()
		u.Limits = l
		u.mu.Unlock()
	}
	ids := map[string]string{}
	for _, o := range res.Objects {
		ids[o.Key] = o.ObjectID
	}
	for _, it := range p.items {
		it.objectID = ids[it.key]
	}
	p.res = res
	p.state = "reserved"
	return p.store.savePack(p)
}

func (u *Uploads) build(p *pack) error {
	key, err := u.checkKey(p.res.AccountKey)
	if err != nil {
		return err
	}
	data, objects, changed, err := u.buildPack(p, key)
	if err != nil {
		return err
	}
	if len(changed) > 0 {
		if err := u.done(p.store, changed...); err != nil {
			return err
		}
		// The reservation names files that changed: the rest go under a new one.
		gone := map[*packItem]bool{}
		for _, d := range changed {
			gone[d.it] = true
		}
		var rest []*packItem
		for _, it := range p.items {
			if !gone[it] {
				rest = append(rest, it)
			}
		}
		p.items = rest
		// A run's other files go back to its planner, so they never go as a smaller pack (section 28.5).
		if p.giveback != nil && len(rest) > 0 {
			return u.giveBack(context.Background(), p)
		}
		p.state = "failed"
		if len(rest) > 0 {
			p.state = "conflict"
		}
		p.rebuilds-- // not the pack's fault
		return p.store.savePack(p)
	}
	if err := p.store.putBytes(p, data); err != nil {
		return err
	}
	p.size, p.md5, p.sha256 = int64(len(data)), md5hex(data), sha256hex(data)
	p.body = signBody{Size: p.size, MD5: p.md5, SHA256: p.sha256, Objects: objects}
	p.state = "built"
	return p.store.savePack(p)
}

// buildPack reads every item, checks it against its queued hash, encrypts it with a fresh key and appends it to the
// pack; then decrypts every object back and checks it ("saved" means readable, section 19.3), and wraps its key. It
// returns the items that changed since they were queued instead, when any did.
func (u *Uploads) buildPack(p *pack, key accountKey) ([]byte, []packObject, []doneItem, error) {
	type built struct {
		it  *packItem
		oid [16]byte
		dek []byte
		off int64
		n   int64
	}
	var all []built
	defer func() {
		for _, b := range all {
			clear(b.dek)
		}
	}()
	var changed []doneItem
	var data []byte
	encrypt := tws1.Encrypt
	if h := u.Hooks.Encrypt; h != nil {
		encrypt = h
	}
	for _, it := range p.items {
		plain, err := it.read()
		if err != nil {
			changed = append(changed, doneItem{it: it, err: errs.Changed(it.source(), err.Error()), final: true})
			continue
		}
		if int64(len(plain)) != it.size || sha256hex(plain) != it.sha256 {
			changed = append(changed, doneItem{it: it, err: errs.Changed(it.source(), ""), final: true})
			continue
		}
		oid, err := ObjectIDBytes(it.objectID)
		if err != nil {
			return nil, nil, nil, &stop{err: fmt.Errorf("teepin: the reservation gave %s no object id", it.key), final: true}
		}
		dek, err := u.newDataKey()
		if err != nil {
			return nil, nil, nil, err
		}
		all = append(all, built{it: it, oid: oid, dek: dek})
		stored, err := encrypt(dek, plain)
		if err != nil {
			return nil, nil, nil, err
		}
		var off int64
		data, off = tws1.AppendNeedle(data, oid, stored)
		all[len(all)-1].off, all[len(all)-1].n = off, int64(len(stored))
	}
	if len(changed) > 0 {
		return nil, nil, changed, nil
	}
	for _, b := range all {
		ok := false
		if stored, err := tws1.ReadNeedle(data, b.off, b.oid, b.n); err == nil {
			if plain, err := tws1.Decrypt(b.dek, stored); err == nil {
				ok = sha256hex(plain) == b.it.sha256
			}
		}
		if !ok {
			return nil, nil, nil, &stop{err: errs.Integrity("build", "%s didn't decrypt back to its source", b.it.key), final: true}
		}
	}
	objects := make([]packObject, 0, len(all))
	for _, b := range all {
		wrapped, err := tws1.WrapKey(key.public, key.account, b.oid, b.dek)
		if err != nil {
			return nil, nil, nil, &stop{err: fmt.Errorf("teepin: wrap %s's key: %w", b.it.key, err), final: true}
		}
		objects = append(objects, packObject{ObjectID: b.it.objectID, Offset: b.off, StoredSize: b.n,
			WrappedKey: b64(wrapped), KeyVersion: key.version})
	}
	return data, objects, nil, nil
}

// source is how an item is named in a FileChangedError: its path, or its key.
func (it *packItem) source() string {
	if it.path != "" {
		return it.path
	}
	return it.key
}

func (u *Uploads) sign(ctx context.Context, p *pack) error {
	var signed signedPut
	_, err := u.control(ctx, http.MethodPost, BucketPath(p.bucket)+"/packs/"+p.res.Pack+"/sign", p.body, &signed, &p.waits,
		u.callTimeout(len(p.items), SignPerObject, u.opts.Timeout, p.body))
	if err != nil {
		switch codeOf(err) {
		case "reservation_abandoned":
			p.state = "conflict"
			return p.store.savePack(p)
		case "manifest_mismatch", "bad_wrapped_key", "invalid_request", "pack_not_found":
			return &stop{err: err, final: true}
		}
		return err
	}
	p.signed = signed
	p.state = "signed"
	return p.store.savePack(p)
}

// send is a pack's one PUT.
func (u *Uploads) send(ctx context.Context, p *pack) error {
	if u.expiresSoon(p.signed.ExpiresAt) {
		p.state = "built" // the same manifest signs again
		return p.store.savePack(p)
	}
	if h := u.Hooks.BeforeSend; h != nil && len(p.items) > 0 {
		h(p.items[0].key)
	}
	data := p.store.getBytes(p)
	if data == nil || sha256hex(data) != p.sha256 {
		p.state = "bad" // the pack file is lost or damaged: a new reservation, fresh keys
		return p.store.savePack(p)
	}
	p.state = "uploading"
	u.flight("uploading", p, false)
	if err := p.store.savePack(p); err != nil {
		return err
	}
	u.Budget.OnRequest()
	_, err := u.putSigned(ctx, p.signed, data)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	switch o := OutcomeOf(err); o {
	case Sent, Unknown:
		p.sentAt, p.hasSent, p.sentUnknown, p.verifyWaited = u.Clock(), true, o == Unknown, 0
		u.flight("sent", p, false)
		p.state = "sent"
		if o == Unknown {
			p.state = "unknown"
		}
	case Resign:
		p.resigns++
		if p.resigns > maxResigns {
			return &stop{err: err, final: true}
		}
		p.state = "built"
	case Bad:
		p.state = "bad"
	case Unsent:
		p.state = "signed"
		if !u.Budget.TryRetry() {
			if werr := u.wait(ctx, &p.waits, p.attempts, err); werr != nil {
				return werr
			}
		}
		p.attempts++
	case Busy:
		p.state = "signed"
		if werr := u.wait(ctx, &p.waits, p.attempts, err); werr != nil {
			return werr
		}
		p.attempts++
	default:
		return &stop{err: err, final: true}
	}
	return p.store.savePack(p)
}

// commitResult is commit's answer.
type commitResult struct {
	Status      string `json:"status"`
	CommittedAt string `json:"committed_at"`
	Backend     string `json:"backend"`
	Objects     []struct {
		Key      string `json:"key"`
		ObjectID string `json:"object_id"`
		Result   string `json:"result"`
		Error    string `json:"error"`
	} `json:"objects"`

	// Purged: committed, then every object in it deleted and the pack's row purged (29.15); no per-object results.
	Purged bool `json:"purged"`
}

func (u *Uploads) commit(ctx context.Context, p *pack) error {
	unknown := p.state == "unknown" || p.state == "uploading"
	if p.state == "sent" {
		p.state = "committing"
		if err := p.store.savePack(p); err != nil {
			return err
		}
	}
	var res commitResult
	status, err := u.control(ctx, http.MethodPost, BucketPath(p.bucket)+"/packs/"+p.res.Pack+"/commit", nil, &res,
		&p.waits, u.callTimeout(len(p.items), CommitPerObject, commitTimeout, nil))
	if err != nil {
		switch codeOf(err) {
		case "reservation_abandoned":
			p.state = "conflict"
		case "pack_mismatch":
			p.state = "bad"
		case "pack_not_signed":
			p.state = "built"
		case "bucket_not_found":
			return &stop{err: err, final: true}
		default:
			return err
		}
		return p.store.savePack(p)
	}
	if status == http.StatusAccepted {
		// Not there yet (section 28.18): asked again after 250 ms, 500 ms, 1 s and 2 s, then every 2 s; the same bytes
		// are sent again only 30 s after an upload that answered success, or 5 s after one whose answer was lost (or a
		// pack resumed mid-upload). The time since is the larger of the clock's and the waits'. The waits count against
		// the upload wait, but a pack always gets its first full cycle (verifying is a normal answer, not storage being
		// away): a pack the network never shows ends after one sending again, kept for ResumeUploads.
		now := u.Clock()
		if !p.hasSent {
			p.sentAt, p.hasSent, p.sentUnknown, p.verifyWaited = now, true, unknown, 0
		}
		limit := ResendAfterSent
		if p.sentUnknown {
			limit = ResendAfterLost
		}
		since := max(now-p.sentAt, p.verifyWaited)
		if since >= limit {
			p.state = "signed" // not there: send the same bytes again
			p.verifies, p.hasSent = 0, false
			return p.store.savePack(p)
		}
		delay := min(VerifyDelay(p.verifies), limit-since) // the last look comes just as the bytes may go again
		p.verifies++
		p.waits.mu.Lock()
		if p.waits.waited+delay > max(u.uploadWait, limit) {
			p.waits.mu.Unlock()
			return &errs.StorageUnavailableError{Message: fmt.Sprintf(
				"storage didn't show the pack within %s of waiting", max(u.uploadWait, limit))}
		}
		p.waits.waited += delay
		p.waits.mu.Unlock()
		p.verifyWaited += delay
		if serr := u.opts.sleep(ctx, delay); serr != nil {
			return Stopped(ctx, serr)
		}
		return p.store.savePack(p)
	}
	if err := u.deliver(p, &res); err != nil {
		return err
	}
	p.state = "committed"
	p.store.dropBytes(p)
	return p.store.savePack(p)
}

// renew gives a pack's items a new reservation and fresh keys (section 17.3).
// A run's pack under the floor gives its files back to the planner instead (section 28.5); the old reservation is
// released either way (section 28.23).
func (u *Uploads) renew(p *pack) error {
	p.rebuilds++
	if p.rebuilds > MaxRebuilds {
		why := p.lastError
		if why == "" {
			why = p.state
		}
		return &stop{err: fmt.Errorf("teepin: the pack was rebuilt %d times: %s", MaxRebuilds, why), final: true}
	}
	var stored int64
	for _, it := range p.items {
		stored += it.stored()
	}
	if p.giveback != nil && stored < u.limits().Floor {
		return u.giveBack(context.Background(), p)
	}
	if p.res.Pack != "" {
		u.releaseReservation(context.Background(), p)
	}
	p.lastError = p.state
	p.store.dropBytes(p)
	p.idem = UUID7()
	p.res = reservation{}
	p.body = signBody{}
	p.signed = signedPut{}
	p.verifies, p.resigns = 0, 0
	for _, it := range p.items {
		it.objectID = ""
	}
	p.state = "new"
	return p.store.savePack(p)
}

// dissolvePack is a resumed pack that was never signed and is under the floor: reserve is asked with its own
// idempotency key (and its files as journaled), so a pack an earlier run already committed answers pack_committed and
// is committed for its stored result. Any other answer releases what it reserved, and its files go back to the planner
// with the rest (section 28.22, point 5).
func (u *Uploads) dissolvePack(ctx context.Context, p *pack) error {
	p.dissolve = false
	p.underFloor = ReasonLeftover // not part of the idempotency digest
	if err := u.reserve(ctx, p); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var ae *errs.APIError
		if !errors.As(err, &ae) {
			return err
		}
		// e.g. pack_too_small from a server in enforce: nothing was committed under this key
	}
	if p.state == "committing" {
		return nil
	}
	return u.giveBack(ctx, p)
}

// giveBack hands a run's pack's remaining files back to its planner (section 28.5): its reservation is released and
// the pack ends as returned, its files planned again with the rest.
func (u *Uploads) giveBack(ctx context.Context, p *pack) error {
	if p.res.Pack != "" {
		u.releaseReservation(ctx, p)
	}
	p.store.dropBytes(p)
	p.state = "returned"
	if err := p.store.savePack(p); err != nil {
		return err
	}
	items := p.open()
	for _, it := range items {
		it.objectID = ""
		it.rebuilds = max(it.rebuilds, p.rebuilds)
	}
	p.giveback(items)
	return nil
}

// releaseReservation frees a given-up reservation at once (section 28.23), best effort: a deployment without the
// route, or any failure, leaves it to expire as before.
func (u *Uploads) releaseReservation(ctx context.Context, p *pack) {
	call := &Call{Method: http.MethodPost, Path: BucketPath(p.bucket) + "/packs/" + p.res.Pack + "/release"}
	_, _ = u.api.Call(ctx, call)
}

// deliver gives each item its result from a commit's answer.
func (u *Uploads) deliver(p *pack, res *commitResult) error {
	at := when(res.CommittedAt)
	if res.Purged {
		// Committed, then every object in it deleted and the pack's row purged (TEEPIN-STORAGE-DESIGN.md 29.15): the
		// answer has no per-object results, but each file was saved. Never uploaded again.
		slog.Info(fmt.Sprintf("teepin: pack %s was committed at %s; its objects have since been deleted", p.res.Pack,
			res.CommittedAt))
		var done []doneItem
		for _, it := range p.open() {
			t := at
			done = append(done, doneItem{it: it, final: true, obj: &Object{ID: it.objectID, Key: it.key, Size: it.size,
				ContentType: it.contentType, Metadata: it.metadata, ChecksumSHA256: it.sha256, ETag: it.sha256,
				Status: "deleted", CreatedAt: at, UpdatedAt: at, UploadedAt: &t}})
		}
		return u.done(p.store, done...)
	}
	type entry = struct {
		Key      string `json:"key"`
		ObjectID string `json:"object_id"`
		Result   string `json:"result"`
		Error    string `json:"error"`
	}
	byID, byKey := map[string]entry{}, map[string]entry{}
	for _, o := range res.Objects {
		byID[o.ObjectID] = o
		byKey[o.Key] = o
	}
	var done []doneItem
	for _, it := range p.open() {
		r, ok := byID[it.objectID]
		if !ok || it.objectID == "" {
			r, ok = byKey[it.key]
		}
		switch {
		case !ok:
			done = append(done, doneItem{it: it, err: fmt.Errorf("teepin: the commit didn't report %s", it.key), final: true})
		case r.Result == "saved":
			id := r.ObjectID
			if id == "" {
				id = it.objectID
			}
			t := at
			obj := &Object{ID: id, Key: it.key, Size: it.size, ContentType: it.contentType, Metadata: it.metadata,
				ChecksumSHA256: it.sha256, ETag: it.sha256, Status: "available", Backend: res.Backend, CreatedAt: at,
				UpdatedAt: at, UploadedAt: &t}
			done = append(done, doneItem{it: it, obj: obj, final: true})
		default:
			code := r.Error
			if code == "" {
				code = "upload_failed"
			}
			status := http.StatusConflict
			if code == "precondition_failed" {
				status = http.StatusPreconditionFailed
			}
			done = append(done, doneItem{it: it, final: true, err: &errs.APIError{StatusCode: status, Code: code,
				Message: it.key + ": " + strings.ReplaceAll(code, "_", " ")}})
		}
	}
	return u.done(p.store, done...)
}

// --- PutObject: shared packs ----------------------------------------------------------------------------------------

// Put stores one object of up to the pack limit in a pack shared with the other calls waiting now, and returns it
// once its pack commits. A waiting call dispatches the queue itself whenever a slot comes free (release wakes it), so
// a slot freed anywhere serves the queue. The wait for a slot is bounded by the upload wait: an item no pack took by
// then leaves the queue (so it certainly wasn't saved) with a StorageUnavailableError; once a pack has it, the pack's
// own bounded waits decide (section 22.4). A cancel while it waits takes it out of the queue; once a pack has it, the
// pack goes on (it is shared) and the call stops waiting for it. data becomes the engine's: the caller never changes
// it afterwards.
func (u *Uploads) Put(ctx context.Context, bucket, key string, data []byte, w WriteOptions) (*Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	it := &packItem{key: key, size: int64(len(data)), sha256: sha256hex(data), read: func() ([]byte, error) { return data, nil },
		contentType: w.ContentType, metadata: cloneMap(w.Metadata), ifNoneMatch: w.IfNoneMatch, ifMatch: w.ifMatch(),
		final: true, done: make(chan struct{})}
	u.mu.Lock()
	a := u.waiting[bucket]
	if a == nil {
		a = NewArrivals[*packItem]()
		u.waiting[bucket] = a
	}
	a.Add(it, it.key, it.stored(), it.request(), u.Clock())
	u.mu.Unlock()
	// The observe time (28.25) doesn't count against the upload wait: a lone save sits in it before its pack may go,
	// and with an upload wait of zero it would otherwise fail while slots are free.
	deadline := time.Now().Add(u.uploadWait + u.limits().Observe)
	leave := func() bool {
		u.mu.Lock()
		defer u.mu.Unlock()
		a := u.waiting[bucket]
		if a == nil || it.picked {
			return false
		}
		a.Remove([]*packItem{it})
		if a.Len() == 0 {
			delete(u.waiting, bucket)
		}
		if len(u.waiting) == 0 {
			u.Limit.SetPriority(false) // no save is left to hold a run up
		}
		return true
	}
	picked := func() bool {
		u.mu.Lock()
		defer u.mu.Unlock()
		return it.picked
	}
	for !picked() {
		// Registered before dispatching, so a slot freed meanwhile wakes this call.
		woken := newSignal()
		u.mu.Lock()
		it.wake = woken
		u.slotWaiters = append(u.slotWaiters, woken)
		u.mu.Unlock()
		u.dispatch()
		if picked() {
			break
		}
		left := time.Until(deadline)
		if left <= 0 {
			break
		}
		u.mu.Lock()
		if u.hasNext {
			left = min(left, max(time.Millisecond, u.next-u.Clock()))
		}
		u.mu.Unlock()
		t := time.NewTimer(left)
		select {
		case <-woken.ch:
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			leave()
			return nil, ctx.Err()
		}
		t.Stop()
	}
	if !picked() && leave() {
		return nil, &errs.StorageUnavailableError{Message: fmt.Sprintf(
			"no upload slot came free within %s; %s wasn't saved", u.uploadWait, key)}
	}
	select {
	case <-it.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if it.err != nil {
		return nil, it.err
	}
	if it.result == nil {
		return nil, fmt.Errorf("teepin: %s: no result", key)
	}
	return it.result, nil
}

// dispatch sends the packs section 28's timing rule allows, oldest bucket first, each when a slot is free and the
// memory cap allows, and notes when the rule next needs a decision. While a pack would go but has no slot, a run's next
// pack waits for it (priority); a pack that is only gathering holds no run up.
func (u *Uploads) dispatch() {
	for {
		u.mu.Lock()
		bucket, plan, reason, ok := u.choose()
		if !ok {
			u.mu.Unlock()
			return
		}
		for _, it := range plan.Pack {
			it.picked = true
			if it.wake != nil {
				it.wake.fire() // its caller may be waiting: its file has a pack now
			}
		}
		p := newPack(bucket, plan.Pack, memoryStore{}, "")
		p.underFloor, p.inflight = reason, plan.Stored
		u.flights.Add(p.id, bucket, plan.Stored)
		h := u.Hooks.Flight
		u.mu.Unlock()
		if h != nil {
			h("add", p.id, false)
		}
		go u.runShared(p)
	}
}

// choose is the next pack to send and its reason (a slot taken for it), under u.mu; it sets the next decision time
// and the priority.
func (u *Uploads) choose() (string, Plan[*packItem], string, bool) {
	now := u.Clock()
	requestBytes := u.requestBytes()
	limits, slow := u.Limits.AtSpeed(requestBytes)
	bps, _ := u.Speed.BytesPerSecond()
	absorbCap := int64(0)
	if !slow {
		absorbCap = AbsorbCap(u.Limits, requestBytes)
	}
	choices, next, hasNext := Schedule(u.waiting, now, limits,
		func(a *Arrivals[*packItem], tailCap int64) Plan[*packItem] {
			return PlanPackCapped(a.Items(), itemStored, itemKey, itemRequest, limits, tailCap >= 0, tailCap)
		},
		func(bucket string, by time.Duration) int64 {
			return u.flights.Coming(bucket, by, now, bps, limits.Observe, u.waiting[bucket].ArrivedSince)
		},
		u.Limit.RunsWaiting(), absorbCap)
	u.next, u.hasNext = next, hasNext
	blocked := false
	for _, c := range choices {
		if u.inflight > 0 && u.inflight+c.Plan.Stored > MemoryCap {
			blocked = true
			continue
		}
		if !u.Limit.TryAcquire() {
			blocked = true
			continue
		}
		a := u.waiting[c.Bucket]
		a.Remove(c.Plan.Pack)
		if a.Len() == 0 {
			delete(u.waiting, c.Bucket)
		}
		u.inflight += c.Plan.Stored
		u.Limit.SetPriority(blocked)
		return c.Bucket, c.Plan, u.reasonFor(c.Plan, c.Decision.Reason, slow), true
	}
	u.Limit.SetPriority(blocked)
	return "", Plan[*packItem]{}, "", false
}

// reasonFor is a pack's reason for being under the floor (section 28.9), or "" at or over it. Callers hold u.mu or
// read Limits consistently.
func (u *Uploads) reasonFor(p Plan[*packItem], reason string, slow bool) string {
	switch {
	case p.Stored >= u.Limits.Floor:
		return ""
	case slow:
		return ReasonSlowLink
	case p.State == PackRequestLimited:
		return ReasonRequestLimit
	case reason != "":
		return reason
	}
	return ReasonLeftover
}

// runShared drives one of PutObject's packs: shared by its callers, so no one caller's cancel stops it; its own
// bounded waits end it.
func (u *Uploads) runShared(p *pack) {
	outcome := OutcomeFailed
	defer func() {
		// Out of the packs in flight, into its grace time (28.25), before its slot is released. Its callers were woken
		// as each item was delivered: one that saved again meanwhile counted twice until here, which can only hold a
		// pack, and the release below has it decided again.
		u.flight("remove", p, p.state == "committed")
		for _, it := range p.items {
			it.finish()
		}
		u.mu.Lock()
		u.inflight -= p.inflight
		u.mu.Unlock()
		u.release(outcome)
	}()
	if err := u.drive(context.Background(), p); err != nil {
		for _, it := range p.items {
			if !it.settled {
				setDone(doneItem{it: it, err: err, final: true}) // every caller gets its answer
			}
		}
		return
	}
	outcome = outcomeOf(p)
}

// outcomeOf is how a driven pack's slot ended, for the adaptive limit.
func outcomeOf(p *pack) Outcome {
	switch {
	case p.wasThrottled():
		return OutcomeThrottled
	case p.state == "committed":
		return OutcomeOK
	}
	return OutcomeFailed
}

// release gives a slot back. Every slot comes back through here, from PutObject's packs, a run's and large uploads'
// parts alike, and PutObject's waiting calls are woken to dispatch: while they wait, a run's next pack waits too
// (AdaptiveLimit's priority).
func (u *Uploads) release(o Outcome) {
	u.Limit.Release(o)
	u.mu.Lock()
	waiters := u.slotWaiters
	u.slotWaiters = nil
	u.mu.Unlock()
	for _, w := range waiters {
		w.fire()
	}
}

// Queued is how many PutObject calls wait for a pack.
func (u *Uploads) Queued() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for _, a := range u.waiting {
		n += a.Len()
	}
	return n
}

// ReleaseSlot gives back a slot taken with Limit.TryAcquire, leaving the limit as it is, and wakes PutObject's
// waiting calls, as a pack's end does.
func (u *Uploads) ReleaseSlot() { u.release(OutcomeFailed) }

func cloneMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// --- empty objects -------------------------------------------------------------------------------------------------

// PutEmpty stores an empty object through the metadata-only route (section 23.5, A2), which carries no bytes: a pack
// holds none, and an upload needs none. The client retries it as a safe call, with no rounds of waiting.
func (u *Uploads) PutEmpty(ctx context.Context, bucket, key string, w WriteOptions) (*Object, error) {
	body := map[string]any{"key": key}
	if w.ContentType != "" {
		body["content_type"] = w.ContentType
	}
	if len(w.Metadata) > 0 {
		body["metadata"] = w.Metadata
	}
	if w.IfNoneMatch {
		body["if_none_match"] = true
	}
	if im := w.ifMatch(); im != "" {
		body["if_match"] = im
	}
	var out Object
	call := &Call{Method: http.MethodPost, Path: BucketPath(bucket) + "/object/empty", Body: body, Out: &out}
	if _, err := u.api.Call(ctx, call); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if IsUnsupported(err) {
			return nil, Unsupported(err)
		}
		var ae *errs.APIError
		if errors.As(err, &ae) && ae.StatusCode == http.StatusPreconditionFailed && w.IfNoneMatch && call.Attempts > 0 {
			c := *ae
			c.Message = strings.TrimSuffix(ae.Message, ".") + "; if this upload was retried, an earlier attempt may have stored it"
			return nil, &c
		}
		return nil, err
	}
	out.ETag = unquoteETag(out.ETag)
	return &out, nil
}
