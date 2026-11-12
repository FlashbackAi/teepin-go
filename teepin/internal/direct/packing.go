// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

// Pack sizes (TEEPIN-STORAGE-DESIGN.md section 28): every write to the storage network is at least 10 MiB, unless it
// is a leftover with its reason. Pure: no I/O and no clock of its own (times are passed in), the same rules as the
// other uploaders, held to them by testdata/pack_planner.json.
//
//   - PackLimits: the sizes and timings, built in and sent by the server with each reservation (28.15).
//   - PlanPack: which waiting items the next pack takes (28.3, 28.17).
//   - Arrivals: the running totals of one bucket's waiting saves, so the timing rule decides without planning.
//   - Decide: when a save's pack goes (28.4, 28.17, 28.25).
//   - InFlight: the saves' packs in flight and when each is expected back, whose callers will very likely save again
//     (28.25 point 2).
//   - Schedule: one dispatch's decisions for every bucket, oldest first (28.17's driver, without its slots and memory).

import (
	"encoding/json"
	"slices"
	"sort"
	"time"
)

// The built-in limits (28.2).
const (
	DefaultPackFloor   = 10 << 20
	DefaultPackCeiling = 20 << 20
	DefaultMaxObjects  = 10000
	// DefaultMaxPackObjects is the server's cap: a pack that has just reached the floor, plus a run's tail (28.14).
	DefaultMaxPackObjects = 140000
	// DefaultMaxRequest is a reserve or sign body's limit on the server.
	DefaultMaxRequest = 64 << 20
	DefaultPackWait   = 5 * time.Second
	DefaultRateWindow = time.Second
	DefaultSettle     = 200 * time.Millisecond
	// DefaultObserve is how long a pack under the floor waits before it may go as a leftover early (28.25 point 1):
	// within it, a burst's arrivals land in the rate window. The server may send 0 to MaxObserveMs.
	DefaultObserve = 300 * time.Millisecond
	MaxObserveMs   = 2000
	// CommitUsualStart is a commit's usual time until any is measured; CommitSamples how many of the last ones its
	// median takes (28.25 point 2).
	CommitUsualStart = 1500 * time.Millisecond
	CommitSamples    = 8
	// RequestOverhead is a reserve body's bytes besides its objects, with room to spare.
	RequestOverhead = 512
	// MemoryCap is the pack bytes in flight per client (28.2): 16 full packs.
	MemoryCap = 320 << 20
	// MaxPackBytes is the pack cap: no ceiling may pass it.
	MaxPackBytes = 64 << 20
)

// SlowMin: on a slow link a pack never shrinks below one TWS1 segment (28.7). A variable only so tests can lower it.
var SlowMin int64 = 64 << 10

// A pack's states (28.17).
const (
	PackFull           = "full"
	PackReady          = "ready"
	PackUnder          = "under"
	PackRequestLimited = "request_limited"
)

// The reasons a pack under the floor gives (28.9).
const (
	ReasonLeftover     = "leftover"
	ReasonSlowLink     = "slow_link"
	ReasonRequestLimit = "request_limit"
)

// The timing rule's answers.
const (
	DecideSend = "send"
	DecideWait = "wait"
	DecideIdle = "idle"
)

// PackLimits are the pack sizes (bytes as stored) and timings a client follows.
type PackLimits struct {
	Floor, Ceiling             int64
	MaxObjects, MaxPackObjects int
	MaxRequest                 int64
	Wait, Window, Settle       time.Duration
	Observe                    time.Duration
}

// DefaultLimits are the limits a client starts with, until the server sends its own (tests replace them).
var DefaultLimits = PackLimits{Floor: DefaultPackFloor, Ceiling: DefaultPackCeiling, MaxObjects: DefaultMaxObjects,
	MaxPackObjects: DefaultMaxPackObjects, MaxRequest: DefaultMaxRequest, Wait: DefaultPackWait,
	Window: DefaultRateWindow, Settle: DefaultSettle, Observe: DefaultObserve}

// RequestBudget is the bytes a reserve body's objects may take.
func (l PackLimits) RequestBudget() int64 { return max(0, l.MaxRequest-RequestOverhead) }

// LimitsFromJSON reads the reserve answer's limits (28.15), whole or not at all: false when absent or when any value
// breaks section 28's rules (the caller then keeps what it has).
func LimitsFromJSON(raw json.RawMessage) (PackLimits, bool) {
	if len(raw) == 0 {
		return PackLimits{}, false
	}
	var d map[string]json.RawMessage
	if json.Unmarshal(raw, &d) != nil {
		return PackLimits{}, false
	}
	v := map[string]int64{}
	for _, k := range []string{"floor_bytes", "ceiling_bytes", "max_objects", "max_pack_objects", "max_request_bytes",
		"wait_ms", "rate_window_ms", "settle_ms"} {
		r, ok := d[k]
		if !ok || len(r) == 0 || r[0] == '"' { // json.Number would also take a quoted number
			return PackLimits{}, false
		}
		var n json.Number
		if json.Unmarshal(r, &n) != nil {
			return PackLimits{}, false
		}
		i, err := n.Int64()
		if err != nil {
			return PackLimits{}, false // a string, a fraction or out of range
		}
		v[k] = i
	}
	floor, ceiling := v["floor_bytes"], v["ceiling_bytes"]
	switch {
	case floor < 1<<20 || ceiling < 2*floor || ceiling > MaxPackBytes:
		return PackLimits{}, false
	case v["max_objects"] < 1 || v["max_pack_objects"] < 1 || v["max_request_bytes"] < 1<<20:
		return PackLimits{}, false
	case v["max_objects"] > 1<<31-1 || v["max_pack_objects"] > 1<<31-1:
		return PackLimits{}, false
	}
	for _, k := range []string{"wait_ms", "rate_window_ms", "settle_ms"} {
		if v[k] < 0 || v[k] > 60000 {
			return PackLimits{}, false
		}
	}
	// observe_ms (28.25) is optional: a server before it sends none, and its other limits still hold.
	observe := int64(DefaultObserve / time.Millisecond)
	if r, ok := d["observe_ms"]; ok && string(r) != "null" {
		if len(r) == 0 || r[0] == '"' {
			return PackLimits{}, false
		}
		var n json.Number
		if json.Unmarshal(r, &n) != nil {
			return PackLimits{}, false
		}
		i, err := n.Int64()
		if err != nil || i < 0 || i > MaxObserveMs {
			return PackLimits{}, false
		}
		observe = i
	}
	return PackLimits{Floor: floor, Ceiling: ceiling, MaxObjects: int(v["max_objects"]),
		MaxPackObjects: int(v["max_pack_objects"]), MaxRequest: v["max_request_bytes"],
		Wait: time.Duration(v["wait_ms"]) * time.Millisecond, Window: time.Duration(v["rate_window_ms"]) * time.Millisecond,
		Settle: time.Duration(v["settle_ms"]) * time.Millisecond, Observe: time.Duration(observe) * time.Millisecond}, true
}

// AtSpeed is l at the measured speed (28.7): requestBytes is what one request carries in 240 s (negative before
// anything is measured). The ceiling comes down toward the floor. Under the floor the link is slow (the second value):
// packs shrink as before section 28, each taking files only while it stays within that size (at least SlowMin), so
// the floor is one byte and the ceiling that size.
func (l PackLimits) AtSpeed(requestBytes int64) (PackLimits, bool) {
	if requestBytes < 0 || requestBytes >= l.Ceiling {
		return l, false
	}
	if requestBytes >= l.Floor {
		l.Ceiling = requestBytes
		return l, false
	}
	l.Floor, l.Ceiling = 1, max(SlowMin, requestBytes)
	return l, true
}

// Plan is the next pack: its items in order, the items left, its state, and its stored and reserve-request bytes.
type Plan[T any] struct {
	Pack, Rest      []T
	State           string
	Stored, Request int64
}

// PlanPack chooses which of items (in order) the next pack takes (28.3, 28.17):
//   - an item whose key is in the pack, or among the earlier items left, waits (writes to one key keep their order);
//   - under the floor, the pack takes the next item whatever its size and the count, until its reserve body would
//     pass the request budget (request_limited);
//   - at or over the floor, it takes an item only if it stays at or under the ceiling, under MaxObjects and within the
//     request budget; a pack that turned one away is full, else ready;
//   - tail (runs): when everything left is over zero and under the floor, and can join (no key in it waits on
//     another), the pack takes it all, within MaxPackObjects and the request budget.
func PlanPack[T any](items []T, storedOf func(T) int64, keyOf func(T) string, requestOf func(T) int64, l PackLimits,
	tail bool) Plan[T] {
	return PlanPackCapped(items, storedOf, keyOf, requestOf, l, tail, -1)
}

// PlanPackCapped is PlanPack with a cap on the tail: with tailCap at 0 or more (a save's remnant, 28.25), the pack
// takes its tail only while it stays within that many stored bytes.
func PlanPackCapped[T any](items []T, storedOf func(T) int64, keyOf func(T) string, requestOf func(T) int64,
	l PackLimits, tail bool, tailCap int64) Plan[T] {
	var p Plan[T]
	keys := map[string]bool{}
	restKeys := map[string]bool{}
	budget := l.RequestBudget()
	turned, limited := false, false
	for _, it := range items {
		k := keyOf(it)
		if keys[k] || restKeys[k] {
			p.Rest = append(p.Rest, it)
			restKeys[k] = true
			continue
		}
		n, r := storedOf(it), requestOf(it)
		var take bool
		if p.Stored < l.Floor {
			take = !limited && (len(p.Pack) == 0 || p.Request+r <= budget)
			limited = !take
		} else {
			take = p.Stored+n <= l.Ceiling && len(p.Pack) < l.MaxObjects && p.Request+r <= budget
			turned = turned || !take
		}
		if take {
			p.Pack = append(p.Pack, it)
			keys[k] = true
			p.Stored += n
			p.Request += r
		} else {
			p.Rest = append(p.Rest, it)
			restKeys[k] = true
		}
	}
	if tail && len(p.Rest) > 0 && joins(p.Rest, keyOf, keys) {
		var size, req int64
		for _, it := range p.Rest {
			size += storedOf(it)
			req += requestOf(it)
		}
		if size < l.Floor && len(p.Pack)+len(p.Rest) <= l.MaxPackObjects && p.Request+req <= budget &&
			(tailCap < 0 || p.Stored+size <= tailCap) {
			p.Pack = append(p.Pack, p.Rest...)
			p.Stored += size
			p.Request += req
			p.Rest = nil
			turned = false
		}
	}
	switch {
	case p.Stored >= l.Floor && turned:
		p.State = PackFull
	case p.Stored >= l.Floor:
		p.State = PackReady
	case limited:
		p.State = PackRequestLimited
	default:
		p.State = PackUnder
	}
	return p
}

// joins reports whether every item left can join the pack: no key twice, none in it.
func joins[T any](rest []T, keyOf func(T) string, keys map[string]bool) bool {
	seen := make(map[string]bool, len(rest))
	for _, it := range rest {
		k := keyOf(it)
		if keys[k] || seen[k] {
			return false
		}
		seen[k] = true
	}
	return true
}

// Decision is the timing rule's answer: send (with the reason for a pack under the floor, or ""), wait until At, or
// idle (a send with no free slot).
type Decision struct {
	Kind   string
	Reason string
	At     time.Duration
}

// Arrival is a save that arrived able to join the next pack: when (on the caller's clock) and its stored bytes.
type Arrival struct {
	At     time.Duration
	Stored int64
}

// Decide is when a save's pack goes (28.4, 28.17, 28.25). Times are durations on the caller's own monotonic clock.
// waiting is the stored bytes that can join the next pack; oldest and last the first and latest arrival among them;
// arrivals the saves that arrived ready to join, without the oldest waiting file's own (28.22: it says nothing about
// what comes next). A pack that would send with no free slot is idle: it keeps filling until one frees.
//
// A pack under the floor goes at its time limit. Before it, it goes early only when nothing says a batch is forming
// (28.25):
//   - not before its oldest file has waited l.Observe, so a burst's arrivals reach the rate window first; unless a
//     later write to one of its keys waits behind it (only its going lets that write go: 75 saves to one key still go
//     one by one at once);
//   - coming: the stored bytes of this bucket's packs in flight expected back before the time limit count as
//     arriving, as their callers will very likely save again;
//   - not while contested: another pack waits for a slot or settles (a full or ready one, or a run's next pack),
//     which a leftover must not take.
func Decide(state string, waiting int64, oldest, last time.Duration, arrivals []Arrival, now time.Duration,
	l PackLimits, slotFree bool, coming int64, contested, behind bool) Decision {
	send := func(reason string) Decision {
		if slotFree {
			return Decision{Kind: DecideSend, Reason: reason}
		}
		return Decision{Kind: DecideIdle}
	}
	switch state {
	case PackFull:
		return send("")
	case PackRequestLimited:
		return send(ReasonRequestLimit)
	case PackReady:
		due := min(last+l.Settle, oldest+l.Wait)
		if now >= due {
			return send("")
		}
		return Decision{Kind: DecideWait, At: due}
	}
	left := oldest + l.Wait - now
	if left <= 0 {
		return send(ReasonLeftover)
	}
	at := oldest + l.Wait
	if !behind && now < oldest+l.Observe {
		return Decision{Kind: DecideWait, At: min(at, oldest+l.Observe)}
	}
	var recent int64
	first := time.Duration(-1)
	for _, a := range arrivals {
		if a.At > now-l.Window {
			recent += a.Stored
			if first < 0 || a.At < first {
				first = a.At
			}
		}
	}
	rate := 0.0
	if l.Window > 0 {
		rate = float64(recent) / l.Window.Seconds()
	}
	if !contested && float64(waiting)+float64(coming)+rate*left.Seconds() < float64(l.Floor) {
		return send(ReasonLeftover)
	}
	if first >= 0 {
		at = min(at, first+l.Window)
	}
	return Decision{Kind: DecideWait, At: at}
}

// Arrivals is one bucket's waiting saves and their running totals (28.17, 28.22). A save whose key already has an
// earlier write waiting can't join the next pack: it counts neither as bytes waiting nor as an arrival. When the
// earlier write leaves, it joins the bytes waiting, but never counts as an arrival. The first waiting save is always
// one that can join, so it is the oldest the timing rule looks at. Not safe for concurrent use: the caller locks.
type Arrivals[T comparable] struct {
	order   []T // arrival order; removed entries are compacted lazily
	entries map[T]*arrival
	byKey   map[string][]T
	seq     uint64
	window  []windowed
	// Waiting and Request are the stored and reserve-request bytes that can join; Last the latest arrival among them.
	Waiting, Request int64
	Last             time.Duration
	// Blocked is how many waiting saves can't join the next pack (an earlier write to their key waits).
	Blocked int
}

type arrival struct {
	key             string
	stored, request int64
	at              time.Duration
	joins           bool
	seq             uint64
}

type windowed struct {
	at     time.Duration
	stored int64
	seq    uint64
}

// NewArrivals is an empty set of waiting saves.
func NewArrivals[T comparable]() *Arrivals[T] {
	return &Arrivals[T]{entries: map[T]*arrival{}, byKey: map[string][]T{}}
}

// Len is how many saves wait.
func (a *Arrivals[T]) Len() int { return len(a.entries) }

// Add records a save that arrived at now.
func (a *Arrivals[T]) Add(item T, key string, stored, request int64, now time.Duration) {
	a.seq++
	same := a.byKey[key]
	e := &arrival{key: key, stored: stored, request: request, at: now, joins: len(same) == 0, seq: a.seq}
	a.byKey[key] = append(same, item)
	a.entries[item] = e
	a.order = append(a.order, item)
	if e.joins {
		a.Waiting += stored
		a.Request += request
		a.Last = now
		a.window = append(a.window, windowed{at: now, stored: stored, seq: e.seq})
	} else {
		a.Blocked++
	}
}

// Remove forgets saves a pack took (or that left the queue).
func (a *Arrivals[T]) Remove(items []T) {
	for _, item := range items {
		e, ok := a.entries[item]
		if !ok {
			continue
		}
		delete(a.entries, item)
		same := a.byKey[e.key]
		for i, x := range same {
			if x == item {
				same = append(same[:i:i], same[i+1:]...)
				break
			}
		}
		if e.joins {
			a.Waiting -= e.stored
			a.Request -= e.request
		} else {
			a.Blocked--
		}
		if len(same) == 0 {
			delete(a.byKey, e.key)
			continue
		}
		a.byKey[e.key] = same
		if e.joins {
			if next := a.entries[same[0]]; !next.joins {
				next.joins = true
				a.Blocked--
				a.Waiting += next.stored
				a.Request += next.request
			}
		}
	}
	if len(a.order) > 2*len(a.entries)+16 {
		kept := a.order[:0]
		for _, it := range a.order {
			if _, ok := a.entries[it]; ok {
				kept = append(kept, it)
			}
		}
		a.order = kept
	}
}

// Items are the waiting saves, in arrival order.
func (a *Arrivals[T]) Items() []T {
	out := make([]T, 0, len(a.entries))
	for _, it := range a.order {
		if _, ok := a.entries[it]; ok {
			out = append(out, it)
		}
	}
	return out
}

// Oldest is the first waiting save and when it arrived (false when none).
func (a *Arrivals[T]) Oldest() (T, time.Duration, bool) {
	for _, it := range a.order {
		if e, ok := a.entries[it]; ok {
			return it, e.at, true
		}
	}
	var zero T
	return zero, 0, false
}

// RecentArrivals are the arrivals within window of now, without the oldest waiting save's own; older ones are
// forgotten.
func (a *Arrivals[T]) RecentArrivals(now, window time.Duration) []Arrival {
	kept := a.window[:0]
	for _, w := range a.window {
		if w.at > now-window {
			kept = append(kept, w)
		}
	}
	a.window = kept
	var skip uint64
	if it, _, ok := a.Oldest(); ok {
		skip = a.entries[it].seq
	}
	out := make([]Arrival, 0, len(kept))
	for _, w := range kept {
		if w.seq != skip {
			out = append(out, Arrival{At: w.at, Stored: w.stored})
		}
	}
	return out
}

// Behind reports whether a later write to a key in the next pack waits behind it.
func (a *Arrivals[T]) Behind() bool { return a.Blocked > 0 }

// ArrivedAt is when a waiting save arrived.
func (a *Arrivals[T]) ArrivedAt(item T) time.Duration { return a.entries[item].at }

// StoredOf is a waiting save's stored bytes.
func (a *Arrivals[T]) StoredOf(item T) int64 { return a.entries[item].stored }

// ArrivedSince is the stored bytes of the saves that arrived able to join at t or later (within the rate window kept),
// whether still waiting or not.
func (a *Arrivals[T]) ArrivedSince(t time.Duration) int64 {
	var n int64
	for _, w := range a.window {
		if w.at >= t {
			n += w.stored
		}
	}
	return n
}

// InFlight is the saves' packs in flight, and when each is expected back (28.25 point 2): the callers blocked in a
// pack will very likely save again once it commits, so its bytes count as coming arrivals.
//
// Expected back, by phase: not uploading yet, now plus its sending time plus a commit's usual time; uploading, from
// the upload's start; sent, the send plus a commit's usual time (the median of the last CommitSamples commits,
// CommitUsualStart until one is measured). Before any speed is measured, every pack counts as coming: at worst a save
// waits the time limit.
//
// A pack that committed keeps counting for a grace time (the observe time), less the bytes that arrived since it came
// back: its callers are on their way back, and between its commit and their saves it is neither in flight nor
// waiting (a small group waiting for it would otherwise go alone just before them).
//
// Not safe for concurrent use: the driver holds its lock. Times are passed in.
type InFlight struct {
	packs    map[string]*flight
	commits  []time.Duration
	returned []returnedPack
}

type flight struct {
	bucket         string
	stored         int64
	up, sent       time.Duration
	hasUp, hasSent bool
}

type returnedPack struct {
	bucket string
	stored int64
	at     time.Duration
}

// NewInFlight is no packs in flight.
func NewInFlight() *InFlight { return &InFlight{packs: map[string]*flight{}} }

// Len is how many packs are in flight.
func (f *InFlight) Len() int { return len(f.packs) }

// Has reports whether key is in flight.
func (f *InFlight) Has(key string) bool { _, ok := f.packs[key]; return ok }

// Add is a save's pack sent on its way.
func (f *InFlight) Add(key, bucket string, stored int64) {
	f.packs[key] = &flight{bucket: bucket, stored: stored}
}

// Uploading is its upload starting (again: a pack sent once more starts over).
func (f *InFlight) Uploading(key string, now time.Duration) {
	if e := f.packs[key]; e != nil {
		e.up, e.hasUp, e.hasSent = now, true, false
	}
}

// Sent is its upload ended: its commit is being checked.
func (f *InFlight) Sent(key string, now time.Duration) {
	if e := f.packs[key]; e != nil {
		e.sent, e.hasSent = now, true
	}
}

// Remove is it leaving: a committed pack's time since its send is a commit's time, and it keeps counting for its grace.
func (f *InFlight) Remove(key string, now time.Duration, committed bool) {
	e := f.packs[key]
	if e == nil {
		return
	}
	delete(f.packs, key)
	if !committed {
		return
	}
	if e.hasSent {
		f.commits = append(f.commits, max(0, now-e.sent))
		if len(f.commits) > CommitSamples {
			f.commits = f.commits[len(f.commits)-CommitSamples:]
		}
	}
	f.returned = append(f.returned, returnedPack{bucket: e.bucket, stored: e.stored, at: now})
}

// CommitUsual is a commit's usual time: the median of the last ones measured.
func (f *InFlight) CommitUsual() time.Duration {
	if len(f.commits) == 0 {
		return CommitUsualStart
	}
	s := slices.Clone(f.commits)
	slices.Sort(s)
	if n := len(s); n%2 == 1 {
		return s[n/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// Expected is when the pack is expected back, at bytesPerSecond (0 before any speed is measured: now).
func (f *InFlight) Expected(key string, now time.Duration, bytesPerSecond float64) time.Duration {
	e := f.packs[key]
	usual := f.CommitUsual()
	if e.hasSent {
		return e.sent + usual
	}
	if bytesPerSecond <= 0 {
		return now
	}
	from := now
	if e.hasUp {
		from = e.up
	}
	return from + time.Duration(float64(e.stored)/bytesPerSecond*float64(time.Second)) + usual
}

// Coming is the stored bytes of bucket's packs expected back by by, and of those that came back within grace of now
// less what arrived since (arrivedSince(t): the stored bytes that arrived at t or later; nil for none).
func (f *InFlight) Coming(bucket string, by, now time.Duration, bytesPerSecond float64, grace time.Duration,
	arrivedSince func(time.Duration) int64) int64 {
	kept := f.returned[:0]
	for _, r := range f.returned {
		if now < r.at+grace {
			kept = append(kept, r)
		}
	}
	f.returned = kept
	var ahead int64
	for k, e := range f.packs {
		if e.bucket == bucket && f.Expected(k, now, bytesPerSecond) <= by {
			ahead += e.stored
		}
	}
	var back int64
	since, seen := time.Duration(0), false
	for _, r := range f.returned {
		if r.bucket == bucket {
			back += r.stored
			if !seen || r.at < since {
				since, seen = r.at, true
			}
		}
	}
	if seen {
		var arrived int64
		if arrivedSince != nil {
			arrived = arrivedSince(since)
		}
		ahead += max(0, back-arrived)
	}
	return ahead
}

// AbsorbCap is the most stored bytes a save's pack may reach by taking its remnant (28.25): the ceiling plus the
// floor, never past what the measured speed sends in one request's time (28.7; requestBytes negative before any is
// measured), nor the pack cap. l are the server's limits, before the speed's clamp.
func AbsorbCap(l PackLimits, requestBytes int64) int64 {
	c := min(l.Ceiling+l.Floor, MaxPackBytes)
	if requestBytes < 0 {
		return c
	}
	return min(c, requestBytes)
}

// RemnantAlone reports whether what a full save's pack turns away is a remnant with nothing coming for it (28.25): its
// rest stored bytes, plus coming, plus its own arrivals in the window (without its oldest's) at their rate for the
// time its oldest has left, stay under the floor. The batch that is going is no evidence for it: a steady stream
// still turns files away. Decided when the pack goes, never after a wait.
func RemnantAlone(rest int64, oldest time.Duration, arrivals []Arrival, coming int64, now time.Duration,
	l PackLimits) bool {
	if rest <= 0 || rest >= l.Floor {
		return false
	}
	var recent int64
	for _, a := range arrivals {
		if a.At > now-l.Window {
			recent += a.Stored
		}
	}
	rate := 0.0
	if l.Window > 0 {
		rate = float64(recent) / l.Window.Seconds()
	}
	left := max(0, oldest+l.Wait-now)
	return float64(rest)+float64(coming)+rate*left.Seconds() < float64(l.Floor)
}

// Choice is a pack the timing rule sends now: its bucket, plan and decision.
type Choice[T comparable] struct {
	Bucket   string
	Plan     Plan[T]
	Decision Decision
}

// Schedule is one dispatch (28.17's driver, 28.25): every bucket's decision, oldest bucket first. It returns the packs
// that may go now, in that order (the driver gives each a slot and memory, or holds it), and the earliest time a
// waiting one next needs a decision (false when none waits).
//
// A bucket's pack is planned only when its totals are at the floor, over the request budget, or it goes (28.22 point
// 6). A bucket under the floor is contested while another bucket's totals are at the floor or over the budget (a
// full, ready or request-limited pack waits or settles), or a run's next pack waits for a slot.
//
// planOf(arrivals, tailCap) plans a bucket's next pack, with its tail taken within tailCap bytes when tailCap is 0 or
// more. A full pack that goes takes its remnant (RemnantAlone) within absorbCap (AbsorbCap; 0 takes none).
func Schedule[T comparable](queues map[string]*Arrivals[T], now time.Duration, l PackLimits,
	planOf func(a *Arrivals[T], tailCap int64) Plan[T], comingOf func(bucket string, by time.Duration) int64,
	runsWaiting bool, absorbCap int64) ([]Choice[T], time.Duration, bool) {
	type bucketAt struct {
		name string
		at   time.Duration
	}
	var order []bucketAt
	for b, a := range queues {
		if _, at, ok := a.Oldest(); ok {
			order = append(order, bucketAt{b, at})
		}
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].at != order[j].at {
			return order[i].at < order[j].at
		}
		return order[i].name < order[j].name
	})
	pending := map[string]bool{}
	for _, b := range order {
		a := queues[b.name]
		if a.Waiting >= l.Floor || a.Request > l.RequestBudget() {
			pending[b.name] = true
		}
	}
	var out []Choice[T]
	var next time.Duration
	hasNext := false
	for _, b := range order {
		bucket, a, oldest := b.name, queues[b.name], b.at
		recent := a.RecentArrivals(now, l.Window)
		coming := comingOf(bucket, oldest+l.Wait)
		contested := runsWaiting
		for other := range pending {
			if other != bucket {
				contested = true
			}
		}
		behind := a.Behind()
		var p Plan[T]
		planned := false
		var d Decision
		if !pending[bucket] {
			d = Decide(PackUnder, a.Waiting, oldest, a.Last, recent, now, l, true, coming, contested, behind)
		} else {
			p, planned = planOf(a, -1), true
			d = Decide(p.State, p.Stored, oldest, a.Last, recent, now, l, true, coming, contested, behind)
		}
		if d.Kind == DecideSend && !planned {
			p = planOf(a, -1)
			if p.State != PackUnder { // decided again from the planned pack
				d = Decide(p.State, p.Stored, oldest, a.Last, recent, now, l, true, coming, contested, behind)
			}
		}
		if d.Kind == DecideWait {
			if !hasNext || d.At < next {
				next, hasNext = d.At, true
			}
			continue
		}
		if d.Kind == DecideSend && p.State == PackFull && len(p.Rest) > 0 && absorbCap > 0 {
			first := p.Rest[0]
			since := a.ArrivedAt(first)
			own := make([]Arrival, 0, len(p.Rest)-1)
			for _, it := range p.Rest[1:] {
				own = append(own, Arrival{At: a.ArrivedAt(it), Stored: a.StoredOf(it)})
			}
			if RemnantAlone(a.Waiting-p.Stored, since, own, comingOf(bucket, since+l.Wait), now, l) {
				if whole := planOf(a, absorbCap); len(whole.Rest) == 0 && whole.Stored >= l.Floor {
					p = whole
				}
			}
		}
		out = append(out, Choice[T]{Bucket: bucket, Plan: p, Decision: d})
	}
	return out, next, hasNext
}
