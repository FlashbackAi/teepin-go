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
//   - Decide: when a save's pack goes (28.4, 28.17).

import (
	"encoding/json"
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
}

// DefaultLimits are the limits a client starts with, until the server sends its own (tests replace them).
var DefaultLimits = PackLimits{Floor: DefaultPackFloor, Ceiling: DefaultPackCeiling, MaxObjects: DefaultMaxObjects,
	MaxPackObjects: DefaultMaxPackObjects, MaxRequest: DefaultMaxRequest, Wait: DefaultPackWait,
	Window: DefaultRateWindow, Settle: DefaultSettle}

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
	return PackLimits{Floor: floor, Ceiling: ceiling, MaxObjects: int(v["max_objects"]),
		MaxPackObjects: int(v["max_pack_objects"]), MaxRequest: v["max_request_bytes"],
		Wait: time.Duration(v["wait_ms"]) * time.Millisecond, Window: time.Duration(v["rate_window_ms"]) * time.Millisecond,
		Settle: time.Duration(v["settle_ms"]) * time.Millisecond}, true
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
		if size < l.Floor && len(p.Pack)+len(p.Rest) <= l.MaxPackObjects && p.Request+req <= budget {
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

// Decide is when a save's pack goes (28.4, 28.17). Times are durations on the caller's own monotonic clock. waiting is
// the stored bytes that can join the next pack; oldest and last the first and latest arrival among them; arrivals the
// saves that arrived ready to join, without the oldest waiting file's own (28.22: it says nothing about what comes
// next). A pack that would send with no free slot is idle: it keeps filling until one frees.
func Decide(state string, waiting int64, oldest, last time.Duration, arrivals []Arrival, now time.Duration,
	l PackLimits, slotFree bool) Decision {
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
	if float64(waiting)+rate*left.Seconds() < float64(l.Floor) {
		return send(ReasonLeftover)
	}
	at := oldest + l.Wait
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
		}
		if len(same) == 0 {
			delete(a.byKey, e.key)
			continue
		}
		a.byKey[e.key] = same
		if e.joins {
			if next := a.entries[same[0]]; !next.joins {
				next.joins = true
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
