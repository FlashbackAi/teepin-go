// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// Section 28's planner and timing rule: the shared vectors (testdata/pack_planner.json, a byte-identical copy of
// teepin-python's tests/vectors/pack_planner.json), and the properties and cases the vectors can't hold.

type vecLimits struct {
	Floor          int64   `json:"floor"`
	Ceiling        int64   `json:"ceiling"`
	MaxObjects     int     `json:"max_objects"`
	MaxPackObjects int     `json:"max_pack_objects"`
	RequestBudget  int64   `json:"request_budget"`
	Wait           float64 `json:"wait"`
	Window         float64 `json:"window"`
	Settle         float64 `json:"settle"`
	Observe        float64 `json:"observe"`
}

func secs(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

func (v vecLimits) limits() PackLimits {
	return PackLimits{Floor: v.Floor, Ceiling: v.Ceiling, MaxObjects: v.MaxObjects, MaxPackObjects: v.MaxPackObjects,
		MaxRequest: v.RequestBudget + RequestOverhead, Wait: secs(v.Wait), Window: secs(v.Window), Settle: secs(v.Settle),
		Observe: secs(v.Observe)}
}

type vecItem struct {
	key             string
	stored, request int64
}

func (it *vecItem) UnmarshalJSON(b []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil || len(raw) != 3 {
		return fmt.Errorf("an item is [key, stored, request]: %s", b)
	}
	if err := json.Unmarshal(raw[0], &it.key); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[1], &it.stored); err != nil {
		return err
	}
	return json.Unmarshal(raw[2], &it.request)
}

type vecPack struct {
	Items  []int  `json:"items"`
	State  string `json:"state"`
	Stored int64  `json:"stored"`
}

type vectors struct {
	Planner []struct {
		Name   string    `json:"name"`
		Limits vecLimits `json:"limits"`
		Tail   bool      `json:"tail"`
		Items  []vecItem `json:"items"`
		Packs  []vecPack `json:"packs"`
	} `json:"planner"`
	PlannerRepeat []struct {
		Name   string    `json:"name"`
		Limits vecLimits `json:"limits"`
		Repeat struct {
			Count   int    `json:"count"`
			Key     string `json:"key"`
			Stored  int64  `json:"stored"`
			Request int64  `json:"request"`
		} `json:"repeat"`
		RunPackSizes   []int `json:"run_pack_sizes"`
		SavesFirstPack int   `json:"saves_first_pack"`
	} `json:"planner_repeat"`
	Decide []struct {
		Name      string      `json:"name"`
		Limits    vecLimits   `json:"limits"`
		State     string      `json:"state"`
		Waiting   int64       `json:"waiting"`
		Oldest    float64     `json:"oldest"`
		Last      float64     `json:"last"`
		Arrivals  [][]float64 `json:"arrivals"`
		Now       float64     `json:"now"`
		SlotFree  bool        `json:"slot_free"`
		Coming    int64       `json:"coming"`
		Contested bool        `json:"contested"`
		Behind    bool        `json:"behind"`
		Decision  struct {
			Kind   string  `json:"kind"`
			Reason *string `json:"reason"`
			At     float64 `json:"at"`
		} `json:"decision"`
	} `json:"decide"`
	PlannerSaves []struct {
		Name    string    `json:"name"`
		Limits  vecLimits `json:"limits"`
		TailCap *int64    `json:"tail_cap"`
		Items   []vecItem `json:"items"`
		Pack    struct {
			Items  []int  `json:"items"`
			Rest   []int  `json:"rest"`
			State  string `json:"state"`
			Stored int64  `json:"stored"`
		} `json:"pack"`
	} `json:"planner_saves"`
	Remnant []struct {
		Name     string      `json:"name"`
		Limits   vecLimits   `json:"limits"`
		Rest     int64       `json:"rest"`
		Oldest   float64     `json:"oldest"`
		Arrivals [][]float64 `json:"arrivals"`
		Coming   int64       `json:"coming"`
		Now      float64     `json:"now"`
		Alone    bool        `json:"alone"`
	} `json:"remnant"`
	AbsorbCap []struct {
		Name         string    `json:"name"`
		Limits       vecLimits `json:"limits"`
		RequestBytes *int64    `json:"request_bytes"`
		Cap          int64     `json:"cap"`
	} `json:"absorb_cap"`
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	b, err := os.ReadFile("testdata/pack_planner.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// planAll plans pack after pack over item indexes, as a run (tail) or saves do.
func planAll(items []vecItem, l PackLimits, tail bool) []Plan[int] {
	idx := make([]int, len(items))
	for i := range idx {
		idx[i] = i
	}
	var out []Plan[int]
	for len(idx) > 0 {
		p := PlanPack(idx, func(i int) int64 { return items[i].stored }, func(i int) string { return items[i].key },
			func(i int) int64 { return items[i].request }, l, tail)
		out = append(out, p)
		idx = p.Rest
	}
	return out
}

func TestPackPlannerVectors(t *testing.T) {
	v := loadVectors(t)
	if len(v.Planner) < 40 || len(v.Decide) < 10 || len(v.PlannerRepeat) != 1 {
		t.Fatalf("vectors: %d planner, %d decide, %d repeat", len(v.Planner), len(v.Decide), len(v.PlannerRepeat))
	}
	for _, c := range v.Planner {
		got := planAll(c.Items, c.Limits.limits(), c.Tail)
		packs := make([]vecPack, len(got))
		for i, p := range got {
			packs[i] = vecPack{Items: p.Pack, State: p.State, Stored: p.Stored}
		}
		if !reflect.DeepEqual(packs, c.Packs) {
			t.Errorf("%s:\n got  %+v\n want %+v", c.Name, packs, c.Packs)
		}
	}
	for _, c := range v.PlannerRepeat {
		r := c.Repeat
		items := make([]vecItem, r.Count)
		for i := range items {
			items[i] = vecItem{key: strings.ReplaceAll(r.Key, "{i}", fmt.Sprint(i)), stored: r.Stored, request: r.Request}
		}
		var sizes []int
		for _, p := range planAll(items, c.Limits.limits(), true) {
			sizes = append(sizes, len(p.Pack))
		}
		if !reflect.DeepEqual(sizes, c.RunPackSizes) {
			t.Errorf("%s: run packs %v, want %v", c.Name, sizes, c.RunPackSizes)
		}
		if first := planAll(items, c.Limits.limits(), false)[0]; len(first.Pack) != c.SavesFirstPack {
			t.Errorf("%s: first save pack %d, want %d", c.Name, len(first.Pack), c.SavesFirstPack)
		}
	}
	for _, c := range v.Decide {
		arrivals := make([]Arrival, len(c.Arrivals))
		for i, a := range c.Arrivals {
			arrivals[i] = Arrival{At: secs(a[0]), Stored: int64(a[1])}
		}
		d := Decide(c.State, c.Waiting, secs(c.Oldest), secs(c.Last), arrivals, secs(c.Now), c.Limits.limits(), c.SlotFree,
			c.Coming, c.Contested, c.Behind)
		reason := ""
		if c.Decision.Reason != nil {
			reason = *c.Decision.Reason
		}
		if d.Kind != c.Decision.Kind || d.Reason != reason || d.At != secs(c.Decision.At) {
			t.Errorf("%s: got %+v, want %+v (reason %q)", c.Name, d, c.Decision, reason)
		}
	}
	if len(v.PlannerSaves) < 5 || len(v.Remnant) < 5 || len(v.AbsorbCap) < 3 {
		t.Fatalf("28.25 vectors: %d planner_saves, %d remnant, %d absorb_cap", len(v.PlannerSaves), len(v.Remnant),
			len(v.AbsorbCap))
	}
	for _, c := range v.PlannerSaves {
		items := c.Items
		idx := make([]int, len(items))
		for i := range idx {
			idx[i] = i
		}
		tailCap := int64(-1)
		if c.TailCap != nil {
			tailCap = *c.TailCap
		}
		p := PlanPackCapped(idx, func(i int) int64 { return items[i].stored }, func(i int) string { return items[i].key },
			func(i int) int64 { return items[i].request }, c.Limits.limits(), tailCap >= 0, tailCap)
		rest := p.Rest
		if rest == nil {
			rest = []int{}
		}
		if !reflect.DeepEqual(p.Pack, c.Pack.Items) || !reflect.DeepEqual(rest, c.Pack.Rest) || p.State != c.Pack.State ||
			p.Stored != c.Pack.Stored {
			t.Errorf("%s: got %v %v %s %d, want %+v", c.Name, p.Pack, rest, p.State, p.Stored, c.Pack)
		}
	}
	for _, c := range v.Remnant {
		arrivals := make([]Arrival, len(c.Arrivals))
		for i, a := range c.Arrivals {
			arrivals[i] = Arrival{At: secs(a[0]), Stored: int64(a[1])}
		}
		if got := RemnantAlone(c.Rest, secs(c.Oldest), arrivals, c.Coming, secs(c.Now), c.Limits.limits()); got != c.Alone {
			t.Errorf("%s: alone %v, want %v", c.Name, got, c.Alone)
		}
	}
	for _, c := range v.AbsorbCap {
		rb := int64(-1)
		if c.RequestBytes != nil {
			rb = *c.RequestBytes
		}
		if got := AbsorbCap(c.Limits.limits(), rb); got != c.Cap {
			t.Errorf("%s: cap %d, want %d", c.Name, got, c.Cap)
		}
	}
}

// The properties 28.3 names, over seeded runs with real sizes.
func TestPackPlannerProperties(t *testing.T) {
	l := DefaultLimits
	for seed := int64(0); seed < 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := 1 + rng.Intn(400)
		items := make([]vecItem, n)
		unique := true
		for i := range items {
			var size int64
			switch rng.Intn(3) {
			case 0:
				size = int64(153 + rng.Intn(4000))
			case 1:
				size = int64(1 + rng.Intn(3<<20))
			default:
				size = int64(1 + rng.Intn(16<<20+153))
			}
			key := fmt.Sprintf("u%d", i)
			if rng.Float64() < 0.2 {
				key = fmt.Sprintf("k%d", rng.Intn(300))
				unique = false
			}
			items[i] = vecItem{key: key, stored: size, request: int64(80 + rng.Intn(320))}
		}
		packs := planAll(items, l, true)
		var flat []int
		lastPack := map[string]int{}
		lastIndex := map[string]int{}
		small := 0
		var total int64
		for pi, p := range packs {
			keys := map[string]bool{}
			var stored int64
			for _, i := range p.Pack {
				k := items[i].key
				if keys[k] {
					t.Fatalf("seed %d: key %s twice in a pack", seed, k)
				}
				keys[k] = true
				if prev, ok := lastPack[k]; ok && (prev >= pi || lastIndex[k] >= i) {
					t.Fatalf("seed %d: writes to %s out of order", seed, k)
				}
				lastPack[k], lastIndex[k] = pi, i
				stored += items[i].stored
				flat = append(flat, i)
			}
			if stored != p.Stored || stored >= 36<<20 {
				t.Fatalf("seed %d: pack of %d bytes (counted %d)", seed, stored, p.Stored)
			}
			if stored < l.Floor {
				small++
			}
		}
		for _, it := range items {
			total += it.stored
		}
		sort.Ints(flat)
		for i, x := range flat {
			if x != i {
				t.Fatalf("seed %d: item %d not in exactly one pack", seed, i)
			}
		}
		if unique && (small > 1 || (small == 1 && total >= 2*l.Floor)) {
			t.Fatalf("seed %d: %d packs under the floor in a run of %d bytes", seed, small, total)
		}
	}
}

func TestLimitsFromJSON(t *testing.T) {
	good := `{"floor_bytes":10485760,"ceiling_bytes":20971520,"max_objects":10000,"max_pack_objects":140000,` +
		`"max_request_bytes":67108864,"wait_ms":5000,"rate_window_ms":1000,"settle_ms":200}`
	if l, ok := LimitsFromJSON(json.RawMessage(good)); !ok || l != DefaultLimits {
		t.Fatalf("defaults: %+v %v", l, ok)
	}
	for _, bad := range []string{"", "null", "{}", `"x"`,
		strings.Replace(good, "10485760", "1048575", 1),
		strings.Replace(good, "20971520", "20971519", 1),
		strings.Replace(good, `"max_objects":10000`, `"max_objects":0`, 1),
		strings.Replace(good, `"wait_ms":5000`, `"wait_ms":-1`, 1),
		strings.Replace(good, `"settle_ms":200`, `"settle_ms":60001`, 1),
		strings.Replace(good, `"floor_bytes":10485760`, `"floor_bytes":"10485760"`, 1),
		strings.Replace(good, `"floor_bytes":10485760`, `"floor_bytes":10485760.5`, 1),
		strings.Replace(good, `,"settle_ms":200`, ``, 1),
		strings.Replace(good, `}`, `,"observe_ms":-1}`, 1),
		strings.Replace(good, `}`, `,"observe_ms":2001}`, 1),
		strings.Replace(good, `}`, `,"observe_ms":0.5}`, 1),
		strings.Replace(good, `}`, `,"observe_ms":"300"}`, 1),
	} {
		if _, ok := LimitsFromJSON(json.RawMessage(bad)); ok {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestArrivalsManyWritesToOneKey(t *testing.T) {
	// 28.22: 75 saves to one key. Only the first can join, so the others count neither as bytes waiting nor as
	// arrivals, and each save goes at once as a leftover.
	a := NewArrivals[int]()
	for i := 0; i < 75; i++ {
		a.Add(i, "same", 1000, 50, time.Duration(i)*time.Millisecond)
	}
	if a.Waiting != 1000 || a.Len() != 75 {
		t.Fatalf("waiting %d of %d", a.Waiting, a.Len())
	}
	first, at, _ := a.Oldest()
	now := 75 * time.Millisecond
	if !a.Behind() {
		t.Fatal("74 writes wait behind the first")
	}
	if d := Decide(PackUnder, a.Waiting, at, a.Last, a.RecentArrivals(now, time.Second), now, DefaultLimits, true, 0,
		false, a.Behind()); first != 0 ||
		d.Kind != DecideSend || d.Reason != ReasonLeftover {
		t.Fatalf("first %d, decision %+v", first, d)
	}
	a.Remove([]int{0})
	next, at, _ := a.Oldest()
	if next != 1 || a.Waiting != 1000 || len(a.RecentArrivals(now, time.Second)) != 1 {
		t.Fatalf("after the first left: oldest %d, waiting %d", next, a.Waiting)
	}
	if d := Decide(PackUnder, a.Waiting, at, a.Last, a.RecentArrivals(now, time.Second), now, DefaultLimits, true, 0,
		false, a.Behind()); d.Kind != DecideSend {
		t.Fatalf("the next: %+v", d)
	}
	// The last one has nothing behind it: it waits the observe time from its own arrival.
	for i := 1; i < 74; i++ {
		a.Remove([]int{i})
	}
	_, at, _ = a.Oldest()
	if a.Behind() || a.Blocked != 0 || at != 74*time.Millisecond {
		t.Fatalf("the last: blocked %d at %v", a.Blocked, at)
	}
	now = 100 * time.Millisecond
	if d := Decide(PackUnder, a.Waiting, at, a.Last, a.RecentArrivals(now, time.Second), now, DefaultLimits, true, 0,
		false, false); d != (Decision{Kind: DecideWait, At: 374 * time.Millisecond}) {
		t.Fatalf("the last: %+v", d)
	}
}

func TestArrivalsTotals(t *testing.T) {
	a := NewArrivals[string]()
	a.Add("x", "a", 100, 10, time.Second)
	a.Add("y", "b", 200, 20, 1500*time.Millisecond)
	a.Add("z", "c", 300, 30, 2*time.Second)
	if a.Waiting != 600 || a.Request != 60 || a.Last != 2*time.Second {
		t.Fatalf("totals %d %d %v", a.Waiting, a.Request, a.Last)
	}
	got := a.RecentArrivals(2200*time.Millisecond, time.Second)
	if !reflect.DeepEqual(got, []Arrival{{1500 * time.Millisecond, 200}, {2 * time.Second, 300}}) {
		t.Fatalf("arrivals %v (the oldest's own left out)", got)
	}
	a.Remove([]string{"x"})
	if it, at, _ := a.Oldest(); it != "y" || at != 1500*time.Millisecond || a.Waiting != 500 {
		t.Fatalf("after x left: %s %v %d", it, at, a.Waiting)
	}
	if !reflect.DeepEqual(a.Items(), []string{"y", "z"}) {
		t.Fatalf("items %v", a.Items())
	}
}

func TestArrivalsLeaveTheOldestOwnArrivalOutEvenInsideTheWindow(t *testing.T) {
	a := NewArrivals[string]()
	a.Add("x", "x", 4<<20, 10, 1500*time.Millisecond)
	a.Add("y", "y", 1<<20, 10, 1800*time.Millisecond)
	if got := a.RecentArrivals(2*time.Second, time.Second); !reflect.DeepEqual(got, []Arrival{{1800 * time.Millisecond, 1 << 20}}) {
		t.Fatalf("arrivals %v", got)
	}
}

func TestAtSpeed(t *testing.T) {
	l := DefaultLimits
	if got, slow := l.AtSpeed(-1); got != l || slow {
		t.Fatal("unmeasured")
	}
	if got, slow := l.AtSpeed(15 << 20); got.Ceiling != 15<<20 || got.Floor != l.Floor || slow {
		t.Fatalf("between: %+v", got)
	}
	if got, slow := l.AtSpeed(1000); got.Floor != 1 || got.Ceiling != SlowMin || !slow {
		t.Fatalf("slow: %+v", got)
	}
}

// 28.25: observe_ms is optional (a server before it sends none, and its other limits still hold) and bounded.
func TestLimitsObserveIsOptional(t *testing.T) {
	good := `{"floor_bytes":10485760,"ceiling_bytes":20971520,"max_objects":10000,"max_pack_objects":140000,` +
		`"max_request_bytes":67108864,"wait_ms":5000,"rate_window_ms":1000,"settle_ms":200`
	for in, want := range map[string]time.Duration{good + "}": DefaultObserve, good + `,"observe_ms":0}`: 0,
		good + `,"observe_ms":2000}`: 2 * time.Second, good + `,"observe_ms":null}`: DefaultObserve} {
		if l, ok := LimitsFromJSON(json.RawMessage(in)); !ok || l.Observe != want {
			t.Errorf("%s: %v %v", in, l.Observe, ok)
		}
	}
}

func dUnder(waiting int64, now time.Duration, arrivals []Arrival, coming int64, contested, behind bool) Decision {
	return Decide(PackUnder, waiting, 0, 0, arrivals, now, DefaultLimits, true, coming, contested, behind)
}

func TestDecideObservesBeforeALeftover(t *testing.T) {
	// 28.25 point 1: never before the oldest has waited observe; then its own bytes aren't an arrival.
	for _, size := range []int64{400_000, 4 << 20, 9 << 20} {
		if d := dUnder(size, 0, nil, 0, false, false); d != (Decision{Kind: DecideWait, At: 300 * time.Millisecond}) {
			t.Fatalf("%d at 0: %+v", size, d)
		}
		if d := dUnder(size, 300*time.Millisecond, nil, 0, false, false); d.Kind != DecideSend || d.Reason != ReasonLeftover {
			t.Fatalf("%d at 300 ms: %+v", size, d)
		}
	}
	// A burst seen within the observe time holds the pack.
	var burst []Arrival
	for i := 1; i < 20; i++ {
		burst = append(burst, Arrival{At: time.Duration(i) * 2 * time.Millisecond, Stored: 400_000})
	}
	if d := dUnder(8_000_000, 300*time.Millisecond, burst, 0, false, false); d.Kind != DecideWait {
		t.Fatalf("burst: %+v", d)
	}
	// The observe time never holds past the time limit.
	l := DefaultLimits
	l.Wait, l.Observe = 200*time.Millisecond, 300*time.Millisecond
	if d := Decide(PackUnder, 1<<20, 0, 0, nil, 100*time.Millisecond, l, true, 0, false, false); d != (Decision{Kind: DecideWait, At: 200 * time.Millisecond}) {
		t.Fatalf("limit: %+v", d)
	}
}

func TestDecideAWriteBehindSkipsTheObserveTime(t *testing.T) {
	if d := dUnder(1000, 0, nil, 0, false, true); d.Kind != DecideSend || d.Reason != ReasonLeftover {
		t.Fatalf("%+v", d)
	}
	if d := dUnder(1<<20, 0, []Arrival{{0, 3 << 20}}, 0, false, true); d.Kind != DecideWait {
		t.Fatalf("a batch forming: %+v", d)
	}
}

func TestDecidePacksInFlightCountAsComing(t *testing.T) {
	now := 300 * time.Millisecond
	if d := dUnder(1<<20, now, nil, 0, false, false); d.Kind != DecideSend {
		t.Fatalf("%+v", d)
	}
	if d := dUnder(1<<20, now, nil, 9<<20, false, false); d != (Decision{Kind: DecideWait, At: 5 * time.Second}) {
		t.Fatalf("coming: %+v", d)
	}
	if d := dUnder(1<<20, now, nil, 8<<20, false, false); d.Kind != DecideSend {
		t.Fatalf("too little coming: %+v", d)
	}
	if d := dUnder(1<<20, 5*time.Second, nil, 100<<20, false, false); d.Kind != DecideSend {
		t.Fatalf("the limit: %+v", d)
	}
}

func TestDecideALeftoverNeverGoesEarlyWhileContested(t *testing.T) {
	if d := dUnder(1<<20, 300*time.Millisecond, nil, 0, true, false); d != (Decision{Kind: DecideWait, At: 5 * time.Second}) {
		t.Fatalf("%+v", d)
	}
	if d := dUnder(1<<20, 5*time.Second, nil, 0, true, false); d.Kind != DecideSend || d.Reason != ReasonLeftover {
		t.Fatalf("at the limit: %+v", d)
	}
	if d := Decide(PackFull, 12<<20, 0, 0, nil, 0, DefaultLimits, true, 0, true, false); d.Kind != DecideSend {
		t.Fatalf("full: %+v", d)
	}
}

func TestArrivalsBlockedFollowsAddsAndRemoves(t *testing.T) {
	a := NewArrivals[string]()
	for i, k := range []string{"x1", "x2", "y", "x3"} {
		a.Add(k, k[:1], 10, 1, time.Duration(i)*time.Second)
	}
	if a.Blocked != 2 || !a.Behind() {
		t.Fatalf("blocked %d", a.Blocked)
	}
	a.Remove([]string{"x2"})
	if a.Blocked != 1 {
		t.Fatalf("after a blocked one left: %d", a.Blocked)
	}
	a.Remove([]string{"x1"})
	if a.Blocked != 0 || a.Behind() || a.Waiting != 20 {
		t.Fatalf("after x1 left: %d %d", a.Blocked, a.Waiting)
	}
	if a.ArrivedAt("x3") != 3*time.Second || a.StoredOf("y") != 10 || a.ArrivedSince(2*time.Second) != 10 {
		t.Fatalf("arrived %v %d %d", a.ArrivedAt("x3"), a.StoredOf("y"), a.ArrivedSince(2*time.Second))
	}
}

func TestInFlightExpectedBackByPhase(t *testing.T) {
	const mib = 1 << 20
	f := NewInFlight()
	f.Add("p", "b", 10*mib)
	if got := f.Expected("p", 3*time.Second, 0); got != 3*time.Second {
		t.Fatalf("unmeasured: %v", got)
	}
	if got := f.Expected("p", 3*time.Second, mib); got != 3*time.Second+10*time.Second+1500*time.Millisecond {
		t.Fatalf("not uploading: %v", got)
	}
	f.Uploading("p", 2*time.Second)
	if got := f.Expected("p", 3*time.Second, mib); got != 2*time.Second+10*time.Second+1500*time.Millisecond {
		t.Fatalf("uploading: %v", got)
	}
	f.Sent("p", 12*time.Second)
	if got := f.Expected("p", 13*time.Second, mib); got != 12*time.Second+1500*time.Millisecond {
		t.Fatalf("sent: %v", got)
	}
	f.Uploading("p", 20*time.Second)
	if got := f.Expected("p", 21*time.Second, mib); got != 20*time.Second+10*time.Second+1500*time.Millisecond {
		t.Fatalf("sent again: %v", got)
	}
}

func TestInFlightCommitUsualIsTheMedianOfTheLastEight(t *testing.T) {
	f := NewInFlight()
	if f.CommitUsual() != CommitUsualStart {
		t.Fatal(f.CommitUsual())
	}
	for i, ms := range []int{400, 500, 9000, 600, 500, 700, 500, 600, 500} {
		k := fmt.Sprint(i)
		f.Add(k, "b", 1)
		f.Sent(k, 100*time.Second)
		f.Remove(k, 100*time.Second+time.Duration(ms)*time.Millisecond, true)
	}
	if got := f.CommitUsual(); got != 550*time.Millisecond { // the last eight: 500 600 500 700 500 600 500 9000
		t.Fatalf("median %v", got)
	}
	f.Add("x", "b", 1)
	f.Sent("x", 0)
	f.Remove("x", 50*time.Second, false) // a failed pack isn't a commit's time
	f.Add("y", "b", 1)
	f.Remove("y", 50*time.Second, true) // nor one never sent
	if f.CommitUsual() != 550*time.Millisecond || f.Len() != 0 {
		t.Fatalf("%v %d", f.CommitUsual(), f.Len())
	}
}

func TestInFlightComingCountsThisBucketsPacksBackInTime(t *testing.T) {
	const mib = 1 << 20
	f := NewInFlight()
	f.Add("a", "b1", 4*mib)
	f.Add("c", "b1", 2*mib)
	f.Add("d", "b2", 8*mib)
	f.Sent("a", 0)      // back at 1.5 s
	f.Uploading("c", 0) // back at 2 + 1.5 s
	s := time.Second
	if got := f.Coming("b1", 2*s, s, mib, 0, nil); got != 4*mib {
		t.Fatalf("by 2 s: %d", got)
	}
	if got := f.Coming("b1", 3500*time.Millisecond, s, mib, 0, nil); got != 6*mib {
		t.Fatalf("by 3.5 s: %d", got)
	}
	if got := f.Coming("b2", 100*s, s, 0, 0, nil); got != 8*mib {
		t.Fatalf("unmeasured: %d", got)
	}
	if got := f.Coming("b3", 100*s, s, mib, 0, nil); got != 0 {
		t.Fatalf("another bucket: %d", got)
	}
}

func TestInFlightACommittedPackCountsForItsGraceLessWhatArrived(t *testing.T) {
	f := NewInFlight()
	f.Add("p", "b", 12_000_000)
	f.Sent("p", 0)
	f.Remove("p", time.Second, true)
	grace := 300 * time.Millisecond
	if got := f.Coming("b", 6*time.Second, time.Second, 1<<20, grace, func(time.Duration) int64 { return 0 }); got != 12_000_000 {
		t.Fatalf("just back: %d", got)
	}
	if got := f.Coming("b", 6*time.Second, 1100*time.Millisecond, 1<<20, grace, func(since time.Duration) int64 {
		if since != time.Second {
			t.Fatalf("since %v", since)
		}
		return 4_000_000
	}); got != 8_000_000 {
		t.Fatalf("partly back: %d", got)
	}
	if got := f.Coming("b", 6*time.Second, 1300*time.Millisecond, 1<<20, grace, nil); got != 0 {
		t.Fatalf("after the grace: %d", got)
	}
}

type schedItem struct {
	key    string
	stored int64
}

func schedPlanOf(l PackLimits) func(a *Arrivals[*schedItem], tailCap int64) Plan[*schedItem] {
	return func(a *Arrivals[*schedItem], tailCap int64) Plan[*schedItem] {
		return PlanPackCapped(a.Items(), func(i *schedItem) int64 { return i.stored }, func(i *schedItem) string { return i.key },
			func(*schedItem) int64 { return 10 }, l, tailCap >= 0, tailCap)
	}
}

func addSched(a *Arrivals[*schedItem], key string, stored int64, at time.Duration) {
	a.Add(&schedItem{key: key, stored: stored}, key, stored, 10, at)
}

func noComing(string, time.Duration) int64 { return 0 }

func TestScheduleHoldsAnEarlyLeftoverWhileAnotherBucketHasAPack(t *testing.T) {
	l := DefaultLimits
	small, full := NewArrivals[*schedItem](), NewArrivals[*schedItem]()
	addSched(small, "s", 1<<20, 0) // older
	for i := 0; i < 12; i++ {
		addSched(full, fmt.Sprint("f", i), 1<<20, 400*time.Millisecond)
	}
	q := map[string]*Arrivals[*schedItem]{"small": small, "full": full}
	go1, next, ok := Schedule(q, 500*time.Millisecond, l, schedPlanOf(l), noComing, false, 0)
	if len(go1) != 0 || !ok || next != 400*time.Millisecond+l.Settle {
		t.Fatalf("settling: %d %v %v", len(go1), next, ok)
	}
	go2, _, _ := Schedule(q, 610*time.Millisecond, l, schedPlanOf(l), noComing, false, 0)
	if len(go2) != 1 || go2[0].Bucket != "full" {
		t.Fatalf("settled: %+v", go2)
	}
	alone := map[string]*Arrivals[*schedItem]{"small": small}
	if g, _, _ := Schedule(alone, 500*time.Millisecond, l, schedPlanOf(l), noComing, false, 0); len(g) != 1 ||
		g[0].Decision.Reason != ReasonLeftover {
		t.Fatalf("alone: %+v", g)
	}
	if g, next, _ := Schedule(alone, 500*time.Millisecond, l, schedPlanOf(l), noComing, true, 0); len(g) != 0 ||
		next != 5*time.Second {
		t.Fatalf("a run waiting: %+v %v", g, next)
	}
	g, _, _ := Schedule(q, 5*time.Second, l, schedPlanOf(l), noComing, true, 0)
	if len(g) != 2 || g[0].Bucket != "small" || g[1].Bucket != "full" {
		t.Fatalf("at the limit: %+v", g)
	}
}

func TestSchedulePassesComingByTheOldestFilesTimeLimit(t *testing.T) {
	a := NewArrivals[*schedItem]()
	addSched(a, "x", 1<<20, 2*time.Second)
	var asked []time.Duration
	coming := func(b string, by time.Duration) int64 {
		asked = append(asked, by)
		return 9 << 20
	}
	g, next, _ := Schedule(map[string]*Arrivals[*schedItem]{"b": a}, 2500*time.Millisecond, DefaultLimits,
		schedPlanOf(DefaultLimits), coming, false, 0)
	if len(g) != 0 || next != 7*time.Second || !reflect.DeepEqual(asked, []time.Duration{7 * time.Second}) {
		t.Fatalf("%+v %v %v", g, next, asked)
	}
}

func fullWithOneOver(at time.Duration) *Arrivals[*schedItem] {
	a := NewArrivals[*schedItem]()
	for i := 0; i < 53; i++ { // 52 files of 400 KB fill the ceiling (20.8 MB); a 53rd is turned away
		addSched(a, fmt.Sprint("r", i), 400_000, at+time.Duration(i)*time.Millisecond)
	}
	return a
}

func TestScheduleAFullPackTakesItsRemnantWhenItGoes(t *testing.T) {
	l := DefaultLimits
	q := map[string]*Arrivals[*schedItem]{"b": fullWithOneOver(0)}
	g, _, _ := Schedule(q, 100*time.Millisecond, l, schedPlanOf(l), noComing, false, AbsorbCap(l, -1))
	if len(g) != 1 || len(g[0].Plan.Pack) != 53 || g[0].Plan.State != PackReady || len(g[0].Plan.Rest) != 0 {
		t.Fatalf("taken: %+v", g)
	}
	for _, c := range []int64{0, 52*400_000 + 399_999} {
		g, _, _ := Schedule(q, 100*time.Millisecond, l, schedPlanOf(l), noComing, false, c)
		if len(g) != 1 || len(g[0].Plan.Pack) != 52 || g[0].Plan.State != PackFull {
			t.Fatalf("cap %d: %+v", c, g)
		}
	}
}

func TestScheduleARemnantWithSomethingComingIsLeftForIt(t *testing.T) {
	l := DefaultLimits
	q := map[string]*Arrivals[*schedItem]{"b": fullWithOneOver(0)}
	coming := func(string, time.Duration) int64 { return 10 << 20 }
	if g, _, _ := Schedule(q, 100*time.Millisecond, l, schedPlanOf(l), coming, false, 30<<20); len(g) != 1 ||
		len(g[0].Plan.Pack) != 52 {
		t.Fatalf("coming: %+v", g)
	}
	// A stream: the turned-away files arrived steadily after the oldest of them.
	b := NewArrivals[*schedItem]()
	for i := 0; i < 52; i++ {
		addSched(b, fmt.Sprint("s", i), 400_000, 0)
	}
	for i := 0; i < 20; i++ {
		addSched(b, fmt.Sprint("t", i), 400_000, time.Duration(i)*50*time.Millisecond)
	}
	if g, _, _ := Schedule(map[string]*Arrivals[*schedItem]{"b": b}, time.Second, l, schedPlanOf(l), noComing, false,
		30<<20); len(g) != 1 || len(g[0].Plan.Pack) != 52 || g[0].Plan.State != PackFull {
		t.Fatalf("stream: %+v", g)
	}
}
