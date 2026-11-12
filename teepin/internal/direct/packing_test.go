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
}

func secs(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

func (v vecLimits) limits() PackLimits {
	return PackLimits{Floor: v.Floor, Ceiling: v.Ceiling, MaxObjects: v.MaxObjects, MaxPackObjects: v.MaxPackObjects,
		MaxRequest: v.RequestBudget + RequestOverhead, Wait: secs(v.Wait), Window: secs(v.Window), Settle: secs(v.Settle)}
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
		Name     string      `json:"name"`
		Limits   vecLimits   `json:"limits"`
		State    string      `json:"state"`
		Waiting  int64       `json:"waiting"`
		Oldest   float64     `json:"oldest"`
		Last     float64     `json:"last"`
		Arrivals [][]float64 `json:"arrivals"`
		Now      float64     `json:"now"`
		SlotFree bool        `json:"slot_free"`
		Decision struct {
			Kind   string  `json:"kind"`
			Reason *string `json:"reason"`
			At     float64 `json:"at"`
		} `json:"decision"`
	} `json:"decide"`
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
		d := Decide(c.State, c.Waiting, secs(c.Oldest), secs(c.Last), arrivals, secs(c.Now), c.Limits.limits(), c.SlotFree)
		reason := ""
		if c.Decision.Reason != nil {
			reason = *c.Decision.Reason
		}
		if d.Kind != c.Decision.Kind || d.Reason != reason || d.At != secs(c.Decision.At) {
			t.Errorf("%s: got %+v, want %+v (reason %q)", c.Name, d, c.Decision, reason)
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
	if d := Decide(PackUnder, a.Waiting, at, a.Last, a.RecentArrivals(now, time.Second), now, DefaultLimits, true); first != 0 ||
		d.Kind != DecideSend || d.Reason != ReasonLeftover {
		t.Fatalf("first %d, decision %+v", first, d)
	}
	a.Remove([]int{0})
	next, at, _ := a.Oldest()
	if next != 1 || a.Waiting != 1000 || len(a.RecentArrivals(now, time.Second)) != 1 {
		t.Fatalf("after the first left: oldest %d, waiting %d", next, a.Waiting)
	}
	if d := Decide(PackUnder, a.Waiting, at, a.Last, a.RecentArrivals(now, time.Second), now, DefaultLimits, true); d.Kind != DecideSend {
		t.Fatalf("the next: %+v", d)
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
