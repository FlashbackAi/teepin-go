// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// The engine's rules (engine.go), the same as the Python SDK's _engine.py and the Node SDK's engine.ts: packs, part
// sizes, the adaptive limit, the retry budget, waits, and what a storage network's answer means. A port of
// engine.test.mjs.

const mib = 1 << 20

type item struct {
	key  string
	size int64
}

func plan(items []item, target int64, maxObjects int) [][]string {
	var out [][]string
	for _, p := range PlanPacks(items, func(i item) int64 { return i.size }, func(i item) string { return i.key }, target, maxObjects) {
		var keys []string
		for _, i := range p {
			keys = append(keys, fmt.Sprintf("%s%d", i.key, i.size))
		}
		out = append(out, keys)
	}
	return out
}

func eqPlans(a, b [][]string) bool {
	return slices.EqualFunc(a, b, func(x, y []string) bool { return slices.Equal(x, y) })
}

func TestPlanPacks_ClosesAPackAtTheTargetSizeAndKeepsTheOrder(t *testing.T) {
	each := tws1.NeedleHeaderSize + tws1.StoredSize(1000)
	items := []item{{"a", 1000}, {"b", 1000}, {"c", 1000}, {"d", 1000}, {"e", 1000}}
	if got := plan(items, each*2, PackMaxObjects); !eqPlans(got, [][]string{{"a1000", "b1000"}, {"c1000", "d1000"}, {"e1000"}}) {
		t.Fatal(got)
	}
}

func TestPlanPacks_ClosesAPackAtTheMostObjects(t *testing.T) {
	if got := plan([]item{{"a", 1}, {"b", 1}, {"c", 1}}, PackTargetBytes, 2); !eqPlans(got, [][]string{{"a1", "b1"}, {"c1"}}) {
		t.Fatal(got)
	}
}

func TestPlanPacks_GivesAnItemOverTheTargetAPackOfItsOwn(t *testing.T) {
	if got := plan([]item{{"big", 20 * mib}, {"s", 10}}, 9*mib, PackMaxObjects); !eqPlans(got, [][]string{{"big20971520"}, {"s10"}}) {
		t.Fatal(got)
	}
}

func TestPlanPacks_NeverPutsOneKeyInAPackTwice_AndKeepsWritesToOneKeyInOrder(t *testing.T) {
	items := []item{{"k", 10}, {"x", 10}, {"k", 20}, {"y", 10}, {"k", 30}}
	if got := plan(items, PackTargetBytes, PackMaxObjects); !eqPlans(got, [][]string{{"k10", "x10", "y10"}, {"k20"}, {"k30"}}) {
		t.Fatal(got)
	}
	// A write to a key that doesn't fit holds back a later write to the same key that would: the later one never
	// lands first.
	held := []item{{"a", 10}, {"k", 20 * mib}, {"k", 10}}
	if got := plan(held, 9*mib, PackMaxObjects); !eqPlans(got, [][]string{{"a10"}, {"k20971520"}, {"k10"}}) {
		t.Fatal(got)
	}
}

func TestPlanPacks_PlansNothingForNothing(t *testing.T) {
	if got := PlanPacks([]item{}, func(item) int64 { return 0 }, func(item) string { return "" }, PackTargetBytes, PackMaxObjects); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestPackedSize_IsTheNeedlesAndTWS1Bytes(t *testing.T) {
	if got, want := PackedSize([]int64{0, 70_000}), 2*tws1.NeedleHeaderSize+tws1.StoredSize(0)+tws1.StoredSize(70_000); got != want {
		t.Fatalf("%d, want %d", got, want)
	}
}

func plainPartSize(t *testing.T, size, want int64) int64 {
	t.Helper()
	p, err := DirectPartSize(size, want, DefaultPartRules, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPartSizes_Default16MiB_AtLeast5MiB_InWholeSegments(t *testing.T) {
	if p := plainPartSize(t, 100*mib, 0); p != DefaultPartBytes {
		t.Fatal(p)
	}
	if p := plainPartSize(t, 100*mib, 1); p != MinPartBytes {
		t.Fatal(p)
	}
	if p := plainPartSize(t, 100*mib, 6*mib+1); p != 6*mib+tws1.SegmentSize {
		t.Fatal(p)
	}
}

func TestPartSizes_DoubleUntilThePartsFitIn3000(t *testing.T) {
	if p := plainPartSize(t, 3001*DefaultPartBytes, 0); p != 2*DefaultPartBytes {
		t.Fatal(p)
	}
}

func TestPartSizes_RefuseAnObjectWhosePartsWouldPass5GiBAsStored(t *testing.T) {
	if _, err := DirectPartSize(3000*6<<30, 0, DefaultPartRules, nil, ""); err == nil {
		t.Fatal("no error")
	}
}

func TestPartSizes_AStreamGets64MiBUnlessToldItsSizeOrAPartSize(t *testing.T) {
	for _, c := range []struct{ expected, want, got int64 }{{0, 0, StreamPartBytes}, {100 * mib, 0, DefaultPartBytes}, {0, 8 * mib, 8 * mib}} {
		if p, err := StreamPartSize(c.expected, c.want, DefaultPartRules, nil, ""); err != nil || p != c.got {
			t.Errorf("StreamPartSize(%d, %d) = %d, %v; want %d", c.expected, c.want, p, err, c.got)
		}
	}
}

func TestAdaptiveLimit_StartsAddsHalvesAndKeepsToItsCeiling(t *testing.T) {
	l := NewAdaptiveLimit(4, 6)
	for range 4 {
		if !l.TryAcquire() {
			t.Fatal("no slot")
		}
	}
	if l.TryAcquire() {
		t.Fatal("a fifth slot")
	}
	for range 4 {
		l.Release(OutcomeOK)
	}
	if lim := l.Limit(); lim < 4.9 || lim > 5.1 {
		t.Fatalf("limit %v", lim)
	}
	for range 100 {
		l.TryAcquire()
		l.Release(OutcomeOK)
	}
	if l.Limit() != 6 {
		t.Fatalf("limit %v", l.Limit())
	}
	l.TryAcquire()
	l.Release(OutcomeThrottled)
	if l.Limit() != 3 {
		t.Fatalf("limit %v", l.Limit())
	}
	l.TryAcquire()
	l.Release(OutcomeFailed)
	if l.Limit() != 3 {
		t.Fatalf("limit %v", l.Limit())
	}
	for range 10 {
		l.TryAcquire()
		l.Release(OutcomeThrottled)
	}
	if l.Limit() != 1 {
		t.Fatalf("limit %v", l.Limit())
	}
}

func TestAdaptiveLimit_WakesAWaitingAcquireWhenASlotIsReleased(t *testing.T) {
	l := NewAdaptiveLimit(1, 1)
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got atomic.Bool
	done := make(chan struct{})
	go func() {
		_ = l.Acquire(context.Background())
		got.Store(true)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	if got.Load() {
		t.Fatal("took a slot that wasn't free")
	}
	l.Release(OutcomeOK)
	<-done
	if l.InFlight() != 1 {
		t.Fatalf("%d in flight", l.InFlight())
	}
}

func TestAdaptiveLimit_HoldsARunsAcquireWhilePutObjectCallsWait_ButNotTheirTryAcquire(t *testing.T) {
	l := NewAdaptiveLimit(1, 1)
	_ = l.Acquire(context.Background())
	l.SetPriority(true)
	var run atomic.Bool
	done := make(chan struct{})
	go func() {
		_ = l.Acquire(context.Background())
		run.Store(true)
		close(done)
	}()
	l.Release(OutcomeOK) // a slot comes free while PutObject calls wait
	time.Sleep(20 * time.Millisecond)
	if run.Load() {
		t.Fatal("the run's next pack didn't wait")
	}
	if !l.TryAcquire() {
		t.Fatal("PutObject didn't get the slot")
	}
	l.SetPriority(false)
	l.Release(OutcomeOK)
	<-done
}

func TestAdaptiveLimit_ACancelledWaitTakesNoSlot(t *testing.T) {
	l := NewAdaptiveLimit(1, 1)
	_ = l.Acquire(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- l.Acquire(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	l.Release(OutcomeOK)
	if l.InFlight() != 0 {
		t.Fatal("the cancelled wait took a slot")
	}
}

func TestRetryBudget_AllowsABurstThenAboutOneRetryInTen(t *testing.T) {
	b := NewRetryBudget(0.1, 3)
	if !b.TryRetry() || !b.TryRetry() || !b.TryRetry() || b.TryRetry() {
		t.Fatal("the burst")
	}
	for range 9 {
		b.OnRequest()
	}
	if b.TryRetry() {
		t.Fatal("a retry before ten requests")
	}
	// Ten steps of 0.1 come to just under 1 in floating point, as in the Python SDK: the eleventh request earns it.
	b.OnRequest()
	b.OnRequest()
	if !b.TryRetry() || b.TryRetry() {
		t.Fatal("one retry after eleven requests")
	}
}

func TestWaits_StartAt5sDoubleTo5MinutesWithJitter_AndFollowRetryAfter(t *testing.T) {
	for range 50 {
		if d := WaitDelay(0, 0); d < 3750*time.Millisecond || d > 5*time.Second {
			t.Fatal(d)
		}
		if d := WaitDelay(9, 0); d < 225*time.Second || d > 300*time.Second {
			t.Fatal(d)
		}
	}
	if d := WaitDelay(3, 7*time.Second); d != 7*time.Second {
		t.Fatal(d)
	}
	if d := WaitDelay(3, 15*time.Minute); d != 5*time.Minute {
		t.Fatal(d)
	}
	if d := WaitDelay(math.MaxInt32, 0); d < 225*time.Second || d > 300*time.Second {
		t.Fatal(d)
	}
}

func apiErr(status int, code, message string) error {
	return &errs.APIError{StatusCode: status, Code: code, Message: message}
}

func TestStorageOutcomes_EachAnswerAsTheEngineActsOnIt(t *testing.T) {
	cases := []struct {
		err  error
		want StorageOutcome
	}{
		{nil, Sent},
		{apiErr(412, "", "x"), Sent},
		{apiErr(401, "", "x"), Resign},
		{apiErr(403, "", "x"), Resign},
		{apiErr(400, "BadDigest", "x"), Bad},
		{apiErr(400, "", "BadDigest: the Content-MD5 did not match"), Bad},
		{apiErr(400, "InvalidArgument", "x"), Refused},
		{apiErr(429, "", "x"), Busy},
		{apiErr(503, "", "x"), Busy},
		{apiErr(404, "", "x"), Refused},
		{&errs.ConnError{Err: errors.New("reset")}, Unknown},
		{&errs.ConnError{Err: errors.New("refused"), Unsent: true}, Unsent},
		{errors.New("a bug"), Refused},
	}
	for _, c := range cases {
		if got := OutcomeOf(c.err); got != c.want {
			t.Errorf("OutcomeOf(%v) = %s, want %s", c.err, got, c.want)
		}
	}
}

func TestStorageOutcomes_WaitOutTheNetworkAndABusyControlPlane_NothingElse(t *testing.T) {
	if !IsPassing(&errs.ConnError{Err: errors.New("reset")}) || !IsPassing(apiErr(502, "", "")) || !IsPassing(apiErr(429, "", "")) {
		t.Fatal("passing")
	}
	if IsPassing(apiErr(409, "", "")) || IsPassing(errors.New("bug")) {
		t.Fatal("not passing")
	}
	if !IsThrottle(apiErr(503, "", "")) || !IsThrottle(apiErr(429, "", "")) || IsThrottle(apiErr(500, "", "")) {
		t.Fatal("throttle")
	}
}

func TestUnsupported_NamesTheOldPathAndIsNeverSilent(t *testing.T) {
	for _, err := range []error{apiErr(501, "direct_upload_unsupported", ""), apiErr(404, "", ""), apiErr(404, "not_found", "")} {
		if !IsUnsupported(err) {
			t.Errorf("%v is unsupported", err)
		}
	}
	for _, err := range []error{apiErr(404, "bucket_not_found", ""), apiErr(400, "", ""), errors.New("x")} {
		if IsUnsupported(err) {
			t.Errorf("%v is not unsupported", err)
		}
	}
	var ae *errs.APIError
	err := Unsupported(&errs.APIError{StatusCode: 501, RequestID: "req-1"})
	if !errors.As(err, &ae) || ae.StatusCode != 501 || ae.Code != "direct_upload_unsupported" || ae.RequestID != "req-1" ||
		!strings.Contains(ae.Message, "direct storage on") {
		t.Fatalf("%+v", ae)
	}
}

func TestUUID7_SortsByTimeAndHasItsVersionAndVariant(t *testing.T) {
	a := UUID7()
	time.Sleep(2 * time.Millisecond)
	b := UUID7()
	if len(a) != 36 || a[14] != '7' || !strings.ContainsAny(a[19:20], "89ab") || a >= b {
		t.Fatalf("%s then %s", a, b)
	}
	if _, err := ObjectIDBytes(a); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "x", "00000000-0000-0000-0000-00000000000g"} {
		if _, err := ObjectIDBytes(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
	if got, err := ObjectIDBytes("urn:uuid:{00000000-0000-0000-0000-0000000000ff}"); err != nil || got[15] != 0xff {
		t.Fatalf("%x %v", got, err)
	}
}

func TestRunPool_RunsEveryItemAtMostNAtOnce(t *testing.T) {
	var inFlight, most atomic.Int32
	var mu sync.Mutex
	var seen []int
	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	err := RunPool(context.Background(), 3, items, func(ctx context.Context, i int) error {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
		}
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		seen = append(seen, i)
		mu.Unlock()
		return nil
	})
	slices.Sort(seen)
	if err != nil || !slices.Equal(seen, items) || most.Load() > 3 || most.Load() < 2 {
		t.Fatalf("%v %v, at most %d at once", err, seen, most.Load())
	}
}

func TestRunPool_AFailureStopsTheOthersAtOnceAndWaitsForThem(t *testing.T) {
	boom := errors.New("boom")
	var stopped, ran atomic.Int32
	t0 := time.Now()
	err := RunPool(context.Background(), 4, []int{0, 1, 2, 3, 4, 5, 6, 7}, func(ctx context.Context, i int) error {
		ran.Add(1)
		if i == 0 {
			time.Sleep(10 * time.Millisecond)
			return boom
		}
		<-ctx.Done() // held until stopped
		stopped.Add(1)
		return ctx.Err()
	})
	if !errors.Is(err, boom) || time.Since(t0) > time.Second {
		t.Fatalf("%v after %s", err, time.Since(t0))
	}
	if stopped.Load() != 3 || ran.Load() != 4 {
		t.Fatalf("%d stopped of %d run", stopped.Load(), ran.Load())
	}
}

func TestRunPool_ACancelReturnsTheContextsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := RunPool(ctx, 2, []int{1, 2, 3}, func(ctx context.Context, i int) error {
		cancel()
		<-ctx.Done()
		return errors.New("something the cancel caused")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSlotsAndWaitCapped(t *testing.T) {
	s := NewSlots(1)
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	s.Release()
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	if WaitCapped(&wg, 10*time.Millisecond) {
		t.Fatal("ended in time")
	}
	wg.Done()
	if !WaitCapped(&wg, time.Second) {
		t.Fatal("didn't end")
	}
}

func TestProgress_AddsUpAndReportsOneCallAtATime(t *testing.T) {
	var last, total int64
	p := NewProgress(100, func(d, t int64) { last, total = d, t })
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { p.Add(10) })
	}
	wg.Wait()
	p.Add(0)
	if last != 100 || total != 100 {
		t.Fatalf("%d of %d", last, total)
	}
	var none *Progress
	none.Add(5) // a nil progress reports nothing
}
