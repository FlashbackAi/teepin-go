// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// Slow links (section 22.7): the speed meter, and the pack and part rules it drives. The Python SDK's
// test_direct_slow.py and the slow-link tests of the Node SDK's engine.test.mjs.

const kib = 1 << 10

// at is a link of bytesPerSecond with the default budget.
func at(bytesPerSecond float64) *LinkSpeed {
	return &LinkSpeed{BytesPerSecond: bytesPerSecond, Budget: RequestBudget}
}

func speedOf(t *testing.T, m *SpeedMeter) float64 {
	t.Helper()
	bps, ok := m.BytesPerSecond()
	if !ok {
		t.Fatal("nothing measured")
	}
	return bps
}

func TestSpeedMeter_MeasuresAMovingEstimateOfCompletedPUTsOnly(t *testing.T) {
	m := NewSpeedMeter()
	if _, ok := m.BytesPerSecond(); ok || m.Link() != nil {
		t.Fatal("measured before any PUT: the default sizes must hold")
	}
	m.Observe(SpeedSampleBytes-1, 10*time.Second) // too small to tell the link from the round trip
	m.Observe(4*mib, 0)                           // no time
	if _, ok := m.BytesPerSecond(); ok {
		t.Fatal("a tiny or zero-time PUT was measured")
	}
	m.Observe(SpeedSampleBytes, 4*time.Second) // the smallest measured: the first sample sets it
	if got := speedOf(t, m); got != SpeedSampleBytes/4 {
		t.Fatalf("%v", got)
	}
	m = NewSpeedMeter()
	m.Observe(2*mib, 4*time.Second)
	if got := speedOf(t, m); got != 512*kib {
		t.Fatalf("%v", got)
	}
	// Faster: the estimate moves toward it, not all the way: averages of bytes and of time, weighted 0.3.
	m.Observe(2*mib, time.Second)
	want := (2.0 * mib) / (4 + 0.3*(1-4))
	if got := speedOf(t, m); got != want {
		t.Fatalf("%v, want %v", got, want)
	}
	// Bytes and time are averaged apart: a long transfer counts for more than a short one.
	m.Observe(8*mib, 2*time.Second)
	b, s := 2.0*mib+0.3*(8*mib-2*mib), (4+0.3*(1-4))+0.3*(2-(4+0.3*(1-4)))
	if got := speedOf(t, m); got != b/s {
		t.Fatalf("%v, want %v", got, b/s)
	}
	if l := m.Link(); l == nil || l.Budget != RequestBudget || l.RequestBytes() != int64(b/s*240) {
		t.Fatalf("%+v", l)
	}
	m.Set(1000)
	if got := speedOf(t, m); got != 1000 || m.Link().RequestBytes() != 240_000 {
		t.Fatalf("%v", got)
	}
	m.Set(0)
	if got := speedOf(t, m); got != b/s {
		t.Fatalf("Set(0) didn't go back to measuring: %v", got)
	}
	// Once measured, a zero-time or tiny PUT still moves nothing.
	m.Observe(4*mib, 0)
	m.Observe(SpeedSampleBytes-1, time.Hour)
	m.ObserveTimeout(4*mib, 0)
	if got := speedOf(t, m); got != b/s {
		t.Fatalf("a zero-time or tiny sample moved it: %v", got)
	}
}

func TestSpeedMeter_TakesATimeoutAsAnUpperBoundThatOnlyLowersIt(t *testing.T) {
	cold := NewSpeedMeter()
	cold.ObserveTimeout(SpeedSampleBytes-1, 30*time.Second) // too small to say anything about the link
	if _, ok := cold.BytesPerSecond(); ok {
		t.Fatal("a tiny timeout was measured")
	}
	cold.ObserveTimeout(3*mib, 30*time.Second) // a cold start: at most 3 MiB in 30 s
	if got := speedOf(t, cold); got != 3.0*mib/30 {
		t.Fatalf("%v", got)
	}

	fast := NewSpeedMeter()
	fast.Observe(mib, time.Second) // 1 MiB/s, then a 1 MiB PUT with no answer in 30 s
	fast.ObserveTimeout(mib, 30*time.Second)
	if got := speedOf(t, fast); got >= 200*kib {
		t.Fatalf("%v", got)
	}

	slow := NewSpeedMeter()
	slow.Observe(mib, 100*time.Second) // about 10 KiB/s; a timeout that bounds it at 100 KiB/s never raises it
	before := speedOf(t, slow)
	slow.ObserveTimeout(mib, 10*time.Second)
	if got := speedOf(t, slow); got > before {
		t.Fatalf("%v > %v", got, before)
	}
}

func TestPackTarget_ShrinksToWhatTheSpeedCarriesInFourMinutesToAFloor(t *testing.T) {
	for _, c := range []struct {
		link *LinkSpeed
		want int64
	}{
		{nil, 9 * mib},                       // nothing measured: the default
		{at(100 * mib), 9 * mib},             // a normal speed keeps the default
		{at(20 * kib), 20 * kib * 240},       // 4,800 KiB
		{at(1), PackMinTarget},               // never below one segment
		{at(64*kib/240.0 + 1), 64*kib + 240}, // just over the floor
	} {
		if got := PackTargetAt(c.link, PackTargetBytes, PackMinTarget); got != c.want {
			t.Errorf("%+v: %d, want %d", c.link, got, c.want)
		}
	}
	if PackMinTarget != tws1.SegmentSize {
		t.Fatal(PackMinTarget)
	}
}

func TestNextPack_StillPacksEveryFileAndSmallFilesTogetherHoweverSlow(t *testing.T) {
	items := []item{{"a", 100 * kib}, {"b", 1000}, {"c", 1000}}
	target := PackTargetAt(at(1), PackTargetBytes, PackMinTarget)
	if got := plan(items, target, PackMaxObjects); !eqPlans(got, [][]string{{"a102400"}, {"b1000", "c1000"}}) {
		t.Fatalf("%v", got)
	}
	first, rest := NextPack(items, func(i item) int64 { return i.size }, func(i item) string { return i.key }, target, PackMaxObjects)
	if len(first) != 1 || first[0].key != "a" || len(rest) != 2 {
		t.Fatalf("%v %v", first, rest)
	}
}

func TestPackObjectLimit_SendsWhatAPackRequestCantCarryInPartsNeverUnderTheSmallestPart(t *testing.T) {
	for _, c := range []struct {
		link *LinkSpeed
		want int64
	}{
		{nil, 16 * mib},
		{at(100 * mib), 16 * mib},
		{at(6 * mib / 240.0), 6 * mib},
		{at(1), MinPartBytes},
	} {
		if got := PackObjectLimitAt(c.link, PackObjectLimit, MinPartBytes); got != c.want {
			t.Errorf("%+v: %d, want %d", c.link, got, c.want)
		}
	}
}

func partAt(t *testing.T, size, want int64, link *LinkSpeed) int64 {
	t.Helper()
	p, err := DirectPartSize(size, want, DefaultPartRules, link, "k")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPartSizes_KeepTheDefaultsAtANormalSpeed(t *testing.T) {
	fast := at(100 * mib)
	if p := partAt(t, 1<<30, 0, fast); p != 16*mib {
		t.Fatal(p)
	}
	if p := partAt(t, 100<<30, 0, fast); p != 64*mib { // doubled to fit 3,000 parts, as with nothing measured
		t.Fatal(p)
	}
	if p, err := StreamPartSize(0, 0, DefaultPartRules, fast, "s"); err != nil || p != 64*mib {
		t.Fatal(p, err)
	}
}

func TestPartSizes_ShrinkToWholeSegmentsUnderTheCap(t *testing.T) {
	speed := at(30_000) // 7,200,000 bytes in 4 minutes
	p := partAt(t, 1<<30, 0, speed)
	if p != 7_200_000/tws1.SegmentSize*tws1.SegmentSize || p%tws1.SegmentSize != 0 || p > 7_200_000 {
		t.Fatal(p)
	}
	if got := partAt(t, 1<<30, 64*mib, speed); got != p { // a part size asked for is capped
		t.Fatal(got)
	}
	if got := partAt(t, 1<<30, 6*mib, speed); got != 6*mib { // and kept when under the cap
		t.Fatal(got)
	}
	if got := (LinkSpeed{BytesPerSecond: 30_000, Budget: RequestBudget}).PartCap(); got != p {
		t.Fatal(got)
	}
}

func TestPartSizes_TakeTheSmallestSizeUnderTheCapWhenPartsMustGrow(t *testing.T) {
	speed := at(30 * mib / 240.0) // a 30 MiB cap
	if p := partAt(t, 3000*24*mib, 0, nil); p != 32*mib {
		t.Fatal(p) // doubled, with nothing measured
	}
	if p := partAt(t, 3000*24*mib, 0, speed); p != 24*mib {
		t.Fatal(p) // doubling would pass the cap: the smallest whole-segment size that fits
	}
	if p := partAt(t, 3000*24*mib+1, 0, speed); p != 24*mib+tws1.SegmentSize {
		t.Fatal(p)
	}
	if p := partAt(t, 3000*20*mib, 0, speed); p != 20*mib { // 16 MiB doubles to 32 MiB, over the cap
		t.Fatal(p)
	}
	_, err := DirectPartSize(3000*31*mib, 0, DefaultPartRules, speed, "huge.bin")
	if !errors.Is(err, errs.ErrConnectionSpeed) || !strings.Contains(err.Error(), "parts can be at most 30 MiB") ||
		!strings.Contains(err.Error(), "more than 3,000 of them") || !strings.Contains(err.Error(), "huge.bin") {
		t.Fatalf("%v", err)
	}
	// Parts that fit under the cap without doubling stay as they are.
	if p := partAt(t, 3000*16*mib, 0, speed); p != 16*mib {
		t.Fatal(p)
	}
}

func TestPartSizes_RefuseASpeedTooSlowForTheSmallestPart(t *testing.T) {
	_, err := DirectPartSize(1<<30, 0, DefaultPartRules, at(20*kib), "big.iso")
	var ae *errs.APIError
	if !errors.As(err, &ae) || ae.StatusCode != http.StatusRequestEntityTooLarge || ae.Code != "connection_speed" ||
		!errors.Is(err, errs.ErrConnectionSpeed) {
		t.Fatalf("%v", err)
	}
	const want = "big.iso: the connection to storage measured 20.0 KiB/s; at that speed even a 5 MiB part would take " +
		"over 4 minutes to send, so 1073741824 bytes is too large to send over this connection"
	if ae.Message != want {
		t.Fatalf("%q\nwant %q", ae.Message, want)
	}
	// A cap just over 5 MiB (rounded down to it) is enough.
	if p := partAt(t, 1<<30, 0, at((5*mib+tws1.SegmentSize/2)/240.0)); p != 5*mib {
		t.Fatal(p)
	}
	// A cap one segment under 5 MiB isn't.
	if _, err := DirectPartSize(1<<30, 0, DefaultPartRules, at((5*mib-1)/240.0), "k"); !errors.Is(err, errs.ErrConnectionSpeed) {
		t.Fatalf("%v", err)
	}
	_, err = StreamPartSize(0, 0, DefaultPartRules, at(20*kib), "s")
	if !errors.Is(err, errs.ErrConnectionSpeed) || !strings.Contains(err.Error(), "so this stream is too large") {
		t.Fatalf("%v", err)
	}
	// Another error is never this one.
	if errors.Is(&errs.APIError{StatusCode: 413, Code: "object_too_large"}, errs.ErrConnectionSpeed) {
		t.Fatal("object_too_large matched")
	}
}

func TestPartSizes_AStreamFollowsTheSameRule(t *testing.T) {
	if p, err := StreamPartSize(0, 0, DefaultPartRules, nil, "s"); err != nil || p != 64*mib {
		t.Fatal(p, err)
	}
	speed := at(40 * kib) // 9,600 KiB in 4 minutes
	for _, c := range []struct{ expected, want int64 }{{0, 0}, {1 << 30, 0}, {0, 32 * mib}, {1 << 30, 32 * mib}} {
		if p, err := StreamPartSize(c.expected, c.want, DefaultPartRules, speed, "s"); err != nil || p != 9600*kib {
			t.Errorf("%+v: %d, %v", c, p, err)
		}
	}
	_, err := StreamPartSize(3001*9600*kib, 0, DefaultPartRules, speed, "s")
	if !errors.Is(err, errs.ErrConnectionSpeed) || !strings.Contains(err.Error(), "parts can be at most 9.375 MiB") {
		t.Fatalf("%v", err)
	}
}

func TestPutTimeout_GivesASlowLinksPUTTimeToFinishAtItsSpeed(t *testing.T) {
	for _, c := range []struct {
		n    int64
		link *LinkSpeed
		want time.Duration
	}{
		{1000, nil, 30 * time.Second},                      // the floor
		{4 * mib, nil, 64 * time.Second},                   // a second per 64 KiB
		{4 * mib, at(100 * mib), 64 * time.Second},         // a fast link changes nothing
		{60 * kib, at(kib), 120 * time.Second},             // 60 s at 1 KiB/s, twice
		{4 * mib, at(32 * kib), 256 * time.Second},         // twice 128 s
		{SpeedSampleBytes, at(64 * kib), 30 * time.Second}, // twice 4 s: still the floor
	} {
		if got := PutTimeout(c.n, PutTimeoutFloor, PutTimeoutSpeed, c.link); got != c.want {
			t.Errorf("%d at %+v: %s, want %s", c.n, c.link, got, c.want)
		}
	}
}

func TestFormat_SpeedsSizesAndCounts(t *testing.T) {
	if FormatSpeed(20*kib) != "20.0 KiB/s" || FormatSpeed(1.5*mib) != "1.5 MiB/s" {
		t.Fatal(FormatSpeed(20*kib), FormatSpeed(1.5*mib))
	}
	if mibString(5*mib) != "5 MiB" || mibString(9600*kib) != "9.375 MiB" {
		t.Fatal(mibString(9600 * kib))
	}
	if thousands(3000) != "3,000" || thousands(4) != "4" || thousands(1234567) != "1,234,567" {
		t.Fatal(thousands(1234567))
	}
	err := TooSlow("", 0, LinkSpeed{BytesPerSecond: 100, Budget: time.Second}, 0, DefaultPartRules)
	if !strings.Contains(err.Error(), "the upload: ") || !strings.Contains(err.Error(), "over 0.02 minutes") {
		t.Fatalf("%v", err)
	}
}
