// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
)

// Section 28.25 against the fakes: when a save's pack goes, through the real driver.

// putAtOnce starts one PutObject per file together, and waits for them all.
func putAtOnce(t *testing.T, c *Client, files []srcFile) {
	t.Helper()
	start := make(chan struct{})
	errs := make([]error, len(files))
	var wg sync.WaitGroup
	for i := range files {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = c.Storage.PutObject(ctx, "photos", files[i].key, bytes.NewReader(files[i].data),
				int64(len(files[i].data)), nil)
		}(i)
	}
	close(start)
	if !waitGroupWithin(&wg, 30*time.Second) {
		t.Fatal("a put is still waiting")
	}
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPackTiming_ABurstOfSavesIsObservedBeforeAnyLeftoverGoes(t *testing.T) {
	// 28.25 point 1, dev's round of 11-12: callers starting together. Under the old rule the first few each went alone
	// and took every slot; now none goes before the observe time, by when the others have arrived.
	e, c := newUploads(t)
	files := make([]srcFile, 30)
	for i := range files {
		files[i] = srcFile{key: fmt.Sprintf("burst/%d", i), data: randomData(2000 + i)}
	}
	putAtOnce(t, c, files)
	e.assertSavedOnce(files)
	if n := slotCounts(e.packsWhere(anyPack)); len(n) > 2 || n[0] < 2 {
		t.Fatalf("packs of %v", n)
	}
}

func TestPackTiming_ASavesPackIsFollowedInFlightUntilItCommits(t *testing.T) {
	// 28.25 point 2: the driver tells the packs in flight each phase, and a committed pack goes into its grace time.
	_, c := newUploads(t)
	type event struct {
		phase, id string
		committed bool
	}
	var mu sync.Mutex
	var got []event
	c.Storage.uploads().Hooks.Flight = func(phase, id string, committed bool) {
		mu.Lock()
		got = append(got, event{phase, id, committed})
		mu.Unlock()
	}
	if _, err := c.Storage.PutObject(ctx, "photos", "followed", bytes.NewReader(randomData(1000)), 1000, nil); err != nil {
		t.Fatal(err)
	}
	// The call returns as its item is delivered; the pack leaves just after.
	waitFor(10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) >= 4
	})
	mu.Lock()
	defer mu.Unlock()
	var phases []string
	for _, ev := range got {
		phases = append(phases, ev.phase)
		if ev.id != got[0].id {
			t.Fatalf("two packs: %+v", got)
		}
	}
	if fmt.Sprint(phases) != "[add uploading sent remove]" || !got[len(got)-1].committed {
		t.Fatalf("%+v", got)
	}
}

func TestPackTiming_AFullSavesPackTakesItsRemnant(t *testing.T) {
	// 28.25: six files fill the ceiling and the seventh would be turned away, alone; the pack takes it.
	e, c := newUploads(t)
	one := direct.PackedSize([]int64{1000})
	l := direct.DefaultLimits
	l.Floor, l.Ceiling = 3*one, 6*one
	c.Storage.uploads().Limits = l
	files := make([]srcFile, 7)
	for i := range files {
		files[i] = srcFile{key: fmt.Sprintf("rem/%d", i), data: randomData(1000)}
	}
	putAtOnce(t, c, files)
	e.assertSavedOnce(files)
	ps := e.packsWhere(anyPack)
	if n := slotCounts(ps); fmt.Sprint(n) != "[7]" {
		t.Fatalf("packs of %v", n)
	}
	for _, p := range ps {
		if p.UnderFloor != "" {
			t.Fatalf("a reason at the floor: %q", p.UnderFloor)
		}
	}
}

func TestPackTiming_NoUploadWaitIsSpentWhileABatchForms(t *testing.T) {
	// 0.4.1: a save held while its batch forms (up to the time limit) isn't failed by an upload wait of zero. A file of
	// 100 KB arrives every 10 ms, so the first ones wait well past the observe time for the batch to reach the floor.
	e, _ := newUploads(t)
	c := e.client(Config{UploadWait: -1})
	l := direct.DefaultLimits
	l.Floor, l.Ceiling, l.Wait, l.Observe = 2<<20, 4<<20, 4*time.Second, 50*time.Millisecond
	c.Storage.uploads().Limits = l
	files := make([]srcFile, 60)
	for i := range files {
		files[i] = srcFile{key: fmt.Sprintf("forming/%02d", i), data: randomData(100_000)}
	}
	errs := make([]error, len(files))
	var wg sync.WaitGroup
	for i := range files {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * 10 * time.Millisecond)
			_, errs[i] = c.Storage.PutObject(ctx, "photos", files[i].key, bytes.NewReader(files[i].data),
				int64(len(files[i].data)), nil)
		}(i)
	}
	if !waitGroupWithin(&wg, 30*time.Second) {
		t.Fatal("a put is still waiting")
	}
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	e.assertSavedOnce(files)
	if n := slotCounts(e.packsWhere(anyPack)); slices.Max(n) <= 10 {
		t.Fatalf("no batch formed: packs of %v", n)
	}
}

func TestPackTiming_TheTimingHoldIsTheTimeLimitOrTheObserveTime(t *testing.T) {
	l := direct.DefaultLimits
	if l.TimingHold() != 5*time.Second {
		t.Fatalf("default: %v", l.TimingHold())
	}
	l.Wait, l.Observe = 30*time.Second, 300*time.Millisecond
	if l.TimingHold() != 30*time.Second {
		t.Fatalf("30 s: %v", l.TimingHold())
	}
	l.Wait = 0
	if l.TimingHold() != 300*time.Millisecond {
		t.Fatalf("no time limit: %v", l.TimingHold())
	}
}

func TestPackTiming_ALoneSaveWithNoUploadWaitIsSavedWhileStorageIsUp(t *testing.T) {
	// The observe time doesn't count against the upload wait: with none, a lone save still goes once observed.
	e, _ := newUploads(t)
	c := e.client(Config{UploadWait: -1})
	files := []srcFile{{key: "lone", data: randomData(500)}}
	if _, err := c.Storage.PutObject(ctx, "photos", "lone", bytes.NewReader(files[0].data), 500, nil); err != nil {
		t.Fatal(err)
	}
	e.assertSavedOnce(files)
}
