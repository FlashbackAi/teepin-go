// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/fakes"
)

// Section 28 against the fakes: what each reserve carries (the reason, the client), the server's limits, a run's
// packs, and the files a pack gives back to its planner.

// packsWhere are copies of the fake's packs that pass keep.
func (e *upEnv) packsWhere(keep func(p *fakes.Pack) bool) []fakes.Pack {
	var out []fakes.Pack
	e.f.API.Do(func() {
		for _, p := range e.f.Direct.Packs {
			if keep(p) {
				out = append(out, *p)
			}
		}
	})
	return out
}

func slotCounts(packs []fakes.Pack) []int {
	var n []int
	for _, p := range packs {
		n = append(n, len(p.Slots))
	}
	slices.Sort(n)
	return n
}

func anyPack(*fakes.Pack) bool { return true }

func TestPackSizes_ALoneSaveGoesAtOnceAsALeftoverNamingTheSDK(t *testing.T) {
	e, c := newUploads(t)
	took := time.Now()
	if _, err := c.Storage.PutObject(ctx, "photos", "k", strings.NewReader("bytes"), 5, nil); err != nil {
		t.Fatal(err)
	}
	// Nothing else is arriving, so it can't reach the floor: it goes at once, never waiting out the 5 s limit.
	if d := time.Since(took); d > 3*time.Second {
		t.Fatalf("took %s", d)
	}
	ps := e.packsWhere(anyPack)
	if len(ps) != 1 || ps[0].UnderFloor != "leftover" || ps[0].Client != "teepin-go/"+Version {
		t.Fatalf("%+v", ps)
	}
}

func TestPackSizes_ARunPacksAtTheFloorAndItsTailJoinsItsLastPack(t *testing.T) {
	// 45 files of 1,000,000 bytes: 20 fill the first pack to the ceiling; the 25 left are over the floor, so the
	// second pack takes 20 and its 5 left are a tail it takes too. Two packs, both at or over the floor, no reason.
	e, c := newUploads(t)
	c.Storage.uploads().Sizes.PackObjectLimit = 16 << 20
	files := e.writeFiles(45, 1_000_000, "f")
	res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
	if err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	ps := e.packsWhere(anyPack)
	if got := slotCounts(ps); !slices.Equal(got, []int{20, 25}) {
		t.Fatalf("packs %v", got)
	}
	for _, p := range ps {
		if p.Packed < 10<<20 || p.UnderFloor != "" {
			t.Fatalf("%d bytes, reason %q", p.Packed, p.UnderFloor)
		}
	}
}

func TestPackSizes_TheServersLimitsFromAReservationAreFollowed(t *testing.T) {
	// With a floor of 1 MiB and a ceiling of 2 MiB from the server (28.15), four files of 900,000 bytes go as two
	// packs; with the built-in 10 MiB floor they would be one.
	e, c := newUploads(t)
	e.f.API.Do(func() {
		e.f.Direct.PackLimits = map[string]any{"floor_bytes": 1 << 20, "ceiling_bytes": 2 << 20, "max_objects": 10000,
			"max_pack_objects": 140000, "max_request_bytes": 64 << 20, "wait_ms": 5000, "rate_window_ms": 1000,
			"settle_ms": 200}
	})
	c.Storage.uploads().Sizes.PackObjectLimit = 16 << 20
	if _, err := c.Storage.PutObject(ctx, "photos", "first", strings.NewReader("x"), 1, nil); err != nil {
		t.Fatal(err)
	}
	files := e.writeFiles(4, 900_000, "f")
	if res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil); err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	if got := slotCounts(e.packsWhere(anyPack)); !slices.Equal(got, []int{1, 2, 2}) {
		t.Fatalf("packs %v", got)
	}
}

func TestPackSizes_VerifyingGetsItsFirstFullCycleEvenWithNoUploadWait(t *testing.T) {
	// Verifying is a normal answer, not storage being away (28.18): with no upload wait, a pack whose answer was lost
	// is still asked about for 5 s, then sent again.
	e, _ := newUploads(t)
	c := e.client(Config{UploadWait: -1})
	e.f.Network.Do(func() { e.f.Network.Drop["PUT"] = 1 })
	if _, err := c.Storage.PutObject(ctx, "photos", "k", strings.NewReader("bytes"), 5, nil); err != nil {
		t.Fatal(err)
	}
	if commits, puts := e.f.API.HitsOf("POST pack commit"), e.f.Network.HitsOf("PUT"); commits != 7 || puts != 2 {
		t.Fatalf("%d commits, %d PUTs", commits, puts)
	}
}

func TestPackSizes_ARunsPackUnderTheFloorThatMustStartAgainGivesItsFilesBack(t *testing.T) {
	// 28.5: a renewal under the floor goes back to the planner (the pack ends "returned"), its reservation released
	// (28.23), and its files go in a new pack.
	e, c := newUploads(t)
	e.canned("POST pack commit", refusal(409, "reservation_abandoned"))
	var mu sync.Mutex
	var states []string
	c.Storage.uploads().Hooks.Saved = func(kind, state string) {
		mu.Lock()
		defer mu.Unlock()
		if kind == "pack" {
			states = append(states, state)
		}
	}
	files := e.writeFiles(3, 3000, "f")
	if res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil); err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	e.assertSavedOnce(files)
	mu.Lock()
	defer mu.Unlock()
	if !slices.Contains(states, "returned") {
		t.Fatalf("states %v", states)
	}
	if n := len(e.packsWhere(func(p *fakes.Pack) bool { return p.Released })); n != 1 {
		t.Fatalf("%d released", n)
	}
}

func TestCrash_AResumedUnsignedPackUnderTheFloorJoinsTheQueuedFiles(t *testing.T) {
	// 28.22 point 5: a run of one file a pack, cut once a pack was built, resumes with its unsigned packs under the
	// floor and its queued files. Each such pack asks reserve with its own idempotency key first (nothing was
	// committed), releases what it reserved, and its file joins the others: every file not yet sent goes in one pack.
	e, _ := newUploads(t)
	files := e.writeFiles(3, 3000, "f")
	limits := oneFileAPack
	if code, out := e.child(childConfig{Files: toUpload(files), Pack: "built", Concurrency: 1, Sizes: testSizes,
		Limits: &limits}); code != crashed {
		t.Fatalf("the child didn't crash: %d %s", code, out)
	}
	before := map[string]bool{}
	sentBefore := map[string]bool{}
	for _, p := range e.packsWhere(func(p *fakes.Pack) bool { return p.Status == "sealed" }) {
		before[p.ID] = true
		for _, s := range p.Slots {
			sentBefore[s.Key] = true
		}
	}
	res, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
	if err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	e.assertSavedOnce(files)
	fresh := e.packsWhere(func(p *fakes.Pack) bool { return p.Status == "sealed" && !before[p.ID] })
	if len(fresh) != 1 || len(fresh[0].Slots) < 2 || fresh[0].UnderFloor != "leftover" {
		t.Fatalf("new packs %+v", fresh)
	}
	if len(fresh[0].Slots) != len(files)-len(sentBefore) {
		t.Fatalf("%d of %d unsent files in the new pack", len(fresh[0].Slots), len(files)-len(sentBefore))
	}
	for _, p := range e.packsWhere(func(p *fakes.Pack) bool { return p.Status == "abandoned" }) {
		if !p.Released {
			t.Fatalf("pack %s abandoned, not released", p.ID)
		}
	}
	e.assertStateClean()
}

func TestPackSizes_APurgedCommitAnswerIsSavedAndNeverSentAgain(t *testing.T) {
	// 29.15: a pack committed long ago whose objects were all deleted and whose row was purged answers "committed,
	// purged" with no per-object results (from reserve's pack_committed, or a resumed commit). Each file was saved: it
	// is reported saved, with the pack's commit time, and one line says the objects have since been deleted.
	e, c := newUploads(t)
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	e.canned("POST packs", fakes.Canned{Status: 409, Body: map[string]any{"error": "pack committed",
		"code": "pack_committed", "pack": "p-old"}})
	e.canned("POST pack commit", fakes.Canned{Status: 200, Body: map[string]any{"status": "committed",
		"committed_at": "2026-06-01T10:00:00Z", "purged": true}})
	files := e.writeFiles(3, 3000, "f")
	res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
	if err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	for _, r := range res {
		if r.Object == nil || r.Object.Status != "deleted" || r.Object.UploadedAt == nil ||
			!r.Object.UploadedAt.Equal(time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)) {
			t.Fatalf("%s: %+v", r.Key, r.Object)
		}
	}
	if n := e.f.Network.HitsOf("PUT"); n != 0 {
		t.Fatalf("%d PUTs: sent again", n)
	}
	if got := strings.Count(logs.String(), "pack p-old was committed at 2026-06-01T10:00:00Z; its objects have since been deleted"); got != 1 {
		t.Fatalf("log: %s", logs.String())
	}
	e.assertStateClean()
}
