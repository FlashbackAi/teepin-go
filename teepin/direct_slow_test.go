// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
)

// Slow links (TEEPIN-STORAGE-DESIGN.md section 22.7), against the fakes: packs and new uploads' parts shrink to the
// speed measured, an object the speed can't carry is refused before anything is sent, and the speed is measured from
// the storage network's PUTs. The Python SDK's test_direct_slow.py, by way of the Node SDK's uploads-large.test.mjs.

// slow fixes the client's measured speed with a one-second request budget, so a request carries one second of it.
func slow(c *Client, bytesPerSecond float64) *direct.Uploads {
	up := c.Storage.uploads()
	up.Speed.SetBudget(time.Second)
	up.Speed.Set(bytesPerSecond)
	return up
}

func isConnectionSpeed(err error) bool {
	return errors.Is(err, ErrConnectionSpeed) && IsTooLarge(err) && ErrorCode(err) == "connection_speed"
}

// assertNothingSent checks no large upload was created and no part sent.
func (e *upEnv) assertNothingSent() {
	e.t.Helper()
	if n := len(e.uploads()); n != 0 || e.f.Network.HitsOf("PUT part") != 0 {
		e.t.Fatalf("%d uploads created, %d parts sent", n, e.f.Network.HitsOf("PUT part"))
	}
}

func (e *upEnv) partSizes() map[string]int64 {
	out := map[string]int64{}
	for _, u := range e.uploads() {
		out[u.Key] = u.PartSize
	}
	return out
}

func TestSlow_ShrinksARunsPacks(t *testing.T) {
	for _, c := range []struct {
		name  string
		speed float64
		packs int
	}{{"slow", 100 * kib, 4}, {"normal", 100 << 20, 1}} {
		t.Run(c.name, func(t *testing.T) {
			e, cl := newUploads(t)
			slow(cl, c.speed) // at 100 KiB/s, three of these files a pack
			files := e.writeFiles(10, 30*kib, "f")
			res, err := cl.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
			if err != nil || !allSaved(res) {
				t.Fatalf("%v %s", err, errorsOf(res))
			}
			if n := e.packs(""); n != c.packs {
				t.Fatalf("%d packs, want %d", n, c.packs)
			}
			e.assertSavedOnce(files)
		})
	}
}

func TestSlow_ARunsLaterPacksTakeTheSpeedMeasuredByThen(t *testing.T) {
	// One pack in flight at a time: the speed drops while the first is sent, and the rest are packed at it.
	e, c := newUploads(t)
	c = e.client(Config{UploadConcurrency: 1})
	up := c.Storage.uploads()
	l := direct.DefaultLimits
	l.Floor, l.Ceiling = 512*kib, 1<<20 // section 28's rules, smaller
	up.Limits = l
	up.Speed.SetBudget(time.Second)
	var once sync.Once
	up.Hooks.BeforeSend = func(string) { once.Do(func() { up.Speed.Set(100 * kib) }) }
	// At full speed the first pack takes 33 files (to the 1 MiB ceiling); the 27 left are over the floor, so no tail.
	// Then the speed drops: 100 KiB in a request, three files a pack.
	files := e.writeFiles(60, 30*kib, "f")
	res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
	if err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	if n := e.packs(""); n != 10 { // 33, then nine of 3
		t.Fatalf("%d packs, want 10", n)
	}
	e.assertSavedOnce(files)
}

func TestSlow_ShrinksPutObjectsSharedPacks(t *testing.T) {
	for _, c := range []struct {
		name   string
		speed  float64
		shared bool
	}{{"slow", 1, false}, {"normal", 0, true}} {
		t.Run(c.name, func(t *testing.T) {
			e, cl := newUploads(t)
			up := cl.Storage.uploads()
			if c.speed > 0 {
				// A target under every object: none share, where at full speed they do.
				slow(cl, c.speed)
				old := direct.SlowMin
				direct.SlowMin = 1
				t.Cleanup(func() { direct.SlowMin = old })
			}
			up.Limit = direct.NewAdaptiveLimit(1, 1)
			e.f.Network.Do(func() { e.f.Network.Delay = 50 * time.Millisecond })
			var wg sync.WaitGroup
			errs := make([]error, 6)
			for i := range errs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					data := randomData(1000 + i)
					_, errs[i] = cl.Storage.PutObject(ctx, "photos", fmt.Sprintf("k%d", i), bytes.NewReader(data), int64(len(data)), nil)
				}()
			}
			wg.Wait()
			if err := errors.Join(errs...); err != nil {
				t.Fatal(err)
			}
			n := e.packs("")
			if c.shared && n >= 6 || !c.shared && n != 6 {
				t.Fatalf("%d packs for 6 objects", n)
			}
		})
	}
}

func TestSlow_ShrinksTheNextLargeUploadsParts(t *testing.T) {
	for _, c := range []struct {
		name  string
		speed float64
		part  int64
	}{{"slow", 100 * kib, 64 * kib}, {"normal", 100 << 20, 128 * kib}} {
		t.Run(c.name, func(t *testing.T) {
			e, cl := newUploads(t)
			slow(cl, c.speed) // 100 KiB a request: parts of 64 KiB, rounded down to whole segments
			data := randomData(3*128*kib + 5)
			if _, err := cl.Storage.PutObject(ctx, "photos", "big", bytes.NewReader(data), int64(len(data)), nil); err != nil {
				t.Fatal(err)
			}
			if got := e.partSizes()["big"]; got != c.part {
				t.Fatalf("parts of %d, want %d", got, c.part)
			}
			e.assertOneUpload("big", data)
		})
	}
}

func TestSlow_RefusesASpeedTooSlowForTheSmallestPartBeforeAnythingIsSent(t *testing.T) {
	e, c := newUploads(t)
	slow(c, 50*kib) // under one 64 KiB part a second
	data := randomData(3*128*kib + 5)
	_, err := c.Storage.PutObject(ctx, "photos", "big", bytes.NewReader(data), int64(len(data)), nil)
	if !isConnectionSpeed(err) || !strings.Contains(err.Error(), "big: the connection to storage measured 50.0 KiB/s") ||
		!strings.Contains(err.Error(), "even a 0.0625 MiB part would take over 0.02 minutes") {
		t.Fatalf("%v", err)
	}
	e.assertNothingSent()
}

func TestSlow_FailsAFileTooLargeAtTheSpeedAloneInItsRun(t *testing.T) {
	e, c := newUploads(t)
	sizes := testSizes
	sizes.MaxParts = 4
	c.Storage.uploads().Sizes = sizes
	slow(c, 192*kib) // parts of at most 192 KiB: four of them hold 768 KiB
	small := e.largeFile("f0", 1000)
	big := e.largeFile("f1", 4*192*kib+1)
	res, err := c.Storage.UploadFiles(ctx, "photos", toUpload([]srcFile{small, big}), nil)
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]UploadResult{}
	for _, r := range res {
		byKey[r.Key] = r
	}
	if !byKey["up/f0"].Saved() {
		t.Fatalf("%+v", byKey["up/f0"])
	}
	if r := byKey["up/f1"]; r.Status != UploadFailed || !isConnectionSpeed(r.Err) ||
		!strings.Contains(r.Err.Error(), "parts can be at most 0.1875 MiB, and it would need more than 4 of them") {
		t.Fatalf("%+v", r)
	}
	e.assertNothingSent()
	// The same file fits at a speed whose parts are large enough.
	slow(c, 256*kib)
	obj, err := c.Storage.UploadFile(ctx, "photos", "f1", big.path, nil)
	if err != nil || obj.Size != 4*192*kib+1 {
		t.Fatalf("%v", err)
	}
}

func TestSlow_AStreamTakesItsPartSizeByTheRule(t *testing.T) {
	e, c := newUploads(t)
	slow(c, 100*kib)
	data := randomData(3*128*kib + 7)
	if _, err := c.Storage.UploadStream(ctx, "photos", "s", stream(data), nil); err != nil {
		t.Fatal(err)
	}
	// With its expected size too, and a part size asked for: capped alike.
	if _, err := c.Storage.UploadStream(ctx, "photos", "x", stream(data),
		&UploadOptions{ExpectedSize: int64(len(data)), PartSize: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if got := e.partSizes(); got["s"] != 64*kib || got["x"] != 64*kib {
		t.Fatalf("%v", got)
	}
	for _, k := range []string{"s", "x"} {
		if back, err := e.f.Direct.ReadBack("photos", k); err != nil || !bytes.Equal(back, data) {
			t.Fatalf("%s: %v", k, err)
		}
	}
}

func TestSlow_RefusesAStreamTooSlowForTheSmallestPartBeforeAnythingIsSent(t *testing.T) {
	e, c := newUploads(t)
	slow(c, 50*kib)
	for _, opts := range []*UploadOptions{nil, {ExpectedSize: 3*128*kib + 7}} {
		_, err := c.Storage.UploadStream(ctx, "photos", "s", stream(randomData(3*128*kib+7)), opts)
		if !isConnectionSpeed(err) {
			t.Fatalf("%v", err)
		}
	}
	e.assertNothingSent()
}

func TestSlow_AShrunkStreamThatRunsOutOfPartsIsTheConnectionsSpeed(t *testing.T) {
	e, c := newUploads(t)
	sizes := testSizes
	sizes.MaxParts = 3
	c.Storage.uploads().Sizes = sizes
	slow(c, 100*kib)
	_, err := c.Storage.UploadStream(ctx, "photos", "s", stream(randomData(5*64*kib)), nil)
	if !isConnectionSpeed(err) || !strings.Contains(err.Error(), "more than 3 of them, so this stream is too large") {
		t.Fatalf("%v", err)
	}
	if u := e.uploads(); len(u) != 1 || u[0].Status != "aborted" {
		t.Fatalf("%+v", u)
	}
	// One whose parts weren't shrunk says so as before.
	c.Storage.uploads().Speed.Set(100 << 20)
	_, err = c.Storage.UploadStream(ctx, "photos", "t", stream(randomData(5*128*kib)), nil)
	if !IsTooLarge(err) || ErrorCode(err) != "too_many_parts" || errors.Is(err, ErrConnectionSpeed) {
		t.Fatalf("%v", err)
	}
}

func TestSlow_MeasuresADelayedPUTButNotAFailedOrATinyOne(t *testing.T) {
	e, c := newUploads(t)
	up := c.Storage.uploads()
	up.Speed.SetSampleBytes(64 * kib)
	e.f.Network.Do(func() { e.f.Network.Delay = 300 * time.Millisecond })
	measured := func() bool {
		_, ok := up.Speed.BytesPerSecond()
		return ok
	}
	put := func(cl *Client, key string, n int) error {
		data := randomData(n)
		_, err := cl.Storage.PutObject(ctx, "photos", key, bytes.NewReader(data), int64(len(data)), nil)
		return err
	}
	if err := put(c, "tiny", 100); err != nil || measured() {
		t.Fatalf("a tiny PUT was measured (%v)", err)
	}
	// Refused (401, signed again until it fails), then unavailable (503, no wait left): never measured.
	e.f.Network.Do(func() { e.f.Network.Reject["PUT"] = 100 })
	if err := put(c, "a", 200*kib); err == nil || measured() {
		t.Fatalf("a refused PUT was measured (%v)", err)
	}
	e.f.Network.Do(func() { e.f.Network.Reject["PUT"], e.f.Network.Fail["PUT"] = 0, 100 })
	quick := e.client(Config{UploadWait: time.Second})
	quick.Storage.uploads().Speed = up.Speed // the same meter
	var su *StorageUnavailableError
	if err := put(quick, "a", 200*kib); !errors.As(err, &su) || measured() {
		t.Fatalf("a failed PUT was measured (%v)", err)
	}
	e.f.Network.Do(func() { e.f.Network.Fail["PUT"] = 0 })
	started := time.Now()
	if err := put(c, "a", 200*kib); err != nil {
		t.Fatal(err)
	}
	took := time.Since(started)
	bps, ok := up.Speed.BytesPerSecond()
	if !ok || bps > (204*kib)/0.3 || bps < (200*kib)/took.Seconds() {
		t.Fatalf("%v (took %s)", bps, took)
	}
	if err := put(c, "b", 100); err != nil {
		t.Fatal(err)
	}
	if after, _ := up.Speed.BytesPerSecond(); after != bps {
		t.Fatalf("a tiny PUT moved the speed: %v, was %v", after, bps)
	}
}

func TestSlow_AColdStartOfTimeoutsShrinksTheNextRequest(t *testing.T) {
	// Nothing measured, and every PUT takes 0.5 s while the timer starts at 0.2 s: the first parts time out, their
	// timeouts lower the estimate, the retries' timers grow with it, and the upload finishes.
	e, c := newUploads(t)
	up := c.Storage.uploads()
	up.PutTimeoutFloor, up.PutTimeoutSpeed = 200*time.Millisecond, 1e12
	up.Speed.SetSampleBytes(64 * kib)
	e.f.Network.Do(func() { e.f.Network.Delay = 500 * time.Millisecond })
	data := randomData(3 * 128 * kib) // whole parts
	if _, err := c.Storage.PutObject(ctx, "photos", "big", bytes.NewReader(data), int64(len(data)), nil); err != nil {
		t.Fatal(err)
	}
	e.assertOneUpload("big", data)
	if n := e.f.Network.HitsOf("PUT part"); n <= 3 {
		t.Fatalf("%d part PUTs: none timed out", n)
	}
	bps, ok := up.Speed.BytesPerSecond()
	if !ok || bps > (128*kib+4*kib)/0.2 {
		t.Fatalf("%v", bps)
	}
	up.Speed.SetBudget(time.Second)
	if got, slow := up.Limits.AtSpeed(up.Speed.Link().RequestBytes()); got.Ceiling >= direct.DefaultPackCeiling || !slow {
		t.Fatalf("pack limits at the measured speed: %+v", got)
	}
}

func TestSlow_GivesASlowLinksPUTTimeToFinishAtItsSpeed(t *testing.T) {
	// A 60 KiB pack at 60 KiB/s may take 2 s: the 0.3 s it takes is well inside its timer, though over the floor.
	e, c := newUploads(t)
	up := slow(c, 60*kib)
	up.PutTimeoutFloor, up.PutTimeoutSpeed = 100*time.Millisecond, 1e12
	e.f.Network.Do(func() { e.f.Network.Delay = 300 * time.Millisecond })
	data := randomData(60 * kib)
	if _, err := c.Storage.PutObject(ctx, "photos", "a", bytes.NewReader(data), int64(len(data)), nil); err != nil {
		t.Fatal(err)
	}
	if n := e.f.Network.HitsOf("PUT"); n != 1 {
		t.Fatalf("%d PUTs: it timed out", n)
	}
}

func TestSlow_GoesOnInPartsWithAFileASlowRunLeftInParts(t *testing.T) {
	// Sent in parts for a slow link, then the process died: resumed by one that has measured nothing (so the file would
	// now pack), it goes on with the same upload, never packed beside it.
	e, c := newUploads(t)
	f := e.largeFile("f", 200*kib) // over the 100 KiB a request carries at the slow speed; under the 256 KiB limit
	if code, out := e.child(childConfig{Files: toUpload([]srcFile{f}), Sent: 2, Concurrency: 1, Speed: 100 * kib,
		SpeedBudget: time.Second}); code != crashed {
		t.Fatalf("the child didn't crash: %d %s", code, out)
	}
	if _, ok := c.Storage.uploads().Speed.BytesPerSecond(); ok {
		t.Fatal("the resuming client measured something")
	}
	res, err := c.Storage.ResumeUploads(ctx, nil)
	if err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	e.assertOneUpload(f.key, f.data)
	if got := e.partSizes()[f.key]; got != 64*kib || e.packs("") != 0 {
		t.Fatalf("parts of %d, %d packs", got, e.packs(""))
	}
}

// --- at the real sizes ---------------------------------------------------------------------------------------------

func TestSlow_SendsTwelveMiBInPartsAndStillPacksThreeMiB(t *testing.T) {
	for _, way := range []string{"PutObject", "Upload", "UploadFile", "UploadFiles", "UploadStream", "CopyObject"} {
		t.Run(way, func(t *testing.T) {
			f, _ := newDirect(t)
			e := &upEnv{t: t, f: f, state: t.TempDir(), src: t.TempDir()}
			c := directClient(t, f, Config{StateDir: e.state, UploadWait: 2 * time.Minute})
			s := c.Storage
			big, small := randomData(12<<20), randomData(3<<20)
			if way == "CopyObject" {
				// At full speed both are packed, under 16 MiB.
				for k, d := range map[string][]byte{"src-big": big, "src-small": small} {
					if _, err := s.PutObject(ctx, "photos", k, bytes.NewReader(d), int64(len(d)), nil); err != nil {
						t.Fatal(err)
					}
				}
			}
			s.uploads().Speed.Set(6 << 20 / 240.0) // 6 MiB in 4 minutes
			// How it went in parts: a journaled run of one (a large file), or a stream read once (a stream's head
			// over the limit).
			var mu sync.Mutex
			journaled, streamed := false, false
			s.uploads().Hooks.Saved = func(kind, _ string) {
				mu.Lock()
				defer mu.Unlock()
				journaled = journaled || kind == "large"
			}
			s.uploads().Hooks.StreamHeld = func(int) {
				mu.Lock()
				defer mu.Unlock()
				streamed = true
			}
			defer func() {
				mu.Lock()
				defer mu.Unlock()
				if want := way == "UploadFile" || way == "UploadFiles"; journaled != want {
					t.Errorf("journaled %v, want %v", journaled, want)
				}
				if want := way == "UploadStream" || way == "CopyObject"; streamed != want {
					t.Errorf("streamed %v, want %v", streamed, want)
				}
			}()
			for k, d := range map[string][]byte{"big": big, "small": small} {
				var err error
				switch way {
				case "PutObject":
					_, err = s.PutObject(ctx, "photos", k, bytes.NewReader(d), int64(len(d)), nil)
				case "Upload":
					_, err = s.Upload(ctx, "photos", k, bytes.NewReader(d), int64(len(d)), nil)
				case "UploadFile", "UploadFiles":
					p := filepath.Join(e.src, k)
					if err := os.WriteFile(p, d, 0o644); err != nil {
						t.Fatal(err)
					}
					if way == "UploadFile" {
						_, err = s.UploadFile(ctx, "photos", k, p, nil)
					} else {
						var res []UploadResult
						res, err = s.UploadFiles(ctx, "photos", []FileToUpload{{Key: k, Path: p}}, nil)
						if err == nil && !allSaved(res) {
							err = errors.New(errorsOf(res))
						}
					}
				case "UploadStream":
					_, err = s.UploadStream(ctx, "photos", k, stream(d), nil)
				case "CopyObject":
					_, err = s.CopyObject(ctx, "photos", k, "photos", "src-"+k, nil)
				}
				if err != nil {
					t.Fatalf("%s: %v", k, err)
				}
			}
			var bigs []string
			for _, u := range e.uploads() {
				switch u.Key {
				case "big":
					bigs = append(bigs, fmt.Sprintf("%s %d", u.Status, u.PartSize))
				case "small":
					t.Fatal("3 MiB went in parts")
				}
			}
			if len(bigs) != 1 || bigs[0] != fmt.Sprintf("completed %d", 6<<20) {
				t.Fatalf("big's uploads: %v", bigs)
			}
			packed := strings.Join(e.sealedKeys(), ",")
			if !strings.Contains(","+packed+",", ",small,") || strings.Contains(","+packed+",", ",big,") {
				t.Fatalf("packed: %s", packed)
			}
			for k, d := range map[string][]byte{"big": big, "small": small} {
				if back, err := f.Direct.ReadBack("photos", k); err != nil || !bytes.Equal(back, d) {
					t.Fatalf("%s reads back wrong: %v", k, err)
				}
			}
		})
	}
}
