// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
	"github.com/FlashbackAi/teepin-go/teepin/internal/fakes"
	"github.com/FlashbackAi/teepin-go/teepin/internal/journal"
	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// Large objects in parts, streams of unknown size, and the client-side copy (internal/direct/large.go; sections 21.2,
// 22.1 E2 b, 22.4 and 23.6), against the fakes with 128 KiB parts and a 256 KiB pack limit. The Python SDK's
// test_direct_large.py, test_direct_stream.py and test_direct_copy.py, by way of the Node SDK's
// uploads-large.test.mjs.

const largeSize = 600_000 // 5 parts of 128 KiB

// uploads are the fake's large uploads.
func (e *upEnv) uploads() []fakes.DirectUpload {
	var out []fakes.DirectUpload
	e.f.API.Do(func() {
		for _, u := range e.f.Direct.Uploads {
			out = append(out, *u)
		}
	})
	return out
}

func (e *upEnv) uploadsIn(status string) int {
	n := 0
	for _, u := range e.uploads() {
		if u.Status == status {
			n++
		}
	}
	return n
}

// assertOneUpload checks a large object reads back its bytes, from exactly one completed upload, with none left open.
func (e *upEnv) assertOneUpload(key string, data []byte) {
	e.t.Helper()
	got, err := e.f.Direct.ReadBack("photos", key)
	if err != nil || !bytes.Equal(got, data) {
		e.t.Fatalf("%s reads back wrong: %v", key, err)
	}
	if e.uploadsIn("completed") != 1 || e.openUploads() != 0 {
		e.t.Fatalf("%d uploads completed, %d open", e.uploadsIn("completed"), e.openUploads())
	}
}

func (e *upEnv) largeFile(name string, size int) srcFile {
	e.t.Helper()
	data := randomData(size)
	p := filepath.Join(e.src, name)
	if err := os.WriteFile(p, data, 0o644); err != nil {
		e.t.Fatal(err)
	}
	return srcFile{key: "up/" + name, path: p, data: data}
}

func (e *upEnv) keyFiles() []string {
	var out []string
	for _, id := range e.runIDs() {
		names, _ := os.ReadDir(filepath.Join(e.state, "uploads", id, journal.KeysDir))
		for _, n := range names {
			out = append(out, n.Name())
		}
	}
	return out
}

// --- large objects -------------------------------------------------------------------------------------------------

func TestLarge_PutObjectOverThePackLimitGoesInParts(t *testing.T) {
	e, c := newUploads(t)
	data := randomData(largeSize)
	obj, err := c.Storage.PutObject(ctx, "photos", "big", bytes.NewReader(data), int64(len(data)),
		&PutObjectOptions{ContentType: "video/mp4", Metadata: map[string]string{"cam": "a"}})
	if err != nil {
		t.Fatal(err)
	}
	// E1: the plain SHA-256, however it was uploaded, as the server answers for client-encrypted objects.
	if obj.ETag != sha256Hex(data) || obj.PartCount != 5 || obj.Size != largeSize || obj.ContentType != "video/mp4" {
		t.Fatalf("%+v", obj)
	}
	e.assertOneUpload("big", data)
	if e.f.API.HitsOf("POST packs") != 0 || e.f.API.HitsOf("PUT object") != 0 {
		t.Fatal("a large object was packed or sent through the API")
	}
	if got, _, err := download(c, "big", nil); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download: %v", err)
	}
}

func TestLarge_UploadOfAReaderAtReportsProgressAndNeverGoesThroughTheAPI(t *testing.T) {
	e, c := newUploads(t)
	data := randomData(largeSize)
	var mu sync.Mutex
	var last [2]int64
	obj, err := c.Storage.Upload(ctx, "photos", "big", bytes.NewReader(data), int64(len(data)), &UploadOptions{
		Progress: func(d, tot int64) { mu.Lock(); last = [2]int64{d, tot}; mu.Unlock() }})
	if err != nil || obj.ETag != sha256Hex(data) {
		t.Fatalf("%+v %v", obj, err)
	}
	if last != [2]int64{largeSize, largeSize} {
		t.Fatalf("progress %v", last)
	}
	e.assertOneUpload("big", data)
	e.f.API.Do(func() {
		for _, r := range e.f.API.Requests {
			if strings.HasSuffix(r.Route, "part") || r.Route == "PUT object" {
				t.Errorf("a byte route of the API was called: %s", r.Route)
			}
		}
	})
}

func TestLarge_PutObjectOfAReaderThatCantSeekIsStreamedAndAShortOneRefused(t *testing.T) {
	e, c := newUploads(t)
	data := randomData(largeSize)
	obj, err := c.Storage.PutObject(ctx, "photos", "s", io.MultiReader(bytes.NewReader(data)), int64(len(data)), nil)
	if err != nil || obj.ETag != sha256Hex(data) {
		t.Fatalf("%+v %v", obj, err)
	}
	e.assertOneUpload("s", data)
	_, err = c.Storage.PutObject(ctx, "photos", "short", io.MultiReader(bytes.NewReader(data[:400_000])), int64(len(data)), nil)
	if err == nil || !strings.Contains(err.Error(), "short") || e.f.API.Object("photos", "short") != nil || e.openUploads() != 0 {
		t.Fatalf("a short body: %v", err)
	}
}

func TestLarge_UploadFileOfALargeFileIsAJournaledRunOfOne(t *testing.T) {
	e, c := newUploads(t)
	f := e.largeFile("big.bin", largeSize)
	var journaled atomic.Bool
	c.Storage.uploads().Hooks.Saved = func(kind, state string) {
		if kind == "large" {
			journaled.Store(len(e.runIDs()) == 1)
		}
	}
	obj, err := c.Storage.UploadFile(ctx, "photos", f.key, f.path, nil)
	if err != nil || obj.ETag != sha256Hex(f.data) || !journaled.Load() {
		t.Fatalf("%+v %v (journaled %v)", obj, err, journaled.Load())
	}
	e.assertOneUpload(f.key, f.data)
	e.assertStateClean()
}

func TestLarge_UploadFileWithStorageDownIsPendingAndResumeFinishesIt(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadWait: -1})
	f := e.largeFile("big.bin", largeSize)
	e.f.Network.Do(func() { e.f.Network.Fail["PUT part"] = 10_000 })
	_, err := c.Storage.UploadFile(ctx, "photos", f.key, f.path, nil)
	var su *StorageUnavailableError
	if !errors.As(err, &su) {
		t.Fatalf("%v", err)
	}
	if len(e.keyFiles()) != 1 || e.openUploads() != 1 {
		t.Fatalf("keys %v, %d open uploads: the upload and its key are kept for resume", e.keyFiles(), e.openUploads())
	}
	e.f.Network.Do(func() { e.f.Network.Fail = map[string]int{} })
	res, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
	if err != nil || statuses(res) != "saved" {
		t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
	}
	e.assertOneUpload(f.key, f.data)
	e.assertStateClean()
}

// R7: a large upload's journal write that fails mid-run (the disk full) stops the run at once. Its context is cancelled
// before the capped wait for the packs still under way, so a pack held on a slow request stops then, rather than
// holding the run until the cap.
func TestLarge_AJournalWriteFailingMidRunCancelsThePacksBeforeTheCappedWait(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadConcurrency: 4})
	up := c.Storage.uploads()
	up.CleanupCap = 5 * time.Second
	small := e.writeFiles(2, 3000, "s")
	big := e.largeFile("big.bin", largeSize)
	e.f.Network.Do(func() { e.f.Network.Delay = 6 * time.Second }) // a pack's PUT is held past the cap
	t.Cleanup(func() { e.f.Network.Do(func() { e.f.Network.Delay = 0 }) })
	up.Hooks.Saved = func(kind, _ string) {
		if kind == "large" { // the large upload goes on only once a pack is on its way
			waitFor(5*time.Second, func() bool { return e.f.Network.HitsOf("PUT") > 0 })
		}
	}
	full := errors.New("no space left on the device")
	t.Cleanup(journal.SetWriteFault(func(_ string, entries []journal.Entry) error {
		for _, en := range entries {
			if en.Table == "parts" { // the large upload's part hashes, journaled once it is created
				return full
			}
		}
		return nil
	}))
	start := time.Now()
	_, err := c.Storage.UploadFiles(ctx, "photos", append(toUpload(small), FileToUpload{Key: big.key, Path: big.path}), nil)
	took := time.Since(start)
	if !errors.Is(err, full) {
		t.Fatalf("the run's error: %v", err)
	}
	if e.f.Network.HitsOf("PUT") == 0 {
		t.Fatal("no pack was under way when the journal write failed")
	}
	t.Logf("stopped in %s", took)
	if took > 2500*time.Millisecond {
		t.Fatalf("the run took %s: the packs under way weren't cancelled before the capped wait", took)
	}
}

func TestLarge_ARunPacksTheSmallFilesAndSendsTheLargeOnesInParts(t *testing.T) {
	e, c := newUploads(t)
	small := e.writeFiles(3, 3000, "s")
	big := e.largeFile("big.bin", largeSize)
	res, err := c.Storage.UploadFiles(ctx, "photos", append(toUpload(small), FileToUpload{Key: big.key, Path: big.path}), nil)
	if err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	e.assertSavedOnce(small)
	e.assertOneUpload(big.key, big.data)
	e.assertStateClean()
}

func TestLarge_FaultsEndWithOneObjectAndOneUpload(t *testing.T) {
	cases := map[string]func(e *upEnv){
		"part lost after storing": func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Lose["PUT part"] = 1 }) },
		"part dropped":            func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Drop["PUT part"] = 1 }) },
		"part busy":               func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Fail["PUT part"] = 2 }) },
		"part signature refused":  func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Reject["PUT part"] = 1 }) },
		"part sign answer lost":   func(e *upEnv) { e.f.API.Do(func() { e.f.API.Lose["POST part sign"] = 1 }) },
		"create answer lost":      func(e *upEnv) { e.f.API.Do(func() { e.f.API.Lose["POST uploads"] = 1 }) },
		"complete answer lost":    func(e *upEnv) { e.f.API.Do(func() { e.f.API.Lose["POST complete"] = 1 }) },
		"complete busy":           func(e *upEnv) { e.f.API.Do(func() { e.f.API.Fail["POST complete"] = 2 }) },
	}
	for fault, arm := range cases {
		t.Run(fault, func(t *testing.T) {
			e, c := newUploads(t)
			arm(e)
			f := e.largeFile("big.bin", largeSize)
			if _, err := c.Storage.UploadFile(ctx, "photos", f.key, f.path, nil); err != nil {
				t.Fatal(err)
			}
			e.assertOneUpload(f.key, f.data)
			if len(e.uploads()) != 1 {
				t.Fatalf("%d uploads created", len(e.uploads()))
			}
			e.assertStateClean()
		})
	}
}

func TestLarge_StartsAgainWithAFreshKeyWhenStorageAssembledAnotherObject(t *testing.T) {
	e, c := newUploads(t)
	var mu sync.Mutex
	var keys [][]byte
	c.Storage.uploads().Hooks.NewDataKey = func() ([]byte, error) {
		k, err := tws1.NewDataKey()
		mu.Lock()
		keys = append(keys, k) // the engine's own key, to see it zeroed
		mu.Unlock()
		return k, err
	}
	e.canned("POST complete", refusal(http.StatusConflict, "upload_mismatch"))
	data := randomData(largeSize)
	if _, err := c.Storage.PutObject(ctx, "photos", "big", bytes.NewReader(data), int64(len(data)), nil); err != nil {
		t.Fatal(err)
	}
	e.assertOneUpload("big", data)
	if e.uploadsIn("aborted") != 1 || len(keys) != 2 {
		t.Fatalf("%d aborted, %d keys: the old upload must be cancelled and a fresh key taken", e.uploadsIn("aborted"), len(keys))
	}
	if !isZeroKey(keys[0]) || !isZeroKey(keys[1]) {
		t.Fatal("a data key was left in memory")
	}
}

func TestLarge_NeverCompletesAPartStorageAcknowledgesWithAnotherETag(t *testing.T) {
	e, c := newUploads(t)
	e.f.Network.Do(func() { e.f.Network.WrongETag["PUT part"] = 1 })
	data := randomData(largeSize)
	_, err := c.Storage.PutObject(ctx, "photos", "big", bytes.NewReader(data), int64(len(data)), nil)
	if reasonOf(err) != "part" {
		t.Fatalf("%v", err)
	}
	if e.uploadsIn("completed") != 0 || e.openUploads() != 0 || e.f.API.Object("photos", "big") != nil {
		t.Fatal("completed, or left open")
	}
}

func TestLarge_RefusesACompletedObjectWhoseChecksumIsntTheFiles(t *testing.T) {
	e, c := newUploads(t)
	e.canned("POST complete", fakes.Canned{Status: http.StatusOK, Body: map[string]any{"id": "x", "key": "big",
		"size_bytes": largeSize, "checksum_sha256": strings.Repeat("0", 64), "etag": strings.Repeat("0", 64)}})
	data := randomData(largeSize)
	_, err := c.Storage.PutObject(ctx, "photos", "big", bytes.NewReader(data), int64(len(data)), nil)
	if reasonOf(err) != "checksum" {
		t.Fatalf("%v", err)
	}
}

func TestLarge_AFailedPreconditionCancelsNothingTwice(t *testing.T) {
	e, c := newUploads(t)
	if _, err := c.Storage.PutObject(ctx, "photos", "big", strings.NewReader("x"), 1, nil); err != nil {
		t.Fatal(err)
	}
	data := randomData(largeSize)
	_, err := c.Storage.PutObject(ctx, "photos", "big", bytes.NewReader(data), int64(len(data)), &PutObjectOptions{IfNoneMatch: true})
	if !IsPreconditionFailed(err) {
		t.Fatalf("%v", err)
	}
	if e.f.API.HitsOf("DELETE upload") > 1 || e.openUploads() != 0 {
		t.Fatalf("%d cancels, %d open", e.f.API.HitsOf("DELETE upload"), e.openUploads())
	}
}

func TestLarge_AFileThatChangedAfterItWasQueuedFailsBeforeAnyUpload(t *testing.T) {
	e, c := newUploads(t)
	f := e.largeFile("big.bin", largeSize)
	c.Storage.uploads().Hooks.Hashed = func(path string) {
		if path == f.path {
			b, _ := os.ReadFile(path)
			b[400_000] ^= 1
			_ = os.WriteFile(path, b, 0o644)
		}
	}
	_, err := c.Storage.UploadFile(ctx, "photos", f.key, f.path, nil)
	var fc *FileChangedError
	if !errors.As(err, &fc) {
		t.Fatalf("%v", err)
	}
	if e.f.API.HitsOf("POST uploads") != 0 {
		t.Fatal("an upload was created")
	}
	e.assertStateClean()
}

func TestLarge_AFileThatChangesWhileItsPartsAreSentIsCancelled(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadConcurrency: 1})
	f := e.largeFile("big.bin", largeSize)
	var once sync.Once
	c.Storage.uploads().Hooks.Saved = func(kind, state string) {
		if kind == "part" && state == "sent" {
			once.Do(func() {
				b, _ := os.ReadFile(f.path)
				b[largeSize-10] ^= 1 // in the last part
				_ = os.WriteFile(f.path, b, 0o644)
			})
		}
	}
	_, err := c.Storage.UploadFile(ctx, "photos", f.key, f.path, nil)
	var fc *FileChangedError
	if !errors.As(err, &fc) {
		t.Fatalf("%v", err)
	}
	ups := e.uploads()
	if len(ups) != 1 || ups[0].Status != "aborted" {
		t.Fatalf("%+v", ups)
	}
	if _, signed := ups[0].Pins[5]; signed {
		t.Fatal("the changed part was signed")
	}
	if len(e.keyFiles()) != 0 {
		t.Fatal("the failed upload's key was kept")
	}
}

func TestLarge_ACrashMidPartsResumesWithTheJournaledKey(t *testing.T) {
	e, _ := newUploads(t)
	f := e.largeFile("big.bin", largeSize)
	if code, out := e.child(childConfig{Files: toUpload([]srcFile{f}), Sent: 2, Concurrency: 1}); code != crashed {
		t.Fatalf("%d %s", code, out)
	}
	ups := e.uploads()
	if len(ups) != 1 || len(e.keyFiles()) != 1 {
		t.Fatalf("%d uploads, keys %v", len(ups), e.keyFiles())
	}
	landed := len(e.f.Network.ListParts(ups[0].MPU))
	before := e.f.Network.HitsOf("PUT part")
	res, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
	if err != nil || statuses(res) != "saved" {
		t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
	}
	if sent := e.f.Network.HitsOf("PUT part") - before; sent != 5-landed {
		t.Fatalf("%d parts sent after resume, with %d of 5 landed", sent, landed)
	}
	e.assertOneUpload(f.key, f.data)
	e.assertStateClean()
}

func TestLarge_ACrashAtEveryStateResumesToOneObject(t *testing.T) {
	for _, at := range []childConfig{{Large: "new"}, {Large: "sending"}, {Sent: 1}, {Sent: 4}, {Large: "completing"}} {
		t.Run(fmt.Sprintf("%s%d", at.Large, at.Sent), func(t *testing.T) {
			e, _ := newUploads(t)
			f := e.largeFile("big.bin", largeSize)
			at.Files, at.Concurrency = toUpload([]srcFile{f}), 1
			if code, out := e.child(at); code != crashed {
				t.Fatalf("%d %s", code, out)
			}
			res, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
			if err != nil || statuses(res) != "saved" {
				t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
			}
			e.assertOneUpload(f.key, f.data)
			e.assertStateClean()
		})
	}
}

func TestDownloadFile_ReplacesATargetAnotherProgramHoldsOpenBriefly(t *testing.T) {
	_, c := newUploads(t)
	data := randomData(3000)
	if _, err := c.Storage.PutObject(ctx, "photos", "k", bytes.NewReader(data), int64(len(data)), nil); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "k")
	_ = os.WriteFile(path, []byte("old"), 0o644)
	held, err := os.Open(path) // on Windows, os.Open keeps others from renaming over it
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		held.Close()
	}()
	if _, err := c.Storage.DownloadFile(ctx, "photos", "k", path, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(path); !bytes.Equal(got, data) {
		t.Fatal("the target wasn't replaced")
	}
}

func TestLarge_ACrashAtCompletingCompletesWithTheJournaledWrap(t *testing.T) {
	e, _ := newUploads(t)
	f := e.largeFile("big.bin", largeSize)
	if code, out := e.child(childConfig{Files: toUpload([]srcFile{f}), Large: "completing"}); code != crashed {
		t.Fatalf("%d %s", code, out)
	}
	before := e.f.Network.HitsOf("PUT part")
	res, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
	if err != nil || statuses(res) != "saved" {
		t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
	}
	if e.f.Network.HitsOf("PUT part") != before {
		t.Fatal("parts were sent again")
	}
	e.assertOneUpload(f.key, f.data)
	e.assertStateClean()
}

func TestLarge_APartChangedAfterResumesCheckIsCaughtByItsHashFromCreate(t *testing.T) {
	// D1 (a): resume hashes the queued file whole first; a part changed right after that check, and before the
	// upload reads it, is caught by the part hashes journaled at create.
	e, _ := newUploads(t)
	f := e.largeFile("big.bin", largeSize)
	if code, out := e.child(childConfig{Files: toUpload([]srcFile{f}), Sent: 1, Concurrency: 1}); code != crashed {
		t.Fatalf("%d %s", code, out)
	}
	c := e.client(Config{UploadConcurrency: 1})
	c.Storage.uploads().Hooks.Hashed = func(path string) {
		b, _ := os.ReadFile(path)
		b[largeSize-10] ^= 1
		_ = os.WriteFile(path, b, 0o644)
	}
	res, err := c.Storage.ResumeUploads(ctx, nil)
	var fc *FileChangedError
	if err != nil || len(res) != 1 || res[0].Status != UploadFailed || !errors.As(res[0].Err, &fc) {
		t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
	}
	if e.openUploads() != 0 || e.f.API.Object("photos", f.key) != nil {
		t.Fatal("the upload was left open, or completed")
	}
	e.assertStateClean()
}

func TestLarge_AFileChangedWhileTheProcessWasDownFailsItsUploadCancelledAndItsKeyDeleted(t *testing.T) {
	e, _ := newUploads(t)
	f := e.largeFile("big.bin", largeSize)
	if code, out := e.child(childConfig{Files: toUpload([]srcFile{f}), Sent: 1, Concurrency: 1}); code != crashed {
		t.Fatalf("%d %s", code, out)
	}
	b, _ := os.ReadFile(f.path)
	b[10] ^= 1 // in a part that landed
	_ = os.WriteFile(f.path, b, 0o644)
	res, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
	var fc *FileChangedError
	if err != nil || statuses(res) != "failed" || !errors.As(res[0].Err, &fc) {
		t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
	}
	if e.openUploads() != 0 || e.uploadsIn("aborted") != 1 {
		t.Fatal("its upload was left open")
	}
	e.assertStateClean()
}

func TestLarge_AnUnfinishedUploadKeepsItsKeyUntilCleanRemovesBoth(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadWait: -1})
	f := e.largeFile("big.bin", largeSize)
	e.f.Network.Do(func() { e.f.Network.Fail["PUT part"] = 10_000 })
	if _, err := c.Storage.UploadFile(ctx, "photos", f.key, f.path, nil); err == nil {
		t.Fatal("storage was down")
	}
	if len(e.keyFiles()) != 1 || e.openUploads() != 1 {
		t.Fatalf("keys %v, %d open", e.keyFiles(), e.openUploads())
	}
	removed, err := c.Storage.CleanUploads(ctx, 0)
	if err != nil || len(removed) != 1 {
		t.Fatalf("%v %d", err, len(removed))
	}
	if e.openUploads() != 0 || e.uploadsIn("aborted") != 1 {
		t.Fatal("clean didn't cancel the open upload")
	}
	e.assertStateClean()
}

func TestLarge_ACompletedUploadsKeyIsDeletedAtOnceWhileTheRestOfItsRunWaits(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadWait: -1})
	small := e.writeFiles(2, 3000, "s")
	big := e.largeFile("big.bin", largeSize)
	e.f.Network.Do(func() { e.f.Network.Fail["PUT"] = 10_000 }) // packs wait; parts go
	res, err := c.Storage.UploadFiles(ctx, "photos", append(toUpload(small), FileToUpload{Key: big.key, Path: big.path}), nil)
	if err != nil || statuses(res) != "pending,pending,saved" {
		t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
	}
	if len(e.runIDs()) != 1 || len(e.keyFiles()) != 0 {
		t.Fatalf("runs %v, keys %v", e.runIDs(), e.keyFiles())
	}
}

// --- streams of unknown size ----------------------------------------------------------------------------------------

func stream(data []byte) io.Reader { return io.MultiReader(bytes.NewReader(data)) }

func TestStream_GoesInPartsTheLastDeclaredAtItsSign(t *testing.T) {
	e, c := newUploads(t)
	data := randomData(largeSize)
	obj, err := c.Storage.UploadStream(ctx, "photos", "s", stream(data), nil)
	if err != nil || obj.ETag != sha256Hex(data) || obj.Size != largeSize {
		t.Fatalf("%+v %v", obj, err)
	}
	e.assertOneUpload("s", data)
	u := e.uploads()[0]
	if u.LastPart != 5 || u.LastSize != largeSize-4*128*kib || u.SHA256 != sha256Hex(data) || u.Size != largeSize {
		t.Fatalf("last %d of %d bytes, size %d", u.LastPart, u.LastSize, u.Size)
	}
	for _, b := range u.SignBodies {
		last, _ := b["last"].(bool)
		if last != (fmt.Sprint(b["plain_size"]) == fmt.Sprint(largeSize-4*128*kib)) {
			t.Fatalf("a sign body %v", b)
		}
	}
}

func TestStream_OfWholePartsEndsWithAFullLastPart(t *testing.T) {
	e, c := newUploads(t)
	data := randomData(3 * 128 * kib)
	if _, err := c.Storage.UploadStream(ctx, "photos", "s", stream(data), nil); err != nil {
		t.Fatal(err)
	}
	e.assertOneUpload("s", data)
	if u := e.uploads()[0]; u.LastPart != 3 || u.LastSize != 128*kib {
		t.Fatalf("%d %d", u.LastPart, u.LastSize)
	}
}

func TestStream_TakesItsPartSizeFromItsExpectedSizeOrAsAsked(t *testing.T) {
	e, c := newUploads(t)
	data := randomData(largeSize)
	if _, err := c.Storage.UploadStream(ctx, "photos", "a", stream(data), &UploadOptions{PartSize: 192 * kib}); err != nil {
		t.Fatal(err)
	}
	// 3,000 parts of 128 KiB can't hold 400 MiB: the part size doubles.
	if _, err := c.Storage.UploadStream(ctx, "photos", "b", stream(data), &UploadOptions{ExpectedSize: 400 << 20}); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, u := range e.uploads() {
		got[u.Key] = u.PartSize
	}
	if got["a"] != 192*kib || got["b"] != 256*kib {
		t.Fatalf("%v", got)
	}
}

func TestStream_PastTheLastPartNumberIsTooLargeAndCancelled(t *testing.T) {
	e, c := newUploads(t)
	sizes := testSizes
	sizes.MaxParts = 4
	c.Storage.uploads().Sizes = sizes
	data := randomData(largeSize) // 5 parts
	_, err := c.Storage.UploadStream(ctx, "photos", "s", stream(data), nil)
	if !IsTooLarge(err) || ErrorCode(err) != "too_many_parts" {
		t.Fatalf("%v", err)
	}
	u := e.uploads()[0]
	if u.Status != "aborted" || u.LastPart != 0 {
		t.Fatalf("%s, last %d: nothing may be claimed as the end", u.Status, u.LastPart)
	}
	// Exactly the most parts are saved.
	exact := randomData(4 * 128 * kib)
	if _, err := c.Storage.UploadStream(ctx, "photos", "t", stream(exact), nil); err != nil {
		t.Fatal(err)
	}
}

func TestStream_KeepsAtMostConcurrencyPartsInFlightAndHoldsTwoMore(t *testing.T) {
	for _, conc := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(conc), func(t *testing.T) {
			e, c := newUploads(t)
			e.f.Network.Do(func() { e.f.Network.Delay = 20 * time.Millisecond })
			var most atomic.Int32
			c.Storage.uploads().Hooks.StreamHeld = func(n int) {
				for {
					m := most.Load()
					if int32(n) <= m || most.CompareAndSwap(m, int32(n)) {
						return
					}
				}
			}
			flight := &inFlight{inner: uploadsHTTP(c).Transport}
			uploadsHTTP(c).Transport = flight
			data := randomData(12 * 128 * kib)
			if _, err := c.Storage.UploadStream(ctx, "photos", "s", stream(data), &UploadOptions{Concurrency: conc}); err != nil {
				t.Fatal(err)
			}
			if got := flight.max.Load(); got > int32(conc) || got < 1 {
				t.Fatalf("%d parts in flight at once, with concurrency %d", got, conc)
			}
			if m := most.Load(); m > int32(conc+2) {
				t.Fatalf("held %d parts at once, with concurrency %d", m, conc)
			}
		})
	}
}

// inFlight is a transport that counts the parts in flight to the storage network.
type inFlight struct {
	inner    http.RoundTripper
	now, max atomic.Int32
}

func (f *inFlight) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodPut || r.URL.Query().Get("partNumber") == "" {
		return f.inner.RoundTrip(r)
	}
	n := f.now.Add(1)
	defer f.now.Add(-1)
	for {
		m := f.max.Load()
		if n <= m || f.max.CompareAndSwap(m, n) {
			break
		}
	}
	return f.inner.RoundTrip(r)
}

func TestStream_SendsFourPartsAtOnceUnlessTold(t *testing.T) {
	e, c := newUploads(t)
	e.f.Network.Do(func() { e.f.Network.Delay = 50 * time.Millisecond })
	// The adaptive limit starts at 4 too: let it allow more, so only the stream's own default holds it at 4.
	c.Storage.uploads().Limit = direct.NewAdaptiveLimit(32, 32)
	flight := &inFlight{inner: uploadsHTTP(c).Transport}
	uploadsHTTP(c).Transport = flight
	if _, err := c.Storage.UploadStream(ctx, "photos", "s", stream(randomData(12*128*kib)), nil); err != nil {
		t.Fatal(err)
	}
	if got := flight.max.Load(); got != 4 {
		t.Fatalf("%d parts at once", got)
	}
}

func TestStream_PassingFailuresAndALostCompleteAreRetried(t *testing.T) {
	for name, arm := range map[string]func(e *upEnv){
		"part busy":            func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Fail["PUT part"] = 2 }) },
		"part lost":            func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Lose["PUT part"] = 1 }) },
		"complete answer lost": func(e *upEnv) { e.f.API.Do(func() { e.f.API.Lose["POST complete"] = 1 }) },
	} {
		t.Run(name, func(t *testing.T) {
			e, c := newUploads(t)
			arm(e)
			data := randomData(largeSize)
			if _, err := c.Storage.UploadStream(ctx, "photos", "s", stream(data), nil); err != nil {
				t.Fatal(err)
			}
			e.assertOneUpload("s", data)
		})
	}
}

// breaking is a stream that fails after n bytes.
type breaking struct {
	r io.Reader
	n int
}

func (b *breaking) Read(p []byte) (int, error) {
	if b.n <= 0 {
		return 0, errors.New("the pipe broke")
	}
	if len(p) > b.n {
		p = p[:b.n]
	}
	n, err := b.r.Read(p)
	b.n -= n
	return n, err
}

func TestStream_FailuresCancelTheUploadAndNeverComplete(t *testing.T) {
	cases := map[string]struct {
		arm    func(e *upEnv)
		src    func(data []byte) io.Reader
		expect string
	}{
		"the stream breaks": {func(*upEnv) {}, func(d []byte) io.Reader { return &breaking{r: bytes.NewReader(d), n: 450_000} }, "the pipe broke"},
		"storage ended the upload": {func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Gone["PUT part"] = 1 }) }, stream,
			"can't be read again"},
		"a part acknowledged wrongly": {func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.WrongETag["PUT part"] = 1 }) },
			stream, "wasn't stored as signed"},
		"another object completed": {func(e *upEnv) {
			e.canned("POST complete", fakes.Canned{Status: http.StatusOK, Body: map[string]any{"size_bytes": 1, "key": "s"}})
		}, stream, "isn't the stream"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e, c := newUploads(t)
			tc.arm(e)
			_, err := c.Storage.UploadStream(ctx, "photos", "s", tc.src(randomData(largeSize)), nil)
			if err == nil || !strings.Contains(err.Error(), tc.expect) {
				t.Fatalf("%v", err)
			}
			if e.openUploads() != 0 || (name != "another object completed" && e.uploadsIn("completed") != 0) {
				t.Fatalf("%d open, %d completed", e.openUploads(), e.uploadsIn("completed"))
			}
		})
	}
}

func TestStream_ZeroesItsDataKeyOnceItIsWrapped(t *testing.T) {
	_, c := newUploads(t)
	var key []byte
	c.Storage.uploads().Hooks.NewDataKey = func() ([]byte, error) {
		k, err := tws1.NewDataKey()
		key = k
		return k, err
	}
	var zeroAtFirstPart atomic.Bool
	var once sync.Once
	uploadsHTTP(c).Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("partNumber") != "" {
			once.Do(func() { zeroAtFirstPart.Store(isZeroKey(key)) })
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	if _, err := c.Storage.UploadStream(ctx, "photos", "s", stream(randomData(largeSize)), nil); err != nil {
		t.Fatal(err)
	}
	if !zeroAtFirstPart.Load() {
		t.Fatal("the data key was still in memory when the first part went")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStream_IsNeverSentToAnUploadCreatedWithAnotherLayout(t *testing.T) {
	e, c := newUploads(t)
	var acct map[string]any
	e.f.API.Do(func() { acct = e.f.Direct.AccountKey() })
	e.canned("POST uploads", fakes.Canned{Status: http.StatusCreated, Body: map[string]any{"upload_id": beef,
		"part_size": 64 * kib, "size": nil, "account_key": acct}})
	_, err := c.Storage.UploadStream(ctx, "photos", "s", stream(randomData(largeSize)), nil)
	if reasonOf(err) != "layout" || e.f.Network.HitsOf("PUT part") != 0 {
		t.Fatalf("%v, %d parts", err, e.f.Network.HitsOf("PUT part"))
	}
}

func TestStream_ACancelCancelsItsUploadOnceEvenWithTheControlPlaneDownAndEndsPromptly(t *testing.T) {
	e, c := newUploads(t)
	e.f.API.Do(func() { e.f.API.Fail["DELETE upload"] = 100 })
	cctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	go func() {
		for i := 0; i < 4; i++ {
			_, _ = pw.Write(randomData(128 * kib))
		}
		// the caller's source stalls: the caller cancels
	}()
	errc := make(chan error, 1)
	go func() {
		_, err := c.Storage.UploadStream(cctx, "photos", "s", pr, nil)
		errc <- err
	}()
	waitFor(10*time.Second, func() bool { return e.f.API.HitsOf("POST part sign") >= 2 })
	began := time.Now()
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	if took := time.Since(began); took > 3*time.Second {
		t.Fatalf("the cancel took %s", took)
	}
	if e.f.API.HitsOf("DELETE upload") != 1 {
		t.Fatalf("%d cancels: one call, never retried while stopping", e.f.API.HitsOf("DELETE upload"))
	}
	pw.Close()
}

func TestStream_AFailedStreamCancelsItsUploadWithBoundedRetriesWhenTheControlPlaneIsDown(t *testing.T) {
	e, c := newUploads(t)
	e.f.API.Do(func() { e.f.API.Fail["DELETE upload"] = 100 })
	began := time.Now()
	_, err := c.Storage.UploadStream(ctx, "photos", "s", &breaking{r: bytes.NewReader(randomData(largeSize)), n: 450_000}, nil)
	if err == nil || time.Since(began) > 5*time.Second {
		t.Fatalf("%v after %s", err, time.Since(began))
	}
	if n := e.f.API.HitsOf("DELETE upload"); n < 1 || n > 1+defaultRetries {
		t.Fatalf("%d cancels", n)
	}
}

// discardParts is a storage network for parts that keeps nothing: each body is read for its MD5 and dropped, so a
// test measures the stream's own memory.
type discardParts struct{ inner http.RoundTripper }

func (d discardParts) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodPut || r.URL.Query().Get("partNumber") == "" {
		return d.inner.RoundTrip(r)
	}
	h := md5.New()
	_, _ = io.Copy(h, r.Body)
	r.Body.Close()
	time.Sleep(30 * time.Millisecond)
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": {`"` + hex.EncodeToString(h.Sum(nil)) + `"`}},
		Body: http.NoBody, Request: r}, nil
}

// randomStream yields n random bytes into the reader's own buffer: it holds nothing of its own.
type randomStream struct{ left int }

func (s *randomStream) Read(p []byte) (int, error) {
	if s.left <= 0 {
		return 0, io.EOF
	}
	n := min(len(p), s.left)
	for i := range p[:n] {
		p[i] = byte(s.left - i)
	}
	s.left -= n
	return n, nil
}

// TestStream_ItsMemoryIsItsPartsInFlightAndThreeMore measures the live heap as each part is sealed: the parts in
// flight, the ciphertext just sealed, and the next part read, concurrency + 2 parts (the plaintext sealed is already
// let go there: Go frees a value once nothing reads it again, so the peak, concurrency + 3, is only while a part is
// sealed). It measures there, with the reader waiting, rather than by sampling from another goroutine: a collection
// running beside the pipeline counts parts that die while it runs, and the most of many samples catches that every
// time.
func TestStream_ItsMemoryIsItsPartsInFlightAndThreeMore(t *testing.T) {
	const part = 2 << 20
	for _, conc := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(conc), func(t *testing.T) {
			_, c := newUploads(t)
			uploadsHTTP(c).Transport = discardParts{inner: uploadsHTTP(c).Transport}
			runtime.GC()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			base := ms.HeapAlloc
			var peak uint64
			seals := 0
			c.Storage.uploads().Hooks.StreamSealed = func() {
				runtime.GC()
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > base {
					peak = max(peak, m.HeapAlloc-base)
				}
				seals++
			}
			// The parts never land (the network keeps nothing), so the complete fails: only the memory is measured.
			_, _ = c.Storage.UploadStream(ctx, "photos", "s", &randomStream{left: 16 * part},
				&UploadOptions{PartSize: part, Concurrency: conc})
			if seals != 16 {
				t.Fatalf("%d parts sealed", seals)
			}
			t.Logf("held %.2f parts at most", float64(peak)/part)
			if bound := uint64(conc+2)*part + part/2; peak > bound {
				t.Fatalf("held %.2f parts as a part was sealed, over concurrency + 2 (%d)", float64(peak)/part, conc+2)
			}
		})
	}
}

// --- the client-side copy ---------------------------------------------------------------------------------------------

func TestCopy_ASmallObjectIsCopiedHereKeepingItsTypeAndMetadata(t *testing.T) {
	e, c := newUploads(t)
	if _, err := c.Storage.PutObject(ctx, "photos", "src", strings.NewReader("hello"), 5,
		&PutObjectOptions{ContentType: "text/plain", Metadata: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	obj, err := c.Storage.CopyObject(ctx, "photos", "dst", "photos", "src", nil)
	if err != nil || obj.ContentType != "text/plain" || obj.Metadata["k"] != "v" || obj.ETag != sha256Hex([]byte("hello")) {
		t.Fatalf("%+v %v", obj, err)
	}
	if got, _ := e.f.Direct.ReadBack("photos", "dst"); string(got) != "hello" || e.f.API.HitsOf("POST object/copy") != 0 {
		t.Fatal("not copied here")
	}
}

func TestCopy_HonoursTheTargetsConditions(t *testing.T) {
	_, c := newUploads(t)
	for _, k := range []string{"src", "dst"} {
		if _, err := c.Storage.PutObject(ctx, "photos", k, strings.NewReader(k), int64(len(k)), nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Storage.CopyObject(ctx, "photos", "dst", "photos", "src", &CopyOptions{IfNoneMatch: true}); !IsPreconditionFailed(err) {
		t.Fatalf("IfNoneMatch: %v", err)
	}
	if _, err := c.Storage.CopyObject(ctx, "photos", "dst", "photos", "src", &CopyOptions{IfMatch: strings.Repeat("0", 64)}); !IsPreconditionFailed(err) {
		t.Fatalf("IfMatch, another: %v", err)
	}
	if _, err := c.Storage.CopyObject(ctx, "photos", "dst", "photos", "src", &CopyOptions{IfMatch: sha256Hex([]byte("dst"))}); err != nil {
		t.Fatalf("IfMatch, the target's: %v", err)
	}
}

func TestCopy_ALargeObjectIsStreamedInPartsAcrossBuckets(t *testing.T) {
	e, c := newUploads(t)
	mustBucket(t, c, "backup")
	data := randomData(largeSize)
	if _, err := c.Storage.PutObject(ctx, "photos", "big", bytes.NewReader(data), int64(len(data)), nil); err != nil {
		t.Fatal(err)
	}
	obj, err := c.Storage.CopyObject(ctx, "backup", "big", "photos", "big", nil)
	if err != nil || obj.ETag != sha256Hex(data) {
		t.Fatalf("%+v %v", obj, err)
	}
	if got, err := e.f.Direct.ReadBack("backup", "big"); err != nil || !bytes.Equal(got, data) {
		t.Fatal("copied wrong")
	}
	streamed := false
	for _, u := range e.uploads() {
		streamed = streamed || (u.Bucket == "backup" && u.LastPart > 0)
	}
	if !streamed {
		t.Fatal("the copy wasn't a stream")
	}
}

func TestCopy_ALargeCopyOverAnExistingTargetIsRefusedItsUploadCancelled(t *testing.T) {
	e, c := newUploads(t)
	data := randomData(largeSize)
	if _, err := c.Storage.PutObject(ctx, "photos", "big", bytes.NewReader(data), int64(len(data)), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Storage.PutObject(ctx, "photos", "dst", strings.NewReader("x"), 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Storage.CopyObject(ctx, "photos", "dst", "photos", "big", &CopyOptions{IfNoneMatch: true}); !IsPreconditionFailed(err) {
		t.Fatalf("%v", err)
	}
	if e.openUploads() != 0 {
		t.Fatal("an upload was left open")
	}
}

func TestCopy_AnEmptyObjectTakesA2AndAStagedSourceIsRefused(t *testing.T) {
	e, c := newUploads(t)
	if _, err := c.Storage.PutObject(ctx, "photos", "empty", bytes.NewReader(nil), 0, &PutObjectOptions{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	obj, err := c.Storage.CopyObject(ctx, "photos", "e2", "photos", "empty", nil)
	if err != nil || obj.Size != 0 || obj.ContentType != "text/plain" || e.f.API.HitsOf("POST object/empty") != 2 {
		t.Fatalf("%+v %v", obj, err)
	}
	e.f.API.PutObject("photos", "staged", []byte("still on the API"), "")
	if _, err := c.Storage.CopyObject(ctx, "photos", "s2", "photos", "staged", nil); !errors.Is(err, ErrObjectStaged) {
		t.Fatalf("%v", err)
	}
}

func TestCopy_ASourceReplacedMidCopyFailsItWithNothingAtTheTarget(t *testing.T) {
	for _, size := range []int{3000, largeSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			e, c := newUploads(t)
			data := randomData(size)
			if _, err := c.Storage.PutObject(ctx, "photos", "src", bytes.NewReader(data), int64(len(data)), nil); err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			replace := func() {
				once.Do(func() {
					if err := e.f.PutBlob("photos", "src", randomData(size), blobPart, "", nil); err != nil {
						t.Error(err)
					}
				})
			}
			if size < largeSize {
				engine(c).Hooks.Session = func(string, string) { replace() } // after its details, before its read
			} else {
				engine(c).Hooks.Window = func(s *direct.Session, off, n int64) (int64, int64) { // after parts are up
					replace()
					return off, n
				}
			}
			_, err := c.Storage.CopyObject(ctx, "photos", "dst", "photos", "src", nil)
			if !errors.Is(err, ErrObjectChanged) {
				t.Fatalf("%v", err)
			}
			if e.f.API.Object("photos", "dst") != nil || e.openUploads() != 0 {
				t.Fatal("something was left at the target, or open")
			}
		})
	}
}

func TestCopy_ASourceWithNoETagIsRefusedNeverCopiedUnpinned(t *testing.T) {
	e, c := newUploads(t)
	e.canned("GET object", fakes.Canned{Status: http.StatusOK, Body: map[string]any{"key": "src", "size_bytes": 10}})
	_, err := c.Storage.CopyObject(ctx, "photos", "dst", "photos", "src", nil)
	if err == nil || !strings.Contains(err.Error(), "no ETag") {
		t.Fatalf("%v", err)
	}
}
