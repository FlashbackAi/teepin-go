// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
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

// Direct uploads in packs (internal/direct/uploads.go, runs.go; TEEPIN-STORAGE-DESIGN.md sections 17, 22.4 and 25),
// against the fake control plane and storage network: PutObject's shared packs, empty objects (A2), upload runs,
// every storage fault, a crash at every state (a child process that exits there) followed by ResumeUploads,
// cancellation (section 22.13), and cleanup. After each case every file is saved exactly once with its own bytes, or
// failed with the right error. The Python SDK's test_direct_packs.py and the upload tests of test_direct_async.py,
// by way of the Node SDK's uploads.test.mjs.

// --- PutObject -------------------------------------------------------------------------------------------------------

func TestPut_IsPackedEncryptedAndDescribed(t *testing.T) {
	e, c := newUploads(t)
	data := []byte("hello, packs")
	obj, err := c.Storage.PutObject(ctx, "photos", "a.txt", bytes.NewReader(data), int64(len(data)),
		&PutObjectOptions{ContentType: "text/plain", Metadata: map[string]string{"owner": "ana"}})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256Hex(data)
	if obj.Key != "a.txt" || obj.Size != int64(len(data)) || obj.ETag != sum || obj.ChecksumSHA256 != sum ||
		obj.ContentType != "text/plain" || obj.Metadata["owner"] != "ana" || obj.UploadedAt == nil || obj.Backend != "fake" {
		t.Fatalf("described as %+v", obj)
	}
	o := e.f.API.Object("photos", "a.txt")
	if o.Location != "packed" || !o.ClientEncrypted || o.ID != obj.ID {
		t.Fatalf("stored as %+v", o)
	}
	if got, err := e.f.Direct.ReadBack("photos", "a.txt"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back %q %v", got, err)
	}
	if bytes.Contains(e.f.Network.Blob(o.Physical), data) {
		t.Fatal("the storage network holds the plaintext")
	}
	for _, r := range e.f.Network.RequestsOf("") {
		if strings.Contains(r.Header["authorization"], fakes.APIKey) {
			t.Fatal("the API key reached the storage network")
		}
	}
	if e.f.API.HitsOf("PUT object") != 0 {
		t.Fatal("bytes went through the API")
	}
}

func TestPut_AnEmptyObjectTakesTheMetadataOnlyRoute(t *testing.T) {
	e, c := newUploads(t)
	obj, err := c.Storage.PutObject(ctx, "photos", "empty", bytes.NewReader(nil), 0,
		&PutObjectOptions{ContentType: "text/plain", Metadata: map[string]string{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	if obj.Size != 0 || obj.ETag != fakes.EmptySHA256 || obj.Metadata["k"] != "v" {
		t.Fatalf("%+v", obj)
	}
	if e.f.API.HitsOf("POST object/empty") != 1 || e.f.API.HitsOf("POST packs") != 0 || len(e.f.Network.RequestsOf("")) != 0 {
		t.Fatal("an empty object sent bytes, or took a pack")
	}
	if _, err := c.Storage.PutObject(ctx, "photos", "empty", bytes.NewReader(nil), 0, &PutObjectOptions{IfNoneMatch: true}); !IsPreconditionFailed(err) {
		t.Fatalf("If-None-Match: %v", err)
	}
	if _, err := c.Storage.PutObject(ctx, "photos", "empty", bytes.NewReader(nil), 0, &PutObjectOptions{IfMatch: strings.Repeat("0", 64)}); !IsPreconditionFailed(err) {
		t.Fatalf("If-Match, another ETag: %v", err)
	}
	if _, err := c.Storage.PutObject(ctx, "photos", "empty", bytes.NewReader(nil), 0, &PutObjectOptions{IfMatch: `"` + fakes.EmptySHA256 + `"`}); err != nil {
		t.Fatalf("If-Match, its ETag (quoted): %v", err)
	}
	got, _, err := download(c, "empty", nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("read back %d bytes, %v", len(got), err)
	}
}

func TestPut_SharesPacksBetweenCallsWaitingAtOnce(t *testing.T) {
	e, c := newUploads(t)
	e.f.Network.Do(func() { e.f.Network.Delay = 50 * time.Millisecond })
	var wg sync.WaitGroup
	files := make([]srcFile, 30)
	errs := make([]error, 30)
	for i := range files {
		files[i] = srcFile{key: fmt.Sprintf("p/%d", i), data: randomData(1000 + i)}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = c.Storage.PutObject(ctx, "photos", files[i].key, bytes.NewReader(files[i].data), int64(len(files[i].data)), nil)
		}(i)
	}
	if !waitGroupWithin(&wg, 30*time.Second) {
		t.Fatal("a put is still waiting: a freed slot woke nobody")
	}
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	e.assertSavedOnce(files)
	if n := e.packs(""); n >= 30 || n < 2 {
		t.Fatalf("%d packs for 30 puts", n)
	}
}

func waitGroupWithin(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

func TestPut_NeverPutsTwoWritesToOneKeyInOnePack(t *testing.T) {
	e, c := newUploads(t)
	e.f.Network.Do(func() { e.f.Network.Delay = 50 * time.Millisecond })
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "same"
			if i%2 == 1 {
				key = fmt.Sprintf("other%d", i)
			}
			if _, err := c.Storage.PutObject(ctx, "photos", key, strings.NewReader("v"+fmt.Sprint(i)), 2, nil); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	e.f.API.Do(func() {
		for _, p := range e.f.Direct.Packs {
			seen := map[string]bool{}
			for _, s := range p.Slots {
				if seen[s.Key] {
					t.Errorf("pack %s holds %s twice", p.ID, s.Key)
				}
				seen[s.Key] = true
			}
		}
	})
}

func TestPut_NeverSignsAPackThatDoesntDecryptBack(t *testing.T) {
	e, c := newUploads(t)
	c.Storage.uploads().Hooks.Encrypt = func(dek, plain []byte) ([]byte, error) {
		stored, err := tws1.Encrypt(dek, plain)
		stored[len(stored)-1] ^= 1
		return stored, err
	}
	_, err := c.Storage.PutObject(ctx, "photos", "k", strings.NewReader("data"), 4, nil)
	if reasonOf(err) != "build" {
		t.Fatalf("%v", err)
	}
	if e.f.API.HitsOf("POST pack sign") != 0 || len(e.f.Network.RequestsOf("PUT")) != 0 {
		t.Fatal("a pack that doesn't decrypt back was signed or sent")
	}
}

func TestPut_FailsOnlyItsOwnObjectOnAFailedPrecondition(t *testing.T) {
	e, c := newUploads(t)
	if _, err := c.Storage.PutObject(ctx, "photos", "taken", strings.NewReader("old"), 3, nil); err != nil {
		t.Fatal(err)
	}
	// Both wait while the only slot is held, so they share one pack.
	up := c.Storage.uploads()
	held := 0
	for up.Limit.TryAcquire() {
		held++
	}
	var wg sync.WaitGroup
	var errTaken, errFree error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errTaken = c.Storage.PutObject(ctx, "photos", "taken", strings.NewReader("new"), 3, &PutObjectOptions{IfNoneMatch: true})
	}()
	go func() {
		defer wg.Done()
		_, errFree = c.Storage.PutObject(ctx, "photos", "free", strings.NewReader("free"), 4, &PutObjectOptions{IfNoneMatch: true})
	}()
	waitFor(5*time.Second, func() bool { return queued(up) == 2 })
	for i := 0; i < held; i++ {
		up.ReleaseSlot()
	}
	wg.Wait()
	if !IsPreconditionFailed(errTaken) || errFree != nil {
		t.Fatalf("taken: %v; free: %v", errTaken, errFree)
	}
	if got, _ := e.f.Direct.ReadBack("photos", "taken"); string(got) != "old" {
		t.Fatalf("taken holds %q", got)
	}
	if e.packs("") != 2 {
		t.Fatalf("%d packs", e.packs(""))
	}
}

// queued is how many PutObject calls wait for a pack.
func queued(up *direct.Uploads) int { return up.Queued() }

func TestPut_IsALoudErrorOnADeploymentWithoutDirectUploads(t *testing.T) {
	api := fakes.StartAPI()
	t.Cleanup(api.Close)
	api.Do(func() { api.Bucket("photos") })
	c := NewClient(Config{BaseURL: api.URL, APIKey: fakes.APIKey, StateDir: t.TempDir()})
	for _, size := range []int{0, 10, 300 * kib} {
		_, err := c.Storage.PutObject(ctx, "photos", "k", bytes.NewReader(make([]byte, size)), int64(size), nil)
		if ErrorCode(err) != "direct_upload_unsupported" || !strings.Contains(err.Error(), "direct") {
			t.Fatalf("%d bytes: %v", size, err)
		}
	}
	if api.HitsOf("PUT object") != 0 {
		t.Fatal("it fell back to the API's byte route")
	}
}

func TestPut_GivesUpOnceStorageHasBeenDownForTheWait(t *testing.T) {
	e, c := newUploads(t)
	e.f.Network.Do(func() { e.f.Network.Fail["PUT"] = 1000 })
	_, err := c.Storage.PutObject(ctx, "photos", "k", strings.NewReader("x"), 1, nil)
	var su *StorageUnavailableError
	if !errors.As(err, &su) {
		t.Fatalf("%v", err)
	}
	var total time.Duration
	for _, d := range e.slept() {
		total += d
	}
	if total > 2*time.Minute || total < time.Minute {
		t.Fatalf("waited %s of a 2 minute wait", total)
	}
}

func TestPut_FailsAtOnceWithNoWait(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadWait: -1})
	e.f.Network.Do(func() { e.f.Network.Fail["PUT"] = 1000 })
	_, err := c.Storage.PutObject(ctx, "photos", "k", strings.NewReader("x"), 1, nil)
	var su *StorageUnavailableError
	if !errors.As(err, &su) {
		t.Fatalf("%v", err)
	}
	for _, d := range e.slept() {
		if d >= time.Second {
			t.Fatalf("waited %s with no wait", d)
		}
	}
}

func TestPut_WaitsOutTooManyPendingUploadsHonouringRetryAfter(t *testing.T) {
	e, c := newUploads(t)
	e.canned("POST packs", fakes.Canned{Status: http.StatusTooManyRequests, Header: map[string]string{"Retry-After": "30"},
		Body: map[string]any{"error": "too many pending uploads", "code": "too_many_pending_uploads"}})
	e.f.API.Do(func() {
		e.f.API.Canned["POST packs"] = append(e.f.API.Canned["POST packs"], e.f.API.Canned["POST packs"]...)
	})
	if _, err := c.Storage.PutObject(ctx, "photos", "k", strings.NewReader("x"), 1, nil); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range e.slept() {
		found = found || d == 30*time.Second
	}
	if !found {
		t.Fatalf("waits %v never honoured Retry-After: 30", e.slept())
	}
}

func TestPut_NeverLogsADataKeyAWrapASignatureOrTheAPIKey(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}), &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	e, c := newUploads(t)
	var keys [][]byte
	c.Storage.uploads().Hooks.NewDataKey = func() ([]byte, error) {
		k, err := tws1.NewDataKey()
		keys = append(keys, append([]byte(nil), k...))
		return k, err
	}
	if _, err := c.Storage.PutObject(ctx, "photos", "k", strings.NewReader("secret"), 6, nil); err != nil {
		t.Fatal(err)
	}
	files := e.writeFiles(2, 1000, "f")
	e.canned("POST packs", refusal(http.StatusUnauthorized, "unauthorized"))
	_, _ = c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
	if _, err := c.Storage.CleanUploads(ctx, 0); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	logged := buf.String()
	mu.Unlock()
	if !strings.Contains(logged, "removing upload run") {
		t.Fatalf("nothing logged to check: %q", logged)
	}
	secrets := []string{fakes.APIKey}
	for _, k := range keys {
		secrets = append(secrets, base64.StdEncoding.EncodeToString(k), fmt.Sprintf("%x", k))
	}
	e.f.API.Do(func() {
		for _, p := range e.f.Direct.Packs {
			if objs, ok := p.Parsed["objects"].([]any); ok {
				for _, o := range objs {
					secrets = append(secrets, o.(map[string]any)["wrapped_key"].(string))
				}
			}
		}
	})
	for _, r := range e.f.Network.RequestsOf("") {
		secrets = append(secrets, r.Header["authorization"])
	}
	for _, s := range secrets {
		if s != "" && strings.Contains(logged, s) {
			t.Fatalf("a secret was logged: %q", s)
		}
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// --- PutObject beside a run (profile 1's review of step 4) -------------------------------------------------------------

func TestPut_QueuedBehindARunIsDispatchedWhenTheRunsPackFreesItsSlot(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadConcurrency: 1})
	c.Storage.uploads().Limits = oneFileAPack
	files := e.writeFiles(6, 1000, "f")
	e.f.Network.Do(func() { e.f.Network.Delay = 200 * time.Millisecond })
	runDone := make(chan time.Time, 1)
	go func() {
		if _, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil); err != nil {
			t.Error(err)
		}
		runDone <- time.Now()
	}()
	time.Sleep(300 * time.Millisecond)
	var puts []time.Time
	for i := 0; i < 3; i++ {
		begun := time.Now()
		if _, err := c.Storage.PutObject(ctx, "photos", fmt.Sprintf("put%d", i), strings.NewReader("x"), 1, nil); err != nil {
			t.Fatal(err)
		}
		if took := time.Since(begun); took > 10*time.Second {
			t.Fatalf("a put during the run took %s: woken by its own wait, not a freed slot", took)
		}
		puts = append(puts, time.Now())
	}
	end := <-runDone
	if !puts[2].Before(end) {
		t.Fatal("the puts waited for the run to end")
	}
}

func TestPut_GivesUpUnsavedWhenNoSlotComesFreeWithinTheWait(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadWait: 200 * time.Millisecond})
	up := c.Storage.uploads()
	for up.Limit.TryAcquire() {
	}
	_, err := c.Storage.PutObject(ctx, "photos", "k", strings.NewReader("x"), 1, nil)
	var su *StorageUnavailableError
	if !errors.As(err, &su) || !strings.Contains(err.Error(), "wasn't saved") {
		t.Fatalf("%v", err)
	}
	if queued(up) != 0 {
		t.Fatal("the item stayed queued")
	}
	if e.packs("") != 0 {
		t.Fatal("it was reserved")
	}
	// No priority is left behind: a run's Acquire takes a freed slot.
	up.ReleaseSlot()
	actx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := up.Limit.Acquire(actx); err != nil {
		t.Fatalf("a run's acquire still waits on PutObject's priority: %v", err)
	}
}

func TestPut_WaitsForASlotHoweverLongTheWait(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadWait: 1000 * time.Hour})
	up := c.Storage.uploads()
	held := 0
	for up.Limit.TryAcquire() {
		held++
	}
	errc := make(chan error, 1)
	go func() {
		_, err := c.Storage.PutObject(ctx, "photos", "k", strings.NewReader("x"), 1, nil)
		errc <- err
	}()
	waitFor(5*time.Second, func() bool { return queued(up) == 1 })
	select {
	case err := <-errc:
		t.Fatalf("returned before a slot came free: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	up.ReleaseSlot()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a freed slot didn't wake it")
	}
}

func TestPut_ACancelWhileWaitingTakesItsObjectOutOfTheQueue(t *testing.T) {
	_, c := newUploads(t)
	up := c.Storage.uploads()
	for up.Limit.TryAcquire() {
	}
	cctx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() {
		_, err := c.Storage.PutObject(cctx, "photos", "k", strings.NewReader("x"), 1, nil)
		errc <- err
	}()
	waitFor(5*time.Second, func() bool { return queued(up) == 1 })
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	if queued(up) != 0 {
		t.Fatal("the cancelled object stayed queued")
	}
}

// A freed slot goes to a waiting PutObject before a run's next pack, however the wake-ups fall (priority, not timing):
// here the slot is freed with no one woken, as when the run's goroutine is first to wake, and the run still waits.
func TestPut_AFreedSlotGoesToAWaitingPutBeforeARunsNextPack(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadWait: 1000 * time.Hour})
	up := c.Storage.uploads()
	for up.Limit.TryAcquire() {
	}
	errc := make(chan error, 1)
	go func() {
		_, err := c.Storage.PutObject(ctx, "photos", "k", strings.NewReader("x"), 1, nil)
		errc <- err
	}()
	waitFor(5*time.Second, func() bool { return queued(up) == 1 })
	up.Limit.Release(direct.OutcomeFailed) // a slot is free, and the put not yet woken
	actx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if err := up.Limit.Acquire(actx); err == nil {
		t.Fatal("a run's pack took the slot a waiting put was owed")
	}
	up.ReleaseSlot() // wakes the put
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestPut_ZeroesEachDataKeyOnceItIsWrapped(t *testing.T) {
	_, c := newUploads(t)
	var mu sync.Mutex
	var keys [][]byte
	c.Storage.uploads().Hooks.NewDataKey = func() ([]byte, error) {
		k, err := tws1.NewDataKey()
		mu.Lock()
		keys = append(keys, k) // the engine's own key, to see it zeroed
		mu.Unlock()
		return k, err
	}
	if _, err := c.Storage.PutObject(ctx, "photos", "k", strings.NewReader("data"), 4, nil); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || !isZeroKey(keys[0]) {
		t.Fatal("a pack's data key was left in memory")
	}
}

// --- the account key ----------------------------------------------------------------------------------------------

func TestAccountKey_IsRefusedWhenItChangesUnderTheSameVersion(t *testing.T) {
	e, c := newUploads(t)
	if _, err := c.Storage.PutObject(ctx, "photos", "a", strings.NewReader("x"), 1, nil); err != nil {
		t.Fatal(err)
	}
	swapAccountKey(t, e, false)
	_, err := c.Storage.PutObject(ctx, "photos", "b", strings.NewReader("x"), 1, nil)
	if reasonOf(err) != "account_key" || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("%v", err)
	}
	// A new client on the same state directory finds the pin there.
	c2 := e.client(Config{})
	if _, err := c2.Storage.PutObject(ctx, "photos", "b", strings.NewReader("x"), 1, nil); reasonOf(err) != "account_key" {
		t.Fatalf("a new client: %v", err)
	}
	// A new version is accepted.
	e.f.API.Do(func() { e.f.Direct.KeyVersion = 2 })
	if _, err := c2.Storage.PutObject(ctx, "photos", "b", strings.NewReader("x"), 1, nil); err != nil {
		t.Fatalf("a new version: %v", err)
	}
}

func swapAccountKey(t *testing.T, e *upEnv, account bool) {
	t.Helper()
	priv, pub, err := tws1.GenerateAccountKey()
	if err != nil {
		t.Fatal(err)
	}
	e.f.API.Do(func() {
		e.f.Direct.PrivateKey, e.f.Direct.PublicKey = priv, pub
		if account {
			e.f.Direct.AccountID = "00000000-0000-7000-8000-0000000000aa"
		}
	})
}

func TestAccountKey_IsRefusedForAnotherAccountInOneClient(t *testing.T) {
	e, c := newUploads(t)
	if _, err := c.Storage.PutObject(ctx, "photos", "a", strings.NewReader("x"), 1, nil); err != nil {
		t.Fatal(err)
	}
	swapAccountKey(t, e, true)
	if _, err := c.Storage.PutObject(ctx, "photos", "b", strings.NewReader("x"), 1, nil); reasonOf(err) != "account_key" ||
		!strings.Contains(err.Error(), "in one client") {
		t.Fatalf("%v", err)
	}
}

// --- faults ------------------------------------------------------------------------------------------------------------

func TestFaults_PassingFaultsEndSavedExactlyOnce(t *testing.T) {
	cases := map[string]func(e *upEnv){
		"PUT lost after storing":     func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Lose["PUT"] = 1 }) },
		"PUT dropped before storing": func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Drop["PUT"] = 1 }) },
		"PUT busy":                   func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Fail["PUT"] = 2 }) },
		"PUT signature refused":      func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Reject["PUT"] = 1 }) },
		"commit answer lost":         func(e *upEnv) { e.f.API.Do(func() { e.f.API.Lose["POST pack commit"] = 1 }) },
		"reserve answer lost":        func(e *upEnv) { e.f.API.Do(func() { e.f.API.Lose["POST packs"] = 1 }) },
		"sign answer lost":           func(e *upEnv) { e.f.API.Do(func() { e.f.API.Lose["POST pack sign"] = 1 }) },
		"reserve busy":               func(e *upEnv) { e.f.API.Do(func() { e.f.API.Fail["POST packs"] = 2 }) },
		"commit verifying": func(e *upEnv) {
			e.canned("POST pack commit", fakes.Canned{Status: http.StatusAccepted, Body: map[string]any{"status": "verifying"}})
		},
	}
	for fault, arm := range cases {
		t.Run(fault, func(t *testing.T) {
			e, c := newUploads(t)
			arm(e)
			files := e.writeFiles(3, 3000, "f")
			res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
			if err != nil || statuses(res) != "saved,saved,saved" {
				t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
			}
			e.assertSavedOnce(files)
			if e.packs("") != 1 {
				t.Fatalf("%d reservations for a passing fault", e.packs(""))
			}
			puts := e.f.Network.HitsOf("PUT")
			switch fault {
			case "PUT lost after storing":
				if puts != 1 {
					t.Fatalf("the commit found it, yet it was sent %d times", puts)
				}
			case "PUT dropped before storing":
				// An unknown outcome (section 28.18): commit is asked again after 250 ms, 500 ms, 1 s and 2 s, then
				// once more as 5 s since the upload come, and the same bytes go again; the next commit finds them.
				if puts != 2 || e.f.API.HitsOf("POST pack commit") != 7 {
					t.Fatalf("%d PUTs, %d commits", puts, e.f.API.HitsOf("POST pack commit"))
				}
				var looks []time.Duration
				for _, d := range e.slept() {
					switch d {
					case 250 * time.Millisecond, 500 * time.Millisecond, time.Second, 1250 * time.Millisecond, 2 * time.Second:
						looks = append(looks, d)
					}
				}
				if want := []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second,
					1250 * time.Millisecond}; !slices.Equal(looks, want) {
					t.Fatalf("looks %v, want %v", looks, want)
				}
			case "PUT busy":
				if puts < 2 {
					t.Fatalf("%d PUTs", puts)
				}
			case "PUT signature refused":
				if e.f.API.HitsOf("POST pack sign") != 2 {
					t.Fatalf("%d signs", e.f.API.HitsOf("POST pack sign"))
				}
			}
			e.assertStateClean()
		})
	}
}

// unsentOnce is a transport whose first PUT to the storage network never connects.
type unsentOnce struct {
	inner http.RoundTripper
	done  atomic.Bool
}

func (u *unsentOnce) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodPut && !strings.Contains(r.URL.Path, "/v1/") && u.done.CompareAndSwap(false, true) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	return u.inner.RoundTrip(r)
}

func TestFaults_TheRestOfTheMatrixEndsSavedExactlyOnce(t *testing.T) {
	cases := map[string]func(e *upEnv, c *Client){
		"reserve abandoned": func(e *upEnv, c *Client) {
			e.canned("POST packs", refusal(http.StatusConflict, "reservation_abandoned"))
		},
		"commit abandoned": func(e *upEnv, c *Client) {
			e.canned("POST pack commit", refusal(http.StatusConflict, "reservation_abandoned"))
		},
		"commit mismatch": func(e *upEnv, c *Client) {
			e.canned("POST pack commit", refusal(http.StatusConflict, "pack_mismatch"))
		},
		"commit unverified": func(e *upEnv, c *Client) {
			e.canned("POST pack commit", refusal(http.StatusBadGateway, "pack_unverified"))
		},
		"PUT bad digest": func(e *upEnv, c *Client) { e.f.Network.Do(func() { e.f.Network.BadDigest["PUT"] = 1 }) },
		"PUT unsent": func(e *upEnv, c *Client) {
			c.httpClient.Transport = &unsentOnce{inner: c.httpClient.Transport}
		},
		"signature about to expire": func(e *upEnv, c *Client) {
			var asked atomic.Bool
			c.Storage.uploads().Hooks.ExpiresSoon = func(soon bool) bool { return asked.CompareAndSwap(false, true) || soon }
		},
	}
	for fault, arm := range cases {
		t.Run(fault, func(t *testing.T) {
			e, c := newUploads(t)
			arm(e, c)
			files := e.writeFiles(3, 3000, "f")
			res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
			if err != nil || statuses(res) != "saved,saved,saved" {
				t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
			}
			e.assertSavedOnce(files)
			switch fault {
			case "signature about to expire":
				if e.f.API.HitsOf("POST pack sign") != 2 || e.f.Network.HitsOf("PUT") != 1 {
					t.Fatal("not signed again before anything was sent")
				}
			case "PUT bad digest", "commit mismatch", "commit abandoned", "reserve abandoned":
				if e.f.API.HitsOf("POST packs") != 2 {
					t.Fatalf("%d reservations, want a new one", e.f.API.HitsOf("POST packs"))
				}
			case "PUT unsent":
				if e.f.Network.HitsOf("PUT") != 1 {
					t.Fatalf("%d PUTs reached storage", e.f.Network.HitsOf("PUT"))
				}
			}
		})
	}
}

func TestFaults_FinalFaultsFailTheirFilesNeverInALoop(t *testing.T) {
	cases := []struct {
		fault, expect string
		arm           func(e *upEnv)
	}{
		{"sign refuses the manifest", "manifest_mismatch", func(e *upEnv) {
			e.canned("POST pack sign", refusal(http.StatusConflict, "manifest_mismatch"))
		}},
		{"the bucket is gone at commit", "bucket_not_found", func(e *upEnv) {
			e.canned("POST pack commit", refusal(http.StatusNotFound, "bucket_not_found"))
		}},
		{"every signature refused", "InvalidSignature", func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.Reject["PUT"] = 100 }) }},
		{"the pack is damaged every time", "rebuilt", func(e *upEnv) { e.f.Network.Do(func() { e.f.Network.BadDigest["PUT"] = 100 }) }},
	}
	for _, tc := range cases {
		t.Run(tc.fault, func(t *testing.T) {
			e, c := newUploads(t)
			tc.arm(e)
			files := e.writeFiles(3, 3000, "f")
			res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
			if err != nil || statuses(res) != "failed,failed,failed" {
				t.Fatalf("%v %s", err, statuses(res))
			}
			for _, r := range res {
				if !strings.Contains(ErrorCode(r.Err)+r.Err.Error(), tc.expect) {
					t.Fatalf("%v", r.Err)
				}
			}
			if e.packs("sealed") != 0 {
				t.Fatal("a pack was committed")
			}
			switch tc.fault {
			case "every signature refused":
				if e.f.Network.HitsOf("PUT") != 4 {
					t.Fatalf("%d PUTs: three signatures again, then no more", e.f.Network.HitsOf("PUT"))
				}
			case "the pack is damaged every time":
				if e.f.API.HitsOf("POST packs") != 1+direct.MaxRebuilds {
					t.Fatalf("%d reservations", e.f.API.HitsOf("POST packs"))
				}
			}
			e.assertStateClean()
		})
	}
}

// --- upload runs -------------------------------------------------------------------------------------------------------

func TestRuns_SaveEveryFileAndLeaveNothingBehind(t *testing.T) {
	e, c := newUploads(t)
	// Section 28 with a floor of 512 KiB and a ceiling of 1 MiB: five files fill the first pack; the seven left are over
	// the floor, so the second takes five and its two left are a tail it takes too. Two packs, both over the floor.
	l := direct.DefaultLimits
	l.Floor, l.Ceiling = 512*kib, 1<<20
	c.Storage.uploads().Limits = l
	files := e.writeFiles(12, 200_000, "f")
	var paths []string
	var total int64
	for _, f := range files {
		paths = append(paths, f.path)
		total += int64(len(f.data))
	}
	list, err := FilesToUpload(paths, "up/", filepath.Dir(files[0].path))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var last [2]int64
	res, err := c.Storage.UploadFiles(ctx, "photos", list, &UploadFilesOptions{Progress: func(d, tot int64) {
		mu.Lock()
		last = [2]int64{d, tot}
		mu.Unlock()
	}})
	if err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	for i, r := range res {
		if r.Key != files[i].key || r.Object.Key != r.Key || r.Path != files[i].path {
			t.Fatalf("result %d is %+v", i, r)
		}
	}
	e.assertSavedOnce(files)
	if n := e.packs(""); n != 2 {
		t.Fatalf("%d packs, want 2", n)
	}
	if last != [2]int64{total, total} {
		t.Fatalf("progress ended at %v of %d", last, total)
	}
	e.assertStateClean()
	history, _ := os.ReadFile(filepath.Join(e.state, "uploads", "history.log"))
	if !strings.Contains(string(history), "saved=12 failed=0") {
		t.Fatalf("history %q", history)
	}
}

func TestRuns_SaveAnEmptyFileWithTheRestThroughA2(t *testing.T) {
	e, c := newUploads(t)
	files := e.writeFiles(3, 3000, "f")
	empty := filepath.Join(e.src, "empty.bin")
	_ = os.WriteFile(empty, nil, 0o644)
	res, err := c.Storage.UploadFiles(ctx, "photos", append(toUpload(files), FileToUpload{Key: "up/empty.bin", Path: empty}), nil)
	if err != nil || statuses(res) != "saved,saved,saved,saved" {
		t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
	}
	if o := e.f.API.Object("photos", "up/empty.bin"); o == nil || o.Size != 0 || e.f.API.HitsOf("POST object/empty") != 1 {
		t.Fatal("the empty file wasn't put through A2")
	}
	e.assertSavedOnce(files)
}

func TestRuns_KeepAnEmptyFileTheAPICantTakeYetPendingAndResumeSavesIt(t *testing.T) {
	e, c := newUploads(t)
	empty := filepath.Join(e.src, "empty.bin")
	_ = os.WriteFile(empty, nil, 0o644)
	e.f.API.Do(func() { e.f.API.Fail["POST object/empty"] = 100 })
	res, err := c.Storage.UploadFiles(ctx, "photos", []FileToUpload{{Key: "up/empty.bin", Path: empty}}, nil)
	if err != nil || statuses(res) != "pending" {
		t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
	}
	e.f.API.Do(func() { e.f.API.Fail = map[string]int{} })
	again, err := c.Storage.ResumeUploads(ctx, nil)
	if err != nil || statuses(again) != "saved" || e.f.API.Object("photos", "up/empty.bin") == nil {
		t.Fatalf("%v %s", err, statuses(again))
	}
	if runs, _ := c.Storage.ListUploadRuns(ctx); len(runs) != 0 {
		t.Fatalf("%d runs left", len(runs))
	}
}

func TestRuns_FailAFileThatChangesAlone(t *testing.T) {
	e, c := newUploads(t)
	files := e.writeFiles(4, 3000, "f")
	changing := files[1].path
	c.Storage.uploads().Hooks.Hashed = func(path string) {
		if path == changing {
			f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			_, _ = f.WriteString("more")
			f.Close()
		}
	}
	res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
	if err != nil {
		t.Fatal(err)
	}
	var fc *FileChangedError
	if res[1].Status != UploadFailed || !errors.As(res[1].Err, &fc) || fc.Path != changing {
		t.Fatalf("the changed file: %+v", res[1])
	}
	rest := []srcFile{files[0], files[2], files[3]}
	for _, i := range []int{0, 2, 3} {
		if !res[i].Saved() {
			t.Fatalf("%s: %v", res[i].Key, res[i].Err)
		}
	}
	e.assertSavedOnce(rest)
	// The changed file's pack gave the others back (section 28.5): its reservation released, one pack sent.
	if released := len(e.packsWhere(func(p *fakes.Pack) bool { return p.Released })); e.packs("sealed") != 1 || released != 1 {
		t.Fatalf("%d sealed, %d released", e.packs("sealed"), released)
	}
}

func TestRuns_FailAnUnreadableFileAlone(t *testing.T) {
	e, c := newUploads(t)
	files := e.writeFiles(2, 3000, "f")
	res, err := c.Storage.UploadFiles(ctx, "photos", append(toUpload(files),
		FileToUpload{Key: "up/missing.bin", Path: filepath.Join(e.state, "nope.bin")}), nil)
	if err != nil || statuses(res) != "saved,saved,failed" {
		t.Fatalf("%v %s", err, statuses(res))
	}
	var fc *FileChangedError
	if !errors.As(res[2].Err, &fc) {
		t.Fatalf("%v", res[2].Err)
	}
}

func TestRuns_KeepNothingOnDiskWithNoJournal(t *testing.T) {
	e, c := newUploads(t)
	files := e.writeFiles(2, 3000, "f")
	res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), &UploadFilesOptions{NoJournal: true})
	if err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	if _, err := os.Stat(filepath.Join(e.state, "uploads")); !os.IsNotExist(err) {
		t.Fatalf("the state directory was written: %v", err)
	}
}

func TestRuns_KeepFilesStorageCouldntTakePendingAndResumeFinishesThem(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadWait: time.Minute})
	e.f.Network.Do(func() { e.f.Network.Fail["PUT"] = 1000 })
	files := e.writeFiles(3, 3000, "f")
	res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
	if err != nil || statuses(res) != "pending,pending,pending" {
		t.Fatalf("%v %s", err, statuses(res))
	}
	for _, r := range res {
		var su *StorageUnavailableError
		if !errors.As(r.Err, &su) || !r.Pending() {
			t.Fatalf("%v", r.Err)
		}
	}
	runs, err := c.Storage.ListUploadRuns(ctx)
	if err != nil || len(runs) != 1 || runs[0].Pending != 3 || runs[0].Failed != 0 || runs[0].InUse || runs[0].Expired {
		t.Fatalf("%+v %v", runs, err)
	}
	e.f.Network.Do(func() { e.f.Network.Fail = map[string]int{} })
	again, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
	if err != nil || len(again) != 3 || !allSaved(again) {
		t.Fatalf("%v %s", err, errorsOf(again))
	}
	e.assertSavedOnce(files)
	e.assertStateClean()
}

func TestRuns_SignAPackAgainWhenItsCommitFindsItUnsigned(t *testing.T) {
	e, c := newUploads(t)
	e.canned("POST pack commit", refusal(http.StatusConflict, "pack_not_signed"))
	files := e.writeFiles(3, 3000, "f")
	res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
	if err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	e.assertSavedOnce(files)
	if e.f.API.HitsOf("POST pack sign") != 2 || e.f.Network.HitsOf("PUT") != 2 || e.packs("") != 1 {
		t.Fatalf("%d signs, %d PUTs", e.f.API.HitsOf("POST pack sign"), e.f.Network.HitsOf("PUT"))
	}
}

func TestRuns_StopOnARevokedKeyAndResumeFinishesTheRun(t *testing.T) {
	e, c := newUploads(t)
	files := e.writeFiles(3, 3000, "f")
	e.canned("POST packs", refusal(http.StatusUnauthorized, "unauthorized"))
	res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
	if !IsUnauthorized(err) || statuses(res) != "pending,pending,pending" {
		t.Fatalf("%v %s", err, statuses(res))
	}
	runs, _ := c.Storage.ListUploadRuns(ctx)
	if len(runs) != 1 || runs[0].Pending != 3 || runs[0].Saved != 0 || runs[0].InUse {
		t.Fatalf("%+v", runs)
	}
	again, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
	if err != nil || !allSaved(again) {
		t.Fatalf("%v %s", err, errorsOf(again))
	}
	if runs, _ := c.Storage.ListUploadRuns(ctx); len(runs) != 0 {
		t.Fatal("the run was left")
	}
	e.assertSavedOnce(files)
}

// --- a crash at every state, then resume --------------------------------------------------------------------------------

func TestCrash_ResumeSavesExactlyOnceFromEveryState(t *testing.T) {
	for _, at := range []string{"new", "reserved", "built", "signed", "uploading", "sent", "committing"} {
		t.Run(at, func(t *testing.T) {
			e, _ := newUploads(t)
			files := e.writeFiles(3, 3000, "f")
			e.crashAt(files, at)
			c := e.client(Config{})
			runs, err := c.Storage.ListUploadRuns(ctx)
			if err != nil || len(runs) != 1 || runs[0].Pending != 3 || runs[0].InUse {
				t.Fatalf("%+v %v", runs, err)
			}
			res, err := c.Storage.ResumeUploads(ctx, nil)
			if err != nil || len(res) != 3 || !allSaved(res) {
				t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
			}
			e.assertSavedOnce(files)
			e.assertStateClean()
		})
	}
}

func TestCrash_ADamagedOrLostPackFileIsRebuiltUnderANewReservation(t *testing.T) {
	for _, damage := range []string{"damaged", "lost"} {
		t.Run(damage, func(t *testing.T) {
			e, _ := newUploads(t)
			files := e.writeFiles(3, 3000, "f")
			e.crashAt(files, "built")
			packs := filepath.Join(e.state, "uploads", e.runIDs()[0], "packs")
			names, _ := os.ReadDir(packs)
			if len(names) != 1 {
				t.Fatalf("%d pack files", len(names))
			}
			p := filepath.Join(packs, names[0].Name())
			if damage == "lost" {
				_ = os.Remove(p)
			} else {
				b, _ := os.ReadFile(p)
				b[100] ^= 0xff
				_ = os.WriteFile(p, b, 0o600)
			}
			res, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
			if err != nil || !allSaved(res) {
				t.Fatalf("%v %s", err, errorsOf(res))
			}
			e.assertSavedOnce(files)
			if e.packs("") != 2 || e.f.Network.HitsOf("PUT") != 1 {
				t.Fatalf("%d reservations, %d PUTs: the bad file was sent", e.packs(""), e.f.Network.HitsOf("PUT"))
			}
			e.assertStateClean()
		})
	}
}

func TestCrash_RenewsAReservationThatExpiredWhileTheProcessWasDown(t *testing.T) {
	e, _ := newUploads(t)
	files := e.writeFiles(3, 3000, "f")
	e.crashAt(files, "reserved")
	e.f.API.Do(func() { e.f.Direct.Skew = 2 * time.Hour })
	res, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
	if err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	e.assertSavedOnce(files)
	if e.packs("abandoned") != 1 {
		t.Fatalf("%d abandoned", e.packs("abandoned"))
	}
}

// copyDir copies a run's directory (a backup of the device's state).
func copyDir(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, p)
		dst := filepath.Join(to, rel)
		if d.IsDir() {
			return os.MkdirAll(dst, 0o700)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, b, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCrash_FindsThePackCommittedWhenAnOldJournalIsRestoredFromABackup(t *testing.T) {
	e, _ := newUploads(t)
	files := e.writeFiles(3, 3000, "f")
	e.crashAt(files, "new")
	id := e.runIDs()[0]
	backup := t.TempDir()
	copyDir(t, filepath.Join(e.state, "uploads", id), backup)
	if res, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil); err != nil || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	e.assertStateClean()
	copyDir(t, backup, filepath.Join(e.state, "uploads", id)) // the backup is restored
	_ = os.Remove(filepath.Join(e.state, "uploads", id, journal.LockFile))
	res, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
	if err != nil || len(res) != 3 || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	for _, r := range res {
		if r.Object.ID != e.f.API.Object("photos", r.Key).ID {
			t.Fatalf("%s matched to another object", r.Key)
		}
	}
	e.assertSavedOnce(files)
	if e.packs("") != 1 {
		t.Fatalf("%d reservations: a second copy", e.packs(""))
	}
	e.assertStateClean()
}

func TestCrash_LeavesARunAnotherHolderIsWorkingOnAlone(t *testing.T) {
	e, _ := newUploads(t)
	files := e.writeFiles(3, 3000, "f")
	e.crashAt(files, "signed")
	id := e.runIDs()[0]
	// Another holder takes the run, through a lock of its own (as another process's would be).
	held := journal.NewLock(filepath.Join(e.state, "uploads", id, journal.LockFile))
	if ok, err := held.Acquire(); !ok || err != nil {
		t.Fatal(ok, err)
	}
	defer held.Release()
	other := e.client(Config{})
	if res, err := other.Storage.ResumeUploads(ctx, nil); err != nil || len(res) != 0 {
		t.Fatalf("%v %d", err, len(res))
	}
	if runs, _ := other.Storage.ListUploadRuns(ctx); len(runs) != 1 || !runs[0].InUse {
		t.Fatalf("%+v", runs)
	}
	if removed, err := other.Storage.CleanUploads(ctx, 0); err != nil || len(removed) != 0 {
		t.Fatalf("%v %d", err, len(removed))
	}
	if len(e.runIDs()) != 1 {
		t.Fatal("the run was removed")
	}
}

func TestCrash_WarnsOfARunWhoseJournalIsDamagedAndResumesTheOthers(t *testing.T) {
	e, _ := newUploads(t)
	files := e.writeFiles(3, 3000, "f")
	e.crashAt(files, "signed")
	id := e.runIDs()[0]
	_ = os.WriteFile(filepath.Join(e.state, "uploads", id, journal.EventsFile), []byte("damaged\n[]\n"), 0o600)
	other := e.writeFiles(2, 100, "g")
	e.crashAt(other, "reserved")
	c := e.client(Config{})
	var warned []string
	c.Storage.uploads().Warn = func(m string) { warned = append(warned, m) }
	res, err := c.Storage.ResumeUploads(ctx, nil)
	if err != nil || len(res) != 2 || !allSaved(res) {
		t.Fatalf("%v %s", err, errorsOf(res))
	}
	if !strings.Contains(strings.Join(warned, "\n"), id) || !strings.Contains(strings.Join(warned, "\n"), "damaged") {
		t.Fatalf("warned %v", warned)
	}
	// It was released, never left locked.
	if ok, _ := journal.NewLock(filepath.Join(e.state, "uploads", id, journal.LockFile)).HeldElsewhere(); ok {
		t.Fatal("the damaged run is still locked")
	}
}

func TestCrash_CleansOnlyOldRunsAndSaysSo(t *testing.T) {
	e, c := newUploads(t)
	files := e.writeFiles(3, 3000, "f")
	e.canned("POST packs", refusal(http.StatusUnauthorized, "unauthorized"))
	if _, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil); !IsUnauthorized(err) {
		t.Fatal(err)
	}
	other := e.client(Config{})
	if removed, err := other.Storage.CleanUploads(ctx, RunExpiry); err != nil || len(removed) != 0 {
		t.Fatalf("a fresh run: %v %d", err, len(removed))
	}
	id := e.runIDs()[0]
	rd, err := journal.TakeRun(e.state, journal.Uploads, id)
	if err != nil || rd == nil {
		t.Fatal(err)
	}
	var run map[string]any
	_, _ = rd.Journal().Get("run", "run", &run)
	run["updated"] = time.Now().Add(-8 * 24 * time.Hour).UnixMilli()
	_ = rd.Journal().Put("run", "run", run)
	_ = rd.Release()
	if runs, _ := other.Storage.ListUploadRuns(ctx); len(runs) != 1 || !runs[0].Expired {
		t.Fatalf("%+v", runs)
	}
	var warned []string
	other.Storage.uploads().Warn = func(m string) { warned = append(warned, m) }
	history := filepath.Join(e.state, "uploads", "history.log")
	hf, _ := os.OpenFile(history, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	_, _ = fmt.Fprintf(hf, "%d run long ago\n", time.Now().Add(-8*24*time.Hour).Unix())
	hf.Close()
	removed, err := other.Storage.CleanUploads(ctx, RunExpiry)
	if err != nil || len(removed) != 1 || removed[0].ID != id {
		t.Fatalf("%v %+v", err, removed)
	}
	if h, _ := os.ReadFile(history); strings.Contains(string(h), "long ago") {
		t.Fatal("CleanUploads didn't trim the history to 7 days")
	}
	if !strings.Contains(strings.Join(warned, "\n"), "files not uploaded") || !strings.Contains(strings.Join(warned, "\n"), id) {
		t.Fatalf("warned %v", warned)
	}
	if runs, _ := other.Storage.ListUploadRuns(ctx); len(runs) != 0 {
		t.Fatal("still listed")
	}
	e.assertStateClean()
}

// --- a cancel, then resume (section 22.13) -------------------------------------------------------------------------

// slowRun is n files, one a pack, and a cleanup cap shorter than a request to the storage network.
func slowRun(e *upEnv, c *Client, n int) []srcFile {
	up := c.Storage.uploads()
	up.Limits = oneFileAPack
	up.CleanupCap = 500 * time.Millisecond
	return e.writeFiles(n, 200_000, "f")
}

func TestCancel_LeavesTheRunForResumeWhichSavesEachFileOnce(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadConcurrency: 1})
	files := slowRun(e, c, 6)
	e.f.Network.Do(func() { e.f.Network.Delay = 300 * time.Millisecond })
	cctx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() {
		_, err := c.Storage.UploadFiles(cctx, "photos", toUpload(files), nil)
		errc <- err
	}()
	time.Sleep(800 * time.Millisecond) // some packs sent, others not yet
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	e.f.Network.Do(func() { e.f.Network.Delay = 0 })
	if len(e.runIDs()) == 0 {
		t.Fatal("the cancelled run kept no journal")
	}
	var res []UploadResult
	waitFor(10*time.Second, func() bool {
		r, err := e.client(Config{}).Storage.ResumeUploads(ctx, nil)
		res = append(res, r...)
		return err == nil && len(e.runIDs()) == 0
	})
	if !allSaved(res) {
		t.Fatalf("%s", errorsOf(res))
	}
	e.assertSavedOnce(files)
}

func TestCancel_StopsEveryGoroutineBeforeItReturnsAndNothingIsSentAfter(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadConcurrency: 3})
	files := slowRun(e, c, 6)
	e.f.Network.Do(func() { e.f.Network.Delay = 2 * time.Second })
	tr := newTracked()
	c.httpClient.Transport = tr
	cctx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() {
		_, err := c.Storage.UploadFiles(cctx, "photos", toUpload(files), nil)
		errc <- err
	}()
	time.Sleep(700 * time.Millisecond)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	if left := tr.running(); len(left) > 0 {
		t.Fatalf("requests outlived the cancel: %v", left)
	}
	sent := len(e.f.Network.RequestsOf(""))
	sealed := len(e.sealedKeys())
	time.Sleep(2500 * time.Millisecond)
	if len(e.f.Network.RequestsOf("")) != sent || len(e.sealedKeys()) != sealed {
		t.Fatal("storage requests or commits came after the cancel returned")
	}
}

func TestCancel_IsResumedInTheSameProcessRightAfter(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadConcurrency: 3})
	files := slowRun(e, c, 6)
	e.f.Network.Do(func() { e.f.Network.Delay = 2 * time.Second })
	cctx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() {
		_, err := c.Storage.UploadFiles(cctx, "photos", toUpload(files), nil)
		errc <- err
	}()
	time.Sleep(700 * time.Millisecond)
	cancel()
	<-errc
	e.f.Network.Do(func() { e.f.Network.Delay = 0 })
	res, err := c.Storage.ResumeUploads(ctx, nil)
	if err != nil || !allSaved(res) {
		t.Fatalf("%v %s %s", err, statuses(res), errorsOf(res))
	}
	if runs, _ := c.Storage.ListUploadRuns(ctx); len(runs) != 0 {
		t.Fatalf("%d runs left", len(runs))
	}
	e.assertSavedOnce(files)
}

func TestCancel_FromTheCallersOwnProgressCallbackIsResumedInTheSameProcess(t *testing.T) {
	e, c := newUploads(t)
	files := slowRun(e, c, 6)
	e.f.Network.Do(func() { e.f.Network.Delay = 300 * time.Millisecond })
	cctx, cancel := context.WithCancel(ctx)
	var calls atomic.Int32
	_, err := c.Storage.UploadFiles(cctx, "photos", toUpload(files), &UploadFilesOptions{Progress: func(int64, int64) {
		if calls.Add(1) == 2 {
			cancel() // the caller stops
		}
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	e.f.Network.Do(func() { e.f.Network.Delay = 0 })
	var res []UploadResult
	waitFor(10*time.Second, func() bool {
		r, err := c.Storage.ResumeUploads(ctx, nil)
		res = append(res, r...)
		return err == nil && len(e.runIDs()) == 0
	})
	e.assertSavedOnce(files)
}

func TestCancel_ReleasesTheRunWhenAPackThatOutlastsTheCapEndsNeverBefore(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadConcurrency: 2})
	files := slowRun(e, c, 3)
	release := make(chan struct{})
	c.Storage.uploads().Hooks.BeforeSend = func(key string) {
		if key == "up/f1.bin" {
			select { // deaf to the cancel, as a request in flight may be
			case <-release:
			case <-time.After(30 * time.Second):
			}
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	var seen atomic.Int32
	began := time.Now()
	_, err := c.Storage.UploadFiles(cctx, "photos", toUpload(files), &UploadFilesOptions{Progress: func(int64, int64) {
		if seen.Add(1) == 1 {
			cancel() // the first pack's commit, while the second is held
		}
	}})
	if !errors.Is(err, context.Canceled) || time.Since(began) > 2*time.Second {
		t.Fatalf("%v after %s", err, time.Since(began))
	}
	if res, err := c.Storage.ResumeUploads(ctx, nil); err != nil || len(res) != 0 {
		t.Fatalf("released while a pack might still commit: %v %d", err, len(res))
	}
	close(release) // the held pack ends: its goroutine, the last, releases the run
	var res []UploadResult
	waitFor(10*time.Second, func() bool {
		r, _ := c.Storage.ResumeUploads(ctx, nil)
		res = append(res, r...)
		return len(r) > 0
	})
	if !allSaved(res) {
		t.Fatalf("%s %s", statuses(res), errorsOf(res))
	}
	e.assertSavedOnce(files)
}

// twoPendingRuns leaves two runs pending (storage down past a wait of 0).
func twoPendingRuns(t *testing.T, e *upEnv) []srcFile {
	t.Helper()
	c := e.client(Config{UploadWait: -1})
	e.f.Network.Do(func() { e.f.Network.Fail["PUT"] = 10_000 })
	a := e.writeFiles(2, 3000, "a")
	b := e.writeFiles(2, 3000, "b")
	for _, files := range [][]srcFile{a, b} {
		res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil)
		if err != nil || statuses(res) != "pending,pending" {
			t.Fatalf("%v %s", err, statuses(res))
		}
	}
	e.f.Network.Do(func() { e.f.Network.Fail = map[string]int{} })
	return append(a, b...)
}

func TestCancel_AnInterruptedResumeReleasesTheRunsItHadNotStarted(t *testing.T) {
	// Profile 1's R-1: ResumeUploads locks every run up front. Cancelled on the first, it releases the runs it hadn't
	// started at once, so a resume in the same process finishes both.
	e, _ := newUploads(t)
	files := twoPendingRuns(t, e)
	ids := e.runIDs()
	c := e.client(Config{})
	cctx, cancel := context.WithCancel(ctx)
	_, err := c.Storage.ResumeUploads(cctx, func(int64, int64) { cancel() })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	second := journal.NewLock(filepath.Join(e.state, "uploads", ids[1], journal.LockFile))
	if held, _ := second.HeldElsewhere(); held {
		t.Fatal("a run ResumeUploads hadn't started is still locked")
	}
	waitFor(10*time.Second, func() bool {
		_, _ = c.Storage.ResumeUploads(ctx, nil)
		runs, _ := c.Storage.ListUploadRuns(ctx)
		return len(runs) == 0
	})
	e.assertSavedOnce(files)
}

func TestCancel_AnInterruptedResumeKeepsTheRunUntilItsLastPackEnds(t *testing.T) {
	e, _ := newUploads(t)
	c := e.client(Config{UploadConcurrency: 2, UploadWait: -1})
	files := slowRun(e, c, 3)
	e.f.Network.Do(func() { e.f.Network.Fail["PUT"] = 10_000 })
	if res, err := c.Storage.UploadFiles(ctx, "photos", toUpload(files), nil); err != nil || statuses(res) != "pending,pending,pending" {
		t.Fatalf("%v %s", err, statuses(res))
	}
	e.f.Network.Do(func() { e.f.Network.Fail = map[string]int{} })
	// The busy answers halved the client's adaptive limit; two packs at once again, so the held one isn't alone.
	c.Storage.uploads().Limit = direct.NewAdaptiveLimit(2, 2)
	release := make(chan struct{})
	c.Storage.uploads().Hooks.BeforeSend = func(key string) {
		if key == "up/f1.bin" {
			select {
			case <-release:
			case <-time.After(30 * time.Second):
			}
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	var seen atomic.Int32
	if _, err := c.Storage.ResumeUploads(cctx, func(int64, int64) {
		if seen.Add(1) == 1 {
			cancel()
		}
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	if res, _ := c.Storage.ResumeUploads(ctx, nil); len(res) != 0 {
		t.Fatal("released while a pack might still commit")
	}
	close(release)
	var res []UploadResult
	waitFor(10*time.Second, func() bool {
		r, _ := c.Storage.ResumeUploads(ctx, nil)
		res = append(res, r...)
		return len(r) > 0
	})
	if !allSaved(res) {
		t.Fatalf("%s", errorsOf(res))
	}
	e.assertSavedOnce(files)
}
