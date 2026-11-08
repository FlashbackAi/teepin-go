// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
	"github.com/FlashbackAi/teepin-go/teepin/internal/fakes"
)

// Cancellation is the context.Context (section 25.2): it ends a read promptly with the context's error, closes its
// responses, cuts a wait short, stops a download's windows and waits for them, and is never retried. The Node SDK's
// cancellation tests, ported; tracked is a transport that sees every response left open.

// trackedClient is a client on the fakes whose requests are tracked.
func trackedClient(t *testing.T, f *fakes.Fakes, cfg Config) (*Client, *tracked) {
	t.Helper()
	tr := newTracked()
	cfg.HTTPClient = &http.Client{Transport: tr}
	return directClient(t, f, cfg), tr
}

func noLeft(t *testing.T, tr *tracked) {
	t.Helper()
	if left := tr.running(); len(left) > 0 {
		t.Fatalf("requests left running: %v", left)
	}
}

func TestCancel_AnAbortMidReadEndsTheBodyAtOnce_ClosingItsResponse(t *testing.T) {
	f, _ := newDirect(t)
	data := blob(t, f, "big", blobSize)
	c, tr := trackedClient(t, f, Config{})
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r, err := c.Storage.GetObject(cctx, "photos", "big", nil)
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 1000)
	if _, err := io.ReadFull(r, p); err != nil || !bytes.Equal(p, data[:1000]) {
		t.Fatal(err)
	}
	t0 := time.Now()
	cancel()
	var err2 error
	for err2 == nil {
		_, err2 = r.Read(p) // what was verified before the cancel may still be handed out
	}
	if !errors.Is(err2, context.Canceled) || time.Since(t0) > time.Second {
		t.Fatalf("%v after %s", err2, time.Since(t0))
	}
	r.Close()
	noLeft(t, tr)
}

func TestCancel_ACloseFromAnotherGoroutineEndsABlockedRead(t *testing.T) {
	f, _ := newDirect(t)
	blob(t, f, "big", blobSize)
	c, tr := trackedClient(t, f, Config{})
	r, err := c.Storage.GetObject(ctx, "photos", "big", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.Network.Do(func() { f.Network.Delay = 2 * time.Second })
	errc := make(chan error, 1)
	go func() {
		_, err := r.Read(make([]byte, 10))
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	t0 := time.Now()
	r.Close()
	if err := <-errc; err == nil || time.Since(t0) > time.Second {
		t.Fatalf("%v after %s", err, time.Since(t0))
	}
	noLeft(t, tr)
}

func TestCancel_AnAbortStopsAParallelDownloadAtOnce_EveryWindowStoppedAndWaitedFor(t *testing.T) {
	f, _ := newDirect(t)
	blob(t, f, "big", blobSize)
	c, tr := trackedClient(t, f, Config{})
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var t0 atomic.Int64
	var m memFile
	_, err := c.Storage.Download(cctx, "photos", "big", &m, &DownloadOptions{Concurrency: 4, Progress: func(d, total int64) {
		if t0.CompareAndSwap(0, time.Now().UnixNano()) {
			cancel()
		}
	}})
	if !errors.Is(err, context.Canceled) || t0.Load() == 0 || time.Since(time.Unix(0, t0.Load())) > time.Second {
		t.Fatalf("%v", err)
	}
	noLeft(t, tr)
}

func TestCancel_AnAbortWhileWaitingForTheStorageNetworksAnswer(t *testing.T) {
	f, _ := newDirect(t)
	blob(t, f, "big", blobSize)
	f.Network.Do(func() { f.Network.Delay = 2 * time.Second }) // before each answer
	c, tr := trackedClient(t, f, Config{})
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var t0 atomic.Int64
	engine(c).Hooks.Open = func(ctx context.Context, r *direct.Signed) error {
		if t0.CompareAndSwap(0, time.Now().UnixNano()) {
			time.AfterFunc(50*time.Millisecond, cancel) // while the request waits for its answer
		}
		return nil
	}
	var m memFile
	_, err := c.Storage.Download(cctx, "photos", "big", &m, nil)
	if !errors.Is(err, context.Canceled) || t0.Load() == 0 || time.Since(time.Unix(0, t0.Load())) > time.Second {
		t.Fatalf("%v", err)
	}
	noLeft(t, tr)
}

func TestCancel_AnAbortDuringAWaitBetweenTriesCutsTheWaitShort(t *testing.T) {
	f, _ := newDirect(t)
	blob(t, f, "big", blobSize)
	f.Network.Do(func() { f.Network.Fail["GET"] = 100 })
	c, _ := trackedClient(t, f, Config{})
	c.sleep = func(ctx context.Context, d time.Duration) error { return sleepCtx(ctx, 10*time.Second) }
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var t0 atomic.Int64
	engine(c).Hooks.Failed = func(err error) {
		if t0.CompareAndSwap(0, time.Now().UnixNano()) {
			time.AfterFunc(50*time.Millisecond, cancel) // during the wait that follows
		}
	}
	var m memFile
	_, err := c.Storage.Download(cctx, "photos", "big", &m, nil)
	if !errors.Is(err, context.Canceled) || t0.Load() == 0 || time.Since(time.Unix(0, t0.Load())) > time.Second {
		t.Fatalf("%v after %s", err, time.Since(time.Unix(0, t0.Load())))
	}
}

func TestCancel_AnAbortIsNeverTriedAgain(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var after atomic.Int32
	engine(c).Hooks.Failed = func(err error) {
		if cctx.Err() != nil {
			after.Add(1)
		}
	}
	var m memFile
	_, err := c.Storage.Download(cctx, "photos", "big", &m, &DownloadOptions{Concurrency: 4, Progress: func(int64, int64) { cancel() }})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n := after.Load(); n > 4 {
		t.Fatalf("%d tries after the cancel: at most one for each window in flight", n)
	}
}

// The later windows wait for their answers until they are stopped: the download ends only if the window that failed
// stops them (the Python SDK's pool lets them finish; here a cancel reaches them at once, as in the Node SDK).
func TestCancel_AFailedWindowStopsTheOthersAtOnce(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	f.Damage("photos", "big", 72+65552+100) // segment 1, in the first window
	var held atomic.Int32
	engine(c).Hooks.Open = func(ctx context.Context, r *direct.Signed) error {
		if r.First < plainWindow {
			return nil
		}
		held.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		var m memFile
		_, err := c.Storage.Download(ctx, "photos", "big", &m, &DownloadOptions{Concurrency: 4})
		done <- err
	}()
	select {
	case err := <-done:
		if reasonOf(err) != "segment" || held.Load() == 0 {
			t.Fatalf("%v, %d held", err, held.Load())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the download never ended: a failed window didn't stop the others")
	}
}

func TestCancel_AReadThatEndsInsideAWindowClosesItsResponse(t *testing.T) {
	f, _ := newDirect(t)
	data := blob(t, f, "big", blobSize)
	c, tr := trackedClient(t, f, Config{})
	if got, _, err := download(c, "big", &DownloadOptions{Concurrency: 1}); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
	if got, err := getBytes(c, "big", &GetObjectOptions{Range: &ByteRange{Start: 1000, End: 1999}}); err != nil || !bytes.Equal(got, data[1000:2000]) {
		t.Fatal(err)
	}
	noLeft(t, tr)
}

func TestCancel_AStorageNetworkThatDoesntAnswerWithinTheClientsTimeoutIsAPassingFailure(t *testing.T) {
	f, _ := newDirect(t)
	blob(t, f, "big", blobSize)
	f.Network.Do(func() { f.Network.Delay = time.Second }) // before each answer
	c := directClient(t, f, Config{Timeout: 200 * time.Millisecond, MaxRetries: 1})
	t0 := time.Now()
	_, err := getBytes(c, "big", nil)
	if !IsRetryable(err) || !strings.Contains(err.Error(), "no answer from the storage network in 200ms") {
		t.Fatal(err)
	}
	if n := len(gets(f)); n != 2 {
		t.Fatalf("%d GETs: tried once more", n)
	}
	if time.Since(t0) > 900*time.Millisecond {
		t.Fatal(time.Since(t0))
	}
}

func TestCancel_ACancelledContextSendsNothing(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.Storage.GetObject(cctx, "photos", "big", nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var m memFile
	if _, err := c.Storage.Download(cctx, "photos", "big", &m, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := c.Storage.DownloadStream(cctx, "photos", "big", io.Discard, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := c.Storage.OpenObject(cctx, "photos", "big", nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if f.API.HitsOf("POST object/read") != 0 || len(gets(f)) != 0 {
		t.Fatal("a cancelled call sent a request")
	}
}

func TestCancel_AnAbortEndsAnOpenObjectsReads_AndCloseStopsItsFetches(t *testing.T) {
	f, _ := newDirect(t)
	blob(t, f, "big", blobSize)
	c, tr := trackedClient(t, f, Config{})
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	file, err := c.Storage.OpenObject(cctx, "photos", "big", &OpenObjectOptions{Prefetch: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadAt(make([]byte, 1000), 0); err != nil { // block 0, with 1 and 2 fetching ahead
		t.Fatal(err)
	}
	f.Network.Do(func() { f.Network.Delay = time.Second })
	errc := make(chan error, 1)
	go func() {
		_, err := file.ReadAt(make([]byte, 300_000), 600_000)
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := file.ReadAt(make([]byte, 10), 300_000); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	file.Close()
	noLeft(t, tr)

	f.Network.Do(func() { f.Network.Delay = 0 })
	g, err := c.Storage.OpenObject(ctx, "photos", "big", &OpenObjectOptions{Prefetch: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.ReadAt(make([]byte, 1000), 0); err != nil {
		t.Fatal(err)
	}
	f.Network.Do(func() { f.Network.Delay = time.Second }) // the fetches from here wait a second: Close must not
	go func() {
		_, err := g.ReadAt(make([]byte, 300_000), 600_000)
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	t0 := time.Now()
	g.Close()
	if err := <-errc; !errors.Is(err, fs.ErrClosed) || time.Since(t0) > 500*time.Millisecond {
		t.Fatalf("%v after %s", err, time.Since(t0))
	}
	noLeft(t, tr)
}

// A cancel is never tried again, even when it lands as a failure that would be: a 404 met as the read is cancelled
// neither signs again nor drops the cached key.
func TestCancel_ACancelThatMeetsA404NeitherSignsAgainNorDropsTheKey(t *testing.T) {
	f, c := newDirect(t)
	smallObject(t, f, "small", 5000)
	if _, _, err := download(c, "small", nil); err != nil {
		t.Fatal(err)
	}
	f.Network.DeleteBlob(f.API.Object("photos", "small").Physical) // the next GET answers 404
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var after atomic.Int32
	engine(c).Hooks.Failed = func(err error) { cancel() }
	engine(c).Hooks.Call = func(bucket, key string, off, length int64, includeKey bool) {
		if cctx.Err() != nil {
			after.Add(1)
		}
	}
	var m memFile
	if _, err := c.Storage.Download(cctx, "photos", "small", &m, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if after.Load() != 0 || engine(c).Keys.Len() != 1 {
		t.Fatalf("%d sessions asked for after the cancel, %d keys held", after.Load(), engine(c).Keys.Len())
	}
}

// A body closed between reads, its window's response open, ends that response at once.
func TestCancel_ABodyClosedBetweenReadsClosesItsResponse(t *testing.T) {
	f, _ := newDirect(t)
	data := blob(t, f, "big", blobSize)
	c, tr := trackedClient(t, f, Config{})
	r, err := c.Storage.GetObject(ctx, "photos", "big", nil)
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 1000)
	if _, err := io.ReadFull(r, p); err != nil || !bytes.Equal(p, data[:1000]) {
		t.Fatal(err)
	}
	if len(tr.running()) == 0 {
		t.Fatal("no window open mid-read")
	}
	r.Close()
	noLeft(t, tr)
}
