// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
	"github.com/FlashbackAi/teepin-go/teepin/internal/fakes"
)

// One direct-upload engine per process (shared.go; TEEPIN-STORAGE-DESIGN.md section 28.23): clients with the same
// key, address and upload settings share one queue, so a client per goroutine still fills packs. teepin-python's
// tests/test_shared_engine.py, ported.

// keyClient is a client on the fakes and the shared state directory with key, its waits skipped, closed when the test
// ends.
func (e *upEnv) keyClient(key string, cfg Config) *Client {
	e.t.Helper()
	cfg.BaseURL, cfg.APIKey, cfg.StateDir = e.f.API.URL, key, e.state
	if cfg.UploadWait == 0 {
		cfg.UploadWait = 2 * time.Minute
	}
	c := NewClient(cfg)
	c.sleep = func(ctx context.Context, d time.Duration) error { return ctx.Err() }
	e.t.Cleanup(func() { _ = c.Close() })
	return c
}

// watchClose records the closes of the shared engine c holds.
func watchClose(t *testing.T, c *Client) *[]bool {
	t.Helper()
	c.Storage.uploads()
	var closed []bool
	sharedMu.Lock()
	defer sharedMu.Unlock()
	e := c.Storage.hold.entry
	real := e.close
	e.close = func() {
		closed = append(closed, true)
		real()
	}
	return &closed
}

func putEach(t *testing.T, of func(i int) *Client, files []srcFile) {
	t.Helper()
	start := make(chan struct{})
	errs := make([]error, len(files))
	var wg sync.WaitGroup
	for i := range files {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = of(i).Storage.PutObject(ctx, "photos", files[i].key, bytes.NewReader(files[i].data),
				int64(len(files[i].data)), nil)
		}(i)
	}
	close(start)
	if !waitGroupWithin(&wg, 60*time.Second) {
		t.Fatal("a put is still waiting")
	}
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestShared_95GoroutinesFillPacksWhetherTheyShareAClientOrEachHaveOne(t *testing.T) {
	// The owner's case: a program that makes a client per goroutine. Its saves fill packs as one shared client's do.
	for _, each := range []bool{false, true} {
		t.Run(fmt.Sprintf("a client each %v", each), func(t *testing.T) {
			e, shared := newUploads(t)
			files := make([]srcFile, 95)
			clients := make([]*Client, len(files))
			for i := range files {
				files[i] = srcFile{key: fmt.Sprintf("t/%d", i), data: randomData(2000)}
				clients[i] = shared
				if each {
					clients[i] = e.client(Config{})
				}
			}
			up := clients[0].Storage.uploads()
			for _, c := range clients {
				if c.Storage.uploads() != up {
					t.Fatal("a client has an engine of its own")
				}
			}
			one := direct.PackedSize([]int64{2000})
			l := direct.DefaultLimits
			l.Floor, l.Ceiling = 20*one, 30*one
			up.Limits = l
			putEach(t, func(i int) *Client { return clients[i] }, files)
			e.assertSavedOnce(files)
			packs := e.packsWhere(anyPack)
			under := 0
			for _, p := range packs {
				if p.UnderFloor != "" {
					under++
				}
			}
			if len(packs) > 5 || under > 1 {
				t.Fatalf("packs of %v, %d under the floor", slotCounts(packs), under)
			}
		})
	}
}

func TestShared_TwoKeysNeverShareAPack(t *testing.T) {
	e, a := newUploads(t)
	b := e.keyClient(fakes.OtherKey, Config{})
	b.Storage.uploads().Sizes = testSizes
	if a.Storage.uploads() == b.Storage.uploads() || uploadsKey(a) == uploadsKey(b) {
		t.Fatal("two keys share an engine")
	}
	files := make([]srcFile, 20)
	for i := range files {
		files[i] = srcFile{key: fmt.Sprintf("k/%d", i), data: randomData(1500 + i)}
	}
	of := func(i int) *Client {
		if i%2 == 1 {
			return a
		}
		return b
	}
	putEach(t, of, files)
	e.assertSavedOnce(files)
	keyOf := map[string]string{}
	for i, f := range files {
		keyOf[f.key] = "Bearer " + of(i).apiKey
	}
	for _, p := range e.packsWhere(anyPack) {
		for _, s := range p.Slots {
			if keyOf[s.Key] != p.Auth {
				t.Fatalf("%s went in a pack reserved with another key", s.Key)
			}
		}
	}
}

func TestShared_ADifferentSettingProjectOrStateDirKeepsAnEngineOfItsOwn(t *testing.T) {
	e, base := newUploads(t)
	same := e.client(Config{})
	if same.Storage.uploads() != base.Storage.uploads() {
		t.Fatal("two clients built the same way don't share")
	}
	// The same address, written with a trailing slash, shares.
	slash := NewClient(Config{BaseURL: e.f.API.URL + "/", APIKey: fakes.APIKey, StateDir: e.state, UploadWait: 2 * time.Minute})
	t.Cleanup(func() { _ = slash.Close() })
	if slash.Storage.uploads() != base.Storage.uploads() {
		t.Fatal("the same address doesn't share")
	}
	others := []*Client{
		e.client(Config{UploadWait: 30 * time.Second}),
		e.client(Config{UploadConcurrency: 4}),
		e.client(Config{Timeout: 30 * time.Second}),
		e.client(Config{MaxRetries: -1}),
		e.client(Config{ProjectID: "p-2"}),
		e.client(Config{UserAgent: "other-app"}),
		directClient(t, e.f, Config{UploadWait: 2 * time.Minute}), // a state directory of its own
	}
	seen := map[*direct.Uploads]bool{base.Storage.uploads(): true}
	for i, c := range others {
		up := c.Storage.uploads()
		if seen[up] {
			t.Fatalf("client %d shares an engine it shouldn't", i)
		}
		seen[up] = true
	}
	if n := sharedUploadsCount(); n != 1+len(others) {
		t.Fatalf("%d shared engines", n)
	}
}

func TestShared_TheRegistryNeverHoldsTheKey(t *testing.T) {
	_, c := newUploads(t)
	k := uploadsKey(c)
	if len(k) != 64 || strings.Contains(k, fakes.APIKey) {
		t.Fatalf("registry key %q", k)
	}
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if _, ok := shared[k]; !ok {
		t.Fatal("the engine isn't registered under its digest")
	}
}

func TestShared_AnOwnHTTPClientKeepsAnEngineOfItsOwn(t *testing.T) {
	e, _ := newUploads(t)
	hc := &http.Client{Transport: defaultTransport()}
	a := e.client(Config{HTTPClient: hc})
	b := e.client(Config{HTTPClient: hc})
	if a.Storage.uploads() == b.Storage.uploads() || a.Storage.hold != nil {
		t.Fatal("a client with its own HTTP client shares")
	}
	if uploadsHTTP(a) != hc {
		t.Fatal("its uploads don't go through its own HTTP client")
	}
	data := randomData(100)
	if _, err := a.Storage.PutObject(ctx, "photos", "own", bytes.NewReader(data), 100, nil); err != nil {
		t.Fatal(err)
	}
	e.assertSavedOnce([]srcFile{{key: "own", data: data}})
}

func TestShared_OneClientClosesWhileAnothersSaveIsOnItsWay(t *testing.T) {
	e, a := newUploads(t)
	b := e.client(Config{})
	if a.Storage.uploads() != b.Storage.uploads() {
		t.Fatal("not shared")
	}
	closed := watchClose(t, a)
	e.f.Network.Do(func() { e.f.Network.Delay = time.Second }) // the pack is on its way when a closes
	files := []srcFile{{key: "c/a", data: randomData(3000)}, {key: "c/b", data: randomData(3100)}}
	done := make(chan []error, 1)
	go func() {
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i, c := range []*Client{a, b} {
			wg.Add(1)
			go func(i int, c *Client) {
				defer wg.Done()
				_, errs[i] = c.Storage.PutObject(ctx, "photos", files[i].key, bytes.NewReader(files[i].data),
					int64(len(files[i].data)), nil)
			}(i, c)
		}
		wg.Wait()
		done <- errs
	}()
	if !waitFor(20*time.Second, func() bool { return e.f.Network.HitsOf("PUT") > 0 }) {
		t.Fatal("the pack never left")
	}
	_ = a.Close()
	if len(*closed) != 0 {
		t.Fatal("a's close closed the engine b still holds")
	}
	select {
	case errs := <-done:
		for _, err := range errs {
			if err != nil {
				t.Fatal(err) // a's own save, in the pack already, finishes too
			}
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the saves never finished")
	}
	e.assertSavedOnce(files)
	if n := len(e.packsWhere(anyPack)); n != 1 {
		t.Fatalf("%d packs", n)
	}
	_ = b.Close()
	if len(*closed) != 1 {
		t.Fatal("the last close didn't close the engine")
	}
}

func TestShared_TheLastCloseShutsTheEngineAndNoOther(t *testing.T) {
	e, a := newUploads(t)
	b := e.client(Config{})
	other := e.keyClient(fakes.OtherKey, Config{})
	if a.Storage.uploads() != b.Storage.uploads() {
		t.Fatal("not shared")
	}
	shared, separate := watchClose(t, a), watchClose(t, other)
	_ = a.Close()
	_ = a.Close() // a second close lets go of nothing more
	if len(*shared) != 0 || sharedUploadsCount() != 2 {
		t.Fatalf("closed %v, %d engines", *shared, sharedUploadsCount())
	}
	_ = b.Close()
	if len(*shared) != 1 || len(*separate) != 0 || sharedUploadsCount() != 1 {
		t.Fatalf("closed %v and %v, %d engines", *shared, *separate, sharedUploadsCount())
	}
	data := randomData(200)
	if _, err := other.Storage.PutObject(ctx, "photos", "still", bytes.NewReader(data), 200, nil); err != nil {
		t.Fatal(err)
	}
	e.assertSavedOnce([]srcFile{{key: "still", data: data}})
}

func TestShared_AClosedClientUsedAgainTakesTheEngineAgain(t *testing.T) {
	e, c := newUploads(t)
	first := c.Storage.uploads()
	_ = c.Close()
	data := randomData(50)
	if _, err := c.Storage.PutObject(ctx, "photos", "again", bytes.NewReader(data), 50, nil); err != nil {
		t.Fatal(err)
	}
	if c.Storage.uploads() == first || sharedUploadsCount() != 1 {
		t.Fatal("the closed client didn't take an engine again")
	}
	e.assertSavedOnce([]srcFile{{key: "again", data: data}})
}

func TestShared_ACollectedClientLetsGo(t *testing.T) {
	e, _ := newUploads(t)
	cfg := Config{BaseURL: e.f.API.URL, APIKey: fakes.APIKey, StateDir: t.TempDir(), UploadWait: 2 * time.Minute}
	keep := NewClient(cfg)
	gone := NewClient(cfg)
	if keep.Storage.uploads() != gone.Storage.uploads() {
		t.Fatal("not shared")
	}
	closed := watchClose(t, keep)
	engines := sharedUploadsCount() // newUploads's own engine, and this one
	gone = nil
	_ = gone
	collectUntil(func() bool { return false })
	sharedMu.Lock()
	still, n := len(*closed), len(shared)
	sharedMu.Unlock()
	if still != 0 || n != engines {
		t.Fatalf("closed %d times with keep alive; %d engines", still, n)
	}
	runtime.KeepAlive(keep)
	keep = nil
	_ = keep
	if !collectUntil(func() bool {
		sharedMu.Lock()
		defer sharedMu.Unlock()
		return len(*closed) == 1 && len(shared) == engines-1
	}) {
		t.Fatal("the last collected client didn't let go")
	}
}

// collectUntil runs the collector until cond holds (cleanups run after a collection, on their own goroutine), for up to
// about a second.
func collectUntil(cond func() bool) bool {
	for range 100 {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		if cond() {
			return true
		}
	}
	return cond()
}
