// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
	"github.com/FlashbackAi/teepin-go/teepin/internal/fakes"
)

// The key cache, batch read sessions, OpenObject and the old path: the rest of test_direct_reads.py and reads.test.mjs.

// --- the key cache ----------------------------------------------------------------------

// includeKeyCalls records whether each read session asked for the key.
func includeKeyCalls(c *Client) func() []bool {
	var mu sync.Mutex
	var calls []bool
	engine(c).Hooks.Call = func(bucket, key string, off, length int64, includeKey bool) {
		mu.Lock()
		calls = append(calls, includeKey)
		mu.Unlock()
	}
	return func() []bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(calls)
	}
}

func TestDirect_ACachedKeyIsNotSentAgain(t *testing.T) {
	f, c := newDirect(t)
	data := smallObject(t, f, "small", 5000)
	calls := includeKeyCalls(c)
	for range 2 {
		if got, _, err := download(c, "small", nil); err != nil || !bytes.Equal(got, data) {
			t.Fatal(err)
		}
	}
	if got := calls(); !slices.Equal(got, []bool{true, false}) {
		t.Fatal(got)
	}
}

// A negative KeyCacheTTL keeps no key: every read fetches its key, and none is held between reads. Zero is the
// default, 10 minutes (the Python and Node SDKs' 0, which Go's zero value can't be; section 25.1).
func TestDirect_AKeyCacheTTLBelowZeroKeepsNoKey(t *testing.T) {
	f, _ := newDirect(t)
	for _, cfg := range []struct {
		ttl, want time.Duration
	}{{0, 10 * time.Minute}, {-1, 0}, {30 * time.Second, 30 * time.Second}} {
		if c := directClient(t, f, Config{KeyCacheTTL: cfg.ttl}); c.keyCacheTTL != cfg.want || engine(c).Keys.TTL() != cfg.want {
			t.Errorf("KeyCacheTTL %v keeps keys %v, want %v", cfg.ttl, c.keyCacheTTL, cfg.want)
		}
	}
	c := directClient(t, f, Config{KeyCacheTTL: -1})
	data := smallObject(t, f, "small", 5000)
	calls := includeKeyCalls(c)
	for range 2 {
		if got, _, err := download(c, "small", nil); err != nil || !bytes.Equal(got, data) {
			t.Fatal(err)
		}
	}
	if got := calls(); !slices.Equal(got, []bool{true, true}) || engine(c).Keys.Len() != 0 {
		t.Fatalf("%v, %d held", got, engine(c).Keys.Len())
	}
}

func TestDirect_ANewVersionFetchesItsKey(t *testing.T) {
	f, c := newDirect(t)
	smallObject(t, f, "small", 5000)
	calls := includeKeyCalls(c)
	if _, _, err := download(c, "small", nil); err != nil {
		t.Fatal(err)
	}
	data := smallObject(t, f, "small", 5000) // replaced: a new version
	if got, _, err := download(c, "small", nil); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
	if got := calls(); !slices.Equal(got, []bool{true, false, true}) {
		t.Fatal(got)
	}
}

func TestDirect_AnExpiredKeyIsZeroedAndFetchedAgain(t *testing.T) {
	f, _ := newDirect(t)
	c := directClient(t, f, Config{KeyCacheTTL: 30 * time.Second})
	smallObject(t, f, "small", 5000)
	calls := includeKeyCalls(c)
	if _, _, err := download(c, "small", nil); err != nil {
		t.Fatal(err)
	}
	cache := engine(c).Keys
	if cache.TTL() != 30*time.Second {
		t.Fatal(cache.TTL())
	}
	k := cache.Held()[0]
	now := time.Now()
	cache.Now = func() time.Time { return now.Add(31 * time.Second) } // 31 seconds later
	if _, _, err := download(c, "small", nil); err != nil {
		t.Fatal(err)
	}
	if got := calls(); !slices.Equal(got, []bool{true, true}) || !isZeroKey(k) {
		t.Fatalf("%v, zeroed %v", got, isZeroKey(k))
	}
}

func TestDirect_CloseZeroesTheCachedKeys(t *testing.T) {
	f, c := newDirect(t)
	smallObject(t, f, "small", 5000)
	if _, _, err := download(c, "small", nil); err != nil {
		t.Fatal(err)
	}
	cache := engine(c).Keys
	k := cache.Held()[0]
	if isZeroKey(k) {
		t.Fatal("a zero key")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if !isZeroKey(k) || cache.Len() != 0 {
		t.Fatal("close kept the key")
	}
	if _, _, err := download(c, "small", nil); err != nil {
		t.Fatalf("the client still works after Close: %v", err)
	}
}

func TestDirect_AReadDropsItsOwnCopyOfTheKey(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	var mu sync.Mutex
	var copies [][]byte
	engine(c).Hooks.Plaintext = func(s *direct.Session, dek []byte, a, b int64) {
		mu.Lock()
		copies = append(copies, dek)
		mu.Unlock()
	}
	r, err := c.Storage.GetObject(ctx, "photos", "big", nil)
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 10)
	if _, err := io.ReadFull(r, p); err != nil || !bytes.Equal(p, data[:10]) {
		t.Fatal(err)
	}
	r.Close() // closed mid-read: zeroed
	if len(copies) == 0 || !isZeroKey(copies[0]) {
		t.Fatal("a closed read kept its key")
	}
	if _, _, err := download(c, "big", &DownloadOptions{Concurrency: 4}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(copies) < 3 {
		t.Fatalf("%d copies", len(copies))
	}
	for _, k := range copies {
		if !isZeroKey(k) {
			t.Fatal("a read kept its key")
		}
	}
}

func TestDirect_ABodyClosedUnreadFetchesNothing(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	r, err := c.Storage.GetObject(ctx, "photos", "big", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read(make([]byte, 1)); err == nil {
		t.Fatal("a read after Close")
	}
	if len(gets(f)) != 0 {
		t.Fatal("a body never read was fetched")
	}
}

// --- batch read sessions --------------------------------------------------------------

func TestDirect_BatchSessionsSignManyObjects_EachRefusalOnItsOwn_TheKeysCached(t *testing.T) {
	f, c := newDirect(t)
	smallObject(t, f, "small", 5000)
	f.Direct.PutServerObject("photos", "server", randomData(100))
	f.API.PutObject("photos", "staged", []byte("x"), "")
	e := engine(c)
	out, err := e.Sessions(ctx, "photos", []string{"small", "server", "staged", "nope", "small"})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(out))
	for k := range out {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"nope", "server", "small", "staged"}) {
		t.Fatal(keys)
	}
	if s := out["small"].Session; s == nil || len(s.DEK) != 32 || !s.Packed() {
		t.Fatalf("small: %+v", out["small"])
	}
	if s := out["server"].Session; s == nil || s.Format != direct.FormatServer {
		t.Fatalf("server: %+v", out["server"])
	}
	if s := out["staged"].Session; s == nil || !s.Staged() {
		t.Fatalf("staged: %+v", out["staged"])
	}
	if err := out["nope"].Err; !IsNotFound(err) || ErrorCode(err) != "object_not_found" {
		t.Fatalf("nope: %v", err)
	}
	if e.Keys.Len() != 2 {
		t.Fatalf("%d keys cached", e.Keys.Len())
	}
	for _, it := range out {
		if it.Session != nil {
			it.Session.Forget()
		}
	}
	f.API.Do(func() {
		f.API.Canned["POST objects/read"] = []fakes.Canned{{Status: 200, Body: map[string]any{"objects": []any{
			map[string]any{"key": "gone", "error": map[string]any{"code": "key_erased", "error": "key erased"}}}}}}
	})
	odd, err := e.Sessions(ctx, "photos", []string{"gone", "lost"})
	if err != nil {
		t.Fatal(err)
	}
	var ae *APIError
	if gone := odd["gone"].Err; !IsKeyErased(gone) || !IsNotFound(gone) || !errors.Is(gone, ErrKeyErased) || !errors.As(gone, &ae) || ae.StatusCode != 410 {
		t.Fatalf("gone: %v", gone)
	}
	if lost := odd["lost"].Err; !IsNotFound(lost) || !bytes.Contains([]byte(lost.Error()), []byte("missing from the read batch")) {
		t.Fatalf("lost: %v", lost)
	}
}

func TestDirect_BatchSessionsSign1000ObjectsToARequest(t *testing.T) {
	f, c := newDirect(t)
	keys := make([]string, 1001)
	for i := range keys {
		keys[i] = fmt.Sprintf("missing-%d", i)
	}
	out, err := engine(c).Sessions(ctx, "photos", keys)
	if err != nil || len(out) != 1001 || f.API.HitsOf("POST objects/read") != 2 {
		t.Fatalf("%v: %d answers in %d requests", err, len(out), f.API.HitsOf("POST objects/read"))
	}
}

// --- OpenObject ----------------------------------------------------------------------

// fetches records each block f fetches.
func fetches(file *ObjectFile) func() [][2]int64 {
	var mu sync.Mutex
	var seen [][2]int64
	real := file.blocks.Fetch
	file.blocks.Fetch = func(ctx context.Context, a, b int64) ([]byte, error) {
		mu.Lock()
		seen = append(seen, [2]int64{a, b})
		mu.Unlock()
		return real(ctx, a, b)
	}
	return func() [][2]int64 {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(seen)
	}
}

func readThrough(t *testing.T, r io.Reader, n int) []byte {
	t.Helper()
	var out []byte
	p := make([]byte, n)
	for {
		k, err := r.Read(p)
		out = append(out, p[:k]...)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestOpenObject_ReadsStraightOnAWindowAtATime(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	file, err := c.Storage.OpenObject(ctx, "photos", "big", &OpenObjectOptions{Prefetch: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	seen := fetches(file)
	if file.Size() != blobSize || file.Info.ETag != sha256Hex(data) || file.Info.ContentType != "video/mp4" {
		t.Fatalf("%+v", file.Info)
	}
	if got := readThrough(t, file, 50_000); !bytes.Equal(got, data) {
		t.Fatal("read wrong")
	}
	if pos, _ := file.Seek(0, io.SeekCurrent); pos != blobSize {
		t.Fatal(pos)
	}
	if n, err := file.Read(make([]byte, 10)); n != 0 || err != io.EOF {
		t.Fatal(n, err)
	}
	var want [][2]int64
	for a := int64(0); a < blobSize; a += plainWindow {
		want = append(want, [2]int64{a, min(a+plainWindow, blobSize)})
	}
	if got := seen(); !slices.Equal(got, want) {
		t.Fatalf("fetched %v: each window once", got)
	}
	if h := file.blocks.Held(); len(h) > 1+2 {
		t.Fatal(h)
	}
}

func TestOpenObject_ReadsElsewhereFetchOnlyWhatTheyNeed(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	file, err := c.Storage.OpenObject(ctx, "photos", "big", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	seen := fetches(file)
	if _, err := file.Seek(-100, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	if got := readThrough(t, file, 100); !bytes.Equal(got, data[blobSize-100:]) {
		t.Fatal("the end")
	}
	p := make([]byte, 10)
	if n, err := file.ReadAt(p, 500_000); n != 10 || err != nil || !bytes.Equal(p, data[500_000:500_010]) {
		t.Fatal(n, err)
	}
	if _, err := file.Seek(500_015, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	q := make([]byte, 3)
	if n, err := file.Read(q); n != 3 || err != nil || !bytes.Equal(q, data[500_015:500_018]) {
		t.Fatal(n, err)
	}
	got := seen()
	for _, s := range got {
		if s[1]-s[0] > 2*65536 {
			t.Fatalf("fetched %v: whole segments, not windows", got)
		}
	}
	if len(got) != 3 {
		t.Fatalf("fetched %v", got)
	}
}

func TestOpenObject_ReadsAPackedObjectOnce(t *testing.T) {
	f, c := newDirect(t)
	data := smallObject(t, f, "small", 40_000)
	file, err := c.Storage.OpenObject(ctx, "photos", "small", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	seen := fetches(file)
	for _, at := range []int64{39_000, 0, 20_000, 100} {
		p := make([]byte, 500)
		n, err := file.ReadAt(p, at)
		want := data[at:min(at+500, 40_000)]
		if !bytes.Equal(p[:n], want) || (err != nil && err != io.EOF) {
			t.Fatalf("at %d: %v", at, err)
		}
	}
	got := seen()
	if len(got) > 2 || slices.ContainsFunc(got, func(s [2]int64) bool { return s != [2]int64{0, 40_000} }) {
		t.Fatalf("fetched %v", got)
	}
}

func TestOpenObject_WorksAsAFileForZip(t *testing.T) {
	f, c := newDirect(t)
	var buf bytes.Buffer
	z := zip.NewWriter(&buf)
	members := map[string][]byte{}
	for i := range 8 {
		name := fmt.Sprintf("m%d.bin", i)
		members[name] = randomData(90_000 + i)
		w, _ := z.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Store})
		_, _ = w.Write(members[name])
	}
	_ = z.Close()
	if err := f.PutBlob("photos", "archive.zip", buf.Bytes(), blobPart, "application/zip", nil); err != nil {
		t.Fatal(err)
	}
	file, err := c.Storage.OpenObject(ctx, "photos", "archive.zip", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	zr, err := zip.NewReader(file, file.Size())
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, m := range zr.File {
		names = append(names, m.Name)
	}
	slices.Sort(names)
	if len(names) != 8 || names[0] != "m0.bin" {
		t.Fatal(names)
	}
	rc, err := zr.Open("m5.bin")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(got, members["m5.bin"]) {
		t.Fatal(err)
	}
}

func TestOpenObject_ReadsAnywhereAloneOrAtOnce(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	file, err := c.Storage.OpenObject(ctx, "photos", "big", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	r := rand.New(rand.NewPCG(1, 2))
	pick := func() (int64, int) { return r.Int64N(blobSize), r.IntN(300_000) }
	check := func(at int64, n int) error {
		p := make([]byte, n)
		k, err := file.ReadAt(p, at)
		if err != nil && err != io.EOF {
			return err
		}
		if want := data[at:min(at+int64(n), blobSize)]; !bytes.Equal(p[:k], want) {
			return fmt.Errorf("at %d: %d bytes, want %d", at, k, len(want))
		}
		return nil
	}
	for range 20 {
		if err := check(pick()); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for range 8 {
		at, n := pick()
		wg.Go(func() {
			if err := check(at, n); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if h := file.blocks.Held(); len(h) > 2+2 {
		t.Fatal(h)
	}
	if _, err := file.ReadAt(make([]byte, 1), -1); err == nil {
		t.Fatal("a negative offset")
	}
}

func TestOpenObject_StopsOnAReplacedObject(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	file, err := c.Storage.OpenObject(ctx, "photos", "big", &OpenObjectOptions{Prefetch: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.ReadAt(make([]byte, 1000), 0); err != nil {
		t.Fatal(err)
	}
	f.API.Do(func() { f.API.Bucket("photos")["big"].ID = beef })
	if _, err := file.ReadAt(make([]byte, 400_000), 800_000); !errors.Is(err, ErrObjectChanged) {
		t.Fatal(err)
	}
}

func TestOpenObject_ZeroesItsKeyWhenClosed(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	var mu sync.Mutex
	var keys [][]byte
	engine(c).Hooks.Plaintext = func(s *direct.Session, dek []byte, a, b int64) {
		mu.Lock()
		keys = append(keys, dek)
		mu.Unlock()
	}
	file, err := c.Storage.OpenObject(ctx, "photos", "big", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReadAt(make([]byte, 10), 0); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	k := keys[0]
	mu.Unlock()
	if isZeroKey(k) {
		t.Fatal("a zero key")
	}
	file.Close()
	if !isZeroKey(k) {
		t.Fatal("close kept the key")
	}
	if _, err := file.ReadAt(make([]byte, 1), 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatal(err)
	}
	if _, err := file.Read(make([]byte, 1)); !errors.Is(err, fs.ErrClosed) {
		t.Fatal(err)
	}
}

func TestOpenObject_FetchesAheadOfTheReader(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	file, err := c.Storage.OpenObject(ctx, "photos", "big", &OpenObjectOptions{Prefetch: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.ReadAt(make([]byte, 1000), 0); err != nil {
		t.Fatal(err)
	}
	held := file.blocks.Held()
	slices.Sort(held)
	if !slices.Equal(held, []int64{0, 1, 2}) {
		t.Fatalf("held %v: the window read and the next two, already fetching", held)
	}
	if got, err := file.blocks.Wait(2); err != nil || len(got) != plainWindow {
		t.Fatalf("%d %v: it lands without the reader asking", len(got), err)
	}
}

func TestOpenObject_FetchesAFailedWindowAgain(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	file, err := c.Storage.OpenObject(ctx, "photos", "big", &OpenObjectOptions{Prefetch: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	real := file.blocks.Fetch
	var calls atomic.Int32
	file.blocks.Fetch = func(ctx context.Context, a, b int64) ([]byte, error) {
		if calls.Add(1) == 1 {
			return nil, &transportError{Err: errors.New("gone")}
		}
		return real(ctx, a, b)
	}
	p := make([]byte, 10)
	if _, err := file.Read(p); !IsRetryable(err) {
		t.Fatal(err)
	}
	if pos, _ := file.Seek(0, io.SeekCurrent); pos != 0 {
		t.Fatal(pos)
	}
	if n, err := file.Read(p); n != 10 || err != nil || !bytes.Equal(p, data[:10]) {
		t.Fatal(n, err)
	}
	if calls.Load() != 2 {
		t.Fatal(calls.Load())
	}
}

func TestDirect_ErrorsMapToTheOtherSDKsNames(t *testing.T) {
	sum := &DataIntegrityError{Reason: "checksum", Message: "x"}
	if !errors.Is(sum, ErrChecksumMismatch) || errors.Is(&DataIntegrityError{Reason: "segment"}, ErrChecksumMismatch) {
		t.Fatal("ChecksumMismatchError is a DataIntegrityError with reason checksum")
	}
	erased := &APIError{StatusCode: http.StatusGone, Code: "key_erased"}
	if !IsKeyErased(erased) || !IsNotFound(erased) || !errors.Is(erased, ErrKeyErased) || IsKeyErased(&APIError{StatusCode: 404}) {
		t.Fatal("KeyErasedError is a NotFoundError")
	}
	if !IsRetryable(&transportError{Err: errors.New("reset")}) {
		t.Fatal("a connection error passes")
	}
}
