// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"io/fs"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// The read engine's parts that need no network: the key cache, sessions, the split into windows, the formats'
// checks, and the block reader behind ObjectFile. The reads themselves are tested against the fakes in package
// teepin (direct_reads_test.go).

func isZero(b []byte) bool { return !slices.ContainsFunc(b, func(x byte) bool { return x != 0 }) }

func fill(n int, v byte) []byte { return bytes.Repeat([]byte{v}, n) }

func TestKeyCache_HandsOutCopies_AndZeroesItsOwnOnReplacementDropClearAndExpiry(t *testing.T) {
	ones := fill(32, 1)
	c := NewKeyCache(time.Minute)
	c.Put("b", "k", "v1", ones)
	_, k, _ := c.Get("b", "k")
	clear(k) // a copy
	if v, k, ok := c.Get("b", "k"); !ok || v != "v1" || !bytes.Equal(k, fill(32, 1)) {
		t.Fatalf("%s %x %v", v, k, ok)
	}
	if !bytes.Equal(ones, fill(32, 1)) {
		t.Fatal("the caller's copy was touched")
	}
	held1 := c.Held()[0]
	c.Put("b", "k", "v1", fill(32, 2)) // the same version: kept
	if _, k, _ := c.Get("b", "k"); k[0] != 1 {
		t.Fatal("the same version replaced")
	}
	c.Put("b", "k", "v2", fill(32, 3)) // a new version replaces it
	if v, _, _ := c.Get("b", "k"); !isZero(held1) || v != "v2" || c.Len() != 1 {
		t.Fatal("replacement")
	}
	held2 := c.Held()[0]
	c.Drop("b", "k")
	if !isZero(held2) || c.Len() != 0 {
		t.Fatal("drop")
	}
	c.Put("a/b", "c", "v", ones)
	if _, _, ok := c.Get("a", "b/c"); ok {
		t.Fatal("names ran together")
	}
	c.Put("b", "x", "v", ones)
	held := c.Held()
	c.Clear()
	if len(held) != 2 || !isZero(held[0]) || !isZero(held[1]) || c.Len() != 0 {
		t.Fatal("clear")
	}
	c.Put("b", "k", "v", ones)
	held3 := c.Held()[0]
	now := time.Now()
	c.Now = func() time.Time { return now.Add(time.Minute + time.Millisecond) }
	if _, _, ok := c.Get("b", "k"); ok || !isZero(held3) || c.Len() != 0 {
		t.Fatal("expiry")
	}
	off := NewKeyCache(0)
	off.Put("b", "k", "v", ones)
	if _, _, ok := off.Get("b", "k"); ok || off.Len() != 0 {
		t.Fatal("a cache of no time kept a key")
	}
}

func TestSession_AFormatNobodyKnowsIsRefused(t *testing.T) {
	_, err := NewSession("photos", []byte(`{"key":"k","format":"zip","source":"storage"}`))
	var die *errs.DataIntegrityError
	if !errors.As(err, &die) || die.Reason != errs.ReasonFormat {
		t.Fatal(err)
	}
	// Staged objects are read through the API, whatever their format.
	s, err := NewSession("photos", []byte(`{"key":"k","format":"plain","source":"api","size":3}`))
	if err != nil || !s.Staged() || s.NeedsKey() {
		t.Fatalf("%+v %v", s, err)
	}
}

func TestSession_ReadsItsJSON(t *testing.T) {
	raw := `{"key":"k","version":"00000000-0000-7000-8000-000000000001","size":5,"stored_size":157,"etag":"\"abc-3\"",
		"sha256":"abc","content_type":"text/plain","metadata":{"a":"b"},"format":"tws1","source":"storage",
		"uploaded_at":"2026-11-06T10:00:00Z","key_material":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
		"requests":[{"method":"GET","url":"http://x/b/p","headers":{"range":"bytes=0-156","n":7},
		"expires_at":"2026-11-06T10:15:00Z","prefix":64,"stored_first":0,"stored_last":156}]}`
	s, err := NewSession("photos", []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	w := s.Window()
	if s.ETag != "abc-3" || s.PartCount() != 3 || s.Checkable() || !s.Packed() || !s.NeedsKey() || len(s.DEK) != 32 ||
		s.DEK[31] != 31 || w.Length() != 64+157 || w.Header["n"] != "7" || w.Expires.IsZero() || s.UploadedAt.IsZero() {
		t.Fatalf("%+v", s)
	}
	dek := s.DEK
	s.Forget()
	if s.DEK != nil || !isZero(dek) {
		t.Fatal("forget")
	}
	if _, err := NewSession("b", []byte(`{"key":"k","format":"tws1","key_material":"not base64!"}`)); err == nil {
		t.Fatal("a key that isn't base64")
	}
}

func session(t *testing.T, format string, size, stored, first, last, prefix int64) *Session {
	t.Helper()
	return &Session{Key: "k", Format: format, Size: size, StoredSize: stored, Source: SourceStorage,
		Requests: []Signed{{First: first, Last: last, Prefix: prefix}}}
}

func TestRanges_OneRangePerServerWindow_OrOneForAnObjectReadWhole(t *testing.T) {
	r := &Reads{}
	size := int64(1_000_003)
	s := session(t, FormatTWS1, size, tws1.StoredSize(size), 0, 299_999, 0)
	var want [][2]int64
	for a := int64(0); a < size; a += 262_144 {
		want = append(want, [2]int64{a, min(a+262_144, size)})
	}
	if got := r.Ranges(s, 0); !slices.Equal(got, want) {
		t.Fatalf("%v", got)
	}
	if got := r.Ranges(s, 100_000); got[0] != [2]int64{0, 65536} {
		t.Fatalf("a part size splits in whole segments: %v", got[:2])
	}
	if got := r.Ranges(s, 1); got[0] != [2]int64{0, 65536} {
		t.Fatalf("a part size under a segment: %v", got[:2])
	}
	plain := session(t, FormatPlain, 700_000, 700_000, 0, 299_999, 0)
	if got := r.Ranges(plain, 100_000); len(got) != 7 || got[6] != [2]int64{600_000, 700_000} {
		t.Fatalf("plain: %v", got)
	}
	for _, s := range []*Session{
		session(t, FormatTWS1, 5000, 5100, 0, 5099, 64),                  // packed
		session(t, FormatServer, 5000, 5028, 0, 5027, 0),                 // teepin-v1
		session(t, FormatTWS1, 5000, tws1.StoredSize(5000), 0, 99999, 0), // one window holds it
	} {
		if got := r.Ranges(s, 0); !slices.Equal(got, [][2]int64{{0, 5000}}) {
			t.Fatalf("%v", got)
		}
	}
	if got := r.Ranges(&Session{Size: 0}, 0); !slices.Equal(got, [][2]int64{{0, 0}}) {
		t.Fatalf("nothing signed: %v", got)
	}
}

func TestAligned_AsksForWholeSegmentsWithinTheWindow(t *testing.T) {
	cases := []struct{ start, end, limit, want int64 }{
		{0, 1 << 30, DefaultWindow, tws1.HeaderSize + (DefaultWindow-tws1.HeaderSize)/tws1.StoredSegmentSize*tws1.StoredSegmentSize},
		{0, 1 << 30, 100, tws1.HeaderSize + tws1.StoredSegmentSize}, // at least one segment
		{72, 1 << 30, 300_000, 4 * tws1.StoredSegmentSize},
		{72, 1000, 300_000, 928}, // at most to the end
	}
	for _, c := range cases {
		if got := aligned(c.start, c.end, c.limit); got != c.want {
			t.Errorf("aligned(%d, %d, %d) = %d, want %d", c.start, c.end, c.limit, got, c.want)
		}
	}
}

func reason(err error) string {
	var die *errs.DataIntegrityError
	if !errors.As(err, &die) {
		return ""
	}
	return die.Reason
}

func sealServer(key, plain, ad []byte) []byte {
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	nonce := make([]byte, 12)
	_, _ = rand.Read(nonce)
	return gcm.Seal(nonce, nonce, plain, ad)
}

func TestOpenServerV1_OpensAndRefusesDamage(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	ad := []byte("teepin/object/v1/account/b-photos/object")
	plain := make([]byte, 1000)
	_, _ = rand.Read(plain)
	sealed := sealServer(key, plain, ad)
	if got, err := OpenServerV1(key, sealed, ad); err != nil || !bytes.Equal(got, plain) {
		t.Fatal(err)
	}
	if got, err := OpenServerV1(key, sealServer(key, nil, ad), ad); err != nil || len(got) != 0 {
		t.Fatal(err)
	}
	flipped := bytes.Clone(sealed)
	flipped[20] ^= 1
	other := make([]byte, 32)
	_, _ = rand.Read(other)
	for _, c := range []struct {
		key, stored, ad []byte
		reason          string
	}{
		{key, flipped, ad, errs.ReasonSegment},
		{key, sealed, []byte("other"), errs.ReasonSegment},
		{other, sealed, ad, errs.ReasonSegment},
		{key[:16], sealed, ad, errs.ReasonFormat},
		{key, sealed[:27], ad, errs.ReasonTruncated},
	} {
		if _, err := OpenServerV1(c.key, c.stored, c.ad); reason(err) != c.reason {
			t.Errorf("%v, want reason %s", err, c.reason)
		}
	}
}

func TestTWS1Checks_GiveThePythonReasons(t *testing.T) {
	dek, _ := tws1.NewDataKey()
	data := make([]byte, 3*tws1.SegmentSize+5)
	_, _ = rand.Read(data)
	stored, _ := tws1.Encrypt(dek, data)
	size := int64(len(data))
	head := stored[:tws1.HeaderSize]
	if err := checkHeader(dek, head, size); err != nil {
		t.Fatal(err)
	}
	other, _ := tws1.NewDataKey()
	if r := reason(checkHeader(other, head, size)); r != errs.ReasonCommitment {
		t.Fatal(r)
	}
	bad := bytes.Clone(head)
	bad[4] = 9
	if r := reason(checkHeader(dek, bad, size)); r != errs.ReasonFormat {
		t.Fatal(r)
	}
	for i := int64(0); i < tws1.Segments(size); i++ {
		off, n := segmentAt(size, i)
		got, err := openSegment(dek, head, size, i, stored[off:off+n])
		if err != nil || !bytes.Equal(got, data[i*tws1.SegmentSize:min((i+1)*tws1.SegmentSize, size)]) {
			t.Fatalf("segment %d: %v", i, err)
		}
	}
	off, n := segmentAt(size, 1)
	seg := bytes.Clone(stored[off : off+n])
	seg[7] ^= 1
	if _, err := openSegment(dek, head, size, 1, seg); reason(err) != errs.ReasonSegment {
		t.Fatal(err)
	}
	// The last segment's flag: segment 3 opened as if segment 2 were the last fails.
	off, n = segmentAt(size, 2)
	if _, err := openSegment(dek, head, 2*tws1.SegmentSize+1, 2, stored[off:off+n]); reason(err) != errs.ReasonSegment {
		t.Fatal(err)
	}
	if _, err := openSegment(dek, head, size, 1, stored[off:off+5]); reason(err) == "" {
		t.Fatal("a cut segment opened")
	}
}

// fetcher is a fetch of a known object's bytes that records each call.
type fetcher struct {
	data  []byte
	mu    sync.Mutex
	calls [][2]int64
	fail  atomic.Int32 // fail this many calls
	hold  chan struct{}
}

func (f *fetcher) fetch(ctx context.Context, a, b int64) ([]byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, [2]int64{a, b})
	f.mu.Unlock()
	if f.fail.Add(-1) >= 0 {
		return nil, errs.Connection("gone")
	}
	if f.hold != nil {
		select {
		case <-f.hold:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return bytes.Clone(f.data[a:b]), nil
}

func (f *fetcher) seen() [][2]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func newFetcher(n int) *fetcher {
	d := make([]byte, n)
	_, _ = rand.Read(d)
	return &fetcher{data: d}
}

func TestBlocks_ReadStraightOnABlockAtATime_AndJumpsFetchOnlyWhatTheyNeed(t *testing.T) {
	f := newFetcher(1000)
	b := NewBlocks(context.Background(), f.fetch, 1000, 300, 10, 1, nil)
	defer b.Close()
	var got []byte
	p := make([]byte, 70)
	for off := int64(0); ; {
		n, err := b.ReadAt(p, off)
		got = append(got, p[:n]...)
		off += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, f.data) {
		t.Fatal("read wrong")
	}
	if seen := f.seen(); !slices.Equal(seen, [][2]int64{{0, 300}, {300, 600}, {600, 900}, {900, 1000}}) {
		t.Fatalf("fetches %v", seen)
	}
	if h := b.Held(); len(h) > 3 {
		t.Fatalf("held %v", h)
	}
	n, err := b.ReadAt(p, 1000)
	if n != 0 || err != io.EOF {
		t.Fatalf("past the end: %d %v", n, err)
	}
	g := NewBlocks(context.Background(), f.fetch, 1000, 300, 10, 1, nil)
	defer g.Close()
	f.calls = nil
	q := make([]byte, 5)
	if n, err := g.ReadAt(q, 503); n != 5 || err != nil || !bytes.Equal(q, f.data[503:508]) {
		t.Fatalf("a jump: %d %v", n, err)
	}
	if seen := f.seen(); !slices.Equal(seen, [][2]int64{{500, 510}}) {
		t.Fatalf("a jump fetched %v", seen)
	}
}

func TestBlocks_AFailedBlockIsFetchedAgain_AndCloseStopsFetchesThenRunsOnClose(t *testing.T) {
	f := newFetcher(1000)
	f.fail.Store(1)
	var closed atomic.Bool
	b := NewBlocks(context.Background(), f.fetch, 1000, 300, 10, 0, func() { closed.Store(true) })
	p := make([]byte, 10)
	var ce *errs.ConnError
	if _, err := b.ReadAt(p, 0); !errors.As(err, &ce) {
		t.Fatalf("the fetch's failure: %v", err)
	}
	if n, err := b.ReadAt(p, 0); n != 10 || err != nil {
		t.Fatalf("again: %d %v", n, err)
	}
	f.hold = make(chan struct{})
	errc := make(chan error, 1)
	go func() {
		_, err := b.ReadAt(make([]byte, 10), 600)
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	t0 := time.Now()
	_ = b.Close()
	if err := <-errc; !errors.Is(err, fs.ErrClosed) || time.Since(t0) > time.Second || !closed.Load() {
		t.Fatalf("%v after %s, closed %v", err, time.Since(t0), closed.Load())
	}
	if _, err := b.ReadAt(p, 0); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	_ = b.Close() // twice is fine
}

func TestBlocks_TheirContextEndsTheirReads(t *testing.T) {
	f := newFetcher(1000)
	f.hold = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBlocks(ctx, f.fetch, 1000, 300, 10, 2, nil)
	defer b.Close()
	errc := make(chan error, 1)
	go func() {
		_, err := b.ReadAt(make([]byte, 10), 0)
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := b.ReadAt(make([]byte, 1), 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestBlocks_ConcurrentReadsAtRandomPlaces(t *testing.T) {
	f := newFetcher(100_000)
	b := NewBlocks(context.Background(), f.fetch, 100_000, 7000, 100, 2, nil)
	defer b.Close()
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			off := int64(i * 6113 % 100_000)
			p := make([]byte, 9000)
			n, err := b.ReadAt(p, off)
			if (err != nil && err != io.EOF) || !bytes.Equal(p[:n], f.data[off:off+int64(n)]) {
				t.Errorf("at %d: %d %v", off, n, err)
			}
		})
	}
	wg.Wait()
	if h := b.Held(); len(h) > 4 {
		t.Fatalf("held %v", h)
	}
}

// A block fetched ahead that falls out of the bound, with nobody waiting for it, has its fetch stopped.
func TestBlocks_AFetchDroppedFromTheReadAheadIsStopped(t *testing.T) {
	data := make([]byte, 10_000)
	var stopped sync.Map
	fetch := func(ctx context.Context, a, b int64) ([]byte, error) {
		if a == 0 || a >= 5000 {
			return bytes.Clone(data[a:b]), nil
		}
		<-ctx.Done() // held until stopped
		stopped.Store(a, true)
		return nil, ctx.Err()
	}
	b := NewBlocks(context.Background(), fetch, 10_000, 1000, 10, 1, nil)
	defer b.Close()
	p := make([]byte, 1000)
	for _, off := range []int64{0, 5000, 6000} { // block 1 is fetched ahead, then dropped for 5, 6 and 7
		if _, err := b.ReadAt(p, off); err != nil {
			t.Fatal(err)
		}
	}
	for range 100 {
		if _, ok := stopped.Load(int64(1000)); ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("block 1's fetch still runs; held %v", b.Held())
}
