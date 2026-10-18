// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func randomBytes(n int) []byte {
	r := rand.New(rand.NewPCG(uint64(n), 7))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.UintN(256))
	}
	return b
}

func mustBucket(t *testing.T, c *Client, name string) {
	t.Helper()
	if _, err := c.Storage.CreateBucket(context.Background(), name); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
}

func TestBucketsAndListingPages(t *testing.T) {
	f, c := newFake(t)
	ctx := context.Background()
	mustBucket(t, c, "b1")
	f.mu.Lock()
	for i := 0; i < 2300; i++ {
		f.buckets["b1"][fmt.Sprintf("k/%05d", i)] = &fakeObject{data: []byte("x"), sha: "s"}
	}
	f.buckets["b1"]["other"] = &fakeObject{data: []byte("y"), sha: "s"}
	f.mu.Unlock()

	n := 0
	for obj, err := range c.Storage.Objects(ctx, "b1", "k/") {
		if err != nil {
			t.Fatal(err)
		}
		if want := fmt.Sprintf("k/%05d", n); obj.Key != want {
			t.Fatalf("object %d is %s, want %s", n, obj.Key, want)
		}
		n++
	}
	if n != 2300 || f.hits["GET objects"] != 3 {
		t.Fatalf("listed %d objects in %d pages", n, f.hits["GET objects"])
	}
	page, err := c.Storage.ListObjects(ctx, "b1", &ListObjectsOptions{Prefix: "other"})
	if err != nil || len(page.Objects) != 1 || page.NextCursor != "" {
		t.Fatalf("last page: %+v %v", page, err)
	}
	if err := c.Storage.DeleteBucket(ctx, "b1"); ErrorCode(err) != "bucket_not_empty" || !IsConflict(err) {
		t.Fatalf("delete a full bucket: %v", err)
	}
	if _, err := c.Storage.GetBucket(ctx, "nope"); !IsNotFound(err) {
		t.Fatalf("missing bucket: %v", err)
	}
}

// A create whose answer was lost, and that the retry finds made, succeeds.
func TestCreateBucketRetryFindsItMade(t *testing.T) {
	f, c := newFake(t)
	f.failN["POST "] = 0
	// The first attempt is answered 503 after the fake made the bucket.
	c.httpClient.Transport = &makeThenFail{inner: http.DefaultTransport, f: f}
	b, err := c.Storage.CreateBucket(context.Background(), "made")
	if err != nil || b.Name != "made" {
		t.Fatalf("%+v %v", b, err)
	}
}

type makeThenFail struct {
	inner http.RoundTripper
	f     *fakeStorage
	once  sync.Once
}

func (m *makeThenFail) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := m.inner.RoundTrip(r)
	failed := false
	m.once.Do(func() { failed = true })
	if failed && err == nil {
		resp.Body.Close()
		return nil, errors.New("connection reset by peer")
	}
	return resp, err
}

func TestSmallUploadIsOneRequestAndVerified(t *testing.T) {
	f, c := newFake(t)
	mustBucket(t, c, "b")
	data := []byte("hello, storage")
	var last int64
	obj, err := c.Storage.Upload(context.Background(), "b", "a.txt", bytes.NewReader(data), int64(len(data)),
		&UploadOptions{ContentType: "text/plain", Metadata: map[string]string{"owner": "ana"}, Progress: func(d, _ int64) { last = d }})
	if err != nil {
		t.Fatal(err)
	}
	if f.hits["PUT object"] != 1 || f.hits["POST uploads"] != 0 || obj.PartCount != 0 || last != int64(len(data)) {
		t.Fatalf("puts %d, uploads %d, parts %d, progress %d", f.hits["PUT object"], f.hits["POST uploads"], obj.PartCount, last)
	}
	stored := f.buckets["b"]["a.txt"]
	if string(stored.data) != string(data) || stored.contentType != "text/plain" || stored.metadata["owner"] != "ana" {
		t.Fatalf("stored %+v", stored)
	}
	want, _ := ComputeETag(bytes.NewReader(data), int64(len(data)), 0, 0)
	if obj.ETag != want {
		t.Fatalf("etag %s, computed %s", obj.ETag, want)
	}
}

func TestLargeUploadInParallelPartsWithRetries(t *testing.T) {
	f, c := newFake(t)
	mustBucket(t, c, "b")
	f.partDelay = 20 * time.Millisecond
	f.failN["PUT part"] = 3
	data := randomBytes(43 << 20)
	var mu sync.Mutex
	var final int64
	obj, err := c.Storage.Upload(context.Background(), "b", "big.bin", bytes.NewReader(data), int64(len(data)), &UploadOptions{
		PartSize: 5 << 20, Concurrency: 4,
		Progress: func(d, total int64) { mu.Lock(); final = d; mu.Unlock() },
	})
	if err != nil {
		t.Fatal(err)
	}
	if obj.PartCount != 9 || !bytes.Equal(f.buckets["b"]["big.bin"].data, data) {
		t.Fatalf("parts %d, bytes equal %v", obj.PartCount, bytes.Equal(f.buckets["b"]["big.bin"].data, data))
	}
	if got := f.maxInflight.Load(); got < 2 || got > 4 {
		t.Fatalf("at most %d parts at once, want 2 to 4", got)
	}
	if final != int64(len(data)) {
		t.Fatalf("progress ended at %d of %d", final, len(data))
	}
	want, _ := ComputeETag(bytes.NewReader(data), int64(len(data)), 5<<20, 9)
	if obj.ETag != want || f.hits["PUT part"] != 9+3 {
		t.Fatalf("etag %s want %s; part requests %d", obj.ETag, want, f.hits["PUT part"])
	}
}

func TestFailedUploadIsCancelledUnlessResumable(t *testing.T) {
	f, c := newFake(t)
	mustBucket(t, c, "b")
	data := randomBytes(20 << 20)
	f.failN["POST complete"] = 100
	_, err := c.Storage.Upload(context.Background(), "b", "x", bytes.NewReader(data), int64(len(data)), &UploadOptions{PartSize: 5 << 20})
	var ue *UploadError
	if !errors.As(err, &ue) || len(f.uploads) != 0 {
		t.Fatalf("err %v, open uploads %d", err, len(f.uploads))
	}
}

// A resumed upload sends only the parts the server lacks or holds with other
// bytes.
func TestResumeSendsOnlyMissingOrChangedParts(t *testing.T) {
	f, c := newFake(t)
	mustBucket(t, c, "b")
	data := randomBytes(26 << 20) // 6 parts of 5 MiB
	opts := &UploadOptions{PartSize: 5 << 20, Resume: true}
	f.failN["POST complete"] = 100
	if _, err := c.Storage.Upload(context.Background(), "b", "r.bin", bytes.NewReader(data), int64(len(data)), opts); err == nil {
		t.Fatal("expected the first try to fail")
	}
	if len(f.uploads) != 1 {
		t.Fatalf("a resumable upload stays open: %d", len(f.uploads))
	}
	f.mu.Lock()
	f.failN["POST complete"] = 0
	for _, up := range f.uploads {
		delete(up.parts, 2) // never arrived
	}
	f.mu.Unlock()
	data[4<<20+10] ^= 0xff    // part 1 changed locally
	data[(5<<20)*3+1] ^= 0xff // part 4 changed locally
	before := f.hits["PUT part"]
	var final int64
	opts.Progress = func(d, _ int64) { final = d }
	obj, err := c.Storage.Upload(context.Background(), "b", "r.bin", bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		t.Fatal(err)
	}
	if sent := f.hits["PUT part"] - before; sent != 3 {
		t.Fatalf("resumed upload sent %d parts, want 3 (parts 1, 2 and 4)", sent)
	}
	if !bytes.Equal(f.buckets["b"]["r.bin"].data, data) || obj.PartCount != 6 || final != int64(len(data)) {
		t.Fatalf("resumed object wrong: parts %d, progress %d", obj.PartCount, final)
	}
}

// When the completion's answer is lost but the object was made, the upload
// still succeeds.
func TestCompleteAnswerLost(t *testing.T) {
	f, c := newFake(t)
	mustBucket(t, c, "b")
	data := randomBytes(21 << 20)
	c.httpClient.Transport = &loseAnswer{inner: http.DefaultTransport, match: "/complete"}
	obj, err := c.Storage.Upload(context.Background(), "b", "l.bin", bytes.NewReader(data), int64(len(data)), &UploadOptions{PartSize: 5 << 20})
	if err != nil || obj.PartCount != 5 || !bytes.Equal(f.buckets["b"]["l.bin"].data, data) {
		t.Fatalf("%+v %v", obj, err)
	}
}

type loseAnswer struct {
	inner http.RoundTripper
	match string
}

func (l *loseAnswer) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := l.inner.RoundTrip(r)
	if err == nil && strings.HasSuffix(r.URL.Path, l.match) {
		resp.Body.Close()
		return nil, errors.New("read: connection reset by peer")
	}
	return resp, err
}

func TestUploadStream(t *testing.T) {
	f, c := newFake(t)
	mustBucket(t, c, "b")
	for _, size := range []int{0, 1000, 5 << 20, 5<<20 + 1, 17<<20 + 3} {
		data := randomBytes(size)
		key := fmt.Sprintf("s%d", size)
		obj, err := c.Storage.UploadStream(context.Background(), "b", key, io.MultiReader(bytes.NewReader(data)), &UploadOptions{PartSize: 5 << 20, Concurrency: 3})
		if err != nil {
			t.Fatalf("%d bytes: %v", size, err)
		}
		if !bytes.Equal(f.buckets["b"][key].data, data) || obj.Size != int64(size) {
			t.Fatalf("%d bytes stored wrong (%d)", size, len(f.buckets["b"][key].data))
		}
		wantParts := 0
		if size > 5<<20 {
			wantParts = (size + 5<<20 - 1) / (5 << 20)
		}
		if obj.PartCount != wantParts {
			t.Fatalf("%d bytes in %d parts, want %d", size, obj.PartCount, wantParts)
		}
	}
}

func TestDownloadParallelContinuesAfterACut(t *testing.T) {
	f, c := newFake(t)
	mustBucket(t, c, "b")
	data := randomBytes(21 << 20)
	if _, err := c.Storage.Upload(context.Background(), "b", "d.bin", bytes.NewReader(data), int64(len(data)), &UploadOptions{PartSize: 5 << 20}); err != nil {
		t.Fatal(err)
	}
	f.cutN = 2
	path := filepath.Join(t.TempDir(), "sub", "d.bin")
	var final int64
	var mu sync.Mutex
	obj, err := c.Storage.DownloadFile(context.Background(), "b", "d.bin", path, &DownloadOptions{PartSize: 4 << 20, Concurrency: 3,
		Progress: func(d, _ int64) { mu.Lock(); final = d; mu.Unlock() }})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, data) || obj.Size != int64(len(data)) || final != int64(len(data)) {
		t.Fatalf("download wrong: %d bytes, progress %d", len(got), final)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("temporary files left: %v", entries)
	}
}

func TestDownloadVerifiesChecksum(t *testing.T) {
	f, c := newFake(t)
	mustBucket(t, c, "b")
	data := randomBytes(9 << 20)
	if _, err := c.Storage.Upload(context.Background(), "b", "v.bin", bytes.NewReader(data), int64(len(data)), nil); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.buckets["b"]["v.bin"].data[100] ^= 1 // the stored bytes rot; the checksum stays
	f.mu.Unlock()
	if _, err := c.Storage.DownloadFile(context.Background(), "b", "v.bin", filepath.Join(t.TempDir(), "v"), &DownloadOptions{PartSize: 2 << 20}); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("parallel: %v", err)
	}
	var buf bytes.Buffer
	if _, err := c.Storage.DownloadStream(context.Background(), "b", "v.bin", &buf, nil); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("stream: %v", err)
	}
}

// An object replaced during a download fails it rather than mixing versions.
func TestDownloadNoticesAReplacedObject(t *testing.T) {
	f, c := newFake(t)
	mustBucket(t, c, "b")
	data := randomBytes(12 << 20)
	if _, err := c.Storage.Upload(context.Background(), "b", "c.bin", bytes.NewReader(data), int64(len(data)), &UploadOptions{PartSize: 5 << 20}); err != nil {
		t.Fatal(err)
	}
	w := &replaceOnWrite{f: f, buf: make([]byte, len(data))}
	_, err := c.Storage.Download(context.Background(), "b", "c.bin", w, &DownloadOptions{PartSize: 1 << 20, Concurrency: 1 + 1})
	if !errors.Is(err, ErrObjectChanged) {
		t.Fatalf("%v", err)
	}
}

type replaceOnWrite struct {
	f    *fakeStorage
	buf  []byte
	once sync.Once
}

func (r *replaceOnWrite) WriteAt(p []byte, off int64) (int, error) {
	r.once.Do(func() {
		r.f.mu.Lock()
		r.f.buckets["b"]["c.bin"] = &fakeObject{data: randomBytes(12 << 20), sha: "new"}
		r.f.mu.Unlock()
	})
	return copy(r.buf[off:], p), nil
}

func TestObjectCalls(t *testing.T) {
	f, c := newFake(t)
	ctx := context.Background()
	mustBucket(t, c, "b")
	put := func(key, body string, o *PutObjectOptions) (*Object, error) {
		return c.Storage.PutObject(ctx, "b", key, strings.NewReader(body), int64(len(body)), o)
	}
	a, err := put("a", "alpha", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := put("a", "again", &PutObjectOptions{IfNoneMatch: true}); !IsPreconditionFailed(err) {
		t.Fatalf("If-None-Match: %v", err)
	}
	if _, err := put("a", "beta", &PutObjectOptions{IfMatch: a.ETag}); err != nil {
		t.Fatalf("If-Match: %v", err)
	}
	r, err := c.Storage.GetObject(ctx, "b", "a", &GetObjectOptions{Range: &ByteRange{Start: 1, End: -1}})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r)
	r.Close()
	if string(body) != "eta" || r.TotalSize != 4 {
		t.Fatalf("range %q of %d", body, r.TotalSize)
	}
	cur, _ := c.Storage.StatObject(ctx, "b", "a")
	if _, err := c.Storage.GetObject(ctx, "b", "a", &GetObjectOptions{IfNoneMatch: cur.ETag}); !errors.Is(err, ErrNotModified) {
		t.Fatalf("If-None-Match read: %v", err)
	}
	if info, err := c.Storage.HeadObject(ctx, "b", "a"); err != nil || info.Size != 4 || info.ETag != cur.ETag {
		t.Fatalf("head %+v %v", info, err)
	}
	if _, err := c.Storage.CopyObject(ctx, "b", "c", "b", "a", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Storage.RenameObject(ctx, "b", "c", "a", true); !IsConflict(err) {
		t.Fatalf("rename onto an object: %v", err)
	}
	if o, err := c.Storage.RenameObject(ctx, "b", "c", "d", true); err != nil || o.Key != "d" {
		t.Fatalf("rename: %+v %v", o, err)
	}
	res, err := c.Storage.DeleteObjects(ctx, "b", []string{"a", "d", "gone"})
	if err != nil || len(res.Deleted) != 2 || len(res.Errors) != 1 || res.Errors[0].Code != "object_not_found" {
		t.Fatalf("bulk delete %+v %v", res, err)
	}
	link, err := c.Storage.PresignDownload(ctx, "b", "a", &PresignDownloadOptions{TTL: time.Hour, Attachment: true})
	if err != nil || !strings.HasPrefix(link.URL, c.BaseURL()+"/v1/storage/d/") {
		t.Fatalf("presign %+v %v", link, err)
	}
	if len(f.buckets["b"]) != 0 {
		t.Fatalf("left %d objects", len(f.buckets["b"]))
	}
}

func TestErrors(t *testing.T) {
	rec := httptest.NewRecorder()
	rec.Header().Set("X-Request-ID", "req-1")
	rec.WriteHeader(http.StatusPaymentRequired)
	_, _ = rec.WriteString(`{"error":"on hold","code":"storage_on_hold","delete_after":"2026-11-01T00:00:00Z"}`)
	err := decodeError(rec.Result())
	var ae *APIError
	if !errors.As(err, &ae) || !IsOnHold(err) || IsNoCredit(err) || ae.DeleteAfter == nil || ae.RequestID != "req-1" {
		t.Fatalf("%+v", ae)
	}
	if !strings.Contains(err.Error(), "storage_on_hold") {
		t.Fatalf("message %q", err.Error())
	}
	_, c := newFake(t)
	c.apiKey = "wrong"
	if _, err := c.Storage.ListBuckets(context.Background()); !IsUnauthorized(err) {
		t.Fatalf("%v", err)
	}
}

func TestBusyServerIsRetriedThenReported(t *testing.T) {
	f, c := newFake(t)
	f.failN["GET "] = 2
	if _, err := c.Storage.ListBuckets(context.Background()); err != nil {
		t.Fatalf("two busy answers are retried: %v", err)
	}
	f.failN["GET "] = 50
	_, err := c.Storage.ListBuckets(context.Background())
	if !IsRetryable(err) || f.hits["GET "] != 3+5 {
		t.Fatalf("after retries: %v (%d requests)", err, f.hits["GET "])
	}
}

func TestPartSizePolicy(t *testing.T) {
	cases := []struct{ size, want, got int64 }{
		{100 << 20, 0, 16 << 20},
		{100 << 20, 1 << 20, MinPartSize},
		{1 << 40, 0, 128 << 20}, // 1 TiB needs bigger parts to stay within 10,000
		{MaxObjectSize, 0, 1 << 30},
	}
	for _, tc := range cases {
		if p := PartSize(tc.size, tc.want); p != tc.got || (tc.size+p-1)/p > MaxParts {
			t.Errorf("PartSize(%d, %d) = %d, want %d", tc.size, tc.want, p, tc.got)
		}
	}
}
