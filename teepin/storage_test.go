// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/FlashbackAi/teepin-go/teepin/internal/fakes"
)

// The calls that move no bytes (buckets, listings, deletes, renames), errors and retries, against the fakes.

func mustBucket(t *testing.T, c *Client, name string) {
	t.Helper()
	if _, err := c.Storage.CreateBucket(context.Background(), name); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
}

func TestBucketsAndListingPages(t *testing.T) {
	f, c := newDirect(t)
	ctx := context.Background()
	mustBucket(t, c, "b1")
	f.API.Do(func() {
		for i := 0; i < 2300; i++ {
			f.API.Bucket("b1")[fmt.Sprintf("k/%05d", i)] = fakes.NewObject([]byte("x"), "", nil, "", 0)
		}
		f.API.Bucket("b1")["other"] = fakes.NewObject([]byte("y"), "", nil, "", 0)
	})
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
	if n != 2300 || f.API.HitsOf("GET objects") != 3 {
		t.Fatalf("listed %d objects in %d pages", n, f.API.HitsOf("GET objects"))
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
	f, c := newDirect(t)
	f.API.Do(func() { f.API.Lose["POST buckets"] = 1 })
	b, err := c.Storage.CreateBucket(context.Background(), "made")
	if err != nil || b.Name != "made" {
		t.Fatalf("%+v %v", b, err)
	}
	if f.API.HitsOf("POST buckets") != 2 {
		t.Fatalf("%d creates", f.API.HitsOf("POST buckets"))
	}
}

func TestObjectCalls(t *testing.T) {
	f, c := newDirect(t)
	ctx := context.Background()
	put := func(key, body string, o *PutObjectOptions) (*Object, error) {
		return c.Storage.PutObject(ctx, "photos", key, strings.NewReader(body), int64(len(body)), o)
	}
	a, err := put("a", "alpha", &PutObjectOptions{ContentType: "text/plain", Metadata: map[string]string{"owner": "ana"}})
	if err != nil {
		t.Fatal(err)
	}
	if a.ETag != sha256Hex([]byte("alpha")) || a.ChecksumSHA256 != a.ETag || a.Size != 5 {
		t.Fatalf("put: %+v", a)
	}
	if _, err := put("a", "again", &PutObjectOptions{IfNoneMatch: true}); !IsPreconditionFailed(err) {
		t.Fatalf("If-None-Match: %v", err)
	}
	if _, err := put("a", "beta", &PutObjectOptions{IfMatch: a.ETag, ContentType: "text/plain",
		Metadata: map[string]string{"owner": "bo"}}); err != nil {
		t.Fatalf("If-Match: %v", err)
	}
	cur, _ := c.Storage.StatObject(ctx, "photos", "a")
	if _, err := c.Storage.GetObject(ctx, "photos", "a", &GetObjectOptions{IfNoneMatch: cur.ETag}); !errors.Is(err, ErrNotModified) {
		t.Fatalf("If-None-Match read: %v", err)
	}
	info, err := c.Storage.HeadObject(ctx, "photos", "a")
	if err != nil || info.Size != 4 || info.ETag != cur.ETag || info.ContentType != "text/plain" || info.Metadata["owner"] != "bo" {
		t.Fatalf("head %+v %v", info, err)
	}
	if _, err := c.Storage.CopyObject(ctx, "photos", "c", "photos", "a", nil); err != nil {
		t.Fatal(err)
	}
	if got, err := f.Direct.ReadBack("photos", "c"); err != nil || string(got) != "beta" {
		t.Fatalf("copy: %q %v", got, err)
	}
	if _, err := c.Storage.RenameObject(ctx, "photos", "c", "a", true); !IsConflict(err) {
		t.Fatalf("rename onto an object: %v", err)
	}
	if o, err := c.Storage.RenameObject(ctx, "photos", "c", "d", true); err != nil || o.Key != "d" {
		t.Fatalf("rename: %+v %v", o, err)
	}
	res, err := c.Storage.DeleteObjects(ctx, "photos", []string{"a", "d", "gone"})
	if err != nil || len(res.Deleted) != 2 || len(res.Errors) != 1 || res.Errors[0].Code != "object_not_found" {
		t.Fatalf("bulk delete %+v %v", res, err)
	}
	if o := f.API.Object("photos", "a"); o != nil {
		t.Fatal("a is still there")
	}
}

func TestErrors(t *testing.T) {
	rec := httptest.NewRecorder()
	rec.Header().Set("X-Request-ID", "req-1")
	rec.WriteHeader(http.StatusPaymentRequired)
	_, _ = rec.WriteString(`{"error":"on hold","code":"storage_on_hold","delete_after":"2026-11-01T00:00:00Z","pack":"p-1"}`)
	err := decodeError(rec.Result())
	var ae *APIError
	if !errors.As(err, &ae) || !IsOnHold(err) || IsNoCredit(err) || ae.DeleteAfter == nil || ae.RequestID != "req-1" {
		t.Fatalf("%+v", ae)
	}
	if ae.Detail("pack") != "p-1" || len(ae.Details) != 1 {
		t.Fatalf("details %v", ae.Details)
	}
	if !strings.Contains(err.Error(), "storage_on_hold") {
		t.Fatalf("message %q", err.Error())
	}
	_, c := newDirect(t)
	c.apiKey = "wrong"
	if _, err := c.Storage.ListBuckets(context.Background()); !IsUnauthorized(err) {
		t.Fatalf("%v", err)
	}
}

func TestBusyServerIsRetriedThenReported(t *testing.T) {
	f, c := newDirect(t)
	f.API.Do(func() { f.API.Fail["GET buckets"] = 2 })
	if _, err := c.Storage.ListBuckets(context.Background()); err != nil {
		t.Fatalf("two busy answers are retried: %v", err)
	}
	f.API.Do(func() { f.API.Fail["GET buckets"] = 50 })
	_, err := c.Storage.ListBuckets(context.Background())
	if !IsRetryable(err) || f.API.HitsOf("GET buckets") != 3+5 {
		t.Fatalf("after retries: %v (%d requests)", err, f.API.HitsOf("GET buckets"))
	}
}

// PartSize and ComputeETag keep earlier releases' rules, so the ETags of objects they uploaded in parts still match.
func TestPartSizeAndComputeETagKeepEarlierReleasesRules(t *testing.T) {
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
	data := randomData(11 << 20)
	if e, err := ComputeETag(bytes.NewReader(data), int64(len(data)), 0, 0); err != nil || e != sha256Hex(data) {
		t.Fatalf("plain: %s %v", e, err)
	}
	ps := PartSize(int64(len(data)), MinPartSize)
	composite := ""
	var sums []byte
	for off := int64(0); off < int64(len(data)); off += ps {
		s := sha256Bytes(data[off:min(off+ps, int64(len(data)))])
		sums = append(sums, s...)
	}
	composite = sha256Hex(sums) + "-3"
	if e, err := ComputeETag(bytes.NewReader(data), int64(len(data)), ps, 3); err != nil || e != composite {
		t.Fatalf("composite: %s %v, want %s", e, err, composite)
	}
	if _, err := ComputeETag(bytes.NewReader(data), int64(len(data)), ps, 2); err == nil {
		t.Fatal("a wrong part count was taken")
	}
}

// DeleteObjectIf deletes only the version it names: a version that replaced it is kept (412), the named one goes, a
// missing object is a 404, and an empty ETag is refused before anything is sent.
func TestDeleteObjectIf(t *testing.T) {
	f, c := newDirect(t)
	ctx := context.Background()
	put := func(body string) *Object {
		t.Helper()
		o, err := c.Storage.PutObject(ctx, "photos", "a", strings.NewReader(body), int64(len(body)), nil)
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	checked := put("the original")
	replaced := put("a newer upload")
	if err := c.Storage.DeleteObjectIf(ctx, "photos", "a", checked.ETag); !IsPreconditionFailed(err) || ErrorCode(err) != "precondition_failed" {
		t.Fatalf("delete of a replaced version: %v", err)
	}
	if o := f.API.Object("photos", "a"); o == nil || o.ETag() != replaced.ETag {
		t.Fatal("the replacement went")
	}
	last := lastRequest(f)
	if last.Route != "DELETE object" || last.Header["if-match"] != `"`+checked.ETag+`"` {
		t.Fatalf("sent %s with If-Match %q", last.Route, last.Header["if-match"])
	}
	if err := c.Storage.DeleteObjectIf(ctx, "photos", "a", `"`+replaced.ETag+`"`); err != nil {
		t.Fatalf("delete of the current version, quoted: %v", err)
	}
	if f.API.Object("photos", "a") != nil {
		t.Fatal("a is still there")
	}
	if err := c.Storage.DeleteObjectIf(ctx, "photos", "a", replaced.ETag); !IsNotFound(err) {
		t.Fatalf("a conditional delete of a missing object: %v", err)
	}
	put("again")
	before := f.API.HitsOf("DELETE object")
	if err := c.Storage.DeleteObjectIf(ctx, "photos", "a", `""`); err == nil || f.API.HitsOf("DELETE object") != before {
		t.Fatalf("an empty ETag: %v, %d requests", err, f.API.HitsOf("DELETE object")-before)
	}
	if f.API.Object("photos", "a") == nil {
		t.Fatal("an empty ETag deleted it")
	}
	// DeleteObject stays unconditional and sends no If-Match.
	if err := c.Storage.DeleteObject(ctx, "photos", "a"); err != nil || f.API.Object("photos", "a") != nil {
		t.Fatalf("unconditional delete: %v", err)
	}
	if last := lastRequest(f); last.Header["if-match"] != "" {
		t.Fatalf("DeleteObject sent If-Match %q", last.Header["if-match"])
	}
}

// lastRequest is the last request the fake API answered.
func lastRequest(f *fakes.Fakes) (r fakes.APIRequest) {
	f.API.Do(func() { r = f.API.Requests[len(f.API.Requests)-1] })
	return r
}
