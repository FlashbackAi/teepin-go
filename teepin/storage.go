// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// StorageService is Teepin Storage. Each method is one API call, named after
// the route in the API reference (/v1/openapi.json); Upload, Download and
// their File and Stream forms (transfer.go) are the high-level way to move
// files of any size.
//
// Every method sends the API key, so call them from a backend you control.
// To let a browser download or upload one object without the key, sign a
// link with PresignDownload or PresignUpload and hand it only that.
type StorageService struct {
	client *Client
}

// Bucket is a bucket of the project.
type Bucket struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Backend     string    `json:"backend"`
	ObjectCount int64     `json:"object_count"`
	TotalBytes  int64     `json:"total_bytes"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Object is a stored object.
type Object struct {
	ID          string            `json:"id"`
	Key         string            `json:"key"`
	Size        int64             `json:"size_bytes"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	// ChecksumSHA256 is the SHA-256 of the object, or for one uploaded in
	// parts the SHA-256 of its parts' SHA-256s.
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
	// ETag is unquoted: the SHA-256, or "<sha256>-<parts>".
	ETag string `json:"etag,omitempty"`
	// PartCount is how many parts it was uploaded in; 0 for one request.
	PartCount  int        `json:"part_count,omitempty"`
	Status     string     `json:"status"`
	Backend    string     `json:"backend"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	UploadedAt *time.Time `json:"uploaded_at,omitempty"`
}

// Hold is whether the account's stored data is on hold for lack of credit.
type Hold struct {
	Held        bool       `json:"held"`
	HeldSince   *time.Time `json:"held_since,omitempty"`
	DeleteAfter *time.Time `json:"delete_after,omitempty"`
}

func bucketPath(bucket string) string { return "/v1/storage/buckets/" + url.PathEscape(bucket) }

func keyQuery(key string) url.Values { return url.Values{"key": {key}} }

// GetHold reports whether the account's stored data is on hold.
func (s *StorageService) GetHold(ctx context.Context) (*Hold, error) {
	var h Hold
	if err := s.client.call(ctx, &request{method: http.MethodGet, path: "/v1/storage/hold"}, &h); err != nil {
		return nil, err
	}
	return &h, nil
}

// CreateBucket makes a bucket. Names are 3 to 63 lowercase letters, digits,
// dots and hyphens.
func (s *StorageService) CreateBucket(ctx context.Context, name string) (*Bucket, error) {
	r := &request{method: http.MethodPost, path: "/v1/storage/buckets", json: map[string]string{"name": name}, mode: retrySafe}
	var b Bucket
	err := s.client.call(ctx, r, &b)
	if err != nil && r.attempt > 0 && ErrorCode(err) == "bucket_exists" {
		// An earlier attempt made it and its answer was lost.
		return s.GetBucket(ctx, name)
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// ListBuckets lists the project's buckets.
func (s *StorageService) ListBuckets(ctx context.Context) ([]Bucket, error) {
	var resp struct {
		Buckets []Bucket `json:"buckets"`
	}
	if err := s.client.call(ctx, &request{method: http.MethodGet, path: "/v1/storage/buckets"}, &resp); err != nil {
		return nil, err
	}
	return resp.Buckets, nil
}

// GetBucket returns a bucket with its object count and size.
func (s *StorageService) GetBucket(ctx context.Context, name string) (*Bucket, error) {
	var b Bucket
	if err := s.client.call(ctx, &request{method: http.MethodGet, path: bucketPath(name)}, &b); err != nil {
		return nil, err
	}
	return &b, nil
}

// DeleteBucket deletes an empty bucket (409 bucket_not_empty otherwise).
func (s *StorageService) DeleteBucket(ctx context.Context, name string) error {
	r := &request{method: http.MethodDelete, path: bucketPath(name)}
	err := s.client.call(ctx, r, nil)
	if err != nil && r.attempt > 0 && ErrorCode(err) == "bucket_not_found" {
		return nil
	}
	return err
}

// ListObjectsOptions narrows ListObjects.
type ListObjectsOptions struct {
	Prefix string
	// Delimiter lists like folders: keys with it after Prefix roll up into
	// ObjectPage.Prefixes. One printable ASCII character, usually "/".
	Delimiter string
	// Cursor is the NextCursor of the page before.
	Cursor string
	// Limit is objects per page, 1 to 1000 (default 1000).
	Limit int
}

// ObjectPage is one page of a listing.
type ObjectPage struct {
	Objects []Object `json:"objects"`
	// Prefixes are the folders under Prefix, when Delimiter was set.
	Prefixes []string `json:"prefixes"`
	// NextCursor starts the next page; empty on the last one.
	NextCursor string `json:"-"`
}

// ListObjects returns one page of objects in key order.
func (s *StorageService) ListObjects(ctx context.Context, bucket string, opts *ListObjectsOptions) (*ObjectPage, error) {
	q := url.Values{}
	if opts != nil {
		if opts.Prefix != "" {
			q.Set("prefix", opts.Prefix)
		}
		if opts.Delimiter != "" {
			q.Set("delimiter", opts.Delimiter)
		}
		if opts.Cursor != "" {
			q.Set("cursor", opts.Cursor)
		}
		if opts.Limit > 0 {
			q.Set("limit", strconv.Itoa(opts.Limit))
		}
	}
	var resp struct {
		Objects    []Object `json:"objects"`
		Prefixes   []string `json:"prefixes"`
		NextCursor *string  `json:"next_cursor"`
	}
	if err := s.client.call(ctx, &request{method: http.MethodGet, path: bucketPath(bucket) + "/objects", query: q}, &resp); err != nil {
		return nil, err
	}
	page := &ObjectPage{Objects: resp.Objects, Prefixes: resp.Prefixes}
	if resp.NextCursor != nil {
		page.NextCursor = *resp.NextCursor
	}
	return page, nil
}

// Objects yields every object under prefix, in key order, fetching pages as
// it goes. Stop early by breaking out of the loop.
//
//	for obj, err := range client.Storage.Objects(ctx, "my-bucket", "photos/") {
//		if err != nil { return err }
//		fmt.Println(obj.Key)
//	}
func (s *StorageService) Objects(ctx context.Context, bucket, prefix string) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		opts := &ListObjectsOptions{Prefix: prefix}
		for {
			page, err := s.ListObjects(ctx, bucket, opts)
			if err != nil {
				yield(Object{}, err)
				return
			}
			for _, o := range page.Objects {
				if !yield(o, nil) {
					return
				}
			}
			if page.NextCursor == "" {
				return
			}
			opts.Cursor = page.NextCursor
		}
	}
}

// PutObjectOptions are the options of a one-request upload.
type PutObjectOptions struct {
	ContentType string
	// Metadata is sent as X-Teepin-Meta-<name> headers: names lowercase
	// letters, digits, '-' and '_'.
	Metadata map[string]string
	// IfMatch writes only over an object with this ETag ("*": any object).
	IfMatch string
	// IfNoneMatch writes only if no object is at the key.
	IfNoneMatch bool
}

func (o *PutObjectOptions) headers() http.Header {
	h := http.Header{}
	if o == nil {
		return h
	}
	if o.ContentType != "" {
		h.Set("Content-Type", o.ContentType)
	}
	for k, v := range o.Metadata {
		h.Set("X-Teepin-Meta-"+k, v)
	}
	if o.IfMatch != "" {
		h.Set("If-Match", quoteETag(o.IfMatch))
	}
	if o.IfNoneMatch {
		h.Set("If-None-Match", "*")
	}
	return h
}

// PutObject uploads an object in one request (up to 5 GiB). When body is
// an io.ReadSeeker the request is retried after a passing failure; any
// other reader is sent once. Upload handles files of any size.
func (s *StorageService) PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64, opts *PutObjectOptions) (*Object, error) {
	r := &request{method: http.MethodPut, path: bucketPath(bucket) + "/object", query: keyQuery(key),
		header: opts.headers(), size: size, transfer: true, mode: retrySafe}
	seeker, canRewind := body.(io.ReadSeeker)
	used := false
	r.body = func() (io.Reader, error) {
		if used {
			if !canRewind {
				return nil, errors.New("teepin: cannot resend a body that is not an io.ReadSeeker")
			}
			if _, err := seeker.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}
		}
		used = true
		return body, nil
	}
	if !canRewind {
		r.mode = retryUnsent
	}
	var obj Object
	err := s.client.call(ctx, r, &obj)
	if err != nil && r.attempt > 0 && opts != nil && opts.IfNoneMatch && IsPreconditionFailed(err) {
		// An earlier attempt may have stored it before its answer was lost.
		return nil, fmt.Errorf("%w (an earlier attempt of this upload may have stored it)", err)
	}
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

// StatObject returns an object's details without its bytes.
func (s *StorageService) StatObject(ctx context.Context, bucket, key string) (*Object, error) {
	var obj Object
	if err := s.client.call(ctx, &request{method: http.MethodGet, path: bucketPath(bucket) + "/object", query: keyQuery(key)}, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

// ObjectInfo is what a download's headers say about an object.
type ObjectInfo struct {
	// Size is the length of this response's body (the range, for a range).
	Size int64
	// TotalSize is the whole object's size.
	TotalSize    int64
	ContentType  string
	ETag         string // unquoted
	LastModified time.Time
}

func infoOf(resp *http.Response) ObjectInfo {
	info := ObjectInfo{
		Size:        resp.ContentLength,
		TotalSize:   resp.ContentLength,
		ContentType: resp.Header.Get("Content-Type"),
		ETag:        unquoteETag(resp.Header.Get("ETag")),
	}
	if t, err := http.ParseTime(resp.Header.Get("Last-Modified")); err == nil {
		info.LastModified = t
	}
	// "bytes 0-99/1234"
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if i := strings.LastIndexByte(cr, '/'); i >= 0 {
			if n, err := strconv.ParseInt(cr[i+1:], 10, 64); err == nil {
				info.TotalSize = n
			}
		}
	}
	return info
}

// HeadObject returns an object's download headers without its bytes.
func (s *StorageService) HeadObject(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	r := &request{method: http.MethodHead, path: bucketPath(bucket) + "/object/content", query: keyQuery(key)}
	ctx, cancel := context.WithTimeout(ctx, s.client.timeout)
	defer cancel()
	resp, err := s.client.send(ctx, r)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	info := infoOf(resp)
	return &info, nil
}

// GetObjectOptions are the options of GetObject.
type GetObjectOptions struct {
	// Range is bytes Start to End, inclusive; End < 0 means to the end.
	// Nil is the whole object.
	Range *ByteRange
	// IfNoneMatch makes GetObject answer ErrNotModified when the object
	// still has this ETag.
	IfNoneMatch string
	// IfMatch fails with ErrObjectChanged unless the object has this ETag,
	// so the bytes of two ranges come from the same object.
	IfMatch string
}

// ByteRange is an inclusive byte range.
type ByteRange struct{ Start, End int64 }

func (r ByteRange) header() string {
	if r.End < 0 {
		return fmt.Sprintf("bytes=%d-", r.Start)
	}
	return fmt.Sprintf("bytes=%d-%d", r.Start, r.End)
}

// ObjectReader is an object's bytes as they arrive. Close it.
type ObjectReader struct {
	io.ReadCloser
	ObjectInfo
}

// GetObject streams an object's bytes, or one range of them. Downloads are
// billed by the bytes actually read.
func (s *StorageService) GetObject(ctx context.Context, bucket, key string, opts *GetObjectOptions) (*ObjectReader, error) {
	return s.get(ctx, bucketPath(bucket)+"/object/content", keyQuery(key), opts)
}

func (s *StorageService) get(ctx context.Context, path string, q url.Values, opts *GetObjectOptions) (*ObjectReader, error) {
	h := http.Header{}
	if opts != nil {
		if opts.Range != nil {
			h.Set("Range", opts.Range.header())
			if opts.IfMatch != "" {
				// A changed object answers the whole object (200), not 206.
				h.Set("If-Range", quoteETag(opts.IfMatch))
			}
		}
		if opts.IfNoneMatch != "" {
			h.Set("If-None-Match", quoteETag(opts.IfNoneMatch))
		}
	}
	resp, err := s.client.send(ctx, &request{method: http.MethodGet, path: path, query: q, header: h, transfer: true})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotModified {
		resp.Body.Close()
		return nil, ErrNotModified
	}
	info := infoOf(resp)
	if opts != nil && opts.IfMatch != "" {
		changed := info.ETag != unquoteETag(opts.IfMatch)
		if opts.Range != nil && resp.StatusCode != http.StatusPartialContent {
			changed = true
		}
		if changed {
			resp.Body.Close()
			return nil, ErrObjectChanged
		}
	}
	return &ObjectReader{ReadCloser: resp.Body, ObjectInfo: info}, nil
}

// DeleteObject deletes an object.
func (s *StorageService) DeleteObject(ctx context.Context, bucket, key string) error {
	r := &request{method: http.MethodDelete, path: bucketPath(bucket) + "/object", query: keyQuery(key)}
	err := s.client.call(ctx, r, nil)
	if err != nil && r.attempt > 0 && ErrorCode(err) == "object_not_found" {
		return nil // an earlier attempt deleted it
	}
	return err
}

// DeleteError is one key DeleteObjects could not delete.
type DeleteError struct {
	Key string `json:"key"`
	// Code is object_not_found, invalid_key or internal_error (worth a retry).
	Code    string `json:"code"`
	Message string `json:"error"`
}

// DeleteResult is what DeleteObjects did.
type DeleteResult struct {
	Deleted []string
	Errors  []DeleteError
}

// DeleteObjects deletes many objects, 1000 to a request; each key succeeds
// or fails on its own. A key that was already gone is reported in Errors
// with code object_not_found.
func (s *StorageService) DeleteObjects(ctx context.Context, bucket string, keys []string) (*DeleteResult, error) {
	out := &DeleteResult{}
	for start := 0; start < len(keys); start += 1000 {
		batch := keys[start:min(start+1000, len(keys))]
		var resp struct {
			Deleted []string      `json:"deleted"`
			Errors  []DeleteError `json:"errors"`
		}
		r := &request{method: http.MethodPost, path: bucketPath(bucket) + "/objects/delete",
			json: map[string][]string{"keys": batch}, mode: retrySafe}
		if err := s.client.call(ctx, r, &resp); err != nil {
			return out, err
		}
		out.Deleted = append(out.Deleted, resp.Deleted...)
		out.Errors = append(out.Errors, resp.Errors...)
	}
	return out, nil
}

// RenameObject moves an object to another key of the bucket. No bytes move,
// so it is instant at any size. With noOverwrite, an object at to makes it
// fail (409 object_exists).
func (s *StorageService) RenameObject(ctx context.Context, bucket, from, to string, noOverwrite bool) (*Object, error) {
	body := map[string]any{"from": from, "to": to, "overwrite": !noOverwrite}
	r := &request{method: http.MethodPost, path: bucketPath(bucket) + "/object/rename", json: body, mode: retryUnsent}
	var obj Object
	err := s.client.call(ctx, r, &obj)
	if err != nil && r.attempt > 0 && ErrorCode(err) == "object_not_found" {
		// An earlier attempt may have moved it; it is done if it is at to.
		if moved, serr := s.StatObject(ctx, bucket, to); serr == nil {
			return moved, nil
		}
	}
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

// CopyOptions are the conditions of CopyObject, on the destination.
type CopyOptions struct {
	IfMatch     string
	IfNoneMatch bool
}

// CopyObject copies srcBucket/srcKey (any bucket of the project) to
// bucket/key. The bytes are read and written again, so a copy is billed as a
// download as well as the stored copy.
func (s *StorageService) CopyObject(ctx context.Context, bucket, key, srcBucket, srcKey string, opts *CopyOptions) (*Object, error) {
	h := http.Header{}
	if opts != nil {
		if opts.IfMatch != "" {
			h.Set("If-Match", quoteETag(opts.IfMatch))
		}
		if opts.IfNoneMatch {
			h.Set("If-None-Match", "*")
		}
	}
	body := map[string]string{"source_bucket": srcBucket, "source_key": srcKey, "key": key}
	var obj Object
	r := &request{method: http.MethodPost, path: bucketPath(bucket) + "/object/copy", json: body, header: h, mode: retrySafe, transfer: true}
	if err := s.client.call(ctx, r, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

// SignedURL is a link that works without a credential until ExpiresAt.
type SignedURL struct {
	URL       string
	ExpiresAt time.Time
}

// PresignDownloadOptions are the options of PresignDownload.
type PresignDownloadOptions struct {
	// TTL is how long the link works: up to 24 hours (default 15 minutes).
	TTL time.Duration
	// Attachment makes a browser save the file instead of showing it.
	Attachment bool
}

// PresignDownload signs a link that downloads one object.
func (s *StorageService) PresignDownload(ctx context.Context, bucket, key string, opts *PresignDownloadOptions) (*SignedURL, error) {
	q := keyQuery(key)
	if opts != nil {
		if opts.TTL > 0 {
			q.Set("ttl_seconds", strconv.Itoa(int(opts.TTL.Seconds())))
		}
		if opts.Attachment {
			q.Set("disposition", "attachment")
		}
	}
	var resp struct {
		URL       string    `json:"url"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := s.client.call(ctx, &request{method: http.MethodPost, path: bucketPath(bucket) + "/object/download-url", query: q, mode: retrySafe}, &resp); err != nil {
		return nil, err
	}
	return &SignedURL{URL: s.client.absolute(resp.URL), ExpiresAt: resp.ExpiresAt}, nil
}

// PresignUploadOptions are the options of PresignUpload.
type PresignUploadOptions struct {
	// TTL is how long the link works: up to 1 hour (default 15 minutes).
	TTL time.Duration
	// MaxBytes is the largest file it accepts (default and most 5 GiB).
	MaxBytes int64
	// ContentType, when set, is the only type the upload may have.
	ContentType string
}

// SignedUpload is a single-use upload link: one successful PUT of URL with
// Headers.
type SignedUpload struct {
	URL       string
	Method    string
	Headers   map[string]string
	MaxBytes  int64
	ExpiresAt time.Time
}

// PresignUpload signs a link a browser can upload one object to, once.
func (s *StorageService) PresignUpload(ctx context.Context, bucket, key string, opts *PresignUploadOptions) (*SignedUpload, error) {
	q := keyQuery(key)
	if opts != nil {
		if opts.TTL > 0 {
			q.Set("ttl_seconds", strconv.Itoa(int(opts.TTL.Seconds())))
		}
		if opts.MaxBytes > 0 {
			q.Set("max_bytes", strconv.FormatInt(opts.MaxBytes, 10))
		}
		if opts.ContentType != "" {
			q.Set("content_type", opts.ContentType)
		}
	}
	var resp struct {
		URL       string            `json:"url"`
		Method    string            `json:"method"`
		Headers   map[string]string `json:"headers"`
		MaxBytes  int64             `json:"max_bytes"`
		ExpiresAt time.Time         `json:"expires_at"`
	}
	if err := s.client.call(ctx, &request{method: http.MethodPost, path: bucketPath(bucket) + "/object/upload-url", query: q, mode: retrySafe}, &resp); err != nil {
		return nil, err
	}
	return &SignedUpload{URL: s.client.absolute(resp.URL), Method: resp.Method, Headers: resp.Headers,
		MaxBytes: resp.MaxBytes, ExpiresAt: resp.ExpiresAt}, nil
}

func (c *Client) absolute(path string) string {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	return c.baseURL + path
}

func quoteETag(etag string) string {
	if etag == "*" || strings.HasPrefix(etag, `"`) || strings.HasPrefix(etag, `W/"`) {
		return etag
	}
	return `"` + etag + `"`
}

func unquoteETag(etag string) string {
	etag = strings.TrimPrefix(strings.TrimSpace(etag), "W/")
	return strings.Trim(etag, `"`)
}
