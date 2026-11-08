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
	"sync"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
)

// StorageService is Teepin Storage. The calls that move no bytes are one API
// call each, named after the route in the API reference (/v1/openapi.json);
// PutObject, GetObject, Upload, Download and their File, Files and Stream
// forms (transfer.go, uploads.go) move the bytes.
//
// Every method sends the API key to the control plane, so call them from a
// backend you control.
//
// Bytes go straight between this device and the storage network, with no file
// byte passing through Teepin's API (TEEPIN-STORAGE-DESIGN.md sections 19, 22
// and 25). Uploads are encrypted here (TWS1, a fresh data key per object,
// wrapped to the account's public key); reads are verified here before any
// byte is used. The API signs the requests and keeps the catalog; the storage
// network sees only those signatures, never the API key.
type StorageService struct {
	client *Client

	readsMu sync.Mutex
	engine  *direct.Reads // direct storage's reads, made at the first one

	uploadsMu sync.Mutex
	up        *direct.Uploads // direct storage's uploads, made at the first one

	downloadsMu sync.Mutex
	dl          *direct.Downloads // batch downloads, made at the first one
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
	// Limit is objects per page, 1 to 1000. Zero or a negative value means
	// the default (1000).
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

// PutObjectOptions are the options of PutObject.
type PutObjectOptions struct {
	ContentType string
	// Metadata names are lowercase letters, digits, '-' and '_'.
	Metadata map[string]string
	// IfMatch writes only over an object with this ETag ("*": any object).
	IfMatch string
	// IfNoneMatch writes only if no object is at the key.
	IfNoneMatch bool
}

func (o *PutObjectOptions) write() direct.WriteOptions {
	if o == nil {
		return direct.WriteOptions{}
	}
	return direct.WriteOptions{ContentType: o.ContentType, Metadata: o.Metadata, IfMatch: o.IfMatch, IfNoneMatch: o.IfNoneMatch}
}

// PutObject stores size bytes of body, read from where it stands, at
// bucket/key: encrypted on this device and sent straight to the storage
// network (section 22.4).
//
//   - Up to 16 MiB, it is packed with the other PutObject calls waiting at
//     the same time into one write, and returns once its pack commits (group
//     commit: a lone call goes at once). The wait for a free send slot is
//     bounded by Config.UploadWait, then a *StorageUnavailableError.
//   - Larger, it goes in parts sent in parallel: read at any offset when body
//     is an io.ReaderAt or io.ReadSeeker, else streamed through once.
//   - Empty, it takes the API's metadata-only route, since it carries no
//     bytes.
//
// On a slow link (section 22.7) the client sizes requests to the speed it
// measured from earlier ones, so each takes about 4 minutes at most: packs
// shrink, an object a pack request couldn't carry in time goes in parts (from
// 5 MiB up), and a new upload's parts shrink. An object the measured speed
// can't carry at all (even a 5 MiB part would take over 4 minutes, or it
// would need more than 3,000 parts) is refused before anything is sent:
// IsTooLarge, and errors.Is(err, ErrConnectionSpeed).
//
// IfMatch and IfNoneMatch fail only this object (IsPreconditionFailed). A
// body shorter than size is an error, and nothing is stored.
func (s *StorageService) PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64, opts *PutObjectOptions) (*Object, error) {
	return s.put(ctx, bucket, key, body, size, opts.write(), 0, nil)
}

// put stores size bytes of body by its size: the metadata-only route, a shared pack, or parts.
func (s *StorageService) put(ctx context.Context, bucket, key string, body io.Reader, size int64, w direct.WriteOptions,
	partSize int64, progress ProgressFunc) (*Object, error) {
	up := s.uploads()
	switch {
	case size < 0:
		return nil, errors.New("teepin: size must not be negative")
	case size > MaxObjectSize:
		return nil, fmt.Errorf("teepin: %d bytes is over the 5 TiB an object may hold", size)
	case size == 0:
		obj, err := up.PutEmpty(ctx, bucket, key, w)
		if err == nil && progress != nil {
			progress(0, 0)
		}
		return objectFrom(obj), err
	case size <= up.PackObjectLimit():
		data := make([]byte, size)
		if _, err := io.ReadFull(body, data); err != nil {
			return nil, fmt.Errorf("teepin: read %s: %w", key, err)
		}
		obj, err := up.Put(ctx, bucket, key, data, w)
		if err != nil {
			return nil, err
		}
		if progress != nil {
			progress(size, size)
		}
		return objectFrom(obj), nil
	}
	if readAt, err := readerAtOf(body); err != nil {
		return nil, err
	} else if readAt != nil {
		obj, err := up.PutLarge(ctx, bucket, key, readAt, size, w, partSize, progress)
		return objectFrom(obj), err
	}
	obj, err := up.PutStream(ctx, bucket, key, &exactly{r: body, left: size, key: key}, w,
		direct.StreamParams{PartSize: partSize, ExpectedSize: size, Progress: progress})
	return objectFrom(obj), err
}

// readerAtOf reads body at any offset from where it stands: an io.ReaderAt (from its start, or from where an
// io.Seeker stands), or an io.ReadSeeker sought and read under a lock. Nil for a reader that is neither.
func readerAtOf(body io.Reader) (func(off, n int64) ([]byte, error), error) {
	var base int64
	seeker, canSeek := body.(io.Seeker)
	if canSeek {
		pos, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		base = pos
	}
	if ra, ok := body.(io.ReaderAt); ok {
		return func(off, n int64) ([]byte, error) { return readAtOf(ra, base+off, n) }, nil
	}
	if !canSeek {
		return nil, nil
	}
	var mu sync.Mutex
	return func(off, n int64) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		if _, err := seeker.Seek(base+off, io.SeekStart); err != nil {
			return nil, err
		}
		buf := make([]byte, n)
		got, err := io.ReadFull(body, buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			err = nil
		}
		return buf[:got], err
	}, nil
}

// readAtOf reads n bytes of r from off, or fewer at its end.
func readAtOf(r io.ReaderAt, off, n int64) ([]byte, error) {
	buf := make([]byte, n)
	got, err := r.ReadAt(buf, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:got], nil
}

// exactly is size bytes of a reader: one that ends sooner is an error, never a shorter object.
type exactly struct {
	r    io.Reader
	left int64
	key  string
}

// Read reads on until size bytes are read; a body that ends sooner is an error.
func (e *exactly) Read(p []byte) (int, error) {
	if e.left <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > e.left {
		p = p[:e.left]
	}
	n, err := e.r.Read(p)
	e.left -= int64(n)
	if err == io.EOF && e.left > 0 {
		return n, fmt.Errorf("teepin: %s: the body ended %d bytes short of its size", e.key, e.left)
	}
	if err == io.EOF {
		err = nil
	}
	return n, err
}

// StatObject returns an object's details without its bytes.
func (s *StorageService) StatObject(ctx context.Context, bucket, key string) (*Object, error) {
	var obj Object
	if err := s.client.call(ctx, &request{method: http.MethodGet, path: bucketPath(bucket) + "/object", query: keyQuery(key)}, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

// ObjectInfo is what a read says about an object.
type ObjectInfo struct {
	// Size is the length of this read's bytes (the range, for a range).
	Size int64
	// TotalSize is the whole object's size.
	TotalSize    int64
	ContentType  string
	ETag         string // unquoted
	LastModified time.Time
	// Metadata is the object's metadata, as stored with it (empty when it has
	// none), as the Python SDK's head_object carries it.
	Metadata map[string]string
}

// HeadObject returns what a read of an object would say about it, without
// its bytes. It is built from the object's catalog details (StatObject), as
// the other SDKs build it: no byte, and no signature, is needed.
func (s *StorageService) HeadObject(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	obj, err := s.StatObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	modified := obj.UpdatedAt
	if modified.IsZero() && obj.UploadedAt != nil {
		modified = *obj.UploadedAt
	}
	info := directInfo(obj.ContentType, obj.ETag, obj.Size, obj.Size, modified, obj.Metadata)
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

// ObjectReader is an object's bytes as they arrive. Close it.
type ObjectReader struct {
	io.ReadCloser
	ObjectInfo
}

// GetObject streams an object's bytes, or one range of them, straight from
// the storage network. Downloads are billed by the bytes signed for.
//
// Each piece is verified on this device before Read hands it out, and a whole
// read's SHA-256 is checked as its last bytes are read (a *DataIntegrityError,
// which is ErrChecksumMismatch, then: discard what was read). IfMatch is
// checked before any byte is fetched, and IfNoneMatch asks the object's
// details first, so a match signs and bills nothing. A range past the end is
// a 416. Read the body to its end or Close it: that ends the read and zeroes
// its copy of the data key.
func (s *StorageService) GetObject(ctx context.Context, bucket, key string, opts *GetObjectOptions) (*ObjectReader, error) {
	var o GetObjectOptions
	if opts != nil {
		o = *opts
	}
	if o.IfNoneMatch != "" {
		obj, err := s.StatObject(ctx, bucket, key)
		if err != nil {
			return nil, err
		}
		if obj.ETag != "" && obj.ETag == unquoteETag(o.IfNoneMatch) {
			return nil, ErrNotModified
		}
	}
	start, end := int64(0), int64(-1)
	if o.Range != nil {
		start, end = o.Range.Start, o.Range.End
	}
	reads := s.reads()
	got, err := reads.Get(ctx, bucket, key, start, end, o.IfMatch)
	if err != nil {
		return nil, err
	}
	sess, dek := got.Session, got.DEK
	body := newVerifiedReader(ctx, func(ctx context.Context, emit func([]byte) error) error {
		defer clear(dek)
		return reads.Plaintext(ctx, sess, dek, got.A, got.B, !got.Ranged, emit)
	}, func() { clear(dek) })
	info := directInfo(sess.ContentType, sess.ETag, got.B-got.A, sess.Size, sess.UploadedAt, sess.Metadata)
	return &ObjectReader{ReadCloser: body, ObjectInfo: info}, nil
}

// DeleteObject deletes an object, whatever version it is. DeleteObjectIf
// deletes only the version you checked.
func (s *StorageService) DeleteObject(ctx context.Context, bucket, key string) error {
	return s.deleteObject(ctx, bucket, key, "")
}

// DeleteObjectIf deletes an object only while its ETag is etag (If-Match,
// quoted or not): a check-then-delete, such as a move's, that can't delete an
// upload which landed after the check. A different version answers 412 and is
// kept (IsPreconditionFailed); a missing object is a 404 (IsNotFound), as
// with DeleteObject. An empty etag is refused before anything is sent, so a
// missing ETag never becomes an unconditional delete.
func (s *StorageService) DeleteObjectIf(ctx context.Context, bucket, key, etag string) error {
	e := unquoteETag(etag)
	if e == "" {
		return fmt.Errorf("teepin: DeleteObjectIf %s needs the ETag of the version to delete", key)
	}
	return s.deleteObject(ctx, bucket, key, e)
}

// deleteObject is DELETE object, with If-Match when etag (unquoted) is set.
func (s *StorageService) deleteObject(ctx context.Context, bucket, key, etag string) error {
	r := &request{method: http.MethodDelete, path: bucketPath(bucket) + "/object", query: keyQuery(key)}
	if etag != "" {
		if etag != "*" {
			etag = `"` + etag + `"`
		}
		r.header = http.Header{"If-Match": {etag}}
	}
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
// bucket/key, keeping its content type and metadata. The conditions are on
// the destination.
//
// The copy is made on this device (section 23.6): the source is read and
// verified, then uploaded as a new object (an encrypted object's key and
// needle name its own object, so its stored bytes can't be reused). So its
// bytes pass through this machine, and a copy is billed as a read as well as
// the stored copy. An empty source takes the metadata-only route; one up to
// 16 MiB is read whole, then packed; a larger one streams through in one
// pass, in parts. Every read is pinned to the version first seen (IfMatch on
// the source's ETag, and a source with none is refused), so a source replaced
// mid-copy fails it (ErrObjectChanged), with nothing left at the target and
// its upload cancelled.
func (s *StorageService) CopyObject(ctx context.Context, bucket, key, srcBucket, srcKey string, opts *CopyOptions) (*Object, error) {
	info, err := s.StatObject(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	w := direct.WriteOptions{ContentType: info.ContentType, Metadata: info.Metadata}
	if opts != nil {
		w.IfMatch, w.IfNoneMatch = opts.IfMatch, opts.IfNoneMatch
	}
	up := s.uploads()
	if info.Size == 0 {
		obj, err := up.PutEmpty(ctx, bucket, key, w)
		return objectFrom(obj), err
	}
	if info.ETag == "" {
		return nil, fmt.Errorf("teepin: %s has no ETag to pin the copy to; refusing to copy it unpinned", srcKey)
	}
	r, err := s.GetObject(ctx, srcBucket, srcKey, &GetObjectOptions{IfMatch: info.ETag})
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if info.Size <= up.PackObjectLimit() {
		data, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		if int64(len(data)) != info.Size {
			return nil, ErrObjectChanged
		}
		obj, err := up.Put(ctx, bucket, key, data, w)
		return objectFrom(obj), err
	}
	obj, err := up.PutStream(ctx, bucket, key, r, w, direct.StreamParams{ExpectedSize: info.Size})
	return objectFrom(obj), err
}

func unquoteETag(etag string) string {
	etag = strings.TrimPrefix(strings.TrimSpace(etag), "W/")
	return strings.Trim(etag, `"`)
}
