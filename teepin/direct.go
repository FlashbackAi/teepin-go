// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
)

// The client's side of direct storage (internal/direct): the engine's calls to the control plane and the storage
// network, and what its reads give back in this package's types.

// directAPI is the client as the engine sees it.
type directAPI struct{ c *Client }

// Call sends a control-plane request as a safe call (every direct route is idempotent), bounded with its retries by
// the call's timeout or the client's.
func (a directAPI) Call(ctx context.Context, call *direct.Call) (int, error) {
	timeout := call.Timeout
	if timeout == 0 {
		timeout = a.c.timeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	r := &request{method: call.Method, path: call.Path, json: call.Body, mode: retrySafe, noRetry: call.NoRetry}
	resp, err := a.c.send(ctx, r)
	call.Attempts = r.attempt
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if call.Out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, nil
	}
	if err := json.NewDecoder(resp.Body).Decode(call.Out); err != nil && !errors.Is(err, io.EOF) {
		return resp.StatusCode, &transportError{Err: err}
	}
	return resp.StatusCode, nil
}

// SendSigned sends a request the control plane signed, once, with exactly its signed headers and never the API key.
func (a directAPI) SendSigned(ctx context.Context, r *direct.SignedRequest) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, r.Method, r.URL, r.Body)
	if err != nil {
		return nil, err
	}
	if r.Body != nil {
		req.ContentLength = r.Size
	}
	for k, v := range r.Header {
		switch strings.ToLower(k) {
		case "content-length":
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil {
				return nil, err
			}
			req.ContentLength = n
		case "host":
			req.Host = v
		default:
			req.Header.Set(k, v)
		}
	}
	if req.ContentLength == 0 {
		req.Body = http.NoBody
	}
	req.Header.Set("User-Agent", a.c.userAgent)
	resp, err := a.c.httpClient.Do(req)
	if err != nil {
		return nil, &transportError{Err: err, Unsent: unsent(err)}
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		return nil, decodeStorageError(resp)
	}
	return resp, nil
}

// reads is the client's read engine, made at the first direct read.
func (s *StorageService) reads() *direct.Reads {
	s.readsMu.Lock()
	defer s.readsMu.Unlock()
	if s.engine == nil {
		c := s.client
		s.engine = direct.NewReads(directAPI{c}, direct.Options{
			MaxRetries:  c.maxRetries,
			Timeout:     c.timeout,
			KeyCacheTTL: c.keyCacheTTL,
			Sleep:       func(ctx context.Context, d time.Duration) error { return c.sleep(ctx, d) },
			Backoff:     backoff,
		})
	}
	return s.engine
}

// uploads is the client's upload engine, taken at the first direct upload: the one this process's clients with the
// same key, address and upload settings share (28.23).
func (s *StorageService) uploads() *direct.Uploads {
	s.uploadsMu.Lock()
	defer s.uploadsMu.Unlock()
	if s.up == nil {
		up, hold := holdUploads(s.client)
		s.up, s.hold = up, hold
		if hold != nil {
			s.collected = runtime.AddCleanup(s, (*uploadHold).release, hold)
		}
	}
	return s.up
}

// uploadEngine is an upload engine sending through c.
func uploadEngine(c *Client) *direct.Uploads {
	return direct.NewUploads(directAPI{c}, direct.Options{
		MaxRetries: c.maxRetries,
		Timeout:    c.timeout,
		Sleep:      func(ctx context.Context, d time.Duration) error { return c.sleep(ctx, d) },
		Backoff:    backoff,
	}, direct.UploadSettings{Wait: c.uploadWait, Concurrency: c.uploadConcurrency, StateDir: c.stateDir})
}

// downloads is the client's batch downloads, made at the first one, on its read engine (sharing its key cache).
func (s *StorageService) downloads() *direct.Downloads {
	reads := s.reads()
	s.downloadsMu.Lock()
	defer s.downloadsMu.Unlock()
	if s.dl == nil {
		s.dl = direct.NewDownloads(reads, s.client.stateDir)
	}
	return s.dl
}

// objectFrom is the upload engine's object as this package's.
func objectFrom(o *direct.Object) *Object {
	if o == nil {
		return nil
	}
	return &Object{ID: o.ID, Key: o.Key, Size: o.Size, ContentType: o.ContentType, Metadata: o.Metadata,
		ChecksumSHA256: o.ChecksumSHA256, ETag: unquoteETag(o.ETag), PartCount: o.PartCount, Status: o.Status,
		Backend: o.Backend, CreatedAt: o.CreatedAt, UpdatedAt: o.UpdatedAt, UploadedAt: o.UploadedAt}
}

// close zeroes the data keys the read engine keeps, and lets go of the shared upload engine: another client's saves go
// on, and the engine closes with its last client.
func (s *StorageService) close() {
	s.readsMu.Lock()
	if s.engine != nil {
		s.engine.Close()
	}
	s.readsMu.Unlock()
	s.uploadsMu.Lock()
	hold := s.hold
	if hold != nil {
		s.collected.Stop()
		s.up, s.hold = nil, nil
	}
	s.uploadsMu.Unlock()
	if hold != nil {
		hold.release()
	}
}

// objectOf is a read session's object, as StatObject gives it.
func objectOf(sess *direct.Session) *Object {
	o := &Object{ID: sess.Version, Key: sess.Key, Size: sess.Size, ContentType: sess.ContentType,
		Metadata: sess.Metadata, ChecksumSHA256: sess.SHA256, ETag: sess.ETag, PartCount: sess.PartCount(),
		Status: "available"}
	if !sess.UploadedAt.IsZero() {
		t := sess.UploadedAt
		o.UploadedAt = &t
		o.CreatedAt, o.UpdatedAt = t, t
	}
	return o
}

// directInfo is what a read says about an object, built from its details (a read session's, or the catalog's for
// HeadObject): the same fields the API's download headers gave, and its metadata.
func directInfo(contentType, etag string, size, total int64, lastModified time.Time, metadata map[string]string) ObjectInfo {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	md := make(map[string]string, len(metadata))
	for k, v := range metadata {
		md[k] = v
	}
	info := ObjectInfo{Size: size, TotalSize: total, ContentType: contentType, ETag: unquoteETag(etag), Metadata: md}
	if !lastModified.IsZero() {
		info.LastModified = lastModified.Truncate(time.Second)
	}
	return info
}

// errReaderClosed is a read of a GetObject body after Close.
var errReaderClosed = errors.New("teepin: read of a closed object body")

// verifiedReader is a direct read's body: each piece verified before Read hands it out, pulled only as it is read.
// Close (or reading to the end) ends the read: its response is closed and its copy of the data key zeroed.
type verifiedReader struct {
	cancel  context.CancelFunc
	release func() // zeroes the read's copy of the key; runs once the read ends, even if it never started

	mu     sync.Mutex
	next   func() ([]byte, error, bool)
	stop   func()
	buf    []byte
	err    error
	closed bool
}

// newVerifiedReader pulls the pieces read gives emit. read runs on the goroutine that calls Read, a piece at a time.
func newVerifiedReader(ctx context.Context, read func(ctx context.Context, emit func([]byte) error) error, release func()) *verifiedReader {
	rctx, cancel := context.WithCancel(ctx)
	errStopped := errors.New("teepin: the reader stopped")
	seq := func(yield func([]byte, error) bool) {
		err := read(rctx, func(c []byte) error {
			if !yield(c, nil) {
				return errStopped
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStopped) {
			yield(nil, err)
		}
	}
	next, stop := iter.Pull2(seq)
	return &verifiedReader{cancel: cancel, release: release, next: next, stop: stop}
}

func (v *verifiedReader) Read(p []byte) (int, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for len(v.buf) == 0 {
		switch {
		case v.closed:
			return 0, errReaderClosed
		case v.err != nil:
			return 0, v.err
		}
		c, err, ok := v.next()
		switch {
		case !ok:
			v.err = io.EOF
			v.end()
		case err != nil:
			v.err = err
			v.end()
		default:
			v.buf = c
		}
	}
	n := copy(p, v.buf)
	v.buf = v.buf[n:]
	return n, nil
}

// end finishes the read: the generator's own cleanup has run, or runs now.
func (v *verifiedReader) end() {
	v.stop()
	v.cancel()
	if v.release != nil {
		v.release()
	}
}

// Close stops the read at once, even one blocked in Read on another goroutine, closing its response and zeroing its
// copy of the data key.
func (v *verifiedReader) Close() error {
	v.cancel() // ends a Read in progress, so the lock below is had promptly
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.closed {
		v.closed = true
		v.buf = nil
		v.end()
	}
	return nil
}
