// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
	"github.com/FlashbackAi/teepin-go/teepin/internal/fakes"
)

// What the direct path's tests share: the fakes with small parts and read windows of about 300 KB, a client on them,
// objects stored as a client stores them, and readers of every kind.

const (
	kib        = 1 << 10
	blobSize   = 1_000_003 // a blob of 8 parts, read in 4 windows
	blobPart   = 128 * kib
	readWindow = 300_000 // not a whole number of segments, as the server's needn't be
	// plainWindow is the plaintext in a window of 300,000 stored bytes: 4 whole segments.
	plainWindow = 262_144
	beef        = "00000000-0000-7000-8000-00000000beef"
)

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func randomData(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// newDirect is the fakes with small parts and windows, and a client on them with no waits between tries.
func newDirect(t *testing.T) (*fakes.Fakes, *Client) {
	t.Helper()
	f := fakes.Start(t)
	f.API.Do(func() {
		f.Direct.MinPartBytes = 1
		f.Direct.ReadWindow = readWindow
	})
	return f, directClient(t, f, Config{})
}

// directClient is a client on the fakes; cfg's address and key are the fakes'.
func directClient(t *testing.T, f *fakes.Fakes, cfg Config) *Client {
	t.Helper()
	cfg.BaseURL, cfg.APIKey = f.API.URL, fakes.APIKey
	if cfg.StateDir == "" {
		cfg.StateDir = t.TempDir()
	}
	c := NewClient(cfg)
	c.sleep = func(ctx context.Context, d time.Duration) error { return ctx.Err() }
	t.Cleanup(func() {
		_ = c.Close()
		c.httpClient.CloseIdleConnections()
	})
	return c
}

// smallObject is a small object, packed as a client packs it: TWS1 behind its needle.
func smallObject(t *testing.T, f *fakes.Fakes, key string, size int) []byte {
	t.Helper()
	data := randomData(size)
	if err := f.PutPacked("photos", key, data, "image/png"); err != nil {
		t.Fatal(err)
	}
	if o := f.API.Object("photos", key); o.Location != "packed" {
		t.Fatalf("%s is %s", key, o.Location)
	}
	return data
}

// blob is a large object, uploaded as a client uploads one in parts of 128 KiB: TWS1 in a blob of its own.
func blob(t *testing.T, f *fakes.Fakes, key string, size int) []byte {
	t.Helper()
	data := randomData(size)
	if err := f.PutBlob("photos", key, data, blobPart, "video/mp4", map[string]string{"camera": "a7"}); err != nil {
		t.Fatal(err)
	}
	if o := f.API.Object("photos", key); o.Location != "blob" || !o.ClientEncrypted {
		t.Fatalf("%s is %s", key, o.Location)
	}
	return data
}

// gets are the storage network's GETs so far.
func gets(f *fakes.Fakes) []fakes.NetworkRequest { return f.Network.RequestsOf(http.MethodGet) }

// startOf is where a GET's range starts.
func startOf(r fakes.NetworkRequest) int64 {
	spec := strings.TrimPrefix(r.Header["range"], "bytes=")
	a, _, _ := strings.Cut(spec, "-")
	n, _ := strconv.ParseInt(a, 10, 64)
	return n
}

// getBytes reads an object (or a range of it) whole through GetObject.
func getBytes(c *Client, key string, opts *GetObjectOptions) ([]byte, error) {
	r, err := c.Storage.GetObject(context.Background(), "photos", key, opts)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// memFile is an in-memory file Download writes to, and reads back for its SHA-256.
type memFile struct {
	mu sync.Mutex
	b  []byte
}

func (m *memFile) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if end := off + int64(len(p)); end > int64(len(m.b)) {
		m.b = append(m.b, make([]byte, end-int64(len(m.b)))...)
	}
	return copy(m.b[off:], p), nil
}

func (m *memFile) ReadAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if off >= int64(len(m.b)) {
		return 0, io.EOF
	}
	n := copy(p, m.b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memFile) bytes() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.b...)
}

// download downloads an object into memory, as Python's download_bytes does.
func download(c *Client, key string, opts *DownloadOptions) ([]byte, *Object, error) {
	var m memFile
	obj, err := c.Storage.Download(context.Background(), "photos", key, &m, opts)
	return m.bytes(), obj, err
}

// engine is the client's read engine, to set its test hooks before a read.
func engine(c *Client) *direct.Reads { return c.Storage.reads() }

func isZeroKey(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

func reasonOf(err error) string {
	var die *DataIntegrityError
	if errors.As(err, &die) {
		return die.Reason
	}
	return ""
}

// tracked is a transport that keeps the requests still running: started, and not yet read to their end, failed or
// closed.
type tracked struct {
	inner http.RoundTripper
	mu    sync.Mutex
	live  map[*trackedBody]string
}

func newTracked() *tracked {
	return &tracked{inner: http.DefaultTransport.(*http.Transport).Clone(), live: map[*trackedBody]string{}}
}

type trackedBody struct {
	io.ReadCloser
	t    *tracked
	once sync.Once
}

func (b *trackedBody) done() {
	b.once.Do(func() {
		b.t.mu.Lock()
		delete(b.t.live, b)
		b.t.mu.Unlock()
	})
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.done()
	}
	return n, err
}

func (b *trackedBody) Close() error {
	b.done()
	return b.ReadCloser.Close()
}

func (t *tracked) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.inner.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	b := &trackedBody{ReadCloser: resp.Body, t: t}
	t.mu.Lock()
	t.live[b] = r.Method + " " + r.URL.String()
	t.mu.Unlock()
	resp.Body = b
	return resp, nil
}

// running is the requests still running.
func (t *tracked) running() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []string
	for _, u := range t.live {
		out = append(out, u)
	}
	return out
}
