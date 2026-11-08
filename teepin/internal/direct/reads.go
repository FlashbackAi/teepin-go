// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// Reads straight from the storage network (TEEPIN-STORAGE-DESIGN.md sections 21.3, 22.5 and 25), a port of the Python
// SDK's _reads.py by way of the Node SDK's reads.ts, with the same rules.
//
// A read session (object/read) gives an object's format, where its bytes are, its data key, and signed requests for
// them. Every byte is verified before it is used:
//
//   - tws1 in its own blob: windows (the server's, 64 MiB), read segment by segment as they stream, each GCM tag
//     checked (the header's key commitment first, the last segment told by the session's size). A window that breaks
//     off goes on from its last verified segment, under a new signature.
//   - anything packed: the needle first (TPK1, the object id, which is the session's version, the stored size, and
//     the stored bytes' SHA-256), then the whole object decrypted (tws1, teepin-v1) or taken as stored (plain).
//   - plain in its own blob (legacy objects only): as stored.
//   - source "api": an object still staged on the API. The API serves no object's bytes since the cut-over (section
//     23.5), which drains the stage first, so such a read is errs.ErrObjectStaged, never a read through the API.
//
// A whole-object read also checks the plaintext SHA-256 against the session's, as its last bytes arrive, except for a
// legacy object stored in parts, whose checksum is a composite.
//
// Data keys are cached in memory for the client's KeyCacheTTL (10 minutes), by bucket, key and version, so later
// reads sign without them. Each read takes its own copy and zeroes it when done; the cache zeroes its copies on
// expiry, replacement, drop and Close.
//
// Failures: a passing one (the network, a busy storage network) is retried with backoff; an expired or refused
// signature (401, 403) is signed again, keeping the cached key; a 404 drops the key and is signed again once (a
// server pack may have been compacted), then a not-found error; a failed check is retried once from the last
// verified byte, then a DataIntegrityError.
//
// Cancellation is the context: it ends a read at once with the context's error, closes its responses, cuts a wait
// short, stops a download's parallel windows and waits for them, and is never retried.

const (
	// DefaultWindow is the server's read window until it shows its own: 64 MiB of stored bytes.
	DefaultWindow = 64 << 20
	// DefaultKeyCacheTTL is how long a data key stays cached unless the client says.
	DefaultKeyCacheTTL = 10 * time.Minute
	// ReadBatch is the most keys a batch read session names (the control plane's cap).
	ReadBatch = 1000
	// readChunk is the most a plain blob's read takes from its body at once.
	readChunk = 1 << 20
)

// errExpired is a signature too close to its expiry to use.
var errExpired = errors.New("teepin: the signature is about to expire")

// BucketPath is a bucket's path on the API.
func BucketPath(bucket string) string { return "/v1/storage/buckets/" + url.PathEscape(bucket) }

// unquoteETag is an ETag without its quotes or weak prefix.
func unquoteETag(etag string) string {
	return strings.Trim(strings.TrimPrefix(strings.TrimSpace(etag), "W/"), `"`)
}

// --- sessions -----------------------------------------------------------------

// Signed is one signed request to the storage network, as a read session gives it.
type Signed struct {
	Method string
	URL    string
	Header map[string]string
	// Expires is when the signature expires; zero when the session didn't say.
	Expires time.Time
	// Prefix is the bytes before the object's own (a packed object's needle).
	Prefix int64
	// First and Last are the object's stored bytes it covers, inclusive.
	First, Last int64
}

// Length is the bytes it answers: the prefix and the object's own.
func (r *Signed) Length() int64 { return r.Prefix + r.Last - r.First + 1 }

type signedJSON struct {
	Method      string         `json:"method"`
	URL         string         `json:"url"`
	Headers     map[string]any `json:"headers"`
	ExpiresAt   string         `json:"expires_at"`
	Prefix      int64          `json:"prefix"`
	StoredFirst int64          `json:"stored_first"`
	StoredLast  int64          `json:"stored_last"`
}

// when is a time from the API, or zero when there is none or it doesn't parse.
func when(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(s))
	if err != nil {
		return time.Time{}
	}
	return t
}

// headerValue is a signed header's value as a string, whatever JSON type it came as.
func headerValue(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	}
	return fmt.Sprint(v)
}

func signedOf(j *signedJSON) Signed {
	h := make(map[string]string, len(j.Headers))
	for k, v := range j.Headers {
		h[k] = headerValue(v)
	}
	m := j.Method
	if m == "" {
		m = http.MethodGet
	}
	return Signed{Method: m, URL: j.URL, Header: h, Expires: when(j.ExpiresAt), Prefix: j.Prefix,
		First: j.StoredFirst, Last: j.StoredLast}
}

type batchErrorJSON struct {
	Code  string `json:"code"`
	Error string `json:"error"`
}

type sessionJSON struct {
	Key            string            `json:"key"`
	Version        string            `json:"version"`
	Size           int64             `json:"size"`
	StoredSize     int64             `json:"stored_size"`
	ETag           string            `json:"etag"`
	SHA256         string            `json:"sha256"`
	ContentType    string            `json:"content_type"`
	Metadata       map[string]string `json:"metadata"`
	Format         string            `json:"format"`
	Source         string            `json:"source"`
	UploadedAt     string            `json:"uploaded_at"`
	KeyMaterial    json.RawMessage   `json:"key_material"`
	AssociatedData string            `json:"associated_data"`
	Requests       []signedJSON      `json:"requests"`
	Error          *batchErrorJSON   `json:"error"`
}

// Session is one read session. DEK is this session's own copy of the data key.
type Session struct {
	Bucket      string
	Key         string
	Version     string
	Size        int64
	StoredSize  int64
	ETag        string
	SHA256      string
	ContentType string
	Metadata    map[string]string
	Format      string
	Source      string
	UploadedAt  time.Time
	DEK         []byte
	AD          []byte
	Requests    []Signed
}

// NewSession reads one session from its JSON. A format nobody knows is a DataIntegrityError (reason "format").
func NewSession(bucket string, raw []byte) (*Session, error) {
	var j sessionJSON
	if err := json.Unmarshal(raw, &j); err != nil {
		return nil, errs.Integrity(errs.ReasonFormat, "a read session that isn't JSON: %v", err)
	}
	return sessionOf(bucket, &j)
}

func sessionOf(bucket string, j *sessionJSON) (*Session, error) {
	s := &Session{Bucket: bucket, Key: j.Key, Version: j.Version, Size: j.Size, StoredSize: j.StoredSize,
		ETag: unquoteETag(j.ETag), SHA256: j.SHA256, ContentType: j.ContentType, Format: j.Format,
		Source: j.Source, UploadedAt: when(j.UploadedAt), Metadata: map[string]string{}}
	for k, v := range j.Metadata {
		s.Metadata[k] = v
	}
	if s.Source == "" {
		s.Source = SourceStorage
	}
	km := j.KeyMaterial
	defer clear(km) // the key's text, as far as Go lets a decoded answer be zeroed
	var b64 string
	if len(km) > 0 && string(km) != "null" {
		if err := json.Unmarshal(km, &b64); err != nil {
			return nil, errs.Integrity(errs.ReasonKey, "%s: the read session's key isn't a string", s.Key)
		}
	}
	if b64 != "" {
		dek, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, errs.Integrity(errs.ReasonKey, "%s: the read session's key isn't base64", s.Key)
		}
		s.DEK = dek
	}
	if j.AssociatedData != "" {
		ad, err := base64.StdEncoding.DecodeString(j.AssociatedData)
		if err != nil {
			s.Forget()
			return nil, errs.Integrity(errs.ReasonFormat, "%s: the read session's associated data isn't base64", s.Key)
		}
		s.AD = ad
	}
	for i := range j.Requests {
		s.Requests = append(s.Requests, signedOf(&j.Requests[i]))
	}
	if s.Source == SourceStorage && !knownFormat(s.Format) {
		s.Forget()
		return nil, errs.Integrity(errs.ReasonFormat, "%s: unknown storage format %q", s.Key, s.Format)
	}
	return s, nil
}

// Staged reports an object still staged on the API, whose bytes this engine can't read (errs.ErrObjectStaged).
func (s *Session) Staged() bool { return s.Source == SourceAPI }

// Packed reports an object in a pack: its request carries the needle before its bytes.
func (s *Session) Packed() bool {
	return len(s.Requests) > 0 && s.Requests[len(s.Requests)-1].Prefix > 0
}

// Window is the window the session signs: its last request (a later window's first one is the header).
func (s *Session) Window() *Signed { return &s.Requests[len(s.Requests)-1] }

// NeedsKey reports an object that can't be read without its data key.
func (s *Session) NeedsKey() bool {
	return !s.Staged() && s.StoredSize > 0 && (s.Format == FormatTWS1 || s.Format == FormatServer)
}

// Checkable reports whether the whole object's SHA-256 can be checked (not a legacy composite of parts).
func (s *Session) Checkable() bool { return s.SHA256 != "" && !strings.Contains(s.ETag, "-") }

// Forget zeroes this session's copy of the key.
func (s *Session) Forget() {
	clear(s.DEK)
	s.DEK = nil
}

var partsSuffix = regexp.MustCompile(`-(\d+)$`)

// PartCount is the parts a legacy object's ETag names ("<sha>-N"); 0 for any other.
func (s *Session) PartCount() int {
	if m := partsSuffix.FindStringSubmatch(s.ETag); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	return 0
}

// --- the key cache --------------------------------------------------------------

type cachedKey struct {
	version string
	key     []byte
	at      time.Time
}

// KeyCache holds data keys by bucket and key, each for one version, for its TTL (0 or less: none are kept). It hands
// out copies, so zeroing one never breaks a read in progress, and zeroes its own on expiry, replacement, drop and
// Clear.
type KeyCache struct {
	ttl time.Duration
	// Now is the clock; a test may move it.
	Now     func() time.Time
	mu      sync.Mutex
	entries map[[2]string]*cachedKey
}

// NewKeyCache keeps keys for ttl.
func NewKeyCache(ttl time.Duration) *KeyCache {
	return &KeyCache{ttl: ttl, Now: time.Now, entries: map[[2]string]*cachedKey{}}
}

// TTL is how long a key is kept.
func (c *KeyCache) TTL() time.Duration { return c.ttl }

// Get is the cached version of bucket/key and a copy of its key, if one is cached and fresh.
func (c *KeyCache) Get(bucket, key string) (string, []byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := [2]string{bucket, key}
	e := c.entries[id]
	if e == nil {
		return "", nil, false
	}
	if c.Now().Sub(e.at) > c.ttl {
		delete(c.entries, id)
		clear(e.key)
		return "", nil, false
	}
	return e.version, append([]byte(nil), e.key...), true
}

// Put keeps a copy of dek for this version (the caller keeps its own). A key already cached for the same version is
// kept; another version's is replaced and zeroed.
func (c *KeyCache) Put(bucket, key, version string, dek []byte) {
	if c.ttl <= 0 {
		return // the cache is off: the caller's copy is the only one
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id := [2]string{bucket, key}
	old := c.entries[id]
	if old != nil && old.version == version {
		return
	}
	c.entries[id] = &cachedKey{version: version, key: append([]byte(nil), dek...), at: c.Now()}
	if old != nil {
		clear(old.key)
	}
}

// Drop forgets bucket/key's key, zeroing it.
func (c *KeyCache) Drop(bucket, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := [2]string{bucket, key}
	if e := c.entries[id]; e != nil {
		clear(e.key)
		delete(c.entries, id)
	}
}

// Clear zeroes and forgets every key.
func (c *KeyCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, e := range c.entries {
		clear(e.key)
		delete(c.entries, id)
	}
}

// Len is how many keys are cached.
func (c *KeyCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Held is the cache's own copies of its keys (not copies of them), so a test can see them zeroed.
func (c *KeyCache) Held() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, 0, len(c.entries))
	for _, e := range c.entries {
		out = append(out, e.key)
	}
	return out
}

// --- the engine -------------------------------------------------------------------

// Hooks let tests watch or change a read's steps, as the Python and Node tests patch methods. Nil fields do nothing;
// set them before any read starts.
type Hooks struct {
	// Call is told of each read-session request, and whether it asked for the key.
	Call func(bucket, key string, off, length int64, includeKey bool)
	// Session is told of each read's first session (a download tried again asks for another).
	Session func(bucket, key string)
	// Window is told of each later window asked for, and may change where it is asked from.
	Window func(s0 *Session, off, length int64) (int64, int64)
	// Open runs before each signed GET; an error it returns is that GET's.
	Open func(ctx context.Context, r *Signed) error
	// Fresh decides whether a signature may be used; fresh is what its expiry says.
	Fresh func(r *Signed, fresh bool) bool
	// Failed is told of each failure a read meets, before it is judged.
	Failed func(err error)
	// Plaintext is told of each range a read verifies, with the copy of the key it uses.
	Plaintext func(s *Session, dek []byte, a, b int64)
}

// Reads reads through read sessions, for one client.
type Reads struct {
	api  API
	opts Options
	// Keys are the cached data keys.
	Keys *KeyCache
	// Hooks are for tests.
	Hooks Hooks
}

// NewReads is the read engine of a client.
func NewReads(api API, opts Options) *Reads {
	return &Reads{api: api, opts: opts, Keys: NewKeyCache(opts.KeyCacheTTL)}
}

// Close zeroes the cached data keys. Reads still work afterwards, fetching their keys again.
func (r *Reads) Close() { r.Keys.Clear() }

// call asks for one read session (object/read).
func (r *Reads) call(ctx context.Context, bucket, key string, off, length int64, includeKey bool) (*Session, error) {
	if h := r.Hooks.Call; h != nil {
		h(bucket, key, off, length, includeKey)
	}
	var raw json.RawMessage
	body := map[string]any{"key": key, "stored_offset": off, "stored_length": length, "include_key": includeKey}
	if _, err := r.api.Call(ctx, &Call{Method: http.MethodPost, Path: BucketPath(bucket) + "/object/read", Body: body, Out: &raw}); err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) == 0 || string(raw) == "null" {
		raw = json.RawMessage("{}")
	}
	return NewSession(bucket, raw)
}

// Session is a read's first session, with the data key: from the cache when it holds this version's, so the server
// needn't send it again. off and length ask for a window of the stored bytes (0 and 0: the server's first window).
func (r *Reads) Session(ctx context.Context, bucket, key string, off, length int64) (*Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if h := r.Hooks.Session; h != nil {
		h(bucket, key)
	}
	version, cached, ok := r.Keys.Get(bucket, key)
	s, err := r.call(ctx, bucket, key, off, length, !ok)
	if err == nil && s.NeedsKey() && s.DEK == nil {
		if ok && version == s.Version {
			s.DEK, cached = cached, nil
		} else {
			s, err = r.call(ctx, bucket, key, off, length, true)
		}
	}
	clear(cached)
	if err != nil {
		return nil, err
	}
	if s.DEK != nil {
		r.Keys.Put(bucket, key, s.Version, s.DEK)
	}
	return s, nil
}

// BatchItem is one object's answer in a batch: its session, or the error that object alone met.
type BatchItem struct {
	Session *Session
	Err     error
}

// Sessions asks for read sessions of many objects, signed ReadBatch to a request (objects/read), each object's
// refusal on its own. Each session holds its own key (also cached); the caller forgets it. A key named twice is
// asked for once. An object still staged comes back as its session (Staged); the caller reports it as
// errs.ErrObjectStaged.
func (r *Reads) Sessions(ctx context.Context, bucket string, keys []string) (map[string]BatchItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := map[string]BatchItem{}
	seen := map[string]bool{}
	var unique []string
	for _, k := range keys {
		if !seen[k] {
			seen[k] = true
			unique = append(unique, k)
		}
	}
	forgetAll := func() {
		for _, it := range out {
			if it.Session != nil {
				it.Session.Forget()
			}
		}
	}
	for i := 0; i < len(unique); i += ReadBatch {
		chunk := unique[i:min(i+ReadBatch, len(unique))]
		var ans struct {
			Objects []json.RawMessage `json:"objects"`
		}
		body := map[string]any{"keys": chunk, "include_keys": true}
		if _, err := r.api.Call(ctx, &Call{Method: http.MethodPost, Path: BucketPath(bucket) + "/objects/read", Body: body, Out: &ans}); err != nil {
			forgetAll()
			return nil, err
		}
		for _, raw := range ans.Objects {
			var j sessionJSON
			if err := json.Unmarshal(raw, &j); err != nil {
				clear(raw)
				continue // an answer that isn't an object: its key is reported missing below
			}
			clear(raw)
			if j.Error != nil {
				out[j.Key] = BatchItem{Err: sessionError(j.Key, j.Error)}
				continue
			}
			s, err := sessionOf(bucket, &j)
			if err != nil {
				out[j.Key] = BatchItem{Err: err}
				continue
			}
			if s.DEK != nil {
				r.Keys.Put(bucket, s.Key, s.Version, s.DEK)
			}
			out[s.Key] = BatchItem{Session: s}
		}
	}
	for _, k := range unique {
		if _, ok := out[k]; !ok {
			out[k] = BatchItem{Err: &errs.APIError{StatusCode: http.StatusNotFound, Code: "object_not_found",
				Message: k + ": missing from the read batch's answer"}}
		}
	}
	return out, nil
}

// sessionError is the error a batch read gives one object, as the single route would answer it.
func sessionError(key string, e *batchErrorJSON) error {
	msg := e.Error
	if msg == "" {
		msg = strings.ReplaceAll(e.Code, "_", " ")
	}
	msg = key + ": " + msg
	switch e.Code {
	case errs.CodeKeyErased:
		return &errs.APIError{StatusCode: http.StatusGone, Code: e.Code, Message: msg}
	case "object_not_found":
		return &errs.APIError{StatusCode: http.StatusNotFound, Code: e.Code, Message: msg}
	}
	return &errs.APIError{StatusCode: http.StatusInternalServerError, Code: e.Code, Message: msg}
}

// Window is a later window of the same version, signed without the key: ErrObjectChanged if the object was replaced.
func (r *Reads) Window(ctx context.Context, s0 *Session, off, length int64) (*Session, error) {
	if h := r.Hooks.Window; h != nil {
		off, length = h(s0, off, length)
	}
	s, err := r.call(ctx, s0.Bucket, s0.Key, off, length, false)
	if err != nil {
		return nil, err
	}
	s.Forget() // never used: the read has its own copy (a server that sent one anyway gets it zeroed)
	if s.Version != s0.Version {
		return nil, errs.ErrObjectChanged
	}
	return s, nil
}

func (r *Reads) fresh(s *Signed) bool {
	ok := s.Expires.IsZero() || time.Until(s.Expires) > SignMargin
	if h := r.Hooks.Fresh; h != nil {
		return h(s, ok)
	}
	return ok
}

// open sends a signed GET and waits for its answer, at most the client's timeout; its body is then read a piece at a
// time, each piece waiting at most the same.
func (r *Reads) open(ctx context.Context, s *Signed) (*body, error) {
	if !r.fresh(s) {
		return nil, errExpired
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if h := r.Hooks.Open; h != nil {
		if err := h(ctx, s); err != nil {
			return nil, Stopped(ctx, err)
		}
	}
	rctx, cancel := context.WithCancel(ctx)
	timeout := r.opts.Timeout
	var timedOut atomic.Bool
	var timer *time.Timer
	if timeout > 0 {
		timer = time.AfterFunc(timeout, func() {
			timedOut.Store(true)
			cancel()
		})
	}
	resp, err := r.api.SendSigned(rctx, &SignedRequest{Method: http.MethodGet, URL: s.URL, Header: s.Header})
	if timer != nil {
		timer.Stop()
	}
	if err == nil && timedOut.Load() {
		resp.Body.Close()
		err = errs.Connection("no answer from the storage network in %s", timeout)
	}
	if err != nil {
		cancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if timedOut.Load() {
			return nil, errs.Connection("no answer from the storage network in %s", timeout)
		}
		return nil, err
	}
	b := &body{ctx: ctx, resp: resp, cancel: cancel, idle: timeout}
	if timeout > 0 {
		b.timer = time.AfterFunc(timeout, func() {
			b.timedOut.Store(true)
			cancel()
		})
		b.timer.Stop()
	}
	return b, nil
}

// body is a signed GET's body as it arrives. Each read waits at most idle for bytes; a body that breaks off is an
// *errs.ConnError (a passing failure), and the read's own context ending is that context's error.
type body struct {
	ctx      context.Context
	resp     *http.Response
	cancel   context.CancelFunc
	idle     time.Duration
	timer    *time.Timer
	timedOut atomic.Bool
	closed   bool
}

func (b *body) Read(p []byte) (int, error) {
	if b.timer != nil {
		b.timer.Reset(b.idle)
	}
	n, err := b.resp.Body.Read(p)
	if b.timer != nil {
		b.timer.Stop()
	}
	switch {
	case err == nil || err == io.EOF:
		return n, err
	case b.ctx.Err() != nil:
		return n, b.ctx.Err()
	case b.timedOut.Load():
		return n, errs.Connection("the storage network sent nothing for %s", b.idle)
	}
	return n, errs.Connection("the read broke off: %v", err)
}

// readExact reads n bytes, or fewer where the body ends.
func (b *body) readExact(n int64) ([]byte, error) {
	buf := make([]byte, n)
	got, err := io.ReadFull(b, buf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return buf[:got], nil
	}
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// readSome reads up to max bytes (at least one, or none at the end).
func (b *body) readSome(max int64) ([]byte, error) {
	buf := make([]byte, max)
	for {
		n, err := b.Read(buf)
		if n > 0 {
			return buf[:n], nil
		}
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// close ends the request: a body not read to its end is cancelled and its connection dropped. It never waits.
func (b *body) close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.timer != nil {
		b.timer.Stop()
	}
	b.cancel()
	b.resp.Body.Close()
}

// consumerError is an error from the caller's side of a read (a write that failed, or a reader that stopped): passed
// through as it is, never judged as a failure of the read.
type consumerError struct{ err error }

func (e *consumerError) Error() string { return e.err.Error() }

// tries counts one read's failures since its last verified bytes, and decides what each means.
type tries struct {
	r        *Reads
	s        *Session
	passing  int
	bad      int
	notFound bool
}

func (r *Reads) tries(s *Session) *tries { return &tries{r: r, s: s} }

func (t *tries) progressed() {
	t.passing = 0
	t.bad = 0
}

// failed returns nil when the read should try again (after any wait), or the error that ends it.
func (t *tries) failed(ctx context.Context, err error) error {
	var ce *consumerError
	if errors.As(err, &ce) {
		return err
	}
	if h := t.r.Hooks.Failed; h != nil {
		h(err)
	}
	if ctx.Err() != nil {
		return ctx.Err() // a cancel is never tried again, whatever the failure it caused looks like
	}
	if errors.Is(err, errs.ErrObjectChanged) {
		return err
	}
	var die *errs.DataIntegrityError
	if errors.As(err, &die) {
		t.bad++ // retried once: a flipped byte in transit, or for good
		if t.bad > 1 {
			return err
		}
		return nil
	}
	var ae *errs.APIError
	isAPI := errors.As(err, &ae)
	if isAPI && ae.StatusCode == http.StatusNotFound {
		// A server pack may have been compacted, so the bytes moved: sign again, once, without the cached key.
		t.r.Keys.Drop(t.s.Bucket, t.s.Key)
		if t.notFound {
			return &errs.APIError{StatusCode: http.StatusNotFound, Code: "object_not_found",
				Message: t.s.Key + ": the storage network doesn't have this object", RequestID: ae.RequestID}
		}
		t.notFound = true
		return nil
	}
	expired := errors.Is(err, errExpired)
	// The signature failed, not the key: sign again and keep the cached key.
	resign := expired || (isAPI && (ae.StatusCode == http.StatusUnauthorized || ae.StatusCode == http.StatusForbidden))
	var conn *errs.ConnError
	passing := errors.As(err, &conn) || (isAPI && (ae.StatusCode == http.StatusTooManyRequests || ae.StatusCode >= 500))
	if !resign && !passing {
		return err
	}
	t.passing++
	if t.passing > t.r.opts.MaxRetries {
		if expired {
			return fmt.Errorf("teepin: %s: every signature expired before it could be used; check this computer's clock", t.s.Key)
		}
		return err
	}
	if passing {
		if serr := t.r.opts.sleep(ctx, t.r.opts.backoff(t.passing-1, err)); serr != nil {
			return Stopped(ctx, serr)
		}
	}
	return nil
}

// --- verified bytes ---------------------------------------------------------------

// Plaintext gives emit the object's bytes [a, b) in order, each verified before it is given; with check (a whole
// read), the object's SHA-256 too, returning a checksum DataIntegrityError after the last bytes. Each slice emit gets
// is its own to keep. An error emit returns ends the read and is returned as it is.
func (r *Reads) Plaintext(ctx context.Context, s *Session, dek []byte, a, b int64, check bool, emit func([]byte) error) error {
	if h := r.Hooks.Plaintext; h != nil {
		h(s, dek, a, b)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	out := func(c []byte) error {
		if err := emit(c); err != nil {
			return &consumerError{err}
		}
		return nil
	}
	err := r.plaintext(ctx, s, dek, a, b, check, out)
	var ce *consumerError
	if errors.As(err, &ce) {
		return ce.err
	}
	return err
}

func (r *Reads) plaintext(ctx context.Context, s *Session, dek []byte, a, b int64, check bool, emit func([]byte) error) error {
	checked := check && s.Checkable()
	if len(s.Requests) == 0 {
		if s.Size != 0 {
			return errs.Integrity(errs.ReasonSession, "%s: the read session signed nothing", s.Key)
		}
		return nil
	}
	if s.Packed() || s.Format == FormatServer {
		data, err := r.whole(ctx, s, dek)
		if err != nil {
			return err
		}
		if checked {
			sum := sha256.Sum256(data)
			if got := hex.EncodeToString(sum[:]); got != s.SHA256 {
				return errs.Checksum(fmt.Sprintf("%s: received %s, stored %s", s.Key, got, s.SHA256))
			}
		}
		if b > a {
			return emit(data[a:b])
		}
		return nil
	}
	var h interface {
		io.Writer
		Sum([]byte) []byte
	}
	if checked {
		h = sha256.New()
	}
	take := func(c []byte) error {
		if h != nil {
			h.Write(c)
		}
		return emit(c)
	}
	var err error
	if s.Format == FormatTWS1 {
		if dek == nil {
			return errs.Integrity(errs.ReasonKey, "%s: the read session carried no key", s.Key)
		}
		err = r.tws1Blob(ctx, s, dek, a, b, take)
	} else {
		err = r.rawBlob(ctx, s, a, b, take)
	}
	if err != nil {
		return err
	}
	if h != nil {
		if got := hex.EncodeToString(h.Sum(nil)); got != s.SHA256 {
			return errs.Checksum(fmt.Sprintf("%s: received %s, stored %s", s.Key, got, s.SHA256))
		}
	}
	return nil
}

// whole is a packed object, its needle checked and decrypted whole; or a teepin-v1 object in its own blob, read
// whole and opened.
func (r *Reads) whole(ctx context.Context, s *Session, dek []byte) ([]byte, error) {
	var stored []byte
	var err error
	if s.Packed() {
		stored, err = r.packed(ctx, s)
	} else {
		stored, err = r.collect(func(emit func([]byte) error) error { return r.rawBlob(ctx, s, 0, s.StoredSize, emit) }, s.StoredSize)
	}
	if err != nil {
		return nil, err
	}
	var data []byte
	switch {
	case s.Format == FormatPlain:
		data = stored
	case dek == nil:
		return nil, errs.Integrity(errs.ReasonKey, "%s: the read session carried no key", s.Key)
	case s.Format == FormatTWS1:
		data, err = tws1.Decrypt(dek, stored)
		if err != nil {
			return nil, integrityOf(err)
		}
	default:
		if data, err = OpenServerV1(dek, stored, s.AD); err != nil {
			return nil, err
		}
	}
	if int64(len(data)) != s.Size {
		return nil, errs.Integrity(errs.ReasonSize, "%s: %d bytes, not the %d stored", s.Key, len(data), s.Size)
	}
	return data, nil
}

// collect is everything a read emits, joined.
func (r *Reads) collect(read func(emit func([]byte) error) error, size int64) ([]byte, error) {
	out := make([]byte, 0, max(0, size))
	err := read(func(c []byte) error {
		out = append(out, c...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Reads) packed(ctx context.Context, s0 *Session) ([]byte, error) {
	t := r.tries(s0)
	cur := s0
	for {
		stored, err := r.packedOnce(ctx, s0, cur)
		if err == nil {
			return stored, nil
		}
		if ferr := t.failed(ctx, err); ferr != nil {
			return nil, ferr
		}
		if cur, err = r.Window(ctx, s0, 0, 0); err != nil {
			return nil, err
		}
	}
}

func (r *Reads) packedOnce(ctx context.Context, s0, cur *Session) ([]byte, error) {
	w := cur.Window()
	b, err := r.open(ctx, w)
	if err != nil {
		return nil, err
	}
	data, err := b.readExact(w.Length())
	b.close()
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != w.Length() {
		return nil, errs.Connection("%d of %d bytes", len(data), w.Length())
	}
	if w.Prefix != tws1.NeedleHeaderSize {
		return nil, errs.Integrity(errs.ReasonNeedle, "%s: the read session signed a needle of %d bytes", s0.Key, w.Prefix)
	}
	id, err := ObjectIDBytes(s0.Version)
	if err != nil {
		return nil, errs.Integrity(errs.ReasonNeedle, "%s: the object's version %q is not its id", s0.Key, s0.Version)
	}
	stored, err := tws1.ReadNeedle(data, w.Prefix, id, int64(len(data))-w.Prefix)
	if err != nil {
		return nil, integrityOf(err)
	}
	return stored, nil
}

func (r *Reads) tws1Blob(ctx context.Context, s0 *Session, dek []byte, a, b int64, emit func([]byte) error) error {
	size := s0.Size
	rg, err := tws1.Locate(size, a, b-a)
	if err != nil {
		return errs.Integrity(errs.ReasonTruncated, "%s: %v", s0.Key, err)
	}
	seg := rg.FirstSegment
	last := seg
	if b > a {
		last = (b - 1) / tws1.SegmentSize
	}
	lastOff, lastLen := segmentAt(size, last)
	end := lastOff + lastLen // the stored end, exclusive
	skip, left := rg.Skip, rg.Length
	var header []byte // checked against the key
	limit := int64(DefaultWindow)
	t := r.tries(s0)
	var cur *Session
	if len(s0.Requests) > 0 && s0.Window().First == segmentStart(size, seg) {
		cur = s0
	}
	window := func() error {
		start := segmentStart(size, seg)
		var asked int64
		if cur == nil {
			asked = aligned(start, end, limit)
			w, err := r.Window(ctx, s0, start, asked)
			if err != nil {
				return err
			}
			cur = w
		}
		win := cur.Window()
		if win.First != start {
			return errs.Integrity(errs.ReasonWindow, "%s: a window signed from %d, not %d", s0.Key, win.First, start)
		}
		if asked > 0 && win.Last-win.First+1 < asked {
			limit = win.Last - win.First + 1 // the server's window
		}
		if header == nil && start > 0 {
			h, err := r.header(ctx, cur, dek, size)
			if err != nil {
				return err
			}
			header = h
		}
		body, err := r.open(ctx, win)
		if err != nil {
			return err
		}
		defer body.close()
		if start == 0 {
			head, err := body.readExact(tws1.HeaderSize)
			if err != nil {
				return err
			}
			if len(head) < tws1.HeaderSize {
				return errs.Connection("the header broke off")
			}
			if header == nil {
				if err := checkHeader(dek, head, size); err != nil {
					return err
				}
				header = head
			} else if subtle.ConstantTimeCompare(head, header) != 1 {
				return errs.Integrity(errs.ReasonHeader, "%s: the header changed", s0.Key)
			}
		}
		from := seg
		for seg <= last {
			soff, slen := segmentAt(size, seg)
			if soff+slen-1 > win.Last {
				break // the window ends inside this segment: the next one starts with it
			}
			data, err := body.readExact(slen)
			if err != nil {
				return err
			}
			if int64(len(data)) < slen {
				return errs.Connection("segment %d broke off", seg)
			}
			if header == nil {
				return errs.Integrity(errs.ReasonHeader, "%s: no header to open segment %d with", s0.Key, seg)
			}
			plain, err := openSegment(dek, header, size, seg, data)
			if err != nil {
				return err
			}
			seg++
			t.progressed()
			out := plain
			if skip > 0 || left < int64(len(plain)) {
				out = plain[min(skip, int64(len(plain))):min(skip+left, int64(len(plain)))]
			}
			skip = 0
			left -= int64(len(out))
			if len(out) > 0 {
				if err := emit(out); err != nil {
					return err
				}
			}
		}
		if seg == from && seg <= last {
			// A window holding no whole segment would be asked for again without end.
			return errs.Integrity(errs.ReasonWindow, "%s: a window from %d to %d holds no whole segment", s0.Key, win.First, win.Last)
		}
		return nil
	}
	for seg <= last {
		if err := window(); err != nil {
			if ferr := t.failed(ctx, err); ferr != nil {
				return ferr
			}
		}
		cur = nil
	}
	return nil
}

// header is the object's checked header, from the one a later window's session signs first.
func (r *Reads) header(ctx context.Context, cur *Session, dek []byte, size int64) ([]byte, error) {
	if len(cur.Requests) < 2 || cur.Requests[0].First != 0 {
		return nil, errs.Integrity(errs.ReasonHeader, "%s: the read session didn't sign the header", cur.Key)
	}
	b, err := r.open(ctx, &cur.Requests[0])
	if err != nil {
		return nil, err
	}
	head, err := b.readExact(tws1.HeaderSize)
	b.close()
	if err != nil {
		return nil, err
	}
	if len(head) < tws1.HeaderSize {
		return nil, errs.Connection("the header broke off")
	}
	if err := checkHeader(dek, head, size); err != nil {
		return nil, err
	}
	return head, nil
}

// rawBlob emits stored bytes [a, b) of an object in its own blob, as stored.
func (r *Reads) rawBlob(ctx context.Context, s0 *Session, a, b int64, emit func([]byte) error) error {
	pos := a
	limit := int64(DefaultWindow)
	t := r.tries(s0)
	var cur *Session
	if len(s0.Requests) > 0 && s0.Window().First == a {
		cur = s0
	}
	window := func() error {
		var asked int64
		if cur == nil {
			asked = min(b-pos, limit)
			w, err := r.Window(ctx, s0, pos, asked)
			if err != nil {
				return err
			}
			cur = w
		}
		win := cur.Window()
		if win.First != pos {
			return errs.Integrity(errs.ReasonWindow, "%s: a window signed from %d, not %d", s0.Key, win.First, pos)
		}
		if asked > 0 && win.Last-win.First+1 < asked {
			limit = win.Last - win.First + 1
		}
		want := min(b, win.Last+1) - pos
		if want <= 0 {
			return errs.Integrity(errs.ReasonWindow, "%s: a window from %d to %d holds nothing", s0.Key, win.First, win.Last)
		}
		body, err := r.open(ctx, win)
		if err != nil {
			return err
		}
		defer body.close()
		for want > 0 {
			chunk, err := body.readSome(min(readChunk, want))
			if err != nil {
				return err
			}
			if len(chunk) == 0 {
				return errs.Connection("%d bytes short", want)
			}
			pos += int64(len(chunk))
			want -= int64(len(chunk))
			t.progressed()
			if err := emit(chunk); err != nil {
				return err
			}
		}
		return nil
	}
	for pos < b {
		if err := window(); err != nil {
			if ferr := t.failed(ctx, err); ferr != nil {
				return ferr
			}
		}
		cur = nil
	}
	return nil
}

// aligned is the stored bytes to ask for from start: whole segments within cap (the header too from 0), at most to
// end.
func aligned(start, end, cap int64) int64 {
	var want int64
	if start == 0 {
		n := max(1, (cap-tws1.HeaderSize)/tws1.StoredSegmentSize)
		want = tws1.HeaderSize + n*tws1.StoredSegmentSize
	} else {
		want = max(1, cap/tws1.StoredSegmentSize) * tws1.StoredSegmentSize
	}
	return min(end-start, want)
}

// --- whole operations -----------------------------------------------------------------

// Ranges is how a download splits: one range per server window (partSize may split finer), or one range for an
// object read whole.
func (r *Reads) Ranges(s *Session, partSize int64) [][2]int64 {
	size := s.Size
	if s.Packed() || len(s.Requests) == 0 || s.Format == FormatServer || s.Window().Last+1 >= s.StoredSize {
		return [][2]int64{{0, size}}
	}
	stride := s.Window().Last - s.Window().First + 1
	if s.Format == FormatTWS1 {
		const seg = tws1.SegmentSize
		stride = max(1, (stride-tws1.HeaderSize)/tws1.StoredSegmentSize) * seg
		if partSize > 0 {
			stride = min(stride, max(seg, partSize/seg*seg))
		}
	} else if partSize > 0 {
		stride = min(stride, partSize)
	}
	var out [][2]int64
	for a := int64(0); a < size; a += stride {
		out = append(out, [2]int64{a, min(a+stride, size)})
	}
	return out
}

// Target is where a download writes.
type Target struct {
	// WriteAt writes p at off. Calls come from several goroutines at once, for ranges that never overlap.
	WriteAt func(p []byte, off int64) error
	// SHA256, when the target can read back what it holds, is the SHA-256 of its first size bytes in hex; nil when
	// it can't.
	SHA256 func(size int64) (string, error)
}

// DownloadParams are how a download runs.
type DownloadParams struct {
	// PartSize is plaintext bytes per range inside a window (0: one range per window).
	PartSize int64
	// Concurrency is how many ranges are fetched at once.
	Concurrency int
	// Progress is told the bytes written so far and the total.
	Progress func(done, total int64)
	// Verify checks the whole object's SHA-256.
	Verify bool
}

func isChecksum(err error) bool {
	var die *errs.DataIntegrityError
	return errors.As(err, &die) && die.Reason == errs.ReasonChecksum
}

// Download writes the whole object to the target open makes (once per attempt, given the object's session): windows
// in parallel, each pinned to the first session's version. A SHA-256 mismatch is tried again once. It returns the
// object's session (its key forgotten). An object still staged is errs.ErrObjectStaged.
func (r *Reads) Download(ctx context.Context, bucket, key string, open func(*Session) (*Target, error), p DownloadParams) (*Session, error) {
	var prog *Progress
	for attempt := 0; ; attempt++ {
		s, err := r.Session(ctx, bucket, key, 0, 0)
		if err != nil {
			return nil, err
		}
		if s.Staged() {
			s.Forget()
			return nil, staged(key)
		}
		dek := clone(s.DEK)
		s.Forget()
		if prog == nil {
			prog = NewProgress(s.Size, p.Progress)
		}
		err = r.downloadOnce(ctx, s, dek, open, p, prog)
		clear(dek)
		if err == nil {
			return s, nil
		}
		if !isChecksum(err) || attempt > 0 {
			return nil, Stopped(ctx, err)
		}
	}
}

func (r *Reads) downloadOnce(ctx context.Context, s *Session, dek []byte, open func(*Session) (*Target, error), p DownloadParams, prog *Progress) error {
	t, err := open(s)
	if err != nil {
		return err
	}
	ranges := r.Ranges(s, p.PartSize)
	if len(ranges) == 1 || p.Concurrency == 1 {
		return r.writeRange(ctx, s, dek, 0, s.Size, p.Verify, t, prog)
	}
	err = RunPool(ctx, p.Concurrency, ranges, func(ctx context.Context, rg [2]int64) error {
		return r.writeRange(ctx, s, dek, rg[0], rg[1], false, t, prog)
	})
	if err != nil {
		return err
	}
	if p.Verify && s.Checkable() && t.SHA256 != nil {
		got, err := t.SHA256(s.Size)
		if err != nil {
			return err
		}
		if got != s.SHA256 {
			return errs.Checksum(fmt.Sprintf("%s: received %s, stored %s", s.Key, got, s.SHA256))
		}
	}
	return nil
}

func (r *Reads) writeRange(ctx context.Context, s *Session, dek []byte, a, b int64, check bool, t *Target, prog *Progress) error {
	pos := a
	return r.Plaintext(ctx, s, dek, a, b, check, func(c []byte) error {
		if err := t.WriteAt(c, pos); err != nil {
			return err
		}
		pos += int64(len(c))
		prog.Add(int64(len(c)))
		return nil
	})
}

// Stream writes the whole object through write, in order: each piece verified before it is written, and with verify
// the whole object's SHA-256 checked at the end (bytes reach write before that check). It returns the object's
// session. An object still staged is errs.ErrObjectStaged.
func (r *Reads) Stream(ctx context.Context, bucket, key string, write func([]byte) error, progress func(done, total int64), verify bool) (*Session, error) {
	s, err := r.Session(ctx, bucket, key, 0, 0)
	if err != nil {
		return nil, err
	}
	if s.Staged() {
		s.Forget()
		return nil, staged(key)
	}
	dek := clone(s.DEK)
	s.Forget()
	defer clear(dek)
	prog := NewProgress(s.Size, progress)
	err = r.Plaintext(ctx, s, dek, 0, s.Size, verify, func(c []byte) error {
		if err := write(c); err != nil {
			return err
		}
		prog.Add(int64(len(c)))
		return nil
	})
	if err != nil {
		return nil, Stopped(ctx, err)
	}
	return s, nil
}

// Opened is a read about to stream: its session, its own copy of the key, and the object's bytes [A, B) it reads.
type Opened struct {
	Session *Session
	DEK     []byte
	A, B    int64
	// Ranged is set when a range was asked for, so the whole object's SHA-256 isn't checked.
	Ranged bool
}

// Get is the first part of GetObject: the session, checked against ifMatch before any byte is fetched, and the range
// to read, bytes start to end inclusive (end negative: to the end). The bytes come from Plaintext. A range past the
// end is a 416, and an object still staged errs.ErrObjectStaged.
func (r *Reads) Get(ctx context.Context, bucket, key string, start, end int64, ifMatch string) (*Opened, error) {
	ranged := start > 0 || end >= 0
	var length int64
	if ranged {
		length = tws1.HeaderSize + tws1.StoredSegmentSize // a range signs a little at first; its window comes later
	}
	s, err := r.Session(ctx, bucket, key, 0, length)
	if err != nil {
		return nil, err
	}
	if ifMatch != "" && s.ETag != unquoteETag(ifMatch) {
		s.Forget()
		return nil, errs.ErrObjectChanged
	}
	if s.Staged() {
		s.Forget()
		return nil, staged(key)
	}
	b := s.Size
	if end >= 0 {
		b = min(end+1, s.Size)
	}
	if start < 0 || (ranged && start >= s.Size) || start > b {
		s.Forget()
		return nil, &errs.APIError{StatusCode: http.StatusRequestedRangeNotSatisfiable, Code: "invalid_range",
			Message: fmt.Sprintf("%s: bytes %d- are past the object's %d bytes", key, start, s.Size)}
	}
	dek := clone(s.DEK)
	s.Forget()
	return &Opened{Session: s, DEK: dek, A: start, B: b, Ranged: ranged}, nil
}

// FileOpened is the first part of OpenObject: the session, its own copy of the key, the block a straight read fetches
// (one window, or the whole object) and the alignment of a jump.
type FileOpened struct {
	Session *Session
	DEK     []byte
	Block   int64
	Align   int64
}

// OpenFile opens an object for reads at any offset. An object still staged is errs.ErrObjectStaged.
func (r *Reads) OpenFile(ctx context.Context, bucket, key string) (*FileOpened, error) {
	s, err := r.Session(ctx, bucket, key, 0, 0)
	if err != nil {
		return nil, err
	}
	if s.Staged() {
		s.Forget()
		return nil, staged(key)
	}
	dek := clone(s.DEK)
	s.Forget()
	block := max(1, r.Ranges(s, 0)[0][1])
	align := int64(tws1.SegmentSize)
	if s.Packed() || s.Format == FormatServer {
		align = max(1, s.Size)
	}
	return &FileOpened{Session: s, DEK: dek, Block: block, Align: align}, nil
}

// Fetch is the verified bytes [a, b), joined; the whole object's SHA-256 is checked when that is what's asked for.
func (r *Reads) Fetch(ctx context.Context, s *Session, dek []byte, a, b int64) ([]byte, error) {
	return r.collect(func(emit func([]byte) error) error {
		return r.Plaintext(ctx, s, dek, a, b, a == 0 && b == s.Size, emit)
	}, b-a)
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

// staged is the error of a read of an object still staged on the API.
func staged(key string) error {
	return fmt.Errorf("%w: %s (it is readable once the stage has drained)", errs.ErrObjectStaged, key)
}
