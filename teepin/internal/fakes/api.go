// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package fakes

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// APIKey is the API key the fake API accepts, with OtherKey.
const APIKey = "tpk_test"

// OtherKey is a second key on the same account: the shared-engine tests (28.23) check two keys never share a pack.
const OtherKey = "tpk_test_other"

// MinPartSize is the old path's smallest part but the last.
const MinPartSize = 5 << 20

const bucketsPath = "/v1/storage/buckets"

// Object is an object in the fake catalog. A staged object's bytes are on the API (Data); a direct one's
// (Location "packed" or "blob") are on the storage network.
type Object struct {
	Data        []byte
	Size        int64
	ID          string
	ContentType string
	Metadata    map[string]string
	SHA         string
	Parts       int
	Updated     time.Time

	// ClientEncrypted: the API never serves or copies its bytes.
	ClientEncrypted bool
	// Location is "staged" on the API, or "packed" or "blob" on the storage network.
	Location string
	// Format is "tws1", "teepin-v1" or "plain" (a direct object's).
	Format string
	// Physical is the storage network's key.
	Physical   string
	StoredSize int64
	// Offset is where its stored bytes start in its pack (past the needle).
	Offset     int64
	Wrapped    []byte
	KeyVersion int
	ServerKey  []byte
	AD         []byte
}

// NewObject is an object staged on the API. sha "" is the data's SHA-256.
func NewObject(data []byte, contentType string, metadata map[string]string, sha string, parts int) *Object {
	if sha == "" {
		s := sha256.Sum256(data)
		sha = hex.EncodeToString(s[:])
	}
	if metadata == nil {
		metadata = map[string]string{}
	}
	return &Object{Data: bytes.Clone(data), Size: int64(len(data)), ContentType: contentType, Metadata: metadata,
		SHA: sha, Parts: parts, Updated: wholeSeconds(time.Now()), Location: "staged"}
}

// ETag is its ETag. E1 (owner, 2026-11-05): an object its client encrypted has the plain SHA-256 as its ETag,
// however it was uploaded.
func (o *Object) ETag() string {
	if o.ClientEncrypted || o.Parts == 0 {
		return o.SHA
	}
	return fmt.Sprintf("%s-%d", o.SHA, o.Parts)
}

// Direct reports an object whose bytes are on the storage network.
func (o *Object) Direct() bool { return o.Location == "packed" || o.Location == "blob" }

// Upload is an upload in parts on the old path: the parts are sent to the API.
type Upload struct {
	Bucket, Key, ContentType string
	Metadata                 map[string]string
	Parts                    map[int][]byte
}

// Canned is an answer a route gives before acting.
type Canned struct {
	Status int
	Body   any
	Header map[string]string
}

// APIRequest is one request the API received.
type APIRequest struct {
	Route  string
	Header map[string]string
	Path   string
	Query  map[string]string
}

// API is the fake Teepin Storage API, the storage half of fake_api.py: buckets and objects (staged on the API, or
// packed or in a blob of their own once Direct stores them), the old routes that remain beside the direct ones (an
// empty object is PUT, a staged object is read from object/content, and a client-encrypted object is refused there
// and by copy), and the old multipart uploads a client without direct storage uses. Signed links are left out.
//
// Faults, each counted down per request, by route name (Route):
//
//   - Fail[route]: answer 503 storage_busy (Retry-After: 1) without acting;
//   - Lose[route]: act on the request, then drop the connection unanswered;
//   - FailAfter[route]: act on the request, then answer 502 (a backend that failed after it may have acted);
//   - Canned[route]: answer these first, without acting;
//   - Cut: content answers stop half way and the connection drops;
//   - Rot: content answers carry one flipped byte;
//   - OnContent: called after each content answer (to replace an object mid-download, say);
//   - BeforeContent: called as each content request begins, before the object is looked up.
type API struct {
	mu      sync.Mutex
	Buckets map[string]map[string]*Object
	Uploads map[string]*Upload
	nextID  int

	Fail, Lose, FailAfter map[string]int
	Canned                map[string][]Canned
	Cut, Rot              int
	OnContent             func()
	BeforeContent         func()
	// PartDelay is how long an old-path part waits once its body has arrived.
	PartDelay             time.Duration
	Inflight, MaxInflight int

	// Hits counts requests by route name, faulted ones included.
	Hits     map[string]int
	Requests []APIRequest
	// Errors are a route's own failures (answered 500): a bug of the fakes, which a test can assert never happened.
	Errors []error

	// Links are the share links minted (POST object/link), in order.
	Links []Link

	// Direct holds the direct routes, once attached (StartDirect).
	Direct *Direct

	URL    string
	server *httptest.Server
}

// Link is a share link the API minted: its object, how long it works, its disposition and its token.
type Link struct {
	Bucket, Key, Disposition, Token string
	TTL                             int64
}

// StartAPI starts an API with no direct routes (it answers 501 for them, as a deployment with direct uploads off).
func StartAPI() *API {
	a := &API{Buckets: map[string]map[string]*Object{}, Uploads: map[string]*Upload{}, Fail: map[string]int{},
		Lose: map[string]int{}, FailAfter: map[string]int{}, Canned: map[string][]Canned{}, Hits: map[string]int{}}
	a.server = httptest.NewServer(a)
	a.URL = a.server.URL
	return a
}

// Close stops listening and drops every connection.
func (a *API) Close() {
	a.server.CloseClientConnections()
	a.server.Close()
}

// Do runs fn under the API's lock (which also guards Direct's state), to change fields while requests may be in
// flight.
func (a *API) Do(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fn()
}

// Bucket is a bucket's objects, the bucket made if it isn't there. The caller holds the lock, or no request is in
// flight.
func (a *API) Bucket(name string) map[string]*Object {
	b := a.Buckets[name]
	if b == nil {
		b = map[string]*Object{}
		a.Buckets[name] = b
	}
	return b
}

// Object is the object at bucket/key, or nil.
func (a *API) Object(bucket, key string) *Object {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.Buckets[bucket][key]
}

// PutObject stores an object directly, as if uploaded (staged on the API).
func (a *API) PutObject(bucket, key string, data []byte, contentType string) *Object {
	a.mu.Lock()
	defer a.mu.Unlock()
	o := NewObject(data, contentType, nil, "", 0)
	a.Bucket(bucket)[key] = o
	return o
}

// HitsOf is how many requests a route has had.
func (a *API) HitsOf(route string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.Hits[route]
}

// ObjectJSON is an object's details, as the API answers them.
func (a *API) ObjectJSON(key string, o *Object) map[string]any {
	t := iso(o.Updated)
	id := o.ID
	if id == "" {
		id = "obj-" + key
	}
	m := map[string]any{"id": id, "key": key, "size_bytes": o.Size, "content_type": o.ContentType,
		"metadata": o.Metadata, "checksum_sha256": o.SHA, "etag": o.ETag(), "status": "available", "backend": "fake",
		"created_at": t, "updated_at": t, "uploaded_at": t}
	if o.Parts > 0 {
		m["part_count"] = o.Parts
	}
	return m
}

// Route names a request like "PUT part" for faults and counts (fake_api.route, from the raw path).
func Route(method, path string) string {
	if !strings.HasPrefix(path, bucketsPath) {
		if rest, ok := strings.CutPrefix(path, "/v1/storage/"); ok {
			return method + " " + strings.Split(rest, "/")[0]
		}
		return path
	}
	trimmed := strings.Trim(path[len(bucketsPath):], "/")
	if trimmed == "" {
		return method + " buckets"
	}
	seg := strings.Split(trimmed, "/")
	switch {
	case len(seg) == 1:
		return method + " bucket"
	case len(seg) == 4 && seg[1] == "packs":
		return method + " pack " + seg[3]
	case len(seg) == 6 && seg[1] == "uploads" && seg[3] == "parts" && seg[5] == "sign":
		return method + " part sign"
	case len(seg) == 5 && seg[1] == "uploads" && seg[3] == "parts":
		return method + " part"
	case len(seg) >= 4 && seg[1] == "uploads" && seg[3] == "complete":
		return method + " complete"
	case len(seg) == 4 && seg[1] == "uploads":
		return method + " " + seg[3]
	case len(seg) == 3 && seg[1] == "uploads":
		return method + " upload"
	}
	return method + " " + strings.Join(seg[1:], "/")
}

// exchange is one request, as the routes read it.
type exchange struct {
	method string
	path   string
	query  map[string]string
	header map[string]string
	body   []byte
}

func (ex *exchange) json() (map[string]any, *reply) {
	m, ok := readJSON(ex.body)
	if !ok {
		return nil, apiError(http.StatusBadRequest, "invalid_request", nil, nil)
	}
	return m, nil
}

// ServeHTTP answers one request to the API.
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path, query := splitURI(r)
	name := Route(r.Method, path)
	header := lowerHeaders(r)
	a.mu.Lock()
	a.Hits[name]++
	a.Requests = append(a.Requests, APIRequest{Route: name, Header: header, Path: path, Query: query})
	failing := a.Fail[name] > 0
	if failing {
		a.Fail[name]--
	}
	lost, failAfter := false, false
	var canned *Canned
	if !failing && a.Lose[name] > 0 {
		a.Lose[name]--
		lost = true
	}
	if !failing && a.FailAfter[name] > 0 {
		a.FailAfter[name]--
		failAfter = true
	}
	if !failing && len(a.Canned[name]) > 0 {
		c := a.Canned[name][0]
		a.Canned[name] = a.Canned[name][1:]
		canned = &c
	}
	a.mu.Unlock()

	if _, ok := r.Header["Content-Length"]; !ok && r.ContentLength < 0 && len(r.TransferEncoding) > 0 {
		writeReply(w, apiError(http.StatusLengthRequired, "length_required", nil, nil))
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return // the client went away
	}
	ex := &exchange{method: r.Method, path: path, query: query, header: header, body: body}
	rep := a.answer(ex, canned, failing)
	if lost {
		abort()
	}
	if failAfter && rep.status < 400 {
		rep = refusal(http.StatusBadGateway, "storage_backend_error", "storage backend error", nil, nil)
	}
	if rep.content != nil {
		a.writeContent(w, r, rep)
		return
	}
	writeReply(w, rep)
}

// answer works out a request's answer. A route that fails (a fake's own bug) is recorded and answered 500.
func (a *API) answer(ex *exchange, canned *Canned, failing bool) (rep *reply) {
	defer func() {
		if v := recover(); v != nil {
			if v == http.ErrAbortHandler {
				panic(v)
			}
			a.mu.Lock()
			a.Errors = append(a.Errors, fmt.Errorf("%s %s: %v", ex.method, ex.path, v))
			a.mu.Unlock()
			rep = apiError(http.StatusInternalServerError, "internal_error", nil, nil)
		}
	}()
	switch {
	case canned != nil:
		body := canned.Body
		if body == nil {
			body = map[string]any{}
		}
		return jsonReply(canned.Status, body, canned.Header)
	case failing:
		return apiError(http.StatusServiceUnavailable, "storage_busy", map[string]string{"Retry-After": "1"}, nil)
	case ex.header["authorization"] != "Bearer "+APIKey && ex.header["authorization"] != "Bearer "+OtherKey:
		return apiError(http.StatusUnauthorized, "unauthorized", nil, nil)
	case ex.path == "/v1/storage/hold":
		return jsonReply(http.StatusOK, map[string]any{"held": false}, nil)
	case ex.path == "/v1/storage/keys/account" && ex.method == http.MethodGet:
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.Direct == nil {
			return apiError(http.StatusNotImplemented, "direct_upload_unsupported", nil, nil)
		}
		return jsonReply(http.StatusOK, a.Direct.AccountKey(), nil)
	case !strings.HasPrefix(ex.path, bucketsPath):
		return apiError(http.StatusNotFound, "not_found", nil, nil)
	}
	return a.storage(ex)
}

func (a *API) storage(ex *exchange) *reply {
	m := ex.method
	trimmed := strings.Trim(ex.path[len(bucketsPath):], "/")
	var seg []string
	if trimmed != "" {
		for _, s := range strings.Split(trimmed, "/") {
			seg = append(seg, unquote(s))
		}
	}
	if len(seg) == 0 {
		return a.bucketsRoot(ex)
	}
	bucket := seg[0]
	rest := strings.Join(seg[1:], "/")
	if rest == "object/content" {
		a.mu.Lock()
		before := a.BeforeContent
		a.mu.Unlock()
		if before != nil {
			before()
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	objs, ok := a.Buckets[bucket]
	if !ok && !(rest == "" && m == http.MethodGet) {
		return apiError(http.StatusNotFound, "bucket_not_found", nil, nil)
	}
	key := ex.query["key"]
	if a.Direct != nil {
		if rep := a.Direct.route(ex, bucket, seg[1:]); rep != nil {
			return rep
		}
	} else if (len(seg) > 1 && seg[1] == "packs") || rest == "object/read" || rest == "objects/read" || rest == "object/empty" {
		return apiError(http.StatusNotImplemented, "direct_upload_unsupported", nil, nil) // as the API answers with direct uploads off
	}
	if rest == "object/content" && objs[key] != nil && objs[key].ClientEncrypted {
		return apiError(http.StatusConflict, "client_encrypted", nil, nil)
	}
	switch {
	case rest == "" && m == http.MethodGet:
		if !ok {
			return apiError(http.StatusNotFound, "bucket_not_found", nil, nil)
		}
		return jsonReply(http.StatusOK, map[string]any{"id": "b-" + bucket, "name": bucket, "backend": "fake", "object_count": len(objs)}, nil)
	case rest == "" && m == http.MethodDelete:
		if len(objs) > 0 {
			return apiError(http.StatusConflict, "bucket_not_empty", nil, nil)
		}
		delete(a.Buckets, bucket)
		return &reply{status: http.StatusNoContent}
	case rest == "objects":
		return a.list(ex, objs)
	case rest == "object" && m == http.MethodPut:
		return a.put(ex, objs, key)
	case rest == "object" && m == http.MethodGet:
		if o := objs[key]; o != nil {
			return jsonReply(http.StatusOK, a.ObjectJSON(key, o), nil)
		}
		return apiError(http.StatusNotFound, "object_not_found", nil, nil)
	case rest == "object" && m == http.MethodDelete:
		if objs[key] == nil {
			return apiError(http.StatusNotFound, "object_not_found", nil, nil) // with or without a condition
		}
		// If-Match deletes only the version it names (quoted or not, or "*" for any): a move's check-then-delete.
		if im := strings.TrimPrefix(strings.TrimSpace(ex.header["if-match"]), "W/"); im != "" && im != "*" && stripQuotes(im) != objs[key].ETag() {
			return apiError(http.StatusPreconditionFailed, "precondition_failed", nil, nil)
		}
		delete(objs, key)
		return &reply{status: http.StatusNoContent}
	case rest == "object/content":
		return a.content(ex, objs[key])
	case rest == "objects/delete":
		req, rep := ex.json()
		if rep != nil {
			return rep
		}
		deleted, failed := []string{}, []map[string]any{}
		keys, _ := req["keys"].([]any)
		for _, kv := range keys {
			k := str(kv)
			if objs[k] == nil {
				failed = append(failed, map[string]any{"key": k, "code": "object_not_found", "error": "object not found"})
				continue
			}
			delete(objs, k)
			deleted = append(deleted, k)
		}
		return jsonReply(http.StatusOK, map[string]any{"deleted": deleted, "errors": failed}, nil)
	case rest == "object/rename":
		req, rep := ex.json()
		if rep != nil {
			return rep
		}
		from, to := str(req["from"]), str(req["to"])
		o := objs[from]
		if o == nil {
			return apiError(http.StatusNotFound, "object_not_found", nil, nil)
		}
		if objs[to] != nil && isFalse(req["overwrite"]) {
			return apiError(http.StatusConflict, "object_exists", nil, nil)
		}
		delete(objs, from)
		objs[to] = o
		return jsonReply(http.StatusOK, a.ObjectJSON(to, o), nil)
	case rest == "object/copy":
		req, rep := ex.json()
		if rep != nil {
			return rep
		}
		src := a.Buckets[str(req["source_bucket"])][str(req["source_key"])]
		if src == nil {
			return apiError(http.StatusNotFound, "object_not_found", nil, nil)
		}
		if src.ClientEncrypted {
			return apiError(http.StatusConflict, "client_encrypted", nil, nil)
		}
		dst := str(req["key"])
		if ex.header["if-none-match"] == "*" && objs[dst] != nil {
			return apiError(http.StatusPreconditionFailed, "precondition_failed", nil, nil)
		}
		meta := map[string]string{}
		for k, v := range src.Metadata {
			meta[k] = v
		}
		cp := NewObject(src.Data, src.ContentType, meta, src.SHA, src.Parts)
		objs[dst] = cp
		return jsonReply(http.StatusOK, a.ObjectJSON(dst, cp), nil)
	case rest == "object/link" && m == http.MethodPost:
		return a.link(ex, bucket, objs)
	case strings.HasPrefix(rest, "uploads"):
		return a.uploads(ex, bucket, seg[1:])
	}
	return apiError(http.StatusNotFound, "not_found", nil, nil)
}

// link is POST object/link (TEEPIN-STORAGE-DESIGN.md section 26.2): {key, ttl_seconds, disposition} to {url,
// expires_at}, for an object that exists. ttl_seconds is 1 to 86,400 (default 900), disposition inline (the default) or
// attachment. The lock is held.
func (a *API) link(ex *exchange, bucket string, objs map[string]*Object) *reply {
	req, rep := ex.json()
	if rep != nil {
		return rep
	}
	key := str(req["key"])
	ttl := int64(900)
	if v, ok := req["ttl_seconds"]; ok && v != nil {
		n, ok := integer(v)
		if !ok || n < 1 || n > 86_400 {
			return invalid("ttl_seconds must be 1 to 86400")
		}
		ttl = n
	}
	disp := "inline"
	if v, ok := req["disposition"]; ok && v != nil {
		disp = str(v)
		if disp != "inline" && disp != "attachment" {
			return invalid(`disposition must be "inline" or "attachment"`)
		}
	}
	if objs[key] == nil {
		return apiError(http.StatusNotFound, "object_not_found", nil, nil)
	}
	token := b32()
	a.Links = append(a.Links, Link{Bucket: bucket, Key: key, Disposition: disp, Token: token, TTL: ttl})
	return jsonReply(http.StatusOK, map[string]any{"url": a.URL + "/l/" + token,
		"expires_at": iso(time.Now().Add(time.Duration(ttl) * time.Second))}, nil)
}

func (a *API) bucketsRoot(ex *exchange) *reply {
	a.mu.Lock()
	defer a.mu.Unlock()
	if ex.method == http.MethodPost {
		req, rep := ex.json()
		if rep != nil {
			return rep
		}
		name := str(req["name"])
		if _, ok := a.Buckets[name]; ok {
			return apiError(http.StatusConflict, "bucket_exists", nil, nil)
		}
		a.Buckets[name] = map[string]*Object{}
		return jsonReply(http.StatusCreated, map[string]any{"id": "b-" + name, "name": name, "backend": "fake", "created_at": iso(time.Now())}, nil)
	}
	out := []map[string]any{}
	for _, n := range sortedKeys(a.Buckets) {
		out = append(out, map[string]any{"id": "b-" + n, "name": n, "backend": "fake"})
	}
	return jsonReply(http.StatusOK, map[string]any{"buckets": out}, nil)
}

func (a *API) list(ex *exchange, objs map[string]*Object) *reply {
	prefix, cursor, delim := ex.query["prefix"], ex.query["cursor"], ex.query["delimiter"]
	limit, err := strconv.Atoi(ex.query["limit"])
	if err != nil || limit <= 0 || limit > 1000 {
		limit = 1000
	}
	seen := map[string]bool{}
	var entries []string
	for k := range objs {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		e := k
		if delim != "" {
			if i := strings.Index(k[len(prefix):], delim); i >= 0 {
				e = k[:len(prefix)+i+len(delim)]
			}
		}
		if e > cursor && !seen[e] {
			seen[e] = true
			entries = append(entries, e)
		}
	}
	sort.Strings(entries)
	var next any
	if len(entries) > limit {
		entries = entries[:limit]
		next = entries[limit-1]
	}
	out, prefixes := []map[string]any{}, []string{}
	for _, e := range entries {
		if o := objs[e]; o != nil && !(delim != "" && strings.HasSuffix(e, delim) && len(e) > len(prefix)) {
			out = append(out, a.ObjectJSON(e, o))
		} else {
			prefixes = append(prefixes, e)
		}
	}
	body := map[string]any{"objects": out, "next_cursor": next}
	if delim != "" {
		body["prefixes"] = prefixes
	}
	return jsonReply(http.StatusOK, body, nil)
}

func (a *API) put(ex *exchange, objs map[string]*Object, key string) *reply {
	if _, ok := ex.header["content-length"]; !ok {
		return apiError(http.StatusLengthRequired, "length_required", nil, nil)
	}
	cur := objs[key]
	if ex.header["if-none-match"] == "*" && cur != nil {
		return apiError(http.StatusPreconditionFailed, "precondition_failed", nil, nil)
	}
	if im := ex.header["if-match"]; im != "" && (cur == nil || (im != "*" && stripQuotes(im) != cur.ETag())) {
		return apiError(http.StatusPreconditionFailed, "precondition_failed", nil, nil)
	}
	meta := map[string]string{}
	for k, v := range ex.header {
		if name, ok := strings.CutPrefix(k, "x-teepin-meta-"); ok {
			meta[name] = v
		}
	}
	o := NewObject(ex.body, ex.header["content-type"], meta, "", 0)
	objs[key] = o
	return jsonReply(http.StatusOK, a.ObjectJSON(key, o), map[string]string{"ETag": `"` + o.ETag() + `"`})
}

// contentReply is the content route's answer, decided under the lock and written after it.
type contentReply struct {
	status int
	header map[string]string
	body   []byte
	head   bool
	cut    bool
}

func (a *API) content(ex *exchange, o *Object) *reply {
	if o == nil {
		return apiError(http.StatusNotFound, "object_not_found", nil, nil)
	}
	etag := `"` + o.ETag() + `"`
	cut, rot := false, false
	if ex.method == http.MethodGet && a.Cut > 0 {
		a.Cut--
		cut = true
	}
	if ex.method == http.MethodGet && !cut && a.Rot > 0 {
		a.Rot--
		rot = true
	}
	ct := o.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	h := map[string]string{"ETag": etag, "Content-Type": ct, "Last-Modified": o.Updated.UTC().Format(http.TimeFormat),
		"Accept-Ranges": "bytes"}
	if inm := ex.header["if-none-match"]; inm != "" && inm == etag {
		return &reply{status: http.StatusNotModified, header: h}
	}
	data := o.Data
	start, end := int64(0), int64(len(data))-1
	status := http.StatusOK
	if rg := ex.header["range"]; rg != "" && (ex.header["if-range"] == "" || ex.header["if-range"] == etag) {
		spec := strings.TrimPrefix(rg, "bytes=")
		as, bs, _ := strings.Cut(spec, "-")
		start, _ = strconv.ParseInt(as, 10, 64)
		if bs != "" {
			end, _ = strconv.ParseInt(bs, 10, 64)
		}
		end = min(end, int64(len(data))-1)
		status = http.StatusPartialContent
		h["Content-Range"] = fmt.Sprintf("bytes %d-%d/%d", start, end, len(data))
	}
	var body []byte
	if start <= end {
		body = bytes.Clone(data[start : end+1])
	}
	if rot && len(body) > 0 {
		body[len(body)/2] ^= 0xff
	}
	return &reply{status: status, content: &contentReply{status: status, header: h, body: body,
		head: ex.method == http.MethodHead, cut: cut}}
}

func (a *API) writeContent(w http.ResponseWriter, r *http.Request, rep *reply) {
	c := rep.content
	h := w.Header()
	for k, v := range c.header {
		h.Set(k, v)
	}
	h.Set("X-Request-ID", "req-fake")
	h.Set("Content-Length", strconv.Itoa(len(c.body)))
	w.WriteHeader(c.status)
	if c.head {
		return
	}
	a.mu.Lock()
	after := a.OnContent
	a.mu.Unlock()
	if c.cut {
		_, _ = w.Write(c.body[:len(c.body)/2])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if after != nil {
			after() // before the client can see the break
		}
		abort()
	}
	_, _ = w.Write(c.body)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	if after != nil {
		after()
	}
}

// uploads are the old path's uploads in parts, and a direct create (handed to Direct).
func (a *API) uploads(ex *exchange, bucket string, seg []string) *reply {
	m := ex.method
	if len(seg) == 1 && m == http.MethodPost {
		req, rep := ex.json()
		if rep != nil {
			return rep
		}
		if truthy(req["direct"]) {
			if a.Direct == nil {
				return apiError(http.StatusNotImplemented, "direct_upload_unsupported", nil, nil)
			}
			return a.Direct.createUpload(bucket, req)
		}
		a.nextID++
		uid := fmt.Sprintf("00000000-0000-0000-0000-%012d", a.nextID)
		a.Uploads[uid] = &Upload{Bucket: bucket, Key: str(req["key"]), ContentType: str(req["content_type"]),
			Metadata: stringMap(req["metadata"]), Parts: map[int][]byte{}}
		return jsonReply(http.StatusCreated, map[string]any{"upload_id": uid, "key": str(req["key"]),
			"content_type": str(req["content_type"]), "created_at": iso(time.Now()),
			"expires_at": iso(time.Now().Add(7 * 24 * time.Hour))}, nil)
	}
	if len(seg) == 1 && m == http.MethodGet {
		prefix := ex.query["prefix"]
		var ids []string
		for id, u := range a.Uploads {
			if u.Bucket == bucket && strings.HasPrefix(u.Key, prefix) {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		out := []map[string]any{}
		for _, id := range ids {
			out = append(out, map[string]any{"upload_id": id, "key": a.Uploads[id].Key, "content_type": a.Uploads[id].ContentType})
		}
		return jsonReply(http.StatusOK, map[string]any{"uploads": out}, nil)
	}
	uid := seg[1]
	up := a.Uploads[uid]
	if up == nil {
		return apiError(http.StatusNotFound, "upload_not_found", nil, nil)
	}
	switch {
	case len(seg) == 2 && m == http.MethodDelete:
		delete(a.Uploads, uid)
		return &reply{status: http.StatusNoContent}
	case len(seg) == 3 && seg[2] == "parts":
		var nums []int
		for n := range up.Parts {
			nums = append(nums, n)
		}
		sort.Ints(nums)
		parts := []map[string]any{}
		for _, n := range nums {
			parts = append(parts, map[string]any{"part_number": n, "size_bytes": len(up.Parts[n]), "etag": md5hex(up.Parts[n])})
		}
		return jsonReply(http.StatusOK, map[string]any{"upload_id": uid, "key": up.Key, "content_type": up.ContentType, "parts": parts}, nil)
	case len(seg) == 4 && seg[2] == "parts":
		if _, ok := ex.header["content-length"]; !ok {
			return apiError(http.StatusLengthRequired, "length_required", nil, nil)
		}
		a.Inflight++
		a.MaxInflight = max(a.MaxInflight, a.Inflight)
		if d := a.PartDelay; d > 0 {
			a.mu.Unlock()
			time.Sleep(d)
			a.mu.Lock()
		}
		a.Inflight--
		num, _ := strconv.Atoi(seg[3])
		up.Parts[num] = bytes.Clone(ex.body)
		sum := md5hex(ex.body)
		return jsonReply(http.StatusOK, map[string]any{"part_number": num, "size_bytes": len(ex.body), "etag": sum,
			"uploaded_at": iso(time.Now())}, map[string]string{"ETag": `"` + sum + `"`})
	case len(seg) == 3 && seg[2] == "complete":
		req, rep := ex.json()
		if rep != nil {
			return rep
		}
		parts, _ := req["parts"].([]any)
		var data []byte
		composite := sha256.New()
		for i, pv := range parts {
			p, _ := pv.(map[string]any)
			n, _ := integer(p["part_number"])
			b, ok := up.Parts[int(n)]
			if !ok || md5hex(b) != stripQuotes(str(p["etag"])) || (i < len(parts)-1 && len(b) < MinPartSize) {
				return apiError(http.StatusBadRequest, "invalid_part", nil, nil)
			}
			data = append(data, b...)
			s := sha256.Sum256(b)
			composite.Write(s[:])
		}
		o := NewObject(data, up.ContentType, up.Metadata, hex.EncodeToString(composite.Sum(nil)), len(parts))
		a.Bucket(bucket)[up.Key] = o
		delete(a.Uploads, uid)
		return jsonReply(http.StatusOK, a.ObjectJSON(up.Key, o), nil)
	}
	return apiError(http.StatusNotFound, "not_found", nil, nil)
}
