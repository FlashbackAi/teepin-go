// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package fakes

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Bucket is the one bucket of the storage network the control plane signs for.
const Bucket = "b"

// The faults that decide a request's fate before it is looked at, in the order they're checked.
var networkFaults = []string{"fail", "reject", "badDigest", "gone", "drop", "lose"}

// Network is a fake storage network, behaving as the storage network's gateway did in the Phase 0 tests
// (TEEPIN-STORAGE-DESIGN.md section 19.13), with fake_shelby.py's rules:
//
//   - every request must carry a signature the control plane made (Sign), covering the method, path, query and every
//     signed header with its exact value; another value, a missing header or an expired signature answers 401;
//   - Content-MD5 is enforced: other bytes answer 400 BadDigest;
//   - If-None-Match: * refuses a key that exists: 412;
//   - a GET's signed Range is the only range it may read;
//   - multipart parts land under an upload the control plane created, and their ETag is their MD5.
//
// Faults, each counted down per request, by kind ("PUT", "PUT part", "GET"):
//
//   - Fail[kind]: answer 503 without acting;
//   - Reject[kind]: answer 401 without acting, as an expired or refused signature is answered;
//   - BadDigest[kind]: answer 400 BadDigest without acting, as a body damaged on the way is answered;
//   - Gone[kind]: answer 404 NoSuchUpload without acting, as a part sent to an upload the network no longer has is;
//   - Drop[kind]: drop the connection before acting (the outcome is then certainly "not stored");
//   - Lose[kind]: act, then drop the connection unanswered (the bytes are stored; the client can't know);
//   - WrongETag[kind]: store the bytes, but answer another ETag;
//   - Rot: a GET's body carries one flipped byte;
//   - Cut: a GET's body stops half way and the connection drops;
//   - Skew: added to the fake's clock (to expire signatures);
//   - Delay: how long each request waits before it's answered.
//
// Requests records every request's kind, path, query and headers, so tests can check that the Teepin API key never
// reaches the storage network.
type Network struct {
	mu     sync.Mutex
	secret []byte
	next   int

	Blobs map[string][]byte
	// ETags holds a multipart object's ETag (the MD5 of its part MD5s, then -N); a single PUT's is its MD5.
	ETags   map[string]string
	Uploads map[string]*Multipart

	Fail, Reject, BadDigest, Gone, Drop, Lose, WrongETag map[string]int
	Rot, Cut                                             int
	Skew, Delay                                          time.Duration

	// Hits counts requests by kind, faulted ones included.
	Hits     map[string]int
	Requests []NetworkRequest

	// URL is where it listens.
	URL    string
	server *httptest.Server
}

// NetworkRequest is one request the storage network received.
type NetworkRequest struct {
	Kind   string
	Path   string
	Query  map[string]string
	Header map[string]string
}

// Multipart is one multipart upload: the key it assembles into, and its parts by number.
type Multipart struct {
	Key   string
	Parts map[int][]byte
}

// PartInfo is one part a multipart upload holds.
type PartInfo struct {
	Number int
	Size   int64
	MD5    string
}

// PartRef names a part for CompleteMultipart.
type PartRef struct {
	Number int
	MD5    string
}

// Signed is a signed request, as the control plane returns it.
type Signed struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt string            `json:"expires_at"`
}

// StartNetwork starts a storage network on 127.0.0.1, on a port of its own.
func StartNetwork() *Network {
	n := &Network{secret: make([]byte, 32), Blobs: map[string][]byte{}, ETags: map[string]string{},
		Uploads: map[string]*Multipart{}, Fail: map[string]int{}, Reject: map[string]int{}, BadDigest: map[string]int{},
		Gone: map[string]int{}, Drop: map[string]int{}, Lose: map[string]int{}, WrongETag: map[string]int{},
		Hits: map[string]int{}}
	_, _ = rand.Read(n.secret)
	n.server = httptest.NewServer(n)
	n.URL = n.server.URL
	return n
}

// Close stops listening and drops every connection.
func (n *Network) Close() {
	n.server.CloseClientConnections()
	n.server.Close()
}

// Do runs fn under the network's lock, to change its fields while requests may be in flight.
func (n *Network) Do(fn func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	fn()
}

// Now is the fake's clock.
func (n *Network) Now() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return time.Now().Add(n.Skew)
}

// --- what the control plane does ------------------------------------------------

// quotePath is Python's urllib.parse.quote(s, safe='/'): everything but unreserved characters and "/" encoded.
func quotePath(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '/' || c == '-' || c == '_' || c == '.' || c == '~' || ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// canonicalQuery is a query's canonical form, for signing and for the signed URL: its pairs sorted, form-encoded.
func canonicalQuery(q map[string]string) string {
	keys := sortedKeys(q)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(q[k]))
	}
	return strings.Join(parts, "&")
}

func (n *Network) mac(method, path string, query map[string]string, headers map[string]string, names []string, expires int64) string {
	lines := []string{method, path, canonicalQuery(query)}
	for _, name := range names {
		lines = append(lines, name+":"+headers[name])
	}
	lines = append(lines, strconv.FormatInt(expires, 10))
	m := hmac.New(sha256.New, n.secret)
	m.Write([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(m.Sum(nil))
}

// Sign is a signed request, as the control plane returns it: method, URL, headers (every one signed, plus the
// authorization) and expiry, ttl from now.
func (n *Network) Sign(method, key string, headers, query map[string]string, ttl time.Duration) Signed {
	path := "/" + Bucket + "/" + quotePath(key)
	expires := n.Now().Add(ttl).Unix()
	signed := map[string]string{}
	for k, v := range headers {
		signed[strings.ToLower(k)] = v
	}
	names := sortedKeys(signed)
	sig := n.mac(method, path, query, signed, names, expires)
	signed["authorization"] = fmt.Sprintf("FAKE-HMAC signed=%s,expires=%d,sig=%s", strings.Join(names, ";"), expires, sig)
	q := ""
	if len(query) > 0 {
		q = "?" + canonicalQuery(query)
	}
	return Signed{Method: method, URL: n.URL + path + q, Headers: signed,
		ExpiresAt: time.Unix(expires, 0).UTC().Format("2006-01-02T15:04:05Z")}
}

var wholeNumber = regexp.MustCompile(`^\s*[+-]?\d+\s*$`)

// Verify is why a request's signature fails, or "" when it holds. headers are by lowercase name.
func (n *Network) Verify(method, path string, query, headers map[string]string) string {
	auth := headers["authorization"]
	if !strings.HasPrefix(auth, "FAKE-HMAC ") {
		return "no signature"
	}
	fields := map[string]string{}
	for _, p := range strings.Split(strings.TrimPrefix(auth, "FAKE-HMAC "), ",") {
		k, v, ok := strings.Cut(p, "=")
		if !ok {
			return "malformed signature"
		}
		fields[k] = v
	}
	signed, ok := fields["signed"]
	if !ok || !wholeNumber.MatchString(fields["expires"]) {
		return "malformed signature"
	}
	names := strings.Split(signed, ";")
	expires, _ := strconv.ParseInt(strings.TrimSpace(fields["expires"]), 10, 64)
	if float64(n.Now().UnixNano())/1e9 > float64(expires) {
		return "expired"
	}
	for _, name := range names {
		if _, ok := headers[name]; !ok {
			return "signed header " + name + " missing"
		}
	}
	want := []byte(n.mac(method, path, query, headers, names, expires))
	if subtle.ConstantTimeCompare(want, []byte(fields["sig"])) != 1 {
		return "signature does not match"
	}
	return ""
}

// CreateMultipart starts a multipart upload into key; it returns the upload's id.
func (n *Network) CreateMultipart(key string) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.next++
	id := fmt.Sprintf("mpu-%d", n.next)
	n.Uploads[id] = &Multipart{Key: key, Parts: map[int][]byte{}}
	return id
}

func md5hex(b []byte) string {
	s := md5.Sum(b)
	return hex.EncodeToString(s[:])
}

// ListParts is each part an upload holds, in order; none for an upload that's gone, as the storage network answers
// after an abort (19.13).
func (n *Network) ListParts(id string) []PartInfo {
	n.mu.Lock()
	defer n.mu.Unlock()
	up := n.Uploads[id]
	if up == nil {
		return nil
	}
	out := make([]PartInfo, 0, len(up.Parts))
	for num, b := range up.Parts {
		out = append(out, PartInfo{Number: num, Size: int64(len(b)), MD5: md5hex(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out
}

// CompleteMultipart assembles the parts named under If-None-Match: *. It answers "ok", "no_such_upload", "exists" or
// "invalid_part".
func (n *Network) CompleteMultipart(id string, parts []PartRef) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	up := n.Uploads[id]
	if up == nil {
		return "no_such_upload"
	}
	if _, ok := n.Blobs[up.Key]; ok {
		return "exists"
	}
	var data, digests []byte
	for _, p := range parts {
		b, ok := up.Parts[p.Number]
		if !ok || md5hex(b) != p.MD5 {
			return "invalid_part"
		}
		data = append(data, b...)
		d, _ := hex.DecodeString(p.MD5)
		digests = append(digests, d...)
	}
	n.Blobs[up.Key] = data
	n.ETags[up.Key] = fmt.Sprintf("%s-%d", md5hex(digests), len(parts))
	delete(n.Uploads, id)
	return "ok"
}

// AbortMultipart forgets an upload and its parts, and reports whether there was one.
func (n *Network) AbortMultipart(id string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.Uploads[id]
	delete(n.Uploads, id)
	return ok
}

// Head is the size and ETag of what is at key, if anything is.
func (n *Network) Head(key string) (int64, string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	b, ok := n.Blobs[key]
	if !ok {
		return 0, "", false
	}
	if e := n.ETags[key]; e != "" {
		return int64(len(b)), e, true
	}
	return int64(len(b)), md5hex(b), true
}

// Blob is a copy of what is at key (nil when nothing is).
func (n *Network) Blob(key string) []byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	b, ok := n.Blobs[key]
	if !ok {
		return nil
	}
	return bytes.Clone(b)
}

// SetBlob puts data at key, as if stored long ago.
func (n *Network) SetBlob(key string, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Blobs[key] = bytes.Clone(data)
	delete(n.ETags, key)
}

// DeleteBlob removes what is at key.
func (n *Network) DeleteBlob(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.Blobs, key)
	delete(n.ETags, key)
}

// RequestsOf is the requests received of a kind ("GET", "PUT", "PUT part"; "" for all).
func (n *Network) RequestsOf(kind string) []NetworkRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []NetworkRequest
	for _, r := range n.Requests {
		if kind == "" || r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

// HitsOf is the requests received of a kind.
func (n *Network) HitsOf(kind string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.Hits[kind]
}

// --- serving --------------------------------------------------------------------

// kindOf is a request's kind, as faults and hits count it.
func kindOf(method string, query map[string]string) string {
	if method == http.MethodPut {
		if _, ok := query["partNumber"]; ok {
			return "PUT part"
		}
	}
	return method
}

func (n *Network) table(name string) map[string]int {
	switch name {
	case "fail":
		return n.Fail
	case "reject":
		return n.Reject
	case "badDigest":
		return n.BadDigest
	case "gone":
		return n.Gone
	case "drop":
		return n.Drop
	}
	return n.Lose
}

func networkError(w http.ResponseWriter, status int, code, message string) {
	if message == "" {
		message = code
	}
	writeReply(w, &reply{status: status, header: map[string]string{"Content-Type": "application/xml"},
		body: []byte("<Error><Code>" + code + "</Code><Message>" + message + "</Message></Error>")})
}

// ServeHTTP answers one request to the storage network.
func (n *Network) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodHead {
		networkError(w, http.StatusNotImplemented, "NotImplemented", "unsupported method "+r.Method)
		return
	}
	path, query := splitURI(r)
	headers := lowerHeaders(r)
	kind := kindOf(r.Method, query)
	n.mu.Lock()
	n.Hits[kind]++
	n.Requests = append(n.Requests, NetworkRequest{Kind: kind, Path: path, Query: query, Header: headers})
	fault := ""
	for _, name := range networkFaults {
		if t := n.table(name); t[kind] > 0 {
			t[kind]--
			fault = name
			break
		}
	}
	delay := n.Delay
	n.mu.Unlock()
	if delay > 0 {
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-r.Context().Done():
			t.Stop()
			return // the client went away
		}
	}
	if fault == "drop" {
		abort()
	}
	var body []byte
	if r.Method == http.MethodPut {
		var err error
		if body, err = io.ReadAll(r.Body); err != nil {
			return // the client went away
		}
	}
	switch fault {
	case "fail":
		networkError(w, http.StatusServiceUnavailable, "ServiceUnavailable", "please retry")
		return
	case "reject":
		networkError(w, http.StatusUnauthorized, "InvalidSignature", "the signature has expired")
		return
	case "badDigest":
		networkError(w, http.StatusBadRequest, "BadDigest", "the Content-MD5 you specified did not match what we received")
		return
	case "gone":
		networkError(w, http.StatusNotFound, "NoSuchUpload", "the specified upload does not exist")
		return
	}
	if why := n.Verify(r.Method, path, query, headers); why != "" {
		networkError(w, http.StatusUnauthorized, "InvalidSignature", why)
		return
	}
	prefix := "/" + Bucket + "/"
	if !strings.HasPrefix(path, prefix) {
		networkError(w, http.StatusNotFound, "NoSuchBucket", "")
		return
	}
	key := unquote(path[len(prefix):])
	switch r.Method {
	case http.MethodPut:
		n.put(w, key, query, headers, body, fault == "lose")
	case http.MethodGet:
		n.get(w, key, headers, fault == "lose")
	default:
		networkError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "")
	}
}

func (n *Network) put(w http.ResponseWriter, key string, query, headers map[string]string, body []byte, lose bool) {
	if headers["content-length"] != strconv.Itoa(len(body)) {
		networkError(w, http.StatusBadRequest, "IncompleteBody", "")
		return
	}
	sum := md5.Sum(body)
	if headers["content-md5"] != b64(sum[:]) {
		networkError(w, http.StatusBadRequest, "BadDigest", "the Content-MD5 you specified did not match what we received")
		return
	}
	kind := "PUT"
	if num, part := query["partNumber"]; part {
		kind = "PUT part"
		n.mu.Lock()
		up := n.Uploads[query["uploadId"]]
		if up == nil {
			n.mu.Unlock()
			networkError(w, http.StatusNotFound, "NoSuchUpload", "")
			return
		}
		pn, err := strconv.Atoi(num)
		if err != nil || pn < 0 || strings.TrimLeft(num, "0123456789") != "" {
			n.mu.Unlock()
			networkError(w, http.StatusBadRequest, "InvalidArgument", "partNumber must be a number")
			return
		}
		up.Parts[pn] = body
		n.mu.Unlock()
	} else {
		n.mu.Lock()
		_, exists := n.Blobs[key]
		guarded := headers["if-none-match"] == "*"
		if !exists || !guarded {
			n.Blobs[key] = body
			delete(n.ETags, key)
		}
		n.mu.Unlock()
		if exists && guarded {
			if lose {
				abort()
			}
			networkError(w, http.StatusPreconditionFailed, "PreconditionFailed", "")
			return
		}
	}
	if lose {
		abort()
	}
	etag := hex.EncodeToString(sum[:])
	n.mu.Lock()
	if n.WrongETag[kind] > 0 {
		n.WrongETag[kind]--
		etag = md5hex([]byte("something else"))
	}
	n.mu.Unlock()
	writeReply(w, &reply{status: http.StatusOK, header: map[string]string{"ETag": `"` + etag + `"`}})
}

var closedRange = regexp.MustCompile(`^(\d+)-(\d+)$`)

func (n *Network) get(w http.ResponseWriter, key string, headers map[string]string, lose bool) {
	rng := headers["range"]
	n.mu.Lock()
	data, ok := n.Blobs[key]
	rot, cut := false, false
	if n.Cut > 0 {
		n.Cut--
		cut = true
	} else if n.Rot > 0 {
		n.Rot--
		rot = true
	}
	n.mu.Unlock()
	if !ok {
		networkError(w, http.StatusNotFound, "NoSuchKey", "")
		return
	}
	if !strings.HasPrefix(rng, "bytes=") {
		networkError(w, http.StatusBadRequest, "InvalidRange", "reads are signed for a range")
		return
	}
	m := closedRange.FindStringSubmatch(strings.TrimPrefix(rng, "bytes="))
	if m == nil {
		networkError(w, http.StatusBadRequest, "InvalidRange", "reads are signed for a closed range")
		return
	}
	start, _ := strconv.ParseInt(m[1], 10, 64)
	end, _ := strconv.ParseInt(m[2], 10, 64)
	end = min(end, int64(len(data))-1)
	if start > end {
		networkError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "")
		return
	}
	body := bytes.Clone(data[start : end+1])
	if rot && len(body) > 0 {
		body[len(body)/2] ^= 0xff
	}
	if lose {
		abort()
	}
	h := w.Header()
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusPartialContent)
	if cut {
		_, _ = w.Write(body[:len(body)/2])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		abort() // half the body, then the connection drops
	}
	_, _ = w.Write(body)
}
