// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

// Package fakes is an in-memory Teepin control plane and storage network for the direct path's tests: a Go port of
// the Node SDK's test fakes (test/fake-api.mjs, fake-direct.mjs, fake-shelby.mjs and fake-http.mjs), which port the
// Python SDK's (tests/fake_api.py, fake_direct.py, fake_shelby.py), with their rules and faults.
//
// It is an ordinary package so every test package of the SDK can share it, and only tests import it. Each fake is an
// httptest server on 127.0.0.1; nothing here calls a real network service.
//
// The fakes run requests concurrently, as a real server does. Every field a request reads or changes is guarded by
// its fake's lock: change one while requests may be in flight inside Do.
package fakes

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// reply is an answer a route works out under its fake's lock, written once the lock is let go.
type reply struct {
	status int
	body   []byte
	header map[string]string
	// content is the content route's answer, written a piece at a time (it may be cut or rotten).
	content *contentReply
}

// jsonReply answers v as JSON.
func jsonReply(status int, v any, header map[string]string) *reply {
	h := map[string]string{"Content-Type": "application/json"}
	for k, x := range header {
		h[k] = x
	}
	return &reply{status: status, body: marshal(v), header: h}
}

// apiError answers as the API does: {"error": the code in words, "code": code, ...extra}.
func apiError(status int, code string, header map[string]string, extra map[string]any) *reply {
	return refusal(status, code, "", header, extra)
}

// refusal answers {"error": message or the code in words, "code": code, ...extra}.
func refusal(status int, code, message string, header map[string]string, extra map[string]any) *reply {
	if message == "" {
		message = strings.ReplaceAll(code, "_", " ")
	}
	body := map[string]any{"error": message, "code": code}
	for k, v := range extra {
		body[k] = v
	}
	return jsonReply(status, body, header)
}

// marshal is v as JSON, keys sorted (Go's encoding of maps), so equal values give equal bytes.
func marshal(v any) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	return bytes.TrimRight(b.Bytes(), "\n")
}

// canonical is v as canonical JSON (keys sorted), to compare requests by value, as the Python fake's
// json.dumps(v, sort_keys=True) does.
func canonical(v any) string { return string(marshal(v)) }

// abort drops the connection, unanswered (or mid-answer, if the answer has begun). net/http recovers this panic
// without logging it.
func abort() { panic(http.ErrAbortHandler) }

// splitURI is a request target's raw path (still percent-encoded, as signed) and its query's first value per name.
func splitURI(r *http.Request) (string, map[string]string) {
	uri := r.RequestURI
	if uri == "" {
		uri = r.URL.RequestURI()
	}
	path, raw, _ := strings.Cut(uri, "?")
	return path, firstValues(raw)
}

// firstValues is the first value of each name in a raw query, as the Python fakes read queries.
func firstValues(raw string) map[string]string {
	out := map[string]string{}
	vals, _ := url.ParseQuery(raw)
	for k, v := range vals {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}

// lowerHeaders is a request's headers by lowercase name, each its first value, with the length and host the server
// takes out of the header map.
func lowerHeaders(r *http.Request) map[string]string {
	out := map[string]string{}
	for k, v := range r.Header {
		if len(v) > 0 {
			out[strings.ToLower(k)] = v[0]
		}
	}
	if _, ok := out["content-length"]; !ok && (r.ContentLength > 0 || (r.ContentLength == 0 && r.Method == http.MethodPut)) {
		out["content-length"] = strconv.FormatInt(r.ContentLength, 10)
	}
	if r.Host != "" {
		out["host"] = r.Host
	}
	return out
}

// unquote is a path segment, percent-decoded (a bad escape stays as it is, as Python's unquote leaves it).
func unquote(s string) string {
	if u, err := url.PathUnescape(s); err == nil {
		return u
	}
	return s
}

// iso is a time as the servers write it: RFC 3339 in UTC, with no fraction when it has none.
func iso(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// wholeSeconds is the time with its fraction of a second dropped, as the control plane stores commit times.
func wholeSeconds(t time.Time) time.Time { return t.Truncate(time.Second) }

// stripQuotes is s without its surrounding double quotes (Python's strip('"')).
func stripQuotes(s string) string { return strings.Trim(s, `"`) }

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// writeReply writes rep (the content route's answer is written by its own code).
func writeReply(w http.ResponseWriter, rep *reply) {
	h := w.Header()
	for k, v := range rep.header {
		h.Set(k, v)
	}
	h.Set("X-Request-ID", "req-fake")
	bodiless := rep.status == http.StatusNoContent || rep.status == http.StatusNotModified
	if !bodiless {
		h.Set("Content-Length", strconv.Itoa(len(rep.body)))
	}
	w.WriteHeader(rep.status)
	if !bodiless {
		_, _ = w.Write(rep.body)
	}
}

// readJSON is a request body as JSON, numbers kept as written: an object, {} when the body is empty, or false when
// it isn't JSON.
func readJSON(body []byte) (map[string]any, bool) {
	if len(bytes.TrimSpace(body)) == 0 {
		return map[string]any{}, true
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, false
	}
	m, ok := v.(map[string]any)
	if !ok {
		return map[string]any{}, true
	}
	return m, true
}

// str is v when it is a string, else "".
func str(v any) string {
	s, _ := v.(string)
	return s
}

// integer is v when it is a whole JSON number.
func integer(v any) (int64, bool) {
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i, true
		}
		if f, err := n.Float64(); err == nil && f == float64(int64(f)) {
			return int64(f), true
		}
	case float64:
		if n == float64(int64(n)) {
			return int64(n), true
		}
	case int64:
		return n, true
	case int:
		return int64(n), true
	}
	return 0, false
}

// toInt is Python's int(v or 0), loosely: a number's whole part, else 0.
func toInt(v any) int64 {
	switch n := v.(type) {
	case json.Number:
		if f, err := n.Float64(); err == nil {
			return int64(f)
		}
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		if err == nil {
			return int64(f)
		}
	}
	i, _ := integer(v)
	return i
}

// truthy is JavaScript's Boolean(v), as the Node fake reads flags.
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case json.Number:
		f, err := x.Float64()
		return err == nil && f != 0
	case float64:
		return x != 0
	}
	return true
}

// isFalse is v === false.
func isFalse(v any) bool {
	b, ok := v.(bool)
	return ok && !b
}

func stringMap(v any) map[string]string {
	out := map[string]string{}
	if m, ok := v.(map[string]any); ok {
		for k, x := range m {
			out[k] = str(x)
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
