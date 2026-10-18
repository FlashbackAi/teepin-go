// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStorage is an in-memory Teepin Storage API following
// /v1/openapi.json, with faults that tests switch on.
type fakeStorage struct {
	mu      sync.Mutex
	buckets map[string]map[string]*fakeObject
	uploads map[string]*fakeUpload
	nextID  int

	// fail answers the next failN[route] requests to a route with 503.
	failN map[string]int
	// cut stops the next cutN content responses half way.
	cutN int
	// hits counts requests per route.
	hits map[string]int
	// inflight is the most part uploads seen at once.
	inflight, maxInflight atomic.Int32
	// partDelay slows part uploads, so concurrency shows.
	partDelay time.Duration
}

type fakeObject struct {
	data        []byte
	contentType string
	metadata    map[string]string
	sha         string
	parts       int
	updated     time.Time
}

func (o *fakeObject) etag() string {
	if o.parts > 0 {
		return fmt.Sprintf("%s-%d", o.sha, o.parts)
	}
	return o.sha
}

type fakeUpload struct {
	bucket, key, contentType string
	metadata                 map[string]string
	parts                    map[int][]byte
}

func newFake(t *testing.T) (*fakeStorage, *Client) {
	t.Helper()
	f := &fakeStorage{buckets: map[string]map[string]*fakeObject{}, uploads: map[string]*fakeUpload{},
		failN: map[string]int{}, hits: map[string]int{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c := NewClient(Config{BaseURL: srv.URL, APIKey: "tpk_test"})
	c.sleep = func(context.Context, time.Duration) error { return nil }
	return f, c
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func apiErr(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error": code, "code": code})
}

func (f *fakeStorage) objectJSON(key string, o *fakeObject) map[string]any {
	m := map[string]any{"id": "obj-" + key, "key": key, "size_bytes": len(o.data), "content_type": o.contentType,
		"metadata": o.metadata, "checksum_sha256": o.sha, "etag": o.etag(), "status": "available", "backend": "fake",
		"created_at": o.updated, "updated_at": o.updated, "uploaded_at": o.updated}
	if o.parts > 0 {
		m["part_count"] = o.parts
	}
	return m
}

// route names a request like "PUT part" for faults and counts.
func route(r *http.Request) string {
	p := strings.TrimPrefix(r.URL.Path, "/v1/storage/buckets")
	seg := strings.Split(strings.Trim(p, "/"), "/")
	switch {
	case len(seg) >= 4 && seg[1] == "uploads" && seg[3] == "parts" && len(seg) == 5:
		return r.Method + " part"
	case len(seg) >= 4 && seg[1] == "uploads" && seg[3] == "complete":
		return r.Method + " complete"
	case len(seg) == 4 && seg[1] == "uploads":
		return r.Method + " " + seg[3]
	case len(seg) == 3 && seg[1] == "uploads":
		return r.Method + " upload"
	case len(seg) >= 2:
		return r.Method + " " + strings.Join(seg[1:], "/")
	}
	return r.Method + " " + p
}

func (f *fakeStorage) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer tpk_test" {
		apiErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	name := route(r)
	f.mu.Lock()
	f.hits[name]++
	if f.failN[name] > 0 {
		f.failN[name]--
		f.mu.Unlock()
		w.Header().Set("Retry-After", "1")
		apiErr(w, http.StatusServiceUnavailable, "storage_busy")
		return
	}
	f.mu.Unlock()

	p := strings.TrimPrefix(r.URL.Path, "/v1/storage/buckets")
	seg := strings.Split(strings.Trim(p, "/"), "/")
	if p == "" || p == "/" {
		f.bucketsRoot(w, r)
		return
	}
	bucket := seg[0]
	f.mu.Lock()
	objs, ok := f.buckets[bucket]
	f.mu.Unlock()
	if !ok && !(len(seg) == 1 && r.Method == http.MethodGet) {
		apiErr(w, http.StatusNotFound, "bucket_not_found")
		return
	}
	rest := strings.Join(seg[1:], "/")
	key := r.URL.Query().Get("key")
	switch {
	case rest == "" && r.Method == http.MethodGet:
		if !ok {
			apiErr(w, http.StatusNotFound, "bucket_not_found")
			return
		}
		writeJSON(w, 200, map[string]any{"id": "b-" + bucket, "name": bucket, "backend": "fake", "object_count": len(objs)})
	case rest == "" && r.Method == http.MethodDelete:
		f.mu.Lock()
		defer f.mu.Unlock()
		if len(objs) > 0 {
			apiErr(w, http.StatusConflict, "bucket_not_empty")
			return
		}
		delete(f.buckets, bucket)
		w.WriteHeader(http.StatusNoContent)
	case rest == "objects":
		f.list(w, r, objs)
	case rest == "object" && r.Method == http.MethodPut:
		f.put(w, r, bucket, key)
	case rest == "object" && r.Method == http.MethodGet:
		f.mu.Lock()
		o := objs[key]
		f.mu.Unlock()
		if o == nil {
			apiErr(w, http.StatusNotFound, "object_not_found")
			return
		}
		writeJSON(w, 200, f.objectJSON(key, o))
	case rest == "object" && r.Method == http.MethodDelete:
		f.mu.Lock()
		defer f.mu.Unlock()
		if objs[key] == nil {
			apiErr(w, http.StatusNotFound, "object_not_found")
			return
		}
		delete(objs, key)
		w.WriteHeader(http.StatusNoContent)
	case rest == "object/content":
		f.content(w, r, objs, key)
	case rest == "objects/delete":
		var req struct{ Keys []string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		deleted, errs := []string{}, []map[string]string{}
		for _, k := range req.Keys {
			if objs[k] == nil {
				errs = append(errs, map[string]string{"key": k, "code": "object_not_found", "error": "object not found"})
				continue
			}
			delete(objs, k)
			deleted = append(deleted, k)
		}
		f.mu.Unlock()
		writeJSON(w, 200, map[string]any{"deleted": deleted, "errors": errs})
	case rest == "object/rename":
		var req struct {
			From, To  string
			Overwrite *bool
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		defer f.mu.Unlock()
		o := objs[req.From]
		if o == nil {
			apiErr(w, http.StatusNotFound, "object_not_found")
			return
		}
		if objs[req.To] != nil && req.Overwrite != nil && !*req.Overwrite {
			apiErr(w, http.StatusConflict, "object_exists")
			return
		}
		delete(objs, req.From)
		objs[req.To] = o
		writeJSON(w, 200, f.objectJSON(req.To, o))
	case rest == "object/copy":
		var req struct{ SourceBucket, SourceKey, Key string }
		_ = json.NewDecoder(r.Body).Decode(&struct {
			SourceBucket *string `json:"source_bucket"`
			SourceKey    *string `json:"source_key"`
			Key          *string `json:"key"`
		}{&req.SourceBucket, &req.SourceKey, &req.Key})
		f.mu.Lock()
		defer f.mu.Unlock()
		src := f.buckets[req.SourceBucket][req.SourceKey]
		if src == nil {
			apiErr(w, http.StatusNotFound, "object_not_found")
			return
		}
		cp := *src
		objs[req.Key] = &cp
		writeJSON(w, 200, f.objectJSON(req.Key, &cp))
	case rest == "object/download-url" || rest == "object/upload-url":
		writeJSON(w, 200, map[string]any{"url": "/v1/storage/d/token-" + key, "method": "PUT", "headers": map[string]string{},
			"max_bytes": 5 << 30, "expires_at": time.Now().Add(15 * time.Minute)})
	case strings.HasPrefix(rest, "uploads"):
		f.uploadsRoute(w, r, bucket, seg[1:])
	default:
		apiErr(w, http.StatusNotFound, "not_found")
	}
}

func (f *fakeStorage) bucketsRoot(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method == http.MethodPost {
		var req struct{ Name string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		if _, ok := f.buckets[req.Name]; ok {
			apiErr(w, http.StatusConflict, "bucket_exists")
			return
		}
		f.buckets[req.Name] = map[string]*fakeObject{}
		writeJSON(w, 201, map[string]any{"id": "b-" + req.Name, "name": req.Name, "backend": "fake"})
		return
	}
	var out []map[string]any
	for name := range f.buckets {
		out = append(out, map[string]any{"id": "b-" + name, "name": name, "backend": "fake"})
	}
	writeJSON(w, 200, map[string]any{"buckets": out})
}

func (f *fakeStorage) list(w http.ResponseWriter, r *http.Request, objs map[string]*fakeObject) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix, cursor, delim := r.URL.Query().Get("prefix"), r.URL.Query().Get("cursor"), r.URL.Query().Get("delimiter")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	// Entries are keys, or with a delimiter the folders they roll up into.
	seen := map[string]bool{}
	var entries []string
	for k := range objs {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		e := k
		if delim != "" {
			if i := strings.Index(k[len(prefix):], delim); i >= 0 {
				e = k[:len(prefix)+i+1]
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
			out = append(out, f.objectJSON(e, o))
		} else {
			prefixes = append(prefixes, e)
		}
	}
	body := map[string]any{"objects": out, "next_cursor": next}
	if delim != "" {
		body["prefixes"] = prefixes
	}
	writeJSON(w, 200, body)
}

func (f *fakeStorage) put(w http.ResponseWriter, r *http.Request, bucket, key string) {
	data, err := io.ReadAll(r.Body)
	if err != nil || int64(len(data)) != r.ContentLength {
		return // the client went away
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := f.buckets[bucket][key]
	if r.Header.Get("If-None-Match") == "*" && cur != nil {
		apiErr(w, http.StatusPreconditionFailed, "precondition_failed")
		return
	}
	if m := r.Header.Get("If-Match"); m != "" && (cur == nil || (m != "*" && strings.Trim(m, `"`) != cur.etag())) {
		apiErr(w, http.StatusPreconditionFailed, "precondition_failed")
		return
	}
	sum := sha256.Sum256(data)
	meta := map[string]string{}
	for k, v := range r.Header {
		if strings.HasPrefix(k, "X-Teepin-Meta-") {
			meta[strings.ToLower(strings.TrimPrefix(k, "X-Teepin-Meta-"))] = v[0]
		}
	}
	o := &fakeObject{data: data, contentType: r.Header.Get("Content-Type"), metadata: meta, sha: hex.EncodeToString(sum[:]), updated: time.Now()}
	f.buckets[bucket][key] = o
	w.Header().Set("ETag", `"`+o.etag()+`"`)
	writeJSON(w, 200, f.objectJSON(key, o))
}

func (f *fakeStorage) content(w http.ResponseWriter, r *http.Request, objs map[string]*fakeObject, key string) {
	f.mu.Lock()
	o := objs[key]
	cut := false
	if o != nil && r.Method == http.MethodGet && f.cutN > 0 {
		f.cutN--
		cut = true
	}
	f.mu.Unlock()
	if o == nil {
		apiErr(w, http.StatusNotFound, "object_not_found")
		return
	}
	etag := `"` + o.etag() + `"`
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Type", o.contentType)
	w.Header().Set("Last-Modified", o.updated.UTC().Format(http.TimeFormat))
	if inm := r.Header.Get("If-None-Match"); inm != "" && inm == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	start, end, status := int64(0), int64(len(o.data))-1, http.StatusOK
	if rg := r.Header.Get("Range"); rg != "" && (r.Header.Get("If-Range") == "" || r.Header.Get("If-Range") == etag) {
		spec := strings.TrimPrefix(rg, "bytes=")
		a, b, _ := strings.Cut(spec, "-")
		start, _ = strconv.ParseInt(a, 10, 64)
		if b != "" {
			end, _ = strconv.ParseInt(b, 10, 64)
		}
		end = min(end, int64(len(o.data))-1)
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(o.data)))
	}
	body := o.data[start : end+1]
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	if cut {
		_, _ = w.Write(body[:len(body)/2])
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		panic(http.ErrAbortHandler) // drops the connection mid-body
	}
	_, _ = w.Write(body)
}

func (f *fakeStorage) uploadsRoute(w http.ResponseWriter, r *http.Request, bucket string, seg []string) {
	switch {
	case len(seg) == 1 && r.Method == http.MethodPost:
		var req struct {
			Key         string            `json:"key"`
			ContentType string            `json:"content_type"`
			Metadata    map[string]string `json:"metadata"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		f.nextID++
		id := fmt.Sprintf("00000000-0000-0000-0000-%012d", f.nextID)
		f.uploads[id] = &fakeUpload{bucket: bucket, key: req.Key, contentType: req.ContentType, metadata: req.Metadata, parts: map[int][]byte{}}
		f.mu.Unlock()
		writeJSON(w, 201, map[string]any{"upload_id": id, "key": req.Key, "content_type": req.ContentType, "created_at": time.Now(), "expires_at": time.Now().Add(7 * 24 * time.Hour)})
		return
	case len(seg) == 1 && r.Method == http.MethodGet:
		f.mu.Lock()
		var ids []string
		for id, u := range f.uploads {
			if u.bucket == bucket && strings.HasPrefix(u.key, r.URL.Query().Get("prefix")) {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		out := []map[string]any{}
		for _, id := range ids {
			out = append(out, map[string]any{"upload_id": id, "key": f.uploads[id].key, "content_type": f.uploads[id].contentType})
		}
		f.mu.Unlock()
		writeJSON(w, 200, map[string]any{"uploads": out})
		return
	}
	id := seg[1]
	f.mu.Lock()
	up := f.uploads[id]
	f.mu.Unlock()
	if up == nil {
		apiErr(w, http.StatusNotFound, "upload_not_found")
		return
	}
	switch {
	case len(seg) == 2 && r.Method == http.MethodDelete:
		f.mu.Lock()
		delete(f.uploads, id)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case len(seg) == 3 && seg[2] == "parts":
		f.mu.Lock()
		var nums []int
		for n := range up.parts {
			nums = append(nums, n)
		}
		sort.Ints(nums)
		parts := []map[string]any{}
		for _, n := range nums {
			sum := md5.Sum(up.parts[n])
			parts = append(parts, map[string]any{"part_number": n, "size_bytes": len(up.parts[n]), "etag": hex.EncodeToString(sum[:])})
		}
		f.mu.Unlock()
		writeJSON(w, 200, map[string]any{"upload_id": id, "key": up.key, "content_type": up.contentType, "parts": parts})
	case len(seg) == 4 && seg[2] == "parts":
		n := f.inflight.Add(1)
		defer f.inflight.Add(-1)
		for {
			m := f.maxInflight.Load()
			if n <= m || f.maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(f.partDelay)
		num, _ := strconv.Atoi(seg[3])
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		f.mu.Lock()
		up.parts[num] = data
		f.mu.Unlock()
		sum := md5.Sum(data)
		w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
		writeJSON(w, 200, map[string]any{"part_number": num, "size_bytes": len(data), "etag": hex.EncodeToString(sum[:])})
	case len(seg) == 3 && seg[2] == "complete":
		var req struct {
			Parts []struct {
				PartNumber int    `json:"part_number"`
				ETag       string `json:"etag"`
			} `json:"parts"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		defer f.mu.Unlock()
		var data []byte
		composite := sha256.New()
		for i, p := range req.Parts {
			b, ok := up.parts[p.PartNumber]
			sum := md5.Sum(b)
			if !ok || hex.EncodeToString(sum[:]) != strings.Trim(p.ETag, `"`) || (i < len(req.Parts)-1 && len(b) < MinPartSize) {
				apiErr(w, http.StatusBadRequest, "invalid_part")
				return
			}
			data = append(data, b...)
			s := sha256.Sum256(b)
			composite.Write(s[:])
		}
		o := &fakeObject{data: data, contentType: up.contentType, metadata: up.metadata, sha: hex.EncodeToString(composite.Sum(nil)),
			parts: len(req.Parts), updated: time.Now()}
		f.buckets[bucket][up.key] = o
		delete(f.uploads, id)
		writeJSON(w, 200, f.objectJSON(up.key, o))
	default:
		apiErr(w, http.StatusNotFound, "not_found")
	}
}
