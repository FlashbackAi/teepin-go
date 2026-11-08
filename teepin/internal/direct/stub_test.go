// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// Read sessions a well-behaved control plane never signs, which the fakes can't make: a stub of the engine's API that
// answers each read session with what a test builds, and serves signed GETs from one object's stored bytes.

type stubAPI struct {
	stored  []byte
	session func(off, length int64) map[string]any
	calls   atomic.Int32
}

func (a *stubAPI) Call(ctx context.Context, c *Call) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	a.calls.Add(1)
	body := c.Body.(map[string]any)
	raw, _ := json.Marshal(a.session(body["stored_offset"].(int64), body["stored_length"].(int64)))
	return http.StatusOK, json.Unmarshal(raw, c.Out)
}

func (a *stubAPI) SendSigned(ctx context.Context, r *SignedRequest) (*http.Response, error) {
	var first, last int64
	if _, err := fmt.Sscanf(r.Header["range"], "bytes=%d-%d", &first, &last); err != nil {
		return nil, err
	}
	last = min(last, int64(len(a.stored))-1)
	return &http.Response{StatusCode: http.StatusPartialContent, Body: io.NopCloser(bytes.NewReader(a.stored[first : last+1]))}, nil
}

func request(first, last int64) map[string]any {
	return map[string]any{"url": "stub", "headers": map[string]any{"range": fmt.Sprintf("bytes=%d-%d", first, last)},
		"stored_first": first, "stored_last": last}
}

func stubSession(format string, size, stored int64, dek []byte, requests ...map[string]any) map[string]any {
	s := map[string]any{"key": "k", "version": "00000000-0000-7000-8000-000000000001", "size": size, "stored_size": stored,
		"format": format, "source": "storage", "requests": requests}
	if dek != nil {
		s["key_material"] = base64.StdEncoding.EncodeToString(dek)
	}
	return s
}

func stubReads(api *stubAPI) *Reads {
	return NewReads(api, Options{MaxRetries: 4, Timeout: time.Second, KeyCacheTTL: time.Minute,
		Sleep: func(ctx context.Context, d time.Duration) error { return ctx.Err() }})
}

// readWithin runs a read, failing the test if it doesn't end within a few seconds (a rule broken here loops).
func readWithin(t *testing.T, read func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- read() }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the read never ended")
		return nil
	}
}

// A later window's session must sign the object's header first (section 22.2, item 4): one that doesn't is refused,
// never read as if its window were the header.
func TestStub_ALaterWindowWithoutItsHeaderRequestIsRefused(t *testing.T) {
	dek, _ := tws1.NewDataKey()
	data := bytes.Repeat([]byte("x"), 3*tws1.SegmentSize)
	stored, _ := tws1.Encrypt(dek, data)
	size := int64(len(data))
	api := &stubAPI{stored: stored, session: func(off, length int64) map[string]any {
		return stubSession(FormatTWS1, size, int64(len(stored)), nil, request(off, min(off+length, int64(len(stored)))-1))
	}}
	r := stubReads(api)
	s0, err := NewSession("b", mustJSON(stubSession(FormatTWS1, size, int64(len(stored)), dek, request(0, 100))))
	if err != nil {
		t.Fatal(err)
	}
	err = readWithin(t, func() error {
		return r.Plaintext(context.Background(), s0, dek, tws1.SegmentSize, size, false, func([]byte) error { return nil })
	})
	if reason(err) != "header" {
		t.Fatal(err)
	}
}

// A plain window that ends before it starts holds nothing: it is refused, not asked for again without end.
func TestStub_APlainWindowHoldingNothingIsRefused(t *testing.T) {
	stored := bytes.Repeat([]byte("p"), 1000)
	api := &stubAPI{stored: stored, session: func(off, length int64) map[string]any {
		return stubSession(FormatPlain, 1000, 1000, nil, request(off, off-1))
	}}
	r := stubReads(api)
	s0, err := NewSession("b", mustJSON(stubSession(FormatPlain, 1000, 1000, nil, request(0, 99))))
	if err != nil {
		t.Fatal(err)
	}
	err = readWithin(t, func() error {
		return r.Plaintext(context.Background(), s0, nil, 500, 600, false, func([]byte) error { return nil })
	})
	if reason(err) != "window" || api.calls.Load() != 2 {
		t.Fatalf("%v after %d sessions", err, api.calls.Load())
	}
}

// An error from the caller's side of a read (a write that failed) ends it as it is, never judged as a failure of
// the read and tried again.
func TestStub_AConsumersErrorEndsTheReadAsItIs(t *testing.T) {
	stored := bytes.Repeat([]byte("p"), 1000)
	api := &stubAPI{stored: stored, session: func(off, length int64) map[string]any {
		return stubSession(FormatPlain, 1000, 1000, nil, request(off, min(off+length, 1000)-1))
	}}
	r := stubReads(api)
	s0, _ := NewSession("b", mustJSON(stubSession(FormatPlain, 1000, 1000, nil, request(0, 999))))
	full := fmt.Errorf("the disk is full: %w", io.ErrShortWrite)
	err := r.Plaintext(context.Background(), s0, nil, 0, 1000, false, func([]byte) error { return full })
	if err != full || api.calls.Load() != 0 {
		t.Fatalf("%v after %d sessions", err, api.calls.Load())
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	if !strings.HasPrefix(string(b), "{") {
		panic("not an object")
	}
	return b
}
