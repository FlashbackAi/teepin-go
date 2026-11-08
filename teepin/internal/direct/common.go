// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/journal"
)

// What the engine's operations share: the client's side (API), the options every engine takes, ids, progress, and
// the plumbing of goroutines and cancellation (a pool whose first failure stops the rest, a capped wait, and a
// semaphore a context can end).

// Call is one control-plane request. Every direct route is idempotent (by idempotency key, pack, part or upload), so
// the client retries each as a safe call.
type Call struct {
	Method string
	// Path is under the API's base, e.g. /v1/storage/buckets/b/object/read.
	Path string
	// Body is sent as JSON; nil sends no body.
	Body any
	// Out, when not nil, receives the JSON answer.
	Out any
	// Timeout bounds the call with its retries; 0 is the client's timeout.
	Timeout time.Duration
	// NoRetry sends it once: a cleanup call while stopping, which must stay bounded.
	NoRetry bool
	// Attempts is set by the call: how many times it was sent again after a passing failure (0: once).
	Attempts int
}

// SignedRequest is a request the control plane signed for the storage network. It goes with exactly its signed
// headers and never with the Teepin API key.
type SignedRequest struct {
	Method string
	URL    string
	// Header holds the signed headers, names in lowercase as signed. content-length is the body's length.
	Header map[string]string
	// Body is sent with the request (nil for none); Size is its length.
	Body io.Reader
	Size int64
}

// API is what the engine needs from the client (package teepin implements it).
type API interface {
	// Call sends a control-plane request, retried as a safe call, and returns the answer's status. An error answer
	// is an *errs.APIError, no answer an *errs.ConnError.
	Call(ctx context.Context, c *Call) (int, error)
	// SendSigned sends a signed request to the storage network once (the engine decides retries), and returns the
	// answer for a status below 400; the caller closes its body. An error answer is an *errs.APIError with the
	// storage network's code, no answer an *errs.ConnError.
	SendSigned(ctx context.Context, r *SignedRequest) (*http.Response, error)
}

// Options are the client's settings an engine follows.
type Options struct {
	// MaxRetries is how many times a passing failure is tried again in a row.
	MaxRetries int
	// Timeout bounds the wait for the storage network's answer, and each wait for more of its body.
	Timeout time.Duration
	// KeyCacheTTL is how long data keys stay cached; 0 or less keeps none.
	KeyCacheTTL time.Duration
	// Sleep waits d unless ctx ends first (the client's, which tests replace); nil is a real wait.
	Sleep func(ctx context.Context, d time.Duration) error
	// Backoff is the wait before try attempt+1 after err; nil is a plain exponential backoff.
	Backoff func(attempt int, err error) time.Duration
}

func (o *Options) sleep(ctx context.Context, d time.Duration) error {
	if o.Sleep != nil {
		return o.Sleep(ctx, d)
	}
	return SleepCtx(ctx, d)
}

func (o *Options) backoff(attempt int, err error) time.Duration {
	if o.Backoff != nil {
		return o.Backoff(attempt, err)
	}
	return min(200*time.Millisecond<<min(attempt, 10), 10*time.Second)
}

// SleepCtx waits d, or until ctx ends (its error then).
func SleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// UUID7 is a UUIDv7 (RFC 9562): a millisecond timestamp, then random bits, so ids sort by time.
func UUID7() string { return journal.UUID7() }

// ObjectIDBytes is a UUID string's 16 bytes (hyphens, braces and a urn:uuid: prefix are allowed, as Python's
// uuid.UUID takes them).
func ObjectIDBytes(id string) ([16]byte, error) {
	var out [16]byte
	s := strings.TrimPrefix(strings.TrimPrefix(id, "urn:"), "uuid:")
	s = strings.Trim(s, "{}")
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return out, fmt.Errorf("teepin: %q is not a UUID", id)
	}
	if _, err := hex.Decode(out[:], []byte(s)); err != nil {
		return out, fmt.Errorf("teepin: %q is not a UUID", id)
	}
	return out, nil
}

// Progress adds up the bytes moved of a total and tells the caller, one call at a time.
type Progress struct {
	mu    sync.Mutex
	done  int64
	total int64
	fn    func(done, total int64)
}

// NewProgress reports to fn (nil reports nothing).
func NewProgress(total int64, fn func(done, total int64)) *Progress {
	return &Progress{total: total, fn: fn}
}

// Add counts n more bytes.
func (p *Progress) Add(n int64) {
	if p == nil || p.fn == nil || n == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done += n
	p.fn(p.done, p.total)
}

// Slots is a semaphore whose waits a context ends.
type Slots struct{ ch chan struct{} }

// NewSlots has n slots (at least 1).
func NewSlots(n int) *Slots { return &Slots{ch: make(chan struct{}, max(1, n))} }

// Acquire takes a slot, or returns ctx's error having taken none.
func (s *Slots) Acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release gives a slot back.
func (s *Slots) Release() { <-s.ch }

// RunPool runs fn for every item, at most n at once. The first failure cancels the others' context, and every worker
// is waited for before it is returned, so nothing is left running. A cancelled ctx returns ctx's error.
//
// The Python SDK's pool lets the other workers finish what they hold; this one stops them at once, as the Node SDK's
// does, so a failed download stops in about the time its requests take to notice.
func RunPool[T any](ctx context.Context, n int, items []T, fn func(context.Context, T) error) error {
	pctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var (
		mu      sync.Mutex
		next    int
		failure error
		wg      sync.WaitGroup
	)
	take := func() (T, bool) {
		mu.Lock()
		defer mu.Unlock()
		var zero T
		if failure != nil || next >= len(items) || pctx.Err() != nil {
			return zero, false
		}
		next++
		return items[next-1], true
	}
	for range max(1, min(n, len(items))) {
		wg.Go(func() {
			for {
				it, ok := take()
				if !ok {
					return
				}
				if err := fn(pctx, it); err != nil {
					mu.Lock()
					first := failure == nil
					if first {
						failure = err
					}
					mu.Unlock()
					if first {
						cancel(err)
					}
					return
				}
			}
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	return failure
}

// WaitCapped waits for wg at most d, and reports whether it ended in time.
func WaitCapped(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

// Stopped is the error of an operation whose context ended: the context's own, never another the operation met
// while it was being stopped.
func Stopped(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// isCtxErr reports a context's error, however it is wrapped.
func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
