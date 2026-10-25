// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

// Package teepin is the Go client for Teepin Web Services (TWS): Teepin
// Inference (AI models in confidential enclaves, OpenAI-compatible) and Teepin
// Storage.
//
//	client := teepin.NewClient(teepin.Config{}) // reads TEEPIN_API_KEY and TEEPIN_BASE_URL
//	resp, err := client.Inference.Chat(ctx, teepin.ChatRequest{Model: "teepin/glm-5.3", Messages: msgs})
//	obj, err := client.Storage.UploadFile(ctx, "my-bucket", "videos/a.mp4", "a.mp4", nil)
//
// Requests that fail for a passing reason (the network, a busy server) are
// retried with backoff; see Config.MaxRetries. Errors from the API are
// *APIError values, with helpers such as IsNotFound and IsOnHold.
package teepin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Version is this SDK's version, sent in the User-Agent.
const Version = "0.2.1"

const (
	// DefaultBaseURL is the public API.
	DefaultBaseURL = "https://api.teepin.com"
	defaultTimeout = 60 * time.Second
	defaultRetries = 4
)

// Client is the Teepin API client. It is safe for concurrent use.
type Client struct {
	baseURL    string
	apiKey     string
	projectID  string
	userAgent  string
	httpClient *http.Client
	timeout    time.Duration
	maxRetries int
	sleep      func(context.Context, time.Duration) error // replaced in tests

	// Instances is GPU and CPU compute.
	Instances *InstancesService
	// Storage is Teepin Storage: buckets, objects and transfers.
	Storage *StorageService
	// Inference is Teepin Inference: models, chat completions (streamed or
	// not) and confidential models' attestation.
	Inference *InferenceService
}

// Config configures a Client. Only APIKey is usually needed.
type Config struct {
	// BaseURL is the API's address. Empty: the TEEPIN_BASE_URL environment
	// variable, else https://api.teepin.com.
	BaseURL string

	// APIKey is a project API key (tpk_...). It names its project. Empty: the
	// TEEPIN_API_KEY environment variable (as OpenAI's SDKs read
	// OPENAI_API_KEY). A value given here always wins over the environment.
	APIKey string

	// ProjectID chooses the project when APIKey is a signed-in session's
	// token rather than a project key. Leave it empty with a project key.
	ProjectID string

	// HTTPClient sends the requests. The default keeps enough connections
	// open for parallel transfers and has no overall timeout, which would cut
	// off a large transfer; Timeout bounds the other calls instead.
	HTTPClient *http.Client

	// Timeout bounds one API call that is not a transfer (default 60s).
	Timeout time.Duration

	// MaxRetries is how many times a request that failed for a passing
	// reason is tried again (default 4). Negative turns retries off.
	MaxRetries int

	// UserAgent is added in front of the SDK's own User-Agent.
	UserAgent string
}

// NewClient makes a client.
func NewClient(config Config) *Client {
	if config.APIKey == "" {
		config.APIKey = os.Getenv("TEEPIN_API_KEY")
	}
	if config.BaseURL == "" {
		config.BaseURL = os.Getenv("TEEPIN_BASE_URL")
	}
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Transport: defaultTransport()}
	}
	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}
	switch {
	case config.MaxRetries == 0:
		config.MaxRetries = defaultRetries
	case config.MaxRetries < 0:
		config.MaxRetries = 0
	}
	ua := "teepin-go/" + Version
	if config.UserAgent != "" {
		ua = config.UserAgent + " " + ua
	}
	c := &Client{
		baseURL:    strings.TrimRight(config.BaseURL, "/"),
		apiKey:     config.APIKey,
		projectID:  config.ProjectID,
		userAgent:  ua,
		httpClient: config.HTTPClient,
		timeout:    config.Timeout,
		maxRetries: config.MaxRetries,
		sleep:      sleepCtx,
	}
	c.Instances = &InstancesService{client: c}
	c.Storage = &StorageService{client: c}
	c.Inference = &InferenceService{client: c}
	return c
}

// BaseURL is the API address the client talks to.
func (c *Client) BaseURL() string { return c.baseURL }

func defaultTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	// Parallel parts and ranges each hold a connection; the default of 2 idle
	// connections per host would reconnect for nearly every one.
	t.MaxIdleConnsPerHost = 64
	t.MaxIdleConns = 128
	return t
}

// retryMode says when a failed request may be sent again.
type retryMode int

const (
	// retrySafe: the request can be repeated with the same effect (reads,
	// PUTs that replace, deletes).
	retrySafe retryMode = iota
	// retryUnsent: only when the server certainly did not act on it: it
	// never arrived, or the server said it is busy and did nothing.
	retryUnsent
	// retryChat: a chat completion, which the API bills once the model has
	// answered, even if the client has gone. Tried again only when it
	// certainly did not bill: a 429, Teepin's own 5xx (its error envelope in
	// the body: the API bills only after it has the model's answer), or a
	// request that never left. Never after a timeout or a cut connection, a
	// 504, or a 5xx from something in front of the API; never once a stream
	// has begun.
	retryChat
)

// request is one API call.
type request struct {
	method string
	path   string
	query  url.Values
	header http.Header
	// json is marshalled as the body; body, when set, makes a fresh raw body
	// for each attempt (nil means the request has no body).
	json any
	body func() (io.Reader, error)
	size int64
	mode retryMode
	// transfer calls are bounded by the caller's context, not Config.Timeout.
	transfer bool
	// attempt is set while sending, so a caller can tell a retry's answer.
	attempt int
}

// send makes the request, retrying as its mode allows, and returns a response
// with a status below 400; the caller closes its body. An error status
// becomes an *APIError.
func (c *Client) send(ctx context.Context, r *request) (*http.Response, error) {
	var payload []byte
	if r.json != nil {
		b, err := json.Marshal(r.json)
		if err != nil {
			return nil, fmt.Errorf("teepin: encode request: %w", err)
		}
		payload = b
	}
	for attempt := 0; ; attempt++ {
		r.attempt = attempt
		resp, err := c.sendOnce(ctx, r, payload)
		if err == nil {
			return resp, nil
		}
		if attempt >= c.maxRetries || !c.retryable(r, err) || ctx.Err() != nil {
			return nil, err
		}
		if serr := c.sleep(ctx, backoff(attempt, err)); serr != nil {
			return nil, err
		}
	}
}

func (c *Client) sendOnce(ctx context.Context, r *request, payload []byte) (*http.Response, error) {
	u := c.baseURL + r.path
	if len(r.query) > 0 {
		u += "?" + r.query.Encode()
	}
	var body io.Reader
	switch {
	case r.body != nil:
		b, err := r.body()
		if err != nil {
			return nil, err
		}
		body = b
	case payload != nil:
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, r.method, u, body)
	if err != nil {
		return nil, fmt.Errorf("teepin: build request: %w", err)
	}
	for k, vs := range r.header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	switch {
	case r.body != nil:
		req.ContentLength = r.size
		if r.size == 0 {
			req.Body = http.NoBody
		}
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/octet-stream")
		}
	case payload != nil:
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", c.userAgent)
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	if c.projectID != "" {
		req.Header.Set("X-Project-ID", c.projectID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &transportError{err: err, unsent: unsent(err)}
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		return nil, decodeError(resp)
	}
	return resp, nil
}

// call sends a JSON request and decodes a JSON answer into out (when not nil).
func (c *Client) call(ctx context.Context, r *request, out any) error {
	if !r.transfer && c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	resp, err := c.send(ctx, r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("teepin: read %s %s: %w", r.method, r.path, err)
	}
	return nil
}

// do is the plain JSON call the compute API uses: reads may be retried,
// writes only when they never reached the server.
func (c *Client) do(ctx context.Context, method, path string, body, result any) error {
	mode := retryUnsent
	if method == http.MethodGet || method == http.MethodDelete {
		mode = retrySafe
	}
	return c.call(ctx, &request{method: method, path: path, json: body, mode: mode}, result)
}

// retryable reports whether err is worth another attempt for r.
func (c *Client) retryable(r *request, err error) bool {
	var te *transportError
	if errors.As(err, &te) {
		if errors.Is(te.err, context.Canceled) || errors.Is(te.err, context.DeadlineExceeded) {
			return false
		}
		return r.mode == retrySafe || te.unsent
	}
	var ae *APIError
	if !errors.As(err, &ae) {
		return false
	}
	if r.mode == retryChat {
		switch ae.StatusCode {
		case http.StatusTooManyRequests:
			return true
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable:
			// Only the codes the API sends before it bills; an HTML 502 from a
			// load balancer (no code) may come after the request was billed.
			return unbilledChatCodes[ae.Code]
		}
		return false
	}
	switch ae.StatusCode {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		// Busy, or credit could not be checked: nothing was done.
		return true
	case http.StatusBadGateway:
		// The storage backend failed; the API says nothing was stored.
		return true
	case http.StatusInternalServerError, http.StatusGatewayTimeout:
		return r.mode == retrySafe
	}
	return false
}

// unbilledChatCodes are the inference API's 5xx codes that come before a chat
// is billed (inference_handlers.go), so the chat may be sent again.
var unbilledChatCodes = map[string]bool{
	"internal_error":      true,
	"upstream_error":      true,
	"model_unavailable":   true,
	"billing_unavailable": true,
}

// transportError is a request that got no HTTP answer.
type transportError struct {
	err    error
	unsent bool
}

func (e *transportError) Error() string { return "teepin: " + e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// unsent reports whether the request certainly never reached the server.
func unsent(err error) bool {
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return true
	}
	var dns *net.DNSError
	return errors.As(err, &dns)
}

// backoff is the wait before attempt+1: exponential with full jitter, or
// the server's Retry-After when it sent one.
func backoff(attempt int, err error) time.Duration {
	var ae *APIError
	if errors.As(err, &ae) && ae.RetryAfter > 0 {
		return min(ae.RetryAfter, time.Minute)
	}
	ceiling := min(200*time.Millisecond<<attempt, 10*time.Second)
	return time.Duration(rand.Int64N(int64(ceiling))) + 50*time.Millisecond
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
