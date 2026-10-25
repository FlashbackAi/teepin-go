// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// inferenceFake answers each request with the next of its handlers, so a
// test scripts a sequence (a 429, then success). It records the requests.
type inferenceFake struct {
	t        *testing.T
	mu       sync.Mutex
	handlers []http.HandlerFunc
	requests []*http.Request
	bodies   []map[string]any
}

func newInferenceClient(t *testing.T, handlers ...http.HandlerFunc) (*Client, *inferenceFake, *[]time.Duration) {
	t.Helper()
	f := &inferenceFake{t: t, handlers: handlers}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c := NewClient(Config{BaseURL: srv.URL, APIKey: "tpk_test", MaxRetries: 2})
	var waits []time.Duration
	c.sleep = func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }
	return c, f, &waits
}

func (f *inferenceFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	i := len(f.requests)
	f.requests = append(f.requests, r)
	var body map[string]any
	if raw, _ := io.ReadAll(r.Body); len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			f.t.Errorf("request body is not JSON: %s", raw)
		}
	}
	f.bodies = append(f.bodies, body)
	f.mu.Unlock()
	if i >= len(f.handlers) {
		f.t.Errorf("unexpected request %d: %s %s", i+1, r.Method, r.URL)
		w.WriteHeader(http.StatusTeapot)
		return
	}
	f.handlers[i](w, r)
}

func inferenceErr(status int, typ, code, message string, header map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-ID", "req-123")
		for k, v := range header {
			w.Header().Set(k, v)
		}
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"error":{"message":%q,"type":%q,"code":%q}}`, message, typ, code)
	}
}

const completion = `{"id":"chatcmpl-1","object":"chat.completion","created":1761300000,"model":"teepin/conf",
 "choices":[{"index":0,"message":{"role":"assistant","content":"Hello there","reasoning_content":"think"},"finish_reason":"stop"}],
 "usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7},"future_field":true}`

func okCompletion(w http.ResponseWriter, _ *http.Request) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("X-Teepin-Attestation", "verified-older-release")
	h.Set("X-Request-ID", "req-abc")
	h.Set("X-RateLimit-Limit", "60")
	h.Set("X-RateLimit-Remaining", "59")
	h.Set("X-RateLimit-Reset", "1761300060")
	io.WriteString(w, completion)
}

func TestListModels(t *testing.T) {
	c, f, _ := newInferenceClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"object":"list","data":[
		 {"id":"teepin/a","object":"model","created":1,"owned_by":"teepin","display_name":"A","context_window":131072,
		  "supports_tools":true,"supports_vision":false,"supports_audio":false,
		  "pricing":{"input_per_million_tokens":0.5,"output_per_million_tokens":1.5},"confidential":false},
		 {"id":"teepin/conf","object":"model","created":2,"owned_by":"teepin","display_name":"Conf",
		  "pricing":{"input_per_million_tokens":2,"output_per_million_tokens":6},"confidential":true,
		  "attestation":{"level":"not-verified","warning":"The enclave could not be verified right now."}}]}`)
	})
	models, err := c.Inference.ListModels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if f.requests[0].URL.Path != "/v1/models" || f.requests[0].Header.Get("Authorization") != "Bearer tpk_test" {
		t.Errorf("request %s %v", f.requests[0].URL, f.requests[0].Header)
	}
	if len(models) != 2 || models[0].ContextWindow != 131072 || !models[0].SupportsTools || models[0].Pricing.OutputPerMillionTokens != 1.5 {
		t.Errorf("models[0] = %+v", models[0])
	}
	if !models[1].Confidential || models[1].Attestation == nil || models[1].Attestation.Level != AttestationNotVerified {
		t.Errorf("models[1] = %+v", models[1])
	}
}

func TestAttestation(t *testing.T) {
	c, f, _ := newInferenceClient(t,
		func(w http.ResponseWriter, _ *http.Request) {
			io.WriteString(w, `{"format":"sev-snp","report":"base64...","nested":{"a":1}}`)
		},
		inferenceErr(404, "invalid_request_error", "attestation_not_supported", `"teepin/a" does not provide an attestation proof`, nil),
	)
	doc, err := c.Inference.Attestation(context.Background(), "teepin/conf")
	if err != nil {
		t.Fatal(err)
	}
	if got := f.requests[0].URL.RawQuery; got != "model=teepin%2Fconf" {
		t.Errorf("query %q: the model id must be encoded", got)
	}
	var parsed map[string]any
	if json.Unmarshal(doc, &parsed) != nil || parsed["format"] != "sev-snp" {
		t.Errorf("document = %s, want it as the provider sent it", doc)
	}
	_, err = c.Inference.Attestation(context.Background(), "teepin/a")
	var ae *APIError
	if !errors.As(err, &ae) || ae.StatusCode != 404 || ae.Code != "attestation_not_supported" || !IsNotFound(err) {
		t.Errorf("err = %v", err)
	}
	if len(f.requests) != 2 {
		t.Errorf("a 404 must not be retried: %d requests", len(f.requests))
	}
}

func TestChat(t *testing.T) {
	c, f, _ := newInferenceClient(t, okCompletion)
	resp, err := c.Inference.Chat(context.Background(), ChatRequest{
		Model:              "teepin/conf",
		Messages:           []Message{SystemMessage("Be brief."), UserMessage("Hi")},
		MaxTokens:          100,
		Temperature:        Float64(0),
		ChatTemplateKwargs: map[string]any{"enable_thinking": false},
		Extra:              map[string]any{"top_k": 20, "model": "ignored: the typed field wins"},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := f.bodies[0]
	if body["model"] != "teepin/conf" || body["max_tokens"] != float64(100) || body["temperature"] != float64(0) || body["top_k"] != float64(20) {
		t.Errorf("body = %v", body)
	}
	for _, absent := range []string{"stream", "top_p", "seed", "tools", "n", "user"} {
		if _, ok := body[absent]; ok {
			t.Errorf("body carries %q though it was not set: %v", absent, body)
		}
	}
	if kw, _ := body["chat_template_kwargs"].(map[string]any); kw["enable_thinking"] != false {
		t.Errorf("chat_template_kwargs = %v", body["chat_template_kwargs"])
	}
	if resp.Text() != "Hello there" || resp.Choices[0].Message.ReasoningContent != "think" || resp.Usage.TotalTokens != 7 {
		t.Errorf("response = %+v", resp)
	}
	if !strings.Contains(string(resp.Raw), "future_field") {
		t.Error("the raw body keeps fields the SDK doesn't model")
	}
	m := resp.Meta
	if m.Attestation != AttestationOlderRelease || m.RequestID != "req-abc" || m.RateLimit == nil ||
		m.RateLimit.Limit != 60 || m.RateLimit.Remaining != 59 || m.RateLimit.Reset.Unix() != 1761300060 {
		t.Errorf("meta = %+v %+v", m, m.RateLimit)
	}
	if _, err := c.Inference.Chat(context.Background(), ChatRequest{Model: "teepin/a"}); err == nil {
		t.Error("no messages must be refused before sending")
	}
}

func sse(frames ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Teepin-Attestation", "verified")
		w.Header().Set("X-Request-ID", "req-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		for _, f := range frames {
			io.WriteString(w, f)
			fl.Flush()
		}
	}
}

func chunk(content string) string {
	return fmt.Sprintf(`data: {"id":"c1","object":"chat.completion.chunk","model":"teepin/a","choices":[{"index":0,"delta":{"content":%q}}]}`+"\n\n", content)
}

func readAll(t *testing.T, s *ChatStream) string {
	t.Helper()
	var b strings.Builder
	for s.Next() {
		b.WriteString(s.Chunk().Text())
	}
	return b.String()
}

func TestChatStream(t *testing.T) {
	usage := `data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}` + "\n\n"
	// A chunk split across writes, a comment, and CRLF line ends.
	split := chunk(" wor")
	c, f, _ := newInferenceClient(t, sse(
		": keep-alive\n\n",
		`data: {"choices":[{"index":0,"delta":{"role":"assistant","content":"Hel"}}]}`+"\r\n\r\n",
		chunk("lo"), split[:20], split[20:], chunk("ld"), usage, "data: [DONE]\n\n",
	))
	s, err := c.Inference.ChatStream(context.Background(), ChatRequest{
		Model: "teepin/a", Messages: []Message{UserMessage("hi")}, StreamOptions: &StreamOptions{IncludeUsage: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Meta.Attestation != AttestationVerified || s.Meta.RequestID != "req-stream" {
		t.Errorf("meta before the first chunk = %+v", s.Meta)
	}
	if got := readAll(t, s); got != "Hello world" {
		t.Errorf("text = %q", got)
	}
	if err := s.Err(); err != nil {
		t.Errorf("err = %v", err)
	}
	if s.Usage() == nil || s.Usage().TotalTokens != 7 {
		t.Errorf("usage = %+v", s.Usage())
	}
	if f.bodies[0]["stream"] != true || f.requests[0].Header.Get("Accept") != "text/event-stream" {
		t.Errorf("request = %v %v", f.bodies[0], f.requests[0].Header)
	}
	if s.Next() {
		t.Error("Next after [DONE]")
	}
	if s.Close() != nil || s.Close() != nil {
		t.Error("Close twice is safe")
	}
}

func TestChatStream_ErrorFrameAndEarlyEnd(t *testing.T) {
	c, f, _ := newInferenceClient(t,
		sse(chunk("par"), `data: {"error":{"message":"the model stopped responding mid-stream","type":"api_error","code":"stream_error"}}`+"\n\n"),
		sse(chunk("cut off")),
	)
	req := ChatRequest{Model: "teepin/a", Messages: []Message{UserMessage("hi")}}
	s, err := c.Inference.ChatStream(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, s); got != "par" {
		t.Errorf("text before the failure = %q", got)
	}
	var ae *APIError
	if !errors.As(s.Err(), &ae) || ae.Code != "stream_error" || ae.Message == "" || ae.StatusCode != http.StatusOK {
		t.Errorf("err = %v, want the stream's error", s.Err())
	}
	s.Close()

	s, err = c.Inference.ChatStream(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	readAll(t, s)
	if !errors.Is(s.Err(), ErrStreamEndedEarly) {
		t.Errorf("a stream without [DONE]: err = %v", s.Err())
	}
	s.Close()
	if len(f.requests) != 2 {
		t.Errorf("a stream that began is never retried: %d requests", len(f.requests))
	}
}

func TestChat_Retries(t *testing.T) {
	t.Run("429 with Retry-After, then success", func(t *testing.T) {
		c, f, waits := newInferenceClient(t,
			inferenceErr(429, "rate_limit_error", "rate_limit_exceeded", "Too many requests", map[string]string{"Retry-After": "7"}),
			okCompletion)
		if _, err := c.Inference.Chat(context.Background(), ChatRequest{Model: "m", Messages: []Message{UserMessage("x")}}); err != nil {
			t.Fatal(err)
		}
		if len(f.requests) != 2 || len(*waits) != 1 || (*waits)[0] != 7*time.Second {
			t.Errorf("requests %d, waits %v: want one retry after 7s", len(f.requests), *waits)
		}
	})
	t.Run("503 and 502, then success", func(t *testing.T) {
		c, f, _ := newInferenceClient(t,
			inferenceErr(503, "api_error", "model_unavailable", "not available", nil),
			inferenceErr(502, "api_error", "upstream_error", "the model returned an error", nil),
			okCompletion)
		if _, err := c.Inference.Chat(context.Background(), ChatRequest{Model: "m", Messages: []Message{UserMessage("x")}}); err != nil {
			t.Fatal(err)
		}
		if len(f.requests) != 3 {
			t.Errorf("requests = %d", len(f.requests))
		}
	})
	t.Run("the limit is kept", func(t *testing.T) {
		busy := inferenceErr(503, "api_error", "model_unavailable", "not available", nil)
		c, f, _ := newInferenceClient(t, busy, busy, busy)
		_, err := c.Inference.Chat(context.Background(), ChatRequest{Model: "m", Messages: []Message{UserMessage("x")}})
		if !IsRetryable(err) || len(f.requests) != 3 {
			t.Errorf("err %v after %d requests, want the last 503 after 1+2", err, len(f.requests))
		}
	})
	t.Run("retries off", func(t *testing.T) {
		c, f, _ := newInferenceClient(t, inferenceErr(429, "rate_limit_error", "rate_limit_exceeded", "x", nil))
		c.maxRetries = 0
		_, err := c.Inference.Chat(context.Background(), ChatRequest{Model: "m", Messages: []Message{UserMessage("x")}})
		if !IsRateLimited(err) || len(f.requests) != 1 {
			t.Errorf("err %v after %d requests", err, len(f.requests))
		}
	})
	t.Run("a stream is retried before it begins", func(t *testing.T) {
		c, f, _ := newInferenceClient(t,
			inferenceErr(429, "rate_limit_error", "too_many_concurrent_requests", "x", map[string]string{"Retry-After": "1"}),
			sse(chunk("ok"), "data: [DONE]\n\n"))
		s, err := c.Inference.ChatStream(context.Background(), ChatRequest{Model: "m", Messages: []Message{UserMessage("x")}})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if readAll(t, s) != "ok" || s.Err() != nil || len(f.requests) != 2 {
			t.Errorf("err %v, %d requests", s.Err(), len(f.requests))
		}
	})
}

func TestChat_ErrorsAreTypedAndNotRetried(t *testing.T) {
	for _, tc := range []struct {
		status    int
		typ, code string
		helper    func(error) bool
	}{
		{400, "invalid_request_error", "invalid_request", IsBadRequest},
		{401, "authentication_error", "unauthorized", IsUnauthorized},
		{402, "insufficient_credit", "insufficient_credit", IsNoCredit},
		{403, "permission_error", "missing_scope", IsForbidden},
		{404, "invalid_request_error", "model_not_found", IsNotFound},
	} {
		c, f, _ := newInferenceClient(t, inferenceErr(tc.status, tc.typ, tc.code, "the reason", nil))
		_, err := c.Inference.Chat(context.Background(), ChatRequest{Model: "m", Messages: []Message{UserMessage("x")}})
		var ae *APIError
		if !errors.As(err, &ae) || ae.StatusCode != tc.status || ae.Code != tc.code || ae.Type != tc.typ ||
			ae.Message != "the reason" || ae.RequestID != "req-123" || !tc.helper(err) {
			t.Errorf("%d: err = %#v", tc.status, err)
		}
		if len(f.requests) != 1 {
			t.Errorf("%d was retried (%d requests)", tc.status, len(f.requests))
		}
	}
	// A 429 after the retries keeps its Retry-After.
	busy := inferenceErr(429, "rate_limit_error", "rate_limit_exceeded", "slow down", map[string]string{"Retry-After": "12"})
	c, _, _ := newInferenceClient(t, busy, busy, busy)
	_, err := c.Inference.Chat(context.Background(), ChatRequest{Model: "m", Messages: []Message{UserMessage("x")}})
	var ae *APIError
	if !errors.As(err, &ae) || ae.RetryAfter != 12*time.Second || !IsRateLimited(err) {
		t.Errorf("429: %#v", err)
	}
}

// The platform's own error shape still reads as before.
func TestDecodeError_PlatformShapeUnchanged(t *testing.T) {
	e := &APIError{}
	if !fillError(e, []byte(`{"error":"bucket not found","code":"bucket_not_found"}`)) || e.Message != "bucket not found" || e.Code != "bucket_not_found" || e.Type != "" {
		t.Errorf("%+v", e)
	}
	if fillError(&APIError{}, []byte(`{"something":"else"}`)) {
		t.Error("a body without an error is not one")
	}
}

// The billing-safe chat rule (contract, "Retries and timeouts"): the API bills
// a completion once the model has answered, even if the client has gone, so a
// chat is retried only when it certainly did not bill.

var chatReq = ChatRequest{Model: "m", Messages: []Message{UserMessage("x")}}

func TestChat_NotBoundByTheClientTimeout(t *testing.T) {
	c, f, _ := newInferenceClient(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond) // a long answer
		okCompletion(w, r)
	})
	c.timeout = 50 * time.Millisecond // the ordinary calls' bound
	if _, err := c.Inference.Chat(context.Background(), chatReq); err != nil {
		t.Fatalf("a chat longer than the client's timeout: %v", err)
	}
	if len(f.requests) != 1 {
		t.Errorf("requests = %d", len(f.requests))
	}
	ctx, cancel := chatDeadline(context.Background(), time.Minute)
	defer cancel()
	if d, ok := ctx.Deadline(); !ok || time.Until(d) < 59*time.Second {
		t.Error("a context without a deadline gets the default")
	}
	mine, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if got, _ := chatDeadline(mine, time.Hour); got != mine {
		t.Error("the caller's deadline is kept")
	}
	if DefaultChatTimeout != 600*time.Second {
		t.Errorf("DefaultChatTimeout = %s", DefaultChatTimeout)
	}
}

func TestChat_TimeoutAfterSendingIsNotRetried(t *testing.T) {
	slow := func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}
	c, f, _ := newInferenceClient(t, slow, slow, slow)
	c.Inference.defaultChatTimeout = 100 * time.Millisecond
	_, err := c.Inference.Chat(context.Background(), chatReq)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the deadline", err)
	}
	if len(f.requests) != 1 {
		t.Errorf("a timed-out chat was sent %d times: it may already have been billed", len(f.requests))
	}
}

func TestChat_ResetAfterSendingIsNotRetried(t *testing.T) {
	reset := func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}
	c, f, _ := newInferenceClient(t, reset, reset, reset)
	if _, err := c.Inference.Chat(context.Background(), chatReq); err == nil {
		t.Fatal("a cut connection must fail")
	}
	if len(f.requests) != 1 {
		t.Errorf("a chat cut after sending was sent %d times", len(f.requests))
	}
	// A GET is safe to repeat.
	c2, f2, _ := newInferenceClient(t, reset, func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"object":"list","data":[]}`)
	})
	if _, err := c2.Inference.ListModels(context.Background()); err != nil || len(f2.requests) != 2 {
		t.Errorf("ListModels after a reset: %v, %d requests", err, len(f2.requests))
	}
}

// refuseFirst fails the first request as a refused connection (it never left).
type refuseFirst struct {
	next  http.RoundTripper
	calls int
}

func (r *refuseFirst) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls++
	if r.calls == 1 {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}
	}
	return r.next.RoundTrip(req)
}

func TestChat_ConnectionRefusedIsRetried(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(okCompletion))
	defer srv.Close()
	rt := &refuseFirst{next: http.DefaultTransport}
	c := NewClient(Config{BaseURL: srv.URL, APIKey: "k", HTTPClient: &http.Client{Transport: rt}})
	c.sleep = func(context.Context, time.Duration) error { return nil }
	if _, err := c.Inference.Chat(context.Background(), chatReq); err != nil {
		t.Fatalf("a refused connection never sent the chat: %v", err)
	}
	if rt.calls != 2 {
		t.Errorf("calls = %d, want the refusal and a retry", rt.calls)
	}
}

func htmlBadGateway(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusBadGateway)
	io.WriteString(w, "<html><body><h1>502 Bad Gateway</h1></body></html>")
}

func TestChat_OnlyTeepinsOwn5xxIsRetried(t *testing.T) {
	c, f, _ := newInferenceClient(t, htmlBadGateway, okCompletion)
	_, err := c.Inference.Chat(context.Background(), chatReq)
	if !statusIs(err, http.StatusBadGateway) || len(f.requests) != 1 {
		t.Errorf("an HTML 502 on a chat: %v after %d requests, want no retry", err, len(f.requests))
	}
	// An envelope with another code is not one the API sends before billing.
	c, f, _ = newInferenceClient(t, inferenceErr(502, "api_error", "something_else", "x", nil), okCompletion)
	if _, err := c.Inference.Chat(context.Background(), chatReq); err == nil || len(f.requests) != 1 {
		t.Errorf("an unknown 502 code on a chat: %v after %d requests", err, len(f.requests))
	}
	gw := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusGatewayTimeout) }
	c, f, _ = newInferenceClient(t, gw, okCompletion)
	if _, err := c.Inference.Chat(context.Background(), chatReq); !statusIs(err, http.StatusGatewayTimeout) || len(f.requests) != 1 {
		t.Errorf("a 504 on a chat: %v after %d requests", err, len(f.requests))
	}
	c, f, _ = newInferenceClient(t,
		inferenceErr(502, "api_error", "upstream_error", "the model returned an error", nil),
		inferenceErr(503, "api_error", "billing_unavailable", "credit could not be checked", nil),
		inferenceErr(500, "api_error", "internal_error", "could not look up the model", nil),
		okCompletion)
	c.maxRetries = 3
	if _, err := c.Inference.Chat(context.Background(), chatReq); err != nil || len(f.requests) != 4 {
		t.Errorf("Teepin's own 502, 503 and 500: %v after %d requests", err, len(f.requests))
	}
	c, f, _ = newInferenceClient(t, htmlBadGateway, func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"object":"list","data":[]}`)
	})
	if _, err := c.Inference.ListModels(context.Background()); err != nil || len(f.requests) != 2 {
		t.Errorf("an HTML 502 on a GET is retried: %v after %d requests", err, len(f.requests))
	}
}
