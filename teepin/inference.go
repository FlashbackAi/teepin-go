// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// InferenceService is Teepin Inference: an OpenAI-compatible chat API over
// Teepin's models, including confidential models served in hardware-attested
// enclaves.
//
//	resp, err := client.Inference.Chat(ctx, teepin.ChatRequest{
//		Model:    "teepin/llama-3.3-70b",
//		Messages: []teepin.Message{teepin.UserMessage("Hello")},
//	})
//	fmt.Println(resp.Text())
//
// A request refused for a passing reason (a 429 rate limit, a 5xx, no answer)
// is retried with backoff, honouring Retry-After (Config.MaxRetries). A
// stream is never retried once it has begun.
type InferenceService struct {
	client *Client
	// defaultChatTimeout replaces DefaultChatTimeout in tests.
	defaultChatTimeout time.Duration
}

// Attestation levels, from the X-Teepin-Attestation header of a confidential
// model's answer. Anything less than verified is a warning, not an error.
const (
	AttestationVerified     = "verified"
	AttestationOlderRelease = "verified-older-release"
	AttestationNotVerified  = "not-verified"
	attestationHeader       = "X-Teepin-Attestation"
	// DefaultChatTimeout bounds a non-streaming chat whose context has no
	// deadline: a long answer takes minutes (OpenAI's SDKs wait as long).
	DefaultChatTimeout       = 600 * time.Second
	inferenceStreamMaxFrame  = 4 << 20
	inferenceStreamFirstSize = 64 << 10
)

// Model is a model this credential may call.
type Model struct {
	// ID is the route to send as ChatRequest.Model, e.g. "teepin/llama-3.3-70b".
	ID            string  `json:"id"`
	Object        string  `json:"object"`
	Created       int64   `json:"created"`
	OwnedBy       string  `json:"owned_by"`
	DisplayName   string  `json:"display_name"`
	ContextWindow int     `json:"context_window,omitempty"`
	SupportsTools bool    `json:"supports_tools"`
	Vision        bool    `json:"supports_vision"`
	Audio         bool    `json:"supports_audio"`
	Pricing       Pricing `json:"pricing"`
	// Confidential is true for a model served in a hardware-attested enclave;
	// Attestation fetches the proof.
	Confidential bool `json:"confidential"`
	// Attestation is set on a confidential model that is not fully verified
	// right now: its level and a warning to show.
	Attestation *ModelAttestation `json:"attestation,omitempty"`
}

// Pricing is a model's price in USD per million tokens.
type Pricing struct {
	InputPerMillionTokens  float64 `json:"input_per_million_tokens"`
	OutputPerMillionTokens float64 `json:"output_per_million_tokens"`
}

// ModelAttestation is a confidential model's verification when it is less
// than fully verified.
type ModelAttestation struct {
	Level   string `json:"level"`
	Warning string `json:"warning"`
}

// Message is one chat message. Content is a string, or content parts
// ([]ContentPart, or anything that encodes as OpenAI's parts).
type Message struct {
	Role       string     `json:"role"`
	Content    any        `json:"content,omitempty"`
	Name       string     `json:"name,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ContentPart is one part of a message's content: text or an image.
type ContentPart struct {
	Type     string    `json:"type"` // "text" or "image_url"
	Text     string    `json:"text,omitempty"`
	ImageURL *ImageURL `json:"image_url,omitempty"`
}

// ImageURL is an image part's address (https:// or a data: URL).
type ImageURL struct {
	URL    string `json:"url"`
	Detail string `json:"detail,omitempty"`
}

// SystemMessage, UserMessage and AssistantMessage are text messages.
func SystemMessage(text string) Message    { return Message{Role: "system", Content: text} }
func UserMessage(text string) Message      { return Message{Role: "user", Content: text} }
func AssistantMessage(text string) Message { return Message{Role: "assistant", Content: text} }

// ToolMessage is a tool's result for the call named by toolCallID.
func ToolMessage(toolCallID, content string) Message {
	return Message{Role: "tool", Content: content, ToolCallID: toolCallID}
}

// Tool is a function the model may call.
type Tool struct {
	Type     string       `json:"type"` // "function"
	Function FunctionSpec `json:"function"`
}

// FunctionSpec describes a callable function; Parameters is a JSON Schema.
type FunctionSpec struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
}

// ToolCall is a model's call of a tool. In a stream, Index says which call a
// delta belongs to and the fields arrive in pieces.
type ToolCall struct {
	Index    *int         `json:"index,omitempty"`
	ID       string       `json:"id,omitempty"`
	Type     string       `json:"type,omitempty"`
	Function FunctionCall `json:"function"`
}

// FunctionCall is the function a ToolCall names, with its JSON arguments.
type FunctionCall struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

// StreamOptions: IncludeUsage asks for the token counts in the last chunk.
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// ChatRequest is a chat completion request. Model and Messages are required;
// the rest are optional (nil or zero leaves the server's default). Extra adds
// other fields the API accepts (top_k, min_p, logit_bias, ...).
type ChatRequest struct {
	Model            string         `json:"model"`
	Messages         []Message      `json:"messages"`
	MaxTokens        int            `json:"max_tokens,omitempty"`
	Temperature      *float64       `json:"temperature,omitempty"`
	TopP             *float64       `json:"top_p,omitempty"`
	Stop             []string       `json:"stop,omitempty"`
	Seed             *int64         `json:"seed,omitempty"`
	N                int            `json:"n,omitempty"`
	FrequencyPenalty *float64       `json:"frequency_penalty,omitempty"`
	PresencePenalty  *float64       `json:"presence_penalty,omitempty"`
	Tools            []Tool         `json:"tools,omitempty"`
	ToolChoice       any            `json:"tool_choice,omitempty"`
	ResponseFormat   any            `json:"response_format,omitempty"`
	StreamOptions    *StreamOptions `json:"stream_options,omitempty"`
	User             string         `json:"user,omitempty"`
	// ChatTemplateKwargs, e.g. {"enable_thinking": false} switches a
	// reasoning model's thinking off.
	ChatTemplateKwargs map[string]any `json:"chat_template_kwargs,omitempty"`
	// Extra fields, merged into the body (a field above wins).
	Extra map[string]any `json:"-"`
}

// Float64 and Int64 make the optional numbers' pointers inline:
// Temperature: teepin.Float64(0.2).
func Float64(v float64) *float64 { return &v }
func Int64(v int64) *int64       { return &v }

// body is the request as JSON, with Extra merged and stream set.
func (r ChatRequest) body(stream bool) (map[string]any, error) {
	if strings.TrimSpace(r.Model) == "" {
		return nil, errors.New("teepin: ChatRequest.Model is required")
	}
	if len(r.Messages) == 0 {
		return nil, errors.New("teepin: ChatRequest.Messages must not be empty")
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("teepin: encode chat request: %w", err)
	}
	out := map[string]any{}
	for k, v := range r.Extra {
		out[k] = v
	}
	var fields map[string]any
	if err := json.Unmarshal(b, &fields); err != nil {
		return nil, fmt.Errorf("teepin: encode chat request: %w", err)
	}
	for k, v := range fields {
		out[k] = v
	}
	if stream {
		out["stream"] = true
	} else {
		delete(out, "stream")
	}
	return out, nil
}

// Usage is a completion's token counts.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ResponseMessage is the model's answer.
type ResponseMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	// ReasoningContent is a reasoning model's thinking, when it shows it.
	ReasoningContent string     `json:"reasoning_content,omitempty"`
	ToolCalls        []ToolCall `json:"tool_calls,omitempty"`
}

// Choice is one of a completion's answers.
type Choice struct {
	Index        int             `json:"index"`
	Message      ResponseMessage `json:"message"`
	FinishReason string          `json:"finish_reason"`
}

// ResponseMeta is what the response's headers say.
type ResponseMeta struct {
	// Attestation is a confidential model's verification for this call:
	// AttestationVerified, AttestationOlderRelease or AttestationNotVerified;
	// empty for other models.
	Attestation string
	// RequestID identifies the request in Teepin's logs, for support.
	RequestID string
	// RateLimit is the key's limit, when the API sent it.
	RateLimit *RateLimit
}

// RateLimit is the X-RateLimit-* headers: requests allowed in the window,
// left in it, and when it resets.
type RateLimit struct {
	Limit     int
	Remaining int
	Reset     time.Time
}

func metaOf(h http.Header) ResponseMeta {
	m := ResponseMeta{Attestation: h.Get(attestationHeader), RequestID: h.Get("X-Request-ID")}
	limit, err1 := strconv.Atoi(h.Get("X-RateLimit-Limit"))
	remaining, err2 := strconv.Atoi(h.Get("X-RateLimit-Remaining"))
	reset, err3 := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64)
	if err1 == nil && err2 == nil && err3 == nil {
		m.RateLimit = &RateLimit{Limit: limit, Remaining: remaining, Reset: time.Unix(reset, 0)}
	}
	return m
}

// ChatResponse is a chat completion.
type ChatResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
	// Raw is the whole body, for fields this SDK doesn't model.
	Raw json.RawMessage `json:"-"`
	// Meta is what the headers said: attestation, request ID, rate limit.
	Meta ResponseMeta `json:"-"`
}

// Text is the first answer's content ("" when there is none).
func (r *ChatResponse) Text() string {
	if r == nil || len(r.Choices) == 0 {
		return ""
	}
	return r.Choices[0].Message.Content
}

// ListModels lists the models this credential may call.
func (s *InferenceService) ListModels(ctx context.Context) ([]Model, error) {
	var out struct {
		Data []Model `json:"data"`
	}
	if err := s.client.call(ctx, &request{method: http.MethodGet, path: "/v1/models", mode: retrySafe}, &out); err != nil {
		return nil, err
	}
	if out.Data == nil {
		out.Data = []Model{}
	}
	return out.Data, nil
}

// Attestation is a confidential model's live attestation proof, as the
// provider issues it (verify it yourself). A model that is not confidential
// is a 404 with code "attestation_not_supported".
func (s *InferenceService) Attestation(ctx context.Context, model string) (json.RawMessage, error) {
	var out json.RawMessage
	r := &request{method: http.MethodGet, path: "/v1/models/attestation", query: url.Values{"model": {model}}, mode: retrySafe}
	if err := s.client.call(ctx, r, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Chat sends a chat completion and waits for the whole answer: up to ctx's
// deadline, or DefaultChatTimeout when it has none (never Config.Timeout: a
// long answer can take minutes). A chat that timed out is not retried, since
// the API may already have billed it.
func (s *InferenceService) Chat(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	body, err := req.body(false)
	if err != nil {
		return nil, err
	}
	ctx, cancel := chatDeadline(ctx, s.chatTimeout())
	defer cancel()
	resp, err := s.client.send(ctx, &request{method: http.MethodPost, path: "/v1/chat/completions", json: body, mode: retryChat, transfer: true})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("teepin: read chat completion: %w", err)
	}
	out := &ChatResponse{Raw: raw, Meta: metaOf(resp.Header)}
	if err := json.Unmarshal(raw, out); err != nil {
		return nil, fmt.Errorf("teepin: decode chat completion: %w", err)
	}
	return out, nil
}

// chatDeadline gives ctx the default deadline when it has none.
func chatDeadline(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// chatTimeout is DefaultChatTimeout, or the test's.
func (s *InferenceService) chatTimeout() time.Duration {
	if s.defaultChatTimeout > 0 {
		return s.defaultChatTimeout
	}
	return DefaultChatTimeout
}

// Delta is the part of an answer one stream chunk adds.
type Delta struct {
	Role             string     `json:"role,omitempty"`
	Content          string     `json:"content,omitempty"`
	ReasoningContent string     `json:"reasoning_content,omitempty"`
	ToolCalls        []ToolCall `json:"tool_calls,omitempty"`
}

// ChunkChoice is one answer's delta in a chunk.
type ChunkChoice struct {
	Index        int    `json:"index"`
	Delta        Delta  `json:"delta"`
	FinishReason string `json:"finish_reason,omitempty"`
}

// ChatChunk is one frame of a streamed completion.
type ChatChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	// Usage is set on the last chunk when StreamOptions.IncludeUsage is.
	Usage *Usage          `json:"usage,omitempty"`
	Raw   json.RawMessage `json:"-"`
}

// Text is the first answer's new content in this chunk.
func (c *ChatChunk) Text() string {
	if c == nil || len(c.Choices) == 0 {
		return ""
	}
	return c.Choices[0].Delta.Content
}

// ErrStreamEndedEarly is a stream that closed before the API said it was
// done: the answer is incomplete.
var ErrStreamEndedEarly = errors.New("teepin: the stream ended before the answer was complete")

// ChatStream is a streamed chat completion. Read it with Next and Chunk, then
// check Err; always Close it.
//
//	stream, err := client.Inference.ChatStream(ctx, req)
//	if err != nil { ... }
//	defer stream.Close()
//	for stream.Next() {
//		fmt.Print(stream.Chunk().Text())
//	}
//	if err := stream.Err(); err != nil { ... }
type ChatStream struct {
	// Meta is what the headers said, available before the first chunk.
	Meta ResponseMeta

	body   io.ReadCloser
	reader *bufio.Reader
	chunk  *ChatChunk
	usage  *Usage
	err    error
	done   bool
}

// ChatStream starts a streamed chat completion. Retries happen only before
// the stream begins.
func (s *InferenceService) ChatStream(ctx context.Context, req ChatRequest) (*ChatStream, error) {
	body, err := req.body(true)
	if err != nil {
		return nil, err
	}
	r := &request{method: http.MethodPost, path: "/v1/chat/completions", json: body, mode: retryChat, transfer: true,
		header: http.Header{"Accept": {"text/event-stream"}}}
	resp, err := s.client.send(ctx, r)
	if err != nil {
		return nil, err
	}
	return newChatStream(resp), nil
}

func newChatStream(resp *http.Response) *ChatStream {
	return &ChatStream{Meta: metaOf(resp.Header), body: resp.Body, reader: bufio.NewReaderSize(resp.Body, inferenceStreamFirstSize)}
}

// Next reads the next chunk, reporting false at the end or on an error.
func (s *ChatStream) Next() bool {
	if s.done || s.err != nil {
		return false
	}
	for {
		data, err := s.frame()
		if err != nil {
			s.err = err
			return false
		}
		if data == "[DONE]" {
			s.done = true
			return false
		}
		if failure := streamError(data); failure != nil {
			s.err = failure
			return false
		}
		chunk := &ChatChunk{Raw: json.RawMessage(data)}
		if err := json.Unmarshal([]byte(data), chunk); err != nil {
			s.err = fmt.Errorf("teepin: decode stream chunk: %w", err)
			return false
		}
		if chunk.Usage != nil {
			s.usage = chunk.Usage
		}
		s.chunk = chunk
		return true
	}
}

// frame reads one server-sent event's data: its "data:" lines joined, up to
// the blank line that ends it. Comments (":") and other fields are skipped.
func (s *ChatStream) frame() (string, error) {
	var data []string
	for {
		line, err := s.reader.ReadString('\n')
		if len(line) > inferenceStreamMaxFrame {
			return "", errors.New("teepin: a stream frame is too large")
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(data) > 0 || strings.HasPrefix(strings.TrimSpace(line), "data:") {
					data = append(data, fieldValue(strings.TrimRight(line, "\r\n")))
					return strings.Join(data, "\n"), nil
				}
				return "", ErrStreamEndedEarly
			}
			return "", fmt.Errorf("teepin: read stream: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			if len(data) > 0 {
				return strings.Join(data, "\n"), nil
			}
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "data:"):
			data = append(data, fieldValue(line))
		}
	}
}

// fieldValue is a "data:" line's value, without the one optional space.
func fieldValue(line string) string {
	v := strings.TrimPrefix(line, "data:")
	return strings.TrimPrefix(v, " ")
}

// streamError is the error frame the API sends when a stream fails part way:
// {"error": {"message", "type", "code"}}. Its StatusCode is the stream's own
// 200 (the headers were already sent), as in the other SDKs; Code says what
// failed ("stream_error").
func streamError(data string) error {
	if !strings.Contains(data, `"error"`) {
		return nil
	}
	var probe struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal([]byte(data), &probe) != nil || len(bytes.TrimSpace(probe.Error)) == 0 || string(probe.Error) == "null" {
		return nil
	}
	e := &APIError{StatusCode: http.StatusOK}
	if !fillError(e, []byte(data)) {
		e.Message = "the stream failed"
	}
	return e
}

// Chunk is the chunk Next just read.
func (s *ChatStream) Chunk() *ChatChunk { return s.chunk }

// Usage is the token counts, once the chunk carrying them has been read
// (ask for them with StreamOptions{IncludeUsage: true}).
func (s *ChatStream) Usage() *Usage { return s.usage }

// Err is the error that ended the stream, or nil when it completed.
func (s *ChatStream) Err() error { return s.err }

// Close releases the connection; it is safe to call more than once.
func (s *ChatStream) Close() error {
	if s.body == nil {
		return nil
	}
	err := s.body.Close()
	s.body = nil
	return err
}
