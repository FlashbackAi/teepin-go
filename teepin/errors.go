// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// APIError is an error answer from the API.
type APIError struct {
	// StatusCode is the HTTP status.
	StatusCode int
	// Code is the API's stable code, e.g. "object_not_found" (empty for the
	// few answers that carry none).
	Code string
	// Type is the error's class on the inference API (OpenAI's envelope),
	// e.g. "rate_limit_error"; empty elsewhere.
	Type string
	// Message is the API's sentence, safe to show a person.
	Message string
	// RequestID identifies the request in Teepin's logs, for support.
	RequestID string
	// RetryAfter is the server's requested wait, when it sent one.
	RetryAfter time.Duration
	// DeleteAfter is set with storage_on_hold: when held data is deleted
	// unless credit is added.
	DeleteAfter *time.Time
}

func (e *APIError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(e.StatusCode)
	}
	if e.Code != "" {
		return fmt.Sprintf("teepin: %s (%d %s)", msg, e.StatusCode, e.Code)
	}
	return fmt.Sprintf("teepin: %s (%d)", msg, e.StatusCode)
}

func decodeError(resp *http.Response) error {
	e := &APIError{StatusCode: resp.StatusCode, RequestID: resp.Header.Get("X-Request-ID")}
	if s := resp.Header.Get("Retry-After"); s != "" {
		if secs, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && secs > 0 {
			e.RetryAfter = time.Duration(secs) * time.Second
		}
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if !fillError(e, raw) {
		e.Message = strings.TrimSpace(string(raw))
	}
	return e
}

// fillError reads an error body into e: the platform's {"error": "...",
// "code": "..."} or the inference API's OpenAI envelope {"error": {"message",
// "type", "code"}}. It reports whether the body was either.
func fillError(e *APIError, raw []byte) bool {
	var body struct {
		Error       json.RawMessage `json:"error"`
		Code        string          `json:"code"`
		DeleteAfter string          `json:"delete_after"`
	}
	if json.Unmarshal(raw, &body) != nil {
		return false
	}
	var envelope struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
	}
	var message string
	switch {
	case json.Unmarshal(body.Error, &message) == nil:
	case json.Unmarshal(body.Error, &envelope) == nil && (envelope.Message != "" || envelope.Code != ""):
		e.Message, e.Type, e.Code = envelope.Message, envelope.Type, envelope.Code
		return true
	}
	if message == "" && body.Code == "" {
		return false
	}
	e.Message, e.Code = message, body.Code
	if t, err := time.Parse(time.RFC3339, body.DeleteAfter); err == nil {
		e.DeleteAfter = &t
	}
	return true
}

// ErrorCode is err's API code, or "" when err is not an API error.
func ErrorCode(err error) string {
	var e *APIError
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

func statusIs(err error, status int) bool {
	var e *APIError
	return errors.As(err, &e) && e.StatusCode == status
}

// IsNotFound reports a missing bucket, object, upload or instance (404).
func IsNotFound(err error) bool { return statusIs(err, http.StatusNotFound) }

// IsConflict reports a conflict (409): a bucket that exists or is not
// empty, an object at a rename's destination, too many buckets.
func IsConflict(err error) bool { return statusIs(err, http.StatusConflict) }

// IsBadRequest reports a request the API refused as invalid (400).
func IsBadRequest(err error) bool { return statusIs(err, http.StatusBadRequest) }

// IsUnauthorized reports a missing or invalid credential (401).
func IsUnauthorized(err error) bool { return statusIs(err, http.StatusUnauthorized) }

// IsForbidden reports a credential without the permission needed (403),
// such as an API key without storage:write.
func IsForbidden(err error) bool { return statusIs(err, http.StatusForbidden) }

// IsPreconditionFailed reports a write refused by If-Match or
// If-None-Match (412).
func IsPreconditionFailed(err error) bool { return statusIs(err, http.StatusPreconditionFailed) }

// IsTooLarge reports an object or part over the size allowed (413).
func IsTooLarge(err error) bool {
	return statusIs(err, http.StatusRequestEntityTooLarge) || ErrorCode(err) == "object_too_large"
}

// IsRateLimited reports a request refused for going over a limit (429):
// requests per minute, or too many at once. APIError.RetryAfter says when
// to try again; the client has already retried it Config.MaxRetries times.
func IsRateLimited(err error) bool { return statusIs(err, http.StatusTooManyRequests) }

// IsNoCredit reports an account without credit for a write (402).
func IsNoCredit(err error) bool { return ErrorCode(err) == "insufficient_credit" }

// IsOnHold reports stored data held because the account has no credit; see
// APIError.DeleteAfter for when it will be deleted.
func IsOnHold(err error) bool { return ErrorCode(err) == "storage_on_hold" }

// IsRetryable reports an error that passes: the network, or a busy server.
// The client already retried it Config.MaxRetries times.
func IsRetryable(err error) bool {
	var te *transportError
	if errors.As(err, &te) {
		return true
	}
	var e *APIError
	if !errors.As(err, &e) {
		return false
	}
	switch e.StatusCode {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// ErrNotModified is GetObject's answer when IfNoneMatch matched (304).
var ErrNotModified = errors.New("teepin: not modified")

// ErrObjectChanged is a download whose object was replaced part way
// through: the bytes already written are from the old object.
var ErrObjectChanged = errors.New("teepin: the object changed during the download")

// ErrChecksumMismatch is a download whose bytes do not match the object's
// SHA-256.
var ErrChecksumMismatch = errors.New("teepin: downloaded bytes do not match the object's checksum")
