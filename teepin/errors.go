// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
)

// Errors, mapped from the Python SDK's names (TEEPIN-STORAGE-DESIGN.md
// section 17.10; the same set in Python, Node and Go):
//
//	Python                   Go
//	TeepinError              any error this package returns (there is no base type)
//	APIError                 *APIError, with helpers such as IsNotFound and IsOnHold
//	NotFoundError            IsNotFound(err): a 404, or an erased key
//	KeyErasedError           IsKeyErased(err), or errors.Is(err, ErrKeyErased): 410 key_erased (also IsNotFound)
//	PreconditionFailedError  IsPreconditionFailed(err)
//	TooLargeError            IsTooLarge(err): 413, object_too_large, or a stream over its parts (too_many_parts)
//	TooLargeError, reason    errors.Is(err, ErrConnectionSpeed) (also IsTooLarge, code connection_speed): an
//	  "connection_speed"     upload the SDK refused itself, before creating or sending anything, as too large
//	                         for the connection at its measured speed (section 22.7)
//	StorageUnavailableError  *StorageUnavailableError (errors.As): waited Config.UploadWait for storage
//	FileChangedError         *FileChangedError (errors.As): a source that changed while it was uploaded
//	RateLimitError           IsRateLimited(err)
//	APIConnectionError       IsRetryable(err): the request got no answer (its type is unexported)
//	ObjectChangedError       errors.Is(err, ErrObjectChanged)
//	DataIntegrityError       *DataIntegrityError (errors.As), with its Reason
//	ChecksumMismatchError    errors.Is(err, ErrChecksumMismatch); from a read also a *DataIntegrityError whose
//	                         Reason is "checksum", as Python's is a subclass of DataIntegrityError
//	(none)                   errors.Is(err, ErrObjectStaged): an object still staged on the API, which Go never
//	                         reads through the API (Python and Node read it through the content route)
//	UnsafePathError (Node)   *UnsafePathError (errors.As), or errors.Is(err, ErrUnsafePath): a batch download's
//	                         key that isn't a safe path under its destination (Python's ValueError)
//
// A cancelled context ends a call with the context's error (errors.Is(err,
// context.Canceled)), and is never retried.

// APIError is an error answer from the API: its status, the API's stable code
// and its message. It is also the storage network's answer
// to a signed request, with that answer's own Code (e.g. "BadDigest").
type APIError = errs.APIError

// DataIntegrityError is data that failed a check, after one retry: a segment
// that doesn't authenticate, a key that doesn't match the object, a damaged
// pack, or bytes that don't match the object's checksum. Reason says which
// check failed. Unverified bytes never reach a caller.
type DataIntegrityError = errs.DataIntegrityError

// StorageUnavailableError is an upload that waited as long as it may
// (Config.UploadWait) for the storage network or the control plane to come
// back. A run's files stay journaled, pending, for ResumeUploads.
type StorageUnavailableError = errs.StorageUnavailableError

// UnsafePathError is a key DownloadFiles or DownloadPrefix refused to write
// under its destination: not a safe path there ("..", an absolute path, a
// name the system can't hold), a link under the destination, a part file
// replaced while it was written, or another key's file.
// errors.Is(err, ErrUnsafePath) reports it too.
type UnsafePathError = errs.UnsafePathError

// ErrUnsafePath is what errors.Is finds in an UnsafePathError.
var ErrUnsafePath = errs.ErrUnsafePath

// FileChangedError is a source that changed while it was uploaded (its size
// or bytes since it was queued), or couldn't be read again: it is never
// uploaded mixed with its old bytes.
type FileChangedError = errs.FileChangedError

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
	var all map[string]any
	if json.Unmarshal(raw, &all) == nil {
		delete(all, "error")
		delete(all, "code")
		delete(all, "delete_after")
		if len(all) > 0 {
			e.Details = all
		}
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

// IsNotFound reports a missing bucket, object, upload or instance (404), or
// an object whose encryption key has been erased (see IsKeyErased).
func IsNotFound(err error) bool { return statusIs(err, http.StatusNotFound) || IsKeyErased(err) }

// IsKeyErased reports an object whose encryption key has been erased (410
// key_erased): it can never be read again.
func IsKeyErased(err error) bool { return errors.Is(err, ErrKeyErased) }

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

// IsTooLarge reports an object or part over the size allowed (413). An
// upload refused for the connection's measured speed is one too; tell it
// apart with errors.Is(err, ErrConnectionSpeed).
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
var ErrNotModified = errs.ErrNotModified

// ErrObjectChanged is a download whose object was replaced part way
// through: the bytes already written are from the old object.
var ErrObjectChanged = errs.ErrObjectChanged

// ErrChecksumMismatch is a download whose bytes do not match the object's
// SHA-256.
var ErrChecksumMismatch = errs.ErrChecksumMismatch

// ErrKeyErased is an object whose encryption key has been erased (410
// key_erased): it can never be read again. IsNotFound reports it too.
var ErrKeyErased = errs.ErrKeyErased

// ErrConnectionSpeed is what errors.Is finds in an upload this SDK refused
// itself, before creating or sending anything, because the connection to
// storage measured too slow to send it (TEEPIN-STORAGE-DESIGN.md section
// 22.7): even a 5 MiB part would take over 4 minutes at the measured speed,
// or the object would need more than 3,000 parts of the largest size the
// speed allows. The error is an *APIError with status 413 and code
// "connection_speed", so IsTooLarge reports it too; its message names the
// object and the speed measured. Upload it over a faster connection, or in
// smaller objects.
var ErrConnectionSpeed = errs.ErrConnectionSpeed

// ErrObjectStaged is a read of an object still staged on the API, which no
// longer serves an object's bytes (TEEPIN-STORAGE-DESIGN.md section 23.5).
// The stage is drained before that cut-over, so it is readable once the
// stage has drained.
var ErrObjectStaged = errs.ErrObjectStaged

// decodeStorageError reads the storage network's error answer to a signed
// request: an S3-style document (<Error><Code>...</Code><Message>...</Message>),
// or anything decodeError reads.
func decodeStorageError(resp *http.Response) error {
	e := &APIError{StatusCode: resp.StatusCode, RequestID: resp.Header.Get("X-Request-ID")}
	if s := resp.Header.Get("Retry-After"); s != "" {
		if secs, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && secs > 0 {
			e.RetryAfter = time.Duration(secs) * time.Second
		}
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var doc struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	switch {
	case xml.Unmarshal(raw, &doc) == nil && doc.Code != "":
		e.Code, e.Message = doc.Code, doc.Message
	case fillError(e, raw):
	default:
		e.Message = strings.TrimSpace(string(raw))
	}
	return e
}
