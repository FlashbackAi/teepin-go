// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

// Package errs holds the errors the SDK's packages share. Package teepin re-exports each under its public name (an
// alias, so it is the same type), and the direct engine (internal/direct) makes and inspects them; neither could
// import the other's.
package errs

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

// APIError is an error answer from the API, or from the storage network for a signed request.
type APIError struct {
	// StatusCode is the HTTP status.
	StatusCode int
	// Code is the API's stable code, e.g. "object_not_found" (empty for the
	// few answers that carry none). For the storage network's answers it is
	// the code of its error document, e.g. "BadDigest".
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
	// Details are the answer's other fields, such as the pack a
	// pack_committed answer names (nil when it has none).
	Details map[string]any
}

// Detail is a field of Details as a string ("" when it isn't one).
func (e *APIError) Detail(name string) string {
	s, _ := e.Details[name].(string)
	return s
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

// Is reports an erased key (410 key_erased) as ErrKeyErased, and an upload refused for the connection's measured
// speed (413 connection_speed) as ErrConnectionSpeed, so errors.Is finds them.
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrKeyErased:
		return e.Code == CodeKeyErased
	case ErrConnectionSpeed:
		return e.Code == CodeConnectionSpeed
	}
	return false
}

// CodeKeyErased is the code of an object whose encryption key has been erased: it can never be read again.
const CodeKeyErased = "key_erased"

// CodeConnectionSpeed is the code of an upload the SDK refused itself, before creating or sending anything, because
// the connection to storage measured too slow to send it (TEEPIN-STORAGE-DESIGN.md section 22.7). Its status is 413.
const CodeConnectionSpeed = "connection_speed"

// ConnError is a request that got no HTTP answer: the network failed, the connection dropped, or the server sent
// nothing in time. It is a passing failure, worth another try.
type ConnError struct {
	Err error
	// Unsent is set when the request certainly never reached the server.
	Unsent bool
}

func (e *ConnError) Error() string { return "teepin: " + e.Err.Error() }
func (e *ConnError) Unwrap() error { return e.Err }

// Connection is a ConnError with a message of its own.
func Connection(format string, args ...any) *ConnError {
	return &ConnError{Err: fmt.Errorf(format, args...)}
}

// The reasons a DataIntegrityError gives, as the Python and Node SDKs name them.
const (
	ReasonSegment    = "segment"    // a segment, or a teepin-v1 object, failed authentication
	ReasonCommitment = "commitment" // the data key does not match the object's header
	ReasonNeedle     = "needle"     // a pack's needle does not match the object
	ReasonTruncated  = "truncated"  // the bytes are cut short
	ReasonFormat     = "format"     // not a format this SDK reads
	ReasonChecksum   = "checksum"   // the whole object's SHA-256 does not match
	ReasonSize       = "size"       // the object decrypted to another size than its session's
	ReasonKey        = "key"        // the read session carried no usable key
	ReasonHeader     = "header"     // a TWS1 header missing, unsigned or changed between windows
	ReasonWindow     = "window"     // a window signed from another place than asked
	ReasonSession    = "session"    // a read session that signed nothing for an object with bytes
)

// DataIntegrityError is data that failed a check, after one retry: a segment that doesn't authenticate, a key that
// doesn't match the object, a damaged pack, or bytes that don't match the object's checksum. Unverified bytes never
// reach a caller, so nothing written before it is from a damaged read.
type DataIntegrityError struct {
	// Reason says which check failed: "segment", "commitment", "needle", "truncated", "format", "checksum", "size",
	// "key", "header", "window" or "session".
	Reason string
	// Message says what failed, for a person.
	Message string
}

func (e *DataIntegrityError) Error() string { return "teepin: " + e.Message }

// Is reports a checksum failure as ErrChecksumMismatch, so code written for that sentinel finds it.
func (e *DataIntegrityError) Is(target error) bool {
	return target == ErrChecksumMismatch && e.Reason == ReasonChecksum
}

// Integrity is a DataIntegrityError for reason, with a message.
func Integrity(reason, format string, args ...any) *DataIntegrityError {
	return &DataIntegrityError{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// Checksum is the DataIntegrityError of bytes that don't match their object's SHA-256.
func Checksum(detail string) *DataIntegrityError {
	msg := "the bytes do not match the object's checksum"
	if detail != "" {
		msg += ": " + detail
	}
	return &DataIntegrityError{Reason: ReasonChecksum, Message: msg}
}

// StorageUnavailableError is an upload that waited as long as it may (the client's UploadWait) for the storage network
// or the control plane to come back. A run's files stay journaled, pending, for ResumeUploads.
type StorageUnavailableError struct {
	// Message says what was waited for, for a person.
	Message string
	// Err is the last failure met while waiting.
	Err error
}

// Error says what was waited for.
func (e *StorageUnavailableError) Error() string { return "teepin: " + e.Message }

// Unwrap is the last failure met while waiting.
func (e *StorageUnavailableError) Unwrap() error { return e.Err }

// FileChangedError is a source that changed while it was uploaded (its size or its bytes since it was queued), or
// that couldn't be read again: it is never uploaded mixed with its old bytes.
type FileChangedError struct {
	// Path is the file (or the object's key, for a source that isn't a file).
	Path string
	// Detail says what changed, when known.
	Detail string
}

// Error names the file, and what changed when known.
func (e *FileChangedError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("teepin: %s changed while it was uploaded: %s", e.Path, e.Detail)
	}
	return fmt.Sprintf("teepin: %s changed while it was uploaded", e.Path)
}

// Changed is a FileChangedError of path, with a detail ("" for none).
func Changed(path, detail string) *FileChangedError {
	return &FileChangedError{Path: path, Detail: detail}
}

// UnsafePathError is a key a batch download can't write under its destination: not a safe path there (a "..", an
// absolute path, a name the system can't hold), a link under the destination, a part file replaced while it was
// written, or another key's file. errors.Is reports it as ErrUnsafePath.
type UnsafePathError struct {
	// Message says which key or path, and why, for a person.
	Message string
}

// Error says which key or path, and why.
func (e *UnsafePathError) Error() string { return "teepin: " + e.Message }

// Is reports it as ErrUnsafePath.
func (e *UnsafePathError) Is(target error) bool { return target == ErrUnsafePath }

// Unsafe is an UnsafePathError with a message.
func Unsafe(format string, args ...any) *UnsafePathError {
	return &UnsafePathError{Message: fmt.Sprintf(format, args...)}
}

var (
	// ErrUnsafePath is a key a batch download refused to write: see UnsafePathError.
	ErrUnsafePath = errors.New("teepin: not a safe path under the destination")
	// ErrNotModified is GetObject's answer when IfNoneMatch matched (304).
	ErrNotModified = errors.New("teepin: not modified")
	// ErrObjectChanged is a read whose object was replaced part way through.
	ErrObjectChanged = errors.New("teepin: the object changed during the download")
	// ErrChecksumMismatch is a download whose bytes do not match the object's SHA-256.
	ErrChecksumMismatch = errors.New("teepin: downloaded bytes do not match the object's checksum")
	// ErrKeyErased is an object whose encryption key has been erased (410 key_erased).
	ErrKeyErased = errors.New("teepin: the object's encryption key has been erased")
	// ErrConnectionSpeed is an upload too large to send over the connection at its measured speed (413
	// connection_speed): even the smallest part would take too long, or it would need too many parts.
	ErrConnectionSpeed = errors.New("teepin: too large to send over this connection at its measured speed")
	// ErrObjectStaged is an object whose read session says it is still staged on the API (source "api"). The API
	// serves no object's bytes since the cut-over (section 23.5), which drains the stage first, so it is readable
	// once the stage has drained.
	ErrObjectStaged = errors.New("teepin: the object is still staged on the API")
)
