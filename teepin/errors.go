// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package sdk

import "fmt"

// APIError represents an error returned by the TEEPIN API
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error (status %d): %s", e.StatusCode, e.Message)
}

// IsNotFound returns true if the error is a 404 Not Found
func IsNotFound(err error) bool {
	apiErr, ok := err.(*APIError)
	return ok && apiErr.StatusCode == 404
}

// IsConflict returns true if the error is a 409 Conflict
func IsConflict(err error) bool {
	apiErr, ok := err.(*APIError)
	return ok && apiErr.StatusCode == 409
}

// IsBadRequest returns true if the error is a 400 Bad Request
func IsBadRequest(err error) bool {
	apiErr, ok := err.(*APIError)
	return ok && apiErr.StatusCode == 400
}
