// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

// Package sdk provides a Go client library for the TEEPIN API.
package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	defaultBaseURL = "http://localhost:8080"
	defaultTimeout = 30 * time.Second
)

// Client is the TEEPIN API client
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client

	// Service clients
	Instances *InstancesService
}

// Config holds configuration for the TEEPIN client
type Config struct {
	// BaseURL is the TEEPIN API endpoint (default: http://localhost:8080)
	BaseURL string

	// APIKey for authentication (optional for now)
	APIKey string

	// HTTPClient allows using a custom HTTP client
	HTTPClient *http.Client

	// Timeout for API requests (default: 30s)
	Timeout time.Duration
}

// NewClient creates a new TEEPIN API client
func NewClient(config Config) *Client {
	// Set defaults
	if config.BaseURL == "" {
		config.BaseURL = defaultBaseURL
	}
	if config.HTTPClient == nil {
		timeout := config.Timeout
		if timeout == 0 {
			timeout = defaultTimeout
		}
		config.HTTPClient = &http.Client{
			Timeout: timeout,
		}
	}

	client := &Client{
		baseURL:    config.BaseURL,
		apiKey:     config.APIKey,
		httpClient: config.HTTPClient,
	}

	// Initialize service clients
	client.Instances = &InstancesService{client: client}

	return client
}

// do executes an HTTP request and handles the response
func (c *Client) do(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	url := c.baseURL + path

	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "teepin-go-sdk/0.1.0")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// Check for errors
	if resp.StatusCode >= 400 {
		return &APIError{
			StatusCode: resp.StatusCode,
			Message:    string(respBody),
		}
	}

	// Parse response
	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}
	}

	return nil
}
