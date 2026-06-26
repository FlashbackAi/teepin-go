// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package sdk

import (
	"context"
	"fmt"
)

// InstancesService handles instance-related API calls
type InstancesService struct {
	client *Client
}

// Create creates a new compute instance
func (s *InstancesService) Create(ctx context.Context, req *CreateInstanceRequest) (*Instance, error) {
	var instance Instance
	err := s.client.do(ctx, "POST", "/v1/compute/instances", req, &instance)
	if err != nil {
		return nil, fmt.Errorf("failed to create instance: %w", err)
	}
	return &instance, nil
}

// List returns all instances
func (s *InstancesService) List(ctx context.Context) ([]Instance, error) {
	var resp ListInstancesResponse
	err := s.client.do(ctx, "GET", "/v1/compute/instances", nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to list instances: %w", err)
	}
	return resp.Instances, nil
}

// Get returns a specific instance by ID
func (s *InstancesService) Get(ctx context.Context, instanceID string) (*Instance, error) {
	path := fmt.Sprintf("/v1/compute/instances/%s", instanceID)
	var instance Instance
	err := s.client.do(ctx, "GET", path, nil, &instance)
	if err != nil {
		return nil, fmt.Errorf("failed to get instance: %w", err)
	}
	return &instance, nil
}

// Delete deletes an instance
func (s *InstancesService) Delete(ctx context.Context, instanceID string) error {
	path := fmt.Sprintf("/v1/compute/instances/%s", instanceID)
	err := s.client.do(ctx, "DELETE", path, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to delete instance: %w", err)
	}
	return nil
}

// ListTypes returns available instance types
func (s *InstancesService) ListTypes(ctx context.Context) ([]InstanceType, error) {
	var resp ListInstanceTypesResponse
	err := s.client.do(ctx, "GET", "/v1/compute/instance-types", nil, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to list instance types: %w", err)
	}
	return resp.InstanceTypes, nil
}

// Logs streams logs from an instance (placeholder for future implementation)
func (s *InstancesService) Logs(ctx context.Context, instanceID string, follow bool) (<-chan string, error) {
	// TODO: Implement log streaming with WebSocket or SSE
	// For now, return an error
	return nil, fmt.Errorf("log streaming not yet implemented")
}

// Metrics returns metrics for an instance (placeholder for future implementation)
func (s *InstancesService) Metrics(ctx context.Context, instanceID string) (map[string]interface{}, error) {
	// TODO: Implement metrics retrieval
	path := fmt.Sprintf("/v1/compute/instances/%s/metrics", instanceID)
	var metrics map[string]interface{}
	err := s.client.do(ctx, "GET", path, nil, &metrics)
	if err != nil {
		return nil, fmt.Errorf("failed to get metrics: %w", err)
	}
	return metrics, nil
}
