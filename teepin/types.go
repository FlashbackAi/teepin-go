// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package sdk

import "time"

// Instance represents a compute instance
type Instance struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Image         string            `json:"image"`
	Status        string            `json:"status"`
	GPUVRAM       string            `json:"gpu_vram,omitempty"`
	AllocatedVRAM string            `json:"allocated_vram,omitempty"`
	CPUUnits      int               `json:"cpu_units"`
	Memory        string            `json:"memory"`
	Endpoint      string            `json:"endpoint,omitempty"`
	InternalIP    string            `json:"internal_ip,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	Labels        map[string]string `json:"labels,omitempty"`
}

// CreateInstanceRequest represents a request to create an instance
type CreateInstanceRequest struct {
	Name     string            `json:"name"`
	Image    string            `json:"image"`
	GPUVRAM  string            `json:"gpu_vram,omitempty"`
	CPUUnits int               `json:"cpu_units"`
	Memory   string            `json:"memory"`
	Env      map[string]string `json:"env,omitempty"`
	Ports    []PortMapping     `json:"ports,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
}

// PortMapping defines port exposure
type PortMapping struct {
	Container int    `json:"container"`
	Public    int    `json:"public,omitempty"`
	Protocol  string `json:"protocol,omitempty"` // tcp, udp
}

// InstanceType represents available instance configurations
type InstanceType struct {
	Name         string  `json:"name"`
	GPUVRAM      string  `json:"gpu_vram,omitempty"`
	GPUMemoryGB  int     `json:"gpu_memory_gb,omitempty"`
	CPUUnits     int     `json:"cpu_units"`
	Memory       string  `json:"memory"`
	PricePerHour float64 `json:"price_per_hour"`
	Description  string  `json:"description"`
}

// ListInstancesResponse represents the response from listing instances
type ListInstancesResponse struct {
	Instances []Instance `json:"instances"`
	Count     int        `json:"count"`
}

// ListInstanceTypesResponse represents the response from listing instance types
type ListInstanceTypesResponse struct {
	InstanceTypes []InstanceType `json:"instance_types"`
}
