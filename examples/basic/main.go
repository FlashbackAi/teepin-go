// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/teepin/teepin-core/pkg/sdk"
)

func main() {
	// Create client
	client := sdk.NewClient(sdk.Config{
		BaseURL: "http://localhost:8080",
	})

	ctx := context.Background()

	fmt.Println("🚀 TEEPIN Go SDK Example")
	fmt.Println()

	// 1. List existing instances
	fmt.Println("📋 Listing instances...")
	instances, err := client.Instances.List(ctx)
	if err != nil {
		log.Fatal(err)
	}

	if len(instances) == 0 {
		fmt.Println("No instances found")
	} else {
		for _, inst := range instances {
			fmt.Printf("  • %s: %s (%s)\n", inst.ID, inst.Name, inst.Status)
		}
	}
	fmt.Println()

	// 2. Create a new instance
	fmt.Println("🎯 Creating new instance...")
	instance, err := client.Instances.Create(ctx, &sdk.CreateInstanceRequest{
		Name:     "sdk-test-instance",
		Image:    "nginx:latest",
		GPUVRAM:  "25GB",
		CPUUnits: 4,
		Memory:   "16GB",
		Env: map[string]string{
			"TEST_VAR": "example",
		},
		Labels: map[string]string{
			"created-by": "go-sdk",
			"example":    "true",
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✓ Instance created: %s\n", instance.ID)
	fmt.Printf("  Name:      %s\n", instance.Name)
	fmt.Printf("  Status:    %s\n", instance.Status)
	fmt.Printf("  GPU VRAM:  %s\n", instance.GPUVRAM)
	fmt.Printf("  CPU:       %d cores\n", instance.CPUUnits)
	fmt.Printf("  Memory:    %s\n", instance.Memory)
	fmt.Println()

	// 3. Get instance details
	fmt.Println("🔍 Fetching instance details...")
	retrieved, err := client.Instances.Get(ctx, instance.ID)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✓ Retrieved instance: %s\n", retrieved.Name)
	fmt.Printf("  Status:     %s\n", retrieved.Status)
	fmt.Printf("  Created:    %s\n", retrieved.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Println()

	// 4. List instances again (should show new instance)
	fmt.Println("📋 Listing instances again...")
	instances, err = client.Instances.List(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Total instances: %d\n", len(instances))
	for _, inst := range instances {
		fmt.Printf("  • %s: %s (%s)\n", inst.ID, inst.Name, inst.Status)
	}
	fmt.Println()

	// 5. Note about cleanup
	fmt.Println("💡 Note: To delete the instance, run:")
	fmt.Printf("   ./teepin delete %s\n", instance.ID)
	fmt.Println()
	fmt.Println("✓ Example completed successfully!")
}
