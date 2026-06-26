# TEEPIN Go SDK

Official Go client library for the TEEPIN GPU cloud platform.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://go.dev/)
[![GoDoc](https://godoc.org/github.com/FlashbackAi/teepin-go?status.svg)](https://godoc.org/github.com/FlashbackAi/teepin-go/teepin)

## Installation

```bash
go get github.com/FlashbackAi/teepin-go/teepin
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/FlashbackAi/teepin-go/teepin"
)

func main() {
    // Create client
    client := teepin.NewClient(teepin.Config{
        BaseURL: "http://localhost:8080",
        APIKey:  "your-api-key", // Optional for now
    })

    ctx := context.Background()

    // Create a GPU instance
    instance, err := client.Instances.Create(ctx, &teepin.CreateInstanceRequest{
        Name:     "my-gpu-instance",
        Image:    "pytorch/pytorch:latest",
        GpuVram:  "25GB",
        CpuUnits: 4,
        Memory:   "16GB",
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Created instance: %s\n", instance.ID)
    fmt.Printf("Status: %s\n", instance.Status)
    fmt.Printf("GPU VRAM: %s\n", instance.GpuVram)
}
```

## Features

- **Type-safe API**: Full Go type definitions for all API resources
- **Context support**: All methods accept `context.Context` for cancellation and timeouts
- **Error handling**: Typed error responses for different HTTP status codes
- **Automatic retries**: Built-in retry logic for transient failures (coming soon)
- **Streaming logs**: Real-time log streaming support (coming soon)

## Usage

### Creating Instances

```go
instance, err := client.Instances.Create(ctx, &teepin.CreateInstanceRequest{
    Name:     "training-job",
    Image:    "nvidia/cuda:12.0-base",
    GpuVram:  "40GB",
    CpuUnits: 8,
    Memory:   "32GB",
    Env: map[string]string{
        "MODEL_PATH": "/models/llama2",
    },
    Ports: []teepin.PortMapping{
        {Container: 8080, Public: 80, Protocol: "tcp"},
    },
})
```

### Listing Instances

```go
instances, err := client.Instances.List(ctx)
if err != nil {
    log.Fatal(err)
}

for _, instance := range instances {
    fmt.Printf("%s: %s (%s)\n", instance.ID, instance.Name, instance.Status)
}
```

### Getting Instance Details

```go
instance, err := client.Instances.Get(ctx, "inst-abc123")
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Instance: %s\n", instance.Name)
fmt.Printf("Image: %s\n", instance.Image)
fmt.Printf("Status: %s\n", instance.Status)
fmt.Printf("Created: %s\n", instance.CreatedAt)
```

### Deleting Instances

```go
err := client.Instances.Delete(ctx, "inst-abc123")
if err != nil {
    log.Fatal(err)
}
```

### Error Handling

```go
instance, err := client.Instances.Get(ctx, "non-existent")
if err != nil {
    switch e := err.(type) {
    case *teepin.NotFoundError:
        fmt.Println("Instance not found")
    case *teepin.UnauthorizedError:
        fmt.Println("Authentication failed")
    case *teepin.APIError:
        fmt.Printf("API error: %s (status %d)\n", e.Message, e.StatusCode)
    default:
        log.Fatal(err)
    }
}
```

## Configuration

### Client Options

```go
client := teepin.NewClient(teepin.Config{
    BaseURL: "https://api.teepin.cloud",  // API endpoint
    APIKey:  "tpk_your_api_key",          // API key (v0.2+)
    Timeout: 30 * time.Second,            // Request timeout
})
```

### Using Environment Variables

```go
// Set TEEPIN_API_URL and TEEPIN_API_KEY environment variables
client := teepin.NewClient(teepin.Config{
    BaseURL: os.Getenv("TEEPIN_API_URL"),
    APIKey:  os.Getenv("TEEPIN_API_KEY"),
})
```

## API Reference

### Client

```go
type Client struct {
    Instances *InstancesService
}

func NewClient(config Config) *Client
```

### InstancesService

```go
type InstancesService struct {
    // Methods
    Create(ctx context.Context, req *CreateInstanceRequest) (*Instance, error)
    List(ctx context.Context) ([]Instance, error)
    Get(ctx context.Context, instanceID string) (*Instance, error)
    Delete(ctx context.Context, instanceID string) error
}
```

### Types

```go
type Instance struct {
    ID            string
    Name          string
    Image         string
    Status        string
    GpuVram       string
    AllocatedVram string
    CpuUnits      int
    Memory        string
    Endpoint      string
    InternalIP    string
    CreatedAt     time.Time
    UpdatedAt     time.Time
    Labels        map[string]string
}

type CreateInstanceRequest struct {
    Name     string
    Image    string
    GpuVram  string
    CpuUnits int
    Memory   string
    Env      map[string]string
    Ports    []PortMapping
    Labels   map[string]string
}
```

## Examples

See [examples/](examples/) directory for complete examples:

- [basic](examples/basic/main.go) - Create, list, and delete instances
- More examples coming soon!

## Development

### Running Tests

```bash
go test ./...
```

### Building

```bash
go build ./teepin
```

## Roadmap

### v0.2.0 (Coming Soon)
- Authentication support (API keys, JWT)
- Project/workspace management
- Billing and usage APIs
- Metrics and monitoring APIs

### v0.3.0
- Streaming logs
- Real-time metrics
- Automatic retries
- Connection pooling

## Contributing

Contributions are welcome! Please see the [main repository](https://github.com/FlashbackAi/teepin-core) for contribution guidelines.

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

## Links

- [TEEPIN Core](https://github.com/FlashbackAi/teepin-core) - Main platform repository
- [Python SDK](https://github.com/FlashbackAi/teepin-python) - Python client library
- [TypeScript SDK](https://github.com/FlashbackAi/teepin-node) - TypeScript/Node.js client library
- [Documentation](https://docs.teepin.cloud) (coming soon)
- [API Reference](https://api.teepin.cloud/docs) (coming soon)

## Support

- **Issues**: [GitHub Issues](https://github.com/FlashbackAi/teepin-go/issues)
- **Discussions**: [GitHub Discussions](https://github.com/FlashbackAi/teepin-core/discussions)
- **Email**: support@teepin.cloud
