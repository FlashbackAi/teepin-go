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

First, get your API key:
```bash
# Login to TEEPIN
teepin auth login

# Your API key is saved to ~/.teepin/credentials
```

Then use the SDK:
```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"

    "github.com/FlashbackAi/teepin-go/teepin"
)

func main() {
    // Get API key from environment or file
    apiKey := os.Getenv("TEEPIN_API_KEY")
    if apiKey == "" {
        log.Fatal("TEEPIN_API_KEY environment variable is required")
    }

    // Create authenticated client
    client := teepin.NewClient(teepin.Config{
        BaseURL: "http://localhost:8080",
        APIKey:  apiKey,
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

### Teepin Storage

```go
bucket, err := client.Storage.CreateBucket(ctx, "my-bucket")

// Any size: one request up to 16 MiB, parallel parts above that. Each part
// is retried on its own; the stored object's ETag is checked against the
// bytes sent. Resume continues an upload that was cut off.
obj, err := client.Storage.UploadFile(ctx, "my-bucket", "videos/a.mp4", "a.mp4", &teepin.UploadOptions{
    Resume:   true,
    Progress: func(done, total int64) { fmt.Printf("%d of %d", done, total) },
})

// Parallel ranges, written to a temporary file and renamed into place,
// then checked against the stored SHA-256.
_, err = client.Storage.DownloadFile(ctx, "my-bucket", "videos/a.mp4", "a-copy.mp4", nil)

// Every object under a prefix, page by page.
for o, err := range client.Storage.Objects(ctx, "my-bucket", "videos/") {
    if err != nil {
        return err
    }
    fmt.Println(o.Key, o.Size)
}

// Like folders: files directly under the prefix, and the folders below it.
page, err := client.Storage.ListObjects(ctx, "my-bucket", &teepin.ListObjectsOptions{Prefix: "videos/", Delimiter: "/"})

// Write only if nothing is there yet; read only if it changed.
_, err = client.Storage.PutObject(ctx, "my-bucket", "lock", strings.NewReader("1"), 1, &teepin.PutObjectOptions{IfNoneMatch: true})
r, err := client.Storage.GetObject(ctx, "my-bucket", "videos/a.mp4", &teepin.GetObjectOptions{IfNoneMatch: obj.ETag})

// Rename is instant at any size; copy writes the bytes again.
_, err = client.Storage.RenameObject(ctx, "my-bucket", "videos/a.mp4", "archive/a.mp4", false)
_, err = client.Storage.CopyObject(ctx, "backup", "a.mp4", "my-bucket", "archive/a.mp4", nil)
res, err := client.Storage.DeleteObjects(ctx, "my-bucket", keys) // 1000 to a request

// Links that work without your key: for a browser to download, or to upload once.
link, err := client.Storage.PresignDownload(ctx, "my-bucket", "archive/a.mp4", &teepin.PresignDownloadOptions{TTL: time.Hour})
up, err := client.Storage.PresignUpload(ctx, "inbox", "upload.bin", &teepin.PresignUploadOptions{MaxBytes: 100 << 20})
```

Every call but a signed link sends your API key, so make them from a backend
you control. To let a browser download or upload one object, sign a link on
your backend and hand it only the link. The calls map one to one onto the API
reference at `https://api.teepin.com/v1/openapi.json`.

### Errors

```go
_, err := client.Storage.StatObject(ctx, "my-bucket", "nope")
switch {
case teepin.IsNotFound(err):
    // 404
case teepin.IsOnHold(err):
    // the account has no credit; see err.(*teepin.APIError).DeleteAfter
case teepin.IsNoCredit(err), teepin.IsTooLarge(err), teepin.IsPreconditionFailed(err):
case err != nil:
    var ae *teepin.APIError
    if errors.As(err, &ae) {
        log.Printf("%s (%s, request %s)", ae.Message, ae.Code, ae.RequestID)
    }
}
```

Requests that fail for a passing reason (the network, a busy server) are
retried with backoff, `Config.MaxRetries` times (default 4).

## Configuration

### Client Options

```go
client := teepin.NewClient(teepin.Config{
    BaseURL: "https://api.teepin.com",    // the default
    APIKey:  "tpk_your_api_key",          // API key (v0.2+)
    Timeout: 30 * time.Second,            // per call; transfers are bounded by ctx
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
    Storage   *StorageService
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
- [storage](examples/storage/main.go) - Create a bucket, upload/download/delete objects, mint a signed link

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

### v0.2.0 (Current)
- ✅ Authentication support (API keys)
- ✅ Project/workspace management
- 🚧 Billing and usage APIs
- 🚧 Metrics and monitoring APIs

### v0.3.0 (Coming Soon)
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
- [API Reference (OpenAPI)](https://api.teepin.com/v1/openapi.json)

## Support

- **Issues**: [GitHub Issues](https://github.com/FlashbackAi/teepin-go/issues)
- **Discussions**: [GitHub Discussions](https://github.com/FlashbackAi/teepin-core/discussions)
- **Email**: support@teepin.cloud
