# Teepin SDK for Go

Teepin Web Services runs AI models in confidential enclaves (Teepin Inference) and stores your files (Teepin
Storage). This is the official Go client.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?logo=go)](https://go.dev/)
[![GoDoc](https://godoc.org/github.com/FlashbackAi/teepin-go?status.svg)](https://godoc.org/github.com/FlashbackAi/teepin-go/teepin)

Links: [Docs](https://www.teepin.com/docs) · [console.teepin.com](https://console.teepin.com) · [teepin.com](https://teepin.com)

## Install

```bash
go get github.com/FlashbackAi/teepin-go/teepin
```

Go 1.23 or newer.

## Authentication

Create an API key in [console.teepin.com](https://console.teepin.com): open a project, then Settings, then API keys.
A key belongs to one project and has the permissions you give it (for example `inference:invoke`,
`storage:read`, `storage:write`).

With `APIKey` or `BaseURL` left empty, the client reads `TEEPIN_API_KEY` and `TEEPIN_BASE_URL`. A value in the
`Config` always wins. The address defaults to `https://api.teepin.com`.

```bash
export TEEPIN_API_KEY=tpk_...
```

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/FlashbackAi/teepin-go/teepin"
)

func main() {
	client := teepin.NewClient(teepin.Config{}) // reads TEEPIN_API_KEY and TEEPIN_BASE_URL
	resp, err := client.Inference.Chat(context.Background(), teepin.ChatRequest{
		Model:              "teepin/glm-5.3",
		Messages:           []teepin.Message{teepin.UserMessage("What is TLS? One sentence.")},
		MaxTokens:          300,
		ChatTemplateKwargs: map[string]any{"enable_thinking": false},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Text())
}
```

Keep your API key on your server. To let a browser download or upload one file, sign a link on your server (see
[Signed links](#signed-links)).

## Teepin Inference

An OpenAI-compatible chat API. The key needs `inference:invoke`, and may be limited to some models.

### Models

```go
models, err := client.Inference.ListModels(ctx)
for _, m := range models {
	fmt.Println(m.ID, m.DisplayName, m.Confidential, m.Pricing.InputPerMillionTokens, m.Pricing.OutputPerMillionTokens)
}
```

### One answer

```go
resp, err := client.Inference.Chat(ctx, teepin.ChatRequest{
	Model:              "teepin/glm-5.3",
	Messages:           []teepin.Message{teepin.SystemMessage("Be brief."), teepin.UserMessage("What is TLS?")},
	MaxTokens:          300,
	ChatTemplateKwargs: map[string]any{"enable_thinking": false},
})
if err != nil {
	return err
}
fmt.Println(resp.Text(), resp.Usage.TotalTokens, resp.Meta.RequestID)
```

### Streaming

```go
stream, err := client.Inference.ChatStream(ctx, teepin.ChatRequest{
	Model:         "teepin/glm-5.3",
	Messages:      []teepin.Message{teepin.UserMessage("Write a haiku about the sea.")},
	StreamOptions: &teepin.StreamOptions{IncludeUsage: true},
})
if err != nil {
	return err
}
defer stream.Close()
for stream.Next() {
	fmt.Print(stream.Chunk().Text())
}
if err := stream.Err(); err != nil { // includes a stream cut off before the end
	return err
}
fmt.Println(stream.Usage())
```

### Reasoning models

Reasoning models (GLM-5.3, for example) think before they answer, and the thinking counts toward `MaxTokens`.
With a small limit the answer can come back empty, with finish reason `length`. For a short answer, turn thinking
off with `ChatTemplateKwargs: map[string]any{"enable_thinking": false}`, or give it room. The thinking itself, when
the model shows it, is in `Choices[0].Message.ReasoningContent`.

### Confidential models and attestation

Confidential models run in hardware enclaves that prove what code they run. Each answer says how far the enclave
was verified for that call: `resp.Meta.Attestation` (and `stream.Meta.Attestation`) is
`teepin.AttestationVerified`, `teepin.AttestationOlderRelease` or `teepin.AttestationNotVerified`. Anything less
than verified is a warning to show, never an error.

```go
proof, err := client.Inference.Attestation(ctx, "teepin/glm-5.3") // the live proof, as JSON, to verify yourself
```

### Errors, retries and rate limits

Limits are per API key: by default 60 chat completions a minute and 10 at a time. A 429 is retried after its
`Retry-After`, and `teepin.IsRateLimited(err)` reports one that still failed.

The server bills an answer once the model has written it, even if the caller has gone. So a chat is retried only
when it certainly wasn't billed: a 429, Teepin's own 500, 502 or 503, or a request that never reached the server.
A chat that timed out or lost its connection after it was sent is returned, not retried. A stream is never
retried once it has begun.

A chat that isn't streamed waits up to the context's deadline, or `teepin.DefaultChatTimeout` (600 seconds) when it
has none.

### With an OpenAI client

The API is OpenAI-compatible. Any OpenAI client works with the base URL `https://api.teepin.com/v1` and your
Teepin API key. This SDK adds the attestation results, billing-safe retries and Teepin's errors.

## Teepin Storage

Buckets and files, with large uploads in parallel parts. The key needs `storage:read` to read and `storage:write`
to write.

### Buckets

```go
bucket, err := client.Storage.CreateBucket(ctx, "my-bucket")
buckets, err := client.Storage.ListBuckets(ctx)
err = client.Storage.DeleteBucket(ctx, "my-bucket") // only when it is empty
```

### Put and get

```go
obj, err := client.Storage.PutObject(ctx, "my-bucket", "notes.txt", strings.NewReader("hello"), 5, nil)

r, err := client.Storage.GetObject(ctx, "my-bucket", "notes.txt", nil)
if err != nil {
	return err
}
defer r.Close()
data, err := io.ReadAll(r)

info, err := client.Storage.StatObject(ctx, "my-bucket", "notes.txt") // size, ETag, content type
```

### Large files

An upload of any size goes in one request up to 16 MiB, and in parallel parts above that. Each part is retried on
its own and checked against the bytes sent. `Resume` continues an upload that was cut off. A download fetches
ranges in parallel into a temporary file, renames it into place and checks the SHA-256.

```go
obj, err := client.Storage.UploadFile(ctx, "my-bucket", "videos/a.mp4", "a.mp4", &teepin.UploadOptions{
	Resume:   true,
	Progress: func(done, total int64) { fmt.Printf("%d of %d\n", done, total) },
})
_, err = client.Storage.DownloadFile(ctx, "my-bucket", "videos/a.mp4", "a-copy.mp4", nil)
```

`Upload`, `UploadStream`, `Download` and `DownloadStream` do the same with readers and writers.

### List

```go
// Every object under a prefix, page by page.
for o, err := range client.Storage.Objects(ctx, "my-bucket", "videos/") {
	if err != nil {
		return err
	}
	fmt.Println(o.Key, o.Size)
}

// One page, with "folders".
page, err := client.Storage.ListObjects(ctx, "my-bucket", &teepin.ListObjectsOptions{Prefix: "videos/", Delimiter: "/"})
```

### Signed links

Links that work without your key, for a limited time:

```go
link, err := client.Storage.PresignDownload(ctx, "my-bucket", "videos/a.mp4", &teepin.PresignDownloadOptions{TTL: time.Hour})
fmt.Println(link.URL)

up, err := client.Storage.PresignUpload(ctx, "my-bucket", "inbox/upload.bin", &teepin.PresignUploadOptions{MaxBytes: 100 << 20})
```

A browser then does one PUT to `up.URL`, with `up.Method` and `up.Headers`.

### Rename, copy and delete

```go
_, err = client.Storage.RenameObject(ctx, "my-bucket", "videos/a.mp4", "archive/a.mp4", false) // instant at any size
_, err = client.Storage.CopyObject(ctx, "backup", "a.mp4", "my-bucket", "archive/a.mp4", nil) // to backup/a.mp4
err = client.Storage.DeleteObject(ctx, "my-bucket", "notes.txt")
res, err := client.Storage.DeleteObjects(ctx, "my-bucket", []string{"a.txt", "b.txt"}) // up to 1000 a request
```

### The S3 endpoint

Teepin Storage also speaks S3 at `https://s3.teepin.com`, so the AWS CLI, the AWS SDK for Go, boto3 and rclone
work unchanged. Create an S3 access key in the console (a project's Settings, then S3 access keys). The region is
`teepin`.

```bash
aws configure set aws_access_key_id TPAK... --profile teepin
aws configure set aws_secret_access_key ... --profile teepin
aws configure set region teepin --profile teepin
aws --profile teepin --endpoint-url https://s3.teepin.com s3 ls
```

The buckets are the same ones this SDK and the console show.

## Errors

Errors from the API are `*teepin.APIError` values, with the API's stable `Code`, `StatusCode`, `Message` and
`RequestID` (for support). Helpers check them:

```go
_, err := client.Storage.StatObject(ctx, "my-bucket", "nope")
switch {
case teepin.IsNotFound(err):
	// 404
case teepin.IsOnHold(err):
	// the account has no credit; see err.(*teepin.APIError).DeleteAfter
case teepin.IsNoCredit(err), teepin.IsRateLimited(err), teepin.IsTooLarge(err):
case err != nil:
	var ae *teepin.APIError
	if errors.As(err, &ae) {
		log.Printf("%s (%s, request %s)", ae.Message, ae.Code, ae.RequestID)
	}
}
```

Also `IsUnauthorized`, `IsForbidden`, `IsConflict`, `IsBadRequest`, `IsPreconditionFailed` and `IsRetryable`.
Storage calls that fail for a passing reason (the network, a busy server) are retried with backoff,
`Config.MaxRetries` times (default 4).

## Configuration

```go
client := teepin.NewClient(teepin.Config{
	APIKey:     "tpk_...",                // default: TEEPIN_API_KEY
	BaseURL:    "https://api.teepin.com", // default: TEEPIN_BASE_URL, else this
	Timeout:    60 * time.Second,         // per call that isn't a transfer or a chat; transfers follow ctx
	MaxRetries: 4,                        // negative turns retries off
})
```

## Other APIs

The SDK also has clients for services that aren't offered yet. They answer 404 until they launch.

## Examples

- [inference](examples/inference/main.go): list the models and stream an answer.
- [storage](examples/storage/main.go): a bucket, an upload and download, a signed link.

```bash
TEEPIN_API_KEY=tpk_... go run ./examples/inference teepin/glm-5.3 "Explain TLS briefly"
```

## Development

```bash
go test ./...
go vet ./...
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
