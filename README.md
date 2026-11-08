# Teepin SDK for Go

Teepin Web Services runs AI models in confidential enclaves (Teepin Inference) and stores your files (Teepin
Storage). This is the official Go client.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go)](https://go.dev/)
[![GoDoc](https://godoc.org/github.com/FlashbackAi/teepin-go?status.svg)](https://godoc.org/github.com/FlashbackAi/teepin-go/teepin)

Links: [Docs](https://www.teepin.com/docs) · [console.teepin.com](https://console.teepin.com) · [teepin.com](https://teepin.com)

## Install

```bash
go get github.com/FlashbackAi/teepin-go/teepin
```

Go 1.26 or newer (the SDK uses the standard library's `crypto/hpke`). It has no dependencies.

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

Keep your API key on your server.

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

Buckets and files of any size. Files are encrypted on this machine and sent straight to the storage network, and read
straight back from it, every byte verified before you get it. No file byte passes through Teepin's API, which keeps
the catalog and signs each request. The key needs `storage:read` to read and `storage:write` to write.

### Buckets and a first file

```go
s := client.Storage
_, err := s.CreateBucket(ctx, "my-bucket")
obj, err := s.UploadFile(ctx, "my-bucket", "videos/launch.mp4", "launch.mp4", nil)
fmt.Println(obj.Key, obj.Size, obj.ETag)

_, err = s.DownloadFile(ctx, "my-bucket", "videos/launch.mp4", "copy.mp4", nil)
buckets, err := s.ListBuckets(ctx)
err = s.DeleteBucket(ctx, "old-bucket") // only when it is empty
```

`UploadFile` sends a file of any size: up to 16 MiB is packed with other objects into one write, and a larger file
goes in parts sent in parallel. Each request is retried on its own, and the object is checked against the bytes read,
so a corrupted transfer is never reported as success. `DownloadFile` fetches a large object in parallel windows, all
pinned to the same version of the object, each byte verified before it is written. It writes a temporary file that is
renamed into place only when the whole object has been checked.

### Upload

```go
// bytes, or any reader of a known size (an io.ReaderAt or io.ReadSeeker is read from where it stands)
obj, err := s.PutObject(ctx, "b", "data.bin", bytes.NewReader(data), int64(len(data)),
	&teepin.PutObjectOptions{ContentType: "application/octet-stream", Metadata: map[string]string{"owner": "ana"}})

// progress and part size
obj, err = s.UploadFile(ctx, "b", "big.iso", "big.iso", &teepin.UploadOptions{
	PartSize: 64 << 20,
	Progress: func(done, total int64) { fmt.Printf("%d/%d\n", done, total) },
})

// many files at once: one result per file, and one file's failure never fails another
files, _ := teepin.FilesToUpload([]string{"a.jpg", "b.jpg"}, "photos/", "")
results, err := s.UploadFiles(ctx, "b", files, nil)
for _, r := range results {
	if !r.Saved() {
		fmt.Println(r.Key, r.Status, r.Err)
	}
}

// after a crash, or a cancelled context: finish what was left
results, err = s.ResumeUploads(ctx, nil)

// a stream of unknown size (a pipe, a socket)
obj, err = s.UploadStream(ctx, "b", "backup.tar", os.Stdin, nil)
```

`PutObject` calls made at the same time share packs: each returns once its own pack is committed. How many packs and
parts are in flight adapts to the network, up to `Config.UploadConcurrency`. A large file and a run of `UploadFiles`
are journaled in the state directory (`Config.StateDir`), so `ResumeUploads` finishes them after a crash, a lost
network, or a cancelled context. `ListUploadRuns` shows the runs kept there, and `CleanUploads` removes old ones. A
stream isn't journaled: if it fails, upload it again. `ListUploads` and `AbortUpload` show and cancel a bucket's open
uploads in parts.

On a slow link the client sizes its requests to the upload speed it measured from earlier ones, so each takes about 4
minutes at most: packs shrink, an object a pack request couldn't carry in time goes in parts (from 5 MiB up), and new
uploads' parts shrink, including a `PartSize` you set. An upload resumed after a crash keeps its part size. An object
the measured speed can't carry (even a 5 MiB part would take over 4 minutes, or it would need more than 3,000 parts)
is refused before anything is sent: `IsTooLarge(err)` and `errors.Is(err, teepin.ErrConnectionSpeed)`.

### Download

```go
_, err = s.DownloadFile(ctx, "b", "big.iso", "/tmp/big.iso", nil) // parallel windows, temp file + rename
_, err = s.Download(ctx, "b", "big.iso", f, nil)                   // into any io.WriterAt, such as an *os.File
_, err = s.DownloadStream(ctx, "b", "app.log", w, nil)             // in order, to any io.Writer

r, err := s.GetObject(ctx, "b", "app.log", &teepin.GetObjectOptions{Range: &teepin.ByteRange{Start: 0, End: 1023}})
if err != nil {
	return err
}
defer r.Close()
head, err := io.ReadAll(r)

f, err := s.OpenObject(ctx, "b", "video.mp4", &teepin.OpenObjectOptions{Prefetch: 2}) // io.ReaderAt and io.ReadSeeker
defer f.Close()
```

A window that breaks off goes on from its last verified bytes. If the object is replaced mid-download, the error is
`teepin.ErrObjectChanged`. The whole object is checked against its SHA-256 (`teepin.ErrChecksumMismatch`).

### Many files to a folder

```go
results, err := s.DownloadFiles(ctx, "b", []string{"a/1.jpg", "a/2.jpg"}, "out", nil) // out/a/1.jpg, out/a/2.jpg
results, err = s.DownloadPrefix(ctx, "b", "photos/2026/", "out", &teepin.DownloadFilesOptions{SkipExisting: true})
for _, r := range results {
	if !r.Saved() && !r.Skipped() {
		log.Printf("%s: %s: %v", r.Key, r.Status, r.Err) // failed, or pending (kept to go on with)
	}
}
results, err = s.ResumeDownloads(ctx, nil) // what a crash, a cancel or a lost network left
```

Read sessions are signed 1,000 objects to a request, and objects (or windows of a large one) are fetched
`Concurrency` at a time (default 8). Each is written to `<name>.teepin-part` beside its target, verified, and renamed
into place only when whole, so a target is never half written. One object's failure never fails another: each has a
`DownloadResult` (saved, skipped, failed or pending). A download stopped by a passing error or a cancelled context is
journaled in the state directory: running the same download again, or `ResumeDownloads`, goes on from the pieces
already written. `ListDownloadRuns` and `CleanDownloads` show and remove the runs kept. `SkipExisting` leaves a file
that already has the object's size and SHA-256 (never compared by ETag), signing and billing nothing for it.
`Paths` names another path under the folder for a key.

A key that isn't a safe path under the folder fails with a `*teepin.UnsafePathError` and writes nothing: `..`, an
absolute path, on Windows a name Windows can't hold, two keys for one file, or a link of any kind under the folder.

### Share links

```go
link, err := s.CreateLink(ctx, "b", "report.pdf", &teepin.CreateLinkOptions{ExpiresIn: time.Hour,
	Disposition: teepin.LinkAttachment})
fmt.Println(link.URL, link.ExpiresAt)
```

A share link opens a console page that anyone with it can use without signing in; the page decrypts the object in
their browser, so its bytes never pass through Teepin's servers. It works for 15 minutes by default, at most 24 hours.
There are no upload links.

### Listing

```go
// Every object under a prefix, page by page.
for o, err := range s.Objects(ctx, "b", "photos/") {
	if err != nil {
		return err
	}
	fmt.Println(o.Key, o.Size)
}

// One page, with "folders".
page, err := s.ListObjects(ctx, "b", &teepin.ListObjectsOptions{Prefix: "photos/", Delimiter: "/"})
```

### ETags

The ETag of an object is the SHA-256 of its bytes, however it was uploaded. ETags are unquoted. To tell whether a
local file matches an object:

```go
fh, _ := os.Open("big.iso")
st, _ := fh.Stat()
etag, _ := teepin.ComputeETag(fh, st.Size(), 0, 0)
obj, _ := s.StatObject(ctx, "b", "big.iso")
same := etag == obj.ETag
```

An object uploaded in parts by an earlier release of this SDK has `<SHA-256 of the parts' SHA-256s>-<parts>` as its
ETag. For one of those, pass the part size it was uploaded with: `teepin.ComputeETag(fh, size, partSize,
obj.PartCount)` (`teepin.PartSize(size, 0)` gives those releases' default).

### Rename, copy and delete

```go
_, err = s.RenameObject(ctx, "b", "draft.txt", "final.txt", false) // instant at any size
_, err = s.CopyObject(ctx, "backup", "a.mp4", "b", "archive/a.mp4", nil) // to backup/a.mp4
err = s.DeleteObject(ctx, "b", "app.log")
err = s.DeleteObjectIf(ctx, "b", "app.log", obj.ETag) // only that version: IsPreconditionFailed if it was replaced
res, err := s.DeleteObjects(ctx, "b", []string{"a.txt", "b.txt"}) // up to 1000 a request
```

`DeleteObjectIf` deletes only the version whose ETag you checked (`If-Match`), so an upload that lands between your
check and the delete is kept: the delete answers 412 (`teepin.IsPreconditionFailed(err)`). A missing object is a 404
either way.

A copy is made on this machine: the source is read and verified, then uploaded as a new object (an encrypted object's
key belongs to that object alone). So its bytes pass through the machine running it, and it is billed as a read as
well as the stored copy. Every read is pinned to the version first seen, so a source replaced mid-copy fails it, with
nothing left at the target.

### Storage retries

A request that fails for a passing reason is sent again after a wait with full jitter, or the server's
`Retry-After`. It is always retried on `429`, `502` and `503`. It is retried on `500`, `504` or a broken connection
only when sending it twice is harmless: reads, deletes, and each step of an upload. A rename can't be repeated, so it
is retried only when it never reached the server.

While the storage network or the API is unavailable, an upload waits for it, up to `Config.UploadWait` (10 minutes),
then fails with a `*teepin.StorageUnavailableError`. Nothing is reported saved that isn't.

When an answer is lost after the server acted, the SDK recovers rather than failing:

- A bucket that turns out to exist on a retry is fetched.
- A delete or cancel whose object is gone on a retry succeeds.
- A rename whose source is gone on a retry returns the destination.
- An upload whose commit answer was lost commits again, and gets the stored result.

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

Storage has a few errors of its own, found with `errors.As` or `errors.Is`:

| Error | Meaning |
|---|---|
| `*teepin.StorageUnavailableError` | storage didn't come back within `Config.UploadWait`; a run's files stay pending for `ResumeUploads` |
| `*teepin.FileChangedError` | a file changed while it was uploaded, so it was never stored mixed with its old bytes |
| `*teepin.DataIntegrityError` | data failed a check after one retry; `Reason` says which (`teepin.ErrChecksumMismatch` matches a checksum) |
| `teepin.ErrObjectChanged` | the object was replaced during a download or a copy |
| `teepin.ErrKeyErased` | the object's encryption key was erased: it can never be read again (`IsNotFound` too) |
| `teepin.ErrConnectionSpeed` | an upload refused before anything was sent: too large for the connection at its measured speed (`IsTooLarge` too, code `connection_speed`) |
| `*teepin.UnsafePathError` | a batch download's key that can't be written under its folder (`teepin.ErrUnsafePath`) |
| `teepin.ErrObjectStaged` | the object is still staged on the API; it is readable once the stage has drained |

## Configuration

```go
client := teepin.NewClient(teepin.Config{
	APIKey:     "tpk_...",                // default: TEEPIN_API_KEY
	BaseURL:    "https://api.teepin.com", // default: TEEPIN_BASE_URL, else this
	Timeout:    60 * time.Second,         // per call that isn't a transfer or a chat; transfers follow ctx
	MaxRetries: 4,                        // tries again after a passing failure

	// Storage
	UploadWait:        10 * time.Minute, // waiting for storage to come back: TEEPIN_UPLOAD_WAIT (seconds)
	UploadConcurrency: 32,               // the most packs and parts in flight: TEEPIN_UPLOAD_CONCURRENCY
	KeyCacheTTL:       10 * time.Minute, // how long reads keep an object's data key in memory
	StateDir:          "",               // upload runs' journals: TEEPIN_STATE_DIR, else this system's
})
```

The values shown are the defaults. For each of these numbers and durations, zero means the default and a negative
value means none: `Timeout` no bound (the context's only), `MaxRetries` no retries, `UploadWait` no wait (an upload
fails at once), `KeyCacheTTL` no key kept after its read. `UploadConcurrency` is the exception: zero or a negative
value means the default. The options follow the same rule: `OpenObjectOptions.Prefetch` is 2 by default and a negative
value fetches nothing ahead; a `Concurrency`, `PartSize` or `ListObjectsOptions.Limit` of zero or less means its
default; and a `CreateLinkOptions.ExpiresIn` of zero means 15 minutes, while a negative one is refused.

The state directory is `%LOCALAPPDATA%\teepin` on Windows, `~/Library/Application Support/teepin` on macOS and
`$XDG_STATE_HOME/teepin` (or `~/.local/state/teepin`) elsewhere: the same place the Python and Node SDKs use. Each
SDK resumes only its own runs. `client.Close()` zeroes the data keys reads keep in memory.

## Other APIs

The SDK also has clients for services that aren't offered yet. They answer 404 until they launch.

## Examples

- [inference](examples/inference/main.go): list the models and stream an answer.
- [storage](examples/storage/main.go): a bucket, an upload with progress, a listing, a verified download.

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
