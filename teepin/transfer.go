// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	// DefaultPartSize is the part size of uploads in parts and of parallel
	// download ranges. Larger files get larger parts, to stay within
	// MaxParts.
	DefaultPartSize = 16 << 20
	// MultipartThreshold: Upload sends larger files in parts.
	MultipartThreshold = 16 << 20
	// DefaultConcurrency is how many parts or ranges move at once.
	DefaultConcurrency = 8
)

// ProgressFunc is told the bytes moved so far and the total (-1 when the
// total is not known yet). Calls come one at a time, possibly from several
// goroutines; keep it quick.
type ProgressFunc func(done, total int64)

// UploadOptions are the options of Upload and its forms.
type UploadOptions struct {
	ContentType string
	Metadata    map[string]string
	// PartSize is the size of each part (default 16 MiB, at least 5 MiB).
	PartSize int64
	// Concurrency is how many parts are sent at once (default 8).
	Concurrency int
	Progress    ProgressFunc
	// Resume continues an earlier, unfinished upload of the same key: parts
	// the server already holds are skipped when their MD5 matches the file.
	// A failed upload is then left open (for 7 days) to be resumed, instead
	// of cancelled.
	Resume bool
}

// UploadError is an upload in parts that failed. With Resume, uploading the
// same file to the same key again continues it.
type UploadError struct {
	UploadID string
	Err      error
}

func (e *UploadError) Error() string {
	return fmt.Sprintf("teepin: upload %s failed: %v", e.UploadID, e.Err)
}
func (e *UploadError) Unwrap() error { return e.Err }

// PartSize is the part size an upload of size bytes uses when want (0 for
// the default) is asked for: at least MinPartSize, doubled until the parts
// fit in MaxParts.
func PartSize(size, want int64) int64 {
	p := want
	if p <= 0 {
		p = DefaultPartSize
	}
	p = max(p, MinPartSize)
	for p < MaxPartSize && (size+p-1)/p > MaxParts {
		p *= 2
	}
	return min(p, MaxPartSize)
}

// ComputeETag is the ETag Teepin gives size bytes of r: the SHA-256 when
// parts is 0, or for an upload in parts of partSize bytes the SHA-256 of
// the parts' SHA-256s with "-<parts>". Compare it with Object.ETag to tell
// whether a local file and an object hold the same bytes.
func ComputeETag(r io.ReaderAt, size, partSize int64, parts int) (string, error) {
	if parts == 0 {
		h := sha256.New()
		if _, err := io.Copy(h, io.NewSectionReader(r, 0, size)); err != nil {
			return "", err
		}
		return hex.EncodeToString(h.Sum(nil)), nil
	}
	if partSize <= 0 || int((size+partSize-1)/partSize) != parts {
		return "", fmt.Errorf("teepin: %d bytes in parts of %d is not %d parts", size, partSize, parts)
	}
	composite := sha256.New()
	for i := 0; i < parts; i++ {
		off := int64(i) * partSize
		h := sha256.New()
		if _, err := io.Copy(h, io.NewSectionReader(r, off, min(partSize, size-off))); err != nil {
			return "", err
		}
		composite.Write(h.Sum(nil))
	}
	return fmt.Sprintf("%s-%d", hex.EncodeToString(composite.Sum(nil)), parts), nil
}

func (o *UploadOptions) normalized() UploadOptions {
	var v UploadOptions
	if o != nil {
		v = *o
	}
	if v.Concurrency <= 0 {
		v.Concurrency = DefaultConcurrency
	}
	return v
}

// Upload stores size bytes of r at bucket/key: in one request up to 16 MiB,
// in parts above that, sent Concurrency at a time. Each part is retried on
// its own after a passing failure, and the object's ETag is checked against
// the bytes read, so a corrupted transfer is never reported as success.
func (s *StorageService) Upload(ctx context.Context, bucket, key string, r io.ReaderAt, size int64, opts *UploadOptions) (*Object, error) {
	o := opts.normalized()
	if size < 0 {
		return nil, errors.New("teepin: size must not be negative")
	}
	if size > MaxObjectSize {
		return nil, fmt.Errorf("teepin: %d bytes is over the 5 TiB an object may hold", size)
	}
	if size <= MultipartThreshold {
		return s.uploadOne(ctx, bucket, key, io.NewSectionReader(r, 0, size), size, o)
	}
	return s.uploadParts(ctx, bucket, key, r, size, o)
}

// UploadFile uploads the file at path. ContentType defaults to one guessed
// from the file's extension.
func (s *StorageService) UploadFile(ctx context.Context, bucket, key, path string, opts *UploadOptions) (*Object, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.IsDir() {
		return nil, fmt.Errorf("teepin: %s is a directory", path)
	}
	o := opts.normalized()
	if o.ContentType == "" {
		o.ContentType = mime.TypeByExtension(filepath.Ext(path))
	}
	return s.Upload(ctx, bucket, key, f, st.Size(), &o)
}

// uploadOne sends a small object in one request, after reading it once to
// know its SHA-256.
func (s *StorageService) uploadOne(ctx context.Context, bucket, key string, r io.Reader, size int64, o UploadOptions) (*Object, error) {
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("teepin: read %s: %w", key, err)
	}
	sum := sha256.Sum256(data)
	prog := newProgress(size, o.Progress)
	body := &countingReadSeeker{ReadSeeker: bytes.NewReader(data), p: prog}
	obj, err := s.PutObject(ctx, bucket, key, body, size, &PutObjectOptions{ContentType: o.ContentType, Metadata: o.Metadata})
	if err != nil {
		return nil, err
	}
	if want := hex.EncodeToString(sum[:]); obj.ETag != "" && obj.ETag != want {
		return nil, fmt.Errorf("%w: stored %s, sent %s", ErrChecksumMismatch, obj.ETag, want)
	}
	return obj, nil
}

// partSums are what the client knows of a part it sent or checked.
type partSums struct {
	md5    string
	sha256 []byte
}

func (s *StorageService) uploadParts(ctx context.Context, bucket, key string, r io.ReaderAt, size int64, o UploadOptions) (*Object, error) {
	partSize := PartSize(size, o.PartSize)
	n := int((size + partSize - 1) / partSize)
	span := func(num int) (int64, int64) {
		off := int64(num-1) * partSize
		return off, min(partSize, size-off)
	}
	prog := newProgress(size, o.Progress)
	sums := make([]partSums, n+1)

	var up *Upload
	if o.Resume {
		var err error
		if up, err = s.resumable(ctx, bucket, key, o, r, n, span, sums, prog); err != nil {
			return nil, err
		}
	}
	if up == nil {
		var err error
		if up, err = s.CreateUpload(ctx, bucket, key, &CreateUploadOptions{ContentType: o.ContentType, Metadata: o.Metadata}); err != nil {
			return nil, err
		}
	}

	var todo []int
	for num := 1; num <= n; num++ {
		if sums[num].md5 == "" {
			todo = append(todo, num)
		}
	}
	err := runParallel(ctx, o.Concurrency, todo, func(ctx context.Context, num int) error {
		off, sz := span(num)
		var md5h, shah hash.Hash
		var counted *countingReader
		part, err := s.uploadPart(ctx, bucket, up.ID, num, r, sz, func() io.Reader {
			if counted != nil {
				counted.stop() // the attempt before failed
			}
			md5h, shah = md5.New(), sha256.New()
			counted = &countingReader{r: io.TeeReader(io.NewSectionReader(r, off, sz), io.MultiWriter(md5h, shah)), p: prog}
			return counted
		})
		if err != nil {
			return fmt.Errorf("part %d: %w", num, err)
		}
		if got := hex.EncodeToString(md5h.Sum(nil)); part.ETag != got {
			return fmt.Errorf("part %d: %w: the server received MD5 %s, sent %s", num, ErrChecksumMismatch, part.ETag, got)
		}
		sums[num] = partSums{md5: part.ETag, sha256: shah.Sum(nil)}
		return nil
	})
	if err != nil {
		return nil, s.failUpload(bucket, up.ID, o.Resume, err)
	}

	completed := make([]CompletedPart, n)
	composite := sha256.New()
	for num := 1; num <= n; num++ {
		completed[num-1] = CompletedPart{Number: num, ETag: sums[num].md5}
		composite.Write(sums[num].sha256)
	}
	want := fmt.Sprintf("%s-%d", hex.EncodeToString(composite.Sum(nil)), n)

	obj, err := s.CompleteUpload(ctx, bucket, up.ID, completed)
	if err != nil && (IsRetryable(err) || ErrorCode(err) == "upload_not_found") {
		// The answer may have been lost after the object was made.
		if got, serr := s.StatObject(ctx, bucket, key); serr == nil && got.ETag == want {
			return got, nil
		}
	}
	if err != nil {
		return nil, s.failUpload(bucket, up.ID, o.Resume, err)
	}
	if obj.ETag != want {
		return nil, fmt.Errorf("%w: stored %s, sent %s", ErrChecksumMismatch, obj.ETag, want)
	}
	return obj, nil
}

// failUpload cancels a failed upload unless it is to be resumed.
func (s *StorageService) failUpload(bucket, id string, keep bool, err error) error {
	if !keep {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = s.AbortUpload(ctx, bucket, id)
	}
	return &UploadError{UploadID: id, Err: err}
}

// resumable finds the newest open upload of key that this file can continue,
// fills sums for the parts it already holds with matching bytes, and counts
// them as moved. It returns nil when there is none.
func (s *StorageService) resumable(ctx context.Context, bucket, key string, o UploadOptions, r io.ReaderAt, n int,
	span func(int) (int64, int64), sums []partSums, prog *progress) (*Upload, error) {
	ups, err := s.ListUploads(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	for i := len(ups) - 1; i >= 0; i-- {
		if ups[i].Key != key || (o.ContentType != "" && ups[i].ContentType != o.ContentType) {
			continue
		}
		up, err := s.ListParts(ctx, bucket, ups[i].ID)
		if IsNotFound(err) {
			continue // completed, cancelled or expired meanwhile
		}
		if err != nil {
			return nil, err
		}
		for _, p := range up.Parts {
			if p.Number < 1 || p.Number > n {
				continue
			}
			off, sz := span(p.Number)
			if p.Size != sz {
				continue // another part size; it is sent again
			}
			md5h, shah := md5.New(), sha256.New()
			if _, err := io.Copy(io.MultiWriter(md5h, shah), io.NewSectionReader(r, off, sz)); err != nil {
				return nil, err
			}
			if hex.EncodeToString(md5h.Sum(nil)) == p.ETag {
				sums[p.Number] = partSums{md5: p.ETag, sha256: shah.Sum(nil)}
				prog.add(sz)
			}
		}
		return up, nil
	}
	return nil, nil
}

// UploadStream uploads everything r yields, when its size is not known
// ahead: what fits in one part is sent in one request, the rest in parts of
// PartSize (so at most PartSize*10000 bytes; raise PartSize for more).
// Up to Concurrency parts are held in memory at once.
func (s *StorageService) UploadStream(ctx context.Context, bucket, key string, r io.Reader, opts *UploadOptions) (*Object, error) {
	o := opts.normalized()
	partSize := max(o.PartSize, 0)
	if partSize == 0 {
		partSize = DefaultPartSize
	}
	partSize = max(partSize, MinPartSize)

	first := make([]byte, partSize+1)
	n, err := io.ReadFull(r, first)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if int64(n) <= partSize {
		return s.uploadOne(ctx, bucket, key, bytes.NewReader(first[:n]), int64(n), o)
	}

	prog := newProgress(-1, o.Progress)
	up, err := s.CreateUpload(ctx, bucket, key, &CreateUploadOptions{ContentType: o.ContentType, Metadata: o.Metadata})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	type chunk struct {
		num  int
		data []byte
	}
	pool := make(chan []byte, o.Concurrency)
	for i := 0; i < o.Concurrency; i++ {
		pool <- make([]byte, partSize)
	}
	jobs := make(chan chunk)
	var mu sync.Mutex
	sums := map[int]partSums{}

	var wg sync.WaitGroup
	for i := 0; i < o.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range jobs {
				err := func() error {
					defer func() { pool <- c.data[:cap(c.data)] }()
					var md5h, shah hash.Hash
					var counted *countingReader
					part, err := s.uploadPart(ctx, bucket, up.ID, c.num, bytes.NewReader(c.data), int64(len(c.data)), func() io.Reader {
						if counted != nil {
							counted.stop()
						}
						md5h, shah = md5.New(), sha256.New()
						counted = &countingReader{r: io.TeeReader(bytes.NewReader(c.data), io.MultiWriter(md5h, shah)), p: prog}
						return counted
					})
					if err != nil {
						return fmt.Errorf("part %d: %w", c.num, err)
					}
					if got := hex.EncodeToString(md5h.Sum(nil)); part.ETag != got {
						return fmt.Errorf("part %d: %w", c.num, ErrChecksumMismatch)
					}
					mu.Lock()
					sums[c.num] = partSums{md5: part.ETag, sha256: shah.Sum(nil)}
					mu.Unlock()
					return nil
				}()
				if err != nil {
					cancel(err)
				}
			}
		}()
	}

	// What the first read took comes first, then the rest of the stream.
	src := io.MultiReader(bytes.NewReader(first[:n]), r)
	num := 0
read:
	for {
		var buf []byte
		select {
		case buf = <-pool:
		case <-ctx.Done():
			break read
		}
		m, err := io.ReadFull(src, buf[:partSize])
		if m > 0 {
			num++
			if num > MaxParts {
				cancel(fmt.Errorf("teepin: the stream is over %d parts of %d bytes; raise PartSize", MaxParts, partSize))
				break
			}
			select {
			case jobs <- chunk{num: num, data: buf[:m]}:
			case <-ctx.Done():
				break read
			}
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			cancel(fmt.Errorf("teepin: read: %w", err))
			break
		}
	}
	close(jobs)
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return nil, s.failUpload(bucket, up.ID, false, err)
	}

	completed := make([]CompletedPart, 0, len(sums))
	nums := make([]int, 0, len(sums))
	for k := range sums {
		nums = append(nums, k)
	}
	sort.Ints(nums)
	composite := sha256.New()
	for _, k := range nums {
		completed = append(completed, CompletedPart{Number: k, ETag: sums[k].md5})
		composite.Write(sums[k].sha256)
	}
	want := fmt.Sprintf("%s-%d", hex.EncodeToString(composite.Sum(nil)), len(nums))
	obj, err := s.CompleteUpload(ctx, bucket, up.ID, completed)
	if err != nil {
		return nil, s.failUpload(bucket, up.ID, false, err)
	}
	if obj.ETag != want {
		return nil, fmt.Errorf("%w: stored %s, sent %s", ErrChecksumMismatch, obj.ETag, want)
	}
	return obj, nil
}

// DownloadOptions are the options of Download and its forms.
type DownloadOptions struct {
	// PartSize is the size of each range fetched at once (default 16 MiB).
	PartSize int64
	// Concurrency is how many ranges are fetched at once (default 8).
	Concurrency int
	Progress    ProgressFunc
	// SkipVerify skips checking the bytes against the object's SHA-256
	// (which needs w to be readable when ranges are fetched in parallel).
	SkipVerify bool
}

func (o *DownloadOptions) normalized() DownloadOptions {
	var v DownloadOptions
	if o != nil {
		v = *o
	}
	if v.Concurrency <= 0 {
		v.Concurrency = DefaultConcurrency
	}
	if v.PartSize <= 0 {
		v.PartSize = DefaultPartSize
	}
	return v
}

// Download writes the object at bucket/key to w: large objects as ranges
// fetched in parallel, every range pinned to the same version of the object
// (ErrObjectChanged if it is replaced meanwhile). A range that breaks off is
// continued from where it stopped. Objects stored in one request are then
// checked against their SHA-256 when w can be read back (an *os.File can).
func (s *StorageService) Download(ctx context.Context, bucket, key string, w io.WriterAt, opts *DownloadOptions) (*Object, error) {
	o := opts.normalized()
	obj, err := s.StatObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	prog := newProgress(obj.Size, o.Progress)
	var ranges []int
	for i := int64(0); i*o.PartSize < obj.Size; i++ {
		ranges = append(ranges, int(i))
	}
	if o.Concurrency == 1 || len(ranges) <= 1 {
		h := sha256.New()
		if err := s.fetchRange(ctx, bucket, key, obj, 0, obj.Size, io.MultiWriter(io.NewOffsetWriter(w, 0), h), prog); err != nil {
			return nil, err
		}
		if !o.SkipVerify && obj.PartCount == 0 && obj.ChecksumSHA256 != "" && hex.EncodeToString(h.Sum(nil)) != obj.ChecksumSHA256 {
			return nil, ErrChecksumMismatch
		}
		return obj, nil
	}
	err = runParallel(ctx, o.Concurrency, ranges, func(ctx context.Context, i int) error {
		start := int64(i) * o.PartSize
		return s.fetchRange(ctx, bucket, key, obj, start, min(o.PartSize, obj.Size-start), io.NewOffsetWriter(w, start), prog)
	})
	if err != nil {
		return nil, err
	}
	if ra, ok := w.(io.ReaderAt); ok && !o.SkipVerify && obj.PartCount == 0 && obj.ChecksumSHA256 != "" {
		got, err := ComputeETag(ra, obj.Size, 0, 0)
		if err != nil {
			return nil, err
		}
		if got != obj.ChecksumSHA256 {
			return nil, ErrChecksumMismatch
		}
	}
	return obj, nil
}

// fetchRange copies length bytes of obj from start into w, continuing from
// where it broke off after a passing failure.
func (s *StorageService) fetchRange(ctx context.Context, bucket, key string, obj *Object, start, length int64, w io.Writer, prog *progress) error {
	if length == 0 {
		return nil
	}
	done := int64(0)
	for attempt := 0; ; attempt++ {
		rng := &ByteRange{Start: start + done, End: start + length - 1}
		body, err := s.GetObject(ctx, bucket, key, &GetObjectOptions{Range: rng, IfMatch: obj.ETag})
		if err == nil {
			var n int64
			n, err = io.Copy(w, &countingReader{r: body, p: prog})
			body.Close()
			done += n
			if err == nil && done < length {
				err = io.ErrUnexpectedEOF
			}
			if err == nil {
				return nil
			}
			err = &transportError{err: err}
		}
		if errors.Is(err, ErrObjectChanged) || !IsRetryable(err) || attempt >= s.client.maxRetries || ctx.Err() != nil {
			return err
		}
		if serr := s.client.sleep(ctx, backoff(attempt, err)); serr != nil {
			return err
		}
	}
}

// DownloadFile downloads the object to path, creating its directory. It
// writes to a temporary file beside path and renames it into place, so path
// is never left half written.
func (s *StorageService) DownloadFile(ctx context.Context, bucket, key, path string, opts *DownloadOptions) (*Object, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".teepin-*")
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()
	obj, err := s.Download(ctx, bucket, key, tmp, opts)
	if err != nil {
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return nil, err
	}
	keep = true
	if obj.UploadedAt != nil {
		_ = os.Chtimes(path, *obj.UploadedAt, *obj.UploadedAt)
	}
	return obj, nil
}

// DownloadStream writes the object to w in order, continuing from where it
// broke off after a passing failure, and checks objects stored in one
// request against their SHA-256 at the end. Bytes reach w before that
// check, so on ErrChecksumMismatch discard what was written.
func (s *StorageService) DownloadStream(ctx context.Context, bucket, key string, w io.Writer, opts *DownloadOptions) (*Object, error) {
	o := opts.normalized()
	obj, err := s.StatObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	if err := s.fetchRange(ctx, bucket, key, obj, 0, obj.Size, io.MultiWriter(w, h), newProgress(obj.Size, o.Progress)); err != nil {
		return nil, err
	}
	if !o.SkipVerify && obj.PartCount == 0 && obj.ChecksumSHA256 != "" && hex.EncodeToString(h.Sum(nil)) != obj.ChecksumSHA256 {
		return nil, ErrChecksumMismatch
	}
	return obj, nil
}

// runParallel runs fn for each item, n at a time, and stops at the first
// error, which it returns.
func runParallel(ctx context.Context, n int, items []int, fn func(context.Context, int) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	work := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < min(n, len(items)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range work {
				if err := fn(ctx, item); err != nil {
					cancel(err)
				}
			}
		}()
	}
feed:
	for _, item := range items {
		select {
		case work <- item:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return nil
}

// progress adds up bytes moved and tells the caller.
type progress struct {
	mu    sync.Mutex
	done  int64
	total int64
	fn    ProgressFunc
}

func newProgress(total int64, fn ProgressFunc) *progress {
	return &progress{total: total, fn: fn}
}

func (p *progress) add(n int64) {
	if p == nil || p.fn == nil || n == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done += n
	p.fn(p.done, p.total)
}

// countingReader counts the bytes read through it into p. An attempt that
// failed is retired with stop: the HTTP client may still be reading its body
// in another goroutine, and those bytes must not count.
type countingReader struct {
	r    io.Reader
	p    *progress
	mu   sync.Mutex
	n    int64
	dead bool
}

func (c *countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.mu.Lock()
	if !c.dead {
		c.n += int64(n)
		c.p.add(int64(n))
	}
	c.mu.Unlock()
	return n, err
}

// stop ends the count and takes back what this reader counted.
func (c *countingReader) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.dead {
		c.dead = true
		c.p.add(-c.n)
	}
}

// countingReadSeeker counts like countingReader and undoes the count when a
// retry rewinds it.
type countingReadSeeker struct {
	io.ReadSeeker
	p *progress
	n int64
}

func (c *countingReadSeeker) Read(b []byte) (int, error) {
	n, err := c.ReadSeeker.Read(b)
	c.n += int64(n)
	c.p.add(int64(n))
	return n, err
}

func (c *countingReadSeeker) Seek(off int64, whence int) (int64, error) {
	pos, err := c.ReadSeeker.Seek(off, whence)
	if err == nil {
		c.p.add(pos - c.n)
		c.n = pos
	}
	return pos, err
}
