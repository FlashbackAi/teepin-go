// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
	"github.com/FlashbackAi/teepin-go/teepin/internal/journal"
)

const (
	// DefaultPartSize is the part size of uploads in parts and of parallel
	// download ranges. Larger files get larger parts, to stay within the most
	// parts an upload may have.
	DefaultPartSize = 16 << 20
	// MultipartThreshold: Upload and PutObject send larger objects in parts;
	// up to it they are packed.
	MultipartThreshold = 16 << 20
	// DefaultConcurrency is how many ranges a download fetches at once.
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
	// PartSize is the plaintext bytes of each part of an upload in parts:
	// at least 5 MiB, in whole 64 KiB segments, doubled as needed to stay
	// within 3,000 parts. Zero or a negative value means the default (16 MiB;
	// a stream's is 64 MiB). On a
	// slow link a new upload's parts, this one included, are capped at what
	// the measured speed sends in about 4 minutes (see PutObject); an upload
	// resumed after a crash keeps the part size it was created with.
	PartSize int64
	// Concurrency is how many parts of a stream (UploadStream) are sealed and
	// sent at once. Zero or a negative value means the default (4). Other
	// uploads adapt how many packs and parts are in flight, up to
	// Config.UploadConcurrency.
	Concurrency int
	// ExpectedSize is a hint of a stream's size (UploadStream), for choosing
	// its part size: without it parts are 64 MiB, so a stream may be up to
	// about 187 GiB.
	ExpectedSize int64
	// Progress is told the bytes saved so far and the total.
	Progress ProgressFunc
}

// PartSize is the part size earlier releases of this SDK used for an upload
// of size bytes when want (0 for the default) was asked for: at least
// MinPartSize, doubled until the parts fit in MaxParts. Objects they uploaded
// in parts keep their composite ETags ("<sha256>-<parts>"), so this is for
// ComputeETag to match those. Objects uploaded now have their SHA-256 as
// their ETag however they were sent.
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

// ComputeETag is the ETag Teepin gives size bytes of r. Objects uploaded now
// have their SHA-256 as their ETag however they were sent (parts 0). An object
// earlier releases uploaded in parts of partSize bytes (see PartSize) has the
// SHA-256 of its parts' SHA-256s with "-<parts>". Compare it with Object.ETag
// to tell whether a local file and an object hold the same bytes.
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

// Upload stores size bytes of r at bucket/key, encrypted on this device and
// sent straight to the storage network (section 22.4): up to 16 MiB packed
// with the other uploads waiting at the same time, above that in parts sent
// in parallel, and an empty object through the API's metadata-only route.
// Each request is retried on its own after a passing failure, and every
// object is checked against the bytes read (a pack's objects decrypted back
// before it is signed, every part's MD5 against what storage holds), so a
// corrupted transfer is never reported as success. The object's ETag is its
// SHA-256.
//
// An upload of bytes keeps its state in memory: nothing could resume it after
// a crash. UploadFile journals a large file, so ResumeUploads finishes it.
func (s *StorageService) Upload(ctx context.Context, bucket, key string, r io.ReaderAt, size int64, opts *UploadOptions) (*Object, error) {
	var o UploadOptions
	if opts != nil {
		o = *opts
	}
	if size < 0 {
		return nil, errors.New("teepin: size must not be negative")
	}
	return s.put(ctx, bucket, key, io.NewSectionReader(r, 0, size), size,
		direct.WriteOptions{ContentType: o.ContentType, Metadata: o.Metadata}, o.PartSize, o.Progress)
}

// UploadFile uploads the file at path like Upload. ContentType defaults to one
// guessed from the file's extension. A file over 16 MiB is a journaled run of
// one (UploadFiles), so after a crash, a cancel or a lost network
// ResumeUploads finishes it.
func (s *StorageService) UploadFile(ctx context.Context, bucket, key, path string, opts *UploadOptions) (*Object, error) {
	var o UploadOptions
	if opts != nil {
		o = *opts
	}
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if st.IsDir() {
		return nil, fmt.Errorf("teepin: %s is a directory", path)
	}
	if o.ContentType == "" {
		o.ContentType = direct.ContentTypeOf(path)
	}
	if st.Size() > s.uploads().PackObjectLimit() {
		// A large file is a journaled run of one: after a crash, ResumeUploads continues it.
		res, err := s.UploadFiles(ctx, bucket, []FileToUpload{{Key: key, Path: path}}, &UploadFilesOptions{
			ContentType: o.ContentType, Metadata: o.Metadata, PartSize: o.PartSize, Progress: o.Progress})
		if err != nil {
			return nil, err
		}
		if len(res) != 1 {
			return nil, fmt.Errorf("teepin: %s: no result", key)
		}
		if res[0].Err != nil {
			return nil, res[0].Err
		}
		return res[0].Object, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return s.Upload(ctx, bucket, key, f, st.Size(), &o)
}

// UploadStream uploads everything r yields when its size is not known ahead
// (a pipe, a socket, a generator). Progress gets -1 as the total.
//
// Up to 16 MiB is packed like any small object. Above that, the stream goes
// in parts it encrypts as it reads them, read one part ahead (the last part
// is sealed as the last), with up to Concurrency parts sealed and sent at
// once (default 4). Parts are 64 MiB unless ExpectedSize (a hint) or
// PartSize chooses: at most 3,000 parts, so a stream may be up to 3,000 times
// its part size. A stream that reaches part 3,000 and goes on fails
// (IsTooLarge), so give ExpectedSize for a larger one. Nothing journals a
// stream: any failure, or a cancel, cancels the upload, and the stream must
// be uploaded again.
//
// Memory: each part in flight holds its ciphertext, and the reader holds the
// part it hands over, the next, and one more while it seals a part: at most
// about (Concurrency + 3) times the part size, 448 MiB with the defaults.
// Lower Concurrency or PartSize (or ExpectedSize) to hold less.
func (s *StorageService) UploadStream(ctx context.Context, bucket, key string, r io.Reader, opts *UploadOptions) (*Object, error) {
	var o UploadOptions
	if opts != nil {
		o = *opts
	}
	limit := s.uploads().PackObjectLimit()
	first := make([]byte, limit+1)
	n, err := io.ReadFull(r, first)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("teepin: read the stream: %w", err)
	}
	if int64(n) <= limit {
		return s.put(ctx, bucket, key, bytes.NewReader(first[:n]), int64(n),
			direct.WriteOptions{ContentType: o.ContentType, Metadata: o.Metadata}, 0, o.Progress)
	}
	// What the first read took comes first, then the rest of the stream; the head is dropped once it is read.
	src := io.MultiReader(bytes.NewReader(first), r)
	first = nil
	obj, err := s.uploads().PutStream(ctx, bucket, key, src, direct.WriteOptions{ContentType: o.ContentType,
		Metadata: o.Metadata}, direct.StreamParams{PartSize: o.PartSize, ExpectedSize: o.ExpectedSize,
		Concurrency: o.Concurrency, Progress: o.Progress})
	return objectFrom(obj), err
}

// DownloadOptions are the options of Download and its forms.
type DownloadOptions struct {
	// PartSize splits the storage network's read windows (64 MiB) into
	// smaller ranges. Zero or a negative value means the default: one range
	// per window.
	PartSize int64
	// Concurrency is how many ranges are fetched at once. Zero or a negative
	// value means the default (8).
	Concurrency int
	Progress    ProgressFunc
	// SkipVerify skips checking the bytes against the object's SHA-256
	// (which needs w to be readable when ranges are fetched in parallel).
	// Each piece is still verified as it arrives.
	SkipVerify bool
}

// Download writes the object at bucket/key to w, straight from the storage
// network: large objects as the network's read windows fetched in parallel,
// each pinned to the same version of the object (ErrObjectChanged if it is
// replaced meanwhile), every byte verified before it is written. A window
// that breaks off goes on from its last verified bytes, and a failed one
// stops the others at once. The whole object's SHA-256 is then checked when w
// can be read back (an *os.File can), and a mismatch is tried once more
// before it is reported.
func (s *StorageService) Download(ctx context.Context, bucket, key string, w io.WriterAt, opts *DownloadOptions) (*Object, error) {
	var o DownloadOptions
	if opts != nil {
		o = *opts
	}
	n := o.Concurrency
	if n <= 0 {
		n = DefaultConcurrency
	}
	open := func(*direct.Session) (*direct.Target, error) {
		t := &direct.Target{WriteAt: func(p []byte, off int64) error {
			_, err := w.WriteAt(p, off)
			return err
		}}
		if ra, ok := w.(io.ReaderAt); ok {
			t.SHA256 = func(size int64) (string, error) { return ComputeETag(ra, size, 0, 0) }
		}
		return t, nil
	}
	sess, err := s.reads().Download(ctx, bucket, key, open, direct.DownloadParams{PartSize: max(o.PartSize, 0),
		Concurrency: n, Progress: o.Progress, Verify: !o.SkipVerify})
	if err != nil {
		return nil, err
	}
	return objectOf(sess), nil
}

// DownloadFile downloads the object to path, creating its directory. It
// writes to a temporary file beside path and renames it into place only once
// every byte is verified, so path never holds unverified or half-written
// bytes.
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
	// A target another program holds open is tried again for about two seconds (W-1).
	if err := journal.RenameOver(tmp.Name(), path); err != nil {
		return nil, err
	}
	keep = true
	if obj.UploadedAt != nil {
		_ = os.Chtimes(path, *obj.UploadedAt, *obj.UploadedAt)
	}
	return obj, nil
}

// DownloadStream writes the object to w in order, straight from the storage
// network: each piece verified before it reaches w (an encrypted object's
// segments one by one, a packed object whole), and a window that breaks off
// goes on from its last verified bytes. The whole object's SHA-256 is checked
// at the end; bytes reach w before that check, so on ErrChecksumMismatch
// discard what was written.
func (s *StorageService) DownloadStream(ctx context.Context, bucket, key string, w io.Writer, opts *DownloadOptions) (*Object, error) {
	var o DownloadOptions
	if opts != nil {
		o = *opts
	}
	write := func(p []byte) error {
		_, err := w.Write(p)
		return err
	}
	sess, err := s.reads().Stream(ctx, bucket, key, write, o.Progress, !o.SkipVerify)
	if err != nil {
		return nil, err
	}
	return objectOf(sess), nil
}
