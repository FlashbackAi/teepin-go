// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"context"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
)

// The part rules of earlier releases, for PartSize and ComputeETag to match the
// ETags of objects they uploaded in parts. Uploads now follow the direct
// path's rules: at most 3,000 parts of whole 64 KiB segments.
const (
	// MinPartSize is the smallest part but the last.
	MinPartSize = 5 << 20
	// MaxPartSize is the largest part.
	MaxPartSize = 5 << 30
	// MaxParts is the most parts of one upload in earlier releases.
	MaxParts = 10000
	// MaxObjectSize is the largest object.
	MaxObjectSize = 5 << 40
)

// Upload is an upload in parts that has not been completed: a large upload
// under way, or one a crash left (ResumeUploads finishes those this machine
// journaled).
type Upload struct {
	ID          string            `json:"upload_id"`
	Key         string            `json:"key"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	// ExpiresAt is when it is cancelled if not completed (7 days).
	ExpiresAt time.Time `json:"expires_at"`
}

func uploadPath(bucket, id string) string {
	return bucketPath(bucket) + "/uploads/" + url.PathEscape(id)
}

// ListUploads lists a bucket's open uploads under prefix, oldest first.
func (s *StorageService) ListUploads(ctx context.Context, bucket, prefix string) ([]Upload, error) {
	q := url.Values{}
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	var resp struct {
		Uploads []Upload `json:"uploads"`
	}
	if err := s.client.call(ctx, &request{method: http.MethodGet, path: bucketPath(bucket) + "/uploads", query: q}, &resp); err != nil {
		return nil, err
	}
	return resp.Uploads, nil
}

// AbortUpload cancels an open upload and drops its parts.
func (s *StorageService) AbortUpload(ctx context.Context, bucket, uploadID string) error {
	r := &request{method: http.MethodDelete, path: uploadPath(bucket, uploadID)}
	err := s.client.call(ctx, r, nil)
	if err != nil && r.attempt > 0 && ErrorCode(err) == "upload_not_found" {
		return nil
	}
	return err
}

// --- upload runs ---------------------------------------------------------------------------------------------------

// RunExpiry is how long an unfinished upload run stays untouched before
// ListUploadRuns shows it as expired (its files not uploaded): CleanUploads'
// usual age.
const RunExpiry = direct.RunExpiry

// The statuses of a file in an upload run (UploadResult.Status).
const (
	// UploadSaved: the file is stored.
	UploadSaved = direct.StatusSaved
	// UploadFailed: the file failed for good (it changed, a precondition
	// failed, ...); Err says why.
	UploadFailed = direct.StatusFailed
	// UploadPending: not uploaded yet because of a passing error (storage
	// unavailable past Config.UploadWait, a revoked key), kept in the run's
	// journal for ResumeUploads; Err says why.
	UploadPending = direct.StatusPending
)

// FileToUpload is one file of an upload run: where it is, and the key it is
// stored at.
type FileToUpload struct {
	Key  string
	Path string
}

// FilesToUpload names paths for UploadFiles as the Python SDK does: each key
// is prefix plus the path relative to root (with "/"), or plus the file's
// name when root is "".
func FilesToUpload(paths []string, prefix, root string) ([]FileToUpload, error) {
	out := make([]FileToUpload, 0, len(paths))
	for _, p := range paths {
		rel := filepath.Base(p)
		if root != "" {
			r, err := filepath.Rel(root, p)
			if err != nil {
				return nil, err
			}
			rel = r
		}
		out = append(out, FileToUpload{Key: prefix + strings.ReplaceAll(rel, string(filepath.Separator), "/"), Path: p})
	}
	return out, nil
}

// UploadFilesOptions are the options of UploadFiles.
type UploadFilesOptions struct {
	// ContentType is every file's (default: guessed from each file's
	// extension).
	ContentType string
	Metadata    map[string]string
	// IfNoneMatch writes each object only if none is at its key.
	IfNoneMatch bool
	// PartSize is the part size of files sent in parts. Zero or a negative
	// value means the default, as UploadOptions.PartSize.
	PartSize int64
	// Progress is told the bytes saved so far and the total, as files are
	// saved.
	Progress ProgressFunc
	// NoJournal keeps the run in memory: nothing on disk, and nothing to
	// resume after a crash.
	NoJournal bool
}

// UploadResult is one file's result from an upload run.
type UploadResult struct {
	// Bucket is the run's bucket (ResumeUploads finishes runs of any bucket).
	Bucket string
	Key    string
	Path   string
	// Status is UploadSaved, UploadFailed or UploadPending.
	Status string
	// Object is the stored object, when it is saved.
	Object *Object
	// Err is why it failed or is pending.
	Err error
}

// Saved reports a stored file.
func (r UploadResult) Saved() bool { return r.Status == UploadSaved }

// Pending reports a file not uploaded yet, kept for ResumeUploads.
func (r UploadResult) Pending() bool { return r.Status == UploadPending }

// UploadRun is an upload run kept in the state directory (Config.StateDir).
type UploadRun struct {
	ID      string
	Bucket  string
	Created time.Time
	Updated time.Time
	Files   int
	Saved   int
	Failed  int
	// Pending are the files not uploaded yet: ResumeUploads continues them.
	Pending int
	// Expired: untouched for longer than RunExpiry; its files were not
	// uploaded, and only CleanUploads removes it.
	Expired bool
	// InUse: a live process (this one included) is working on it.
	InUse bool
}

func resultsOf(rs []direct.UploadResult) []UploadResult {
	out := make([]UploadResult, 0, len(rs))
	for _, r := range rs {
		out = append(out, UploadResult{Bucket: r.Bucket, Key: r.Key, Path: r.Path, Status: r.Status,
			Object: objectFrom(r.Object), Err: r.Err})
	}
	return out
}

// progressOf is a ProgressFunc as the engine takes it (nil for none).
func progressOf(p ProgressFunc) func(done, total int64) {
	if p == nil {
		return nil
	}
	return p
}

// UploadFiles uploads many files at once, as one run, and returns one result
// per file, in order: saved, failed (with its error), or pending (kept for
// ResumeUploads). One file's failure never fails another, and returns no
// error. Only what stops the whole run does: a revoked key, no permission or
// no credit (with the results so far; its files stay pending), the state
// directory being unwritable, or a cancelled ctx (the run stays, and
// ResumeUploads in this process or another finishes it).
//
// Small files are encrypted on this device and packed together; large ones go
// in parts. Each file is hashed when queued and checked again as it's sent, so
// a file that changes meanwhile fails with a *FileChangedError. The run is
// journaled in the state directory (Config.StateDir) unless NoJournal, so
// ResumeUploads finishes it after a crash.
func (s *StorageService) UploadFiles(ctx context.Context, bucket string, files []FileToUpload, opts *UploadFilesOptions) ([]UploadResult, error) {
	var o UploadFilesOptions
	if opts != nil {
		o = *opts
	}
	run := make([]direct.RunFile, 0, len(files))
	for _, f := range files {
		run = append(run, direct.RunFile{Key: f.Key, Path: f.Path})
	}
	rs, err := s.uploads().UploadFiles(ctx, bucket, run, direct.RunOptions{ContentType: o.ContentType,
		Metadata: o.Metadata, IfNoneMatch: o.IfNoneMatch, NoJournal: o.NoJournal, PartSize: o.PartSize,
		Progress: progressOf(o.Progress)})
	if rs == nil {
		return nil, err
	}
	return resultsOf(rs), err
}

// ResumeUploads finishes every upload run an earlier process (or a cancelled
// call in this one) left unfinished, from its journal in the state directory,
// and returns its files' results. Runs a live process is working on are left
// alone. Each SDK resumes only its own runs: those the Python or Node SDKs
// left are theirs to finish.
func (s *StorageService) ResumeUploads(ctx context.Context, progress ProgressFunc) ([]UploadResult, error) {
	rs, err := s.uploads().ResumeUploads(ctx, progressOf(progress))
	return resultsOf(rs), err
}

// ListUploadRuns lists the upload runs kept in the state directory: how many
// files each has saved, failed and still pending, whether a live process is
// working on it, and whether it has expired (untouched for RunExpiry: its
// files were not uploaded).
func (s *StorageService) ListUploadRuns(ctx context.Context) ([]UploadRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runs, err := s.uploads().ListRuns()
	if err != nil {
		return nil, err
	}
	out := make([]UploadRun, 0, len(runs))
	for _, r := range runs {
		out = append(out, UploadRun(r))
	}
	return out, nil
}

// CleanUploads removes the local state of upload runs untouched for longer
// than olderThan (RunExpiry is the usual age; 0 removes every run no process
// is working on), cancelling their open uploads, with a warning for each
// (their pending files are not uploaded): never done silently. Finished runs
// remove their own state. It also trims the history of finished runs to 7
// days.
func (s *StorageService) CleanUploads(ctx context.Context, olderThan time.Duration) ([]UploadRun, error) {
	runs, err := s.uploads().Clean(ctx, olderThan)
	out := make([]UploadRun, 0, len(runs))
	for _, r := range runs {
		out = append(out, UploadRun(r))
	}
	return out, err
}
