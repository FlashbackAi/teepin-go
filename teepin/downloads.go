// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"context"
	"strings"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
)

// PartSuffix is added to a batch download's target path for the file it is
// written into: <name>.teepin-part beside the target, renamed into place only
// when every byte is written and verified.
const PartSuffix = direct.PartSuffix

// The statuses of an object in a batch download (DownloadResult.Status).
const (
	// DownloadSaved: the object is written to its path, verified.
	DownloadSaved = direct.DownloadSaved
	// DownloadSkipped: the file there already has the object's size and
	// SHA-256 (SkipExisting), or the key is a folder marker (ending in "/").
	DownloadSkipped = direct.DownloadSkipped
	// DownloadFailed: final (not found, replaced while read, damaged, still
	// staged, a key that isn't a safe path); Err says why.
	DownloadFailed = direct.DownloadFailed
	// DownloadPending: stopped by a passing error (the network, the storage
	// network, a revoked key, a cancel), kept in the run's journal with its
	// part file, so running the same download again, or ResumeDownloads,
	// goes on from its written pieces. Err says why.
	DownloadPending = direct.DownloadPending
)

// DownloadFilesOptions are the options of DownloadFiles and DownloadPrefix.
type DownloadFilesOptions struct {
	// Paths names a key's path under the destination ("/"-separated, as a
	// key is); a key it doesn't name goes to its own key's path. DownloadFiles
	// only: DownloadPrefix maps each key to its path after the prefix.
	Paths map[string]string
	// SkipExisting leaves a file that already has the object's size and
	// SHA-256 (never compared by ETag): nothing is signed, or billed, for it.
	SkipExisting bool
	// Concurrency is how many objects, or windows of a large object, are
	// fetched at once. Zero or a negative value means the default (8).
	Concurrency int
	// Progress is told the bytes written so far, of all the objects.
	Progress ProgressFunc
	// NoJournal keeps no journal in the state directory: a download stopped
	// part way starts again when it is run again.
	NoJournal bool
}

// DownloadResult is one object's result from DownloadFiles, DownloadPrefix
// or ResumeDownloads. One object's failure never fails another.
type DownloadResult struct {
	// Bucket is the run's bucket (ResumeDownloads finishes runs of any
	// bucket).
	Bucket string
	// Key is the object's key.
	Key string
	// Path is where it was written (or would have been).
	Path string
	// Status is DownloadSaved, DownloadSkipped, DownloadFailed or
	// DownloadPending.
	Status string
	// Object is the object, for saved and skipped (not for a folder marker).
	Object *Object
	// Err is why it failed or is pending.
	Err error
	// Resumed: it went on from pieces an earlier run had written.
	Resumed bool
}

// Saved reports an object written to its path, verified.
func (r DownloadResult) Saved() bool { return r.Status == DownloadSaved }

// Skipped reports a file already there with the object's size and SHA-256,
// or a folder marker.
func (r DownloadResult) Skipped() bool { return r.Status == DownloadSkipped }

// Pending reports an object kept to go on with later.
func (r DownloadResult) Pending() bool { return r.Status == DownloadPending }

// DownloadRun is a download run kept in the state directory (Config.StateDir).
type DownloadRun struct {
	ID     string
	Bucket string
	// Dir is the run's destination folder.
	Dir     string
	Created time.Time
	Updated time.Time
	Files   int
	Saved   int
	Failed  int
	// Pending are the objects not written yet: ResumeDownloads, or the same
	// download run again, goes on with them.
	Pending int
	// Expired: untouched for longer than RunExpiry; only CleanDownloads
	// removes it.
	Expired bool
	// InUse: a live process (this one included) is working on it.
	InUse bool
}

func (o *DownloadFilesOptions) batch() direct.BatchParams {
	if o == nil {
		return direct.BatchParams{}
	}
	return direct.BatchParams{SkipExisting: o.SkipExisting, Concurrency: o.Concurrency,
		Progress: progressOf(o.Progress), NoJournal: o.NoJournal}
}

func catalogOf(o *Object) *direct.Catalog {
	return &direct.Catalog{Size: o.Size, SHA256: o.ChecksumSHA256, ETag: o.ETag, Ref: o}
}

func downloadResults(rs []direct.DownloadResult) []DownloadResult {
	if rs == nil {
		return nil
	}
	out := make([]DownloadResult, 0, len(rs))
	for _, r := range rs {
		d := DownloadResult{Bucket: r.Bucket, Key: r.Key, Path: r.Path, Status: r.Status, Err: r.Err,
			Resumed: r.Resumed}
		switch {
		case r.Session != nil:
			d.Object = objectOf(r.Session)
		case r.Ref != nil:
			d.Object, _ = r.Ref.(*Object)
		}
		out = append(out, d)
	}
	return out
}

// DownloadFiles downloads many objects, each to dir joined with its key
// (a/b.jpg to dir/a/b.jpg; Paths can name another path under dir), straight
// from the storage network, and returns one DownloadResult per key, in order
// (a key given twice is one), then any key whose file another key already
// takes (failed). One object's failure never fails another.
//
// Read sessions are signed 1,000 objects to a request. Up to Concurrency
// objects, or windows of a large object, are fetched at once, each verified
// before it is written to <name>.teepin-part beside its target; an object is
// checked against its SHA-256 and renamed into place only when whole, with
// its upload time as the file's.
//
// A download stopped by a passing error (the network, the storage network
// unavailable, a revoked key) or a cancelled ctx is pending: its part file and
// the run's journal stay in the state directory, and running the same download
// again, or ResumeDownloads (in this process or another), goes on from what
// was written, unless the object has changed since.
//
// A key that isn't a safe path under dir fails (*UnsafePathError): "..", an
// absolute path, on Windows a name Windows can't hold, a link under dir (of
// any kind, wherever it leads), or two keys for one file (names that differ
// only by case, where names ignore case). A key ending in "/" (a folder
// marker) is skipped. An object still staged on the API fails with
// ErrObjectStaged.
//
// The error is only what stops the whole call: a cancelled ctx (with every
// result, those not finished pending), the state directory unwritable, or a
// failure signing the read sessions or reading an object's details for
// SkipExisting.
func (s *StorageService) DownloadFiles(ctx context.Context, bucket string, keys []string, dir string, opts *DownloadFilesOptions) ([]DownloadResult, error) {
	pairs := make([]direct.DownloadPair, 0, len(keys))
	for _, k := range keys {
		rel := k
		if opts != nil {
			if p, ok := opts.Paths[k]; ok {
				rel = p
			}
		}
		pairs = append(pairs, direct.DownloadPair{Key: k, Rel: rel})
	}
	p := opts.batch()
	p.Stat = func(ctx context.Context, key string) (*direct.Catalog, error) {
		obj, err := s.StatObject(ctx, bucket, key)
		if err != nil {
			return nil, err
		}
		return catalogOf(obj), nil
	}
	rs, err := s.downloads().Run(ctx, bucket, pairs, dir, p)
	return downloadResults(rs), err
}

// DownloadPrefix downloads every object under prefix (all of them for ""),
// each to dir joined with its key after the prefix (and any "/" it then
// starts with), as DownloadFiles does. With SkipExisting the listing's size
// and SHA-256 decide, so a file already there costs nothing more.
func (s *StorageService) DownloadPrefix(ctx context.Context, bucket, prefix, dir string, opts *DownloadFilesOptions) ([]DownloadResult, error) {
	var pairs []direct.DownloadPair
	details := map[string]*direct.Catalog{}
	for obj, err := range s.Objects(ctx, bucket, prefix) {
		if err != nil {
			return nil, err
		}
		o := obj
		pairs = append(pairs, direct.DownloadPair{Key: o.Key, Rel: strings.TrimLeft(o.Key[len(prefix):], "/")})
		details[o.Key] = catalogOf(&o)
	}
	p := opts.batch()
	p.Details = details
	rs, err := s.downloads().Run(ctx, bucket, pairs, dir, p)
	return downloadResults(rs), err
}

// ResumeDownloads goes on with every download run an earlier process (or a
// cancelled call in this one) left unfinished, from its journal in the state
// directory: each run's unfinished objects downloaded again to the same paths,
// from the pieces already written (an object replaced since starts again). It
// returns their results. Runs a live process is working on are left alone,
// and each SDK resumes only its own runs. Running the same DownloadFiles call
// again does the same for its objects.
func (s *StorageService) ResumeDownloads(ctx context.Context, progress ProgressFunc) ([]DownloadResult, error) {
	rs, err := s.downloads().Resume(ctx, progressOf(progress))
	return downloadResults(rs), err
}

// ListDownloadRuns lists the download runs kept in the state directory: how
// many objects each has saved, failed and still pending, whether a live
// process is working on it, and whether it has expired (untouched for
// RunExpiry).
func (s *StorageService) ListDownloadRuns(ctx context.Context) ([]DownloadRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runs, err := s.downloads().ListRuns()
	if err != nil {
		return nil, err
	}
	out := make([]DownloadRun, 0, len(runs))
	for _, r := range runs {
		out = append(out, DownloadRun(r))
	}
	return out, nil
}

// CleanDownloads removes the download runs untouched for longer than
// olderThan (RunExpiry is the usual age; 0 removes every run no process is
// working on), with their part files, with a warning for each: never done
// silently. Finished runs remove their own state.
func (s *StorageService) CleanDownloads(ctx context.Context, olderThan time.Duration) ([]DownloadRun, error) {
	runs, err := s.downloads().Clean(ctx, olderThan)
	out := make([]DownloadRun, 0, len(runs))
	for _, r := range runs {
		out = append(out, DownloadRun(r))
	}
	return out, err
}
