// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
	"github.com/FlashbackAi/teepin-go/teepin/internal/journal"
)

// Batch downloads to a folder (TEEPIN-STORAGE-DESIGN.md sections 19.15, 22.5 and 25.1), a port of teepin-python's
// _downloads.py (the reference) by way of teepin-node's downloads.ts, with the same rules.
//
// Read sessions are signed 1,000 objects to a request (objects/read), and a large object's later windows one at a
// time. The work is a pool of pieces, one per object or per window of a large object, Concurrency at a time:
//
//   - a piece's bytes are verified as they arrive and written into <name>.teepin-part beside the target, fsynced, then
//     journaled;
//   - an object whose pieces are all written is checked against its SHA-256 (read back from the part file), then
//     renamed into place (journal.RenameOver), so a target is never half written;
//   - a run stopped by a passing error (the network, the storage network, a revoked key), by a cancelled context or by
//     the process ending keeps its journal and part files: running the same download again, or Resume, goes on from
//     the written pieces of each object whose version hasn't changed.
//
// SkipExisting decides from the object's details before anything is signed (signatures are metered): the file there
// must have the object's size and plaintext SHA-256, never its ETag.
//
// Paths (downloads_paths.go has the rules): no link under the destination may carry a write out of it. That's checked
// before anything is signed, and again before a part file is made and before it's renamed. A part file is only ever
// new (never a name already there, which could be a link into another file) or one an earlier run made with no other
// name, and it's written only through a handle checked to still be that file, so a link or a file put in its place
// later is never written through.
//
// The journal is the state directory's downloads/<run-id>/ (internal/journal, kind Downloads): a run row with the
// bucket and the destination, a row per object (by path) and a row per written piece. A run adopts the unfinished
// objects of this SDK's earlier runs at its paths: each is taken out of its run's journal first, so no two journals
// ever claim one part file's pieces, and a run left with nothing unfinished is removed.

// The statuses of a batch download's object.
const (
	// DownloadSaved: the object is written to its path, verified.
	DownloadSaved = "saved"
	// DownloadSkipped: the file there already has the object's size and SHA-256, or the key is a folder marker.
	DownloadSkipped = "skipped"
	// DownloadFailed: final (not found, replaced while read, damaged, a key that isn't a safe path).
	DownloadFailed = "failed"
	// DownloadPending: stopped by a passing error, kept in the run's journal with its part file, so running the
	// download again (or Resume) goes on from its written pieces.
	DownloadPending = "pending"
)

// Catalog is what skipping an existing file needs of an object: its size, SHA-256 and ETag. Ref is the caller's own
// value for the object, returned in a skipped result.
type Catalog struct {
	Size   int64
	SHA256 string
	ETag   string
	Ref    any
}

// DownloadResult is one object's result.
type DownloadResult struct {
	// Bucket is the run's bucket.
	Bucket string
	Key    string
	Path   string
	// Status is DownloadSaved, DownloadSkipped, DownloadFailed or DownloadPending.
	Status string
	// Session is the object's read session (its key forgotten), when it is saved.
	Session *Session
	// Ref is the Catalog's Ref, when it is skipped.
	Ref any
	// Err is why it failed or is pending.
	Err error
	// Resumed: it went on from pieces an earlier run had written.
	Resumed bool
}

// BatchParams are a batch download's choices.
type BatchParams struct {
	// SkipExisting leaves a file that already has the object's size and SHA-256.
	SkipExisting bool
	// Concurrency is how many pieces are fetched at once (0: 8).
	Concurrency int
	// Progress is told the bytes written so far, of all the objects.
	Progress func(done, total int64)
	// NoJournal keeps nothing on disk: nothing to go on from after a stop.
	NoJournal bool
	// Details are objects' catalog details already known (a listing's), for SkipExisting.
	Details map[string]*Catalog
	// Stat is an object's catalog details, for SkipExisting when Details lacks it; a not-found error fails that key.
	Stat func(ctx context.Context, key string) (*Catalog, error)
}

// DownloadRun is a download run kept in the state directory.
type DownloadRun struct {
	ID      string
	Bucket  string
	Dir     string
	Created time.Time
	Updated time.Time
	Files   int
	Saved   int
	Failed  int
	// Pending are the objects not written yet: Resume, or the same download run again, goes on with them.
	Pending int
	// Expired: untouched for longer than RunExpiry.
	Expired bool
	// InUse: a live process (this one included) is working on it.
	InUse bool
}

// DownloadHooks let tests watch a download's steps. Nil fields do nothing.
type DownloadHooks struct {
	// Journaled is told of each journal write, once it is durable.
	Journaled func(entries []journal.Entry)
	// Synced is told of each piece's fsync, with the part file's path.
	Synced func(part string)
	// Sessions runs just before the read sessions are asked for.
	Sessions func()
	// CaseInsensitive decides whether names under the destination ignore case, in place of the probe.
	CaseInsensitive func(dir string) bool
	// Chunk may change each verified chunk before it is written (a test's damage on the way to the disk).
	Chunk func(c []byte) []byte
	// Target is the path a finished object is renamed to, in place of its own: a test's two keys that the file
	// system takes for one file (NFC and NFD forms of a name on APFS, which fold doesn't join).
	Target func(path string) string
}

// Downloads is batch downloads for one client. Safe for concurrent use.
type Downloads struct {
	reads    *Reads
	stateDir string
	// Hooks are for tests.
	Hooks DownloadHooks
	// Warn is where warnings go (a run Clean removes, a damaged journal); slog's default logger when nil.
	Warn func(msg string)
}

// NewDownloads is the batch downloads of a client, through its read engine (so they share its key cache).
func NewDownloads(reads *Reads, stateDir string) *Downloads {
	return &Downloads{reads: reads, stateDir: stateDir}
}

func (d *Downloads) warn(msg string) {
	if d.Warn != nil {
		d.Warn(msg)
		return
	}
	slog.Warn(msg)
}

// The journal's rows.
type dlRunRow struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Bucket  string `json:"bucket"`
	Dir     string `json:"dir"`
	Created int64  `json:"created"`
	Updated int64  `json:"updated"`
	Status  string `json:"status"`
}

type dlRow struct {
	Path    string `json:"path"`
	Key     string `json:"key"`
	Version string `json:"version"`
	Size    int64  `json:"size"`
	State   string `json:"state"` // open, saved or failed
	Error   string `json:"error,omitempty"`
}

type rangeRow struct {
	Path string `json:"path"`
	A    int64  `json:"a"`
	B    int64  `json:"b"`
}

const (
	tableRun      = "run"
	tableDownload = "downloads"
	tableRange    = "ranges"
	stateOpen     = "open"
)

// rangeID names a piece's row: its path (which never holds a NUL) and its bounds.
func rangeID(path string, r [2]int64) string {
	return path + "\x00" + strconv.FormatInt(r[0], 10) + "-" + strconv.FormatInt(r[1], 10)
}

func putRange(path string, r [2]int64) journal.Entry {
	return journal.Entry{Table: tableRange, ID: rangeID(path, r), Row: rangeRow{Path: path, A: r[0], B: r[1]}}
}

// prior is an earlier run's unfinished object: its version, size and written pieces.
type prior struct {
	version string
	size    int64
	ranges  map[[2]int64]bool
}

// dlTarget is one object being downloaded to its path.
type dlTarget struct {
	e      *planEntry
	part   string
	s      *Session
	dek    []byte // this download's own copy of the data key, zeroed when the run ends
	ranges [][2]int64
	prior  *prior
	id     fileID // the part file, once made or adopted
	hasID  bool

	mu      sync.Mutex
	done    map[[2]int64]bool
	err     error
	resumed bool
	saved   bool
}

func (t *dlTarget) failed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err != nil
}

func (t *dlTarget) fail(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err == nil {
		t.err = err
	}
}

// markDone records a written piece, and reports whether every piece is now written (true once, to the last).
func (t *dlTarget) markDone(r [2]int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done[r] = true
	return len(t.done) == len(t.ranges)
}

func (t *dlTarget) doneBytes() int64 {
	var n int64
	for r := range t.done {
		n += r[1] - r[0]
	}
	return n
}

// batch is one DownloadFiles call, or one run of Resume.
type batch struct {
	d      *Downloads
	ctx    context.Context
	bucket string
	root   string
	p      BatchParams
	j      *journal.Journal
	rd     *journal.RunDir // nil: in memory
	prog   *Progress

	results map[*planEntry]DownloadResult
	targets []*dlTarget

	// placedMu serializes the renames into place; placed is the identity of each file this batch renamed into place,
	// with its key, so a later key the file system takes for the same file fails instead of replacing it.
	placedMu sync.Mutex
	placed   map[[2]uint64]string

	stopMu  sync.Mutex
	stopErr error // what stopped the run (a revoked key): the rest stays pending
	ended   bool  // the run went to its end (not stopped by a cancel or an error out of it)
}

// Run downloads every key of pairs to its path under dir, and returns one result per key, in the order the keys
// came (a key given twice is one), then the keys that would land on another key's file. Only what stops the whole run
// is an error: a cancelled ctx (with every result so far, the rest pending), the state directory unwritable, or a
// failure signing the read sessions or reading an object's details.
func (d *Downloads) Run(ctx context.Context, bucket string, pairs []DownloadPair, dir string, p BatchParams) ([]DownloadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	insensitive := false
	if h := d.Hooks.CaseInsensitive; h != nil {
		insensitive = h(root)
	} else {
		insensitive = CaseInsensitive(root)
	}
	entries, collisions := planPaths(root, pairs, insensitive)
	if p.Concurrency <= 0 {
		p.Concurrency = 8
	}
	b := &batch{d: d, ctx: ctx, bucket: bucket, root: root, p: p, results: map[*planEntry]DownloadResult{}}
	if err := b.open(); err != nil {
		return nil, err
	}
	defer b.end()
	var todo []*planEntry
	for _, e := range entries {
		if e.result == nil {
			todo = append(todo, e)
		}
	}
	err = b.run(todo)
	out := b.output(entries, collisions)
	for i := range out {
		out[i].Bucket = bucket
	}
	if err != nil {
		return out, Stopped(ctx, err)
	}
	b.ended = true
	return out, nil
}

// open starts the run's journal: on disk, locked by this process, or in memory.
func (b *batch) open() error {
	if b.p.NoJournal {
		b.j = journal.Memory()
		return nil
	}
	rd, err := journal.CreateRun(b.d.stateDir, journal.Downloads)
	if err != nil {
		return err
	}
	b.rd, b.j = rd, rd.Journal()
	now := time.Now().UnixMilli()
	row := dlRunRow{ID: rd.ID, Kind: journal.Downloads, Bucket: b.bucket, Dir: b.root, Created: now, Updated: now,
		Status: stateOpen}
	if err := b.write(journal.Entry{Table: tableRun, ID: tableRun, Row: row}); err != nil {
		_ = rd.Remove()
		b.rd = nil
		return err
	}
	return nil
}

// write writes entries to the run's journal together, durably.
func (b *batch) write(entries ...journal.Entry) error {
	if err := b.j.Write(entries...); err != nil {
		return err
	}
	if h := b.d.Hooks.Journaled; h != nil {
		h(entries)
	}
	return nil
}

func (b *batch) run(todo []*planEntry) error {
	todo, err := b.skip(todo)
	if err != nil {
		return err
	}
	var adopted map[string]*prior
	if b.rd != nil {
		paths := map[string]bool{}
		for _, e := range todo {
			paths[e.path] = true
		}
		adopted = b.adopt(paths)
	}
	if h := b.d.Hooks.Sessions; h != nil {
		h()
	}
	var sessions map[string]BatchItem
	defer func() {
		for _, it := range sessions {
			if it.Session != nil {
				it.Session.Forget()
			}
		}
	}()
	if len(todo) > 0 {
		keys := make([]string, 0, len(todo))
		for _, e := range todo {
			keys = append(keys, e.key)
		}
		if sessions, err = b.d.reads.Sessions(b.ctx, b.bucket, keys); err != nil {
			return err
		}
	}
	for _, e := range todo {
		it := sessions[e.key]
		switch {
		case it.Err != nil:
			if adopted[e.path] != nil && !downloadPassing(it.Err) {
				rmPart(b.root, e.path) // an earlier run's part file of an object that can't be read any more
			}
			b.fail(e, it.Err)
		case it.Session.Staged():
			b.fail(e, staged(e.key))
		default:
			b.target(e, it.Session, adopted[e.path])
		}
	}
	if err := b.parts(); err != nil {
		return err
	}
	return b.pieces()
}

func (b *batch) fail(e *planEntry, err error) {
	b.results[e] = DownloadResult{Key: e.key, Path: e.path, Status: DownloadFailed, Err: err}
}

// skip is what's left after SkipExisting, decided before anything is signed.
func (b *batch) skip(entries []*planEntry) ([]*planEntry, error) {
	if !b.p.SkipExisting {
		return entries, nil
	}
	var todo []*planEntry
	for _, e := range entries {
		if fi, err := os.Stat(e.path); err == nil && fi.Mode().IsRegular() {
			cat := b.p.Details[e.key]
			if cat == nil && b.p.Stat != nil {
				var err error
				if cat, err = b.p.Stat(b.ctx, e.key); err != nil {
					if statusOf(err) == 404 {
						b.fail(e, err)
						continue
					}
					return nil, err
				}
			}
			same, err := sameFile(b.ctx, e.path, cat)
			if err != nil && b.ctx.Err() != nil {
				return nil, b.ctx.Err()
			}
			if same {
				b.results[e] = DownloadResult{Key: e.key, Path: e.path, Status: DownloadSkipped, Ref: cat.Ref}
				continue
			}
		}
		todo = append(todo, e)
	}
	return todo, nil
}

// sameFile reports whether the file at path already is the object: its size and plaintext SHA-256 (never the ETag).
// An object whose checksum is a composite of parts can't be compared, so it is never the same.
func sameFile(ctx context.Context, path string, cat *Catalog) (bool, error) {
	if cat == nil {
		return false, nil
	}
	fi, err := os.Stat(path)
	if err != nil || fi.Size() != cat.Size {
		return false, nil
	}
	if cat.SHA256 == "" || containsDash(cat.ETag) {
		return false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return false, nil
	}
	defer f.Close()
	got, err := sha256Of(ctx, f)
	if err != nil {
		return false, err
	}
	return got == cat.SHA256, nil
}

func containsDash(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			return true
		}
	}
	return false
}

// ctxReader stops a long read once ctx ends.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// sha256Of is the SHA-256 of what r holds from its start, in hex.
func sha256Of(ctx context.Context, r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.CopyBuffer(h, ctxReader{ctx, r}, make([]byte, 1<<20)); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// target is the object's target: its path checked again, its folder made, and its earlier run's part file adopted
// when it's still this version's and only this run's to write. Nothing is written yet.
func (b *batch) target(e *planEntry, s *Session, pr *prior) {
	err := checkInside(b.root, e.path) // again, just before anything is written: links can appear
	if err == nil {
		err = os.MkdirAll(filepath.Dir(e.path), 0o755)
	}
	if err != nil {
		s.Forget()
		b.fail(e, err)
		return
	}
	dek := clone(s.DEK)
	s.Forget()
	t := &dlTarget{e: e, part: e.path + PartSuffix, s: s, dek: dek, ranges: b.d.reads.Ranges(s, 0), prior: pr,
		done: map[[2]int64]bool{}}
	if pr != nil && pr.version == s.Version && pr.size == s.Size {
		if id, ok := ownFile(t.part, s.Size); ok {
			t.id, t.hasID = id, true
			for _, r := range t.ranges {
				if pr.ranges[r] {
					t.done[r] = true
				}
			}
			t.resumed = len(t.done) > 0
		}
	}
	b.targets = append(b.targets, t)
}

// ownFile is the identity of the file at path when it is a regular file of size bytes with no other name (not a
// link, nor hard-linked), so writing into it writes nothing else.
func ownFile(path string, size int64) (fileID, bool) {
	id, n, fi, err := pathID(path)
	if err != nil || !fi.Mode().IsRegular() || n != 1 || fi.Size() != size {
		return fileID{}, false
	}
	return id, true
}

// parts journals every target, then makes the part files of those not adopted. Each is new, never a name already
// there: writing into an old name could write through a link (hard or symbolic) into another file.
func (b *batch) parts() error {
	var entries []journal.Entry
	for _, t := range b.targets {
		if t.prior != nil {
			for r := range t.prior.ranges { // an adopted piece this version doesn't have
				if !t.done[r] {
					entries = append(entries, journal.Entry{Table: tableRange, ID: rangeID(t.e.path, r), Remove: true})
				}
			}
		}
		entries = append(entries, journal.Entry{Table: tableDownload, ID: t.e.path, Row: t.row(stateOpen, "")})
		for _, r := range t.ranges {
			if t.done[r] {
				entries = append(entries, putRange(t.e.path, r))
			}
		}
	}
	if len(entries) > 0 {
		if err := b.write(entries...); err != nil {
			return err
		}
	}
	kept := b.targets[:0]
	for _, t := range b.targets {
		if !t.hasID {
			if err := b.newPart(t); err != nil {
				clear(t.dek)
				b.fail(t.e, err)
				continue
			}
		}
		kept = append(kept, t)
	}
	b.targets = kept
	return nil
}

func (t *dlTarget) row(state, msg string) dlRow {
	return dlRow{Path: t.e.path, Key: t.e.key, Version: t.s.Version, Size: t.s.Size, State: state, Error: msg}
}

// newPart makes the target's part file, new: whatever had its name is removed first (a regular file only; a link was
// refused before), and the file is made with O_EXCL, so a link put there meanwhile fails it.
func (b *batch) newPart(t *dlTarget) error {
	if fi, err := os.Lstat(t.part); err == nil {
		if !fi.Mode().IsRegular() {
			return errs.Unsafe("%q isn't a safe path under the destination: something other than a file is at %s",
				t.e.path, t.part)
		}
		if err := journalRemove(t.part); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(t.part, os.O_RDWR|os.O_CREATE|os.O_EXCL|oNoFollow, 0o666)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(t.s.Size); err != nil {
		return err
	}
	id, _, err := handleID(f)
	if err != nil {
		return err
	}
	t.id, t.hasID = id, true
	return nil
}

// piece is one piece of work: a range of an object to write, or (nil) an object whose pieces are all written.
type piece struct {
	t *dlTarget
	r *[2]int64
}

func (b *batch) pieces() error {
	var total int64
	for _, t := range b.targets {
		total += t.s.Size
	}
	b.prog = NewProgress(total, b.p.Progress)
	var work []piece
	for _, t := range b.targets {
		b.prog.Add(t.doneBytes())
		if len(t.done) == len(t.ranges) {
			work = append(work, piece{t: t}) // every piece written before a stop: finish it
		}
		for i := range t.ranges {
			if !t.done[t.ranges[i]] {
				work = append(work, piece{t: t, r: &t.ranges[i]})
			}
		}
	}
	err := RunPool(b.ctx, b.p.Concurrency, work, func(ctx context.Context, w piece) error {
		b.piece(ctx, w)
		return nil // a piece's error is its object's, never the pool's
	})
	for _, t := range b.targets {
		b.results[t.e] = b.result(t)
	}
	return err
}

func (b *batch) stopped() error {
	b.stopMu.Lock()
	defer b.stopMu.Unlock()
	return b.stopErr
}

// piece writes one piece, and finishes its object once every piece is written. Its error is recorded on its object;
// one that stops the run (a revoked key, no credit) stops the pieces not started.
func (b *batch) piece(ctx context.Context, w piece) {
	t := w.t
	if b.stopped() != nil || t.failed() || ctx.Err() != nil {
		return
	}
	err := func() error {
		if w.r != nil {
			if err := b.writePiece(ctx, t, *w.r); err != nil {
				return err
			}
			if err := b.write(putRange(t.e.path, *w.r)); err != nil {
				return err
			}
			if !t.markDone(*w.r) {
				return nil
			}
		}
		return b.finish(ctx, t)
	}()
	if err == nil {
		return
	}
	t.fail(err)
	if stopsRun(err) {
		b.stopMu.Lock()
		if b.stopErr == nil {
			b.stopErr = err
		}
		b.stopMu.Unlock()
	}
}

// openOwn opens the target's part file (never through a symbolic link where the OS can tell) and checks it is still
// the file this run made or adopted, with no other name.
func openOwn(t *dlTarget, write bool) (*os.File, error) {
	flag := os.O_RDONLY
	if write {
		flag = os.O_RDWR
	}
	f, err := os.OpenFile(t.part, flag|oNoFollow, 0)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, replaced(t)
		}
		return nil, err
	}
	id, n, err := handleID(f)
	if err == nil && (!id.same(t.id) || n != 1) {
		err = replaced(t)
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func replaced(t *dlTarget) error { return errs.Unsafe("%s was replaced while it was written", t.part) }

// checkOwn is an UnsafePathError unless the target's part file is still the one this run made or adopted, with no
// other name.
func checkOwn(t *dlTarget) error {
	id, n, fi, err := pathID(t.part)
	if err != nil || !fi.Mode().IsRegular() || n != 1 || !id.same(t.id) {
		return replaced(t)
	}
	return nil
}

// writePiece writes the object's bytes [a, b), each verified before it is written, then fsyncs them.
func (b *batch) writePiece(ctx context.Context, t *dlTarget, r [2]int64) error {
	f, err := openOwn(t, true)
	if err != nil {
		return err
	}
	defer f.Close()
	pos := r[0]
	err = b.d.reads.Plaintext(ctx, t.s, t.dek, r[0], r[1], len(t.ranges) == 1, func(c []byte) error {
		if h := b.d.Hooks.Chunk; h != nil {
			c = h(c)
		}
		if _, err := f.WriteAt(c, pos); err != nil {
			return err
		}
		pos += int64(len(c))
		b.prog.Add(int64(len(c)))
		return nil
	})
	if err != nil {
		return err
	}
	if pos != r[1] {
		return errs.Integrity(errs.ReasonTruncated, "%s: %d bytes of the %d from %d", t.e.key, pos-r[0], r[1]-r[0], r[0])
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if h := b.d.Hooks.Synced; h != nil {
		h(t.part)
	}
	return nil
}

// finish checks an object whose pieces are all written against its SHA-256, then renames it into place, with the
// object's upload time.
func (b *batch) finish(ctx context.Context, t *dlTarget) error {
	if len(t.ranges) > 1 && t.s.Checkable() {
		f, err := openOwn(t, false)
		if err != nil {
			return err
		}
		got, err := sha256Of(ctx, f)
		_ = f.Close()
		if err != nil {
			return err
		}
		if got != t.s.SHA256 {
			return errs.Checksum(fmt.Sprintf("%s: received %s, stored %s", t.e.key, got, t.s.SHA256))
		}
	}
	if err := checkInside(b.root, t.e.path); err != nil { // and again before the rename
		return err
	}
	if err := checkOwn(t); err != nil {
		return err
	}
	path, err := b.place(t)
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.saved = true
	t.mu.Unlock()
	if !t.s.UploadedAt.IsZero() {
		_ = os.Chtimes(path, t.s.UploadedAt, t.s.UploadedAt)
	}
	_ = b.write(journal.Entry{Table: tableDownload, ID: t.e.path, Row: t.row(DownloadSaved, "")})
	return nil
}

// place renames the target's part file into place, unless the file at its path is one this batch already renamed into
// place for another key: names fold doesn't join (NFC and NFD forms of one name, say) can still be one file to the
// file system, and the later key's rename would replace the earlier's file with both reported saved. The later key
// fails then, as planPaths fails a key whose path another key takes. Renames are one at a time, so two keys finishing
// together can't both find the path free.
func (b *batch) place(t *dlTarget) (string, error) {
	path := t.e.path
	if h := b.d.Hooks.Target; h != nil {
		path = h(path)
	}
	b.placedMu.Lock()
	defer b.placedMu.Unlock()
	if id, _, _, err := pathID(path); err == nil {
		if k, ok := id.key(); ok {
			if other, taken := b.placed[k]; taken && other != t.e.key {
				return "", errs.Unsafe("%q and %q both go to %s", t.e.key, other, path)
			}
		}
	}
	if err := journal.RenameOver(t.part, path); err != nil { // on Windows, waits a moment for another program
		return "", err
	}
	if k, ok := t.id.key(); ok { // the part file, renamed: the file now at path
		if b.placed == nil {
			b.placed = map[[2]uint64]string{}
		}
		b.placed[k] = t.e.key
	}
	return path, nil
}

// downloadPassing reports an error after which an object's download is kept to go on with later: one that stops the
// run, a cancel, the network, a busy or failing service, or the local disk (full, say: any failed system call).
func downloadPassing(err error) bool {
	if stopsRun(err) || isCtxErr(err) || IsPassing(err) || isUnavailable(err) {
		return true
	}
	var unsafe *errs.UnsafePathError
	if errors.As(err, &unsafe) {
		return false
	}
	var pe *fs.PathError
	var le *os.LinkError
	var se *os.SyscallError
	var en syscall.Errno
	return errors.As(err, &pe) || errors.As(err, &le) || errors.As(err, &se) || errors.As(err, &en)
}

func (b *batch) result(t *dlTarget) DownloadResult {
	t.mu.Lock()
	saved, err := t.saved, t.err
	t.mu.Unlock()
	clear(t.dek)
	out := DownloadResult{Key: t.e.key, Path: t.e.path, Resumed: t.resumed}
	if saved {
		out.Status, out.Session = DownloadSaved, t.s
		return out
	}
	if err == nil {
		err = b.stopped()
	}
	if err == nil {
		err = b.ctx.Err()
	}
	out.Err = err
	if err == nil || downloadPassing(err) {
		out.Status = DownloadPending
		return out
	}
	// Final: nothing to go on from. The part file is removed only while no link leads its path out of the destination.
	rmPart(b.root, t.e.path)
	_ = b.write(journal.Entry{Table: tableDownload, ID: t.e.path, Row: t.row(DownloadFailed, err.Error())})
	out.Status = DownloadFailed
	return out
}

// output is one result per entry, in order, then the collisions; nil when the run stopped before every object had a
// result.
func (b *batch) output(entries []*planEntry, collisions []DownloadResult) []DownloadResult {
	out := make([]DownloadResult, 0, len(entries)+len(collisions))
	for _, e := range entries {
		if e.result != nil {
			out = append(out, *e.result)
			continue
		}
		r, ok := b.results[e]
		if !ok {
			return nil
		}
		out = append(out, r)
	}
	return append(out, collisions...)
}

// end closes the run, always: removed when nothing is left to go on with, else kept (unlocked) for the next run.
func (b *batch) end() {
	for _, t := range b.targets {
		clear(t.dek)
	}
	if b.rd == nil {
		_ = b.j.Close()
		return
	}
	pending := false
	for _, r := range b.results {
		pending = pending || r.Status == DownloadPending
	}
	if pending || !b.ended {
		var run dlRunRow
		if ok, _ := b.j.Get(tableRun, tableRun, &run); ok {
			run.Updated, run.Status = time.Now().UnixMilli(), stateOpen
			_ = b.write(journal.Entry{Table: tableRun, ID: tableRun, Row: run})
		}
		_ = b.rd.Release()
		return
	}
	if err := b.rd.Remove(); err != nil {
		_ = b.rd.Release() // a run that can't be removed is removed by the next one
	}
}

// openRows are a download journal's unfinished objects by path, with their written pieces.
func openRows(j *journal.Journal) (map[string]dlRow, map[string]map[[2]int64]bool) {
	rows := map[string]dlRow{}
	for _, raw := range j.Rows(tableDownload) {
		var r dlRow
		if json.Unmarshal(raw, &r) == nil && r.State == stateOpen {
			rows[r.Path] = r
		}
	}
	ranges := map[string]map[[2]int64]bool{}
	for _, raw := range j.Rows(tableRange) {
		var r rangeRow
		if json.Unmarshal(raw, &r) != nil || r.A < 0 || r.B < r.A {
			continue
		}
		if _, ok := rows[r.Path]; !ok {
			continue
		}
		if ranges[r.Path] == nil {
			ranges[r.Path] = map[[2]int64]bool{}
		}
		ranges[r.Path][[2]int64{r.A, r.B}] = true
	}
	return rows, ranges
}

// adopt takes earlier runs' unfinished objects at these paths: their version, size and written pieces, moved into
// this run's journal. Each is taken out of its run's journal first, so no two journals ever claim one part file's
// pieces (a stop in between loses them, and the object starts again). A run left with nothing unfinished is removed;
// one another process holds is left alone.
func (b *batch) adopt(paths map[string]bool) map[string]*prior {
	out := map[string]*prior{}
	ids, err := journal.RunIDs(b.d.stateDir, journal.Downloads)
	if err != nil {
		return out
	}
	for _, id := range ids {
		if id == b.rd.ID {
			continue
		}
		rd, err := journal.TakeRun(b.d.stateDir, journal.Downloads, id)
		if err != nil {
			b.d.warn(fmt.Sprintf("teepin: download run %s can't be read: %v", id, err))
			continue
		}
		if rd == nil {
			continue // another process is working on it
		}
		rows, ranges := openRows(rd.Journal())
		var taken []dlRow
		left := 0
		for path, r := range rows {
			if paths[path] && out[path] == nil {
				taken = append(taken, r)
			} else {
				left++
			}
		}
		if left == 0 {
			if err := rd.Remove(); err != nil {
				_ = rd.Release()
				continue // not removed: nothing taken
			}
		} else {
			if len(taken) > 0 {
				var drop []journal.Entry
				for _, r := range taken {
					drop = append(drop, journal.Entry{Table: tableDownload, ID: r.Path, Remove: true})
					for rg := range ranges[r.Path] {
						drop = append(drop, journal.Entry{Table: tableRange, ID: rangeID(r.Path, rg), Remove: true})
					}
				}
				if err := rd.Journal().Write(drop...); err != nil {
					_ = rd.Release()
					continue // not rewritten: nothing taken
				}
			}
			_ = rd.Release()
		}
		if len(taken) == 0 {
			continue
		}
		var keep []journal.Entry
		for _, r := range taken {
			keep = append(keep, journal.Entry{Table: tableDownload, ID: r.Path, Row: r})
			for rg := range ranges[r.Path] {
				keep = append(keep, putRange(r.Path, rg))
			}
		}
		if err := b.write(keep...); err != nil {
			continue // lost: those objects start again
		}
		for _, r := range taken {
			rs := ranges[r.Path]
			if rs == nil {
				rs = map[[2]int64]bool{}
			}
			out[r.Path] = &prior{version: r.Version, size: r.Size, ranges: rs}
		}
	}
	return out
}

// --- resume, listing and cleanup ----------------------------------------------------------------------------------

// Resume goes on with every download run of this SDK that no live process is working on: each run's unfinished
// objects downloaded again to the same paths, adopting the pieces it wrote. It returns their results.
func (d *Downloads) Resume(ctx context.Context, progress func(done, total int64)) ([]DownloadResult, error) {
	ids, err := journal.RunIDs(d.stateDir, journal.Downloads)
	if err != nil {
		return nil, err
	}
	var out []DownloadResult
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		j, inUse, err := journal.PeekRun(d.stateDir, journal.Downloads, id)
		if err != nil {
			d.warn(fmt.Sprintf("teepin: download run %s can't be resumed: %v", id, err))
			continue
		}
		if inUse {
			continue
		}
		var run dlRunRow
		if ok, err := j.Get(tableRun, tableRun, &run); !ok || err != nil || run.Dir == "" {
			d.warn(fmt.Sprintf("teepin: download run %s can't be resumed: its journal names no destination", id))
			continue
		}
		rows, _ := openRows(j)
		var pairs []DownloadPair
		for _, r := range sortedRows(rows) {
			rel, err := filepath.Rel(run.Dir, r.Path)
			if err != nil || !filepath.IsLocal(rel) {
				continue
			}
			pairs = append(pairs, DownloadPair{Key: r.Key, Rel: filepath.ToSlash(rel)})
		}
		if len(pairs) == 0 {
			if rd, err := journal.LockRun(d.stateDir, journal.Downloads, id); err == nil && rd != nil {
				_ = rd.Remove() // nothing left to go on with
			}
			continue
		}
		res, err := d.Run(ctx, run.Bucket, pairs, run.Dir, BatchParams{Progress: progress})
		out = append(out, res...)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

func sortedRows(rows map[string]dlRow) []dlRow {
	out := make([]dlRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, r)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Path < out[k].Path })
	return out
}

// ListRuns is the download runs kept in the state directory. A run whose journal can't be read is left out.
func (d *Downloads) ListRuns() ([]DownloadRun, error) {
	ids, err := journal.RunIDs(d.stateDir, journal.Downloads)
	if err != nil {
		return nil, err
	}
	out := []DownloadRun{}
	now := time.Now()
	for _, id := range ids {
		j, inUse, err := journal.PeekRun(d.stateDir, journal.Downloads, id)
		if err != nil {
			continue
		}
		var run dlRunRow
		if ok, err := j.Get(tableRun, tableRun, &run); !ok || err != nil {
			continue
		}
		r := DownloadRun{ID: id, Bucket: run.Bucket, Dir: run.Dir, Created: time.UnixMilli(run.Created),
			Updated: time.UnixMilli(run.Updated), InUse: inUse}
		for _, raw := range j.Rows(tableDownload) {
			var row dlRow
			if json.Unmarshal(raw, &row) != nil {
				continue
			}
			r.Files++
			switch row.State {
			case DownloadSaved:
				r.Saved++
			case DownloadFailed:
				r.Failed++
			default:
				r.Pending++
			}
		}
		r.Expired = now.Sub(r.Updated) > RunExpiry
		out = append(out, r)
	}
	return out, nil
}

// Clean removes download runs untouched for longer than olderThan that no process is working on, with a warning for
// each: never silently. Their part files go with them (each only while no link leads its path out of the run's
// destination, and only a regular file).
func (d *Downloads) Clean(ctx context.Context, olderThan time.Duration) ([]DownloadRun, error) {
	runs, err := d.ListRuns()
	if err != nil {
		return nil, err
	}
	removed := []DownloadRun{}
	cutoff := time.Now().Add(-olderThan)
	for _, r := range runs {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if r.InUse || r.Updated.After(cutoff) {
			continue
		}
		rd, err := journal.TakeRun(d.stateDir, journal.Downloads, r.ID)
		if err != nil || rd == nil {
			continue
		}
		rows, _ := openRows(rd.Journal())
		if r.Dir != "" {
			for path := range rows {
				rmPart(r.Dir, path)
			}
		}
		d.warn(fmt.Sprintf("teepin: removing download run %s (bucket %s): %d objects not downloaded, untouched since %s",
			r.ID, r.Bucket, r.Pending, r.Updated.UTC().Format(time.RFC3339)))
		if err := rd.Remove(); err != nil {
			_ = rd.Release()
			return removed, err
		}
		removed = append(removed, r)
	}
	return removed, nil
}
