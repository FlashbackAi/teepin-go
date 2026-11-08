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
	"mime"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
	"github.com/FlashbackAi/teepin-go/teepin/internal/journal"
	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// Upload runs (sections 22.4, 22.6 and 17.12): many files sent as one run, journaled in the state directory (or in
// memory), so ResumeUploads finishes a run a crash, a lost network, a revoked key or a cancel cut short. The journal's
// tables: run (one row), files (one per file, numbered in order), packs, uploads (large ones) and parts.

// The statuses of a file in a run.
const (
	// StatusSaved: the file is stored.
	StatusSaved = "saved"
	// StatusFailed: the file failed for good (it changed, a precondition failed, ...).
	StatusFailed = "failed"
	// StatusPending: not uploaded yet because of a passing error (storage unavailable past the wait, a revoked key),
	// kept in the run's journal for ResumeUploads.
	StatusPending = "pending"
)

// RunFile is one file of a run: the key it is stored at, and its path.
type RunFile struct {
	Key  string
	Path string
}

// UploadResult is one file's result from a run.
type UploadResult struct {
	// Bucket is the run's bucket.
	Bucket string
	Key    string
	Path   string
	// Status is StatusSaved, StatusFailed or StatusPending.
	Status string
	Object *Object
	// Err is why it failed or is pending.
	Err error
}

// UploadRun is an upload run kept in the state directory.
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
	// Expired: untouched for longer than RunExpiry; its files were not uploaded, and only CleanUploads removes it.
	Expired bool
	// InUse: a live process (this one included) is working on it.
	InUse bool
}

// RunOptions are a run's choices.
type RunOptions struct {
	// ContentType is every file's (default: guessed from each file's name).
	ContentType string
	Metadata    map[string]string
	// IfNoneMatch writes each object only if none is at its key.
	IfNoneMatch bool
	// NoJournal keeps the run in memory: nothing on disk, and nothing to resume after a crash.
	NoJournal bool
	// PartSize is the part size of files sent in parts (0: the default).
	PartSize int64
	// Progress is told of the bytes saved so far, and the total.
	Progress func(done, total int64)
}

// runRow is a run's own row.
type runRow struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Bucket      string `json:"bucket"`
	Created     int64  `json:"created"`
	Updated     int64  `json:"updated"`
	Status      string `json:"status"`
	IfNoneMatch bool   `json:"if_none_match,omitempty"`
}

// fileRow is one file of a run.
type fileRow struct {
	ID          string            `json:"_id"`
	Path        string            `json:"path"`
	Key         string            `json:"key"`
	Size        int64             `json:"size"`
	Mtime       string            `json:"mtime"`
	SHA256      string            `json:"sha256"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	IfNoneMatch bool              `json:"if_none_match,omitempty"`
	IfMatch     string            `json:"if_match,omitempty"`
	State       string            `json:"state"`
	Pack        string            `json:"pack,omitempty"`
	ObjectID    string            `json:"object_id,omitempty"`
	Error       string            `json:"error,omitempty"`
	Result      *Object           `json:"result,omitempty"`
}

// packRow is a pack of a run.
type packRow struct {
	ID          string       `json:"_id"`
	Idem        string       `json:"idempotency_key"`
	State       string       `json:"state"`
	Reservation *reservation `json:"reservation,omitempty"`
	FileName    string       `json:"file_name,omitempty"`
	Size        int64        `json:"size,omitempty"`
	MD5         string       `json:"md5,omitempty"`
	SHA256      string       `json:"sha256,omitempty"`
	SignBody    *signBody    `json:"sign_body,omitempty"`
	Signed      *signedPut   `json:"signed,omitempty"`
	Attempts    int          `json:"attempts,omitempty"`
	LastError   string       `json:"last_error,omitempty"`
	Updated     int64        `json:"updated"`
}

// runStore is a run's packs in its journal, and their bytes in its directory (or in memory, for a run journaled in
// memory).
type runStore struct {
	u     *Uploads
	j     *journal.Journal
	place *journal.RunDir // nil: in memory
}

func (s *runStore) touch() []journal.Entry {
	var run runRow
	if ok, _ := s.j.Get("run", "run", &run); !ok {
		return nil
	}
	run.Updated = time.Now().UnixMilli()
	return []journal.Entry{{Table: "run", ID: "run", Row: run}}
}

func (s *runStore) savePack(p *pack) error {
	row := packRow{ID: p.id, Idem: p.idem, State: p.state, FileName: p.fileName, Size: p.size, MD5: p.md5,
		SHA256: p.sha256, Attempts: p.attempts, LastError: p.lastError, Updated: time.Now().UnixMilli()}
	if p.res.Pack != "" || p.res.AccountKey.AccountID != "" {
		r := p.res
		row.Reservation = &r
	}
	if p.body.SHA256 != "" {
		b := p.body
		row.SignBody = &b
	}
	if p.signed.URL != "" {
		sp := p.signed
		row.Signed = &sp
	}
	entries := []journal.Entry{{Table: "packs", ID: p.id, Row: row}}
	var marked []*packItem
	for _, it := range p.items {
		mark := p.id + "|" + it.objectID
		if it.fileID == "" || it.settled || it.journaled == mark {
			continue
		}
		var f fileRow
		if ok, _ := s.j.Get("files", it.fileID, &f); !ok {
			continue
		}
		f.Pack, f.ObjectID, f.State = p.id, it.objectID, "packed"
		entries = append(entries, journal.Entry{Table: "files", ID: it.fileID, Row: f})
		marked = append(marked, it)
	}
	entries = append(entries, s.touch()...)
	if err := s.j.Write(entries...); err != nil {
		return &stateError{err}
	}
	for _, it := range marked {
		it.journaled = p.id + "|" + it.objectID
	}
	s.u.saved("pack", p.state)
	return nil
}

func (s *runStore) putBytes(p *pack, data []byte) error {
	if s.place == nil {
		p.data = data
		return nil
	}
	p.fileName = p.id + ".pack"
	if err := s.place.WritePack(p.fileName, data); err != nil {
		return &stateError{err}
	}
	return nil
}

func (s *runStore) getBytes(p *pack) []byte {
	if s.place == nil {
		return p.data
	}
	if p.fileName == "" {
		return nil
	}
	return s.place.ReadPack(p.fileName)
}

func (s *runStore) dropBytes(p *pack) {
	p.data = nil
	if s.place != nil && p.fileName != "" {
		_ = s.place.RemovePack(p.fileName)
		p.fileName = ""
	}
}

func (s *runStore) itemsDone(done []doneItem) error {
	var entries []journal.Entry
	for _, d := range done {
		setDone(d)
		if d.it.fileID == "" {
			continue
		}
		var f fileRow
		if ok, _ := s.j.Get("files", d.it.fileID, &f); !ok {
			continue
		}
		switch {
		case d.obj != nil:
			f.State, f.ObjectID, f.Result, f.Error = StatusSaved, d.obj.ID, d.obj, ""
		case d.final:
			f.State, f.Error = StatusFailed, describe(d.err)
		default:
			f.Error = describe(d.err)
		}
		entries = append(entries, journal.Entry{Table: "files", ID: d.it.fileID, Row: f})
	}
	if err := s.j.Write(entries...); err != nil {
		return &stateError{err}
	}
	return nil
}

// runState is a run between its steps: begun (journaled and hashed), its empty files put, then sent and finished.
type runState struct {
	bucket string
	store  *runStore
	items  []*packItem
	packs  []*pack
	// results already known, by file id.
	results map[string]UploadResult
}

// runCloser decides who releases an interrupted run: whichever ends last, the run's own step (once it has stopped)
// or its last pack goroutine. A run that isn't interrupted is finished by its own step as usual, and this never
// fires.
type runCloser struct {
	mu      sync.Mutex
	live    int
	stopped bool
	fired   bool
}

func (c *runCloser) started() {
	c.mu.Lock()
	c.live++
	c.mu.Unlock()
}

// ended reports, for the one call that should, that the run is to be released: a goroutine ended (taskDone), or the
// run's step stopped.
func (c *runCloser) ended(taskDone bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if taskDone {
		c.live--
	} else {
		c.stopped = true
	}
	if c.stopped && c.live == 0 && !c.fired {
		c.fired = true
		return true
	}
	return false
}

// UploadFiles uploads files as one run, and returns one result per file, in order: saved, failed (with its error), or
// pending (kept for ResumeUploads). One file's failure never fails another, and returns no error: only what stops
// the whole run does (a revoked key, no permission, no credit: with the results so far), the state directory being
// unwritable, or a cancel.
func (u *Uploads) UploadFiles(ctx context.Context, bucket string, files []RunFile, o RunOptions) ([]UploadResult, error) {
	state, err := u.beginFiles(ctx, bucket, files, o)
	if err != nil {
		return nil, err
	}
	return u.runState(ctx, state, o.Progress, o.PartSize)
}

// fileHash is a file's size, modification time and SHA-256 when it was queued.
type fileHash struct {
	size   int64
	mtime  string
	sha256 string
}

// hashFile is a file's size, modification time and SHA-256; a file that changes while it is read is a changed file.
func (u *Uploads) hashFile(path string) (fileHash, error) {
	defer func() {
		if h := u.Hooks.Hashed; h != nil {
			h(path)
		}
	}()
	st, err := os.Stat(path)
	if err != nil {
		return fileHash{}, errs.Changed(path, err.Error())
	}
	if !st.Mode().IsRegular() {
		return fileHash{}, errs.Changed(path, "it is not a file")
	}
	f, err := os.Open(path)
	if err != nil {
		return fileHash{}, errs.Changed(path, err.Error())
	}
	h := sha256.New()
	_, err = io.Copy(h, f)
	f.Close()
	if err != nil {
		return fileHash{}, errs.Changed(path, err.Error())
	}
	st2, err := os.Stat(path)
	if err != nil {
		return fileHash{}, errs.Changed(path, err.Error())
	}
	if st2.Size() != st.Size() || !st2.ModTime().Equal(st.ModTime()) {
		return fileHash{}, errs.Changed(path, "it changed while it was read")
	}
	return fileHash{size: st.Size(), mtime: strconv.FormatInt(st.ModTime().UnixNano(), 10),
		sha256: hex.EncodeToString(h.Sum(nil))}, nil
}

// readWhole reads a small file whole, as up to size + 1 bytes, so a file that grew shows it.
func readWhole(path string, size int64) ([]byte, error) {
	return readAt(path, 0, size+1)
}

// readAt reads n bytes of a file from off (fewer at its end).
func readAt(path string, off, n int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, n)
	got, err := f.ReadAt(buf, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:got], nil
}

// ContentTypeOf is a content type guessed from a file's name ("" when there's none).
func ContentTypeOf(path string) string { return mime.TypeByExtension(filepath.Ext(path)) }

// beginFiles is a run's first step: journaled (on disk unless NoJournal), and every file hashed.
func (u *Uploads) beginFiles(ctx context.Context, bucket string, files []RunFile, o RunOptions) (*runState, error) {
	store := &runStore{u: u}
	id := UUID7()
	if o.NoJournal {
		store.j = journal.Memory()
	} else {
		place, err := journal.CreateRun(u.stateDir, journal.Uploads)
		if err != nil {
			return nil, err
		}
		store.place, store.j, id = place, place.Journal(), place.ID
	}
	now := time.Now().UnixMilli()
	state, err := func() (*runState, error) {
		if err := store.j.Put("run", "run", runRow{ID: id, Kind: "upload", Bucket: bucket, Created: now, Updated: now,
			Status: "open", IfNoneMatch: o.IfNoneMatch}); err != nil {
			return nil, &stateError{err}
		}
		hashed := make([]fileHash, len(files))
		failed := make([]error, len(files))
		idx := make([]int, len(files))
		for i := range idx {
			idx[i] = i
		}
		if err := RunPool(ctx, u.Limit.Ceiling(), idx, func(_ context.Context, i int) error {
			hashed[i], failed[i] = u.hashFile(files[i].Path)
			return nil
		}); err != nil {
			return nil, err
		}
		return u.journalFiles(bucket, store, files, hashed, failed, o)
	}()
	if err != nil {
		u.abandon(store) // nothing is under way
		return nil, Stopped(ctx, err)
	}
	return state, nil
}

func (u *Uploads) journalFiles(bucket string, store *runStore, files []RunFile, hashed []fileHash, failed []error,
	o RunOptions) (*runState, error) {
	state := &runState{bucket: bucket, store: store, results: map[string]UploadResult{}}
	var entries []journal.Entry
	for i, f := range files {
		id := store.j.NextID("files")
		if failed[i] != nil {
			entries = append(entries, journal.Entry{Table: "files", ID: id, Row: fileRow{ID: id, Path: f.Path, Key: f.Key,
				Size: -1, State: StatusFailed, Error: describe(failed[i])}})
			state.results[id] = UploadResult{Key: f.Key, Path: f.Path, Status: StatusFailed, Err: failed[i]}
			continue
		}
		h := hashed[i]
		ct := o.ContentType
		if ct == "" {
			ct = ContentTypeOf(f.Path)
		}
		md := cloneMap(o.Metadata)
		entries = append(entries, journal.Entry{Table: "files", ID: id, Row: fileRow{ID: id, Path: f.Path, Key: f.Key,
			Size: h.size, Mtime: h.mtime, SHA256: h.sha256, ContentType: ct, Metadata: md, IfNoneMatch: o.IfNoneMatch,
			State: "queued"}})
		state.items = append(state.items, fileItem(f.Key, f.Path, h.size, h.sha256, ct, md, o.IfNoneMatch, id))
	}
	entries = append(entries, store.touch()...)
	if err := store.j.Write(entries...); err != nil {
		return nil, &stateError{err}
	}
	return state, nil
}

func fileItem(key, path string, size int64, sha, contentType string, metadata map[string]string, ifNoneMatch bool,
	fileID string) *packItem {
	return &packItem{key: key, size: size, sha256: sha, read: func() ([]byte, error) { return readWhole(path, size) },
		contentType: contentType, metadata: metadata, ifNoneMatch: ifNoneMatch, path: path, fileID: fileID, final: true}
}

// runState is a run's middle and last steps: its empty files put (the A2 route), then its queued items packed, its
// packs already under way and its large files sent, every file's result recorded and the run finished.
func (u *Uploads) runState(ctx context.Context, state *runState, progress func(done, total int64), partSize int64) ([]UploadResult, error) {
	for _, it := range state.items {
		if it.size != 0 {
			continue
		}
		obj, err := u.PutEmpty(ctx, state.bucket, it.key, WriteOptions{ContentType: it.contentType, Metadata: it.metadata,
			IfNoneMatch: it.ifNoneMatch})
		if ctx.Err() != nil {
			u.abandon(state.store) // interrupted before any pack started
			return nil, ctx.Err()
		}
		if derr := u.done(state.store, doneItem{it: it, obj: obj, err: err, final: err == nil || emptyPutFinal(err)}); derr != nil {
			u.abandon(state.store)
			return nil, derr
		}
	}
	// The empty files stay in state.items, so sendRun reports them with the rest (it packs only sizes above 0).
	res, err := u.sendRun(ctx, state, progress, partSize)
	for i := range res {
		res[i].Bucket = state.bucket
	}
	return res, err
}

// emptyPutFinal reports whether an empty file's failed put is final. A passing failure (the network, a busy server)
// or one that stops the run (a revoked key, no credit) leaves it queued in the journal for ResumeUploads, as a
// pack's files are left.
func emptyPutFinal(err error) bool { return !(IsPassing(err) || stopsRun(err)) }

func (u *Uploads) sendRun(ctx context.Context, state *runState, progress func(done, total int64), partSize int64) ([]UploadResult, error) {
	store := state.store
	lim := u.PackObjectLimit()
	// A file whose upload in parts a crash left open goes on in parts, whatever the speed now (section 22.7).
	inParts := map[string]bool{}
	for _, raw := range store.j.Rows("uploads") {
		var r uploadRow
		if json.Unmarshal(raw, &r) == nil && r.FileID != "" && r.State != "completed" && r.State != "failed" {
			inParts[r.FileID] = true
		}
	}
	var small, big []*packItem
	for _, it := range state.items {
		switch {
		case it.size > lim || (it.size > 0 && it.fileID != "" && inParts[it.fileID]):
			big = append(big, it)
		case it.size > 0:
			small = append(small, it)
		}
	}
	// The small files are packed as slots come free, each pack at the speed measured by then (section 22.7).
	packs := slices.Clone(state.packs)
	var total int64
	for _, p := range packs {
		for _, it := range p.items {
			total += it.size
		}
	}
	for _, it := range small {
		total += it.size
	}
	for _, it := range big {
		total += it.size
	}
	prog := NewProgress(total, progress)
	closer := &runCloser{}
	// The run's goroutines stop when the caller cancels, or when this step fails.
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu      sync.Mutex
		stopErr error // what stops the run: a revoked key, no permission, no credit
		failure error // a goroutine's failure that stops the run at once (its state unwritable)
		wg      sync.WaitGroup
	)
	stopped := func() error {
		mu.Lock()
		defer mu.Unlock()
		return stopErr
	}
	failed := func(err error) {
		mu.Lock()
		if failure == nil {
			failure = err
		}
		mu.Unlock()
		cancel()
	}
	packTask := func(p *pack) {
		defer wg.Done()
		defer func() {
			if closer.ended(true) {
				u.abandon(store) // the last goroutine of an interrupted run releases it
			}
		}()
		outcome := OutcomeFailed
		defer func() { u.release(outcome) }()
		if se := stopped(); se != nil {
			if err := u.fail(p, se, false); err != nil {
				failed(err)
			}
			return
		}
		err := u.drive(rctx, p)
		if err == nil {
			outcome = outcomeOf(p)
			for _, it := range p.items {
				if it.result != nil {
					prog.Add(it.size)
				}
			}
			return
		}
		if rctx.Err() != nil {
			return
		}
		if stopsRun(err) {
			mu.Lock()
			if stopErr == nil {
				stopErr = err
			}
			mu.Unlock()
			return
		}
		failed(err)
	}
	interrupted := func() error {
		mu.Lock()
		f := failure
		mu.Unlock()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return f
	}

	var stopNow error
	// slot takes a send slot for the next pack; false (stopNow set) when the run stops instead.
	slot := func() bool {
		if err := u.Limit.Acquire(rctx); err != nil {
			stopNow = Stopped(ctx, err)
			return false
		}
		if f := interrupted(); f != nil {
			u.release(OutcomeFailed)
			stopNow = f
			return false
		}
		return true
	}
	start := func(p *pack) {
		closer.started()
		wg.Add(1)
		go packTask(p)
	}
	for _, p := range slices.Clone(packs) {
		if !slot() {
			break
		}
		start(p)
	}
	for rest := small; stopNow == nil && len(rest) > 0; {
		if !slot() {
			break
		}
		var chunk []*packItem
		chunk, rest = NextPack(rest, func(i *packItem) int64 { return i.size }, func(i *packItem) string { return i.key },
			u.PackTarget(), u.Sizes.PackMaxObjects)
		p := newPack(state.bucket, chunk, store, "")
		packs = append(packs, p)
		start(p)
	}
	if stopNow == nil {
		for _, it := range big {
			if se := stopped(); se != nil {
				if err := u.done(store, doneItem{it: it, err: se, final: false}); err != nil {
					stopNow = err
					break
				}
				continue
			}
			obj, err := u.largeFile(rctx, state.bucket, it, store, partSize, prog)
			if rctx.Err() != nil {
				stopNow = interrupted()
				if stopNow == nil {
					stopNow = rctx.Err()
				}
				break
			}
			var se *stateError
			var d doneItem
			switch {
			case err == nil:
				d = doneItem{it: it, obj: obj, final: true}
			case errors.As(err, &se):
				stopNow = err
			case stopsRun(err):
				d = doneItem{it: it, err: err, final: false}
				mu.Lock()
				if stopErr == nil {
					stopErr = err
				}
				mu.Unlock()
			case isUnavailable(err):
				d = doneItem{it: it, err: err, final: false}
			default:
				d = doneItem{it: it, err: err, final: true}
			}
			if stopNow != nil {
				break
			}
			if derr := u.done(store, d); derr != nil {
				stopNow = derr
				break
			}
		}
	}
	if stopNow == nil {
		// A goroutine that fails stops the run at once (its context is cancelled), as joining it first would.
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
			stopNow = interrupted()
		case <-rctx.Done():
			stopNow = interrupted()
			if stopNow == nil {
				stopNow = rctx.Err()
			}
		}
	}
	if stopNow != nil {
		// Interrupted, or failed: every pack still under way is stopped and waited for (up to the cap); then the run's
		// journal is closed and its lock released, so ResumeUploads in this same process can finish it. Never while a
		// pack might still commit (it could be saved twice): whichever ends last, this step or the last pack
		// goroutine, releases it.
		cancel()
		WaitCapped(&wg, u.CleanupCap)
		if closer.ended(false) {
			u.abandon(store)
		}
		return nil, stopNow
	}
	seen := map[*packItem]bool{}
	every := slices.Clone(state.items)
	for _, it := range every {
		seen[it] = true
	}
	for _, p := range packs {
		for _, it := range p.items {
			if !seen[it] {
				every = append(every, it)
				seen[it] = true
			}
		}
	}
	results := state.results
	for _, it := range every {
		if it.fileID == "" {
			continue
		}
		if _, ok := results[it.fileID]; ok {
			continue
		}
		// pending: a passing error (storage down, a revoked key) left it journaled for ResumeUploads; failed is final.
		status := StatusPending
		switch {
		case it.result != nil:
			status = StatusSaved
		case it.final:
			status = StatusFailed
		}
		results[it.fileID] = UploadResult{Key: it.key, Path: it.path, Status: status, Object: it.result, Err: it.err}
	}
	ferr := u.finish(store)
	out := sortedResults(results)
	if se := stopped(); se != nil {
		return out, se
	}
	return out, ferr
}

// sortedResults are results in their files' order.
func sortedResults(m map[string]UploadResult) []UploadResult {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, _ := strconv.Atoi(ids[i])
		b, _ := strconv.Atoi(ids[j])
		return a < b
	})
	out := make([]UploadResult, 0, len(ids))
	for _, id := range ids {
		out = append(out, m[id])
	}
	return out
}

// largeFile sends a large file of a run: the upload its journal left unfinished, or a new one.
func (u *Uploads) largeFile(ctx context.Context, bucket string, it *packItem, store *runStore, partSize int64, prog *Progress) (*Object, error) {
	l, err := u.largeItem(bucket, it, store, partSize)
	if err != nil {
		return nil, err
	}
	return u.runLarge(ctx, l, prog)
}

func (u *Uploads) largeItem(bucket string, it *packItem, store *runStore, partSize int64) (*large, error) {
	path := it.path
	read := func(off, n int64) ([]byte, error) { return readAt(path, off, n) }
	w := WriteOptions{ContentType: it.contentType, Metadata: it.metadata, IfNoneMatch: it.ifNoneMatch, IfMatch: it.ifMatch}
	var open []uploadRow
	for _, raw := range store.j.Rows("uploads") {
		var r uploadRow
		if json.Unmarshal(raw, &r) == nil && r.FileID == it.fileID && r.State != "completed" && r.State != "failed" {
			open = append(open, r)
		}
	}
	sort.SliceStable(open, func(i, j int) bool { return open[i].Updated > open[j].Updated })
	if len(open) > 0 {
		l, err := largeFromRow(open[0], read, w, store, path)
		if err != nil {
			return nil, err
		}
		if l.state == "new" && len(l.plain) == 0 {
			// Never created, so no part hashes were journaled: take them now, against the queued hash.
			whole, parts, err := hashFileParts(path, it.size, l.partSize)
			if err != nil {
				return nil, err
			}
			if whole != it.sha256 {
				return nil, errs.Changed(path, "")
			}
			l.plain = parts
		}
		return l, nil
	}
	psize, err := u.partSize(it.size, partSize, it.key)
	if err != nil {
		return nil, err
	}
	whole, parts, err := hashFileParts(path, it.size, psize)
	if err != nil {
		return nil, err
	}
	if whole != it.sha256 {
		return nil, errs.Changed(path, "")
	}
	l, err := newLarge(bucket, it.key, it.size, it.sha256, read, w, psize, parts)
	if err != nil {
		return nil, err
	}
	l.store, l.fileID, l.path = store, it.fileID, path
	return l, nil
}

// hashFileParts takes a file's whole and part hashes in one pass; a file that can't be read the same way is a changed
// file.
func hashFileParts(path string, size, partSize int64) (string, map[int]string, error) {
	layout, err := tws1.Parts(size, partSize)
	if err != nil {
		return "", nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", nil, errs.Changed(path, err.Error())
	}
	defer f.Close()
	whole := sha256.New()
	parts := make(map[int]string, len(layout))
	for _, p := range layout {
		h := sha256.New()
		n, err := io.Copy(io.MultiWriter(whole, h), io.NewSectionReader(f, p.PlainFirst, p.PlainSize))
		if err != nil {
			return "", nil, errs.Changed(path, err.Error())
		}
		if n != p.PlainSize {
			return "", nil, errs.Changed(path, "it changed size while it was read")
		}
		parts[p.Number] = hex.EncodeToString(h.Sum(nil))
	}
	return hex.EncodeToString(whole.Sum(nil)), parts, nil
}

// abandon leaves an interrupted run for ResumeUploads: its journal closed and its lock released (its directory
// stays). Safe to call twice.
func (u *Uploads) abandon(store *runStore) {
	if store.place != nil {
		_ = store.place.Release()
		return
	}
	_ = store.j.Close()
}

// countStates counts a run's files by state.
func countStates(j *journal.Journal) map[string]int {
	out := map[string]int{}
	for _, raw := range j.Rows("files") {
		var f fileRow
		if json.Unmarshal(raw, &f) == nil {
			out[f.State]++
		}
	}
	return out
}

// finish ends a run: one with files still pending stays, released, for ResumeUploads; one that's done is deleted,
// with a line in the history.
func (u *Uploads) finish(store *runStore) error {
	j := store.j
	counts := countStates(j)
	var run runRow
	_, _ = j.Get("run", "run", &run)
	if counts["queued"]+counts["packed"] > 0 {
		run.Updated, run.Status = time.Now().UnixMilli(), "open"
		err := j.Put("run", "run", run)
		u.abandon(store)
		if err != nil {
			return &stateError{err}
		}
		return nil
	}
	_ = j.Close()
	if store.place == nil {
		return nil
	}
	id := run.ID
	if id == "" {
		id = store.place.ID
	}
	herr := journal.AppendHistory(u.stateDir, journal.Uploads, fmt.Sprintf("run %s bucket=%s saved=%d failed=%d", id,
		run.Bucket, counts[StatusSaved], counts[StatusFailed]))
	rerr := store.place.Remove()
	warnAt := int64(StateWarnBytes)
	if v, err := strconv.ParseInt(os.Getenv(StateWarnEnv), 10, 64); err == nil && v > 0 {
		warnAt = v
	}
	if size := journal.DirectorySize(filepath.Join(u.stateDir, journal.Uploads)); size > warnAt {
		u.warn(fmt.Sprintf("teepin: the upload state directory holds %d bytes; CleanUploads removes old runs", size))
	}
	if rerr != nil {
		return &stateError{rerr}
	}
	if herr != nil {
		return &stateError{herr}
	}
	return nil
}

// --- resume, listing and cleanup ----------------------------------------------------------------------------------

// ResumeUploads continues every interrupted run no live process (this one included) is working on, and returns their
// files' results. It locks every run up front; the runs it hasn't started are released on any exit (profile 1's
// R-1), a cancel included, so a later call finishes them.
func (u *Uploads) ResumeUploads(ctx context.Context, progress func(done, total int64)) ([]UploadResult, error) {
	ids, err := journal.RunIDs(u.stateDir, journal.Uploads)
	if err != nil {
		return nil, err
	}
	var locked []*journal.RunDir
	defer func() {
		for _, rd := range locked { // those not started, left for the next ResumeUploads
			_ = rd.Release()
		}
	}()
	for _, id := range ids {
		rd, err := journal.LockRun(u.stateDir, journal.Uploads, id)
		if err != nil {
			u.warn(fmt.Sprintf("teepin: upload run %s can't be resumed: %v", id, err))
			continue
		}
		if rd != nil {
			locked = append(locked, rd)
		}
	}
	var out []UploadResult
	for len(locked) > 0 {
		rd := locked[0]
		locked = locked[1:]
		if err := ctx.Err(); err != nil {
			_ = rd.Release()
			return out, err
		}
		if err := rd.Open(); err != nil {
			// A damaged journal: never skipped silently, and never in the way of the other runs.
			u.warn(fmt.Sprintf("teepin: upload run %s can't be resumed: %v", rd.ID, err))
			_ = rd.Release()
			continue
		}
		state, err := u.beginResume(ctx, rd)
		if err != nil {
			_ = rd.Release() // nothing was sent yet
			return out, Stopped(ctx, err)
		}
		// From here an interrupt releases the run when its last pack ends (sendRun).
		res, err := u.runState(ctx, state, progress, 0)
		out = append(out, res...)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

// beginResume is a run's first step on resume: its journal read back, its queued files checked again (a changed one
// fails, and its large upload is cancelled).
func (u *Uploads) beginResume(ctx context.Context, rd *journal.RunDir) (*runState, error) {
	store := &runStore{u: u, j: rd.Journal(), place: rd}
	state, changed, err := u.readRun(store)
	if err != nil {
		return nil, err
	}
	for _, c := range changed {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		u.endUploads(ctx, store, c.it.fileID) // its large upload, if any: cancelled, its key deleted
		ferr := errs.Changed(c.it.path, "")
		if err := u.done(store, doneItem{it: c.it, err: ferr, final: true}); err != nil {
			return nil, err
		}
		state.results[c.it.fileID] = UploadResult{Key: c.it.key, Path: c.it.path, Status: StatusFailed, Err: ferr}
	}
	return state, nil
}

func (u *Uploads) readRun(store *runStore) (*runState, []doneItem, error) {
	j := store.j
	var run runRow
	_, _ = j.Get("run", "run", &run)
	state := &runState{bucket: run.Bucket, store: store, results: map[string]UploadResult{}}
	byPack := map[string][]*packItem{}
	var packOrder []string
	var changed []doneItem
	for _, raw := range j.Rows("files") {
		var f fileRow
		if err := json.Unmarshal(raw, &f); err != nil {
			return nil, nil, &stateError{err}
		}
		switch f.State {
		case StatusSaved:
			state.results[f.ID] = UploadResult{Key: f.Key, Path: f.Path, Status: StatusSaved, Object: f.Result}
			continue
		case StatusFailed:
			state.results[f.ID] = UploadResult{Key: f.Key, Path: f.Path, Status: StatusFailed, Err: &recorded{f.Error}}
			continue
		}
		it := fileItem(f.Key, f.Path, f.Size, f.SHA256, f.ContentType, f.Metadata, f.IfNoneMatch, f.ID)
		it.ifMatch = f.IfMatch
		it.objectID = f.ObjectID
		if f.State == "packed" && f.Pack != "" {
			it.journaled = f.Pack + "|" + it.objectID
			if _, ok := byPack[f.Pack]; !ok {
				packOrder = append(packOrder, f.Pack)
			}
			byPack[f.Pack] = append(byPack[f.Pack], it)
			continue
		}
		// Queued: checked again as it's queued again (section 17.5); a change fails it.
		h, err := u.hashFile(f.Path)
		if err != nil || h.size != f.Size || h.sha256 != f.SHA256 {
			changed = append(changed, doneItem{it: it})
			continue
		}
		state.items = append(state.items, it)
	}
	for _, raw := range j.Rows("packs") {
		var r packRow
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, nil, &stateError{err}
		}
		if r.State == "committed" || r.State == "failed" {
			continue
		}
		its := byPack[r.ID]
		delete(byPack, r.ID)
		if len(its) == 0 {
			continue
		}
		p := newPack(state.bucket, its, store, r.ID)
		p.state, p.idem = r.State, r.Idem
		if p.state == "" {
			p.state = "new"
		}
		if r.Reservation != nil {
			p.res = *r.Reservation
		}
		if r.SignBody != nil {
			p.body = *r.SignBody
		}
		if r.Signed != nil {
			p.signed = *r.Signed
		}
		p.fileName, p.size, p.md5, p.sha256, p.attempts = r.FileName, r.Size, r.MD5, r.SHA256, r.Attempts
		if p.state == "uploading" {
			p.state = "unknown" // the PUT may have landed: commit first
		}
		state.packs = append(state.packs, p)
	}
	for _, id := range packOrder { // packed into a pack that's gone: queue them again
		state.items = append(state.items, byPack[id]...)
	}
	u.sweepOrphans(store, state.packs)
	return state, changed, nil
}

// sweepOrphans deletes the pack files the journal doesn't name, and the data keys of uploads that have ended
// (section 17.12, E3).
func (u *Uploads) sweepOrphans(store *runStore, packs []*pack) {
	rd := store.place
	keepPacks := map[string]bool{}
	for _, p := range packs {
		if p.fileName != "" {
			keepPacks[p.fileName] = true
		}
	}
	for _, name := range rd.PackNames() {
		if !keepPacks[name] {
			_ = rd.RemovePack(name)
		}
	}
	keepKeys := map[string]bool{}
	for _, raw := range store.j.Rows("uploads") {
		var r uploadRow
		if json.Unmarshal(raw, &r) == nil && openUpload[r.State] {
			keepKeys[r.ID] = true
		}
	}
	for _, id := range rd.KeyNames() {
		if !keepKeys[id] {
			_ = rd.RemoveKey(id)
		}
	}
}

// endUploads cancels a file's unfinished large uploads at the storage network and deletes their journaled data keys
// (E3): a file that won't be sent must not leave parts behind to be kept, and billed, until they expire. One call
// each, never the control plane's rounds of waiting.
func (u *Uploads) endUploads(ctx context.Context, store *runStore, fileID string) {
	for _, raw := range store.j.Rows("uploads") {
		var r uploadRow
		if json.Unmarshal(raw, &r) != nil || r.FileID != fileID || !openUpload[r.State] {
			continue
		}
		u.abortUpload(ctx, r.Bucket, r.UploadID, false)
		r.State, r.Updated = "failed", time.Now().UnixMilli()
		_ = store.j.Put("uploads", r.ID, r)
		if store.place != nil {
			_ = store.place.RemoveKey(r.ID)
		}
	}
}

// ListRuns is the upload runs kept in the state directory. A run whose journal can't be read is left out.
func (u *Uploads) ListRuns() ([]UploadRun, error) {
	ids, err := journal.RunIDs(u.stateDir, journal.Uploads)
	if err != nil {
		return nil, err
	}
	out := []UploadRun{}
	now := time.Now()
	for _, id := range ids {
		j, inUse, err := journal.PeekRun(u.stateDir, journal.Uploads, id)
		if err != nil {
			continue // a damaged journal is left out, not fatal
		}
		var run runRow
		if ok, err := j.Get("run", "run", &run); !ok || err != nil {
			continue
		}
		counts := countStates(j)
		files := 0
		for _, n := range counts {
			files += n
		}
		updated := time.UnixMilli(run.Updated)
		rid := run.ID
		if rid == "" {
			rid = id
		}
		out = append(out, UploadRun{ID: rid, Bucket: run.Bucket, Created: time.UnixMilli(run.Created), Updated: updated,
			Files: files, Saved: counts[StatusSaved], Failed: counts[StatusFailed],
			Pending: counts["queued"] + counts["packed"], Expired: now.Sub(updated) > RunExpiry, InUse: inUse})
	}
	return out, nil
}

// Clean removes runs untouched for longer than olderThan that no process is working on, with a warning for each:
// never silently. Their open large uploads are cancelled (one call each), and their data keys go with their journals
// (E3). It then trims the history to 7 days (G-3).
func (u *Uploads) Clean(ctx context.Context, olderThan time.Duration) ([]UploadRun, error) {
	runs, err := u.ListRuns()
	if err != nil {
		return nil, err
	}
	removed := []UploadRun{}
	cutoff := time.Now().Add(-olderThan)
	for _, r := range runs {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if r.InUse || r.Updated.After(cutoff) {
			continue
		}
		rd, err := journal.TakeRun(u.stateDir, journal.Uploads, r.ID)
		if err != nil || rd == nil {
			continue
		}
		for _, raw := range rd.Journal().Rows("uploads") {
			var up uploadRow
			if json.Unmarshal(raw, &up) == nil && up.UploadID != "" && openUpload[up.State] {
				u.abortUpload(ctx, up.Bucket, up.UploadID, false)
			}
		}
		u.warn(fmt.Sprintf("teepin: removing upload run %s (bucket %s): %d files not uploaded, untouched since %s",
			r.ID, r.Bucket, r.Pending, r.Updated.UTC().Format(time.RFC3339)))
		if err := rd.Remove(); err != nil {
			_ = rd.Release()
			return removed, err
		}
		removed = append(removed, r)
	}
	if err := journal.TrimHistory(u.stateDir, journal.Uploads); err != nil {
		return removed, err
	}
	return removed, nil
}
