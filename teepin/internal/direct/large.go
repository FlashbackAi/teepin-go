// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
	"github.com/FlashbackAi/teepin-go/teepin/internal/journal"
	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// Large objects, uploaded directly in parts (TEEPIN-STORAGE-DESIGN.md sections 21.1 D1, 21.2 and 22.4), a port of the
// Python SDK's _large.py by way of the Node SDK's large.ts, with the same rules.
//
// One TWS1 stream per object, cut into parts at segment boundaries. The control plane starts the storage network's
// upload, signs each part (its first sign pins the part's MD5 and SHA-256), and completes it with the wrapped data
// key. An upload goes through these states, each journaled before its action:
//
//	new → sending → completing → completed
//
//   - new: its idempotency key is journaled, so a lost create answer gets the same upload again.
//   - sending: the upload exists, and its data key (E3: a file of its own, 0600, in the run's directory), stream
//     header and wrap are journaled, so a crash resumes the remaining parts.
//   - Each part's plaintext SHA-256 is journaled before its first byte leaves (D1 condition a): all of them at create,
//     taken in the same pass as the whole object's. A part sealed again is read and hashed again first (condition b):
//     a changed file fails the upload, never mixes two versions.
//   - completing: every part is sent. Complete is idempotent, so a lost answer is asked for again.
//
// An upload the storage network or control plane ended (expired, mismatched) starts again under a new create with a
// fresh key. A final failure cancels it, so its parts aren't kept (and billed, D5) for nothing.
//
// A stream whose size isn't known (section 22.1, E2 b) is created without its size and SHA-256, read one part ahead
// (TWS1's last flag needs to know which part is last), its last part declared at its sign, and the size and SHA-256
// brought by complete. Nothing can read it again, so it isn't journaled: any failure cancels it, and the caller
// uploads it again. Memory: each part in flight holds only its ciphertext, handed to its goroutine in a box the
// goroutine empties (section 22.12's rule), and the reader holds the part it hands over, the next, and one more while
// it seals a part: at most concurrency + 3 parts.

// openUpload are the states of a large upload that may still have parts at the storage network.
var openUpload = map[string]bool{"new": true, "sending": true, "completing": true}

// uploadRow is a large upload as its run's journal keeps it.
type uploadRow struct {
	ID         string `json:"_id"`
	FileID     string `json:"file_id,omitempty"`
	Bucket     string `json:"bucket"`
	Key        string `json:"key"`
	Idem       string `json:"idempotency_key"`
	UploadID   string `json:"upload_id"`
	State      string `json:"state"`
	Size       int64  `json:"size"`
	PartSize   int64  `json:"part_size"`
	SHA256     string `json:"sha256"`
	Header     string `json:"header,omitempty"`
	Wrapped    string `json:"wrapped_key,omitempty"`
	KeyVersion int    `json:"key_version,omitempty"`
	LastError  string `json:"last_error,omitempty"`
	Updated    int64  `json:"updated"`
}

// partRow is a part of a large upload as its run's journal keeps it.
type partRow struct {
	ID          string `json:"_id"`
	Upload      string `json:"upload"`
	Number      int    `json:"number"`
	PlainSHA256 string `json:"plain_sha256"`
	MD5         string `json:"md5,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	State       string `json:"state"`
}

// large is one large upload's state, journaled when it has a run's store.
type large struct {
	id, idem, uploadID, state string
	bucket, key               string
	size                      int64
	sha256                    string
	readAt                    func(off, n int64) ([]byte, error)
	w                         WriteOptions
	partSize, partCount       int64
	store                     *runStore // nil: in memory
	fileID, path              string

	dek        []byte
	header     []byte
	stream     *tws1.Stream
	sealMu     sync.Mutex // the stream seals one part at a time
	wrapped    string
	keyVersion int
	keyOnDisk  bool
	resumed    bool
	restarts   int
	lastError  string
	waits

	mu    sync.Mutex
	plain map[int]string // part number: its plaintext's SHA-256 (D1 a)
}

func newLarge(bucket, key string, size int64, sha string, readAt func(off, n int64) ([]byte, error), w WriteOptions,
	partSize int64, plain map[int]string) (*large, error) {
	n, err := tws1.PartCount(size, partSize)
	if err != nil {
		return nil, err
	}
	if plain == nil {
		plain = map[int]string{}
	}
	return &large{id: UUID7(), idem: UUID7(), state: "new", bucket: bucket, key: key, size: size, sha256: sha,
		readAt: readAt, w: w, partSize: partSize, partCount: n, plain: plain}, nil
}

// durable reports whether its journal survives the process (a resumable upload).
func (l *large) durable() bool { return l.store != nil && l.store.j.Durable() }

func (l *large) source() string {
	if l.path != "" {
		return l.path
	}
	return l.key
}

func (l *large) row() uploadRow {
	r := uploadRow{ID: l.id, FileID: l.fileID, Bucket: l.bucket, Key: l.key, Idem: l.idem, UploadID: l.uploadID,
		State: l.state, Size: l.size, PartSize: l.partSize, SHA256: l.sha256, Wrapped: l.wrapped,
		KeyVersion: l.keyVersion, LastError: l.lastError, Updated: time.Now().UnixMilli()}
	if len(l.header) > 0 {
		r.Header = base64.StdEncoding.EncodeToString(l.header)
	}
	return r
}

func (l *large) partEntry(n int, plain, md5, sha, state string) journal.Entry {
	id := l.id + ":" + strconv.Itoa(n)
	return journal.Entry{Table: "parts", ID: id, Row: partRow{ID: id, Upload: l.id, Number: n, PlainSHA256: plain,
		MD5: md5, SHA256: sha, State: state}}
}

// save journals the upload's state, with its data key first (on disk, while it has one).
func (l *large) save() error {
	if l.store == nil {
		return nil
	}
	if err := l.saveKey(); err != nil {
		return err
	}
	if err := l.store.j.Put("uploads", l.id, l.row()); err != nil {
		return &stateError{err}
	}
	l.store.u.saved("large", l.state)
	return nil
}

func (l *large) savePart(n int, plain, md5, sha, state string) error {
	if l.store == nil {
		return nil
	}
	if err := l.store.j.Write(l.partEntry(n, plain, md5, sha, state)); err != nil {
		return &stateError{err}
	}
	l.store.u.saved("part", state)
	return nil
}

// saveCreated journals every part's hash before any byte leaves (D1 a), then the data key, header and wrap, before
// any part is signed.
func (l *large) saveCreated() error {
	if l.store == nil {
		return nil
	}
	if err := l.saveKey(); err != nil {
		return err
	}
	l.mu.Lock()
	nums := make([]int, 0, len(l.plain))
	for n := range l.plain {
		nums = append(nums, n)
	}
	slices.Sort(nums)
	entries := make([]journal.Entry, 0, len(nums)+1)
	for _, n := range nums {
		entries = append(entries, l.partEntry(n, l.plain[n], "", "", "hashed"))
	}
	l.mu.Unlock()
	entries = append(entries, journal.Entry{Table: "uploads", ID: l.id, Row: l.row()})
	if err := l.store.j.Write(entries...); err != nil {
		return &stateError{err}
	}
	l.store.u.saved("large", l.state)
	return nil
}

func (l *large) saveKey() error {
	if l.store == nil || l.store.place == nil || l.dek == nil || l.keyOnDisk {
		return nil
	}
	if err := l.store.place.WriteKey(l.id, l.dek); err != nil {
		return &stateError{err}
	}
	l.keyOnDisk = true
	return nil
}

// forgetKey zeroes the data key and deletes its file (E3: promptly).
func (l *large) forgetKey() {
	clear(l.dek)
	l.dek = nil
	l.stream = nil
	if l.keyOnDisk && l.store != nil && l.store.place != nil {
		l.keyOnDisk = false
		_ = l.store.place.RemoveKey(l.id)
	}
}

// reset gives the upload a new create and a fresh key. The source's part hashes stay: they still catch a file that
// changed.
func (l *large) reset() {
	l.forgetKey()
	l.idem = UUID7()
	l.uploadID, l.header, l.wrapped, l.keyVersion = "", nil, "", 0
	l.resumed = false
	l.state = "new"
}

// largeFromRow is an upload a crashed run left in its journal.
func largeFromRow(r uploadRow, readAt func(off, n int64) ([]byte, error), w WriteOptions, store *runStore, path string) (*large, error) {
	l, err := newLarge(r.Bucket, r.Key, r.Size, r.SHA256, readAt, w, r.PartSize, nil)
	if err != nil {
		return nil, err
	}
	l.id, l.idem, l.uploadID, l.state = r.ID, r.Idem, r.UploadID, r.State
	l.store, l.fileID, l.path = store, r.FileID, path
	if store.place != nil {
		if key, err := store.place.ReadKey(l.id); err == nil && len(key) == tws1.KeySize {
			l.dek, l.keyOnDisk = key, true
		}
	}
	if r.Header != "" {
		l.header, _ = base64.StdEncoding.DecodeString(r.Header)
	}
	l.wrapped, l.keyVersion = r.Wrapped, r.KeyVersion
	for _, raw := range store.j.Rows("parts") {
		var p partRow
		if json.Unmarshal(raw, &p) == nil && p.Upload == l.id {
			l.plain[p.Number] = p.PlainSHA256
		}
	}
	l.resumed = true
	return l, nil
}

// PutLarge stores an object over the pack limit from data that can be read at any offset, in parts, its state kept in
// memory: nothing could resume it after a crash. progress (nil for none) is told of the bytes saved.
func (u *Uploads) PutLarge(ctx context.Context, bucket, key string, readAt func(off, n int64) ([]byte, error), size int64,
	w WriteOptions, partSize int64, progress func(done, total int64)) (*Object, error) {
	psize, err := u.partSize(size, partSize, key)
	if err != nil {
		return nil, err
	}
	whole, parts, err := hashParts(readAt, size, psize)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var fc *errs.FileChangedError
		if !errors.As(err, &fc) {
			err = errs.Changed(key, err.Error())
		}
		return nil, err
	}
	w.IfMatch = w.ifMatch()
	l, err := newLarge(bucket, key, size, whole, readAt, w, psize, parts)
	if err != nil {
		return nil, err
	}
	return u.runLarge(ctx, l, NewProgress(size, progress))
}

// hashParts takes, in one pass, the whole source's SHA-256 (declared at create) and each part's (checked as the part
// is sent, D1).
func hashParts(readAt func(off, n int64) ([]byte, error), size, partSize int64) (string, map[int]string, error) {
	layout, err := tws1.Parts(size, partSize)
	if err != nil {
		return "", nil, err
	}
	whole := sha256.New()
	parts := make(map[int]string, len(layout))
	for _, p := range layout {
		data, err := readAt(p.PlainFirst, p.PlainSize)
		if err != nil {
			return "", nil, err
		}
		if int64(len(data)) != p.PlainSize {
			return "", nil, fmt.Errorf("the source changed size while it was read")
		}
		whole.Write(data)
		parts[p.Number] = sha256hex(data)
	}
	return hex.EncodeToString(whole.Sum(nil)), parts, nil
}

// runLarge takes a large upload to its object. A final failure cancels it; an error after which it may resume
// (storage unavailable, a revoked key) keeps it, in its journal, when it has one on disk. A cancel leaves a journaled
// upload for ResumeUploads, and cancels one in memory, which nothing could resume.
func (u *Uploads) runLarge(ctx context.Context, l *large, prog *Progress) (*Object, error) {
	obj, err := u.largeLoop(ctx, l, prog)
	if err == nil {
		return obj, nil
	}
	if ctx.Err() != nil {
		if !l.durable() {
			u.abortUpload(ctx, l.bucket, l.uploadID, true)
			l.forgetKey()
		}
		return nil, ctx.Err()
	}
	var se *stateError
	if errors.As(err, &se) {
		return nil, err
	}
	if keep(err) {
		if !l.durable() {
			u.abortUpload(ctx, l.bucket, l.uploadID, false) // nothing could resume it
			l.forgetKey()
		}
		return nil, err
	}
	// Final: cancelled, so its parts aren't kept (or billed) for nothing.
	u.abortUpload(ctx, l.bucket, l.uploadID, false)
	l.forgetKey()
	l.state = "failed"
	l.lastError = describe(err)
	if serr := l.save(); serr != nil {
		return nil, serr
	}
	return nil, err
}

func (u *Uploads) largeLoop(ctx context.Context, l *large, prog *Progress) (*Object, error) {
	for {
		obj, err := u.attempt(ctx, l, prog)
		var r *restart
		if err == nil || !errors.As(err, &r) || ctx.Err() != nil {
			return obj, err
		}
		l.restarts++
		l.lastError = r.why
		u.abortUpload(ctx, l.bucket, l.uploadID, false)
		if l.restarts > MaxRebuilds {
			return nil, fmt.Errorf("teepin: %s: the upload was started again %d times (%s)", l.key, l.restarts, r.why)
		}
		l.reset()
		if err := l.save(); err != nil {
			return nil, err
		}
	}
}

func (u *Uploads) attempt(ctx context.Context, l *large, prog *Progress) (*Object, error) {
	if l.state == "new" {
		if err := u.create(ctx, l); err != nil {
			return nil, err
		}
	}
	if l.state == "sending" {
		if l.stream == nil {
			if l.dek == nil || len(l.header) == 0 {
				return nil, &restart{why: "the data key is gone"}
			}
			s, err := tws1.ResumeStream(l.dek, l.header, l.size, l.partSize)
			if err != nil {
				return nil, &restart{why: "the data key doesn't open its upload's header"}
			}
			l.stream = s
		}
		if err := u.sendParts(ctx, l, prog); err != nil {
			return nil, err
		}
		l.state = "completing"
		if err := l.save(); err != nil {
			return nil, err
		}
	}
	return u.complete(ctx, l)
}

// createJSON is create's answer.
type createJSON struct {
	UploadID   string         `json:"upload_id"`
	Size       *int64         `json:"size"`
	PartSize   int64          `json:"part_size"`
	AccountKey accountKeyJSON `json:"account_key"`
}

func (w WriteOptions) addTo(spec map[string]any) {
	if w.ContentType != "" {
		spec["content_type"] = w.ContentType
	}
	if len(w.Metadata) > 0 {
		spec["metadata"] = w.Metadata
	}
	if w.IfNoneMatch {
		spec["if_none_match"] = true
	}
	if im := w.ifMatch(); im != "" {
		spec["if_match"] = im
	}
}

func (u *Uploads) create(ctx context.Context, l *large) error {
	if err := l.save(); err != nil { // the idempotency key, before the call
		return err
	}
	spec := map[string]any{"direct": true, "idempotency_key": l.idem, "key": l.key, "size": l.size,
		"part_size": l.partSize, "sha256": l.sha256}
	l.w.addTo(spec)
	var res createJSON
	if _, err := u.control(ctx, http.MethodPost, BucketPath(l.bucket)+"/uploads", spec, &res, &l.waits, 0); err != nil {
		switch codeOf(err) {
		case "upload_completed":
			var ae *errs.APIError
			errors.As(err, &ae)
			l.uploadID = ae.Detail("upload_id")
			l.state = "completing"
			return l.save()
		case "reservation_abandoned":
			return &restart{why: "reservation_abandoned"}
		}
		return err
	}
	key, err := u.checkKey(res.AccountKey)
	if err != nil {
		return err
	}
	if res.PartSize != l.partSize || res.Size == nil || *res.Size != l.size {
		return errs.Integrity("layout", "%s: the upload was created with another layout", l.key)
	}
	oid, err := ObjectIDBytes(res.UploadID)
	if err != nil {
		return errs.Integrity("layout", "%s: the upload's id %q is not a UUID", l.key, res.UploadID)
	}
	l.uploadID = res.UploadID
	dek, err := u.newDataKey()
	if err != nil {
		return err
	}
	l.dek = dek
	if l.stream, err = tws1.NewStream(dek, l.size, l.partSize); err != nil {
		return err
	}
	l.header = l.stream.Header()
	wrapped, err := tws1.WrapKey(key.public, key.account, oid, dek)
	if err != nil {
		return err
	}
	l.wrapped, l.keyVersion = b64(wrapped), key.version
	l.state = "sending"
	return l.saveCreated()
}

// partsJSON is list parts' answer.
type partsJSON struct {
	Parts []struct {
		PartNumber int  `json:"part_number"`
		Landed     bool `json:"landed"`
	} `json:"parts"`
}

func (u *Uploads) sendParts(ctx context.Context, l *large, prog *Progress) error {
	landed := map[int]bool{}
	if l.resumed {
		// After a crash: what storage holds, exactly as signed. Every part's source is still checked.
		var ans partsJSON
		if _, err := u.control(ctx, http.MethodGet, BucketPath(l.bucket)+"/uploads/"+l.uploadID+"/parts", nil, &ans,
			&l.waits, 0); err != nil {
			return err
		}
		for _, p := range ans.Parts {
			if p.Landed {
				landed[p.PartNumber] = true
			}
		}
	}
	for n := 1; int64(n) <= l.partCount; n++ {
		if landed[n] {
			if _, err := u.checkSource(l, n); err != nil {
				return err
			}
			p, _ := tws1.PartOf(l.size, l.partSize, n)
			prog.Add(p.PlainSize)
		}
	}
	var todo []int
	for n := 1; int64(n) <= l.partCount; n++ {
		if !landed[n] {
			todo = append(todo, n)
		}
	}
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		failed  []error
		stopped bool
	)
	isStopped := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return stopped
	}
	task := func(n int) {
		defer wg.Done()
		outcome := OutcomeFailed
		defer func() { u.release(outcome) }()
		if isStopped() {
			return
		}
		if err := u.sendPart(pctx, l, n); err != nil {
			mu.Lock()
			failed = append(failed, err) // the coordinator decides
			stopped = true
			mu.Unlock()
			return
		}
		outcome = OutcomeOK
		if l.wasThrottled() {
			outcome = OutcomeThrottled
		}
		p, _ := tws1.PartOf(l.size, l.partSize, n)
		prog.Add(p.PlainSize)
	}
	for _, n := range todo {
		if isStopped() {
			break
		}
		if err := u.Limit.Acquire(pctx); err != nil {
			break
		}
		wg.Add(1)
		go task(n)
	}
	if !joinOrStop(ctx, &wg, cancel, u.CleanupCap) {
		return ctx.Err() // interrupted: each part goroutine is stopped and waited for, up to the cap (section 22.13)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// The most telling error: a changed file or a restart, before a passing failure.
	return telling(failed)
}

// joinOrStop waits for wg, or, when ctx ends first, stops the goroutines (cancel) and waits for them at most cap. It
// reports whether they ended without ctx ending.
func joinOrStop(ctx context.Context, wg *sync.WaitGroup, cancel func(), cap time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		cancel()
		t := time.NewTimer(cap)
		defer t.Stop()
		select {
		case <-done:
		case <-t.C:
		}
		return false
	}
}

// telling is the most telling of the errors part goroutines met: a changed file, a restart, damaged data, then the
// first.
func telling(failed []error) error {
	if len(failed) == 0 {
		return nil
	}
	var fc *errs.FileChangedError
	var r *restart
	var die *errs.DataIntegrityError
	for _, match := range []func(error) bool{
		func(e error) bool { return errors.As(e, &fc) },
		func(e error) bool { return errors.As(e, &r) },
		func(e error) bool { return errors.As(e, &die) },
	} {
		for _, e := range failed {
			if match(e) {
				return e
			}
		}
	}
	return failed[0]
}

// checkSource reads and hashes part n's plaintext: journaled before its first byte leaves (D1 a), and the same as
// journaled on every later read (D1 b).
func (u *Uploads) checkSource(l *large, n int) ([]byte, error) {
	p, err := tws1.PartOf(l.size, l.partSize, n)
	if err != nil {
		return nil, err
	}
	data, err := l.readAt(p.PlainFirst, p.PlainSize)
	if err != nil {
		return nil, errs.Changed(l.source(), err.Error())
	}
	if int64(len(data)) != p.PlainSize {
		return nil, errs.Changed(l.source(), fmt.Sprintf("part %d is %d bytes, not %d", n, len(data), p.PlainSize))
	}
	h := sha256hex(data)
	l.mu.Lock()
	known, ok := l.plain[n]
	if !ok {
		l.plain[n] = h
	}
	l.mu.Unlock()
	switch {
	case !ok:
		if err := l.savePart(n, h, "", "", "hashed"); err != nil {
			return nil, err
		}
	case known != h:
		return nil, errs.Changed(l.source(), fmt.Sprintf("part %d changed", n))
	}
	return data, nil
}

// target is what a part is sent to: an upload, and what its waits are charged to.
type target struct {
	bucket, key, uploadID string
	w                     *waits
}

func (u *Uploads) signPart(ctx context.Context, t target, n int, md5, sha string, extra map[string]any) (signedPut, error) {
	body := map[string]any{"md5": md5, "sha256": sha}
	for k, v := range extra {
		body[k] = v
	}
	var signed signedPut
	_, err := u.control(ctx, http.MethodPost, fmt.Sprintf("%s/uploads/%s/parts/%d/sign", BucketPath(t.bucket), t.uploadID, n),
		body, &signed, t.w, 0)
	if err != nil {
		switch codeOf(err) {
		case "upload_not_found", "reservation_abandoned":
			return signed, &restart{why: codeOf(err)}
		case "part_mismatch":
			// The source hashes the same, so the bytes can only differ by key: never sent under another.
			return signed, errs.Integrity("part", "%s: part %d was signed with other digests", t.key, n)
		}
	}
	return signed, err
}

func (u *Uploads) sendPart(ctx context.Context, l *large, n int) error {
	stream := l.stream
	if stream == nil {
		return &restart{why: "the stream is gone"}
	}
	plain, err := u.checkSource(l, n)
	if err != nil {
		return err
	}
	l.sealMu.Lock()
	sealed, err := stream.SealPart(n, plain)
	l.sealMu.Unlock()
	clear(plain)
	if err != nil {
		return err
	}
	md5s, sha := md5hex(sealed), sha256hex(sealed)
	t := target{bucket: l.bucket, key: l.key, uploadID: l.uploadID, w: &l.waits}
	recheck := func() error {
		_, err := u.checkSource(l, n)
		return err
	}
	if err := u.sendSealed(ctx, t, n, sealed, md5s, sha, nil, recheck); err != nil {
		return err
	}
	l.mu.Lock()
	plainHash := l.plain[n]
	l.mu.Unlock()
	return l.savePart(n, plainHash, md5s, sha, "sent")
}

// sendSealed signs part n and sends it until storage holds it as signed; recheck (nil for none) reads the source
// again when storage holds other bytes.
func (u *Uploads) sendSealed(ctx context.Context, t target, n int, sealed []byte, md5s, sha string, extra map[string]any,
	recheck func() error) error {
	signed, err := u.signPart(ctx, t, n, md5s, sha, extra)
	if err != nil {
		return err
	}
	attempts, resigns := 0, 0
	for {
		if u.expiresSoon(signed.ExpiresAt) {
			if signed, err = u.signPart(ctx, t, n, md5s, sha, extra); err != nil {
				return err
			}
		}
		u.Budget.OnRequest()
		etag, err := u.putSigned(ctx, signed, sealed)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		outcome := OutcomeOf(err)
		// A part's ETag is its MD5; an empty one is accepted (a browser can't read it, and the rule is the same).
		if outcome == Sent && err == nil && etag != "" && etag != md5s {
			outcome = Bad
		}
		switch outcome {
		case Sent:
			return nil
		case Resign:
			resigns++
			if resigns > maxResigns {
				return err
			}
			if signed, err = u.signPart(ctx, t, n, md5s, sha, extra); err != nil {
				return err
			}
		case Bad:
			// Not the bytes signed: read and hash the source again (D1 b); the same source means a bug.
			if recheck != nil {
				if err := recheck(); err != nil {
					return err
				}
			}
			return errs.Integrity("part", "%s: part %d wasn't stored as signed", t.key, n)
		case Unknown, Unsent:
			// A part may simply be sent again: the same number, the same bytes.
			if !u.Budget.TryRetry() {
				if werr := u.wait(ctx, t.w, attempts, err); werr != nil {
					return werr
				}
			}
			attempts++
		case Busy:
			if werr := u.wait(ctx, t.w, attempts, err); werr != nil {
				return werr
			}
			attempts++
		default:
			if statusOf(err) == http.StatusNotFound {
				return &restart{why: "the storage network no longer has the upload"}
			}
			return err
		}
	}
}

func (u *Uploads) complete(ctx context.Context, l *large) (*Object, error) {
	var out Object
	_, err := u.control(ctx, http.MethodPost, BucketPath(l.bucket)+"/uploads/"+l.uploadID+"/complete",
		map[string]any{"wrapped_key": l.wrapped, "key_version": l.keyVersion}, &out, &l.waits, commitTimeout)
	if err != nil {
		if c := codeOf(err); c == "upload_mismatch" || c == "reservation_abandoned" {
			return nil, &restart{why: c}
		}
		return nil, err
	}
	out.ETag = unquoteETag(out.ETag)
	if out.ChecksumSHA256 != "" && out.ChecksumSHA256 != l.sha256 {
		return nil, errs.Integrity("checksum", "%s: the object's checksum isn't the file's", l.key)
	}
	l.state = "completed"
	l.forgetKey()
	if err := l.save(); err != nil {
		return nil, err
	}
	return &out, nil
}

// --- streams of unknown size (section 22.1, E2 b) -----------------------------------------------------------------

// streamUpload is a stream's upload: created without its size, its last part declared at its sign, and its size and
// SHA-256 brought by complete. Held in memory only: nothing could read the stream again to resume it.
type streamUpload struct {
	bucket, key string
	partSize    int64
	idem        string
	uploadID    string
	stream      *tws1.OpenStream
	wrapped     string
	keyVersion  int
	w           WriteOptions
	// slow is the measured link when it shrank the part size (section 22.7), else nil: running out of parts is then
	// the connection's speed.
	slow *LinkSpeed
	waits
}

// StreamParams are a stream upload's choices.
type StreamParams struct {
	// PartSize is each part's plaintext bytes (0: chosen from ExpectedSize, else 64 MiB).
	PartSize int64
	// ExpectedSize is a hint of the stream's size, for choosing the part size.
	ExpectedSize int64
	// Concurrency is how many parts are sealed and sent at once (0: Sizes.StreamConcurrency).
	Concurrency int
	// Progress is told of the bytes saved, with -1 as the total.
	Progress func(done, total int64)
}

// PutStream uploads everything src yields, a stream whose size isn't known, in parts held in memory: at most
// concurrency + 3 of them. Any failure, or a cancel, cancels the upload (one call): a stream can't be read again.
func (u *Uploads) PutStream(ctx context.Context, bucket, key string, src io.Reader, w WriteOptions, sp StreamParams) (*Object, error) {
	link := u.Speed.Link()
	psize, err := StreamPartSize(sp.ExpectedSize, sp.PartSize, u.Sizes.PartRules, link, key)
	if err != nil {
		return nil, err
	}
	n := sp.Concurrency
	if n <= 0 {
		n = u.Sizes.StreamConcurrency
	}
	w.IfMatch = w.ifMatch()
	s := &streamUpload{bucket: bucket, key: key, partSize: psize, idem: UUID7(), w: w}
	if link != nil {
		// Its parts were shrunk for a slow link when the rule without the link gives larger ones.
		if base, err := StreamPartSize(sp.ExpectedSize, sp.PartSize, u.Sizes.PartRules, nil, key); err != nil || psize < base {
			s.slow = link
		}
	}
	obj, err := u.streamAttempt(ctx, s, src, max(1, n), NewProgress(-1, sp.Progress))
	s.stream = nil
	if err == nil {
		return obj, nil
	}
	if ctx.Err() == nil {
		var r *restart
		if errors.As(err, &r) {
			err = fmt.Errorf("teepin: %s: the upload was ended (%s); a stream can't be read again, so upload it again", key, r.why)
		}
	}
	u.abortUpload(ctx, s.bucket, s.uploadID, ctx.Err() != nil)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, err
}

// readFull reads len(buf) bytes of src, or fewer at its end, returning what it read. A cancel ends the wait at once;
// a read the source holds up then ends when the source returns, into a buffer no one uses any more.
func readFull(ctx context.Context, src io.Reader, buf []byte) ([]byte, error) {
	type result struct {
		n   int
		err error
	}
	ch := make(chan result, 1)
	go func() {
		n, err := io.ReadFull(src, buf)
		ch <- result{n, err}
	}()
	select {
	case r := <-ch:
		if r.err == io.EOF || r.err == io.ErrUnexpectedEOF {
			return buf[:r.n], nil
		}
		if r.err != nil {
			return nil, fmt.Errorf("teepin: read the stream: %w", r.err)
		}
		return buf, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (u *Uploads) streamAttempt(ctx context.Context, s *streamUpload, src io.Reader, concurrency int, prog *Progress) (*Object, error) {
	if err := u.createStream(ctx, s); err != nil {
		return nil, err
	}
	stream := s.stream
	psize := s.partSize
	// The next part's plaintext, read straight into one buffer (a part never exists twice while it is read).
	take := func() ([]byte, error) { return readFull(ctx, src, make([]byte, psize)) }
	t := target{bucket: s.bucket, key: s.key, uploadID: s.uploadID, w: &s.waits}
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		failed  []error
		stopped bool
		held    int // parts held: the reader's (the one it seals, the next) and those in flight
	)
	slots := NewSlots(concurrency)
	isStopped := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return stopped
	}
	hold := func(d int) {
		mu.Lock()
		held += d
		now := held
		mu.Unlock()
		if d > 0 {
			if h := u.Hooks.StreamHeld; h != nil {
				h(now)
			}
		}
	}
	// task signs and sends part n, sealed from size plaintext bytes; its sealed bytes come in a box it empties when it
	// ends, before its slot is given back.
	task := func(n int, box *[]byte, size int64, last bool) {
		defer wg.Done()
		outcome := OutcomeFailed
		defer func() {
			*box = nil
			hold(-1)
			u.release(outcome)
			slots.Release()
		}()
		if isStopped() {
			return
		}
		var extra map[string]any
		if last {
			extra = map[string]any{"last": true, "plain_size": size}
		}
		sealed := *box
		if err := u.sendSealed(pctx, t, n, sealed, md5hex(sealed), sha256hex(sealed), extra, nil); err != nil {
			mu.Lock()
			failed = append(failed, err) // the reader decides
			stopped = true
			mu.Unlock()
			return
		}
		outcome = OutcomeOK
		if s.wasThrottled() {
			outcome = OutcomeThrottled
		}
		prog.Add(size)
	}
	whole := sha256.New()
	var total int64
	// interrupted stops every part goroutine and waits for them, up to the cap (section 22.13).
	interrupted := func(err error) (*Object, error) {
		mu.Lock()
		stopped = true
		mu.Unlock()
		cancel()
		WaitCapped(&wg, u.CleanupCap)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	cur, err := take()
	if err != nil {
		return interrupted(err)
	}
	hold(1)
	if len(cur) == 0 {
		return interrupted(fmt.Errorf("teepin: %s: the stream is empty", s.key))
	}
	for n := 1; !isStopped(); n++ {
		// One part ahead: a part is the last only when nothing follows it.
		nxt, err := take()
		if err != nil {
			return interrupted(err)
		}
		last := len(nxt) == 0
		if !last {
			hold(1)
		}
		if !last && int64(n) >= u.Sizes.MaxParts {
			if s.slow != nil {
				return interrupted(TooSlow(s.key, 0, *s.slow, psize, u.Sizes.PartRules))
			}
			return interrupted(&errs.APIError{StatusCode: http.StatusRequestEntityTooLarge, Code: "too_many_parts",
				Message: fmt.Sprintf("%s: the stream is over %d parts of %d bytes; give its expected size (or a larger "+
					"part size) so its parts are large enough", s.key, u.Sizes.MaxParts, psize)})
		}
		size := int64(len(cur))
		total += size
		whole.Write(cur)
		sealed, err := stream.SealPart(n, cur, last)
		if h := u.Hooks.StreamSealed; h != nil && err == nil {
			h()
		}
		cur = nxt // the plaintext sealed is dropped before the part is handed over
		if err != nil {
			return interrupted(err)
		}
		if err := slots.Acquire(pctx); err != nil {
			return interrupted(err)
		}
		if err := u.Limit.Acquire(pctx); err != nil {
			slots.Release()
			return interrupted(err)
		}
		box := new([]byte) // the goroutine's own: it empties it when it ends
		*box, sealed = sealed, nil
		wg.Add(1)
		go task(n, box, size, last)
		if last {
			break
		}
	}
	if !joinOrStop(ctx, &wg, cancel, u.CleanupCap) {
		return nil, ctx.Err()
	}
	if len(failed) > 0 {
		return nil, telling(failed)
	}
	if isStopped() {
		return nil, fmt.Errorf("teepin: %s: the stream stopped", s.key)
	}
	return u.completeStream(ctx, s, total, hex.EncodeToString(whole.Sum(nil)))
}

func (u *Uploads) createStream(ctx context.Context, s *streamUpload) error {
	spec := map[string]any{"direct": true, "idempotency_key": s.idem, "key": s.key, "part_size": s.partSize}
	s.w.addTo(spec)
	var res createJSON
	if _, err := u.control(ctx, http.MethodPost, BucketPath(s.bucket)+"/uploads", spec, &res, &s.waits, 0); err != nil {
		return err
	}
	key, err := u.checkKey(res.AccountKey)
	if err != nil {
		return err
	}
	s.uploadID = res.UploadID
	if res.PartSize != s.partSize || res.Size != nil {
		return errs.Integrity("layout", "%s: the upload was created with another layout", s.key)
	}
	oid, err := ObjectIDBytes(res.UploadID)
	if err != nil {
		return errs.Integrity("layout", "%s: the upload's id %q is not a UUID", s.key, res.UploadID)
	}
	dek, err := u.newDataKey()
	if err != nil {
		return err
	}
	defer clear(dek) // the stream holds its derived keys; nothing journals this one
	if s.stream, err = tws1.NewOpenStream(dek, s.partSize); err != nil {
		return err
	}
	wrapped, err := tws1.WrapKey(key.public, key.account, oid, dek)
	if err != nil {
		return err
	}
	s.wrapped, s.keyVersion = b64(wrapped), key.version
	return nil
}

// completeStream completes a stream's upload with the size and SHA-256 read.
func (u *Uploads) completeStream(ctx context.Context, s *streamUpload, size int64, sha string) (*Object, error) {
	var out Object
	_, err := u.control(ctx, http.MethodPost, BucketPath(s.bucket)+"/uploads/"+s.uploadID+"/complete",
		map[string]any{"wrapped_key": s.wrapped, "key_version": s.keyVersion, "size": size, "sha256": sha}, &out,
		&s.waits, commitTimeout)
	if err != nil {
		if c := codeOf(err); c == "upload_mismatch" || c == "reservation_abandoned" {
			return nil, &restart{why: c}
		}
		return nil, err
	}
	out.ETag = unquoteETag(out.ETag)
	if (out.ChecksumSHA256 != "" && out.ChecksumSHA256 != sha) || out.Size != size {
		return nil, errs.Integrity("checksum", "%s: the object isn't the stream that was read", s.key)
	}
	return &out, nil
}
