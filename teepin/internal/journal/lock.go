// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// LockGrace is how long a lock file whose content can't be read yet is taken as being written: after that it is
// stale.
const LockGrace = 10 * time.Second

// lockJSON is what a lock file holds.
type lockJSON struct {
	Host  string `json:"host"`
	PID   int    `json:"pid"`
	Start string `json:"start,omitempty"`
	Nonce string `json:"nonce"`
}

// lockInfo is a lock file as read.
type lockInfo struct {
	// id is the lock's identity: its nonce, or for one that can't be read, its time and size.
	id      string
	host    string
	pid     int
	start   string
	garbled bool
	mtime   time.Time
}

// registry is the locks this process holds, or is taking, by absolute path: two runs in one process never share one
// either, and an interrupted run is released in the same process (S7-2).
var registry = struct {
	sync.Mutex
	held map[string]bool
}{held: map[string]bool{}}

func registered(key string) bool {
	registry.Lock()
	defer registry.Unlock()
	return registry.held[key]
}

var (
	hostOnce sync.Once
	hostName string
	selfOnce sync.Once
	selfTime string
)

func host() string {
	hostOnce.Do(func() { hostName, _ = os.Hostname() })
	return hostName
}

// selfStart is this process's start token (asked once).
func selfStart() string {
	selfOnce.Do(func() { selfTime = processStart(os.Getpid()) })
	return selfTime
}

// Lock is an exclusive lock on a run, held while it is open, so two processes (or two runs in one process) never work
// one run (section 17.2). It is a file made with O_EXCL holding the host, the PID, the process's start time and a
// nonce. A process that ends without releasing it leaves it stale, and the next taker takes it over: a lock whose
// process is gone (a dead PID on this host, or a reused PID whose start time differs), one of this process's own that
// no run here holds, or one left unreadable for LockGrace. A lock from another host is never stale. Safe for
// concurrent use.
type Lock struct {
	path string
	key  string
	mu   sync.Mutex
	mine string // our nonce while we hold it
}

// NewLock is the lock at path (not taken).
func NewLock(path string) *Lock {
	key, err := filepath.Abs(path)
	if err != nil {
		key = path
	}
	return &Lock{path: path, key: key}
}

// Path is the lock file's path.
func (l *Lock) Path() string { return l.path }

// Held reports whether this Lock holds it.
func (l *Lock) Held() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.mine != ""
}

// Acquire takes the lock if no live process (this one included) holds it, and reports whether it did.
func (l *Lock) Acquire() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mine != "" {
		return true, nil
	}
	registry.Lock()
	if registry.held[l.key] {
		registry.Unlock()
		return false, nil // this process holds it, or is taking it
	}
	registry.held[l.key] = true
	registry.Unlock()
	ok := false
	defer func() {
		if !ok {
			registry.Lock()
			delete(registry.held, l.key)
			registry.Unlock()
		}
	}()
	nonce := randomHex(16)
	text, err := json.Marshal(lockJSON{Host: host(), PID: os.Getpid(), Start: selfStart(), Nonce: nonce})
	if err != nil {
		return false, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		err := createExclusive(l.path, text)
		if err == nil {
			l.mine, ok = nonce, true
			return true, nil
		}
		if !exists(err) {
			return false, err
		}
		cur, err := readLock(l.path)
		if err != nil {
			return false, err
		}
		if cur == nil {
			continue // released meanwhile: try again
		}
		if !l.stale(cur, true) {
			return false, nil
		}
		took, err := l.takeOver(cur, text)
		if err != nil {
			return false, err
		}
		if took {
			l.mine, ok = nonce, true
			return true, nil
		}
	}
	return false, nil
}

// stale reports whether a lock no longer guards anything: its process is gone, or it is this process's and no run
// here holds it (taking: this Lock is the one taking it, so its registry mark is its own).
func (l *Lock) stale(cur *lockInfo, taking bool) bool {
	if cur.garbled {
		return time.Since(cur.mtime) > LockGrace
	}
	if cur.host != host() || cur.pid <= 0 {
		return false
	}
	if cur.pid == os.Getpid() {
		return taking || !registered(l.key)
	}
	return gone(cur)
}

// gone reports whether a lock's process has ended: a dead PID, or a live one that started at another time (the PID
// was reused).
func gone(cur *lockInfo) bool {
	if !alive(cur.pid) {
		return true
	}
	if cur.start != "" {
		if now := processStart(cur.pid); now != "" && now != cur.start {
			return true
		}
	}
	return false
}

// takeOver takes over a stale lock: only one taker's claim (an O_EXCL file named by the stale lock's identity) wins,
// and the winner replaces the lock whole, by rename, only if it is still that stale one.
func (l *Lock) takeOver(cur *lockInfo, text []byte) (bool, error) {
	claim := l.path + ".take-" + strings.Map(func(r rune) rune {
		if r == '-' || ('0' <= r && r <= '9') || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') {
			return r
		}
		return -1
	}, cur.id)
	if err := createExclusive(claim, text); err != nil {
		if !exists(err) {
			return false, err
		}
		// Another taker's claim. One whose taker died is cleared, so a later try can go on.
		if c, _ := readLock(claim); c != nil && staleClaim(c) {
			_ = removeFile(claim)
		}
		return false, nil
	}
	defer func() { _ = removeFile(claim) }()
	again, err := readLock(l.path)
	if err != nil {
		return false, err
	}
	if again == nil || again.id != cur.id {
		return false, nil // replaced meanwhile
	}
	// Replaced whole, so there's never a moment without a lock. A replace that still fails after its retries (another
	// program holds the lock file open) is not taken: Acquire goes round again, and never fails for it.
	if err := WriteAtomic(l.path, text); err != nil {
		return false, nil
	}
	return true, nil
}

// staleClaim reports whether a takeover claim's taker died before it finished.
func staleClaim(c *lockInfo) bool {
	if c.garbled {
		return time.Since(c.mtime) > LockGrace
	}
	if c.host != host() || c.pid <= 0 || c.pid == os.Getpid() {
		return false // another host's, or a taker in this process, at work now
	}
	return gone(c)
}

// Release lets the lock go, deleting its file if it is still ours. Safe to call twice.
func (l *Lock) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	nonce := l.mine
	if nonce == "" {
		return nil
	}
	l.mine = ""
	defer func() {
		registry.Lock()
		delete(registry.held, l.key)
		registry.Unlock()
	}()
	cur, err := readLock(l.path)
	if err != nil || cur == nil || cur.id != nonce {
		return nil
	}
	return removeFile(l.path)
}

// HeldElsewhere reports whether a live process (this one included, through another Lock) holds the lock.
func (l *Lock) HeldElsewhere() (bool, error) {
	l.mu.Lock()
	mine := l.mine
	l.mu.Unlock()
	if mine != "" {
		return false, nil
	}
	cur, err := readLock(l.path)
	if err != nil || cur == nil {
		return false, err
	}
	if registered(l.key) {
		return true, nil
	}
	return !l.stale(cur, false), nil
}

// readLock reads a lock file; nil when it isn't there.
func readLock(path string) (*lockInfo, error) {
	data, err := ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	st, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var j lockJSON
	if json.Unmarshal(data, &j) != nil || j.Nonce == "" || j.Host == "" || j.PID <= 0 {
		return &lockInfo{id: fmt.Sprintf("g%d-%d", st.ModTime().UnixMilli(), st.Size()), garbled: true, mtime: st.ModTime()}, nil
	}
	return &lockInfo{id: j.Nonce, host: j.Host, pid: j.PID, start: j.Start, mtime: st.ModTime()}, nil
}

// createExclusive creates path with data only if nothing is there (O_EXCL), fsynced.
func createExclusive(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(path)
	}
	return err
}

// exists reports a create that found a file there (on Windows, also one being deleted, which can't be opened until
// the last handle on it closes: as good as there).
func exists(err error) bool { return errors.Is(err, fs.ErrExist) || sharingViolation(err) }
