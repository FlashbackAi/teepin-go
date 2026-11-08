// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

// Package journal is the local state of direct storage's runs (TEEPIN-STORAGE-DESIGN.md sections 17.2, 22.6, 24.4
// and 25.4): where it lives, each run's directory and journal, and the lock that keeps two processes off one run.
//
//   - The state directory is per OS: $XDG_STATE_HOME/teepin (by default ~/.local/state/teepin), ~/Library/Application
//     Support/teepin on macOS, %LOCALAPPDATA%\teepin on Windows, or TEEPIN_STATE_DIR: the same place the Python and
//     Node SDKs use. It is created 0700 and its files 0600, since journals hold file paths and wrapped keys. On Windows
//     the profile directory's user-only ACL is inherited.
//   - Each run has <state>/<kind>/<run-id>/ (kind: "uploads" or "downloads") holding run.go.json, an append-only
//     events.go.jsonl, a lock file, and for uploads packs/ and keys/ (a large upload's data key, E3). Run ids are
//     UUIDv7, so they sort by time. The file names are Go's own: the Python SDK marks its runs with journal.sqlite3 and
//     the Node SDK with run.json, so each SDK lists and resumes only its own runs (section 25.7, decision 2).
//   - The journal: every write is one line (a batch of rows, so it lands whole or not at all), written and fsynced
//     before the action that needs it (section 17.1, rule 3). A torn last line (a crash mid-write) is ignored; a bad
//     line anywhere else is an error. When a run is reopened its journal is compacted: rewritten as one line in a
//     temporary file, fsynced, renamed over the old one, and the directory fsynced where the OS allows.
//   - The lock (lock.go): a file made with O_EXCL holding the host, the PID, the process's start time and a nonce. A
//     stale one is taken over through an O_EXCL claim and replaced whole by rename, so there is never a moment without
//     a lock and a live lock is never moved.
//   - history.log (history.go) is shared by every SDK on the machine: appended only, a line per write.
//
// Step 4's downloads use the same pieces with kind "downloads" and tables of their own: the journal stores any JSON
// rows by table and id.
package journal

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const (
	// StateEnv names the environment variable that overrides the state directory.
	StateEnv = "TEEPIN_STATE_DIR"
	// RunFile marks a directory as one of this SDK's runs: its format, id, kind and creation time.
	RunFile = "run.go.json"
	// EventsFile is a run's append-only journal: one JSON line per write.
	EventsFile = "events.go.jsonl"
	// LockFile is a run's lock.
	LockFile = "lock"
	// PacksDir holds an upload run's pack files.
	PacksDir = "packs"
	// KeysDir holds an upload run's large uploads' data keys (E3), one 0600 file each.
	KeysDir = "keys"
	// Format is this journal's format version, in RunFile; a newer one is refused.
	Format = 1
	// Uploads is the kind of an upload run.
	Uploads = "uploads"
	// Downloads is the kind of a download run.
	Downloads = "downloads"
)

const posix = runtime.GOOS != "windows"

// DefaultStateDir is the state directory for this system (section 19.12), or TEEPIN_STATE_DIR: the same place the
// Python and Node SDKs use. Each SDK keeps its own runs there.
func DefaultStateDir() string {
	if d := os.Getenv(StateEnv); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	switch runtime.GOOS {
	case "windows":
		base := os.Getenv("LOCALAPPDATA")
		if base == "" {
			base = filepath.Join(home, "AppData", "Local")
		}
		return filepath.Join(base, "teepin")
	case "darwin", "ios":
		return filepath.Join(home, "Library", "Application Support", "teepin")
	}
	base := os.Getenv("XDG_STATE_HOME")
	if base == "" {
		base = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(base, "teepin")
}

// PrivateDir creates path (and its parents) readable by its owner only.
func PrivateDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	if posix {
		return os.Chmod(path, 0o700)
	}
	return nil
}

// FsyncDir fsyncs a directory, so a rename or a new file in it survives a crash. It does nothing where the OS can't
// (Windows), and ignores a file system that can't: the rename is still atomic.
func FsyncDir(dir string) {
	if !posix {
		return
	}
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = f.Sync()
	_ = f.Close()
}

// randomHex is n random bytes in hex.
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // crypto/rand.Read never fails (Go 1.24 and later)
	return hex.EncodeToString(b)
}

// UUID7 is a UUIDv7 (RFC 9562): a millisecond timestamp, then random bits, so ids sort by time.
func UUID7() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	ms := uint64(time.Now().UnixMilli())
	for i := 5; i >= 0; i-- {
		b[i] = byte(ms)
		ms >>= 8
	}
	b[6] = b[6]&0x0f | 0x70
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// WriteAtomic writes data to path all at once: a temporary file (0600), fsynced, renamed over path, and the
// directory fsynced. A reader sees the old bytes or the new, never a mix.
func WriteAtomic(path string, data []byte) error {
	tmp := path + "." + randomHex(16) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
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
	if err == nil {
		err = RenameOver(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	FsyncDir(filepath.Dir(path))
	return nil
}

// sharingRetry is how long a rename over a file, or a delete, is tried again while another process has the file open
// (tests shorten it).
var sharingRetry = 2 * time.Second

// RenameOver renames from over to. On Windows a rename over a file another program has open fails (a sharing
// violation, or access denied) until that program closes it, so it is tried again for about two seconds (profile 1's
// W-1, as the Python and Node SDKs do); elsewhere it runs once.
func RenameOver(from, to string) error {
	return retrySharing(func() error { return os.Rename(from, to) })
}

// ReadFile reads a whole file without keeping others from renaming or deleting it meanwhile (on Windows, a file
// opened the usual way can't be renamed over or deleted until it is closed).
func ReadFile(path string) ([]byte, error) { return readShared(path) }

// removeFile deletes path; nothing when it isn't there.
func removeFile(path string) error {
	err := retrySharing(func() error { return os.Remove(path) })
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// Retry runs op, again for up to about two seconds while another process has the file open (Windows' sharing
// violations; elsewhere op runs once): a delete or rename of a file another program may be reading.
func Retry(op func() error) error { return retrySharing(op) }

// retrySharing runs op, again for up to sharingRetry while another process has the file open (Windows' sharing
// violations; elsewhere op runs once).
func retrySharing(op func() error) error {
	deadline := time.Now().Add(sharingRetry)
	for {
		err := op()
		if err == nil || !sharingViolation(err) || !time.Now().Before(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// DirectorySize is the bytes of the files under path.
func DirectorySize(path string) int64 {
	var total int64
	_ = filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}
