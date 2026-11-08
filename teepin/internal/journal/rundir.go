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
	"sort"
	"strings"
	"sync"
	"time"
)

// RunDir is a run's directory in the state directory, with its lock and journal. Its methods are safe for concurrent
// use once it is open.
type RunDir struct {
	// ID is the run's id, a UUIDv7.
	ID string
	// Kind is Uploads or Downloads.
	Kind string
	// Path is the run's directory.
	Path string

	lock     *Lock
	mu       sync.Mutex
	j        *Journal
	released bool
}

func newRunDir(stateDir, kind, id string) *RunDir {
	path := filepath.Join(stateDir, kind, id)
	return &RunDir{ID: id, Kind: kind, Path: path, lock: NewLock(filepath.Join(path, LockFile))}
}

// CreateRun makes a new run of kind, locked by this process, with its journal open.
func CreateRun(stateDir, kind string) (*RunDir, error) {
	if err := PrivateDir(filepath.Join(stateDir, kind)); err != nil {
		return nil, err
	}
	rd := newRunDir(stateDir, kind, UUID7())
	if err := PrivateDir(rd.Path); err != nil {
		return nil, err
	}
	ok, err := rd.lock.Acquire()
	if err == nil && !ok {
		err = fmt.Errorf("journal: run %s is locked", rd.ID)
	}
	if err != nil {
		_ = os.RemoveAll(rd.Path)
		return nil, err
	}
	if err := rd.create(stateDir); err != nil {
		_ = rd.lock.Release()
		_ = os.RemoveAll(rd.Path)
		return nil, err
	}
	return rd, nil
}

func (rd *RunDir) create(stateDir string) error {
	if rd.Kind == Uploads {
		if err := PrivateDir(filepath.Join(rd.Path, PacksDir)); err != nil {
			return err
		}
		if err := PrivateDir(filepath.Join(rd.Path, KeysDir)); err != nil {
			return err
		}
	}
	meta, err := json.Marshal(runMeta{Format: Format, ID: rd.ID, Kind: rd.Kind, Created: time.Now().UnixMilli()})
	if err != nil {
		return err
	}
	if err := WriteAtomic(filepath.Join(rd.Path, RunFile), append(meta, '\n')); err != nil {
		return err
	}
	j, err := Open(rd.Path)
	if err != nil {
		return err
	}
	rd.j = j
	FsyncDir(filepath.Join(stateDir, rd.Kind))
	return nil
}

// LockRun locks a run for this caller without opening its journal (Open does); nil when a live process, or another
// run in this one, holds it.
func LockRun(stateDir, kind, id string) (*RunDir, error) {
	rd := newRunDir(stateDir, kind, id)
	ok, err := rd.lock.Acquire()
	if err != nil || !ok {
		return nil, err
	}
	return rd, nil
}

// Open opens a locked run's journal (replayed and compacted).
func (rd *RunDir) Open() error {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	if rd.j != nil {
		return nil
	}
	j, err := Open(rd.Path)
	if err != nil {
		return err
	}
	rd.j = j
	return nil
}

// TakeRun locks a run for this caller and opens its journal; nil when a live process, or another run in this one,
// holds it. A journal that can't be opened (damaged, or from a newer SDK) is an error, with the lock let go.
func TakeRun(stateDir, kind, id string) (*RunDir, error) {
	rd, err := LockRun(stateDir, kind, id)
	if err != nil || rd == nil {
		return nil, err
	}
	if err := rd.Open(); err != nil {
		_ = rd.lock.Release()
		return nil, err
	}
	return rd, nil
}

// PeekRun reads a run's journal without taking it (read-only), and whether a live process holds it.
func PeekRun(stateDir, kind, id string) (*Journal, bool, error) {
	rd := newRunDir(stateDir, kind, id)
	inUse, err := rd.lock.HeldElsewhere()
	if err != nil {
		return nil, false, err
	}
	j, err := Read(rd.Path)
	if err != nil {
		return nil, inUse, err
	}
	return j, inUse, nil
}

// RunIDs is every run of a kind in the state directory, oldest first: the directories holding a RunFile (the other
// SDKs' runs hold theirs, so they're never listed here).
func RunIDs(stateDir, kind string) ([]string, error) {
	root := filepath.Join(stateDir, kind)
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if st, err := os.Stat(filepath.Join(root, e.Name(), RunFile)); err == nil && st.Mode().IsRegular() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// Journal is the run's journal (nil until it is open).
func (rd *RunDir) Journal() *Journal {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	return rd.j
}

// Lock is the run's lock.
func (rd *RunDir) Lock() *Lock { return rd.lock }

func (rd *RunDir) packPath(name string) string {
	return filepath.Join(rd.Path, PacksDir, filepath.Base(name))
}

func (rd *RunDir) keyPath(id string) string {
	return filepath.Join(rd.Path, KeysDir, filepath.Base(id)+".key")
}

// WritePack writes a pack file atomically (0600, fsynced).
func (rd *RunDir) WritePack(name string, data []byte) error {
	if err := PrivateDir(filepath.Join(rd.Path, PacksDir)); err != nil {
		return err
	}
	return WriteAtomic(rd.packPath(name), data)
}

// ReadPack reads a pack file; nil when it isn't there (or can't be read).
func (rd *RunDir) ReadPack(name string) []byte {
	b, err := os.ReadFile(rd.packPath(name))
	if err != nil {
		return nil
	}
	return b
}

// RemovePack deletes a pack file.
func (rd *RunDir) RemovePack(name string) error { return removeFile(rd.packPath(name)) }

// PackNames are the pack files kept.
func (rd *RunDir) PackNames() []string { return names(filepath.Join(rd.Path, PacksDir)) }

// WriteKey writes a large upload's data key (E3) to a file of its own, atomically: 0600, fsynced, before the
// upload's parts are signed.
func (rd *RunDir) WriteKey(id string, key []byte) error {
	if err := PrivateDir(filepath.Join(rd.Path, KeysDir)); err != nil {
		return err
	}
	return WriteAtomic(rd.keyPath(id), key)
}

// ReadKey reads a data key; nil when it isn't there.
func (rd *RunDir) ReadKey(id string) ([]byte, error) {
	b, err := os.ReadFile(rd.keyPath(id))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

// RemoveKey overwrites a data key's file with zeros (fsynced), then deletes it.
func (rd *RunDir) RemoveKey(id string) error {
	path := rd.keyPath(id)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err == nil {
		_, err = f.WriteAt(make([]byte, st.Size()), 0)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return removeFile(path)
}

// KeyNames are the ids of the data keys kept; leftovers of a write cut short are deleted.
func (rd *RunDir) KeyNames() []string {
	dir := filepath.Join(rd.Path, KeysDir)
	var out []string
	for _, n := range names(dir) {
		if id, ok := strings.CutSuffix(n, ".key"); ok {
			out = append(out, id)
		} else {
			_ = removeFile(filepath.Join(dir, n))
		}
	}
	return out
}

// Release closes the journal and lets the lock go: the run stays for a later resume. Safe to call twice.
func (rd *RunDir) Release() error {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	if rd.released {
		return nil
	}
	rd.released = true
	var err error
	if rd.j != nil {
		err = rd.j.Close()
	}
	if lerr := rd.lock.Release(); err == nil {
		err = lerr
	}
	return err
}

// Remove deletes the run while it still holds the lock (its RunFile first, so it's no longer listed; a data key still
// kept is zeroed before it goes), then lets the lock go.
func (rd *RunDir) Remove() error {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	if rd.j != nil {
		_ = rd.j.Close()
	}
	if err := removeFile(filepath.Join(rd.Path, RunFile)); err != nil {
		return err
	}
	if rd.Kind == Uploads {
		for _, id := range rd.KeyNames() {
			_ = rd.RemoveKey(id)
		}
	}
	for _, n := range names(rd.Path) {
		if n != LockFile {
			p := filepath.Join(rd.Path, n)
			_ = retrySharing(func() error { return os.RemoveAll(p) })
		}
	}
	rd.released = true
	err := rd.lock.Release()
	if rerr := retrySharing(func() error { return os.RemoveAll(rd.Path) }); err == nil {
		err = rerr
	}
	return err
}

// names are a directory's entries; none when it can't be read.
func names(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}
