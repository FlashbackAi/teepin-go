// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package journal

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	// HistoryFile is the one-line-per-run summary of finished runs, in each kind's directory.
	HistoryFile = "history.log"
	// HistoryTrimBytes: an append that finds the history past this many bytes trims it to HistoryKeep.
	HistoryTrimBytes = 1 << 20
	// HistoryKeep is how long the history keeps a line, when it is trimmed.
	HistoryKeep = 7 * 24 * time.Hour
)

// AppendHistory adds one line about a finished run of kind (section 17.12): "<unix seconds> <line>". Every SDK on this
// machine shares the file, so a line is added with one O_APPEND write and the file is never rewritten here (profile
// 1's G-3). It's trimmed to 7 days by TrimHistory: by CleanUploads, or here once it passes HistoryTrimBytes.
func AppendHistory(stateDir, kind, line string) error {
	root := filepath.Join(stateDir, kind)
	if err := PrivateDir(root); err != nil {
		return err
	}
	return appendHistory(filepath.Join(root, HistoryFile), line, HistoryTrimBytes)
}

func appendHistory(path, line string, trimAt int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write([]byte(strconv.FormatInt(time.Now().Unix(), 10) + " " + line + "\n"))
	var size int64
	if err == nil {
		var st os.FileInfo
		if st, err = f.Stat(); err == nil {
			size = st.Size()
		}
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if size > trimAt {
		return trimHistory(path)
	}
	return nil
}

// TrimHistory keeps the last HistoryKeep of kind's history, in one atomic rewrite. A line another SDK appends during
// the rewrite can be lost, so this runs only at CleanUploads or past the size cap, never on each run.
func TrimHistory(stateDir, kind string) error {
	return trimHistory(filepath.Join(stateDir, kind, HistoryFile))
}

func trimHistory(path string) error {
	old, err := ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	since := float64(time.Now().Add(-HistoryKeep).Unix())
	var kept []byte
	for _, l := range bytes.Split(old, []byte{'\n'}) {
		first, _, _ := bytes.Cut(l, []byte{' '})
		if t, err := strconv.ParseFloat(string(first), 64); err == nil && t >= since {
			kept = append(append(kept, l...), '\n')
		}
	}
	return WriteAtomic(path, kept)
}

// Pins are the first public key seen for each account and key version (section 11.4): a different key under the same
// version is refused. They're kept in the state directory's keys.json, which the Python and Node SDKs share (the same
// shape: account id, then version, then the key in base64), and in memory when it can't be written. Safe for
// concurrent use.
type Pins struct {
	mu   sync.Mutex
	path string
	mem  map[string]map[string]string
}

// NewPins keeps pins in stateDir ("" keeps them in memory only).
func NewPins(stateDir string) *Pins {
	p := &Pins{mem: map[string]map[string]string{}}
	if stateDir != "" {
		p.path = filepath.Join(stateDir, "keys.json")
	}
	return p
}

// Check pins the key if it's new, and reports false when another key holds that account and version.
func (p *Pins) Check(accountID string, version int, publicKey []byte) bool {
	encoded := base64.StdEncoding.EncodeToString(publicKey)
	v := strconv.Itoa(version)
	p.mu.Lock()
	defer p.mu.Unlock()
	pins := p.load()
	if have, ok := pins[accountID][v]; ok {
		return have == encoded
	}
	if pins[accountID] == nil {
		pins[accountID] = map[string]string{}
	}
	pins[accountID][v] = encoded
	p.mem = pins
	if p.path != "" {
		if b, err := json.Marshal(pins); err == nil && PrivateDir(filepath.Dir(p.path)) == nil {
			_ = WriteAtomic(p.path, b) // a read-only state directory: the pin stays in memory
		}
	}
	return true
}

// load is the pins on disk merged with those in memory.
func (p *Pins) load() map[string]map[string]string {
	out := map[string]map[string]string{}
	if p.path != "" {
		if b, err := ReadFile(p.path); err == nil {
			var disk map[string]map[string]string
			if json.Unmarshal(b, &disk) == nil {
				for a, vs := range disk {
					out[a] = map[string]string{}
					for k, x := range vs {
						out[a][k] = x
					}
				}
			}
		}
	}
	for a, vs := range p.mem {
		if out[a] == nil {
			out[a] = map[string]string{}
		}
		for k, x := range vs {
			out[a][k] = x // what this client pinned holds, as in the Python and Node SDKs
		}
	}
	return out
}
