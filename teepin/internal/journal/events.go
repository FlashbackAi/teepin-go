// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package journal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
)

// Entry is one write in a batch: a row put by table and id (replacing any row with that id), or removed.
type Entry struct {
	Table string
	ID    string
	// Row is marshalled as JSON; it must marshal to a JSON object.
	Row any
	// Remove removes the row instead.
	Remove bool
}

// wire is an entry as a line of the events file holds it.
type wire struct {
	Table  string          `json:"table"`
	ID     string          `json:"id"`
	Row    json.RawMessage `json:"row,omitempty"`
	Remove bool            `json:"remove,omitempty"`
}

// table is a table's rows, in the order their ids were first put.
type table struct {
	order []string
	byID  map[string]json.RawMessage
}

// rows are a journal's tables in memory: what every write and a replay apply to.
type rows struct {
	tables   map[string]*table
	tableSeq []string
	counters map[string]int64
}

func newRows() *rows { return &rows{tables: map[string]*table{}, counters: map[string]int64{}} }

func (r *rows) apply(e wire) {
	t := r.tables[e.Table]
	if t == nil {
		t = &table{byID: map[string]json.RawMessage{}}
		r.tables[e.Table] = t
		r.tableSeq = append(r.tableSeq, e.Table)
	}
	if e.Remove {
		if _, ok := t.byID[e.ID]; ok {
			delete(t.byID, e.ID)
			for i, id := range t.order {
				if id == e.ID {
					t.order = append(t.order[:i:i], t.order[i+1:]...)
					break
				}
			}
		}
		return
	}
	if _, ok := t.byID[e.ID]; !ok {
		t.order = append(t.order, e.ID) // a row keeps its first place
	}
	t.byID[e.ID] = bytes.Clone(e.Row)
	if n, err := strconv.ParseInt(e.ID, 10, 64); err == nil && n > r.counters[e.Table] {
		r.counters[e.Table] = n
	}
}

func (r *rows) entries() []wire {
	var out []wire
	for _, name := range r.tableSeq {
		t := r.tables[name]
		for _, id := range t.order {
			out = append(out, wire{Table: name, ID: id, Row: t.byID[id]})
		}
	}
	return out
}

// Journal is one run's journal: rows by table and id, kept on disk (a run file and an append-only events file) or in
// memory (a run that needn't survive the process). Every write is durable when Write returns, and a batch lands whole
// or not at all. Safe for concurrent use.
type Journal struct {
	mu       sync.Mutex
	dir      string // "" in memory
	readOnly bool
	data     *rows
	f        *os.File
	closed   bool
	broken   error
}

// Memory is a journal kept in memory: a run that no crash could resume (journal off).
func Memory() *Journal { return &Journal{data: newRows()} }

// runMeta is what RunFile holds.
type runMeta struct {
	Format  int    `json:"format"`
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Created int64  `json:"created"`
}

// replay applies a run's events to r; it returns how many lines it took, and whether the last was torn.
func replay(dir string, r *rows) (int, bool, error) {
	meta, err := ReadFile(filepath.Join(dir, RunFile))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, false, fmt.Errorf("journal: %s is not a run: it has no %s", dir, RunFile)
		}
		return 0, false, err
	}
	var m runMeta
	if json.Unmarshal(meta, &m) != nil {
		return 0, false, fmt.Errorf("journal: %s is damaged", filepath.Join(dir, RunFile))
	}
	if m.Format < 1 || m.Format > Format {
		return 0, false, fmt.Errorf("journal: this run was written by a newer SDK (journal format %d); upgrade to resume it", m.Format)
	}
	raw, err := ReadFile(filepath.Join(dir, EventsFile))
	if errors.Is(err, fs.ErrNotExist) || len(raw) == 0 {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	parts := bytes.Split(raw, []byte{'\n'})
	// A file that doesn't end in a newline ends in a torn line: its write never finished, so its action never happened.
	torn := len(parts[len(parts)-1]) > 0
	lines := parts[:len(parts)-1]
	for i, line := range lines {
		entries, ok := parseLine(line)
		if !ok {
			if i == len(lines)-1 && !torn {
				// The last line, damaged (a crash can leave zeros where it was being written): ignored.
				return i, true, nil
			}
			return 0, false, fmt.Errorf("journal: %s is damaged at line %d", filepath.Join(dir, EventsFile), i+1)
		}
		for _, e := range entries {
			r.apply(e)
		}
	}
	return len(lines), torn, nil
}

// parseLine is one events line's entries, or false when it isn't one.
func parseLine(line []byte) ([]wire, bool) {
	var entries []wire
	if json.Unmarshal(line, &entries) != nil || entries == nil {
		return nil, false
	}
	for _, e := range entries {
		if e.Table == "" {
			return nil, false
		}
		if !e.Remove {
			t := bytes.TrimSpace(e.Row)
			if len(t) == 0 || t[0] != '{' {
				return nil, false
			}
		}
	}
	return entries, true
}

// Open opens a run's journal to write: replayed, then compacted when it has more than one line or a torn one.
func Open(dir string) (*Journal, error) {
	j := &Journal{dir: dir, data: newRows()}
	lines, torn, err := replay(dir, j.data)
	if err != nil {
		return nil, err
	}
	if torn || lines > 1 {
		if err := j.rewrite(); err != nil {
			return nil, err
		}
	}
	if err := j.openEvents(); err != nil {
		return nil, err
	}
	FsyncDir(dir)
	return j, nil
}

// Read reads a run's journal without writing anything (a run another process may hold): its rows, read-only.
func Read(dir string) (*Journal, error) {
	j := &Journal{dir: dir, data: newRows(), readOnly: true}
	if _, _, err := replay(dir, j.data); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *Journal) openEvents() error {
	path := filepath.Join(j.dir, EventsFile)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if posix {
		_ = os.Chmod(path, 0o600)
	}
	j.f = f
	return nil
}

// rewrite writes every row as one line, atomically.
func (j *Journal) rewrite() error {
	var data []byte
	if all := j.data.entries(); len(all) > 0 {
		b, err := json.Marshal(all)
		if err != nil {
			return err
		}
		data = append(b, '\n')
	}
	return WriteAtomic(filepath.Join(j.dir, EventsFile), data)
}

// Durable reports whether the journal survives the process (a resumable run).
func (j *Journal) Durable() bool { return j.dir != "" && !j.readOnly }

// Dir is the run's directory ("" for a journal in memory).
func (j *Journal) Dir() string { return j.dir }

// writeFault is a test's fault (SetWriteFault): nil, or the function each write to a journal on disk asks first.
var writeFault atomic.Pointer[func(dir string, entries []Entry) error]

// SetWriteFault is for tests: until the returned restore is called, each write to a journal on disk first asks fault,
// with the run's directory and the entries, and an error it returns fails that write as a failed append does (the
// disk full, say): nothing is written, and the journal is broken from then on. Nil clears it.
func SetWriteFault(fault func(dir string, entries []Entry) error) (restore func()) {
	var p *func(string, []Entry) error
	if fault != nil {
		p = &fault
	}
	old := writeFault.Swap(p)
	return func() { writeFault.Store(old) }
}

// Write writes entries together, durably: after a crash all of them are there, or none.
func (j *Journal) Write(entries ...Entry) error {
	if len(entries) == 0 {
		return nil
	}
	ws := make([]wire, 0, len(entries))
	for _, e := range entries {
		if e.Table == "" {
			return errors.New("journal: an entry with no table")
		}
		w := wire{Table: e.Table, ID: e.ID, Remove: e.Remove}
		if !e.Remove {
			b, err := json.Marshal(e.Row)
			if err != nil {
				return fmt.Errorf("journal: encode a %s row: %w", e.Table, err)
			}
			if t := bytes.TrimSpace(b); len(t) == 0 || t[0] != '{' {
				return fmt.Errorf("journal: a %s row must be a JSON object", e.Table)
			}
			w.Row = b
		}
		ws = append(ws, w)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.check(); err != nil {
		return err
	}
	if j.dir != "" {
		if f := writeFault.Load(); f != nil {
			if err := (*f)(j.dir, entries); err != nil {
				j.broken = err
				return fmt.Errorf("journal: %w", err)
			}
		}
		line, err := json.Marshal(ws)
		if err != nil {
			return err
		}
		line = append(line, '\n')
		if _, err := j.f.Write(line); err != nil {
			// A line that may be half written: nothing more is appended after it, so on reopen it is the torn last line.
			j.broken = err
			return fmt.Errorf("journal: %w", err)
		}
		if err := j.f.Sync(); err != nil {
			j.broken = err
			return fmt.Errorf("journal: %w", err)
		}
	}
	for _, w := range ws {
		j.data.apply(w)
	}
	return nil
}

// Put puts one row (replacing any row with its id), durably.
func (j *Journal) Put(table, id string, row any) error {
	return j.Write(Entry{Table: table, ID: id, Row: row})
}

// Get decodes a row into out; false when there is none.
func (j *Journal) Get(table, id string, out any) (bool, error) {
	j.mu.Lock()
	var raw json.RawMessage
	if t := j.data.tables[table]; t != nil {
		raw = t.byID[id]
	}
	j.mu.Unlock()
	if raw == nil {
		return false, nil
	}
	return true, json.Unmarshal(raw, out)
}

// Rows is every row of a table, in the order their ids were first put; each is the caller's to decode.
func (j *Journal) Rows(table string) []json.RawMessage {
	j.mu.Lock()
	defer j.mu.Unlock()
	t := j.data.tables[table]
	if t == nil {
		return nil
	}
	out := make([]json.RawMessage, 0, len(t.order))
	for _, id := range t.order {
		out = append(out, bytes.Clone(t.byID[id]))
	}
	return out
}

// NextID is a fresh id for a row of table, unique in this journal: the rows of a table are numbered in order.
func (j *Journal) NextID(table string) string {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.data.counters[table]++
	return strconv.FormatInt(j.data.counters[table], 10)
}

// Compact rewrites the journal as one line, atomically.
func (j *Journal) Compact() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.check(); err != nil {
		return err
	}
	if j.dir == "" {
		return nil
	}
	_ = j.f.Close()
	j.f = nil
	if err := j.rewrite(); err != nil {
		j.broken = err
		return err
	}
	if err := j.openEvents(); err != nil {
		j.broken = err
		return err
	}
	return nil
}

// Close closes the journal; later writes fail. Safe to call twice.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	if j.f != nil {
		err := j.f.Close()
		j.f = nil
		return err
	}
	return nil
}

func (j *Journal) check() error {
	switch {
	case j.closed:
		return errors.New("journal: written after it was closed")
	case j.readOnly:
		return errors.New("journal: read only")
	case j.broken != nil:
		return fmt.Errorf("journal: a write failed; reopen the run to go on: %w", j.broken)
	}
	return nil
}
