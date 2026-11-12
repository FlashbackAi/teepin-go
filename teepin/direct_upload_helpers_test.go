// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
	"github.com/FlashbackAi/teepin-go/teepin/internal/fakes"
)

// What the upload tests share: shrunk sizes (so large uploads and streams are quick), a client whose waits are
// recorded instead of slept, files on disk, the invariant checks (each file saved exactly once with its own bytes,
// nothing left behind), and a child process that dies at a chosen journal state, for the crash tests.

const (
	// crashed is the child's exit code when it died where it was told to.
	crashed = 77
	// childEnv carries the child's configuration (JSON).
	childEnv = "TEEPIN_TEST_UPLOAD_CHILD"
)

// testSizes are the engine's sizes shrunk as the Python and Node tests shrink them: 128 KiB parts, a 256 KiB pack
// limit. A pack's own sizes are section 28's defaults (Uploads.Limits).
var testSizes = direct.Sizes{
	PartRules:         direct.PartRules{DefaultBytes: 128 * kib, MinBytes: 64 * kib, MaxParts: 3000, StreamBytes: 128 * kib},
	PackObjectLimit:   256 * kib,
	StreamConcurrency: 4,
}

// oneFileAPack are pack limits that put each file in its own pack (a floor of one byte, a ceiling of two): for tests
// that need several packs from a few small files.
var oneFileAPack = func() direct.PackLimits {
	l := direct.DefaultLimits
	l.Floor, l.Ceiling = 1, 2
	return l
}()

func TestMain(m *testing.M) {
	if cfg := os.Getenv(childEnv); cfg != "" {
		os.Exit(uploadChild(cfg))
	}
	if cfg := os.Getenv(downloadChildEnv); cfg != "" {
		os.Exit(downloadChild(cfg))
	}
	os.Exit(m.Run())
}

// childConfig is what the crash child is told.
type childConfig struct {
	URL         string         `json:"url"`
	State       string         `json:"state"`
	Files       []FileToUpload `json:"files"`
	Pack        string         `json:"pack"`  // exit right after a pack's state is journaled
	Large       string         `json:"large"` // exit right after a large upload's state is journaled
	Sent        int            `json:"sent"`  // exit right after the nth part is journaled sent
	Sizes       direct.Sizes   `json:"sizes"`
	Concurrency int            `json:"concurrency"`
	// Speed fixes the measured speed (bytes per second) with a request budget of SpeedBudget; 0 measures.
	Speed       float64       `json:"speed"`
	SpeedBudget time.Duration `json:"speed_budget"`
	// Limits replaces the built-in pack limits (section 28) when set.
	Limits *direct.PackLimits `json:"limits"`
}

// uploadChild runs UploadFiles against the parent's fakes and exits as a crash would (os.Exit: no deferred cleanup,
// its journal and lock left behind) right after the chosen state is journaled, before the action it was written for.
func uploadChild(raw string) int {
	var cfg childConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	c := NewClient(Config{BaseURL: cfg.URL, APIKey: fakes.APIKey, StateDir: cfg.State,
		UploadConcurrency: cfg.Concurrency, UploadWait: 2 * time.Minute})
	c.sleep = func(ctx context.Context, d time.Duration) error { return ctx.Err() }
	up := c.Storage.uploads()
	up.Sizes = cfg.Sizes
	if cfg.Limits != nil {
		up.Limits = *cfg.Limits
	}
	if cfg.Speed > 0 {
		up.Speed.SetBudget(cfg.SpeedBudget)
		up.Speed.Set(cfg.Speed)
	}
	var mu sync.Mutex
	left := cfg.Sent
	up.Hooks.Saved = func(kind, state string) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case kind == "pack" && state == cfg.Pack, kind == "large" && state == cfg.Large:
			os.Exit(crashed)
		case kind == "part" && state == "sent" && cfg.Sent > 0:
			if left--; left == 0 {
				os.Exit(crashed)
			}
		}
	}
	res, err := c.Storage.UploadFiles(context.Background(), "photos", cfg.Files, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	var st []string
	for _, r := range res {
		st = append(st, r.Status)
	}
	fmt.Println(strings.Join(st, ","))
	return 0
}

// upEnv is an upload test's world: the fakes, a state directory, a source directory, and the waits a client asked for.
type upEnv struct {
	t      *testing.T
	f      *fakes.Fakes
	state  string
	src    string
	mu     sync.Mutex
	sleeps []time.Duration
}

// newUploads is the fakes (parts of any size, small read windows) with a state directory, and a client on them.
func newUploads(t *testing.T) (*upEnv, *Client) {
	t.Helper()
	f, _ := newDirect(t)
	e := &upEnv{t: t, f: f, state: t.TempDir(), src: t.TempDir()}
	return e, e.client(Config{})
}

// client is a client on the fakes and the shared state directory, with shrunk sizes and its waits recorded.
func (e *upEnv) client(cfg Config) *Client {
	e.t.Helper()
	cfg.StateDir = e.state
	if cfg.UploadWait == 0 {
		cfg.UploadWait = 2 * time.Minute
	}
	c := directClient(e.t, e.f, cfg)
	c.sleep = func(ctx context.Context, d time.Duration) error {
		e.mu.Lock()
		e.sleeps = append(e.sleeps, d)
		e.mu.Unlock()
		return ctx.Err()
	}
	c.Storage.uploads().Sizes = testSizes
	return c
}

// uploadsHTTP is the HTTP client c's uploads go out through: the shared engine's own client's (28.23), or c's own for
// an engine of its own. Tests that wrap the transport wrap this one.
func uploadsHTTP(c *Client) *http.Client {
	c.Storage.uploads()
	c.Storage.uploadsMu.Lock()
	defer c.Storage.uploadsMu.Unlock()
	if h := c.Storage.hold; h != nil && h.entry != nil {
		return h.entry.client.httpClient
	}
	return c.httpClient
}

func (e *upEnv) slept() []time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]time.Duration(nil), e.sleeps...)
}

// srcFile is one file written for a test.
type srcFile struct {
	key  string
	path string
	data []byte
}

// writeFiles writes n files of size + i random bytes under prefix, as keys "up/<prefix><i>.bin".
func (e *upEnv) writeFiles(n, size int, prefix string) []srcFile {
	e.t.Helper()
	dir := filepath.Join(e.src, "files")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		e.t.Fatal(err)
	}
	var out []srcFile
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("%s%d.bin", prefix, i)
		data := randomData(size + i)
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0o644); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, srcFile{key: "up/" + name, path: p, data: data})
	}
	return out
}

func toUpload(files []srcFile) []FileToUpload {
	out := make([]FileToUpload, 0, len(files))
	for _, f := range files {
		out = append(out, FileToUpload{Key: f.key, Path: f.path})
	}
	return out
}

func statuses(rs []UploadResult) string {
	var s []string
	for _, r := range rs {
		s = append(s, r.Status)
	}
	return strings.Join(s, ",")
}

func errorsOf(rs []UploadResult) string {
	var s []string
	for _, r := range rs {
		if r.Err != nil {
			s = append(s, r.Key+": "+r.Err.Error())
		}
	}
	return strings.Join(s, "; ")
}

func allSaved(rs []UploadResult) bool {
	for _, r := range rs {
		if !r.Saved() {
			return false
		}
	}
	return len(rs) > 0
}

// sealedKeys are the keys committed packs saved, once per pack that saved them.
func (e *upEnv) sealedKeys() []string {
	var keys []string
	e.f.API.Do(func() {
		for _, p := range e.f.Direct.Packs {
			if p.Status != "sealed" {
				continue
			}
			objs, _ := p.Result["objects"].([]map[string]any)
			for _, o := range objs {
				if o["result"] == "saved" {
					keys = append(keys, o["key"].(string))
				}
			}
		}
	})
	sort.Strings(keys)
	return keys
}

// packs counts the reservations made, by status ("" for all).
func (e *upEnv) packs(status string) int {
	n := 0
	e.f.API.Do(func() {
		for _, p := range e.f.Direct.Packs {
			if status == "" || p.Status == status {
				n++
			}
		}
	})
	return n
}

// assertSavedOnce checks each file reads back its own bytes and was saved by exactly one committed pack.
func (e *upEnv) assertSavedOnce(files []srcFile) {
	e.t.Helper()
	var want []string
	for _, f := range files {
		got, err := e.f.Direct.ReadBack("photos", f.key)
		if err != nil || !bytes.Equal(got, f.data) {
			e.t.Fatalf("%s reads back wrong: %v", f.key, err)
		}
		want = append(want, f.key)
	}
	sort.Strings(want)
	if got := e.sealedKeys(); strings.Join(got, ",") != strings.Join(want, ",") {
		e.t.Fatalf("saved by committed packs: %v, want each of %v once", got, want)
	}
}

// runIDs are the runs left in the state directory.
func (e *upEnv) runIDs() []string {
	entries, _ := os.ReadDir(filepath.Join(e.state, "uploads"))
	var out []string
	for _, x := range entries {
		if x.IsDir() {
			out = append(out, x.Name())
		}
	}
	return out
}

// assertStateClean checks no run state is left in the state directory (the history aside).
func (e *upEnv) assertStateClean() {
	e.t.Helper()
	entries, _ := os.ReadDir(filepath.Join(e.state, "uploads"))
	var left []string
	for _, x := range entries {
		if x.Name() != "history.log" {
			left = append(left, x.Name())
		}
	}
	if len(left) > 0 {
		e.t.Fatalf("run state left behind: %v", left)
	}
}

// openUploads counts the large uploads open at the fake.
func (e *upEnv) openUploads() int {
	n := 0
	e.f.API.Do(func() { n = len(e.f.Direct.OpenUploads()) })
	return n
}

// child runs the crash child against these fakes; it returns its exit code and output.
func (e *upEnv) child(cfg childConfig) (int, string) {
	e.t.Helper()
	cfg.URL, cfg.State = e.f.API.URL, e.state
	if cfg.Sizes.PackObjectLimit == 0 {
		cfg.Sizes = testSizes
	}
	raw, _ := json.Marshal(cfg)
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), childEnv+"="+string(raw))
	out, err := cmd.CombinedOutput()
	if cmd.ProcessState == nil {
		e.t.Fatalf("the child didn't run: %v", err)
	}
	return cmd.ProcessState.ExitCode(), string(out)
}

// crashAt runs files in a child that dies at a pack's state.
func (e *upEnv) crashAt(files []srcFile, pack string) {
	e.t.Helper()
	if code, out := e.child(childConfig{Files: toUpload(files), Pack: pack}); code != crashed {
		e.t.Fatalf("the child didn't crash at %s: %d %s", pack, code, out)
	}
}

func sha256Bytes(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}

// canned answers a route with these first, before acting.
func (e *upEnv) canned(route string, answers ...fakes.Canned) {
	e.f.API.Do(func() { e.f.API.Canned[route] = append(e.f.API.Canned[route], answers...) })
}

func refusal(status int, code string) fakes.Canned {
	return fakes.Canned{Status: status, Body: map[string]any{"error": strings.ReplaceAll(code, "_", " "), "code": code}}
}

// waitFor polls cond for up to d.
func waitFor(d time.Duration, cond func() bool) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}
