// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Direct storage's Config fields (section 25.1), with the Python SDK's environment variables and defaults.

func TestConfig_DirectStorageDefaults(t *testing.T) {
	t.Setenv("TEEPIN_UPLOAD_WAIT", "")
	t.Setenv("TEEPIN_UPLOAD_CONCURRENCY", "")
	t.Setenv("TEEPIN_STATE_DIR", "")
	c := NewClient(Config{})
	if c.keyCacheTTL != 10*time.Minute || c.uploadWait != 10*time.Minute || c.uploadConcurrency != 32 {
		t.Fatalf("key cache %v, wait %v, concurrency %d", c.keyCacheTTL, c.uploadWait, c.uploadConcurrency)
	}
	if !strings.HasSuffix(c.stateDir, "teepin") || !filepath.IsAbs(c.stateDir) {
		t.Fatalf("state dir %s", c.stateDir)
	}
}

func TestConfig_DirectStorageReadsTheEnvironment(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TEEPIN_UPLOAD_WAIT", "2.5")
	t.Setenv("TEEPIN_UPLOAD_CONCURRENCY", "7")
	t.Setenv("TEEPIN_STATE_DIR", dir)
	c := NewClient(Config{})
	if c.uploadWait != 2500*time.Millisecond || c.uploadConcurrency != 7 || c.stateDir != dir {
		t.Fatalf("wait %v, concurrency %d, state %s", c.uploadWait, c.uploadConcurrency, c.stateDir)
	}
	// A value given wins over the environment.
	c = NewClient(Config{UploadWait: time.Second, UploadConcurrency: 3, StateDir: "elsewhere"})
	if c.uploadWait != time.Second || c.uploadConcurrency != 3 || c.stateDir != "elsewhere" {
		t.Fatalf("wait %v, concurrency %d, state %s", c.uploadWait, c.uploadConcurrency, c.stateDir)
	}
	// Values that aren't positive numbers are the defaults, as the Python SDK reads them.
	for _, v := range []string{"0", "-3", "soon", "NaN", "1e400"} {
		t.Setenv("TEEPIN_UPLOAD_WAIT", v)
		t.Setenv("TEEPIN_UPLOAD_CONCURRENCY", v)
		if c := NewClient(Config{}); c.uploadWait != 10*time.Minute || c.uploadConcurrency != 32 {
			t.Errorf("%q: wait %v, concurrency %d", v, c.uploadWait, c.uploadConcurrency)
		}
	}
}

func TestConfig_NegativeIsNone(t *testing.T) {
	c := NewClient(Config{KeyCacheTTL: -1, UploadWait: -1})
	if c.keyCacheTTL != 0 || c.uploadWait != 0 {
		t.Fatalf("key cache %v, wait %v", c.keyCacheTTL, c.uploadWait)
	}
}

func TestConfig_TheStateDirectoryPerSystem(t *testing.T) {
	t.Setenv("TEEPIN_STATE_DIR", "")
	t.Setenv("LOCALAPPDATA", filepath.Join("C:", "local"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(string(filepath.Separator), "xdg"))
	want := map[string]string{
		"windows": filepath.Join("C:", "local", "teepin"),
		"linux":   filepath.Join(string(filepath.Separator), "xdg", "teepin"),
	}[runtime.GOOS]
	if want != "" && defaultStateDir() != want {
		t.Fatalf("%s, want %s", defaultStateDir(), want)
	}
	if runtime.GOOS == "darwin" && !strings.HasSuffix(defaultStateDir(), filepath.Join("Library", "Application Support", "teepin")) {
		t.Fatal(defaultStateDir())
	}
}

// The fakes are for tests only: no file of the SDK itself imports them.
func TestTheFakesAreImportedOnlyByTests(t *testing.T) {
	var files []string
	for _, pattern := range []string{"*.go", "internal/*/*.go", "../examples/*/*.go"} {
		m, _ := filepath.Glob(pattern)
		files = append(files, m...)
	}
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") || strings.Contains(filepath.ToSlash(name), "internal/fakes/") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			if p, _ := strconv.Unquote(imp.Path.Value); strings.HasSuffix(p, "/internal/fakes") {
				t.Errorf("%s imports the fakes", name)
			}
		}
		checked++
	}
	if checked < 10 {
		t.Fatalf("only %d files checked", checked)
	}
}
