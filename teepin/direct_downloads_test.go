// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
	"github.com/FlashbackAi/teepin-go/teepin/internal/fakes"
	"github.com/FlashbackAi/teepin-go/teepin/internal/journal"
)

// Batch downloads, DownloadFiles and DownloadPrefix (TEEPIN-STORAGE-DESIGN.md sections 19.15, 22.5 and 25.1), ported
// from teepin-python's test_direct_downloads.py and teepin-node's downloads.test.mjs: sessions signed in batches,
// pieces verified into .teepin-part files and renamed only when whole, a journal so a stopped run goes on (in this
// process, after a cancel, or in another, after a crash), SkipExisting by size and SHA-256 before anything is signed,
// and keys that aren't safe paths refused. Against the fakes, with read windows of about 300 KB.

// dlEnv is a download test's world: the fakes, a state directory, and a client on both.
type dlEnv struct {
	t     *testing.T
	f     *fakes.Fakes
	c     *Client
	state string
}

func newDownloads(t *testing.T) *dlEnv {
	t.Helper()
	f, _ := newDirect(t)
	state := t.TempDir()
	return &dlEnv{t: t, f: f, c: directClient(t, f, Config{StateDir: state}), state: state}
}

// dl downloads keys to out; the call itself must not fail.
func (e *dlEnv) dl(keys []string, out string, o *DownloadFilesOptions) []DownloadResult {
	e.t.Helper()
	rs, err := e.c.Storage.DownloadFiles(ctx, "photos", keys, out, o)
	if err != nil {
		e.t.Fatalf("DownloadFiles: %v", err)
	}
	return rs
}

// runs are the state directory's download runs.
func (e *dlEnv) runs() []string {
	e.t.Helper()
	ids, err := journal.RunIDs(e.state, journal.Downloads)
	if err != nil {
		e.t.Fatal(err)
	}
	return ids
}

// partFiles are the part files under root.
func partFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && strings.HasSuffix(p, PartSuffix) {
			out = append(out, p)
		}
		return nil
	})
	return out
}

func byKey(rs []DownloadResult) map[string]DownloadResult {
	out := map[string]DownloadResult{}
	for _, r := range rs {
		out[r.Key] = r
	}
	return out
}

func mustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func exists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

// rangePath is the path a journal entry of a written piece is for ("" for another entry).
func rangePath(e journal.Entry) string {
	if e.Table != "ranges" || e.Remove {
		return ""
	}
	p, _, _ := strings.Cut(e.ID, "\x00")
	return p
}

// rangeStart is where a written piece's journal entry starts.
func rangeStart(e journal.Entry) int64 {
	_, bounds, _ := strings.Cut(e.ID, "\x00")
	a, _, _ := strings.Cut(bounds, "-")
	n, _ := strconv.ParseInt(a, 10, 64)
	return n
}

// pieceLog records the pieces each read verifies, by key: the start of each.
type pieceLog struct {
	mu  sync.Mutex
	got map[string][]int64
}

func watchFetches(c *Client) *pieceLog {
	f := &pieceLog{got: map[string][]int64{}}
	engine(c).Hooks.Plaintext = func(s *direct.Session, _ []byte, a, _ int64) {
		f.mu.Lock()
		f.got[s.Key] = append(f.got[s.Key], a)
		f.mu.Unlock()
	}
	return f
}

func (f *pieceLog) of(key string) []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.got[key])
}

func TestDownloads_ManyObjectsOfEveryKindOneResultEach(t *testing.T) {
	e := newDownloads(t)
	want := map[string][]byte{
		"a/small.png": smallObject(t, e.f, "a/small.png", 5000),
		"a/b/big.mp4": blob(t, e.f, "a/b/big.mp4", blobSize),
	}
	if _, err := e.c.Storage.PutObject(ctx, "photos", "empty", bytes.NewReader(nil), 0, nil); err != nil {
		t.Fatal(err)
	}
	want["empty"] = []byte{}
	want["old/server"] = randomData(3000)
	e.f.Direct.PutServerObject("photos", "old/server", want["old/server"])
	want["legacy"] = randomData(400_000)
	e.f.Direct.PutPlainBlob("photos", "legacy", want["legacy"], 0)
	e.f.API.PutObject("photos", "staged", []byte("still staged"), "") // still on the API's stage: failed, never read
	keys := []string{"a/small.png", "a/b/big.mp4", "empty", "old/server", "legacy", "staged", "missing"}
	out := filepath.Join(t.TempDir(), "out")
	var mu sync.Mutex
	var last [2]int64
	rs := e.dl(keys, out, &DownloadFilesOptions{Progress: func(done, total int64) {
		mu.Lock()
		last = [2]int64{done, total}
		mu.Unlock()
	}})
	if len(rs) != len(keys) {
		t.Fatalf("%d results", len(rs))
	}
	var total int64
	for i, r := range rs[:5] {
		if r.Key != keys[i] || !r.Saved() || r.Path != filepath.Join(out, filepath.FromSlash(r.Key)) {
			t.Fatalf("%+v", r)
		}
		if !bytes.Equal(mustRead(t, r.Path), want[r.Key]) {
			t.Fatalf("%s: wrong bytes", r.Key)
		}
		if r.Object == nil || r.Object.Size != int64(len(want[r.Key])) {
			t.Fatalf("%s: object %+v", r.Key, r.Object)
		}
		total += int64(len(want[r.Key]))
	}
	if r := rs[5]; r.Status != DownloadFailed || !errors.Is(r.Err, ErrObjectStaged) || exists(filepath.Join(out, "staged")) {
		t.Fatalf("staged: %+v", r)
	}
	if r := rs[6]; r.Status != DownloadFailed || !IsNotFound(r.Err) {
		t.Fatalf("missing: %+v", r)
	}
	if n := e.f.API.HitsOf("POST objects/read"); n != 1 {
		t.Fatalf("%d batches signed the first windows", n)
	}
	if e.f.API.HitsOf("GET object/content") != 0 {
		t.Fatal("something was read through the API")
	}
	if len(partFiles(t, out)) != 0 || len(e.runs()) != 0 {
		t.Fatalf("left behind: %v %v", partFiles(t, out), e.runs())
	}
	if last != [2]int64{total, total} {
		t.Fatalf("progress ended at %v, want %d", last, total)
	}
}

func TestDownloads_AThousandAndOneKeysTakeTwoBatches(t *testing.T) {
	e := newDownloads(t)
	keys := make([]string, 1001)
	for i := range keys {
		keys[i] = fmt.Sprintf("none/%d", i)
	}
	rs := e.dl(keys, t.TempDir(), nil)
	if n := e.f.API.HitsOf("POST objects/read"); n != 2 {
		t.Fatalf("%d batches", n)
	}
	for _, r := range rs {
		if r.Status != DownloadFailed || !IsNotFound(r.Err) {
			t.Fatalf("%+v", r)
		}
	}
}

func TestDownloads_PrefixMapsKeysUnderThePrefix(t *testing.T) {
	e := newDownloads(t)
	a := smallObject(t, e.f, "photos/2026/a.jpg", 2000)
	b := blob(t, e.f, "photos/2026/sub/b.jpg", 300_000)
	smallObject(t, e.f, "photos/2025/c.jpg", 100)
	out := filepath.Join(t.TempDir(), "out")
	rs, err := e.c.Storage.DownloadPrefix(ctx, "photos", "photos/2026/", out, nil)
	if err != nil || len(rs) != 2 || !rs[0].Saved() || !rs[1].Saved() {
		t.Fatalf("%+v %v", rs, err)
	}
	if !bytes.Equal(mustRead(t, filepath.Join(out, "a.jpg")), a) || !bytes.Equal(mustRead(t, filepath.Join(out, "sub", "b.jpg")), b) {
		t.Fatal("wrong bytes")
	}
	if exists(filepath.Join(out, "c.jpg")) {
		t.Fatal("an object outside the prefix was downloaded")
	}
}

func TestDownloads_PathsNameEachKeysPath(t *testing.T) {
	e := newDownloads(t)
	a := smallObject(t, e.f, "deep/a.jpg", 2000)
	out := t.TempDir()
	rs := e.dl([]string{"deep/a.jpg"}, out, &DownloadFilesOptions{Paths: map[string]string{"deep/a.jpg": "x/renamed.jpg"}})
	if !rs[0].Saved() || rs[0].Path != filepath.Join(out, "x", "renamed.jpg") || !bytes.Equal(mustRead(t, rs[0].Path), a) {
		t.Fatalf("%+v", rs[0])
	}
	rs = e.dl([]string{"deep/a.jpg"}, out, &DownloadFilesOptions{Paths: map[string]string{"deep/a.jpg": "../out.jpg"}})
	if rs[0].Status != DownloadFailed || !errors.Is(rs[0].Err, ErrUnsafePath) {
		t.Fatalf("a path out of the destination: %+v", rs[0])
	}
}

// --- SkipExisting ----------------------------------------------------------------------

func TestDownloads_SkipExistingComparesSizeAndSHA256BeforeSigning(t *testing.T) {
	e := newDownloads(t)
	same := smallObject(t, e.f, "same", 4000)
	other := smallObject(t, e.f, "other", 4000)
	longer := smallObject(t, e.f, "longer", 4000)
	out := t.TempDir()
	if err := os.WriteFile(filepath.Join(out, "same"), same, 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(out, "other"), randomData(4000), 0o644) // same size, other bytes
	_ = os.WriteFile(filepath.Join(out, "longer"), []byte("short"), 0o644)
	var signed []string
	e.c.Storage.downloads().Hooks.Sessions = func() { signed = append(signed, "batch") }
	r := byKey(e.dl([]string{"same", "other", "longer"}, out, &DownloadFilesOptions{SkipExisting: true}))
	if !r["same"].Skipped() || r["same"].Object == nil || !r["other"].Saved() || !r["longer"].Saved() {
		t.Fatalf("%+v", r)
	}
	if !bytes.Equal(mustRead(t, filepath.Join(out, "other")), other) || !bytes.Equal(mustRead(t, filepath.Join(out, "longer")), longer) {
		t.Fatal("wrong bytes")
	}
	// All three already there: nothing is signed, or billed.
	hits := e.f.API.HitsOf("POST objects/read")
	r = byKey(e.dl([]string{"same", "other", "longer"}, out, &DownloadFilesOptions{SkipExisting: true}))
	if !r["same"].Skipped() || !r["other"].Skipped() || !r["longer"].Skipped() || e.f.API.HitsOf("POST objects/read") != hits {
		t.Fatalf("%+v, %d batches", r, e.f.API.HitsOf("POST objects/read")-hits)
	}
}

func TestDownloads_SkipExistingWithAKeyThatIsGoneFailsItAlone(t *testing.T) {
	e := newDownloads(t)
	here := smallObject(t, e.f, "here", 3000)
	out := t.TempDir()
	_ = os.WriteFile(filepath.Join(out, "gone"), []byte("an old copy"), 0o644)
	r := byKey(e.dl([]string{"gone", "here"}, out, &DownloadFilesOptions{SkipExisting: true}))
	if r["gone"].Status != DownloadFailed || !IsNotFound(r["gone"].Err) || string(mustRead(t, filepath.Join(out, "gone"))) != "an old copy" {
		t.Fatalf("%+v", r["gone"])
	}
	if !r["here"].Saved() || !bytes.Equal(mustRead(t, filepath.Join(out, "here")), here) {
		t.Fatalf("%+v", r["here"])
	}
}

func TestDownloads_PrefixSkipsFromTheListing(t *testing.T) {
	e := newDownloads(t)
	same := smallObject(t, e.f, "p/same", 4000)
	out := t.TempDir()
	_ = os.WriteFile(filepath.Join(out, "same"), same, 0o644)
	rs, err := e.c.Storage.DownloadPrefix(ctx, "photos", "p/", out, &DownloadFilesOptions{SkipExisting: true})
	if err != nil || len(rs) != 1 || !rs[0].Skipped() {
		t.Fatalf("%+v %v", rs, err)
	}
	if e.f.API.HitsOf("POST objects/read") != 0 || e.f.API.HitsOf("GET object") != 0 {
		t.Fatal("the listing should have decided")
	}
}

func TestDownloads_ALegacyCompositeChecksumIsNeverTakenAsTheSame(t *testing.T) {
	e := newDownloads(t)
	data := randomData(5000)
	e.f.Direct.PutPlainBlob("photos", "legacy", data, 3) // its SHA-256 matches, but its ETag is "<sha>-3"
	out := t.TempDir()
	_ = os.WriteFile(filepath.Join(out, "legacy"), data, 0o644)
	if r := e.dl([]string{"legacy"}, out, &DownloadFilesOptions{SkipExisting: true}); !r[0].Saved() {
		t.Fatalf("%+v", r[0])
	}
}

// --- paths ---------------------------------------------------------------------------

func TestDownloads_AKeyThatIsntASafePathFailsAndWritesNothing(t *testing.T) {
	for _, key := range []string{"../evil", "a/../../evil", "/abs", "a//b", "a/./b", "."} {
		t.Run(key, func(t *testing.T) {
			e := newDownloads(t)
			e.f.API.PutObject("photos", key, []byte("x"), "")
			root := t.TempDir()
			r := e.dl([]string{key}, filepath.Join(root, "deep", "out"), nil)
			var unsafe *UnsafePathError
			if r[0].Status != DownloadFailed || !errors.As(r[0].Err, &unsafe) || !errors.Is(r[0].Err, ErrUnsafePath) {
				t.Fatalf("%+v", r[0])
			}
			if e.f.API.HitsOf("POST objects/read") != 0 {
				t.Fatal("refused only after signing")
			}
			if exists(filepath.Join(root, "evil")) || exists(filepath.Join(root, "deep", "evil")) {
				t.Fatal("written outside the destination")
			}
		})
	}
}

func TestDownloads_TheSameKeyTwiceIsOneDownload(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "a", 100)
	if r := e.dl([]string{"a", "a"}, t.TempDir(), nil); len(r) != 1 || !r[0].Saved() {
		t.Fatalf("%+v", r)
	}
}

func TestDownloads_AFolderMarkerIsSkipped(t *testing.T) {
	e := newDownloads(t)
	e.f.API.PutObject("photos", "dir/", nil, "")
	if r := e.dl([]string{"dir/"}, t.TempDir(), nil); !r[0].Skipped() || e.f.API.HitsOf("POST objects/read") != 0 {
		t.Fatalf("%+v", r)
	}
}

func TestDownloads_TwoKeysForOnePathSaveTheFirstAndFailTheSecond(t *testing.T) {
	// DownloadPrefix("p") maps both "p/a" and "pa" to "a": the first key is saved, the second fails, and both are
	// reported.
	e := newDownloads(t)
	first := smallObject(t, e.f, "p/a", 3000)
	smallObject(t, e.f, "pa", 4000)
	out := t.TempDir()
	rs, err := e.c.Storage.DownloadPrefix(ctx, "photos", "p", out, nil)
	r := byKey(rs)
	if err != nil || len(rs) != 2 || !r["p/a"].Saved() || !bytes.Equal(mustRead(t, filepath.Join(out, "a")), first) {
		t.Fatalf("%+v %v", rs, err)
	}
	if r["pa"].Status != DownloadFailed || !strings.Contains(r["pa"].Err.Error(), "both go to") || rs[1].Key != "pa" {
		t.Fatalf("%+v", r["pa"])
	}
}

func TestDownloads_KeysThatDifferByCaseCollideWhereNamesIgnoreCase(t *testing.T) {
	e := newDownloads(t)
	a := smallObject(t, e.f, "A.txt", 100)
	smallObject(t, e.f, "a.txt", 200)
	e.c.Storage.downloads().Hooks.CaseInsensitive = func(string) bool { return true }
	out := t.TempDir()
	r := byKey(e.dl([]string{"A.txt", "a.txt"}, out, nil))
	if !r["A.txt"].Saved() || !bytes.Equal(mustRead(t, filepath.Join(out, "A.txt")), a) {
		t.Fatalf("%+v", r["A.txt"])
	}
	if r["a.txt"].Status != DownloadFailed || !strings.Contains(r["a.txt"].Err.Error(), "both go to") {
		t.Fatalf("%+v", r["a.txt"])
	}
}

// Two keys the plan calls two paths but the file system takes for one file (NFC and NFD forms of a name on APFS,
// which fold doesn't join), here through the Target hook: the first renamed into place is saved, and the second's
// rename onto that file fails its key rather than replace it with both reported saved. A file that was there before
// the batch is replaced as usual.
func TestDownloads_TwoKeysTheFileSystemTakesForOneFileSaveOnlyOne(t *testing.T) {
	for _, workers := range []int{1, 8} {
		t.Run(fmt.Sprint(workers, " at a time"), func(t *testing.T) {
			e := newDownloads(t)
			data := map[string][]byte{"x1": smallObject(t, e.f, "x1", 3000), "x2": smallObject(t, e.f, "x2", 4000)}
			out := t.TempDir()
			one := filepath.Join(out, "x1")
			if err := os.WriteFile(one, []byte("there before the batch"), 0o644); err != nil {
				t.Fatal(err)
			}
			e.c.Storage.downloads().Hooks.Target = func(p string) string {
				if filepath.Base(p) == "x2" {
					return one
				}
				return p
			}
			rs := e.dl([]string{"x1", "x2"}, out, &DownloadFilesOptions{Concurrency: workers})
			var saved, failed []DownloadResult
			for _, r := range rs {
				switch {
				case r.Saved():
					saved = append(saved, r)
				case r.Status == DownloadFailed:
					failed = append(failed, r)
				}
			}
			if len(saved) != 1 || len(failed) != 1 || !strings.Contains(failed[0].Err.Error(), "both go to") {
				t.Fatalf("%+v", rs)
			}
			if !bytes.Equal(mustRead(t, one), data[saved[0].Key]) {
				t.Fatalf("%s holds another key's bytes than %s, the one saved", one, saved[0].Key)
			}
			if left, _ := os.ReadDir(out); len(left) != 1 || left[0].Name() != "x1" {
				t.Fatalf("left %v", left)
			}
		})
	}
}

func TestDownloads_TheCaseProbeTellsThisFileSystemAndLeavesNothing(t *testing.T) {
	root := t.TempDir()
	want := runtime.GOOS == "windows" || runtime.GOOS == "darwin"
	if runtime.GOOS == "darwin" {
		_, err := os.Stat(strings.ToUpper(root))
		want = err == nil
	}
	d := filepath.Join(root, "probe")
	if got := direct.CaseInsensitive(d); got != want {
		t.Fatalf("case insensitive: %v, want %v", got, want)
	}
	if left, _ := os.ReadDir(d); len(left) != 0 {
		t.Fatalf("left %v", left)
	}
}

// dirLink makes link a link to the folder target: a symbolic link, or on Windows without the privilege a junction
// (which needs none).
func dirLink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err == nil {
		return
	}
	if runtime.GOOS == "windows" {
		if out, err := exec.Command("cmd", "/c", "mklink", "/J", link, target).CombinedOutput(); err == nil {
			return
		} else {
			t.Fatalf("mklink /J: %v %s", err, out)
		}
	}
	t.Skip("links aren't permitted here")
}

func TestDownloads_ALinkUnderTheDestinationCantLeadOutOfIt(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "sub/x", 100)
	root := t.TempDir()
	outside, out := filepath.Join(root, "outside"), filepath.Join(root, "out")
	_ = os.Mkdir(outside, 0o755)
	_ = os.Mkdir(out, 0o755)
	dirLink(t, outside, filepath.Join(out, "sub"))
	r := e.dl([]string{"sub/x"}, out, nil)
	if r[0].Status != DownloadFailed || !errors.Is(r[0].Err, ErrUnsafePath) || !strings.Contains(r[0].Err.Error(), "link") {
		t.Fatalf("%+v", r[0])
	}
	if left, _ := os.ReadDir(outside); len(left) != 0 || e.f.API.HitsOf("POST objects/read") != 0 {
		t.Fatalf("written outside: %v", left)
	}
}

func TestDownloads_ALinkToNothingUnderTheDestinationIsRefusedNeverFollowed(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "sub/x", 100)
	root := t.TempDir()
	out := filepath.Join(root, "out")
	_ = os.Mkdir(out, 0o755)
	nowhere := filepath.Join(root, "outside", "not-yet") // making sub's folder would make this
	dirLink(t, nowhere, filepath.Join(out, "sub"))
	r := e.dl([]string{"sub/x"}, out, nil)
	if r[0].Status != DownloadFailed || !errors.Is(r[0].Err, ErrUnsafePath) || exists(filepath.Join(root, "outside")) {
		t.Fatalf("%+v", r[0])
	}
}

func TestDownloads_APartFileThatIsALinkIsRefused(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "x", 100)
	root := t.TempDir()
	victim, out := filepath.Join(root, "victim"), filepath.Join(root, "out")
	_ = os.WriteFile(victim, []byte("keep me"), 0o644)
	_ = os.Mkdir(out, 0o755)
	if err := os.Symlink(victim, filepath.Join(out, "x"+PartSuffix)); err != nil {
		t.Skip("symbolic links to files aren't permitted here")
	}
	r := e.dl([]string{"x"}, out, nil)
	if r[0].Status != DownloadFailed || !errors.Is(r[0].Err, ErrUnsafePath) || string(mustRead(t, victim)) != "keep me" {
		t.Fatalf("%+v", r[0])
	}
}

func TestDownloads_APartFileThatIsALinkToAFolderIsRefused(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "x", 100)
	root := t.TempDir()
	outside, out := filepath.Join(root, "outside"), filepath.Join(root, "out")
	_ = os.Mkdir(outside, 0o755)
	_ = os.Mkdir(out, 0o755)
	dirLink(t, outside, filepath.Join(out, "x"+PartSuffix))
	r := e.dl([]string{"x"}, out, nil)
	if r[0].Status != DownloadFailed || !errors.Is(r[0].Err, ErrUnsafePath) || !strings.Contains(r[0].Err.Error(), "link") {
		t.Fatalf("%+v", r[0])
	}
	if left, _ := os.ReadDir(outside); len(left) != 0 || e.f.API.HitsOf("POST objects/read") != 0 {
		t.Fatalf("written outside: %v", left)
	}
}

func TestDownloads_AHardLinkedPartFileIsNeverWrittenThrough(t *testing.T) {
	e := newDownloads(t)
	data := smallObject(t, e.f, "x", 100)
	root := t.TempDir()
	victim, out := filepath.Join(root, "victim"), filepath.Join(root, "out")
	_ = os.WriteFile(victim, []byte("keep me"), 0o644)
	_ = os.Mkdir(out, 0o755)
	if err := os.Link(victim, filepath.Join(out, "x"+PartSuffix)); err != nil {
		t.Fatal(err)
	}
	r := e.dl([]string{"x"}, out, nil)
	if !r[0].Saved() || !bytes.Equal(mustRead(t, filepath.Join(out, "x")), data) || string(mustRead(t, victim)) != "keep me" {
		t.Fatalf("%+v", r[0])
	}
}

// stopAfter cancels a download as the nth piece of key begins (counted from 1), so the pieces before it are written
// and journaled.
func stopAfter(c *Client, key string, n int) (context.Context, context.CancelFunc) {
	cctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	seen := 0
	engine(c).Hooks.Plaintext = func(s *direct.Session, _ []byte, _, _ int64) {
		if s.Key != key {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if seen++; seen == n {
			cancel()
		}
	}
	return cctx, cancel
}

func TestDownloads_AHardLinkedPartFileIsntResumed(t *testing.T) {
	e := newDownloads(t)
	data := blob(t, e.f, "big", blobSize)
	root := t.TempDir()
	out := filepath.Join(root, "out")
	cctx, cancel := stopAfter(e.c, "big", 3)
	defer cancel()
	if _, err := e.c.Storage.DownloadFiles(cctx, "photos", []string{"big"}, out, &DownloadFilesOptions{Concurrency: 1}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	engine(e.c).Hooks.Plaintext = nil
	part := filepath.Join(out, "big"+PartSuffix)
	if err := os.Link(part, filepath.Join(root, "victim")); err != nil { // another name for the part's data
		t.Fatal(err)
	}
	before := mustRead(t, filepath.Join(root, "victim"))
	r := e.dl([]string{"big"}, out, nil)
	if !r[0].Saved() || r[0].Resumed || !bytes.Equal(mustRead(t, filepath.Join(out, "big")), data) {
		t.Fatalf("%+v", r[0])
	}
	if !bytes.Equal(mustRead(t, filepath.Join(root, "victim")), before) {
		t.Fatal("the other name's data was written")
	}
}

func TestDownloads_ALinkMadeAfterThePlanIsCaughtBeforeWritingForEachObject(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "sub/x", 100)
	smallObject(t, e.f, "sub/y", 200)
	root := t.TempDir()
	outside, out := filepath.Join(root, "outside"), filepath.Join(root, "out")
	_ = os.Mkdir(outside, 0o755)
	_ = os.Mkdir(out, 0o755)
	e.c.Storage.downloads().Hooks.Sessions = func() { dirLink(t, outside, filepath.Join(out, "sub")) } // after the plan
	for _, r := range e.dl([]string{"sub/x", "sub/y"}, out, nil) {
		if r.Status != DownloadFailed || !errors.Is(r.Err, ErrUnsafePath) || !strings.Contains(r.Err.Error(), "link") {
			t.Fatalf("%+v", r)
		}
	}
	if left, _ := os.ReadDir(outside); len(left) != 0 {
		t.Fatalf("written outside: %v", left)
	}
}

func TestDownloads_ALinkMadeAfterThePartFileIsCaughtBeforeTheRename(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "sub/x", 100)
	root := t.TempDir()
	outside, out := filepath.Join(root, "outside"), filepath.Join(root, "out")
	_ = os.Mkdir(outside, 0o755)
	_ = os.Mkdir(out, 0o755)
	// Once the piece is written, its folder is moved out of the destination and a link to it put in its place: the
	// part file is still this run's own, but renaming it would put the object outside.
	var hookErr error
	e.c.Storage.downloads().Hooks.Journaled = func(entries []journal.Entry) {
		for _, en := range entries {
			if rangePath(en) == "" {
				continue
			}
			if err := os.Rename(filepath.Join(out, "sub"), filepath.Join(outside, "moved")); err != nil {
				hookErr = err
				return
			}
			dirLink(t, filepath.Join(outside, "moved"), filepath.Join(out, "sub"))
		}
	}
	r := e.dl([]string{"sub/x"}, out, nil)
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if r[0].Status != DownloadFailed || !errors.Is(r[0].Err, ErrUnsafePath) {
		t.Fatalf("%+v", r[0])
	}
	if left, _ := os.ReadDir(filepath.Join(outside, "moved")); len(left) != 1 || left[0].Name() != "x"+PartSuffix {
		t.Fatalf("renamed or removed out there: %v", left) // never renamed, nor removed, outside
	}
}

func TestDownloads_APartFileReplacedWhileItIsWrittenIsNeverWrittenThroughNorRenamed(t *testing.T) {
	e := newDownloads(t)
	blob(t, e.f, "big", blobSize)
	smallObject(t, e.f, "small", 3000)
	root := t.TempDir()
	out := filepath.Join(root, "out")
	victims := map[string]string{filepath.Join(out, "big"): filepath.Join(root, "victim-big"),
		filepath.Join(out, "small"): filepath.Join(root, "victim-small")}
	for _, v := range victims {
		_ = os.WriteFile(v, []byte("keep me"), 0o644)
	}
	// After big's first piece (its later pieces open the part file again), and after small's only piece (before its
	// rename), the part file is swapped for another name of a victim's data.
	var hookErr error
	e.c.Storage.downloads().Hooks.Journaled = func(entries []journal.Entry) {
		for _, en := range entries {
			p := rangePath(en)
			v, ok := victims[p]
			if !ok {
				continue
			}
			delete(victims, p)
			if err := os.Remove(p + PartSuffix); err != nil {
				hookErr = err
			}
			if err := os.Link(v, p+PartSuffix); err != nil {
				hookErr = err
			}
		}
	}
	r := byKey(e.dl([]string{"big", "small"}, out, &DownloadFilesOptions{Concurrency: 1}))
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	for _, k := range []string{"big", "small"} {
		if r[k].Status != DownloadFailed || !errors.Is(r[k].Err, ErrUnsafePath) {
			t.Fatalf("%s: %+v", k, r[k])
		}
		if string(mustRead(t, filepath.Join(root, "victim-"+k))) != "keep me" || exists(filepath.Join(out, k)) {
			t.Fatalf("%s: written through, or renamed", k)
		}
	}
}

// --- stopping and going on ----------------------------------------------------------------

func TestDownloads_ACancelledRunGoesOnFromItsWrittenPieces(t *testing.T) {
	e := newDownloads(t)
	data := blob(t, e.f, "big", blobSize)
	small := smallObject(t, e.f, "small", 3000)
	out := filepath.Join(t.TempDir(), "out")
	var written []int64
	e.c.Storage.downloads().Hooks.Journaled = func(entries []journal.Entry) {
		for _, en := range entries {
			if p := rangePath(en); p == filepath.Join(out, "big") {
				written = append(written, rangeStart(en))
			}
		}
	}
	cctx, cancel := stopAfter(e.c, "big", 3) // the third piece begins: two are written
	defer cancel()
	rs, err := e.c.Storage.DownloadFiles(cctx, "photos", []string{"big", "small"}, out, &DownloadFilesOptions{Concurrency: 1})
	if !errors.Is(err, context.Canceled) || len(rs) != 2 || !rs[0].Pending() || !rs[1].Pending() {
		t.Fatalf("%+v %v", rs, err)
	}
	if len(e.runs()) != 1 || !exists(filepath.Join(out, "big"+PartSuffix)) || exists(filepath.Join(out, "big")) {
		t.Fatalf("runs %v", e.runs())
	}
	if len(written) != 2 {
		t.Fatalf("written %v", written)
	}
	e.c.Storage.downloads().Hooks.Journaled = nil
	seen := watchFetches(e.c)
	r := byKey(e.dl([]string{"big", "small"}, out, nil))
	if !r["big"].Saved() || !r["big"].Resumed || !bytes.Equal(mustRead(t, filepath.Join(out, "big")), data) {
		t.Fatalf("%+v", r["big"])
	}
	if !r["small"].Saved() || !bytes.Equal(mustRead(t, filepath.Join(out, "small")), small) {
		t.Fatalf("%+v", r["small"])
	}
	for _, a := range seen.of("big") {
		if slices.Contains(written, a) {
			t.Fatalf("piece %d fetched again (written %v)", a, written)
		}
	}
	if len(e.runs()) != 0 || len(partFiles(t, out)) != 0 {
		t.Fatal("left behind")
	}
}

func TestDownloads_AReplacedObjectStartsAgain(t *testing.T) {
	e := newDownloads(t)
	blob(t, e.f, "big", blobSize)
	out := filepath.Join(t.TempDir(), "out")
	cctx, cancel := stopAfter(e.c, "big", 2)
	defer cancel()
	if _, err := e.c.Storage.DownloadFiles(cctx, "photos", []string{"big"}, out, &DownloadFilesOptions{Concurrency: 1}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	engine(e.c).Hooks.Plaintext = nil
	data := blob(t, e.f, "big", blobSize) // a new version
	r := e.dl([]string{"big"}, out, nil)
	if !r[0].Saved() || r[0].Resumed || !bytes.Equal(mustRead(t, filepath.Join(out, "big")), data) || len(e.runs()) != 0 {
		t.Fatalf("%+v %v", r[0], e.runs())
	}
}

func TestDownloads_APassingErrorLeavesTheObjectPendingThenItGoesOn(t *testing.T) {
	e := newDownloads(t)
	data := blob(t, e.f, "big", blobSize)
	out := filepath.Join(t.TempDir(), "out")
	e.f.Network.Do(func() { e.f.Network.Fail["GET"] = 10_000 })
	r := e.dl([]string{"big"}, out, nil)
	if !r[0].Pending() || r[0].Err == nil || len(e.runs()) != 1 || !exists(filepath.Join(out, "big"+PartSuffix)) {
		t.Fatalf("%+v %v", r[0], e.runs())
	}
	e.f.Network.Do(func() { e.f.Network.Fail["GET"] = 0 })
	r = e.dl([]string{"big"}, out, nil)
	if !r[0].Saved() || !bytes.Equal(mustRead(t, filepath.Join(out, "big")), data) || len(e.runs()) != 0 {
		t.Fatalf("%+v %v", r[0], e.runs())
	}
}

func TestDownloads_AConnectionDroppedEveryTimeLeavesTheObjectPending(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "small", 3000)
	e.f.Network.Do(func() { e.f.Network.Drop["GET"] = 10_000 })
	out := t.TempDir()
	r := e.dl([]string{"small"}, out, nil)
	if !r[0].Pending() || !IsRetryable(r[0].Err) || !exists(filepath.Join(out, "small"+PartSuffix)) {
		t.Fatalf("%+v", r[0])
	}
}

func TestDownloads_APieceThatComesUpShortIsNeverJournaledNorSaved(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "small", 3000)
	d := e.c.Storage.downloads()
	d.Hooks.Chunk = func(c []byte) []byte { return c[:len(c)-1] } // a byte lost on the way
	var journaled []string
	d.Hooks.Journaled = func(entries []journal.Entry) {
		for _, en := range entries {
			journaled = append(journaled, en.Table)
		}
	}
	out := t.TempDir()
	r := e.dl([]string{"small"}, out, nil)
	if r[0].Status != DownloadFailed || reasonOf(r[0].Err) != "truncated" || slices.Contains(journaled, "ranges") || exists(filepath.Join(out, "small")) {
		t.Fatalf("%+v %v", r[0], journaled)
	}
}

func TestDownloads_ALocalErrorLeavesTheObjectPendingAndTheNextRunOnlyRenamesIt(t *testing.T) {
	e := newDownloads(t)
	data := smallObject(t, e.f, "small", 3000)
	out := t.TempDir()
	_ = os.MkdirAll(filepath.Join(out, "small", "inside"), 0o755) // a folder where the file goes
	r := e.dl([]string{"small"}, out, nil)
	if !r[0].Pending() || !exists(filepath.Join(out, "small"+PartSuffix)) || len(e.runs()) != 1 {
		t.Fatalf("%+v", r[0])
	}
	_ = os.RemoveAll(filepath.Join(out, "small"))
	seen := watchFetches(e.c)
	r = e.dl([]string{"small"}, out, nil)
	if !r[0].Saved() || !r[0].Resumed || !bytes.Equal(mustRead(t, filepath.Join(out, "small")), data) {
		t.Fatalf("%+v", r[0])
	}
	if len(seen.of("small")) != 0 || len(e.runs()) != 0 {
		t.Fatal("every piece was written: only the rename was left")
	}
}

func TestDownloads_ARevokedKeyStopsTheRunAndTheRestWait(t *testing.T) {
	e := newDownloads(t)
	blob(t, e.f, "big", blobSize)
	for i := range 3 {
		smallObject(t, e.f, fmt.Sprintf("s%d", i), 2000)
	}
	revoked := fakes.Canned{Status: http.StatusUnauthorized, Body: map[string]any{"error": "revoked", "code": "invalid_api_key"}}
	e.f.API.Do(func() {
		for range 50 {
			e.f.API.Canned["POST object/read"] = append(e.f.API.Canned["POST object/read"], revoked)
		}
	})
	rs := e.dl([]string{"big", "s0", "s1", "s2"}, t.TempDir(), &DownloadFilesOptions{Concurrency: 1})
	for _, r := range rs {
		if !r.Pending() || !IsUnauthorized(r.Err) {
			t.Fatalf("%+v", r)
		}
	}
	if len(e.runs()) != 1 {
		t.Fatal("the run is kept")
	}
}

func TestDownloads_DamageFailsOneObjectAndNotTheRest(t *testing.T) {
	e := newDownloads(t)
	blob(t, e.f, "big", blobSize)
	small := smallObject(t, e.f, "small", 3000)
	e.f.Damage("photos", "big", 500_000)
	out := t.TempDir()
	r := byKey(e.dl([]string{"big", "small"}, out, nil))
	var die *DataIntegrityError
	if r["big"].Status != DownloadFailed || !errors.As(r["big"].Err, &die) || exists(filepath.Join(out, "big")) {
		t.Fatalf("%+v", r["big"])
	}
	if !r["small"].Saved() || !bytes.Equal(mustRead(t, filepath.Join(out, "small")), small) || len(partFiles(t, out)) != 0 {
		t.Fatalf("%+v", r["small"])
	}
}

func TestDownloads_AnErasedKeyFailsItsObjectAlone(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "gone", 3000)
	keep := smallObject(t, e.f, "kept", 3000)
	e.f.API.Do(func() { e.f.Direct.Erased = map[string]bool{"gone": true} })
	out := t.TempDir()
	r := byKey(e.dl([]string{"gone", "kept"}, out, nil))
	if r["gone"].Status != DownloadFailed || !IsKeyErased(r["gone"].Err) {
		t.Fatalf("%+v", r["gone"])
	}
	if !r["kept"].Saved() || !bytes.Equal(mustRead(t, filepath.Join(out, "kept")), keep) {
		t.Fatalf("%+v", r["kept"])
	}
}

func TestDownloads_WithoutAJournalNothingIsKept(t *testing.T) {
	e := newDownloads(t)
	blob(t, e.f, "big", blobSize)
	e.f.Network.Do(func() { e.f.Network.Fail["GET"] = 10_000 })
	r := e.dl([]string{"big"}, t.TempDir(), &DownloadFilesOptions{NoJournal: true})
	if !r[0].Pending() || len(e.runs()) != 0 {
		t.Fatalf("%+v %v", r[0], e.runs())
	}
}

func TestDownloads_AWrongChecksumFailsTheObjectInABatch(t *testing.T) {
	e := newDownloads(t)
	blob(t, e.f, "big", blobSize)
	e.f.API.Do(func() { e.f.API.Buckets["photos"]["big"].SHA = strings.Repeat("0", 64) })
	out := t.TempDir()
	r := e.dl([]string{"big"}, out, nil)
	if r[0].Status != DownloadFailed || !errors.Is(r[0].Err, ErrChecksumMismatch) || exists(filepath.Join(out, "big")) || len(partFiles(t, out)) != 0 {
		t.Fatalf("%+v", r[0])
	}
}

func TestDownloads_EachPieceIsFsyncedBeforeItIsJournaled(t *testing.T) {
	e := newDownloads(t)
	blob(t, e.f, "big", blobSize)
	d := e.c.Storage.downloads()
	var order []string
	d.Hooks.Synced = func(string) { order = append(order, "fsync") }
	d.Hooks.Journaled = func(entries []journal.Entry) {
		for _, en := range entries {
			if rangePath(en) != "" {
				order = append(order, "journal")
			}
		}
	}
	r := e.dl([]string{"big"}, t.TempDir(), &DownloadFilesOptions{Concurrency: 1})
	if !r[0].Saved() || len(order) != 8 {
		t.Fatalf("%+v %v", r[0], order)
	}
	for i := 0; i < len(order); i += 2 {
		if order[i] != "fsync" || order[i+1] != "journal" {
			t.Fatalf("order %v", order)
		}
	}
}

func TestDownloads_ASavedFileHasTheObjectsUploadTime(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "small", 3000)
	when := time.Date(2021, 5, 4, 3, 2, 1, 0, time.UTC)
	e.f.API.Do(func() { e.f.API.Buckets["photos"]["small"].Updated = when })
	out := t.TempDir()
	r := e.dl([]string{"small"}, out, nil)
	fi, err := os.Stat(filepath.Join(out, "small"))
	if !r[0].Saved() || r[0].Object.UploadedAt == nil || !r[0].Object.UploadedAt.Equal(when) || err != nil || !fi.ModTime().Equal(when) {
		t.Fatalf("%+v %v %v", r[0], fi, err)
	}
}

func TestDownloads_ARunAnotherHolderIsWorkingOnIsLeftAlone(t *testing.T) {
	e := newDownloads(t)
	data := blob(t, e.f, "big", blobSize)
	out := t.TempDir()
	e.f.Network.Do(func() { e.f.Network.Fail["GET"] = 10_000 })
	if r := e.dl([]string{"big"}, out, nil); !r[0].Pending() {
		t.Fatalf("%+v", r[0])
	}
	held := e.runs()
	rd, err := journal.LockRun(e.state, journal.Downloads, held[0]) // another holder opens that run
	if err != nil || rd == nil {
		t.Fatal(err)
	}
	defer rd.Release()
	e.f.Network.Do(func() { e.f.Network.Fail["GET"] = 0 })
	r := e.dl([]string{"big"}, out, nil)
	if !r[0].Saved() || r[0].Resumed || !bytes.Equal(mustRead(t, filepath.Join(out, "big")), data) || !slices.Contains(e.runs(), held[0]) {
		t.Fatalf("%+v %v", r[0], e.runs())
	}
}

func TestDownloads_ACancelWhileTheSessionsAreSignedReturnsItAndTheNextRunClearsTheRun(t *testing.T) {
	e := newDownloads(t)
	data := smallObject(t, e.f, "small", 3000)
	cctx, cancel := context.WithCancel(ctx)
	e.c.Storage.downloads().Hooks.Sessions = cancel
	out := t.TempDir()
	if _, err := e.c.Storage.DownloadFiles(cctx, "photos", []string{"small"}, out, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(e.runs()) != 1 {
		t.Fatal("the stopped run is kept")
	}
	e.c.Storage.downloads().Hooks.Sessions = nil
	r := e.dl([]string{"small"}, out, nil)
	if !r[0].Saved() || !bytes.Equal(mustRead(t, filepath.Join(out, "small")), data) || len(e.runs()) != 0 {
		t.Fatalf("%+v %v", r[0], e.runs())
	}
}

func TestDownloads_ACancelledContextStartsNothing(t *testing.T) {
	e := newDownloads(t)
	smallObject(t, e.f, "small", 3000)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	out := filepath.Join(t.TempDir(), "out")
	if _, err := e.c.Storage.DownloadFiles(cctx, "photos", []string{"small"}, out, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if exists(out) || len(e.runs()) != 0 || e.f.API.HitsOf("POST objects/read") != 0 {
		t.Fatal("something started")
	}
}

func TestDownloads_ACancelStopsEveryWorkerAndTheNextRunGoesOn(t *testing.T) {
	e := newDownloads(t)
	want := map[string][]byte{}
	var keys []string
	for i := range 3 {
		k := fmt.Sprintf("big%d", i)
		want[k] = blob(t, e.f, k, blobSize)
		keys = append(keys, k)
	}
	tr := newTracked()
	c := directClient(t, e.f, Config{StateDir: e.state, HTTPClient: &http.Client{Transport: tr}})
	cctx, cancel := stopAfter(c, "big1", 2)
	defer cancel()
	out := t.TempDir()
	rs, err := c.Storage.DownloadFiles(cctx, "photos", keys, out, &DownloadFilesOptions{Concurrency: 4})
	if !errors.Is(err, context.Canceled) || len(rs) != 3 {
		t.Fatalf("%+v %v", rs, err)
	}
	if live := tr.running(); len(live) != 0 {
		t.Fatalf("still running after the call returned: %v", live)
	}
	r := byKey(e.dl(keys, out, nil))
	for _, k := range keys {
		if !r[k].Saved() || !bytes.Equal(mustRead(t, filepath.Join(out, k)), want[k]) {
			t.Fatalf("%s: %+v", k, r[k])
		}
	}
	if len(e.runs()) != 0 || len(partFiles(t, out)) != 0 {
		t.Fatal("left behind")
	}
}

// --- resume, listing, cleanup -----------------------------------------------------------

func TestDownloads_ResumeFinishesAnInterruptedRunAndListShowsIt(t *testing.T) {
	e := newDownloads(t)
	data := blob(t, e.f, "big", blobSize)
	small := smallObject(t, e.f, "small", 3000)
	out := filepath.Join(t.TempDir(), "out")
	cctx, cancel := stopAfter(e.c, "big", 2)
	defer cancel()
	if _, err := e.c.Storage.DownloadFiles(cctx, "photos", []string{"big", "small"}, out, &DownloadFilesOptions{Concurrency: 1}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	engine(e.c).Hooks.Plaintext = nil
	runs, err := e.c.Storage.ListDownloadRuns(ctx)
	if err != nil || len(runs) != 1 || runs[0].Pending != 2 || runs[0].Bucket != "photos" || runs[0].InUse || runs[0].Expired {
		t.Fatalf("%+v %v", runs, err)
	}
	if abs, _ := filepath.Abs(out); runs[0].Dir != abs {
		t.Fatalf("dir %s", runs[0].Dir)
	}
	other := directClient(t, e.f, Config{StateDir: e.state}) // another process, as far as the run can tell
	var last int64
	rs, err := other.Storage.ResumeDownloads(ctx, func(done, _ int64) { last = done })
	r := byKey(rs)
	if err != nil || len(rs) != 2 || !r["big"].Saved() || !r["big"].Resumed || !r["small"].Saved() {
		t.Fatalf("%+v %v", rs, err)
	}
	if !bytes.Equal(mustRead(t, filepath.Join(out, "big")), data) || !bytes.Equal(mustRead(t, filepath.Join(out, "small")), small) {
		t.Fatal("wrong bytes")
	}
	if last != blobSize+3000 || len(e.runs()) != 0 {
		t.Fatalf("progress %d, runs %v", last, e.runs())
	}
	if rs, err := other.Storage.ResumeDownloads(ctx, nil); err != nil || len(rs) != 0 {
		t.Fatalf("nothing left: %+v %v", rs, err)
	}
}

func TestDownloads_ResumeLeavesARunInUseAlone(t *testing.T) {
	e := newDownloads(t)
	blob(t, e.f, "big", blobSize)
	e.f.Network.Do(func() { e.f.Network.Fail["GET"] = 10_000 })
	e.dl([]string{"big"}, t.TempDir(), nil)
	e.f.Network.Do(func() { e.f.Network.Fail["GET"] = 0 })
	held := e.runs()
	rd, err := journal.LockRun(e.state, journal.Downloads, held[0])
	if err != nil || rd == nil {
		t.Fatal(err)
	}
	defer rd.Release()
	runs, _ := e.c.Storage.ListDownloadRuns(ctx)
	if len(runs) != 1 || !runs[0].InUse {
		t.Fatalf("%+v", runs)
	}
	if rs, err := e.c.Storage.ResumeDownloads(ctx, nil); err != nil || len(rs) != 0 || !slices.Equal(e.runs(), held) {
		t.Fatalf("%+v %v", rs, err)
	}
}

func TestDownloads_CleanRemovesOnlyOldRunsWithTheirPartFiles(t *testing.T) {
	e := newDownloads(t)
	blob(t, e.f, "big", blobSize)
	e.f.Network.Do(func() { e.f.Network.Fail["GET"] = 10_000 })
	oldOut, newOut := t.TempDir(), t.TempDir()
	e.dl([]string{"big"}, oldOut, nil)
	old := e.runs()[0]
	ageRun(t, e.state, old, 8*24*time.Hour)
	e.dl([]string{"big"}, newOut, nil)
	var warned []string
	e.c.Storage.downloads().Warn = func(m string) { warned = append(warned, m) }
	removed, err := e.c.Storage.CleanDownloads(ctx, RunExpiry)
	if err != nil || len(removed) != 1 || removed[0].ID != old || len(warned) != 1 || !strings.Contains(warned[0], old) {
		t.Fatalf("%+v %v %v", removed, err, warned)
	}
	if exists(filepath.Join(oldOut, "big"+PartSuffix)) || !exists(filepath.Join(newOut, "big"+PartSuffix)) {
		t.Fatal("only the old run's part file goes")
	}
	if runs := e.runs(); len(runs) != 1 || runs[0] == old {
		t.Fatalf("%v", runs)
	}
	if removed, err := e.c.Storage.CleanDownloads(ctx, 0); err != nil || len(removed) != 1 || len(e.runs()) != 0 {
		t.Fatalf("0 removes every run not in use: %+v %v", removed, err)
	}
}

// ageRun makes a download run look untouched for d: its run row's update time moved back.
func ageRun(t *testing.T, state, id string, d time.Duration) {
	t.Helper()
	rd, err := journal.TakeRun(state, journal.Downloads, id)
	if err != nil || rd == nil {
		t.Fatal(err)
	}
	defer rd.Release()
	var row map[string]any
	if ok, err := rd.Journal().Get("run", "run", &row); !ok || err != nil {
		t.Fatal(err)
	}
	row["updated"] = time.Now().Add(-d).UnixMilli()
	if err := rd.Journal().Put("run", "run", row); err != nil {
		t.Fatal(err)
	}
}

// --- a crash at every journal write ------------------------------------------------------

const downloadChildEnv = "TEEPIN_TEST_DOWNLOAD_CHILD"

type downloadChildConfig struct {
	URL   string   `json:"url"`
	State string   `json:"state"`
	Out   string   `json:"out"`
	Keys  []string `json:"keys"`
	// Journal: exit right after the nth journal write; Fetch: exit as big's nth piece is fetched.
	Journal int `json:"journal"`
	Fetch   int `json:"fetch"`
}

// downloadChild runs DownloadFiles against the parent's fakes and exits as a crash would (os.Exit: no deferred
// cleanup, its journal, lock and part files left behind) at the chosen point.
func downloadChild(raw string) int {
	var cfg downloadChildConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	c := NewClient(Config{BaseURL: cfg.URL, APIKey: fakes.APIKey, StateDir: cfg.State})
	c.sleep = func(ctx context.Context, d time.Duration) error { return ctx.Err() }
	var mu sync.Mutex
	n := 0
	if cfg.Journal > 0 {
		c.Storage.downloads().Hooks.Journaled = func([]journal.Entry) {
			mu.Lock()
			defer mu.Unlock()
			if n++; n == cfg.Journal {
				os.Exit(crashed)
			}
		}
	} else {
		engine(c).Hooks.Plaintext = func(s *direct.Session, _ []byte, _, _ int64) {
			mu.Lock()
			defer mu.Unlock()
			if s.Key == "big" {
				if n++; n == cfg.Fetch {
					os.Exit(crashed)
				}
			}
		}
	}
	if _, err := c.Storage.DownloadFiles(context.Background(), "photos", cfg.Keys, cfg.Out, &DownloadFilesOptions{Concurrency: 1}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	return 0 // never crashed
}

func TestCrash_ADownloadKilledAtEveryJournalWriteIsAdoptedByTheNextCall(t *testing.T) {
	// Journal writes, at concurrency 1: 1 the run; 2 both objects; 3-6 big's four pieces; 7 big saved; 8 small's piece;
	// 9 small saved. Then a kill as big's third piece is fetched.
	cases := []downloadChildConfig{{Fetch: 3}}
	for i := 1; i <= 9; i++ {
		cases = append(cases, downloadChildConfig{Journal: i})
	}
	for _, kill := range cases {
		name := fmt.Sprintf("journal-%d", kill.Journal)
		if kill.Fetch > 0 {
			name = fmt.Sprintf("fetch-%d", kill.Fetch)
		}
		t.Run(name, func(t *testing.T) {
			e := newDownloads(t)
			data := blob(t, e.f, "big", blobSize)
			small := smallObject(t, e.f, "small", 3000)
			out := filepath.Join(t.TempDir(), "out")
			kill.URL, kill.State, kill.Out, kill.Keys = e.f.API.URL, e.state, out, []string{"big", "small"}
			raw, _ := json.Marshal(kill)
			cmd := exec.Command(os.Args[0], "-test.run=^$")
			cmd.Env = append(os.Environ(), downloadChildEnv+"="+string(raw))
			res, _ := cmd.CombinedOutput()
			if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != crashed {
				t.Fatalf("the child didn't crash: %v %s", cmd.ProcessState, res)
			}
			runs := e.runs()
			if len(runs) != 1 {
				t.Fatalf("the killed run is kept: %v", runs)
			}
			bigPath := filepath.Join(out, "big")
			j, err := journal.Read(filepath.Join(e.state, journal.Downloads, runs[0]))
			if err != nil {
				t.Fatal(err)
			}
			written := map[string]bool{}
			for _, raw := range j.Rows("ranges") {
				var rg struct {
					Path string `json:"path"`
					A    int64  `json:"a"`
				}
				if json.Unmarshal(raw, &rg) == nil && rg.Path == bigPath {
					written[fmt.Sprint(rg.A)] = true
				}
			}
			var big struct {
				State string `json:"state"`
			}
			_, _ = j.Get("downloads", bigPath, &big)
			bigSaved := big.State == "saved"
			if exists(bigPath) != bigSaved {
				t.Fatalf("big there: %v, saved: %v", exists(bigPath), bigSaved)
			}
			// A write cut short by the crash, after the last whole one.
			f, err := os.OpenFile(filepath.Join(e.state, journal.Downloads, runs[0], journal.EventsFile), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = f.WriteString(`[{"table":"ranges","id":`)
			_ = f.Close()

			seen := watchFetches(e.c)
			r := byKey(e.dl([]string{"big", "small"}, out, nil))
			if !r["big"].Saved() || !r["small"].Saved() {
				t.Fatalf("%+v", r)
			}
			if !bytes.Equal(mustRead(t, bigPath), data) || !bytes.Equal(mustRead(t, filepath.Join(out, "small")), small) {
				t.Fatal("wrong bytes")
			}
			if !bigSaved {
				for _, a := range seen.of("big") {
					if written[fmt.Sprint(a)] {
						t.Fatalf("written piece %d fetched again", a)
					}
				}
				if r["big"].Resumed != (len(written) > 0) {
					t.Fatalf("resumed %v, %d pieces written", r["big"].Resumed, len(written))
				}
			}
			if len(e.runs()) != 0 || len(partFiles(t, out)) != 0 {
				t.Fatalf("left behind: %v %v", e.runs(), partFiles(t, out))
			}
		})
	}
}

// --- share links ---------------------------------------------------------------------------

func TestCreateLink_MintsALinkWithItsTTLAndDisposition(t *testing.T) {
	f, c := newDirect(t)
	smallObject(t, f, "a.png", 100)
	before := time.Now()
	link, err := c.Storage.CreateLink(ctx, "photos", "a.png", nil)
	if err != nil || !strings.HasPrefix(link.URL, f.API.URL+"/l/") {
		t.Fatalf("%+v %v", link, err)
	}
	if d := link.ExpiresAt.Sub(before); d < 14*time.Minute || d > 16*time.Minute {
		t.Fatalf("expires in %s", d)
	}
	if _, err := c.Storage.CreateLink(ctx, "photos", "a.png", &CreateLinkOptions{ExpiresIn: time.Hour, Disposition: LinkAttachment}); err != nil {
		t.Fatal(err)
	}
	var links []fakes.Link
	f.API.Do(func() { links = slices.Clone(f.API.Links) })
	if len(links) != 2 || links[0].TTL != 900 || links[0].Disposition != "inline" || links[1].TTL != 3600 || links[1].Disposition != "attachment" {
		t.Fatalf("%+v", links)
	}
}

func TestCreateLink_ChecksItsOptionsBeforeSendingAnything(t *testing.T) {
	f, c := newDirect(t)
	smallObject(t, f, "a.png", 100)
	for _, o := range []CreateLinkOptions{{ExpiresIn: time.Millisecond}, {ExpiresIn: -time.Second},
		{ExpiresIn: 24*time.Hour + time.Second}, {Disposition: "download"}} {
		if _, err := c.Storage.CreateLink(ctx, "photos", "a.png", &o); err == nil {
			t.Fatalf("%+v was accepted", o)
		}
	}
	if f.API.HitsOf("POST object/link") != 0 {
		t.Fatal("something was sent")
	}
	for _, o := range []CreateLinkOptions{{ExpiresIn: time.Second}, {ExpiresIn: 24 * time.Hour}} {
		if _, err := c.Storage.CreateLink(ctx, "photos", "a.png", &o); err != nil {
			t.Fatalf("%+v: %v", o, err)
		}
	}
	if _, err := c.Storage.CreateLink(ctx, "photos", "nope", nil); !IsNotFound(err) {
		t.Fatalf("a missing object: %v", err)
	}
}

func TestDownloads_AnAdoptedObjectLeavesItsEarlierRunAndTheRestStays(t *testing.T) {
	e := newDownloads(t)
	big := blob(t, e.f, "big", blobSize)
	blob(t, e.f, "other", blobSize)
	out := t.TempDir()
	e.f.Network.Do(func() { e.f.Network.Fail["GET"] = 10_000 })
	if rs := e.dl([]string{"big", "other"}, out, nil); !rs[0].Pending() || !rs[1].Pending() {
		t.Fatalf("%+v", rs)
	}
	e.f.Network.Do(func() { e.f.Network.Fail["GET"] = 0 })
	if r := e.dl([]string{"big"}, out, nil); !r[0].Saved() || !bytes.Equal(mustRead(t, filepath.Join(out, "big")), big) {
		t.Fatalf("%+v", r[0])
	}
	runs, err := e.c.Storage.ListDownloadRuns(ctx)
	if err != nil || len(runs) != 1 || runs[0].Pending != 1 || runs[0].Files != 1 {
		t.Fatalf("the earlier run keeps only the object not taken: %+v %v", runs, err)
	}
}
