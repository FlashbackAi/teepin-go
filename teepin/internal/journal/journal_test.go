// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package journal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The journal, run directories, locks and the shared files of the state directory (sections 17.2, 22.6, 24.4 and
// 25.4): the Node SDK's upload-journal.test.mjs, ported, with Go's own file names and the Windows liveness rule (G-1)
// and rename retries (W-1). Child processes are this test binary run again with childEnv set.

const childEnv = "TEEPIN_TEST_JOURNAL_CHILD"

type childCfg struct {
	Mode string `json:"mode"` // "write": write rows until killed; "lock": take a lock; "wait": wait for done; "exit"
	Dir  string `json:"dir"`
	Lock string `json:"lock"`
	Go   string `json:"go"`
	Done string `json:"done"`
}

func TestMain(m *testing.M) {
	if raw := os.Getenv(childEnv); raw != "" {
		os.Exit(child(raw))
	}
	os.Exit(m.Run())
}

func child(raw string) int {
	var cfg childCfg
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return 2
	}
	waitFile := func(p string) {
		for p != "" {
			if _, err := os.Stat(p); err == nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	switch cfg.Mode {
	case "write":
		j, err := Open(cfg.Dir)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 3
		}
		fmt.Println("open")
		pad := strings.Repeat("x", 4000)
		for i := 1; ; i++ {
			id := strconv.Itoa(i)
			if err := j.Write(Entry{Table: "t", ID: id, Row: map[string]any{"n": i, "pad": pad}},
				Entry{Table: "u", ID: id, Row: map[string]any{"n": i}}); err != nil {
				return 4
			}
		}
	case "lock":
		selfStart() // learnt before racing, so takers start together once the go file appears
		waitFile(cfg.Go)
		l := NewLock(cfg.Lock)
		ok, err := l.Acquire()
		if err != nil {
			fmt.Print("error: ", err)
			return 5
		}
		if ok {
			fmt.Print("held")
		} else {
			fmt.Print("refused")
		}
		waitFile(cfg.Done) // holds it until every taker has answered
		if ok {
			_ = l.Release()
		}
	case "wait":
		fmt.Println("ready")
		waitFile(cfg.Done)
	}
	return 0
}

func childCmd(t *testing.T, cfg childCfg) *exec.Cmd {
	t.Helper()
	raw, _ := json.Marshal(cfg)
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), childEnv+"="+string(raw))
	return cmd
}

// deadPID is the PID of a process that has ended.
func deadPID(t *testing.T) int {
	t.Helper()
	cmd := childCmd(t, childCfg{Mode: "exit"})
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	return cmd.Process.Pid
}

// liveChild is a process that runs until done is called.
func liveChild(t *testing.T) (int, func()) {
	t.Helper()
	done := filepath.Join(t.TempDir(), "done")
	cmd := childCmd(t, childCfg{Mode: "wait", Done: done})
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 6)
	_, _ = out.Read(buf) // "ready": it runs
	stop := func() {
		_ = os.WriteFile(done, nil, 0o600)
		_ = cmd.Wait()
	}
	t.Cleanup(stop)
	return cmd.Process.Pid, stop
}

func newRun(t *testing.T) *RunDir {
	t.Helper()
	rd, err := CreateRun(t.TempDir(), Uploads)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rd.Release() })
	return rd
}

func events(t *testing.T, dir string) []string {
	t.Helper()
	b, _ := os.ReadFile(filepath.Join(dir, EventsFile))
	s := strings.TrimSuffix(string(b), "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

type row struct {
	N   int    `json:"n"`
	Pad string `json:"pad,omitempty"`
}

// --- the journal -------------------------------------------------------------------------------------------------------

func TestJournal_ReplaysWhatWasWrittenAndIsCompactedToOneLineWhenReopened(t *testing.T) {
	rd := newRun(t)
	j := rd.Journal()
	for i := 1; i <= 3; i++ {
		if err := j.Put("files", j.NextID("files"), row{N: i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Write(Entry{Table: "files", ID: "2", Remove: true}, Entry{Table: "packs", ID: "p", Row: row{N: 9}}); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	if n := len(events(t, rd.Path)); n != 4 {
		t.Fatalf("%d lines", n)
	}
	j2, err := Open(rd.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	if n := len(events(t, rd.Path)); n != 1 {
		t.Fatalf("compacted to %d lines", n)
	}
	var got []int
	for _, raw := range j2.Rows("files") {
		var r row
		_ = json.Unmarshal(raw, &r)
		got = append(got, r.N)
	}
	var p row
	if ok, _ := j2.Get("packs", "p", &p); !ok || p.N != 9 || fmt.Sprint(got) != "[1 3]" {
		t.Fatalf("files %v, pack %v", got, p)
	}
	if id := j2.NextID("files"); id != "4" {
		t.Fatalf("the next id is %s", id)
	}
}

func TestJournal_IsOnDiskWhenAWriteReturns(t *testing.T) {
	rd := newRun(t)
	if err := rd.Journal().Put("files", "1", row{N: 42}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(rd.Path, EventsFile)); !bytes.Contains(b, []byte(`"n":42`)) {
		t.Fatalf("%q", b)
	}
}

func TestJournal_IgnoresATornLastLineAndDropsItWhenCompacting(t *testing.T) {
	rd := newRun(t)
	_ = rd.Journal().Put("files", "1", row{N: 1})
	_ = rd.Journal().Close()
	f, _ := os.OpenFile(filepath.Join(rd.Path, EventsFile), os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.WriteString(`[{"table":"files","id":"2","row":{"n"`)
	f.Close()
	j, err := Open(rd.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if len(j.Rows("files")) != 1 || len(events(t, rd.Path)) != 1 {
		t.Fatal("the torn line was taken, or kept")
	}
	b, _ := os.ReadFile(filepath.Join(rd.Path, EventsFile))
	if bytes.Contains(b, []byte(`"id":"2"`)) {
		t.Fatal("the torn line survived compaction")
	}
}

func TestJournal_IgnoresADamagedLastLine(t *testing.T) {
	rd := newRun(t)
	_ = rd.Journal().Put("files", "1", row{N: 1})
	_ = rd.Journal().Close()
	f, _ := os.OpenFile(filepath.Join(rd.Path, EventsFile), os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.Write(append(make([]byte, 40), '\n')) // zeros where a line was being written
	f.Close()
	j, err := Open(rd.Path)
	if err != nil || len(j.Rows("files")) != 1 {
		t.Fatalf("%v", err)
	}
	_ = j.Close()
}

func TestJournal_RefusesABadLineAnywhereButTheLast(t *testing.T) {
	rd := newRun(t)
	_ = rd.Journal().Close()
	_ = os.WriteFile(filepath.Join(rd.Path, EventsFile), []byte("damaged\n[]\n"), 0o600)
	if _, err := Open(rd.Path); err == nil || !strings.Contains(err.Error(), "damaged at line 1") {
		t.Fatalf("%v", err)
	}
	if _, err := Read(rd.Path); err == nil {
		t.Fatal("read a damaged journal")
	}
}

func TestJournal_RefusesARunANewerSDKWroteAndADirectoryThatIsNoRun(t *testing.T) {
	rd := newRun(t)
	_ = rd.Journal().Close()
	_ = os.WriteFile(filepath.Join(rd.Path, RunFile), []byte(`{"format":2}`), 0o600)
	if _, err := Open(rd.Path); err == nil || !strings.Contains(err.Error(), "newer SDK") {
		t.Fatalf("%v", err)
	}
	if _, err := Open(t.TempDir()); err == nil || !strings.Contains(err.Error(), "is not a run") {
		t.Fatalf("%v", err)
	}
}

func TestJournal_RefusesWritesOnceClosedOrReadOnly(t *testing.T) {
	rd := newRun(t)
	_ = rd.Journal().Put("files", "1", row{N: 1})
	ro, err := Read(rd.Path)
	if err != nil || ro.Durable() {
		t.Fatal(err)
	}
	if err := ro.Put("files", "2", row{}); err == nil {
		t.Fatal("wrote a read-only journal")
	}
	_ = rd.Journal().Close()
	if err := rd.Journal().Put("files", "2", row{}); err == nil {
		t.Fatal("wrote a closed journal")
	}
	if err := Memory().Put("t", "1", []int{1}); err == nil {
		t.Fatal("a row that isn't an object was taken")
	}
}

// SetWriteFault fails the writes its fault picks as a failed append does: nothing written, and the journal refuses
// every write after it; a journal in memory never asks it, and restore clears it.
func TestJournal_AWriteFaultFailsThatWriteAndBreaksTheJournal(t *testing.T) {
	rd := newRun(t)
	other := newRun(t)
	full := fmt.Errorf("no space left on the device")
	var asked []string
	restore := SetWriteFault(func(dir string, entries []Entry) error {
		asked = append(asked, entries[0].Table)
		if dir == rd.Path && entries[0].Table == "parts" {
			return full
		}
		return nil
	})
	if err := rd.Journal().Put("files", "1", row{N: 1}); err != nil {
		t.Fatal(err)
	}
	if err := rd.Journal().Put("parts", "1", row{N: 2}); err == nil || !strings.Contains(err.Error(), full.Error()) {
		t.Fatalf("the faulted write: %v", err)
	}
	if err := rd.Journal().Put("files", "2", row{N: 3}); err == nil {
		t.Fatal("a broken journal took another write")
	}
	if got := events(t, rd.Path); len(got) != 1 {
		t.Fatalf("lines on disk: %q", got)
	}
	if err := other.Journal().Put("parts", "1", row{N: 4}); err != nil {
		t.Fatalf("another run's write: %v", err)
	}
	if err := Memory().Put("parts", "1", row{N: 5}); err != nil || len(asked) != 3 {
		t.Fatalf("in memory: %v, asked %v", err, asked)
	}
	restore()
	if err := other.Journal().Put("parts", "2", row{N: 6}); err != nil || len(asked) != 3 {
		t.Fatalf("after restore: %v, asked %v", err, asked)
	}
}

func TestJournal_CompactsByAnAtomicRewriteWhileOpen(t *testing.T) {
	rd := newRun(t)
	j := rd.Journal()
	for i := 1; i <= 5; i++ {
		_ = j.Put("files", strconv.Itoa(i), row{N: i})
	}
	if err := j.Compact(); err != nil {
		t.Fatal(err)
	}
	if n := len(events(t, rd.Path)); n != 1 {
		t.Fatalf("%d lines", n)
	}
	if err := j.Put("files", "6", row{N: 6}); err != nil || len(events(t, rd.Path)) != 2 {
		t.Fatalf("appending after compaction: %v", err)
	}
	assertNoTemp(t, rd.Path)
}

func assertNoTemp(t *testing.T, dir string) {
	t.Helper()
	_ = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && strings.HasSuffix(p, ".tmp") {
			t.Errorf("a temporary file was left: %s", p)
		}
		return nil
	})
}

func TestJournal_SurvivesAProcessKilledMidWrite(t *testing.T) {
	for round := 0; round < 3; round++ {
		rd := newRun(t)
		_ = rd.Journal().Close()
		cmd := childCmd(t, childCfg{Mode: "write", Dir: rd.Path})
		out, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 5)
		_, _ = out.Read(buf)
		time.Sleep(time.Duration(150+100*round) * time.Millisecond)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		j, err := Open(rd.Path)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		ts, us := j.Rows("t"), j.Rows("u")
		if len(ts) == 0 || len(ts) != len(us) {
			t.Fatalf("round %d: %d and %d rows: a batch landed in part", round, len(ts), len(us))
		}
		for _, raw := range ts {
			var r row
			if err := json.Unmarshal(raw, &r); err != nil || len(r.Pad) != 4000 {
				t.Fatalf("round %d: a row isn't whole", round)
			}
		}
		_ = j.Close()
	}
}

// --- run directories ---------------------------------------------------------------------------------------------------

func TestRunDir_IsMadeLockedTakenByOneHolderAtATimeAndRemovedWhole(t *testing.T) {
	state := t.TempDir()
	rd, err := CreateRun(state, Uploads)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := TakeRun(state, Uploads, rd.ID); err != nil || again != nil {
		t.Fatalf("taken twice: %v", err)
	}
	if _, inUse, err := PeekRun(state, Uploads, rd.ID); err != nil || !inUse {
		t.Fatalf("in use %v, %v", inUse, err)
	}
	_ = rd.Release()
	taken, err := TakeRun(state, Uploads, rd.ID)
	if err != nil || taken == nil {
		t.Fatalf("not taken after its release: %v", err)
	}
	_ = taken.WritePack("a.pack", []byte("pack"))
	_ = taken.WriteKey("u1", make([]byte, 32))
	if err := taken.Remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(taken.Path); !os.IsNotExist(err) {
		t.Fatalf("the run is still there: %v", err)
	}
	if ids, _ := RunIDs(state, Uploads); len(ids) != 0 {
		t.Fatalf("%v", ids)
	}
}

func TestRunDir_ZeroesADataKeyBeforeDeletingIt(t *testing.T) {
	rd := newRun(t)
	key := bytes.Repeat([]byte{7}, 32)
	if err := rd.WriteKey("u1", key); err != nil {
		t.Fatal(err)
	}
	if got, err := rd.ReadKey("u1"); err != nil || !bytes.Equal(got, key) {
		t.Fatalf("%v", err)
	}
	// A second name for the key's file sees what the deletion leaves in it.
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Link(rd.keyPath("u1"), link); err != nil {
		t.Skipf("no hard links here: %v", err)
	}
	if err := rd.RemoveKey("u1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(link); !bytes.Equal(got, make([]byte, 32)) {
		t.Fatalf("the key's bytes weren't zeroed: %x", got)
	}
	if got, _ := rd.ReadKey("u1"); got != nil || len(rd.KeyNames()) != 0 {
		t.Fatal("the key is still there")
	}
}

func TestRunDir_WritesPacksAndKeysAtomicallyLeavingNoTemporaryFile(t *testing.T) {
	rd := newRun(t)
	for i := 0; i < 5; i++ {
		_ = rd.WritePack(fmt.Sprintf("%d.pack", i), bytes.Repeat([]byte{byte(i)}, 1000))
		_ = rd.WriteKey(fmt.Sprint(i), make([]byte, 32))
	}
	assertNoTemp(t, rd.Path)
	if len(rd.PackNames()) != 5 || len(rd.KeyNames()) != 5 {
		t.Fatal("not all written")
	}
	if posix {
		st, _ := os.Stat(rd.keyPath("0"))
		if st.Mode().Perm() != 0o600 {
			t.Fatalf("a key's file is %v", st.Mode().Perm())
		}
		st, _ = os.Stat(rd.Path)
		if st.Mode().Perm() != 0o700 {
			t.Fatalf("a run's directory is %v", st.Mode().Perm())
		}
	}
}

func TestRunDir_IDsListOnlyThisSDKsRuns(t *testing.T) {
	state := t.TempDir()
	rd, _ := CreateRun(state, Uploads)
	defer rd.Release()
	for name, marker := range map[string]string{"python-run": "journal.sqlite3", "node-run": "run.json"} {
		_ = os.MkdirAll(filepath.Join(state, Uploads, name), 0o700)
		_ = os.WriteFile(filepath.Join(state, Uploads, name, marker), []byte("{}"), 0o600)
	}
	if ids, _ := RunIDs(state, Uploads); len(ids) != 1 || ids[0] != rd.ID {
		t.Fatalf("%v", ids)
	}
}

// --- the run lock ------------------------------------------------------------------------------------------------------

func writeLock(t *testing.T, path string, j lockJSON) {
	t.Helper()
	b, _ := json.Marshal(j)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func lockPath(t *testing.T) string { return filepath.Join(t.TempDir(), LockFile) }

func TestLock_IsHeldByOneLockAtATimeInThisProcessAndReleased(t *testing.T) {
	p := lockPath(t)
	a, b := NewLock(p), NewLock(p)
	if ok, err := a.Acquire(); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, _ := b.Acquire(); ok {
		t.Fatal("two locks held it")
	}
	if held, _ := b.HeldElsewhere(); !held {
		t.Fatal("not seen as held")
	}
	_ = a.Release()
	_ = a.Release() // twice is fine
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("the lock file was left")
	}
	if ok, _ := b.Acquire(); !ok {
		t.Fatal("not taken after its release")
	}
	_ = b.Release()
}

func TestLock_TakesOverALockWhoseProcessIsGone(t *testing.T) {
	p := lockPath(t)
	writeLock(t, p, lockJSON{Host: host(), PID: deadPID(t), Nonce: "dead"})
	l := NewLock(p)
	if ok, err := l.Acquire(); !ok || err != nil {
		t.Fatal(ok, err)
	}
	defer l.Release()
	if cur, _ := readLock(p); cur.pid != os.Getpid() {
		t.Fatal("not replaced by ours")
	}
}

func TestLock_TakesOverAReusedPIDAndKeepsALiveHolders(t *testing.T) {
	pid, _ := liveChild(t)
	start := processStart(pid)
	if start == "" {
		t.Skip("this system can't tell a process's start time")
	}
	p := lockPath(t)
	writeLock(t, p, lockJSON{Host: host(), PID: pid, Start: start, Nonce: "live"})
	if ok, _ := NewLock(p).Acquire(); ok {
		t.Fatal("a live holder's lock was taken")
	}
	writeLock(t, p, lockJSON{Host: host(), PID: pid, Start: "another process", Nonce: "reused"})
	l := NewLock(p)
	if ok, err := l.Acquire(); !ok || err != nil {
		t.Fatal("a reused PID's lock wasn't taken over", err)
	}
	_ = l.Release()
}

func TestLock_NeverTakesAnotherHostsLock(t *testing.T) {
	p := lockPath(t)
	writeLock(t, p, lockJSON{Host: host() + "-elsewhere", PID: deadPID(t), Nonce: "far"})
	if ok, _ := NewLock(p).Acquire(); ok {
		t.Fatal("another host's lock was taken")
	}
}

func TestLock_HoldsALockStillBeingWrittenAndTakesOneLeftUnreadable(t *testing.T) {
	p := lockPath(t)
	_ = os.WriteFile(p, []byte(`{"host":`), 0o600)
	if ok, _ := NewLock(p).Acquire(); ok {
		t.Fatal("a lock still being written was taken")
	}
	old := time.Now().Add(-2 * LockGrace)
	_ = os.Chtimes(p, old, old)
	l := NewLock(p)
	if ok, err := l.Acquire(); !ok || err != nil {
		t.Fatal("a lock left unreadable wasn't taken", err)
	}
	_ = l.Release()
}

func TestLock_TakesALeftoverOfThisProcessesOwn(t *testing.T) {
	p := lockPath(t)
	writeLock(t, p, lockJSON{Host: host(), PID: os.Getpid(), Start: selfStart(), Nonce: "leftover"})
	if held, _ := NewLock(p).HeldElsewhere(); held {
		t.Fatal("a leftover no lock here holds is seen as held")
	}
	l := NewLock(p)
	if ok, err := l.Acquire(); !ok || err != nil {
		t.Fatal(ok, err)
	}
	_ = l.Release()
}

func TestLock_ClearsTheClaimOfATakerThatDiedThenTakesTheLock(t *testing.T) {
	p := lockPath(t)
	writeLock(t, p, lockJSON{Host: host(), PID: deadPID(t), Nonce: "stale"})
	writeLock(t, p+".take-stale", lockJSON{Host: host(), PID: deadPID(t), Nonce: "taker"})
	l := NewLock(p)
	if ok, err := l.Acquire(); !ok || err != nil {
		t.Fatal(ok, err)
	}
	_ = l.Release()
	if _, err := os.Stat(p + ".take-stale"); !os.IsNotExist(err) {
		t.Fatal("the dead taker's claim was left")
	}
}

func TestLock_LetsOnlyOneOfTwoProcessesRacingForAStaleLockTakeIt(t *testing.T) {
	for round := 0; round < 3; round++ {
		dir := t.TempDir()
		p := filepath.Join(dir, LockFile)
		writeLock(t, p, lockJSON{Host: host(), PID: deadPID(t), Nonce: "stale"})
		goFile, done := filepath.Join(dir, "go"), filepath.Join(dir, "done")
		var cmds []*exec.Cmd
		var outs []*bytes.Buffer
		for i := 0; i < 2; i++ {
			cmd := childCmd(t, childCfg{Mode: "lock", Lock: p, Go: goFile, Done: done})
			var out bytes.Buffer
			cmd.Stdout = &out
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			cmds, outs = append(cmds, cmd), append(outs, &out)
		}
		time.Sleep(300 * time.Millisecond)
		_ = os.WriteFile(goFile, nil, 0o600)
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) && (outs[0].Len() == 0 || outs[1].Len() == 0) {
			time.Sleep(10 * time.Millisecond)
		}
		_ = os.WriteFile(done, nil, 0o600)
		for _, c := range cmds {
			_ = c.Wait()
		}
		got := outs[0].String() + "," + outs[1].String()
		if got != "held,refused" && got != "refused,held" {
			t.Fatalf("round %d: %s", round, got)
		}
	}
}

func TestLock_NeverReplacesALockALiveHolderTookAfterItWasJudgedStale(t *testing.T) {
	p := lockPath(t)
	writeLock(t, p, lockJSON{Host: host(), PID: deadPID(t), Nonce: "stale"})
	cur, _ := readLock(p)
	pid, _ := liveChild(t)
	live := lockJSON{Host: host(), PID: pid, Start: processStart(pid), Nonce: "live"}
	writeLock(t, p, live) // the stale lock was taken over by a live holder meanwhile
	text, _ := json.Marshal(lockJSON{Host: host(), PID: os.Getpid(), Nonce: "mine"})
	l := NewLock(p)
	if took, err := l.takeOver(cur, text); took || err != nil {
		t.Fatal(took, err)
	}
	if now, _ := readLock(p); now.id != "live" {
		t.Fatalf("the live holder's lock was replaced by %s", now.id)
	}
}

func TestLock_ReleasesOnlyALockThatIsStillItsOwn(t *testing.T) {
	p := lockPath(t)
	l := NewLock(p)
	if ok, _ := l.Acquire(); !ok {
		t.Fatal("not taken")
	}
	writeLock(t, p, lockJSON{Host: host(), PID: os.Getpid() + 1, Nonce: "another"})
	_ = l.Release()
	if cur, _ := readLock(p); cur == nil || cur.id != "another" {
		t.Fatal("another's lock was deleted")
	}
}

func TestLock_AProcessIsAliveOnlyWhileItRuns(t *testing.T) {
	if !alive(os.Getpid()) {
		t.Fatal("this process isn't alive")
	}
	if alive(deadPID(t)) {
		t.Fatal("an ended process is alive")
	}
	pid, stop := liveChild(t)
	if !alive(pid) {
		t.Fatal("a running process isn't alive")
	}
	start := processStart(pid)
	if runtime.GOOS == "windows" || runtime.GOOS == "linux" {
		if start == "" || processStart(pid) != start {
			t.Fatalf("start time %q isn't stable", start)
		}
	}
	stop()
}

// --- W-1: renames over a file another program holds open ---------------------------------------------------------------

func TestRenameOver_IsTriedAgainWhileAnotherProgramHoldsTheTarget(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	_ = os.WriteFile(target, []byte("old"), 0o600)
	held, err := os.Open(target) // os.Open doesn't let others rename over the file on Windows
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		held.Close()
	}()
	if err := WriteAtomic(target, []byte("new")); err != nil {
		t.Fatalf("a target held for 100 ms: %v", err)
	}
	if b, _ := os.ReadFile(target); string(b) != "new" {
		t.Fatalf("%q", b)
	}
	assertNoTemp(t, dir)
}

func TestRenameOver_GivesUpAfterItsWaitAndATakeoverIsThenNotTaken(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("only Windows refuses a rename over an open file")
	}
	old := sharingRetry
	sharingRetry = 200 * time.Millisecond
	defer func() { sharingRetry = old }()
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	_ = os.WriteFile(target, []byte("old"), 0o600)
	held, _ := os.Open(target)
	begun := time.Now()
	err := WriteAtomic(target, []byte("new"))
	if err == nil || time.Since(begun) < 150*time.Millisecond {
		t.Fatalf("%v after %s", err, time.Since(begun))
	}
	held.Close()
	assertNoTemp(t, dir)

	p := filepath.Join(dir, LockFile)
	writeLock(t, p, lockJSON{Host: host(), PID: deadPID(t), Nonce: "stale"})
	hold, _ := os.Open(p)
	defer hold.Close()
	l := NewLock(p)
	if ok, err := l.Acquire(); ok || err != nil {
		t.Fatalf("a takeover whose replace failed: taken %v, %v", ok, err)
	}
	if cur, _ := readLock(p); cur.id != "stale" {
		t.Fatal("the lock was replaced")
	}
}

// --- the state directory, the history and the pins ----------------------------------------------------------------------

func TestStateDir_IsTEEPIN_STATE_DIRWhenSetElseTheSystems(t *testing.T) {
	t.Setenv(StateEnv, filepath.Join("x", "state"))
	if d := DefaultStateDir(); d != filepath.Join("x", "state") {
		t.Fatal(d)
	}
	t.Setenv(StateEnv, "")
	t.Setenv("LOCALAPPDATA", filepath.Join("C:", "local"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(string(filepath.Separator), "xdg"))
	want := map[string]string{"windows": filepath.Join("C:", "local", "teepin"),
		"linux": filepath.Join(string(filepath.Separator), "xdg", "teepin")}[runtime.GOOS]
	if want != "" && DefaultStateDir() != want {
		t.Fatalf("%s, want %s", DefaultStateDir(), want)
	}
}

func TestHistory_AppendsWithoutRewritingAndTrimsAWeekOnlyWhenAskedOrPastTheCap(t *testing.T) {
	state := t.TempDir()
	path := filepath.Join(state, Uploads, HistoryFile)
	_ = os.MkdirAll(filepath.Dir(path), 0o700)
	old := fmt.Sprintf("%d run long ago\n", time.Now().Add(-8*24*time.Hour).Unix())
	_ = os.WriteFile(path, []byte(old), 0o600)
	held, _ := os.Open(path) // an append works while another program reads it
	if err := AppendHistory(state, Uploads, "run a saved=1"); err != nil {
		t.Fatal(err)
	}
	held.Close()
	b, _ := os.ReadFile(path)
	if !strings.HasPrefix(string(b), old) || !strings.HasSuffix(string(b), " run a saved=1\n") {
		t.Fatalf("an append rewrote the history: %q", b)
	}
	if err := TrimHistory(state, Uploads); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); strings.Contains(string(b), "long ago") || !strings.Contains(string(b), "run a") {
		t.Fatalf("trimmed to %q", b)
	}
	_ = os.WriteFile(path, []byte(old), 0o600)
	if err := appendHistory(path, "run b", 10); err != nil { // past a cap of 10 bytes: trimmed
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); strings.Contains(string(b), "long ago") || !strings.Contains(string(b), "run b") {
		t.Fatalf("past the cap: %q", b)
	}
}

func TestPins_PinTheFirstKeyOfAnAccountAndVersionAcrossInstances(t *testing.T) {
	state := t.TempDir()
	a, b := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	if !NewPins(state).Check("acct", 1, a) {
		t.Fatal("the first key was refused")
	}
	p := NewPins(state)
	if p.Check("acct", 1, b) || !p.Check("acct", 1, a) {
		t.Fatal("the pin didn't hold across instances")
	}
	if !p.Check("acct", 2, b) || !p.Check("other", 1, b) {
		t.Fatal("a new version or another account was refused")
	}
	var file map[string]map[string]string
	raw, _ := os.ReadFile(filepath.Join(state, "keys.json"))
	if err := json.Unmarshal(raw, &file); err != nil || len(file["acct"]) != 2 {
		t.Fatalf("keys.json %s: %v", raw, err)
	}
	// A state directory that can't be written keeps the pin in memory.
	m := NewPins("")
	if !m.Check("acct", 1, a) || m.Check("acct", 1, b) {
		t.Fatal("in memory")
	}
}

func TestJournal_AnEntryWithNoTableIsRefused(t *testing.T) {
	if err := Memory().Write(Entry{ID: "1", Row: row{}}); err == nil {
		t.Fatal("taken")
	}
	if _, ok := parseLine([]byte(`[{"table":"","id":"1","row":{}}]`)); ok {
		t.Fatal("a line with no table was taken")
	}
	if _, ok := parseLine([]byte(`[{"table":"t","id":"1","row":[1]}]`)); ok {
		t.Fatal("a row that isn't an object was taken")
	}
	if es, ok := parseLine([]byte(`[{"table":"t","id":"1","remove":true}]`)); !ok || !es[0].Remove {
		t.Fatal("a removal was refused")
	}
}
