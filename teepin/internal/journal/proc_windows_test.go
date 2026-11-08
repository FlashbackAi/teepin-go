// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package journal

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// G-1 (profile 1): on Windows a process that has ended can still be opened while anyone holds a handle to it, so
// opening it doesn't mean it runs: its exit code must say STILL_ACTIVE.
func TestAlive_AnEndedProcessThatCanStillBeOpenedIsNotAlive(t *testing.T) {
	done := filepath.Join(t.TempDir(), "done")
	cmd := childCmd(t, childCfg{Mode: "wait", Done: done})
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 6)
	_, _ = out.Read(buf)
	pid := cmd.Process.Pid
	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.CloseHandle(h) // keeps the ended process openable
	if !alive(pid) {
		t.Fatal("a running process isn't alive")
	}
	_ = os.WriteFile(done, nil, 0o600)
	_ = cmd.Wait()
	again, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		t.Fatalf("the ended process can't be opened, so this test shows nothing: %v", err)
	}
	syscall.CloseHandle(again)
	if alive(pid) {
		t.Fatal("an ended process that can still be opened counts as alive")
	}
	p := filepath.Join(t.TempDir(), LockFile)
	writeLock(t, p, lockJSON{Host: host(), PID: pid, Nonce: "ended"})
	l := NewLock(p)
	if ok, err := l.Acquire(); !ok || err != nil {
		t.Fatal("its lock wasn't taken over", err)
	}
	_ = l.Release()
}
