// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package journal

import (
	"errors"
	"strconv"
	"syscall"
)

const (
	// processQueryLimitedInformation is the least access that answers whether a process runs, and when it started.
	processQueryLimitedInformation = 0x1000
	// stillActive is GetExitCodeProcess's answer for a process that hasn't exited (STILL_ACTIVE).
	stillActive = 259
)

// alive reports whether process pid runs. Opening it is not enough: a process that has exited can still be opened
// while anyone holds a handle to it, so its exit code must say STILL_ACTIVE (profile 1's G-1). A process this user may
// not open (another user's, or a protected one) is there, so it counts as alive.
func alive(pid int) bool {
	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return errors.Is(err, syscall.ERROR_ACCESS_DENIED)
	}
	defer syscall.CloseHandle(h)
	var code uint32
	if err := syscall.GetExitCodeProcess(h, &code); err != nil {
		return true // it is there; whether it has ended can't be told
	}
	return code == stillActive
}

// processStart is when process pid started, as a token that is the same for the same process every time and differs
// for a process that reused its PID: its creation time (GetProcessTimes). "" when it can't be told.
func processStart(pid int) string {
	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return ""
	}
	defer syscall.CloseHandle(h)
	var created, exited, kernel, user syscall.Filetime
	if err := syscall.GetProcessTimes(h, &created, &exited, &kernel, &user); err != nil {
		return ""
	}
	return "win:" + strconv.FormatUint(uint64(created.HighDateTime)<<32|uint64(created.LowDateTime), 10)
}
