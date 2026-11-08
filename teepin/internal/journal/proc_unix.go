// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

//go:build unix

package journal

import (
	"errors"
	"syscall"
)

// alive reports whether process pid runs: kill(pid, 0). One this user may not signal (another user's) is there, so it
// counts as alive.
func alive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
