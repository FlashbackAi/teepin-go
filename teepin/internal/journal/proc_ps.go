// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

//go:build unix && !linux && !android

package journal

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// processStart is when process pid started, as a token that is the same for the same process every time and differs
// for a process that reused its PID: ps's start time (macOS and the BSDs have no /proc to read it from). "" when it
// can't be told.
func processStart(pid int) string {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ps", "-o", "lstart=", "-p", strconv.Itoa(pid))
	cmd.Env = append(os.Environ(), "TZ=UTC", "LC_ALL=C")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	s := strings.TrimSpace(string(out))
	if s == "" {
		return ""
	}
	return "ps:" + s
}
