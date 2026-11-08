// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

//go:build linux || android

package journal

import (
	"os"
	"strconv"
	"strings"
)

// processStart is when process pid started, as a token that is the same for the same process every time and differs
// for a process that reused its PID: /proc's start time in ticks since boot (field 22 of its stat), with the boot's
// id. "" when it can't be told.
func processStart(pid int) string {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')') // the command name, in parentheses, may hold spaces
	if i < 0 || i+2 > len(s) {
		return ""
	}
	fields := strings.Fields(s[i+2:]) // from field 3 (the state) on
	if len(fields) < 20 {
		return ""
	}
	boot, _ := os.ReadFile("/proc/sys/kernel/random/boot_id")
	return "linux:" + strings.TrimSpace(string(boot)) + ":" + fields[19]
}
