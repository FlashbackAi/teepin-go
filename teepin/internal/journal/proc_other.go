// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

//go:build !unix && !windows

package journal

// alive can't be told on this system: a lock's process counts as alive, so its lock is never taken over by mistake.
func alive(int) bool { return true }

// processStart can't be told on this system.
func processStart(int) string { return "" }
