// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

//go:build !windows

package journal

import "os"

// sharingViolation is Windows' alone: elsewhere an open file can always be renamed over or deleted.
func sharingViolation(error) bool { return false }

// readShared reads a whole file (elsewhere than Windows, an open file never keeps others from renaming over it).
func readShared(path string) ([]byte, error) { return os.ReadFile(path) }
