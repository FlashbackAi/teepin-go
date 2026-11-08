// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

//go:build !unix && !windows

package direct

import "os"

// oNoFollow is nothing where the OS has no links to follow.
const oNoFollow = 0

// fileID is a file's identity where the OS gives none: its details, compared with os.SameFile.
type fileID struct{ fi os.FileInfo }

func (a fileID) same(b fileID) bool { return a.fi != nil && b.fi != nil && os.SameFile(a.fi, b.fi) }

// key is no map key: these OSes give no identity beyond os.SameFile, so a batch's placed-file guard is off here.
func (a fileID) key() (k [2]uint64, ok bool) { return k, false }

func handleID(f *os.File) (fileID, uint64, error) {
	fi, err := f.Stat()
	return fileID{fi}, 1, err
}

func pathID(path string) (fileID, uint64, os.FileInfo, error) {
	fi, err := os.Lstat(path)
	return fileID{fi}, 1, fi, err
}
