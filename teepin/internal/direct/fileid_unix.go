// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

//go:build unix

package direct

import (
	"fmt"
	"os"
	"syscall"
)

// oNoFollow makes an open fail on a symbolic link rather than follow it.
const oNoFollow = syscall.O_NOFOLLOW

// fileID is a file's identity: its device and inode.
type fileID struct{ dev, ino uint64 }

func (a fileID) same(b fileID) bool { return a == b }

// key is the identity as a map key; ok is false where the OS gives none (fileid_other.go).
func (a fileID) key() (k [2]uint64, ok bool) { return [2]uint64{a.dev, a.ino}, true }

func idOfInfo(fi os.FileInfo) (fileID, uint64, error) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fileID{}, 0, fmt.Errorf("teepin: %s: no file identity", fi.Name())
	}
	return fileID{dev: uint64(st.Dev), ino: st.Ino}, uint64(st.Nlink), nil
}

// handleID is an open file's identity and how many names it has.
func handleID(f *os.File) (fileID, uint64, error) {
	fi, err := f.Stat()
	if err != nil {
		return fileID{}, 0, err
	}
	return idOfInfo(fi)
}

// pathID is the identity of the file at path itself (a link's own, never its target's), how many names it has, and
// its details.
func pathID(path string) (fileID, uint64, os.FileInfo, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return fileID{}, 0, nil, err
	}
	id, n, err := idOfInfo(fi)
	return id, n, fi, err
}
