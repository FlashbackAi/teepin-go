// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"os"
	"syscall"
)

// oNoFollow is nothing on Windows: there a handle's identity check stops a link.
const oNoFollow = 0

// fileID is a file's identity: its volume and file index.
type fileID struct{ dev, ino uint64 }

func (a fileID) same(b fileID) bool { return a == b }

// key is the identity as a map key; ok is false where the OS gives none (fileid_other.go).
func (a fileID) key() (k [2]uint64, ok bool) { return [2]uint64{a.dev, a.ino}, true }

func idOfHandle(h syscall.Handle) (fileID, uint64, error) {
	var info syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(h, &info); err != nil {
		return fileID{}, 0, err
	}
	return fileID{dev: uint64(info.VolumeSerialNumber), ino: uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow)},
		uint64(info.NumberOfLinks), nil
}

// handleID is an open file's identity and how many names it has.
func handleID(f *os.File) (fileID, uint64, error) { return idOfHandle(syscall.Handle(f.Fd())) }

// pathID is the identity of the file at path itself (a link's or a junction's own, never its target's), how many
// names it has, and its details.
func pathID(path string) (fileID, uint64, os.FileInfo, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return fileID{}, 0, nil, err
	}
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return fileID{}, 0, nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	// No access asked for: only the file's details are read. The reparse point itself is opened, never followed.
	h, err := syscall.CreateFile(p, 0, syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE, nil,
		syscall.OPEN_EXISTING, syscall.FILE_FLAG_OPEN_REPARSE_POINT|syscall.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return fileID{}, 0, nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	defer syscall.CloseHandle(h)
	id, n, err := idOfHandle(h)
	return id, n, fi, err
}
