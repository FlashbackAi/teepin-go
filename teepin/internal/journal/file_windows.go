// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package journal

import (
	"errors"
	"io"
	"os"
	"syscall"
)

const errorSharingViolation syscall.Errno = 32

// sharingViolation reports a file another handle has open: Windows refuses to rename over it or delete it until that
// handle closes.
func sharingViolation(err error) bool {
	return errors.Is(err, errorSharingViolation) || errors.Is(err, syscall.ERROR_ACCESS_DENIED)
}

// readShared reads a whole file through a handle that lets others rename over or delete it meanwhile
// (FILE_SHARE_DELETE, which os.Open doesn't ask for).
func readShared(path string) ([]byte, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	h, err := syscall.CreateFile(p, syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE, nil, syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		if errors.Is(err, syscall.ERROR_FILE_NOT_FOUND) || errors.Is(err, syscall.ERROR_PATH_NOT_FOUND) {
			err = os.ErrNotExist
		}
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	f := os.NewFile(uintptr(h), path)
	defer f.Close()
	return io.ReadAll(f)
}
