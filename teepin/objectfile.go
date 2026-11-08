// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
)

// OpenObjectOptions are the options of OpenObject.
type OpenObjectOptions struct {
	// Prefetch is how many blocks a reader reading straight on fetches ahead.
	// Zero means the default (2); a negative value means none: no block is
	// fetched ahead.
	Prefetch int
}

// ObjectFile is an object open for reading at any offset: an io.ReaderAt, an
// io.ReadSeeker and an io.Closer, safe for concurrent ReadAt calls.
//
// Reading straight on fetches a block at a time (a window of the storage
// network, 64 MiB) with the next Prefetch blocks ahead, so up to Prefetch + 2
// blocks are held in memory; a read somewhere else fetches only what it
// needs, in whole segments. Every byte is verified before it is read, and
// every block comes from the version first opened: ErrObjectChanged if the
// object is replaced while it is read.
//
// Close it when done: that stops its fetches and zeroes its copy of the data
// key. Reads after Close fail with an error that is fs.ErrClosed.
type ObjectFile struct {
	// Info is the object's details, as HeadObject gives them.
	Info ObjectInfo

	blocks *direct.Blocks

	mu  sync.Mutex
	pos int64
}

// OpenObject opens an object for reading at any offset. ctx bounds every read
// of the file: when it ends, so do they (pass a context that lives as long as
// the file is used).
func (s *StorageService) OpenObject(ctx context.Context, bucket, key string, opts *OpenObjectOptions) (*ObjectFile, error) {
	prefetch := 2
	if opts != nil && opts.Prefetch != 0 {
		prefetch = max(0, opts.Prefetch)
	}
	reads := s.reads()
	opened, err := reads.OpenFile(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	sess, dek := opened.Session, opened.DEK
	fetch := func(ctx context.Context, a, b int64) ([]byte, error) { return reads.Fetch(ctx, sess, dek, a, b) }
	info := directInfo(sess.ContentType, sess.ETag, sess.Size, sess.Size, sess.UploadedAt, sess.Metadata)
	blocks := direct.NewBlocks(ctx, fetch, sess.Size, opened.Block, opened.Align, prefetch, func() { clear(dek) })
	return &ObjectFile{Info: info, blocks: blocks}, nil
}

// Size is the object's size in bytes.
func (f *ObjectFile) Size() int64 { return f.blocks.Size() }

// ReadAt reads len(p) bytes from off; fewer only at the object's end, with
// io.EOF.
func (f *ObjectFile) ReadAt(p []byte, off int64) (int, error) { return f.blocks.ReadAt(p, off) }

// Read reads from where the last Read or Seek left off.
func (f *ObjectFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.blocks.ReadAt(p, f.pos)
	f.pos += int64(n)
	if err == io.EOF && n > 0 {
		err = nil // the next Read reports the end
	}
	return n, err
}

// Seek sets where the next Read starts, as io.Seeker does. A position past
// the end is allowed; reads there return io.EOF.
func (f *ObjectFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var pos int64
	switch whence {
	case io.SeekStart:
		pos = offset
	case io.SeekCurrent:
		pos = f.pos + offset
	case io.SeekEnd:
		pos = f.blocks.Size() + offset
	default:
		return f.pos, errors.New("teepin: Seek: invalid whence")
	}
	if pos < 0 {
		return f.pos, errors.New("teepin: Seek: negative position")
	}
	f.pos = pos
	return pos, nil
}

// Close stops the file's fetches and zeroes its copy of the data key. It is
// safe to call more than once.
func (f *ObjectFile) Close() error { return f.blocks.Close() }
