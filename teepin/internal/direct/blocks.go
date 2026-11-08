// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"sync"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
)

// ErrClosed is a read of an object file after Close. It is fs.ErrClosed, as for an os.File.
var ErrClosed = fmt.Errorf("teepin: the object file is closed: %w", fs.ErrClosed)

// FetchFunc fetches an object's verified bytes [a, b).
type FetchFunc func(ctx context.Context, a, b int64) ([]byte, error)

// Blocks reads an object at any offset through a fetch of verified ranges (TEEPIN-STORAGE-DESIGN.md section 22.5,
// open_object): the core of teepin.ObjectFile.
//
// Reading straight on (each read starting where the last ended) fetches a block at a time (one window, 64 MiB on the
// storage network) and the next prefetch blocks ahead, each in its own goroutine and signed as it is fetched; a read
// somewhere else fetches only what it needs, in whole segments (align). Up to prefetch + 2 blocks are held. A failed
// block is fetched again by the next read that needs it. It is safe for concurrent use.
type Blocks struct {
	// Fetch is the fetch; a test may wrap it before the first read.
	Fetch    FetchFunc
	size     int64
	block    int64
	align    int64
	prefetch int
	parent   context.Context
	ctx      context.Context // the file's: ends with the parent, or with Close
	cancel   context.CancelCauseFunc
	onClose  func()
	running  sync.WaitGroup // fetches and reads in progress

	mu      sync.Mutex
	entries map[int64]*block
	order   []int64 // oldest first
	end     int64   // where the last read ended: reading on from here is reading straight on
	closed  bool
}

type block struct {
	done    chan struct{}
	data    []byte
	err     error
	cancel  context.CancelFunc
	readers int
}

// NewBlocks reads an object of size bytes in blocks of block bytes, jumps aligned to align, with prefetch blocks
// fetched ahead (negative is none). ctx bounds every read: when it ends, so do they. onClose runs once, after Close
// has stopped every fetch (it zeroes the key copy the fetches use).
func NewBlocks(ctx context.Context, fetch FetchFunc, size, blk, align int64, prefetch int, onClose func()) *Blocks {
	fctx, cancel := context.WithCancelCause(ctx)
	return &Blocks{Fetch: fetch, size: size, block: max(1, blk), align: max(1, align), prefetch: max(0, prefetch),
		parent: ctx, ctx: fctx, cancel: cancel, onClose: onClose, entries: map[int64]*block{}}
}

// Size is the object's size in bytes.
func (b *Blocks) Size() int64 { return b.size }

// ReadAt reads len(p) bytes from off: fewer only at the object's end, where it returns io.EOF, as io.ReaderAt does.
// Every byte is verified before it is read.
func (b *Blocks) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("teepin: a read at a negative offset")
	}
	b.mu.Lock()
	if b.closed || b.parent.Err() != nil {
		b.mu.Unlock()
		return 0, b.stopped()
	}
	b.running.Add(1)
	straight := off == b.end
	_, held := b.entries[off/b.block]
	b.mu.Unlock()
	defer b.running.Done()

	n := min(int64(len(p)), max(0, b.size-off))
	if n == 0 {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	// A jump fetches only what it reads, unless the object is read whole anyway (packed): then its one block is
	// cached like any other.
	whole := b.align >= b.block
	var err error
	if !straight && !whole && n < b.block/2 && !held {
		err = b.some(p[:n], off)
	} else {
		err = b.blocks(p[:n], off)
	}
	if err == nil {
		b.mu.Lock()
		if b.closed {
			err = ErrClosed
		}
		b.end = off + n
		b.mu.Unlock()
	}
	if err != nil {
		if b.parent.Err() != nil || b.isClosed() {
			return 0, b.stopped()
		}
		return 0, err
	}
	if n < int64(len(p)) {
		return int(n), io.EOF
	}
	return int(n), nil
}

// stopped is why reads end: the parent context's error, or ErrClosed.
func (b *Blocks) stopped() error {
	if err := b.parent.Err(); err != nil {
		return err
	}
	return ErrClosed
}

func (b *Blocks) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// blocks fills p from off, a block at a time, then fetches ahead.
func (b *Blocks) blocks(p []byte, off int64) error {
	pos, got := off, 0
	for got < len(p) {
		i := pos / b.block
		data, err := b.get(i)
		if err != nil {
			return err
		}
		o := pos - i*b.block
		if o >= int64(len(data)) {
			return errs.Integrity(errs.ReasonTruncated, "the object is shorter than its size")
		}
		c := copy(p[got:], data[o:])
		got += c
		pos += int64(c)
	}
	b.ahead(pos / b.block)
	return nil
}

// some fills p from off with a fetch of its own, in whole segments.
func (b *Blocks) some(p []byte, off int64) error {
	a := off / b.align * b.align
	e := min(b.size, (off+int64(len(p))+b.align-1)/b.align*b.align)
	data, err := b.Fetch(b.ctx, a, e)
	if err != nil {
		return err
	}
	if int64(len(data)) < off-a+int64(len(p)) {
		return errs.Integrity(errs.ReasonTruncated, "the object is shorter than its size")
	}
	copy(p, data[off-a:])
	return nil
}

// get waits for block i, fetching it if it isn't held.
func (b *Blocks) get(i int64) ([]byte, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrClosed
	}
	e := b.submit(i)
	e.readers++
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		e.readers--
		b.mu.Unlock()
	}()
	select {
	case <-e.done:
		return e.data, e.err
	case <-b.ctx.Done():
		return nil, context.Cause(b.ctx)
	}
}

// submit starts block i's fetch unless it is held, and makes it the newest; the oldest past the bound are dropped
// (and stopped, when nobody waits for them). b.mu is held.
func (b *Blocks) submit(i int64) *block {
	e := b.entries[i]
	if e != nil {
		b.order = slices.DeleteFunc(b.order, func(j int64) bool { return j == i })
	} else {
		a := i * b.block
		end := min(a+b.block, b.size)
		ctx, cancel := context.WithCancel(b.ctx)
		e = &block{done: make(chan struct{}), cancel: cancel}
		b.entries[i] = e
		b.running.Add(1)
		go func() {
			defer b.running.Done()
			defer cancel()
			data, err := b.Fetch(ctx, a, end)
			e.data, e.err = data, err
			close(e.done)
			if err != nil {
				// Fetched again by the next read that needs it; a block fetched ahead that nobody waits for fails
				// quietly.
				b.mu.Lock()
				if b.entries[i] == e {
					delete(b.entries, i)
					b.order = slices.DeleteFunc(b.order, func(j int64) bool { return j == i })
				}
				b.mu.Unlock()
			}
		}()
	}
	b.order = append(b.order, i)
	for len(b.order) > b.prefetch+2 {
		oldest := b.order[0]
		b.order = b.order[1:]
		old := b.entries[oldest]
		delete(b.entries, oldest)
		if old != nil && old.readers == 0 {
			old.cancel()
		}
	}
	return e
}

// ahead starts the fetches of blocks i to i+prefetch.
func (b *Blocks) ahead(i int64) {
	if b.size == 0 {
		return
	}
	last := (b.size - 1) / b.block
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	for j := i; j <= min(i+int64(b.prefetch), last); j++ {
		if j*b.block < b.size {
			b.submit(j)
		}
	}
}

// Held is the blocks held, oldest first (for tests).
func (b *Blocks) Held() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.order)
}

// Wait waits for held block i's fetch and returns its bytes (for tests); an error when it isn't held.
func (b *Blocks) Wait(i int64) ([]byte, error) {
	b.mu.Lock()
	e := b.entries[i]
	b.mu.Unlock()
	if e == nil {
		return nil, fmt.Errorf("teepin: block %d isn't held", i)
	}
	<-e.done
	return e.data, e.err
}

// Close stops every fetch, waits for them (at most CleanupCap), then runs onClose. Reads after it fail with
// ErrClosed. It never fails.
func (b *Blocks) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.entries = map[int64]*block{}
	b.order = nil
	b.mu.Unlock()
	b.cancel(ErrClosed)
	WaitCapped(&b.running, CleanupCap)
	if b.onClose != nil {
		b.onClose()
	}
	return nil
}
