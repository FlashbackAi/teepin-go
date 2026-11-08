// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package tws1

import (
	"errors"
	"fmt"
)

// Large objects (TEEPIN-STORAGE-DESIGN.md section 21.1, D1): a large object is one TWS1 stream, exactly as Encrypt
// makes it, uploaded in parts cut at segment boundaries. Part 1 is the header and the first segments; every later
// part is the next run of whole segments; the last part holds what is left. So a reader sees one ordinary TWS1
// object, and any range of it decrypts on its own.
//
// Retries. A part sent again is sealed again under the same header, so it must be sealed from exactly the same
// plaintext: then its bytes are identical, and no nonce is ever used for two different plaintexts. A client records
// each part's plaintext hash before the part's first byte leaves, and re-reads and re-hashes before sealing it again;
// a changed file restarts the upload with a fresh data key. The control plane backs this up: the first signature of
// a part fixes its MD5, so different bytes for a part can never be stored.

// Part is one part of a large object: which plaintext it holds and where its bytes sit in the stored object.
type Part struct {
	Number      int   // 1 to the number of parts
	PlainFirst  int64 // offset of its first plaintext byte
	PlainSize   int64
	StoredFirst int64 // offset of its first stored byte; part 1 starts at 0, with the header
	StoredSize  int64
}

// ErrPartLayout means an object can't be laid out in parts of the size asked for.
var ErrPartLayout = errors.New("tws1: invalid part layout")

// Parts lays out an object of plaintextSize bytes (at least 1) in parts of partSize plaintext bytes, a positive
// multiple of SegmentSize; the last part holds the rest. The parts' stored sizes add up to StoredSize(plaintextSize).
func Parts(plaintextSize, partSize int64) ([]Part, error) {
	if err := checkLayout(plaintextSize, partSize); err != nil {
		return nil, err
	}
	n := (plaintextSize + partSize - 1) / partSize
	parts := make([]Part, n)
	for j := int64(0); j < n; j++ {
		parts[j] = partAt(plaintextSize, partSize, j)
	}
	return parts, nil
}

// PartCount is how many parts Parts makes, without making them.
func PartCount(plaintextSize, partSize int64) (int64, error) {
	if err := checkLayout(plaintextSize, partSize); err != nil {
		return 0, err
	}
	return (plaintextSize + partSize - 1) / partSize, nil
}

// PartOf is part number (1-based) of the layout Parts makes.
func PartOf(plaintextSize, partSize int64, number int) (Part, error) {
	n, err := PartCount(plaintextSize, partSize)
	if err != nil {
		return Part{}, err
	}
	if number < 1 || int64(number) > n {
		return Part{}, fmt.Errorf("%w: part %d of %d", ErrPartLayout, number, n)
	}
	return partAt(plaintextSize, partSize, int64(number-1)), nil
}

func checkLayout(plaintextSize, partSize int64) error {
	switch {
	case plaintextSize < 1:
		return fmt.Errorf("%w: a large object has at least one byte", ErrPartLayout)
	case partSize <= 0 || partSize%SegmentSize != 0:
		return fmt.Errorf("%w: a part holds a positive multiple of %d plaintext bytes", ErrPartLayout, SegmentSize)
	case Segments(plaintextSize) > MaxSegments:
		return fmt.Errorf("%w: an object may have at most %d segments", ErrPartLayout, int64(MaxSegments))
	}
	return nil
}

// partAt is the j-th part (0-based) of a valid layout.
func partAt(plaintextSize, partSize, j int64) Part {
	p := Part{Number: int(j + 1), PlainFirst: j * partSize}
	p.PlainSize = min(partSize, plaintextSize-p.PlainFirst)
	segments := (p.PlainSize + SegmentSize - 1) / SegmentSize
	p.StoredSize = segments*TagSize + p.PlainSize
	if j == 0 {
		p.StoredSize += HeaderSize
	} else {
		p.StoredFirst = HeaderSize + (p.PlainFirst/SegmentSize)*StoredSegmentSize
	}
	return p
}

// Stream seals the parts of one large object, under one header.
type Stream struct {
	keys      *segmentKeys
	header    []byte
	size      int64
	segments  int64
	partSize  int64
	partCount int64
}

// NewStream starts a large object of plaintextSize bytes in parts of partSize, under a fresh header. The header
// (Header) must be kept, in the client's journal, to seal any part again after a restart.
func NewStream(dek []byte, plaintextSize, partSize int64) (*Stream, error) {
	n, err := PartCount(plaintextSize, partSize)
	if err != nil {
		return nil, err
	}
	header, keys, err := newHeader(dek)
	if err != nil {
		return nil, err
	}
	return &Stream{keys: keys, header: header, size: plaintextSize, segments: Segments(plaintextSize),
		partSize: partSize, partCount: n}, nil
}

// ResumeStream continues a large object from its header, after checking the header's commitment against dek.
func ResumeStream(dek, header []byte, plaintextSize, partSize int64) (*Stream, error) {
	n, err := PartCount(plaintextSize, partSize)
	if err != nil {
		return nil, err
	}
	keys, err := parseHeader(dek, header)
	if err != nil {
		return nil, err
	}
	return &Stream{keys: keys, header: append([]byte(nil), header[:HeaderSize]...), size: plaintextSize,
		segments: Segments(plaintextSize), partSize: partSize, partCount: n}, nil
}

// Header is the object's header: the start of part 1.
func (s *Stream) Header() []byte { return append([]byte(nil), s.header...) }

// SealPart seals one part from exactly its plaintext and returns its stored bytes. Sealing the same part from the
// same plaintext always gives the same bytes; never seal a part from other plaintext under the same header.
func (s *Stream) SealPart(number int, plain []byte) ([]byte, error) {
	if number < 1 || int64(number) > s.partCount {
		return nil, fmt.Errorf("%w: part %d of %d", ErrPartLayout, number, s.partCount)
	}
	p := partAt(s.size, s.partSize, int64(number-1))
	if int64(len(plain)) != p.PlainSize {
		return nil, fmt.Errorf("%w: part %d holds %d plaintext bytes, not %d", ErrPartLayout, number, p.PlainSize, len(plain))
	}
	out := make([]byte, 0, p.StoredSize)
	if number == 1 {
		out = append(out, s.header...)
	}
	first := p.PlainFirst / SegmentSize
	for off := int64(0); off < p.PlainSize; off += SegmentSize {
		i := first + off/SegmentSize
		end := min(off+SegmentSize, p.PlainSize)
		out = s.keys.aead.Seal(out, s.keys.nonce(i, i == s.segments-1), plain[off:end], s.header)
	}
	return out, nil
}
