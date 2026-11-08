// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package tws1

import "fmt"

// OpenStream seals the parts of a large object whose size isn't known yet: a stream (TEEPIN-STORAGE-DESIGN.md
// section 22.1, E2 b). Every part but the last holds exactly the part size, and the last is sealed as the last when
// the stream ends. For the same key, header and plaintext it seals exactly what Stream does, so storage and readers
// can't tell the two apart.
//
// It is the SDK's own addition to this copy of the reference (the Python and Node SDKs have it too): the server never
// seals anything, so teepin-core's pkg/tws1 has no need of it. It holds only the keys derived from the data key, so
// the caller zeroes the data key as soon as it is wrapped.
type OpenStream struct {
	keys     *segmentKeys
	header   []byte
	partSize int64
}

// NewOpenStream starts a stream in parts of partSize plaintext bytes (a positive multiple of SegmentSize), under a
// fresh header.
func NewOpenStream(dek []byte, partSize int64) (*OpenStream, error) {
	if partSize <= 0 || partSize%SegmentSize != 0 {
		return nil, fmt.Errorf("%w: a part holds a positive multiple of %d plaintext bytes", ErrPartLayout, SegmentSize)
	}
	header, keys, err := newHeader(dek)
	if err != nil {
		return nil, err
	}
	return &OpenStream{keys: keys, header: header, partSize: partSize}, nil
}

// Header is the object's header: the start of part 1.
func (s *OpenStream) Header() []byte { return append([]byte(nil), s.header...) }

// SealPart seals part number from exactly its plaintext: partSize bytes, or for the last part 1 to partSize bytes,
// its last segment sealed as the object's last. As with Stream, never seal a part from other plaintext under the same
// header.
func (s *OpenStream) SealPart(number int, plain []byte, last bool) ([]byte, error) {
	n := int64(len(plain))
	switch {
	case number < 1:
		return nil, fmt.Errorf("%w: part %d", ErrPartLayout, number)
	case !last && n != s.partSize:
		return nil, fmt.Errorf("%w: part %d holds %d plaintext bytes, not %d", ErrPartLayout, number, n, s.partSize)
	case last && (n < 1 || n > s.partSize):
		return nil, fmt.Errorf("%w: the last part holds 1 to %d plaintext bytes, not %d", ErrPartLayout, s.partSize, n)
	}
	first := int64(number-1) * (s.partSize / SegmentSize)
	segments := (n + SegmentSize - 1) / SegmentSize
	if first+segments > MaxSegments {
		return nil, fmt.Errorf("%w: an object may have at most %d segments", ErrPartLayout, int64(MaxSegments))
	}
	size := segments*TagSize + n
	if number == 1 {
		size += HeaderSize
	}
	out := make([]byte, 0, size)
	if number == 1 {
		out = append(out, s.header...)
	}
	for i := int64(0); i < segments; i++ {
		start := i * SegmentSize
		end := min(start+SegmentSize, n)
		out = s.keys.aead.Seal(out, s.keys.nonce(first+i, last && i == segments-1), plain[start:end], s.header)
	}
	return out, nil
}
