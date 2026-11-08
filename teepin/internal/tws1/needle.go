// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package tws1

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// A client pack holds its objects as needles, in exactly the format the control plane's own packs use
// (teepin-core's pkg/objectstore, packing.go): a 64-byte header, then the object's stored (encrypted) bytes. The header is
// "TPK1" | object id (16) | stored size (uint64 big-endian) | SHA-256 of the stored bytes (32) | 4 zero bytes. It makes
// every pack self-describing, so its index could be rebuilt from the pack alone, and lets a scrub check each object.
const NeedleHeaderSize = 64

var needleMagic = [4]byte{'T', 'P', 'K', '1'}

// ErrNeedle means a needle does not match the catalog, lies outside the pack, or fails its checksum.
var ErrNeedle = errors.New("tws1: needle does not match its object")

// AppendNeedle appends one object's needle to pack and returns the extended pack and the offset of the object's
// stored bytes within it (the offset the control plane records).
func AppendNeedle(pack []byte, objectID [16]byte, stored []byte) ([]byte, int64) {
	var h [NeedleHeaderSize]byte
	copy(h[0:4], needleMagic[:])
	copy(h[4:20], objectID[:])
	binary.BigEndian.PutUint64(h[20:28], uint64(len(stored)))
	sum := sha256.Sum256(stored)
	copy(h[28:60], sum[:])
	pack = append(pack, h[:]...)
	offset := int64(len(pack))
	return append(pack, stored...), offset
}

// ReadNeedle checks the needle whose stored bytes start at offset in pack and returns those bytes.
func ReadNeedle(pack []byte, offset int64, objectID [16]byte, storedSize int64) ([]byte, error) {
	start := offset - NeedleHeaderSize
	if start < 0 || storedSize < 0 || offset+storedSize > int64(len(pack)) {
		return nil, ErrNeedle
	}
	h := pack[start:offset]
	if !bytes.Equal(h[0:4], needleMagic[:]) || !bytes.Equal(h[4:20], objectID[:]) ||
		binary.BigEndian.Uint64(h[20:28]) != uint64(storedSize) || !bytes.Equal(h[60:64], []byte{0, 0, 0, 0}) {
		return nil, ErrNeedle
	}
	stored := pack[offset : offset+storedSize]
	if sum := sha256.Sum256(stored); !bytes.Equal(sum[:], h[28:60]) {
		return nil, ErrNeedle
	}
	return stored, nil
}
