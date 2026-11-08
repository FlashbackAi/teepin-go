// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// The three formats objects are stored in, as a read session names them (TEEPIN-STORAGE-DESIGN.md sections 17.7 and
// 21.3), a port of the Python SDK's _formats.py:
//
//   - tws1: encrypted on the device (internal/tws1), read by segment;
//   - teepin-v1: encrypted by Teepin when it staged a small object (teepin-core's crypt.go): one AES-256-GCM message,
//     nonce, ciphertext and tag, bound to the session's associated_data. It is read whole and its tag checked before
//     any byte is used;
//   - plain: stored as is (objects written before encryption).
//
// A session's source says where the bytes are: storage (fetched with the signed requests) or api (staged objects,
// whose bytes this SDK can't read: errs.ErrObjectStaged).
const (
	FormatTWS1   = "tws1"
	FormatServer = "teepin-v1"
	FormatPlain  = "plain"

	SourceStorage = "storage"
	SourceAPI     = "api"
)

const (
	serverNonceSize = 12
	serverTagSize   = 16
)

// knownFormat reports a format this engine reads.
func knownFormat(f string) bool { return f == FormatTWS1 || f == FormatServer || f == FormatPlain }

// OpenServerV1 is a teepin-v1 object's plaintext, after its tag is checked; a DataIntegrityError if it doesn't
// authenticate.
func OpenServerV1(key, stored, associatedData []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, errs.Integrity(errs.ReasonFormat, "a teepin-v1 key is 32 bytes")
	}
	if len(stored) < serverNonceSize+serverTagSize {
		return nil, errs.Integrity(errs.ReasonTruncated, "the object is too short to be encrypted")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errs.Integrity(errs.ReasonFormat, "a teepin-v1 key: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errs.Integrity(errs.ReasonFormat, "a teepin-v1 key: %v", err)
	}
	out, err := gcm.Open(nil, stored[:serverNonceSize], stored[serverNonceSize:], associatedData)
	if err != nil {
		return nil, errs.Integrity(errs.ReasonSegment, "the object failed authentication")
	}
	return out, nil
}

// integrityOf maps a tws1 failure to the DataIntegrityError the other SDKs raise for it, with its reason.
func integrityOf(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, tws1.ErrAuth):
		return errs.Integrity(errs.ReasonSegment, "a segment failed authentication")
	case errors.Is(err, tws1.ErrCommitment):
		return errs.Integrity(errs.ReasonCommitment, "the data key does not match the object's commitment")
	case errors.Is(err, tws1.ErrTruncated):
		return errs.Integrity(errs.ReasonTruncated, "the object is truncated")
	case errors.Is(err, tws1.ErrFormat):
		return errs.Integrity(errs.ReasonFormat, "not a TWS1 object, or an unsupported version")
	case errors.Is(err, tws1.ErrNeedle):
		return errs.Integrity(errs.ReasonNeedle, "the pack's needle does not match the object")
	case errors.Is(err, tws1.ErrKeySize):
		return errs.Integrity(errs.ReasonKey, "the read session's data key is not 32 bytes")
	}
	return errs.Integrity(errs.ReasonFormat, "%v", err)
}

// segmentAt is the stored offset and length of segment index of an object of plaintextSize bytes.
func segmentAt(plaintextSize, index int64) (int64, int64) {
	start := tws1.HeaderSize + index*tws1.StoredSegmentSize
	if index == tws1.Segments(plaintextSize)-1 {
		return start, tws1.StoredSize(plaintextSize) - start
	}
	return start, tws1.StoredSegmentSize
}

// segmentStart is where a read from segment index starts: 0 for the first (its header comes with it).
func segmentStart(plaintextSize, index int64) int64 {
	if index == 0 {
		return 0
	}
	off, _ := segmentAt(plaintextSize, index)
	return off
}

// checkHeader checks a TWS1 header against the data key: its format, then its key commitment in constant time, before
// any segment is used (section 22.5's order).
func checkHeader(dek, header []byte, plaintextSize int64) error {
	_, err := tws1.DecryptRange(dek, header, plaintextSize, tws1.Range{}, nil)
	return integrityOf(err)
}

// openSegment opens one sealed segment of a TWS1 object, whose header has been checked. Each call derives the
// object's keys again: the format package keeps them unexported, and the derivation costs a few microseconds against
// a segment's tens, far below a network's speed.
func openSegment(dek, header []byte, plaintextSize, index int64, sealed []byte) ([]byte, error) {
	plain := int64(len(sealed)) - tws1.TagSize
	if plain < 0 {
		return nil, errs.Integrity(errs.ReasonTruncated, "segment %d is truncated", index)
	}
	r := tws1.Range{FirstSegment: index, StoredLength: int64(len(sealed)), Length: plain}
	out, err := tws1.DecryptRange(dek, header, plaintextSize, r, sealed)
	return out, integrityOf(err)
}
