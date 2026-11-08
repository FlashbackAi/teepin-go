// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

// Package tws1 is TWS1, the format in which Teepin's clients encrypt every object before it leaves the customer's
// device, and the key wrapping that goes with it (TEEPIN-STORAGE-DESIGN.md sections 11.3, 11.4 and 20.1).
//
// It is the Go SDK's copy of teepin-core's pkg/tws1, the reference, which makes the test vectors every client checks
// against (testdata/vectors.json, copied byte for byte). The SDK can't import teepin-core without pulling the API
// server's dependencies into customers' builds (section 25.2), so the two are kept in step by those vectors.
//
// An object is a 72-byte header followed by segments of 64 KiB of plaintext, each sealed with AES-256-GCM:
//
//	header   = "TWS1" | version 0x01 | algorithm 0x01 | log2(segment size) 0x10 | 0x00 | salt (32) | commitment (32)
//	PRK      = HKDF-Extract(SHA-512, salt, data key)
//	commit   = HKDF-Expand(SHA-512, PRK, "tws1/commit" | header[0:40], 32)
//	key      = HKDF-Expand(SHA-512, PRK, "tws1/key"    | header[0:40], 32)
//	prefix   = HKDF-Expand(SHA-512, PRK, "tws1/nonce"  | header[0:40], 7)
//	nonce(i) = prefix | uint32 big-endian i | 0x01 for the last segment, else 0x00
//	segment  = AES-256-GCM(key, nonce(i), plaintext segment, associated data = the whole 72-byte header)
//
// The commitment binds the data key to the object, so a reader with the wrong key fails before decrypting anything,
// and no object can be crafted to decrypt differently under two keys. The last-segment flag stops truncation and the
// segment index stops reordering. An empty object is one empty last segment.
package tws1

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
)

const (
	// HeaderSize is the size of an object's header.
	HeaderSize = 72
	// SegmentSize is how much plaintext one segment holds; only the last segment may hold less.
	SegmentSize = 1 << segmentLog2
	// TagSize is the authentication tag each segment adds.
	TagSize = 16
	// StoredSegmentSize is the stored size of every segment but the last.
	StoredSegmentSize = SegmentSize + TagSize
	// KeySize is the size of an object's data key.
	KeySize = 32
	// SaltSize is the size of the salt in the header.
	SaltSize = 32
	// MaxSegments is the most segments one object may have (the index is 32 bits).
	MaxSegments = 1<<32 - 1

	version     = 0x01
	algorithm   = 0x01
	segmentLog2 = 16
	prefixSize  = 7
	boundSize   = 40 // header bytes the derivations are bound to: everything before the commitment
)

var magic = [4]byte{'T', 'W', 'S', '1'}

var (
	// ErrFormat means the bytes are not a TWS1 object this package understands.
	ErrFormat = errors.New("tws1: not a TWS1 object, or an unsupported version")
	// ErrCommitment means the data key is not the one the object was encrypted with.
	ErrCommitment = errors.New("tws1: the data key does not match the object's commitment")
	// ErrAuth means a segment failed authentication: the bytes were damaged or altered.
	ErrAuth = errors.New("tws1: a segment failed authentication")
	// ErrTruncated means the object is cut short, or has bytes after its last segment.
	ErrTruncated = errors.New("tws1: the object is truncated or has extra bytes")
	// ErrKeySize means a data key is not KeySize bytes.
	ErrKeySize = errors.New("tws1: a data key must be 32 bytes")
)

// Segments returns how many segments an object of plaintextSize bytes has. An empty object has one.
func Segments(plaintextSize int64) int64 {
	if plaintextSize <= 0 {
		return 1
	}
	return (plaintextSize + SegmentSize - 1) / SegmentSize
}

// StoredSize returns how many bytes an object of plaintextSize bytes takes once encrypted.
func StoredSize(plaintextSize int64) int64 {
	return HeaderSize + Segments(plaintextSize)*TagSize + plaintextSize
}

// segmentKeys are the keys derived from one object's data key and header.
type segmentKeys struct {
	aead       cipher.AEAD
	prefix     [prefixSize]byte
	commitment [32]byte
}

func deriveKeys(dek, header []byte) (*segmentKeys, error) {
	if len(dek) != KeySize {
		return nil, ErrKeySize
	}
	bound := string(header[:boundSize])
	prk, err := hkdf.Extract(sha512.New, dek, header[8:8+SaltSize])
	if err != nil {
		return nil, fmt.Errorf("tws1: derive keys: %w", err)
	}
	commitment, err := hkdf.Expand(sha512.New, prk, "tws1/commit"+bound, 32)
	if err != nil {
		return nil, fmt.Errorf("tws1: derive keys: %w", err)
	}
	key, err := hkdf.Expand(sha512.New, prk, "tws1/key"+bound, 32)
	if err != nil {
		return nil, fmt.Errorf("tws1: derive keys: %w", err)
	}
	prefix, err := hkdf.Expand(sha512.New, prk, "tws1/nonce"+bound, prefixSize)
	if err != nil {
		return nil, fmt.Errorf("tws1: derive keys: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("tws1: derive keys: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("tws1: derive keys: %w", err)
	}
	k := &segmentKeys{aead: aead}
	copy(k.prefix[:], prefix)
	copy(k.commitment[:], commitment)
	return k, nil
}

func (k *segmentKeys) nonce(index int64, last bool) []byte {
	n := make([]byte, 12)
	copy(n, k.prefix[:])
	binary.BigEndian.PutUint32(n[prefixSize:], uint32(index))
	if last {
		n[11] = 1
	}
	return n
}

// saltSource supplies every salt. Only this package's tests replace it, to generate the fixed test vectors; outside a
// test binary a replaced source makes Encrypt panic, so a fixed salt can never reach a real object.
var saltSource io.Reader = rand.Reader

// NewDataKey returns a fresh random data key. Every object gets its own, and a key is never used for two objects.
func NewDataKey() ([]byte, error) {
	dek := make([]byte, KeySize)
	if _, err := rand.Read(dek); err != nil {
		return nil, fmt.Errorf("tws1: new data key: %w", err)
	}
	return dek, nil
}

// Encrypt encrypts plaintext under dek as one TWS1 object, with a fresh random salt. Every encryption uses a fresh
// salt (TEEPIN-STORAGE-DESIGN.md section 11.3): a retried upload is encrypted again, never re-sealed under an old
// header. Clients store a 0-byte object as a catalog entry only (section 15.4, row Q1); the format still defines it,
// as one empty last segment.
func Encrypt(dek, plaintext []byte) ([]byte, error) {
	if len(dek) != KeySize {
		return nil, ErrKeySize
	}
	n := Segments(int64(len(plaintext)))
	if n > MaxSegments {
		return nil, fmt.Errorf("tws1: an object may have at most %d segments", int64(MaxSegments))
	}
	header, keys, err := newHeader(dek)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, StoredSize(int64(len(plaintext))))
	out = append(out, header...)
	for i := int64(0); i < n; i++ {
		start := i * SegmentSize
		end := min(start+SegmentSize, int64(len(plaintext)))
		out = keys.aead.Seal(out, keys.nonce(i, i == n-1), plaintext[start:end], header)
	}
	return out, nil
}

// newHeader makes a fresh object header under dek (a new random salt and its commitment) and derives its keys.
func newHeader(dek []byte) ([]byte, *segmentKeys, error) {
	if len(dek) != KeySize {
		return nil, nil, ErrKeySize
	}
	if saltSource != rand.Reader && !testing.Testing() {
		panic("tws1: the salt source was replaced outside a test")
	}
	header := make([]byte, HeaderSize)
	copy(header[0:4], magic[:])
	header[4], header[5], header[6], header[7] = version, algorithm, segmentLog2, 0
	if _, err := io.ReadFull(saltSource, header[8:8+SaltSize]); err != nil {
		return nil, nil, fmt.Errorf("tws1: read salt: %w", err)
	}
	keys, err := deriveKeys(dek, header)
	if err != nil {
		return nil, nil, err
	}
	copy(header[boundSize:], keys.commitment[:])
	return header, keys, nil
}

// parseHeader checks an object's header and derives its keys. It compares the commitment, in constant time, before
// anything is decrypted, and fails with ErrCommitment for the wrong data key.
func parseHeader(dek, header []byte) (*segmentKeys, error) {
	if len(header) < HeaderSize {
		return nil, ErrTruncated
	}
	header = header[:HeaderSize]
	if [4]byte(header[0:4]) != magic || header[4] != version || header[5] != algorithm ||
		header[6] != segmentLog2 || header[7] != 0 {
		return nil, ErrFormat
	}
	keys, err := deriveKeys(dek, header)
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare(keys.commitment[:], header[boundSize:HeaderSize]) != 1 {
		return nil, ErrCommitment
	}
	return keys, nil
}

// Decrypt checks and decrypts a whole TWS1 object.
func Decrypt(dek, stored []byte) ([]byte, error) {
	keys, err := parseHeader(dek, stored)
	if err != nil {
		return nil, err
	}
	header, body := stored[:HeaderSize], stored[HeaderSize:]
	if len(body) < TagSize {
		return nil, ErrTruncated
	}
	n := (int64(len(body)) + StoredSegmentSize - 1) / StoredSegmentSize
	if last := int64(len(body)) - (n-1)*StoredSegmentSize; last < TagSize {
		return nil, ErrTruncated // a final segment too short to hold its tag
	}
	out := make([]byte, 0, int64(len(body))-n*TagSize)
	for i := int64(0); i < n; i++ {
		start := i * StoredSegmentSize
		end := min(start+StoredSegmentSize, int64(len(body)))
		if out, err = keys.aead.Open(out, keys.nonce(i, i == n-1), body[start:end], header); err != nil {
			return nil, ErrAuth
		}
	}
	return out, nil
}

// Range says which stored bytes hold a plaintext range, and how to cut the range out of their decryption.
type Range struct {
	FirstSegment int64 // index of the first segment to fetch
	StoredOffset int64 // where that segment starts in the stored object
	StoredLength int64 // how many stored bytes to fetch, whole segments only
	Skip         int64 // plaintext bytes to drop from the front of the decrypted segments
	Length       int64 // plaintext bytes to return after Skip
}

// Locate maps the plaintext range [offset, offset+length) of an object of plaintextSize bytes onto its stored
// segments. A reader fetches the header (the first HeaderSize bytes) and Range's stored bytes, then calls
// DecryptRange.
func Locate(plaintextSize, offset, length int64) (Range, error) {
	if offset < 0 || length < 0 || offset+length > plaintextSize {
		return Range{}, fmt.Errorf("tws1: range [%d, %d) is outside an object of %d bytes", offset, offset+length,
			plaintextSize)
	}
	n := Segments(plaintextSize)
	first := min(offset/SegmentSize, n-1)
	last := first
	if length > 0 {
		last = (offset + length - 1) / SegmentSize
	}
	storedEnd := HeaderSize + last*StoredSegmentSize + StoredSegmentSize
	if last == n-1 {
		storedEnd = StoredSize(plaintextSize)
	}
	start := HeaderSize + first*StoredSegmentSize
	return Range{
		FirstSegment: first,
		StoredOffset: start,
		StoredLength: storedEnd - start,
		Skip:         offset - first*SegmentSize,
		Length:       length,
	}, nil
}

// DecryptRange checks and decrypts the stored segments r describes. header is the object's first HeaderSize bytes,
// stored the r.StoredLength bytes at r.StoredOffset, and plaintextSize the object's size (it says which segment is
// the last). It returns exactly the plaintext range Locate was asked for.
func DecryptRange(dek, header []byte, plaintextSize int64, r Range, stored []byte) ([]byte, error) {
	keys, err := parseHeader(dek, header)
	if err != nil {
		return nil, err
	}
	if int64(len(stored)) != r.StoredLength {
		return nil, ErrTruncated
	}
	n := Segments(plaintextSize)
	out := make([]byte, 0, r.StoredLength)
	for i, off := r.FirstSegment, int64(0); off < int64(len(stored)); i++ {
		end := min(off+StoredSegmentSize, int64(len(stored)))
		if i >= n {
			return nil, ErrTruncated
		}
		if out, err = keys.aead.Open(out, keys.nonce(i, i == n-1), stored[off:end], header[:HeaderSize]); err != nil {
			return nil, ErrAuth
		}
		off = end
	}
	if r.Skip+r.Length > int64(len(out)) {
		return nil, ErrTruncated
	}
	return out[r.Skip : r.Skip+r.Length], nil
}
