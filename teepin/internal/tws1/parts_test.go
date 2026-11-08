// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package tws1

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"
)

// A large object sealed part by part is one ordinary TWS1 object: the parts are contiguous, add up to StoredSize,
// decrypt whole and by range, and a part sealed again from the same plaintext is byte for byte the same.
func TestParts_OneStream(t *testing.T) {
	const seg = SegmentSize
	cases := []struct{ size, partSize int64 }{
		{1, seg},
		{seg - 1, seg},
		{seg, seg},
		{seg + 1, seg},
		{3*seg + 100, 2 * seg},
		{4 * seg, 2 * seg},
		{5*seg + 7, 80 * seg},
		{81 * seg, 80 * seg},
		{200*seg + 33, 80 * seg},
	}
	for _, c := range cases {
		dek, _ := NewDataKey()
		plain := make([]byte, c.size)
		rand.Read(plain)
		parts, err := Parts(c.size, c.partSize)
		if err != nil {
			t.Fatalf("%d/%d: %v", c.size, c.partSize, err)
		}
		if n, _ := PartCount(c.size, c.partSize); n != int64(len(parts)) {
			t.Fatalf("%d/%d: PartCount %d, Parts %d", c.size, c.partSize, n, len(parts))
		}
		s, err := NewStream(dek, c.size, c.partSize)
		if err != nil {
			t.Fatal(err)
		}
		var stored []byte
		var nextPlain, nextStored int64
		for _, p := range parts {
			if p.PlainFirst != nextPlain || p.StoredFirst != nextStored {
				t.Fatalf("%d/%d part %d starts at %d/%d, want %d/%d", c.size, c.partSize, p.Number, p.PlainFirst,
					p.StoredFirst, nextPlain, nextStored)
			}
			if got, _ := PartOf(c.size, c.partSize, p.Number); got != p {
				t.Fatalf("PartOf %d: %+v, want %+v", p.Number, got, p)
			}
			sealed, err := s.SealPart(p.Number, plain[p.PlainFirst:p.PlainFirst+p.PlainSize])
			if err != nil || int64(len(sealed)) != p.StoredSize {
				t.Fatalf("%d/%d part %d: %d bytes, want %d (%v)", c.size, c.partSize, p.Number, len(sealed), p.StoredSize, err)
			}
			again, _ := s.SealPart(p.Number, plain[p.PlainFirst:p.PlainFirst+p.PlainSize])
			if !bytes.Equal(again, sealed) {
				t.Fatalf("part %d sealed again from the same plaintext differs", p.Number)
			}
			stored = append(stored, sealed...)
			nextPlain, nextStored = p.PlainFirst+p.PlainSize, p.StoredFirst+p.StoredSize
		}
		if nextPlain != c.size || int64(len(stored)) != StoredSize(c.size) {
			t.Fatalf("%d/%d: parts cover %d plaintext and %d stored bytes, want %d and %d", c.size, c.partSize,
				nextPlain, len(stored), c.size, StoredSize(c.size))
		}
		got, err := Decrypt(dek, stored)
		if err != nil || !bytes.Equal(got, plain) {
			t.Fatalf("%d/%d: the parts don't decrypt as one object: %v", c.size, c.partSize, err)
		}
		// A range across a part boundary.
		if len(parts) > 1 {
			off := parts[1].PlainFirst - 10
			length := min(20, c.size-off)
			r, err := Locate(c.size, off, length)
			if err != nil {
				t.Fatal(err)
			}
			part, err := DecryptRange(dek, stored[:HeaderSize], c.size, r, stored[r.StoredOffset:r.StoredOffset+r.StoredLength])
			if err != nil || !bytes.Equal(part, plain[off:off+length]) {
				t.Fatalf("%d/%d: a range across parts 1 and 2: %v", c.size, c.partSize, err)
			}
		}
		// After a restart, from the journalled header: the same bytes again.
		resumed, err := ResumeStream(dek, s.Header(), c.size, c.partSize)
		if err != nil {
			t.Fatal(err)
		}
		last := parts[len(parts)-1]
		again, err := resumed.SealPart(last.Number, plain[last.PlainFirst:])
		if err != nil || !bytes.Equal(again, stored[last.StoredFirst:]) {
			t.Fatalf("%d/%d: the last part sealed after a restart differs: %v", c.size, c.partSize, err)
		}
	}
}

func TestParts_Refusals(t *testing.T) {
	for _, c := range []struct{ size, partSize int64 }{{0, SegmentSize}, {10, 0}, {10, SegmentSize + 1}, {10, -SegmentSize}} {
		if _, err := Parts(c.size, c.partSize); !errors.Is(err, ErrPartLayout) {
			t.Errorf("Parts(%d, %d): %v, want ErrPartLayout", c.size, c.partSize, err)
		}
	}
	dek, _ := NewDataKey()
	s, err := NewStream(dek, 3*SegmentSize, 2*SegmentSize)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SealPart(3, nil); !errors.Is(err, ErrPartLayout) {
		t.Errorf("a part past the end: %v", err)
	}
	if _, err := s.SealPart(1, make([]byte, 10)); !errors.Is(err, ErrPartLayout) {
		t.Errorf("a part of the wrong size: %v", err)
	}
	other, _ := NewDataKey()
	if _, err := ResumeStream(other, s.Header(), 3*SegmentSize, 2*SegmentSize); !errors.Is(err, ErrCommitment) {
		t.Errorf("resuming with another key: %v, want ErrCommitment", err)
	}
}
