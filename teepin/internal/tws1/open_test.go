// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package tws1

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"
)

// OpenStream seals exactly what Stream does for the same key, header and plaintext, so storage and readers can't tell
// a stream's object from one of known size (the Python SDK's test of _tws1.OpenStream).
func TestOpenStream_SealsExactlyWhatStreamDoes(t *testing.T) {
	const part = 2 * SegmentSize
	for _, size := range []int{1, SegmentSize, part, part + 1, 3*part - 7, 3 * part, 4*part + SegmentSize} {
		dek, _ := NewDataKey()
		plain := make([]byte, size)
		_, _ = rand.Read(plain)
		open, err := NewOpenStream(dek, part)
		if err != nil {
			t.Fatal(err)
		}
		known, err := ResumeStream(dek, open.Header(), int64(size), part)
		if err != nil {
			t.Fatal(err)
		}
		var whole []byte
		layout, _ := Parts(int64(size), part)
		for _, p := range layout {
			last := p.Number == len(layout)
			chunk := plain[p.PlainFirst : p.PlainFirst+p.PlainSize]
			got, err := open.SealPart(p.Number, chunk, last)
			if err != nil {
				t.Fatal(err)
			}
			want, _ := known.SealPart(p.Number, chunk)
			if !bytes.Equal(got, want) {
				t.Fatalf("%d bytes: part %d differs", size, p.Number)
			}
			whole = append(whole, got...)
		}
		if back, err := Decrypt(dek, whole); err != nil || !bytes.Equal(back, plain) {
			t.Fatalf("%d bytes: %v", size, err)
		}
	}
}

func TestOpenStream_RefusesPartsOfAnotherSize(t *testing.T) {
	dek, _ := NewDataKey()
	if _, err := NewOpenStream(dek, SegmentSize+1); !errors.Is(err, ErrPartLayout) {
		t.Fatal(err)
	}
	s, _ := NewOpenStream(dek, SegmentSize)
	for _, c := range []struct {
		n    int
		size int
		last bool
	}{{1, SegmentSize - 1, false}, {1, 0, true}, {1, SegmentSize + 1, true}, {0, SegmentSize, false}} {
		if _, err := s.SealPart(c.n, make([]byte, c.size), c.last); !errors.Is(err, ErrPartLayout) {
			t.Fatalf("%+v: %v", c, err)
		}
	}
}
