// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package tws1

import (
	"bytes"
	"crypto/rand"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func mustKey(t *testing.T) []byte {
	t.Helper()
	k, err := NewDataKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

var sizes = []int{0, 1, 100, SegmentSize - 1, SegmentSize, SegmentSize + 1, 2 * SegmentSize, 3*SegmentSize + 100}

func TestRoundTrip(t *testing.T) {
	dek := mustKey(t)
	for _, n := range sizes {
		pt := pattern(n)
		ct, err := Encrypt(dek, pt)
		if err != nil {
			t.Fatalf("size %d: %v", n, err)
		}
		if int64(len(ct)) != StoredSize(int64(n)) {
			t.Fatalf("size %d: stored %d bytes, StoredSize says %d", n, len(ct), StoredSize(int64(n)))
		}
		got, err := Decrypt(dek, ct)
		if err != nil {
			t.Fatalf("size %d: decrypt: %v", n, err)
		}
		if !bytes.Equal(got, pt) {
			t.Fatalf("size %d: round trip changed the bytes", n)
		}
	}
}

func TestSegmentsAndStoredSize(t *testing.T) {
	cases := []struct{ size, segments, stored int64 }{
		{0, 1, HeaderSize + TagSize},
		{1, 1, HeaderSize + TagSize + 1},
		{SegmentSize, 1, HeaderSize + TagSize + SegmentSize},
		{SegmentSize + 1, 2, HeaderSize + 2*TagSize + SegmentSize + 1},
	}
	for _, c := range cases {
		if got := Segments(c.size); got != c.segments {
			t.Errorf("Segments(%d) = %d, want %d", c.size, got, c.segments)
		}
		if got := StoredSize(c.size); got != c.stored {
			t.Errorf("StoredSize(%d) = %d, want %d", c.size, got, c.stored)
		}
	}
}

func TestFreshSaltEveryTime(t *testing.T) {
	dek := mustKey(t)
	a, _ := Encrypt(dek, pattern(1000))
	b, _ := Encrypt(dek, pattern(1000))
	if bytes.Equal(a[8:40], b[8:40]) || bytes.Equal(a, b) {
		t.Fatal("two encryptions of the same bytes share a salt")
	}
}

func TestWrongKeyFailsBeforeDecrypting(t *testing.T) {
	ct, _ := Encrypt(mustKey(t), pattern(5000))
	if _, err := Decrypt(mustKey(t), ct); !errors.Is(err, ErrCommitment) {
		t.Fatalf("wrong key: got %v, want ErrCommitment", err)
	}
}

func TestBadKeySize(t *testing.T) {
	if _, err := Encrypt(make([]byte, 16), nil); !errors.Is(err, ErrKeySize) {
		t.Fatalf("got %v, want ErrKeySize", err)
	}
}

func TestTamperingIsCaught(t *testing.T) {
	dek := mustKey(t)
	pt := pattern(3*SegmentSize + 10)
	ct, _ := Encrypt(dek, pt)
	flip := func(i int) []byte {
		c := bytes.Clone(ct)
		c[i] ^= 0x01
		return c
	}
	segStart := func(i int) int { return HeaderSize + i*StoredSegmentSize }
	cases := []struct {
		name string
		in   []byte
		want error
	}{
		{"magic", flip(0), ErrFormat},
		{"version", flip(4), ErrFormat},
		{"segment size", flip(6), ErrFormat},
		{"reserved", flip(7), ErrFormat},
		{"salt", flip(20), ErrCommitment},
		{"commitment", flip(50), ErrCommitment},
		{"first segment", flip(segStart(0) + 5), ErrAuth},
		{"last segment tag", flip(len(ct) - 1), ErrAuth},
		{"drop the last segment", ct[:segStart(3)], ErrAuth},
		{"cut inside the last segment", ct[:len(ct)-3], ErrAuth},
		{"extra byte", append(bytes.Clone(ct), 0), ErrAuth},
		{"header only", ct[:HeaderSize], ErrTruncated},
		{"short header", ct[:HeaderSize-1], ErrTruncated},
		{"segments swapped", func() []byte {
			c := bytes.Clone(ct)
			a := bytes.Clone(c[segStart(0):segStart(1)])
			copy(c[segStart(0):segStart(1)], c[segStart(1):segStart(2)])
			copy(c[segStart(1):segStart(2)], a)
			return c
		}(), ErrAuth},
	}
	for _, c := range cases {
		if _, err := Decrypt(dek, c.in); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
}

func TestRanges(t *testing.T) {
	dek := mustKey(t)
	for _, n := range sizes {
		pt := pattern(n)
		ct, _ := Encrypt(dek, pt)
		spans := [][2]int{{0, n}, {0, 0}, {n, 0}}
		if n > 2 {
			spans = append(spans, [2]int{1, n - 2}, [2]int{n / 2, 1})
		}
		if n > SegmentSize+10 {
			spans = append(spans, [2]int{SegmentSize - 5, 10}, [2]int{SegmentSize, 1}, [2]int{SegmentSize - 1, 2})
		}
		for _, s := range spans {
			r, err := Locate(int64(n), int64(s[0]), int64(s[1]))
			if err != nil {
				t.Fatalf("size %d range %v: %v", n, s, err)
			}
			got, err := DecryptRange(dek, ct[:HeaderSize], int64(n), r, ct[r.StoredOffset:r.StoredOffset+r.StoredLength])
			if err != nil {
				t.Fatalf("size %d range %v: %v", n, s, err)
			}
			if !bytes.Equal(got, pt[s[0]:s[0]+s[1]]) {
				t.Fatalf("size %d range %v: wrong bytes", n, s)
			}
		}
	}
	if _, err := Locate(10, 5, 6); err == nil {
		t.Fatal("a range past the end was accepted")
	}
	ct, _ := Encrypt(dek, pattern(2*SegmentSize))
	r, _ := Locate(2*SegmentSize, 0, 10)
	if _, err := DecryptRange(dek, ct[:HeaderSize], 2*SegmentSize, r, ct[r.StoredOffset:r.StoredOffset+r.StoredLength-1]); !errors.Is(err, ErrTruncated) {
		t.Fatalf("short range: got %v, want ErrTruncated", err)
	}
	r2 := r
	r2.FirstSegment = 1 // the right bytes presented as the wrong segment
	if _, err := DecryptRange(dek, ct[:HeaderSize], 2*SegmentSize, r2, ct[r.StoredOffset:r.StoredOffset+r.StoredLength]); !errors.Is(err, ErrAuth) {
		t.Fatalf("segment presented at the wrong index: got %v, want ErrAuth", err)
	}
}

func TestWrapUnwrap(t *testing.T) {
	priv, pub, err := GenerateAccountKey()
	if err != nil {
		t.Fatal(err)
	}
	if p2, _ := AccountPublicKey(priv); !bytes.Equal(p2, pub) {
		t.Fatal("AccountPublicKey disagrees with GenerateAccountKey")
	}
	var acct, obj, other [16]byte
	rand.Read(acct[:])
	rand.Read(obj[:])
	rand.Read(other[:])
	dek := mustKey(t)
	w, err := WrapKey(pub, acct, obj, dek)
	if err != nil {
		t.Fatal(err)
	}
	if len(w) != WrappedKeySize {
		t.Fatalf("wrapped key is %d bytes", len(w))
	}
	got, err := UnwrapKey(priv, acct, obj, w)
	if err != nil || !bytes.Equal(got, dek) {
		t.Fatalf("unwrap: %v", err)
	}
	priv2, _, _ := GenerateAccountKey()
	bad := bytes.Clone(w)
	bad[40] ^= 1
	for name, f := range map[string]func() ([]byte, error){
		"other object":  func() ([]byte, error) { return UnwrapKey(priv, acct, other, w) },
		"other account": func() ([]byte, error) { return UnwrapKey(priv, other, obj, w) },
		"other key":     func() ([]byte, error) { return UnwrapKey(priv2, acct, obj, w) },
		"damaged":       func() ([]byte, error) { return UnwrapKey(priv, acct, obj, bad) },
		"short":         func() ([]byte, error) { return UnwrapKey(priv, acct, obj, w[:79]) },
	} {
		if _, err := f(); !errors.Is(err, ErrUnwrap) {
			t.Errorf("%s: got %v, want ErrUnwrap", name, err)
		}
	}
}

func TestNeedles(t *testing.T) {
	var a, b [16]byte
	rand.Read(a[:])
	rand.Read(b[:])
	pack, offA := AppendNeedle(nil, a, []byte("first object"))
	pack, offB := AppendNeedle(pack, b, []byte("second"))
	if offA != NeedleHeaderSize || offB != 2*NeedleHeaderSize+12 {
		t.Fatalf("offsets %d, %d", offA, offB)
	}
	if got, err := ReadNeedle(pack, offB, b, 6); err != nil || string(got) != "second" {
		t.Fatalf("read: %q, %v", got, err)
	}
	for name, f := range map[string]func() ([]byte, error){
		"wrong id":   func() ([]byte, error) { return ReadNeedle(pack, offB, a, 6) },
		"wrong size": func() ([]byte, error) { return ReadNeedle(pack, offB, b, 5) },
		"outside":    func() ([]byte, error) { return ReadNeedle(pack, offB, b, 100) },
		"no header":  func() ([]byte, error) { return ReadNeedle(pack, 10, a, 2) },
		"damaged": func() ([]byte, error) {
			p := bytes.Clone(pack)
			p[offA] ^= 1
			return ReadNeedle(p, offA, a, 12)
		},
	} {
		if _, err := f(); !errors.Is(err, ErrNeedle) {
			t.Errorf("%s: got %v, want ErrNeedle", name, err)
		}
	}
}

// TestSaltSourceIsOnlyReplacedInTests checks that no production file assigns saltSource apart from its declaration,
// so a fixed salt can only ever come from this package's own tests (Encrypt also panics outside a test binary).
func TestSaltSourceIsOnlyReplacedInTests(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if as, ok := n.(*ast.AssignStmt); ok {
				for _, lhs := range as.Lhs {
					if id, ok := lhs.(*ast.Ident); ok && id.Name == "saltSource" {
						t.Errorf("%s assigns saltSource outside a test", fset.Position(as.Pos()))
					}
				}
			}
			return true
		})
	}
}
