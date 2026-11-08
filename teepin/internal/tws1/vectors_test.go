// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package tws1

import (
	"bytes"
	"crypto/ecdh"
	"crypto/hpke"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// The shared test vectors (testdata/vectors.json) are what every implementation of TWS1 checks itself against: the
// Python, Node and Go SDKs, the CLI, the apps and the console. They are made by teepin-core's pkg/tws1 (the reference,
// with -update there) and this file is a byte-identical copy, so this package checks itself against them and never
// writes them: TestVectorsMatchTheReference compares the copies when the other repositories are beside this one.
//
// Encryption vectors are deterministic: a fixed data key and salt, and a plaintext where byte i is i mod 251. HPKE
// wraps are randomized by design, so those vectors are checked in the decryption direction.

type objectVector struct {
	Name          string `json:"name"`
	DataKey       string `json:"data_key"`
	Salt          string `json:"salt"`
	PlaintextSize int64  `json:"plaintext_size"`
	StoredSize    int64  `json:"stored_size"`
	StoredSHA256  string `json:"stored_sha256"`
	Stored        string `json:"stored,omitempty"`
}

type rangeVector struct {
	Object       string `json:"object"`
	Offset       int64  `json:"offset"`
	Length       int64  `json:"length"`
	FirstSegment int64  `json:"first_segment"`
	StoredOffset int64  `json:"stored_offset"`
	StoredLength int64  `json:"stored_length"`
	Skip         int64  `json:"skip"`
}

type wrapVector struct {
	AccountPrivateKey string `json:"account_private_key"`
	AccountPublicKey  string `json:"account_public_key"`
	AccountID         string `json:"account_id"`
	ObjectID          string `json:"object_id"`
	Info              string `json:"info"`
	DataKey           string `json:"data_key"`
	Wrapped           string `json:"wrapped"`
}

type packVector struct {
	ObjectIDs []string `json:"object_ids"`
	Objects   []string `json:"objects"` // each object's stored bytes, in pack order
	Offsets   []int64  `json:"offsets"`
	Pack      string   `json:"pack"`
}

type rfcVector struct {
	Info  string `json:"info"`
	SkRm  string `json:"skRm"`
	PkRm  string `json:"pkRm"`
	Enc   string `json:"enc"`
	AAD0  string `json:"aad_0"`
	CT0   string `json:"ct_0"`
	AAD1  string `json:"aad_1"`
	CT1   string `json:"ct_1"`
	PT    string `json:"pt"`
	Notes string `json:"notes"`
}

// partVector is a large object's layout in parts (parts.go): each part's place and the digests of its stored bytes,
// which are a slice of the named object's stored bytes. The MD5 is what a part's upload is signed with.
type partVector struct {
	Object   string `json:"object"`
	PartSize int64  `json:"part_size"`
	Parts    []struct {
		Number       int    `json:"number"`
		PlainFirst   int64  `json:"plain_first"`
		PlainSize    int64  `json:"plain_size"`
		StoredFirst  int64  `json:"stored_first"`
		StoredSize   int64  `json:"stored_size"`
		StoredSHA256 string `json:"stored_sha256"`
		StoredMD5    string `json:"stored_md5"`
	} `json:"parts"`
}

type vectorFile struct {
	Format    string         `json:"format"`
	Spec      string         `json:"spec"`
	Plaintext string         `json:"plaintext"`
	Salts     string         `json:"salts"`
	Objects   []objectVector `json:"objects"`
	Ranges    []rangeVector  `json:"ranges"`
	Parts     []partVector   `json:"parts"`
	Wraps     []wrapVector   `json:"wraps"`
	Pack      packVector     `json:"pack"`
	RFC9180   rfcVector      `json:"rfc9180_a_1_1"`
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

func TestVectors(t *testing.T) {
	path := filepath.Join("testdata", "vectors.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read vectors (a copy of teepin-core/pkg/tws1/testdata/vectors.json): %v", err)
	}
	var vf vectorFile
	if err := json.Unmarshal(raw, &vf); err != nil {
		t.Fatal(err)
	}

	// Encryption is deterministic for a given key and salt, and decryption gives the pattern back.
	prev := saltSource
	t.Cleanup(func() { saltSource = prev })
	for _, ov := range vf.Objects {
		dek := unhex(t, ov.DataKey)
		saltSource = bytes.NewReader(unhex(t, ov.Salt))
		ct, err := Encrypt(dek, pattern(int(ov.PlaintextSize)))
		if err != nil {
			t.Fatalf("%s: %v", ov.Name, err)
		}
		sum := sha256.Sum256(ct)
		if hex.EncodeToString(sum[:]) != ov.StoredSHA256 || int64(len(ct)) != ov.StoredSize {
			t.Fatalf("%s: stored bytes differ from the vector", ov.Name)
		}
		if ov.Stored != "" && hex.EncodeToString(ct) != ov.Stored {
			t.Fatalf("%s: stored bytes differ from the vector", ov.Name)
		}
		pt, err := Decrypt(dek, ct)
		if err != nil || !bytes.Equal(pt, pattern(int(ov.PlaintextSize))) {
			t.Fatalf("%s: decrypt: %v", ov.Name, err)
		}
	}
	// A large object's parts, sealed one by one under its header, are slices of the object (parts.go).
	if len(vf.Parts) == 0 {
		t.Fatal("the vectors have no part layouts (the copy is incomplete)")
	}
	for _, pv := range vf.Parts {
		var ov objectVector
		for _, o := range vf.Objects {
			if o.Name == pv.Object {
				ov = o
			}
		}
		dek := unhex(t, ov.DataKey)
		saltSource = bytes.NewReader(unhex(t, ov.Salt))
		s, err := NewStream(dek, ov.PlaintextSize, pv.PartSize)
		if err != nil {
			t.Fatalf("parts of %s: %v", pv.Object, err)
		}
		plain := pattern(int(ov.PlaintextSize))
		for _, want := range pv.Parts {
			p, err := PartOf(ov.PlaintextSize, pv.PartSize, want.Number)
			if err != nil || p.PlainFirst != want.PlainFirst || p.PlainSize != want.PlainSize ||
				p.StoredFirst != want.StoredFirst || p.StoredSize != want.StoredSize {
				t.Fatalf("%s/%d part %d: layout %+v, %v", pv.Object, pv.PartSize, want.Number, p, err)
			}
			sealed, err := s.SealPart(p.Number, plain[p.PlainFirst:p.PlainFirst+p.PlainSize])
			s2, m5 := sha256.Sum256(sealed), md5.Sum(sealed)
			if err != nil || hex.EncodeToString(s2[:]) != want.StoredSHA256 || hex.EncodeToString(m5[:]) != want.StoredMD5 {
				t.Fatalf("%s/%d part %d: sealed bytes differ from the vector (%v)", pv.Object, pv.PartSize, want.Number, err)
			}
		}
	}
	for _, rv := range vf.Ranges {
		var size int64
		for _, ov := range vf.Objects {
			if ov.Name == rv.Object {
				size = ov.PlaintextSize
			}
		}
		loc, err := Locate(size, rv.Offset, rv.Length)
		if err != nil || loc.FirstSegment != rv.FirstSegment || loc.StoredOffset != rv.StoredOffset ||
			loc.StoredLength != rv.StoredLength || loc.Skip != rv.Skip {
			t.Fatalf("range %+v: Locate gave %+v, %v", rv, loc, err)
		}
	}
	for _, wv := range vf.Wraps {
		var acct, obj [16]byte
		copy(acct[:], unhex(t, wv.AccountID))
		copy(obj[:], unhex(t, wv.ObjectID))
		if pub, _ := AccountPublicKey(unhex(t, wv.AccountPrivateKey)); hex.EncodeToString(pub) != wv.AccountPublicKey {
			t.Fatal("wrap vector: public key does not match the private key")
		}
		if hex.EncodeToString(KeyInfo(acct, obj)) != wv.Info {
			t.Fatal("wrap vector: info string differs")
		}
		dek, err := UnwrapKey(unhex(t, wv.AccountPrivateKey), acct, obj, unhex(t, wv.Wrapped))
		if err != nil || hex.EncodeToString(dek) != wv.DataKey {
			t.Fatalf("wrap vector: %v", err)
		}
	}
	pack := unhex(t, vf.Pack.Pack)
	for i := range vf.Pack.ObjectIDs {
		var id [16]byte
		copy(id[:], unhex(t, vf.Pack.ObjectIDs[i]))
		want := unhex(t, vf.Pack.Objects[i])
		got, err := ReadNeedle(pack, vf.Pack.Offsets[i], id, int64(len(want)))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("pack needle %d: %v", i, err)
		}
	}

	// RFC 9180 A.1.1 through the standard library, the same suite WrapKey uses.
	rv := vf.RFC9180
	sk, err := ecdh.X25519().NewPrivateKey(unhex(t, rv.SkRm))
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(sk.PublicKey().Bytes()) != rv.PkRm {
		t.Fatal("RFC 9180 A.1.1: pkRm does not match skRm")
	}
	hsk, err := hpke.NewDHKEMPrivateKey(sk)
	if err != nil {
		t.Fatal(err)
	}
	r, err := hpke.NewRecipient(unhex(t, rv.Enc), hsk, hpke.HKDFSHA256(), hpke.AES128GCM(), unhex(t, rv.Info))
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range [][2]string{{rv.AAD0, rv.CT0}, {rv.AAD1, rv.CT1}} {
		pt, err := r.Open(unhex(t, c[0]), unhex(t, c[1]))
		if err != nil || hex.EncodeToString(pt) != rv.PT {
			t.Fatalf("RFC 9180 A.1.1 encryption %d: %v", i, err)
		}
	}
}

// TestVectorsMatchTheReference checks this copy of the vectors against teepin-core's (the reference that makes them)
// and the other SDKs' copies, whichever are checked out beside this repository. Each must be byte-identical.
func TestVectorsMatchTheReference(t *testing.T) {
	ours, err := os.ReadFile(filepath.Join("testdata", "vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join("..", "..", "..", "..")
	compared := 0
	for _, other := range []string{
		filepath.Join(root, "teepin-core", "pkg", "tws1", "testdata", "vectors.json"),
		filepath.Join(root, "teepin-python", "tests", "vectors", "tws1.json"),
		filepath.Join(root, "teepin-node", "test", "vectors", "tws1.json"),
	} {
		theirs, err := os.ReadFile(other)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		compared++
		if !bytes.Equal(ours, theirs) {
			t.Errorf("%s differs from this copy: copy teepin-core's vectors here again", other)
		}
	}
	t.Logf("compared with %d other copies", compared)
}
