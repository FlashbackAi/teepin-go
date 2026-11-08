// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package tws1

import (
	"crypto/ecdh"
	"crypto/hpke"
	"errors"
	"fmt"
)

// Data keys are wrapped with HPKE (RFC 9180) in base mode, to the account's public key, with the suite of RFC 9180
// appendix A.1, so every implementation can check itself against the RFC's own vectors:
// DHKEM(X25519, HKDF-SHA256), HKDF-SHA256, AES-128-GCM. The info string binds a wrapped key to its account and
// object, so a wrapped key moved to another catalog row does not open; a copy re-wraps the key instead
// (TEEPIN-STORAGE-DESIGN.md section 15.4, row D6, and section 20.1).
const (
	// KEMID, KDFID and AEADID name the HPKE suite.
	KEMID  = 0x0020
	KDFID  = 0x0001
	AEADID = 0x0001
	// Suite is the suite's name in the API.
	Suite = "x25519-hkdfsha256-aes128gcm"
	// PublicKeySize and PrivateKeySize are the sizes of an account's X25519 key pair.
	PublicKeySize  = 32
	PrivateKeySize = 32
	// WrappedKeySize is a wrapped data key: the 32-byte encapsulated key, then the 32-byte key sealed with its tag.
	WrappedKeySize = 32 + KeySize + TagSize

	infoLabel = "teepin/tws1/dek/v1"
)

// ErrUnwrap means a wrapped data key did not open: the wrong account key, the wrong object, or damaged bytes.
var ErrUnwrap = errors.New("tws1: the wrapped data key does not open for this account and object")

// KeyInfo is the HPKE info string for one object's data key.
func KeyInfo(accountID, objectID [16]byte) []byte {
	info := make([]byte, 0, len(infoLabel)+32)
	info = append(info, infoLabel...)
	info = append(info, accountID[:]...)
	return append(info, objectID[:]...)
}

// GenerateAccountKey returns a new X25519 key pair for an account: the private key's 32 bytes and the public key's.
func GenerateAccountKey() (private, public []byte, err error) {
	k, err := ecdh.X25519().GenerateKey(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("tws1: generate account key: %w", err)
	}
	return k.Bytes(), k.PublicKey().Bytes(), nil
}

// AccountPublicKey returns the public key that belongs to an account's private key.
func AccountPublicKey(private []byte) ([]byte, error) {
	k, err := ecdh.X25519().NewPrivateKey(private)
	if err != nil {
		return nil, fmt.Errorf("tws1: account private key: %w", err)
	}
	return k.PublicKey().Bytes(), nil
}

// WrapKey wraps one object's data key to the account's public key.
func WrapKey(public []byte, accountID, objectID [16]byte, dek []byte) ([]byte, error) {
	if len(dek) != KeySize {
		return nil, ErrKeySize
	}
	pub, err := ecdh.X25519().NewPublicKey(public)
	if err != nil {
		return nil, fmt.Errorf("tws1: account public key: %w", err)
	}
	pk, err := hpke.NewDHKEMPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("tws1: account public key: %w", err)
	}
	wrapped, err := hpke.Seal(pk, hpke.HKDFSHA256(), hpke.AES128GCM(), KeyInfo(accountID, objectID), dek)
	if err != nil {
		return nil, fmt.Errorf("tws1: wrap data key: %w", err)
	}
	if len(wrapped) != WrappedKeySize {
		return nil, fmt.Errorf("tws1: wrapped data key is %d bytes, want %d", len(wrapped), WrappedKeySize)
	}
	return wrapped, nil
}

// UnwrapKey opens one object's wrapped data key with the account's private key.
func UnwrapKey(private []byte, accountID, objectID [16]byte, wrapped []byte) ([]byte, error) {
	if len(wrapped) != WrappedKeySize {
		return nil, ErrUnwrap
	}
	priv, err := ecdh.X25519().NewPrivateKey(private)
	if err != nil {
		return nil, fmt.Errorf("tws1: account private key: %w", err)
	}
	sk, err := hpke.NewDHKEMPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("tws1: account private key: %w", err)
	}
	dek, err := hpke.Open(sk, hpke.HKDFSHA256(), hpke.AES128GCM(), KeyInfo(accountID, objectID), wrapped)
	if err != nil || len(dek) != KeySize {
		return nil, ErrUnwrap
	}
	return dek, nil
}
