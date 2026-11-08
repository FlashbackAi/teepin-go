// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package fakes

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// Fakes are the three fakes wired together, as the Python tests' fake and direct fixtures make them: an API, its
// direct routes and their storage network, with a "photos" bucket.
type Fakes struct {
	API     *API
	Direct  *Direct
	Network *Network
	// HTTP sends the helpers' own requests.
	HTTP *http.Client
	idem int
}

// Start starts the fakes, stops them when tb's test ends, and fails the test if a route of the fakes failed (a bug of
// the fakes, answered 500).
func Start(tb testing.TB) *Fakes {
	tb.Helper()
	api := StartAPI()
	d := StartDirect(api, nil)
	api.Do(func() { api.Bucket("photos") })
	f := &Fakes{API: api, Direct: d, Network: d.Network, HTTP: &http.Client{}}
	tb.Cleanup(func() {
		f.HTTP.CloseIdleConnections()
		d.Close()
		api.Close()
		api.Do(func() {
			for _, err := range api.Errors {
				tb.Errorf("a route of the fakes failed: %v", err)
			}
		})
	})
	return f
}

// Answer is an answer, read whole.
type Answer struct {
	Status int
	Header http.Header
	Body   []byte
}

// JSON is the answer's body decoded as JSON (nil when it isn't).
func (a *Answer) JSON() map[string]any {
	var m map[string]any
	_ = json.Unmarshal(a.Body, &m)
	return m
}

// Code is a JSON error answer's code.
func (a *Answer) Code() string {
	s, _ := a.JSON()["code"].(string)
	return s
}

func (f *Fakes) do(req *http.Request) (*Answer, error) {
	resp, err := f.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &Answer{Status: resp.StatusCode, Header: resp.Header, Body: body}, nil
}

// Call makes a call to the fake API under /v1/storage, as a client makes one: body (nil for none) as JSON, with the
// API key.
func (f *Fakes) Call(method, path string, body any, query map[string]string) (*Answer, error) {
	u := f.API.URL + "/v1/storage" + path
	if len(query) > 0 {
		v := url.Values{}
		for k, x := range query {
			v.Set(k, x)
		}
		u += "?" + v.Encode()
	}
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, u, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+APIKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return f.do(req)
}

// mustCall is Call, which must answer below 300.
func (f *Fakes) mustCall(method, path string, body any, out any) error {
	a, err := f.Call(method, path, body, nil)
	if err != nil {
		return err
	}
	if a.Status >= 300 {
		return fmt.Errorf("%s %s: %d %s", method, path, a.Status, a.Body)
	}
	if out != nil && len(a.Body) > 0 {
		return json.Unmarshal(a.Body, out)
	}
	return nil
}

// Send sends a signed request as it was signed, with body.
func (f *Fakes) Send(s Signed, body []byte) (*Answer, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(s.Method, s.URL, r)
	if err != nil {
		return nil, err
	}
	for k, v := range s.Headers {
		switch k {
		case "content-length":
			if _, err := fmt.Sscan(v, &req.ContentLength); err != nil {
				return nil, err
			}
		default:
			req.Header.Set(k, v)
		}
	}
	if req.ContentLength == 0 {
		req.Body = http.NoBody
	}
	return f.do(req)
}

func (f *Fakes) nextIdem(prefix string) string {
	f.idem++
	return fmt.Sprintf("%s%d", prefix, f.idem)
}

func hexSum(b []byte) (string, string) {
	m := md5.Sum(b)
	s := sha256.Sum256(b)
	return hex.EncodeToString(m[:]), hex.EncodeToString(s[:])
}

// PutPacked stores a small object as a client packs it: TWS1 behind its needle, in a pack of its own, reserved,
// signed, sent and committed through the fakes' routes.
func (f *Fakes) PutPacked(bucket, key string, data []byte, contentType string) error {
	_, sum := hexSum(data)
	var res struct {
		Pack       string `json:"pack"`
		AccountKey struct {
			AccountID string `json:"account_id"`
			Version   int    `json:"version"`
			PublicKey string `json:"public_key"`
		} `json:"account_key"`
		Objects []struct {
			Key      string `json:"key"`
			ObjectID string `json:"object_id"`
		} `json:"objects"`
	}
	reserve := map[string]any{"idempotency_key": f.nextIdem("i"), "objects": []any{map[string]any{"key": key,
		"size": len(data), "sha256": sum, "content_type": contentType}}}
	if err := f.mustCall(http.MethodPost, "/buckets/"+bucket+"/packs", reserve, &res); err != nil {
		return err
	}
	pub, err := base64.StdEncoding.DecodeString(res.AccountKey.PublicKey)
	if err != nil {
		return err
	}
	account, _ := IDBytes(res.AccountKey.AccountID)
	var pack []byte
	var objects []any
	for _, o := range res.Objects {
		dek, err := tws1.NewDataKey()
		if err != nil {
			return err
		}
		stored, err := tws1.Encrypt(dek, data)
		if err != nil {
			return err
		}
		oid, _ := IDBytes(o.ObjectID)
		var offset int64
		pack, offset = tws1.AppendNeedle(pack, oid, stored)
		wrapped, err := tws1.WrapKey(pub, account, oid, dek)
		if err != nil {
			return err
		}
		objects = append(objects, map[string]any{"object_id": o.ObjectID, "offset": offset, "stored_size": len(stored),
			"wrapped_key": b64(wrapped), "key_version": res.AccountKey.Version})
	}
	pmd5, psha := hexSum(pack)
	var signed Signed
	base := "/buckets/" + bucket + "/packs/" + res.Pack
	if err := f.mustCall(http.MethodPost, base+"/sign", map[string]any{"size": len(pack), "md5": pmd5, "sha256": psha, "objects": objects}, &signed); err != nil {
		return err
	}
	a, err := f.Send(signed, pack)
	if err != nil {
		return err
	}
	if a.Status != http.StatusOK {
		return fmt.Errorf("PUT of the pack: %d %s", a.Status, a.Body)
	}
	return f.mustCall(http.MethodPost, base+"/commit", nil, nil)
}

// PutBlob stores a large object as a client uploads one in parts of partSize: TWS1 in a blob of its own, created,
// each part signed and sent, then completed with its wrapped key, through the fakes' routes.
func (f *Fakes) PutBlob(bucket, key string, data []byte, partSize int64, contentType string, metadata map[string]string) error {
	_, sum := hexSum(data)
	create := map[string]any{"direct": true, "idempotency_key": f.nextIdem("u"), "key": key, "size": len(data),
		"part_size": partSize, "sha256": sum, "content_type": contentType}
	if metadata != nil {
		create["metadata"] = metadata
	}
	var up struct {
		ID string `json:"upload_id"`
	}
	if err := f.mustCall(http.MethodPost, "/buckets/"+bucket+"/uploads", create, &up); err != nil {
		return err
	}
	dek, err := tws1.NewDataKey()
	if err != nil {
		return err
	}
	stream, err := tws1.NewStream(dek, int64(len(data)), partSize)
	if err != nil {
		return err
	}
	parts, err := tws1.Parts(int64(len(data)), partSize)
	if err != nil {
		return err
	}
	base := "/buckets/" + bucket + "/uploads/" + up.ID
	for _, p := range parts {
		sealed, err := stream.SealPart(p.Number, data[p.PlainFirst:p.PlainFirst+p.PlainSize])
		if err != nil {
			return err
		}
		m, s := hexSum(sealed)
		var signed Signed
		if err := f.mustCall(http.MethodPost, fmt.Sprintf("%s/parts/%d/sign", base, p.Number), map[string]any{"md5": m, "sha256": s}, &signed); err != nil {
			return err
		}
		a, err := f.Send(signed, sealed)
		if err != nil {
			return err
		}
		if a.Status != http.StatusOK {
			return fmt.Errorf("PUT of part %d: %d %s", p.Number, a.Status, a.Body)
		}
	}
	var acct, oid [16]byte
	var pub []byte
	var version int
	f.API.Do(func() {
		acct, _ = IDBytes(f.Direct.AccountID)
		pub = f.Direct.PublicKey
		version = f.Direct.KeyVersion
	})
	oid, _ = IDBytes(up.ID)
	wrapped, err := tws1.WrapKey(pub, acct, oid, dek)
	if err != nil {
		return err
	}
	return f.mustCall(http.MethodPost, base+"/complete", map[string]any{"wrapped_key": b64(wrapped), "key_version": version}, nil)
}

// Damage flips a stored byte of bucket/key for good: every read of it fails its check.
func (f *Fakes) Damage(bucket, key string, at int64) {
	o := f.API.Object(bucket, key)
	f.Network.Do(func() {
		b := bytes.Clone(f.Network.Blobs[o.Physical])
		b[at] ^= 0xff
		f.Network.Blobs[o.Physical] = b
	})
}
