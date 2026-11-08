// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package fakes

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// The fake control plane and storage network keep teepin-core's rules (M1, M2 and section 22.2), driven over plain
// HTTP the way a client would, so the SDK's direct tests can trust them. The first tests are test_fake_direct.py's,
// ported by way of the Node SDK's fake-direct.test.mjs; the rest check what the Python file leaves to the SDK's own
// tests: each storage-network rule and fault, the fake API's faults, a stream's upload (E2 b), and the commit and
// complete checks.

const seg = tws1.SegmentSize

func random(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func md5b64(b []byte) string {
	s := md5.Sum(b)
	return base64.StdEncoding.EncodeToString(s[:])
}

func unb64(t *testing.T, s any) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s.(string))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func id(t *testing.T, s any) [16]byte {
	t.Helper()
	b, ok := IDBytes(s.(string))
	if !ok {
		t.Fatalf("%v is not a UUID", s)
	}
	return b
}

var xmlCode = regexp.MustCompile(`<Code>([^<]*)</Code>`)
var xmlMessage = regexp.MustCompile(`<Message>([^<]*)</Message>`)

// xml is an S3-style error answer's code and message.
func xml(a *Answer) (string, string) {
	c, m := xmlCode.FindSubmatch(a.Body), xmlMessage.FindSubmatch(a.Body)
	var code, msg string
	if c != nil {
		code = string(c[1])
	}
	if m != nil {
		msg = string(m[1])
	}
	return code, msg
}

func call(t *testing.T, f *Fakes, method, path string, body any) *Answer {
	t.Helper()
	a, err := f.Call(method, path, body, nil)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return a
}

func send(t *testing.T, f *Fakes, s Signed, body []byte) *Answer {
	t.Helper()
	a, err := f.Send(s, body)
	if err != nil {
		t.Fatalf("%s %s: %v", s.Method, s.URL, err)
	}
	return a
}

func signedOf(t *testing.T, a *Answer) Signed {
	t.Helper()
	var s Signed
	if err := json.Unmarshal(a.Body, &s); err != nil || s.URL == "" {
		t.Fatalf("not a signed request: %d %s", a.Status, a.Body)
	}
	return s
}

// fetchRange is a signed read's bytes; it must answer 206.
func fetchRange(t *testing.T, f *Fakes, req map[string]any) []byte {
	t.Helper()
	headers := map[string]string{}
	for k, v := range req["headers"].(map[string]any) {
		headers[k] = v.(string)
	}
	a := send(t, f, Signed{Method: http.MethodGet, URL: req["url"].(string), Headers: headers}, nil)
	if a.Status != http.StatusPartialContent {
		t.Fatalf("a signed read answered %d %s", a.Status, a.Body)
	}
	return a.Body
}

type files []struct {
	key  string
	data []byte
}

func reserve(t *testing.T, f *Fakes, fs files, idem string) *Answer {
	t.Helper()
	var objects []any
	for _, x := range fs {
		_, sum := hexSum(x.data)
		objects = append(objects, map[string]any{"key": x.key, "size": len(x.data), "sha256": sum})
	}
	return call(t, f, http.MethodPost, "/buckets/photos/packs", map[string]any{"idempotency_key": idem, "objects": objects})
}

// build is a pack built as a client builds it, and its sign body; accountID "" is the account the answer names.
func build(t *testing.T, res map[string]any, fs files, accountID string) ([]byte, map[string]any) {
	t.Helper()
	ak := res["account_key"].(map[string]any)
	if accountID == "" {
		accountID = ak["account_id"].(string)
	}
	account := id(t, accountID)
	data := map[string][]byte{}
	for _, x := range fs {
		data[x.key] = x.data
	}
	var pack []byte
	var objects []any
	for _, ov := range res["objects"].([]any) {
		o := ov.(map[string]any)
		dek, _ := tws1.NewDataKey()
		stored, err := tws1.Encrypt(dek, data[o["key"].(string)])
		if err != nil {
			t.Fatal(err)
		}
		oid := id(t, o["object_id"])
		var offset int64
		pack, offset = tws1.AppendNeedle(pack, oid, stored)
		wrapped, err := tws1.WrapKey(unb64(t, ak["public_key"]), account, oid, dek)
		if err != nil {
			t.Fatal(err)
		}
		objects = append(objects, map[string]any{"object_id": o["object_id"], "offset": offset, "stored_size": len(stored),
			"wrapped_key": b64(wrapped), "key_version": ak["version"]})
	}
	m, s := hexSum(pack)
	return pack, map[string]any{"size": len(pack), "md5": m, "sha256": s, "objects": objects}
}

func signPut(f *Fakes, key string, data []byte, extra, query map[string]string) Signed {
	h := map[string]string{"content-length": fmt.Sprint(len(data)), "content-md5": md5b64(data)}
	for k, v := range extra {
		h[k] = v
	}
	return f.Network.Sign(http.MethodPut, key, h, query, 15*time.Minute)
}

func signGet(f *Fakes, key string, first, last int) Signed {
	return f.Network.Sign(http.MethodGet, key, map[string]string{"range": fmt.Sprintf("bytes=%d-%d", first, last)}, nil, 15*time.Minute)
}

func readBack(t *testing.T, f *Fakes, key string) []byte {
	t.Helper()
	b, err := f.Direct.ReadBack("photos", key)
	if err != nil {
		t.Fatalf("read back %s: %v", key, err)
	}
	return b
}

func TestFakes_APackReservedBuiltSignedSentCommittedAndRead(t *testing.T) {
	f := Start(t)
	fs := files{{"a.txt", []byte("hello")}, {"b.bin", random(70_000)}}
	r := reserve(t, f, fs, "idem-1")
	if r.Status != http.StatusCreated {
		t.Fatalf("reserve: %d %s", r.Status, r.Body)
	}
	res := r.JSON()
	if res["account_key"].(map[string]any)["account_id"] != f.Direct.AccountID {
		t.Fatal("the reservation names another account")
	}
	if again := reserve(t, f, fs, "idem-1").JSON(); again["pack"] != res["pack"] {
		t.Fatal("a repeat reserve made another pack")
	}
	if c := reserve(t, f, files{{"a.txt", []byte("x")}}, "idem-1").Code(); c != "idempotency_mismatch" {
		t.Fatalf("another request under the same key: %s", c)
	}
	pack, body := build(t, res, fs, "")
	base := "/buckets/photos/packs/" + res["pack"].(string)
	signed := signedOf(t, call(t, f, http.MethodPost, base+"/sign", body))
	if s := call(t, f, http.MethodPost, base+"/commit", nil).Status; s != http.StatusAccepted {
		t.Fatalf("commit before the bytes landed: %d", s)
	}
	if s := send(t, f, signed, pack).Status; s != http.StatusOK {
		t.Fatalf("PUT: %d", s)
	}
	if s := send(t, f, signed, pack).Status; s != http.StatusPreconditionFailed {
		t.Fatalf("a replay overwrote the pack: %d", s)
	}
	if s := call(t, f, http.MethodPost, base+"/sign", body).Status; s != http.StatusOK {
		t.Fatalf("the same manifest signed again: %d", s)
	}
	changed := map[string]any{}
	for k, v := range body {
		changed[k] = v
	}
	changed["md5"] = strings.Repeat("0", 32)
	if c := call(t, f, http.MethodPost, base+"/sign", changed).Code(); c != "manifest_mismatch" {
		t.Fatalf("a changed manifest: %s", c)
	}
	c := call(t, f, http.MethodPost, base+"/commit", nil)
	result := c.JSON()
	if c.Status != http.StatusOK || result["status"] != "committed" || result["committed_at"] == "" || result["backend"] != f.Direct.Backend {
		t.Fatalf("commit: %d %s", c.Status, c.Body)
	}
	objs := result["objects"].([]any)
	for _, o := range objs {
		if o.(map[string]any)["result"] != "saved" {
			t.Fatalf("commit: %s", c.Body)
		}
	}
	if again := call(t, f, http.MethodPost, base+"/commit", nil); !bytes.Equal(again.Body, c.Body) {
		t.Fatalf("a repeat commit answered otherwise: %s", again.Body)
	}
	for _, x := range fs {
		if !bytes.Equal(readBack(t, f, x.key), x.data) {
			t.Fatalf("%s read back wrong", x.key)
		}
		s := call(t, f, http.MethodPost, "/buckets/photos/object/read", map[string]any{"key": x.key}).JSON()
		reqs := s["requests"].([]any)
		if s["format"] != "tws1" || s["source"] != "storage" || len(reqs) != 1 {
			t.Fatalf("session: %v", s)
		}
		req := reqs[0].(map[string]any)
		raw := fetchRange(t, f, req)
		prefix := int64(req["prefix"].(float64))
		stored, err := tws1.ReadNeedle(raw, prefix, id(t, s["version"]), int64(len(raw))-prefix)
		if err != nil {
			t.Fatal(err)
		}
		plain, err := tws1.Decrypt(unb64(t, s["key_material"]), stored)
		if err != nil || !bytes.Equal(plain, x.data) {
			t.Fatalf("%s decrypted wrong: %v", x.key, err)
		}
		if s["uploaded_at"] != result["committed_at"] {
			t.Fatalf("uploaded_at %v, committed_at %v", s["uploaded_at"], result["committed_at"])
		}
	}
	meta := call(t, f, http.MethodGet, "/buckets/photos/object?key=a.txt", nil).JSON()
	if meta["id"] != objs[0].(map[string]any)["object_id"] || meta["size_bytes"] != float64(5) {
		t.Fatalf("details: %v", meta)
	}
	if a := call(t, f, http.MethodGet, "/buckets/photos/object/content?key=a.txt", nil); a.Status != http.StatusConflict || a.Code() != "client_encrypted" {
		t.Fatalf("content of a client-encrypted object: %d %s", a.Status, a.Body)
	}
}

func TestFakes_BadManifestsAndWrapsRefusedAtSign(t *testing.T) {
	f := Start(t)
	fs := files{{"a", []byte("one")}, {"b", []byte("two")}}
	res := reserve(t, f, fs, "idem-1").JSON()
	_, body := build(t, res, fs, "")
	base := "/buckets/photos/packs/" + res["pack"].(string)
	objects := body["objects"].([]any)
	first := map[string]any{}
	for k, v := range objects[0].(map[string]any) {
		first[k] = v
	}
	first["offset"] = 65
	shifted := map[string]any{"size": body["size"], "md5": body["md5"], "sha256": body["sha256"], "objects": append([]any{first}, objects[1:]...)}
	if c := call(t, f, http.MethodPost, base+"/sign", shifted).Code(); c != "invalid_request" {
		t.Fatalf("a needle out of place: %s", c)
	}
	_, other := build(t, res, fs, "00000000-0000-0000-0000-000000000001")
	if c := call(t, f, http.MethodPost, base+"/sign", other).Code(); c != "bad_wrapped_key" {
		t.Fatalf("wraps to another account: %s", c)
	}
	if c := call(t, f, http.MethodPost, base+"/commit", nil).Code(); c != "pack_not_signed" {
		t.Fatalf("commit of an unsigned pack: %s", c)
	}
}

func TestFakes_APackWithOtherBytesIsRefusedByStorage(t *testing.T) {
	f := Start(t)
	fs := files{{"a", []byte("one")}}
	res := reserve(t, f, fs, "idem-1").JSON()
	pack, body := build(t, res, fs, "")
	signed := signedOf(t, call(t, f, http.MethodPost, "/buckets/photos/packs/"+res["pack"].(string)+"/sign", body))
	damaged := bytes.Clone(pack)
	damaged[len(damaged)-1] ^= 1
	if s := send(t, f, signed, damaged).Status; s != http.StatusBadRequest {
		t.Fatalf("damaged bytes: %d", s)
	}
	if s := send(t, f, signed, pack).Status; s != http.StatusOK {
		t.Fatalf("the right bytes: %d", s)
	}
}

func TestFakes_PreconditionsFailPerObject(t *testing.T) {
	f := Start(t)
	f.API.PutObject("photos", "exists", []byte("old"), "")
	_, newSum := hexSum([]byte("new"))
	_, abcSum := hexSum([]byte("abc"))
	objects := []any{
		map[string]any{"key": "exists", "size": 3, "sha256": newSum, "if_none_match": true},
		map[string]any{"key": "fresh", "size": 3, "sha256": abcSum},
	}
	res := call(t, f, http.MethodPost, "/buckets/photos/packs", map[string]any{"idempotency_key": "p", "objects": objects}).JSON()
	pack, body := build(t, res, files{{"exists", []byte("new")}, {"fresh", []byte("abc")}}, "")
	base := "/buckets/photos/packs/" + res["pack"].(string)
	send(t, f, signedOf(t, call(t, f, http.MethodPost, base+"/sign", body)), pack)
	out := call(t, f, http.MethodPost, base+"/commit", nil).JSON()
	var got []string
	for _, o := range out["objects"].([]any) {
		m := o.(map[string]any)
		got = append(got, fmt.Sprint(m["key"], " ", m["result"], " ", m["error"]))
	}
	if want := []string{"exists failed precondition_failed", "fresh saved <nil>"}; !slices.Equal(got, want) {
		t.Fatalf("results %q, want %q", got, want)
	}
}

func TestFakes_AnExpiredReservationIsAbandoned(t *testing.T) {
	f := Start(t)
	fs := files{{"a", []byte("one")}}
	res := reserve(t, f, fs, "idem-1").JSON()
	_, body := build(t, res, fs, "")
	f.API.Do(func() { f.Direct.Skew = 2 * time.Hour })
	if c := call(t, f, http.MethodPost, "/buckets/photos/packs/"+res["pack"].(string)+"/sign", body).Code(); c != "reservation_abandoned" {
		t.Fatalf("sign of an expired reservation: %s", c)
	}
	if c := reserve(t, f, fs, "idem-1").Code(); c != "reservation_abandoned" {
		t.Fatalf("reserve again: %s", c)
	}
}

func TestFakes_ThePendingCap(t *testing.T) {
	f := Start(t)
	f.API.Do(func() { f.Direct.MaxReservedPacks = 2 })
	for i := range 2 {
		if s := reserve(t, f, files{{"a", []byte("x")}}, fmt.Sprintf("i%d", i)).Status; s != http.StatusCreated {
			t.Fatalf("reserve %d: %d", i, s)
		}
	}
	r := reserve(t, f, files{{"a", []byte("x")}}, "i3")
	if r.Status != http.StatusTooManyRequests || r.Code() != "too_many_pending_uploads" || r.Header.Get("Retry-After") == "" {
		t.Fatalf("over the cap: %d %s", r.Status, r.Body)
	}
}

func TestFakes_ALargeUploadInPartsReadBackInWindows(t *testing.T) {
	f := Start(t)
	f.API.Do(func() { f.Direct.MinPartBytes = 1 }) // small parts, to keep the test fast
	psize := int64(2 * seg)
	data := random(5*seg + 999)
	_, sum := hexSum(data)
	spec := map[string]any{"direct": true, "idempotency_key": "u1", "key": "big.bin", "size": len(data), "part_size": psize, "sha256": sum}
	r := call(t, f, http.MethodPost, "/buckets/photos/uploads", spec)
	up := r.JSON()
	if r.Status != http.StatusCreated || up["direct"] != true || up["part_count"] != float64(3) ||
		up["account_key"].(map[string]any)["account_id"] != f.Direct.AccountID {
		t.Fatalf("create: %d %s", r.Status, r.Body)
	}
	if again := call(t, f, http.MethodPost, "/buckets/photos/uploads", spec).JSON(); again["upload_id"] != up["upload_id"] {
		t.Fatal("a repeat create made another upload")
	}
	dek, _ := tws1.NewDataKey()
	stream, err := tws1.NewStream(dek, int64(len(data)), psize)
	if err != nil {
		t.Fatal(err)
	}
	base := "/buckets/photos/uploads/" + up["upload_id"].(string)
	parts, _ := tws1.Parts(int64(len(data)), psize)
	for _, p := range parts {
		sealed, _ := stream.SealPart(p.Number, data[p.PlainFirst:p.PlainFirst+p.PlainSize])
		m, s := hexSum(sealed)
		signed := signedOf(t, call(t, f, http.MethodPost, fmt.Sprintf("%s/parts/%d/sign", base, p.Number), map[string]any{"md5": m, "sha256": s}))
		if st := send(t, f, signed, sealed).Status; st != http.StatusOK {
			t.Fatalf("part %d: %d", p.Number, st)
		}
	}
	other := map[string]any{"md5": strings.Repeat("0", 32), "sha256": strings.Repeat("0", 64)}
	if c := call(t, f, http.MethodPost, base+"/parts/1/sign", other).Code(); c != "part_mismatch" {
		t.Fatalf("a part signed again with other bytes: %s", c)
	}
	for _, p := range call(t, f, http.MethodGet, base+"/parts", nil).JSON()["parts"].([]any) {
		if p.(map[string]any)["landed"] != true {
			t.Fatalf("a part not landed: %v", p)
		}
	}
	wrapped, _ := tws1.WrapKey(f.Direct.PublicKey, id(t, f.Direct.AccountID), id(t, up["upload_id"]), dek)
	complete := map[string]any{"wrapped_key": b64(wrapped), "key_version": 1}
	c := call(t, f, http.MethodPost, base+"/complete", complete)
	if c.Status != http.StatusOK {
		t.Fatalf("complete: %d %s", c.Status, c.Body)
	}
	if again := call(t, f, http.MethodPost, base+"/complete", complete); !bytes.Equal(again.Body, c.Body) {
		t.Fatalf("a repeat complete answered otherwise: %s", again.Body)
	}
	if !bytes.Equal(readBack(t, f, "big.bin"), data) {
		t.Fatal("read back wrong")
	}
	// Reads in windows: the header comes first past the start.
	f.API.Do(func() { f.Direct.ReadWindow = 3 * tws1.StoredSegmentSize })
	s0 := call(t, f, http.MethodPost, "/buckets/photos/object/read", map[string]any{"key": "big.bin"}).JSON()
	if reqs := s0["requests"].([]any); len(reqs) != 1 || reqs[0].(map[string]any)["stored_first"] != float64(0) {
		t.Fatalf("the first window: %v", reqs)
	}
	rng, _ := tws1.Locate(int64(len(data)), 3*seg+5, 100)
	var metered int64
	f.API.Do(func() { metered = f.Direct.Metered })
	s1 := call(t, f, http.MethodPost, "/buckets/photos/object/read", map[string]any{"key": "big.bin",
		"stored_offset": rng.StoredOffset, "stored_length": rng.StoredLength}).JSON()
	f.API.Do(func() { metered = f.Direct.Metered - metered })
	if metered != rng.StoredLength+tws1.HeaderSize {
		t.Fatalf("metered %d, want %d", metered, rng.StoredLength+tws1.HeaderSize)
	}
	reqs := s1["requests"].([]any)
	head, window := reqs[0].(map[string]any), reqs[1].(map[string]any)
	if head["stored_first"] != float64(0) || head["stored_last"] != float64(tws1.HeaderSize-1) {
		t.Fatalf("the header request: %v", head)
	}
	got, err := tws1.DecryptRange(unb64(t, s1["key_material"]), fetchRange(t, f, head), int64(len(data)), rng, fetchRange(t, f, window))
	start := rng.FirstSegment*seg + rng.Skip
	if err != nil || !bytes.Equal(got, data[start:start+rng.Length]) {
		t.Fatalf("the range decrypted wrong: %v", err)
	}
}

func TestFakes_UploadRefusals(t *testing.T) {
	f := Start(t)
	f.API.Do(func() { f.Direct.MinPartBytes = 1 })
	data := random(3 * seg)
	_, sum := hexSum(data)
	spec := map[string]any{"direct": true, "idempotency_key": "u2", "key": "x.bin", "size": len(data), "part_size": seg, "sha256": sum}
	up := call(t, f, http.MethodPost, "/buckets/photos/uploads", spec).JSON()
	base := "/buckets/photos/uploads/" + up["upload_id"].(string)
	a, _ := f.Call(http.MethodPut, base+"/parts/1", nil, nil)
	if a.Code() != "upload_is_direct" {
		t.Fatalf("a part through the API: %s", a.Body)
	}
	bad := map[string]any{"wrapped_key": b64(random(80)), "key_version": 1}
	if c := call(t, f, http.MethodPost, base+"/complete", bad).Code(); c != "bad_wrapped_key" {
		t.Fatalf("a wrap that doesn't open: %s", c)
	}
	dek := random(32)
	w, _ := tws1.WrapKey(f.Direct.PublicKey, id(t, f.Direct.AccountID), id(t, up["upload_id"]), dek)
	good := map[string]any{"wrapped_key": b64(w), "key_version": 1}
	if c := call(t, f, http.MethodPost, base+"/complete", good).Code(); c != "invalid_part" {
		t.Fatalf("complete with nothing signed: %s", c)
	}
	if s := call(t, f, http.MethodDelete, base, nil).Status; s != http.StatusNoContent {
		t.Fatalf("abort: %d", s)
	}
	if s := call(t, f, http.MethodDelete, base, nil).Status; s != http.StatusNotFound {
		t.Fatalf("abort again: %d", s)
	}
	if c := call(t, f, http.MethodPost, base+"/complete", good).Code(); c != "reservation_abandoned" {
		t.Fatalf("complete after abort: %s", c)
	}
	spec["part_size"] = 1000
	if c := call(t, f, http.MethodPost, "/buckets/photos/uploads", spec).Code(); c != "invalid_request" {
		t.Fatalf("a part size that isn't whole segments: %s", c)
	}
}

func TestFakes_ReadSessionsForEveryFormat(t *testing.T) {
	f := Start(t)
	plain := f.Direct.PutPlainBlob("photos", "legacy.txt", []byte("plain bytes"), 0)
	server := f.Direct.PutServerObject("photos", "staged-then-packed.txt", []byte("server format"))
	f.API.PutObject("photos", "staged.txt", []byte("still on the API"), "")
	keys := []string{"legacy.txt", "staged-then-packed.txt", "staged.txt", "nope"}
	out := call(t, f, http.MethodPost, "/buckets/photos/objects/read", map[string]any{"keys": keys}).JSON()["objects"].([]any)
	by := map[string]map[string]any{}
	for _, o := range out {
		m := o.(map[string]any)
		by[m["key"].(string)] = m
	}
	legacy := by["legacy.txt"]
	if legacy["format"] != "plain" || legacy["key_material"] != nil {
		t.Fatalf("legacy: %v", legacy)
	}
	if got := fetchRange(t, f, legacy["requests"].([]any)[0].(map[string]any)); string(got) != "plain bytes" {
		t.Fatalf("legacy bytes %q", got)
	}
	if plain.ETag() != legacy["etag"] {
		t.Fatal("legacy etag")
	}
	s := by["staged-then-packed.txt"]
	if s["format"] != "teepin-v1" || s["associated_data"] == nil {
		t.Fatalf("server format: %v", s)
	}
	req := s["requests"].([]any)[0].(map[string]any)
	raw := fetchRange(t, f, req)
	prefix := int64(req["prefix"].(float64))
	stored, err := tws1.ReadNeedle(raw, prefix, id(t, s["version"]), int64(len(raw))-prefix)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := OpenServerV1(unb64(t, s["key_material"]), stored, unb64(t, s["associated_data"])); err != nil || string(got) != "server format" {
		t.Fatalf("server format opened %q, %v", got, err)
	}
	if server.Size != 13 {
		t.Fatalf("server object size %d", server.Size)
	}
	if st := by["staged.txt"]; st["source"] != "api" || len(st["requests"].([]any)) != 0 {
		t.Fatalf("staged: %v", st)
	}
	if by["nope"]["error"].(map[string]any)["code"] != "object_not_found" {
		t.Fatalf("missing: %v", by["nope"])
	}
	noKeys := call(t, f, http.MethodPost, "/buckets/photos/object/read", map[string]any{"key": "staged-then-packed.txt", "include_key": false}).JSON()
	if noKeys["key_material"] != nil {
		t.Fatal("include_key false sent the key")
	}
	if !bytes.Equal(readBack(t, f, "staged-then-packed.txt"), []byte("server format")) || !bytes.Equal(readBack(t, f, "legacy.txt"), []byte("plain bytes")) {
		t.Fatal("read back wrong")
	}
}

func TestFakes_TheAPIKeyNeverReachesStorage(t *testing.T) {
	f := Start(t)
	fs := files{{"a", []byte("one")}}
	res := reserve(t, f, fs, "idem-1").JSON()
	pack, body := build(t, res, fs, "")
	send(t, f, signedOf(t, call(t, f, http.MethodPost, "/buckets/photos/packs/"+res["pack"].(string)+"/sign", body)), pack)
	reqs := f.Network.RequestsOf("")
	if len(reqs) == 0 {
		t.Fatal("nothing reached storage")
	}
	for _, r := range reqs {
		if b, _ := json.Marshal(r.Header); bytes.Contains(b, []byte(APIKey)) {
			t.Fatalf("the API key reached storage: %v", r.Header)
		}
	}
}

func TestFakes_ACommitStorageContradicts_AndAReservationOnceCommitted(t *testing.T) {
	f := Start(t)
	one := files{{"a", []byte("one")}}
	res := reserve(t, f, one, "short").JSON()
	_, body := build(t, res, one, "")
	pid := res["pack"].(string)
	if s := call(t, f, http.MethodPost, "/buckets/photos/packs/"+pid+"/sign", body).Status; s != http.StatusOK {
		t.Fatalf("sign: %d", s)
	}
	var key string
	f.API.Do(func() { key = f.Direct.Packs[pid].Key })
	size, _ := integer(json.Number(fmt.Sprint(body["size"])))
	f.Network.SetBlob(key, make([]byte, size+1)) // another size: the pack is abandoned
	if c := call(t, f, http.MethodPost, "/buckets/photos/packs/"+pid+"/commit", nil).Code(); c != "pack_mismatch" {
		t.Fatalf("another size: %s", c)
	}
	f.API.Do(func() {
		if f.Direct.Packs[pid].Status != "abandoned" {
			t.Errorf("status %s", f.Direct.Packs[pid].Status)
		}
	})

	two := files{{"b", []byte("two")}}
	res2 := reserve(t, f, two, "other").JSON()
	pack2, body2 := build(t, res2, two, "")
	pid2 := res2["pack"].(string)
	if s := call(t, f, http.MethodPost, "/buckets/photos/packs/"+pid2+"/sign", body2).Status; s != http.StatusOK {
		t.Fatalf("sign: %d", s)
	}
	f.API.Do(func() { key = f.Direct.Packs[pid2].Key })
	f.Network.SetBlob(key, make([]byte, len(pack2))) // the size, other bytes
	if c := call(t, f, http.MethodPost, "/buckets/photos/packs/"+pid2+"/commit", nil); c.Status != http.StatusBadGateway || c.Code() != "pack_unverified" {
		t.Fatalf("other bytes: %d %s", c.Status, c.Body)
	}
	f.Network.SetBlob(key, pack2)
	if s := call(t, f, http.MethodPost, "/buckets/photos/packs/"+pid2+"/commit", nil).Status; s != http.StatusOK {
		t.Fatalf("commit: %d", s)
	}
	again := reserve(t, f, two, "other")
	if again.Status != http.StatusConflict || again.Code() != "pack_committed" || again.JSON()["pack"] != pid2 {
		t.Fatalf("reserve once committed: %d %s", again.Status, again.Body)
	}
	f.API.Do(func() {
		if n := len(f.Direct.OpenPacks()); n != 0 {
			t.Errorf("%d packs left open", n)
		}
	})
}

func TestFakes_AStreamsUploadKeepsTheFiveRules(t *testing.T) {
	f := Start(t)
	f.API.Do(func() { f.Direct.MinPartBytes = 1 })
	psize := int64(seg)
	data := random(2*seg + 500) // three parts, the last of 500 bytes
	_, sum := hexSum(data)
	spec := map[string]any{"direct": true, "idempotency_key": "s1", "key": "stream.bin", "part_size": psize}
	created := call(t, f, http.MethodPost, "/buckets/photos/uploads", spec)
	up := created.JSON()
	if created.Status != http.StatusCreated || up["size"] != nil || up["part_count"] != nil || up["sha256"] != nil {
		t.Fatalf("create: %d %s", created.Status, created.Body)
	}
	halfOpen := map[string]any{"direct": true, "idempotency_key": "s2", "key": "stream.bin", "part_size": psize, "sha256": sum}
	if c := call(t, f, http.MethodPost, "/buckets/photos/uploads", halfOpen).Code(); c != "invalid_request" {
		t.Fatalf("a SHA-256 without the size: %s", c)
	}
	// Sealed as one stream of the whole size: the parts' bytes are what a stream's sealer makes.
	dek, _ := tws1.NewDataKey()
	stream, _ := tws1.NewStream(dek, int64(len(data)), psize)
	part := func(n int) []byte {
		p, _ := tws1.PartOf(int64(len(data)), psize, n)
		b, _ := stream.SealPart(n, data[p.PlainFirst:p.PlainFirst+p.PlainSize])
		return b
	}
	p1, p2, p3 := part(1), part(2), part(3)
	base := "/buckets/photos/uploads/" + up["upload_id"].(string)
	sign := func(n int, sealed []byte, extra map[string]any) *Answer {
		m, s := hexSum(sealed)
		body := map[string]any{"md5": m, "sha256": s}
		for k, v := range extra {
			body[k] = v
		}
		return call(t, f, http.MethodPost, fmt.Sprintf("%s/parts/%d/sign", base, n), body)
	}
	if s := send(t, f, signedOf(t, sign(2, p2, nil)), p2).Status; s != http.StatusOK {
		t.Fatalf("part 2: %d", s)
	}
	if c := sign(1, p1, map[string]any{"last": true, "plain_size": psize}).Code(); c != "invalid_part" {
		t.Fatalf("a last part below a signed one: %s", c)
	}
	if s := send(t, f, signedOf(t, sign(3, p3, map[string]any{"last": true, "plain_size": 500})), p3).Status; s != http.StatusOK {
		t.Fatalf("part 3: %d", s)
	}
	f.API.Do(func() {
		u := f.Direct.Uploads[up["upload_id"].(string)]
		if u.LastPart != 3 || u.LastSize != 500 {
			t.Errorf("last part %d of %d bytes", u.LastPart, u.LastSize)
		}
	})
	if c := sign(4, p3, nil).Code(); c != "invalid_part" {
		t.Fatalf("above the last part: %s", c)
	}
	if c := sign(3, p3, nil).Code(); c != "part_mismatch" {
		t.Fatalf("the last part signed as another: %s", c)
	}
	if c := sign(2, p2, map[string]any{"last": true, "plain_size": psize}).Code(); c != "part_mismatch" {
		t.Fatalf("a second last part: %s", c)
	}
	w, _ := tws1.WrapKey(f.Direct.PublicKey, id(t, f.Direct.AccountID), id(t, up["upload_id"]), dek)
	early := map[string]any{"wrapped_key": b64(w), "key_version": 1, "size": len(data), "sha256": sum}
	if c := call(t, f, http.MethodPost, base+"/complete", early).Code(); c != "invalid_part" {
		t.Fatalf("complete with 2 of 3 signed: %s", c)
	}
	if s := send(t, f, signedOf(t, sign(1, p1, nil)), p1).Status; s != http.StatusOK {
		t.Fatalf("part 1: %d", s)
	}
	var listed []string
	for _, p := range call(t, f, http.MethodGet, base+"/parts", nil).JSON()["parts"].([]any) {
		m := p.(map[string]any)
		listed = append(listed, fmt.Sprint(m["part_number"], " ", m["stored_size"], " ", m["landed"]))
	}
	want := []string{fmt.Sprint(1, " ", tws1.HeaderSize+seg+16, " true"), fmt.Sprint(2, " ", seg+16, " true"), fmt.Sprint(3, " ", 516, " true")}
	if !slices.Equal(listed, want) {
		t.Fatalf("parts %q, want %q", listed, want)
	}
	short := map[string]any{"wrapped_key": early["wrapped_key"], "key_version": 1, "size": len(data) - 1, "sha256": sum}
	if c := call(t, f, http.MethodPost, base+"/complete", short).Code(); c != "invalid_request" {
		t.Fatalf("a size the parts don't make: %s", c)
	}
	noSum := map[string]any{"wrapped_key": early["wrapped_key"], "key_version": 1, "size": len(data)}
	if c := call(t, f, http.MethodPost, base+"/complete", noSum).Code(); c != "invalid_request" {
		t.Fatalf("no SHA-256: %s", c)
	}
	c := call(t, f, http.MethodPost, base+"/complete", early)
	obj := c.JSON()
	if c.Status != http.StatusOK || obj["size_bytes"] != float64(len(data)) || obj["etag"] != sum || obj["part_count"] != float64(3) {
		t.Fatalf("complete: %d %s", c.Status, c.Body)
	}
	if c.Header.Get("ETag") != `"`+sum+`"` {
		t.Fatalf("ETag header %s", c.Header.Get("ETag"))
	}
	if !bytes.Equal(readBack(t, f, "stream.bin"), data) {
		t.Fatal("read back wrong")
	}
	repeat := call(t, f, http.MethodPost, "/buckets/photos/uploads", spec)
	if repeat.Status != http.StatusConflict || repeat.Code() != "upload_completed" || repeat.JSON()["upload_id"] != up["upload_id"] {
		t.Fatalf("create again: %d %s", repeat.Status, repeat.Body)
	}
}

func TestFakes_AnUploadsPreconditionAtComplete_AndExpiredUploadsAbandoned(t *testing.T) {
	f := Start(t)
	f.API.Do(func() { f.Direct.MinPartBytes = 1 })
	f.API.PutObject("photos", "taken", []byte("old"), "")
	data := random(1000)
	_, sum := hexSum(data)
	spec := map[string]any{"direct": true, "idempotency_key": "c1", "key": "taken", "size": len(data), "part_size": seg, "sha256": sum, "if_none_match": true}
	up := call(t, f, http.MethodPost, "/buckets/photos/uploads", spec).JSON()
	base := "/buckets/photos/uploads/" + up["upload_id"].(string)
	dek, _ := tws1.NewDataKey()
	stream, _ := tws1.NewStream(dek, int64(len(data)), seg)
	sealed, _ := stream.SealPart(1, data)
	m, s := hexSum(sealed)
	if st := send(t, f, signedOf(t, call(t, f, http.MethodPost, base+"/parts/1/sign", map[string]any{"md5": m, "sha256": s})), sealed).Status; st != http.StatusOK {
		t.Fatalf("part: %d", st)
	}
	w, _ := tws1.WrapKey(f.Direct.PublicKey, id(t, f.Direct.AccountID), id(t, up["upload_id"]), dek)
	complete := map[string]any{"wrapped_key": b64(w), "key_version": 1}
	for range 2 {
		if c := call(t, f, http.MethodPost, base+"/complete", complete); c.Status != http.StatusPreconditionFailed || c.Code() != "precondition_failed" {
			t.Fatalf("complete over an object: %d %s", c.Status, c.Body)
		}
	}
	var physical string
	f.API.Do(func() {
		u := f.Direct.Uploads[up["upload_id"].(string)]
		physical = u.Physical
		if u.Status != "aborted" {
			t.Errorf("status %s", u.Status)
		}
	})
	if f.Network.Blob(physical) != nil {
		t.Fatal("the assembled blob was kept")
	}
	if o := f.API.Object("photos", "taken"); string(o.Data) != "old" {
		t.Fatal("the old object was replaced")
	}

	f.API.Do(func() { f.Direct.MaxOpenUploads = 1 })
	spec2 := map[string]any{}
	for k, v := range spec {
		spec2[k] = v
	}
	spec2["idempotency_key"], spec2["key"] = "c2", "later"
	open := call(t, f, http.MethodPost, "/buckets/photos/uploads", spec2).JSON()
	spec3 := map[string]any{}
	for k, v := range spec2 {
		spec3[k] = v
	}
	spec3["idempotency_key"] = "c3"
	capped := call(t, f, http.MethodPost, "/buckets/photos/uploads", spec3)
	if capped.Status != http.StatusTooManyRequests || capped.Code() != "too_many_pending_uploads" || capped.Header.Get("Retry-After") == "" {
		t.Fatalf("over the cap: %d %s", capped.Status, capped.Body)
	}
	f.API.Do(func() { f.Direct.Skew = 8 * 24 * time.Hour })
	late := call(t, f, http.MethodPost, "/buckets/photos/uploads/"+open["upload_id"].(string)+"/parts/1/sign", map[string]any{"md5": m, "sha256": s})
	if late.Status != http.StatusNotFound || late.Code() != "upload_not_found" {
		t.Fatalf("sign after expiry: %d %s", late.Status, late.Body)
	}
	var mpu string
	f.API.Do(func() {
		gone := f.Direct.Uploads[open["upload_id"].(string)]
		mpu = gone.MPU
		if gone.Status != "abandoned" {
			t.Errorf("status %s", gone.Status)
		}
	})
	f.Network.Do(func() {
		if f.Network.Uploads[mpu] != nil {
			t.Error("the storage network's upload was kept")
		}
	})
	if c := call(t, f, http.MethodPost, "/buckets/photos/uploads", spec2).Code(); c != "reservation_abandoned" {
		t.Fatalf("create again: %s", c)
	}
}

func TestFakes_FaultsAndCannedAnswersPerRoute_AndRouteNames(t *testing.T) {
	names := [][3]string{
		{"POST", "/v1/storage/buckets/photos/packs", "POST packs"},
		{"POST", "/v1/storage/buckets/photos/packs/p1/sign", "POST pack sign"},
		{"POST", "/v1/storage/buckets/photos/packs/p1/commit", "POST pack commit"},
		{"POST", "/v1/storage/buckets/photos/uploads", "POST uploads"},
		{"POST", "/v1/storage/buckets/photos/uploads/u1/parts/3/sign", "POST part sign"},
		{"PUT", "/v1/storage/buckets/photos/uploads/u1/parts/3", "PUT part"},
		{"GET", "/v1/storage/buckets/photos/uploads/u1/parts", "GET parts"},
		{"POST", "/v1/storage/buckets/photos/uploads/u1/complete", "POST complete"},
		{"DELETE", "/v1/storage/buckets/photos/uploads/u1", "DELETE upload"},
		{"POST", "/v1/storage/buckets/photos/object/read", "POST object/read"},
		{"POST", "/v1/storage/buckets/photos/objects/read", "POST objects/read"},
		{"POST", "/v1/storage/buckets/photos/object/empty", "POST object/empty"},
		{"GET", "/v1/storage/buckets/photos/object/content", "GET object/content"},
		{"PUT", "/v1/storage/buckets/photos/object", "PUT object"},
		{"GET", "/v1/storage/keys/account", "GET keys"},
		{"POST", "/v1/storage/buckets", "POST buckets"},
		{"DELETE", "/v1/storage/buckets/photos", "DELETE bucket"},
	}
	for _, n := range names {
		if got := Route(n[0], n[1]); got != n[2] {
			t.Errorf("Route(%s, %s) = %q, want %q", n[0], n[1], got, n[2])
		}
	}

	f := Start(t)
	fs := files{{"a", []byte("one")}}
	f.API.Do(func() {
		f.API.Canned["POST packs"] = []Canned{{Status: 409, Body: map[string]any{"error": "reservation abandoned", "code": "reservation_abandoned"},
			Header: map[string]string{"Retry-After": "7"}}}
	})
	canned := reserve(t, f, fs, "idem-1")
	if canned.Status != http.StatusConflict || canned.Code() != "reservation_abandoned" || canned.Header.Get("Retry-After") != "7" {
		t.Fatalf("canned: %d %s", canned.Status, canned.Body)
	}
	f.API.Do(func() { f.API.Fail["POST packs"] = 1 })
	busy := reserve(t, f, fs, "idem-1")
	if busy.Status != http.StatusServiceUnavailable || busy.Code() != "storage_busy" || busy.Header.Get("Retry-After") != "1" {
		t.Fatalf("busy: %d %s", busy.Status, busy.Body)
	}
	packs := func() int {
		var n int
		f.API.Do(func() { n = len(f.Direct.Packs) })
		return n
	}
	if packs() != 0 {
		t.Fatal("a fault acted")
	}
	f.API.Do(func() { f.API.Lose["POST packs"] = 1 })
	if _, err := f.Call(http.MethodPost, "/buckets/photos/packs", map[string]any{"idempotency_key": "idem-1",
		"objects": []any{map[string]any{"key": "a", "size": 3, "sha256": sha256hex([]byte("one"))}}}, nil); err == nil {
		t.Fatal("a lost answer arrived")
	}
	if packs() != 1 {
		t.Fatal("a lost request didn't act")
	}
	kept := reserve(t, f, fs, "idem-1")
	var pid string
	f.API.Do(func() {
		for k := range f.Direct.Packs {
			pid = k
		}
	})
	if kept.Status != http.StatusCreated || kept.JSON()["pack"] != pid {
		t.Fatalf("the lost reservation: %d %s", kept.Status, kept.Body)
	}
	f.API.Do(func() { f.API.FailAfter["POST packs"] = 1 })
	after := reserve(t, f, files{{"b", []byte("two")}}, "idem-2")
	if after.Status != http.StatusBadGateway || after.Code() != "storage_backend_error" || packs() != 2 {
		t.Fatalf("fail after: %d %s", after.Status, after.Body)
	}
	if n := f.API.HitsOf("POST packs"); n != 5 {
		t.Fatalf("%d hits", n)
	}

	key := call(t, f, http.MethodGet, "/keys/account", nil).JSON()
	if key["account_id"] != f.Direct.AccountID || key["suite"] != tws1.Suite {
		t.Fatalf("account key: %v", key)
	}
	resp, err := http.Get(f.API.URL + "/v1/storage/keys/account")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no API key: %d", resp.StatusCode)
	}
	bare := StartAPI() // no direct routes: the API answers as with direct uploads off
	defer bare.Close()
	req, _ := http.NewRequest(http.MethodGet, bare.URL+"/v1/storage/keys/account", nil)
	req.Header.Set("Authorization", "Bearer "+APIKey)
	a, err := f.do(req)
	if err != nil || a.Status != http.StatusNotImplemented || a.Code() != "direct_upload_unsupported" {
		t.Fatalf("no direct routes: %v %v", a, err)
	}
}

func TestFakes_TheOldRoutesTheDirectPathStillUses(t *testing.T) {
	f := Start(t)
	req, _ := http.NewRequest(http.MethodPut, f.API.URL+"/v1/storage/buckets/photos/object?key=empty", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+APIKey)
	req.Header.Set("Content-Type", "text/plain")
	req.ContentLength = 0
	empty, err := f.do(req)
	if err != nil || empty.Status != http.StatusOK || empty.JSON()["size_bytes"] != float64(0) || empty.JSON()["etag"] != EmptySHA256 {
		t.Fatalf("an empty PUT: %v %v", empty, err)
	}
	f.API.PutObject("photos", "staged.txt", []byte("still on the API"), "")
	content := call(t, f, http.MethodGet, "/buckets/photos/object/content?key=staged.txt", nil)
	if content.Status != http.StatusOK || string(content.Body) != "still on the API" || f.API.HitsOf("GET object/content") != 1 {
		t.Fatalf("content: %d %s", content.Status, content.Body)
	}
	if err := f.PutPacked("photos", "sealed", []byte("client encrypted"), ""); err != nil {
		t.Fatal(err)
	}
	cp := call(t, f, http.MethodPost, "/buckets/photos/object/copy", map[string]any{"source_bucket": "photos", "source_key": "sealed", "key": "copy"})
	if cp.Status != http.StatusConflict || cp.Code() != "client_encrypted" {
		t.Fatalf("copy: %d %s", cp.Status, cp.Body)
	}
	head := call(t, f, http.MethodHead, "/buckets/photos/object/content?key=sealed", nil)
	if head.Status != http.StatusConflict {
		t.Fatalf("HEAD: %d", head.Status)
	}
	var listed []string
	for _, o := range call(t, f, http.MethodGet, "/buckets/photos/objects", nil).JSON()["objects"].([]any) {
		listed = append(listed, o.(map[string]any)["key"].(string))
	}
	if !slices.Equal(listed, []string{"empty", "sealed", "staged.txt"}) {
		t.Fatalf("listed %q", listed)
	}
}

func TestFakes_AnEmptyObjectThroughA2(t *testing.T) {
	f := Start(t)
	ok := call(t, f, http.MethodPost, "/buckets/photos/object/empty", map[string]any{"key": "e", "content_type": "text/plain",
		"metadata": map[string]any{"a": "b"}, "if_none_match": true})
	if ok.Status != http.StatusOK || ok.JSON()["etag"] != EmptySHA256 || ok.JSON()["size_bytes"] != float64(0) || ok.Header.Get("ETag") != `"`+EmptySHA256+`"` {
		t.Fatalf("empty: %d %s", ok.Status, ok.Body)
	}
	if c := call(t, f, http.MethodPost, "/buckets/photos/object/empty", map[string]any{"key": "e", "if_none_match": true}); c.Status != http.StatusPreconditionFailed {
		t.Fatalf("over an object with If-None-Match: %d", c.Status)
	}
	for _, body := range []any{
		map[string]any{"key": "x", "size": 0},
		map[string]any{"key": 1},
		map[string]any{"key": "x", "metadata": map[string]any{"a": 1}},
		map[string]any{"key": ""},
	} {
		if c := call(t, f, http.MethodPost, "/buckets/photos/object/empty", body); c.Status != http.StatusBadRequest {
			t.Fatalf("%v: %d", body, c.Status)
		}
	}
	req, _ := http.NewRequest(http.MethodPost, f.API.URL+"/v1/storage/buckets/photos/object/empty", strings.NewReader(`{"key":"x"} trailing`))
	req.Header.Set("Authorization", "Bearer "+APIKey)
	if a, err := f.do(req); err != nil || a.Status != http.StatusBadRequest {
		t.Fatalf("bytes after the object: %v %v", a, err)
	}
	s := call(t, f, http.MethodPost, "/buckets/photos/object/read", map[string]any{"key": "e"}).JSON()
	if s["stored_size"] != float64(0) || len(s["requests"].([]any)) != 0 || s["source"] != "storage" {
		t.Fatalf("an empty object's session: %v", s)
	}
}

func TestFakes_StorageChecksTheSignature(t *testing.T) {
	f := Start(t)
	data := []byte("some bytes")
	signed := signPut(f, "k/1", data, map[string]string{"if-none-match": "*"}, nil)
	refused := func(s Signed) string {
		t.Helper()
		a := send(t, f, s, data)
		code, msg := xml(a)
		if a.Status != http.StatusUnauthorized || code != "InvalidSignature" {
			t.Fatalf("%d %s", a.Status, a.Body)
		}
		return msg
	}
	with := func(change func(h map[string]string)) Signed {
		h := map[string]string{}
		for k, v := range signed.Headers {
			h[k] = v
		}
		change(h)
		return Signed{Method: signed.Method, URL: signed.URL, Headers: h}
	}
	if m := refused(with(func(h map[string]string) { h["if-none-match"] = "x" })); m != "signature does not match" {
		t.Fatalf("another value: %s", m)
	}
	if m := refused(with(func(h map[string]string) { delete(h, "if-none-match") })); m != "signed header if-none-match missing" {
		t.Fatalf("a missing header: %s", m)
	}
	if m := refused(Signed{Method: signed.Method, URL: strings.Replace(signed.URL, "k/1", "k/2", 1), Headers: signed.Headers}); m != "signature does not match" {
		t.Fatalf("another path: %s", m)
	}
	if m := refused(with(func(h map[string]string) { delete(h, "authorization") })); m != "no signature" {
		t.Fatalf("no signature: %s", m)
	}
	q := signPut(f, "o/x", data, nil, map[string]string{"partNumber": "1", "uploadId": "mpu-1"})
	if m := refused(Signed{Method: q.Method, URL: strings.Replace(q.URL, "partNumber=1", "partNumber=2", 1), Headers: q.Headers}); m != "signature does not match" {
		t.Fatalf("another query: %s", m)
	}
	g := signGet(f, "k/1", 0, 3)
	if a := send(t, f, Signed{Method: http.MethodHead, URL: g.URL, Headers: g.Headers}, nil); a.Status != http.StatusUnauthorized {
		t.Fatalf("signed for another method: %d", a.Status)
	}
	f.Network.Do(func() { f.Network.Skew = 901 * time.Second })
	if m := refused(signed); m != "expired" {
		t.Fatalf("expired: %s", m)
	}
	f.Network.Do(func() { f.Network.Skew = 0 })
	if s := send(t, f, signed, data).Status; s != http.StatusOK {
		t.Fatalf("PUT: %d", s)
	}
	if !bytes.Equal(f.Network.Blob("k/1"), data) {
		t.Fatal("not stored")
	}
	if size, etag, ok := f.Network.Head("k/1"); !ok || size != int64(len(data)) || etag != md5hex(data) {
		t.Fatalf("head %d %s %v", size, etag, ok)
	}
	if !regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$`).MatchString(signed.ExpiresAt) {
		t.Fatalf("expires_at %s", signed.ExpiresAt)
	}
}

func TestFakes_StorageEnforcesContentMD5AndIfNoneMatch_AndReadsOnlyTheSignedRange(t *testing.T) {
	f := Start(t)
	data := []byte("0123456789")
	signed := signPut(f, "k", data, map[string]string{"if-none-match": "*"}, nil)
	bad := send(t, f, signed, []byte("012345678X"))
	if code, _ := xml(bad); bad.Status != http.StatusBadRequest || code != "BadDigest" || f.Network.Blob("k") != nil {
		t.Fatalf("other bytes: %d %s", bad.Status, bad.Body)
	}
	ok := send(t, f, signed, data)
	if ok.Status != http.StatusOK || ok.Header.Get("ETag") != `"`+md5hex(data)+`"` {
		t.Fatalf("PUT: %d %v", ok.Status, ok.Header)
	}
	replay := send(t, f, signed, data)
	if code, _ := xml(replay); replay.Status != http.StatusPreconditionFailed || code != "PreconditionFailed" {
		t.Fatalf("replay: %d %s", replay.Status, replay.Body)
	}
	read := signGet(f, "k", 2, 5)
	r := send(t, f, read, nil)
	if r.Status != http.StatusPartialContent || string(r.Body) != "2345" || r.Header.Get("Content-Range") != "bytes 2-5/10" {
		t.Fatalf("a range: %d %q %v", r.Status, r.Body, r.Header)
	}
	h := map[string]string{}
	for k, v := range read.Headers {
		h[k] = v
	}
	h["range"] = "bytes=0-9"
	if a := send(t, f, Signed{Method: http.MethodGet, URL: read.URL, Headers: h}, nil); a.Status != http.StatusUnauthorized {
		t.Fatalf("a widened range: %d", a.Status)
	}
	if a := send(t, f, signGet(f, "k", 8, 100), nil); string(a.Body) != "89" {
		t.Fatalf("a range past the end: %q", a.Body)
	}
	if a := send(t, f, signGet(f, "k", 10, 20), nil); a.Status != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("a range after the end: %d", a.Status)
	}
	if a := send(t, f, signGet(f, "nothing", 0, 1), nil); a.Status != http.StatusNotFound {
		t.Fatalf("nothing there: %d", a.Status)
	} else if code, _ := xml(a); code != "NoSuchKey" {
		t.Fatalf("nothing there: %s", code)
	}
	if a := send(t, f, f.Network.Sign(http.MethodGet, "k", map[string]string{}, nil, 15*time.Minute), nil); a.Status != http.StatusUnauthorized {
		t.Fatalf("nothing signed: %d", a.Status)
	}
	if a := send(t, f, f.Network.Sign(http.MethodGet, "k", map[string]string{"x-amz-content-sha256": "x"}, nil, 15*time.Minute), nil); a.Status != http.StatusBadRequest {
		t.Fatalf("no range: %d", a.Status)
	} else if code, _ := xml(a); code != "InvalidRange" {
		t.Fatalf("no range: %s", code)
	}
}

func TestFakes_StorageLandsPartsUnderAnUploadTheControlPlaneMade(t *testing.T) {
	f := Start(t)
	uid := f.Network.CreateMultipart("o/big")
	parts := [][]byte{random(1000), random(700)}
	for i, p := range parts {
		r := send(t, f, signPut(f, "o/big", p, nil, map[string]string{"partNumber": fmt.Sprint(i + 1), "uploadId": uid}), p)
		if r.Status != http.StatusOK || r.Header.Get("ETag") != `"`+md5hex(p)+`"` {
			t.Fatalf("part %d: %d", i+1, r.Status)
		}
	}
	if got := f.Network.ListParts(uid); !slices.Equal(got, []PartInfo{{1, 1000, md5hex(parts[0])}, {2, 700, md5hex(parts[1])}}) {
		t.Fatalf("parts %v", got)
	}
	stray := send(t, f, signPut(f, "o/big", parts[0], nil, map[string]string{"partNumber": "1", "uploadId": "mpu-999"}), parts[0])
	if code, _ := xml(stray); stray.Status != http.StatusNotFound || code != "NoSuchUpload" {
		t.Fatalf("a stray part: %d %s", stray.Status, stray.Body)
	}
	if r := f.Network.CompleteMultipart(uid, []PartRef{{1, md5hex(parts[1])}}); r != "invalid_part" {
		t.Fatalf("complete with the wrong digest: %s", r)
	}
	if r := f.Network.CompleteMultipart(uid, []PartRef{{1, md5hex(parts[0])}, {2, md5hex(parts[1])}}); r != "ok" {
		t.Fatalf("complete: %s", r)
	}
	d0, _ := hexDecode(md5hex(parts[0]))
	d1, _ := hexDecode(md5hex(parts[1]))
	etag := md5hex(append(d0, d1...)) + "-2"
	if size, e, ok := f.Network.Head("o/big"); !ok || size != 1700 || e != etag {
		t.Fatalf("head %d %s", size, e)
	}
	if !bytes.Equal(f.Network.Blob("o/big"), append(bytes.Clone(parts[0]), parts[1]...)) {
		t.Fatal("assembled wrong")
	}
	if r := f.Network.CompleteMultipart(uid, nil); r != "no_such_upload" {
		t.Fatalf("complete again: %s", r)
	}
	if len(f.Network.ListParts(uid)) != 0 {
		t.Fatal("parts after complete")
	}
	again := f.Network.CreateMultipart("o/big")
	if r := f.Network.CompleteMultipart(again, nil); r != "exists" {
		t.Fatalf("complete onto an object: %s", r)
	}
	if !f.Network.AbortMultipart(again) || f.Network.AbortMultipart(again) {
		t.Fatal("abort")
	}
}

func TestFakes_StorageAnswersEachFaultOncePerRequestByKind(t *testing.T) {
	f := Start(t)
	data := []byte("payload")
	cases := []struct {
		knob   func() map[string]int
		status int
		code   string
	}{
		{func() map[string]int { return f.Network.Fail }, 503, "ServiceUnavailable"},
		{func() map[string]int { return f.Network.Reject }, 401, "InvalidSignature"},
		{func() map[string]int { return f.Network.BadDigest }, 400, "BadDigest"},
		{func() map[string]int { return f.Network.Gone }, 404, "NoSuchUpload"},
	}
	for i, c := range cases {
		key := fmt.Sprintf("f/%d", i)
		f.Network.Do(func() {
			c.knob()["PUT part"] = 1 // another kind: untouched by a PUT
			c.knob()["PUT"] = 1
		})
		r := send(t, f, signPut(f, key, data, nil, nil), data)
		if code, _ := xml(r); r.Status != c.status || code != c.code || f.Network.Blob(key) != nil {
			t.Fatalf("fault %d: %d %s", i, r.Status, r.Body)
		}
		if s := send(t, f, signPut(f, key, data, nil, nil), data).Status; s != http.StatusOK {
			t.Fatalf("fault %d, then: %d", i, s)
		}
		f.Network.Do(func() {
			if c.knob()["PUT"] != 0 || c.knob()["PUT part"] != 1 {
				t.Errorf("fault %d counted wrong", i)
			}
			c.knob()["PUT part"] = 0
		})
	}
	if n := f.Network.HitsOf("PUT"); n != 2*len(cases) {
		t.Fatalf("%d PUTs", n)
	}
}

func TestFakes_StorageDropsLosesMisreportsRotsCutsAndDelays(t *testing.T) {
	f := Start(t)
	data := random(1000)
	f.Network.Do(func() { f.Network.Drop["PUT"] = 1 })
	if _, err := f.Send(signPut(f, "d", data, nil, nil), data); err == nil {
		t.Fatal("a dropped PUT answered")
	}
	if f.Network.Blob("d") != nil {
		t.Fatal("a dropped PUT stored")
	}
	f.Network.Do(func() { f.Network.Lose["PUT"] = 1 })
	if _, err := f.Send(signPut(f, "l", data, nil, nil), data); err == nil {
		t.Fatal("a lost PUT answered")
	}
	if !bytes.Equal(f.Network.Blob("l"), data) {
		t.Fatal("a lost PUT didn't store")
	}
	f.Network.Do(func() { f.Network.Lose["PUT part"] = 1 })
	uid := f.Network.CreateMultipart("o/l")
	if _, err := f.Send(signPut(f, "o/l", data, nil, map[string]string{"partNumber": "1", "uploadId": uid}), data); err == nil {
		t.Fatal("a lost part answered")
	}
	if got := f.Network.ListParts(uid); len(got) != 1 || got[0].Size != 1000 {
		t.Fatalf("parts %v", got)
	}
	f.Network.Do(func() { f.Network.WrongETag["PUT"] = 1 })
	wrong := send(t, f, signPut(f, "w", data, nil, nil), data)
	if wrong.Status != http.StatusOK || wrong.Header.Get("ETag") == `"`+md5hex(data)+`"` || !bytes.Equal(f.Network.Blob("w"), data) {
		t.Fatalf("misreported: %d %v", wrong.Status, wrong.Header)
	}
	if e := send(t, f, signPut(f, "w", data, nil, nil), data).Header.Get("ETag"); e != `"`+md5hex(data)+`"` {
		t.Fatalf("then %s", e)
	}

	f.Network.Do(func() { f.Network.Rot = 1 })
	rotten := send(t, f, signGet(f, "l", 0, 999), nil).Body
	var flipped []int
	for i := range rotten {
		if rotten[i] != data[i] {
			flipped = append(flipped, i)
		}
	}
	if !slices.Equal(flipped, []int{500}) {
		t.Fatalf("flipped %v", flipped)
	}
	if !bytes.Equal(send(t, f, signGet(f, "l", 0, 999), nil).Body, data) {
		t.Fatal("rot twice")
	}
	f.Network.Do(func() { f.Network.Cut = 1 })
	if _, err := f.Send(signGet(f, "l", 0, 999), nil); err == nil {
		t.Fatal("a cut body read whole")
	}
	if !bytes.Equal(send(t, f, signGet(f, "l", 0, 999), nil).Body, data) {
		t.Fatal("cut twice")
	}
	f.Network.Do(func() { f.Network.Lose["GET"] = 1 })
	// On a fresh connection: Go's transport sends a GET again by itself when a reused connection drops unanswered.
	f.HTTP.CloseIdleConnections()
	if _, err := f.Send(signGet(f, "l", 0, 9), nil); err == nil {
		t.Fatal("a lost GET answered")
	}

	f.Network.Do(func() { f.Network.Delay = 50 * time.Millisecond })
	t0 := time.Now()
	if s := send(t, f, signGet(f, "l", 0, 9), nil).Status; s != http.StatusPartialContent {
		t.Fatalf("delayed: %d", s)
	}
	if time.Since(t0) < 40*time.Millisecond {
		t.Fatal("not delayed")
	}
	for _, r := range f.Network.RequestsOf("") {
		if !strings.HasPrefix(r.Path, "/b/") {
			t.Fatalf("a request to %s", r.Path)
		}
	}
	if f.Network.HitsOf("GET") < 6 {
		t.Fatalf("%d GETs", f.Network.HitsOf("GET"))
	}
}

func TestFakes_TheClientHelpersStoreAsAClientDoes(t *testing.T) {
	f := Start(t)
	small := random(5000)
	if err := f.PutPacked("photos", "small", small, "image/png"); err != nil {
		t.Fatal(err)
	}
	big := random(5*seg + 7)
	f.API.Do(func() { f.Direct.MinPartBytes = 1 })
	if err := f.PutBlob("photos", "big", big, 2*seg, "video/mp4", map[string]string{"camera": "a7"}); err != nil {
		t.Fatal(err)
	}
	if o := f.API.Object("photos", "small"); o.Location != "packed" || !o.ClientEncrypted || o.ContentType != "image/png" {
		t.Fatalf("small: %+v", o)
	}
	if o := f.API.Object("photos", "big"); o.Location != "blob" || !o.ClientEncrypted || o.Metadata["camera"] != "a7" {
		t.Fatalf("big: %+v", o)
	}
	if !bytes.Equal(readBack(t, f, "small"), small) || !bytes.Equal(readBack(t, f, "big"), big) {
		t.Fatal("read back wrong")
	}
}

func hexDecode(s string) ([]byte, error) {
	b := make([]byte, len(s)/2)
	_, err := fmt.Sscanf(s, "%x", &b)
	return b, err
}

// DELETE object with If-Match deletes only the version it names, quoted, unquoted or weak, or any with "*"; a missing
// object is 404 with or without it, as the API answers.
func TestFakes_ConditionalDelete(t *testing.T) {
	f := Start(t)
	del := func(key, ifMatch string) *Answer {
		t.Helper()
		req, err := http.NewRequest(http.MethodDelete, f.API.URL+"/v1/storage/buckets/photos/object?key="+key, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+APIKey)
		if ifMatch != "" {
			req.Header.Set("If-Match", ifMatch)
		}
		a, err := f.do(req)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	etag := f.API.PutObject("photos", "a", []byte("one"), "").ETag()
	other := strings.Repeat("0", 64)
	for _, im := range []string{other, `"` + other + `"`, `W/"` + other + `"`} {
		if a := del("a", im); a.Status != http.StatusPreconditionFailed || a.Code() != "precondition_failed" {
			t.Fatalf("If-Match %s: %d %s", im, a.Status, a.Code())
		}
	}
	if f.API.Object("photos", "a") == nil {
		t.Fatal("a mismatched delete deleted it")
	}
	if a := del("a", `"`+etag+`"`); a.Status != http.StatusNoContent || f.API.Object("photos", "a") != nil {
		t.Fatalf("the matching ETag, quoted: %d", a.Status)
	}
	if a := del("a", etag); a.Status != http.StatusNotFound || a.Code() != "object_not_found" {
		t.Fatalf("missing, with a condition: %d %s", a.Status, a.Code())
	}
	etag = f.API.PutObject("photos", "a", []byte("two"), "").ETag()
	if a := del("a", `W/"`+etag+`"`); a.Status != http.StatusNoContent {
		t.Fatalf("the matching ETag, weak: %d", a.Status)
	}
	f.API.PutObject("photos", "a", []byte("three"), "")
	if a := del("a", "*"); a.Status != http.StatusNoContent {
		t.Fatalf("If-Match *: %d", a.Status)
	}
	f.API.PutObject("photos", "a", []byte("four"), "")
	if a := del("a", ""); a.Status != http.StatusNoContent {
		t.Fatalf("no condition: %d", a.Status)
	}
}
