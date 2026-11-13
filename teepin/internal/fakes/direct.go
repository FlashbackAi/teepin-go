// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package fakes

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// The formats and sources a read session names.
const (
	FormatTWS1    = "tws1"
	FormatServer  = "teepin-v1"
	FormatPlain   = "plain"
	SourceStorage = "storage"
	SourceAPI     = "api"
)

// The storage network's part limits (S3-style): 5 MiB to 5 GiB as stored, all but the last at least 5 MiB.
const (
	MinPartBytes = 5 << 20
	MaxPartBytes = 5 << 30
)

// EmptySHA256 is the SHA-256 of no bytes: an empty object's checksum and ETag.
const EmptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// emptyBodyLimit is the most an empty object's request may be (teepin-core's emptyBodyLimit).
const emptyBodyLimit = 64 << 10

// emptyFields are an empty object's request fields and their JSON types, nothing else.
var emptyFields = map[string]string{"key": "string", "content_type": "string", "metadata": "object",
	"if_none_match": "boolean", "if_match": "string"}

var (
	hex32 = regexp.MustCompile(`^[0-9a-f]{32}$`)
	hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

const (
	serverNonceSize = 12
	serverTagSize   = 16
	signTTL         = 15 * time.Minute
)

func sha256hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// b32 is 16 random bytes in lowercase base32, unpadded: the backend key names the control plane picks.
func b32() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]))
}

// IDBytes is a UUID string's 16 bytes; false when it isn't one.
func IDBytes(id string) ([16]byte, bool) {
	var out [16]byte
	s := strings.ReplaceAll(id, "-", "")
	if len(s) != 32 {
		return out, false
	}
	_, err := hex.Decode(out[:], []byte(s))
	return out, err == nil
}

// SealServerV1 is a teepin-v1 object, as Teepin seals one it staged: a 12-byte nonce, then AES-256-GCM's ciphertext
// and tag.
func SealServerV1(key, plaintext, associatedData []byte) []byte {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	gcm, _ := cipher.NewGCM(block)
	nonce := make([]byte, serverNonceSize)
	_, _ = rand.Read(nonce)
	return gcm.Seal(nonce, nonce, plaintext, associatedData)
}

// OpenServerV1 is a teepin-v1 object's plaintext, after its tag is checked.
func OpenServerV1(key, stored, associatedData []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("a teepin-v1 key is 32 bytes")
	}
	if len(stored) < serverNonceSize+serverTagSize {
		return nil, fmt.Errorf("the object is too short to be encrypted")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, _ := cipher.NewGCM(block)
	return gcm.Open(nil, stored[:serverNonceSize], stored[serverNonceSize:], associatedData)
}

// Slot is one object a pack reserved, with the request's names.
type Slot struct {
	Key         string
	ObjectID    string
	Size        int64
	SHA256      string
	ContentType string
	Metadata    map[string]string
	IfNoneMatch bool
	IfMatch     string
}

// Pack is a pack's reservation, then its signed manifest, then its commit's result.
type Pack struct {
	ID     string
	Key    string
	Bucket string
	Slots  []Slot
	Digest string
	// Status is "reserved", "sealed" or "abandoned".
	Status  string
	Expires time.Time
	// Manifest is the first sign's manifest (canonical JSON), which every later sign must repeat; "" until signed.
	Manifest string
	// Parsed is the first sign's request, as sent.
	Parsed map[string]any
	Result map[string]any
	Signs  int
	// What the reserve recorded (section 28.16): the reason for a pack under the floor, the client's name, and the
	// pack's stored size as declared.
	UnderFloor string
	Client     string
	Packed     int64
	// Auth is the Authorization header that reserved it (the shared-engine tests: one key per pack).
	Auth string
	// Released is set when the client gave the reservation up (section 28.23).
	Released bool
}

// DirectUpload is a large upload in parts, straight to the storage network.
type DirectUpload struct {
	ID     string
	Bucket string
	Key    string
	// Size is unknown (-1) until complete for an upload created without it (E2 b).
	Size        int64
	PartSize    int64
	SHA256      string
	ContentType string
	Metadata    map[string]string
	IfNoneMatch bool
	IfMatch     string
	Digest      string
	// MPU is the storage network's multipart upload.
	MPU      string
	Physical string
	// Status is "open", "completing", "completed", "aborted" or "abandoned".
	Status  string
	Created time.Time
	Expires time.Time
	// Pins are each part's [md5, sha256], pinned at its first sign.
	Pins map[int][2]string
	// Result is the object's details once completed; PreconditionFailed is set when complete found the
	// precondition failed.
	Result             map[string]any
	PreconditionFailed bool
	Wrapped            []byte
	// PartCount is -1 until the size is known.
	PartCount int64
	// LastPart and LastSize are an upload of unknown size's last part, pinned at its first sign (0: none yet).
	LastPart int
	LastSize int64
	// SignBodies are the sign requests received, as sent.
	SignBodies []map[string]any
}

// stored is part n's stored size: from the layout, or, before the size is known, full unless it is the pinned last
// part.
func (u *DirectUpload) stored(n int) int64 {
	if u.Size >= 0 {
		p, _ := tws1.PartOf(u.Size, u.PartSize, n)
		return p.StoredSize
	}
	plain := u.PartSize
	if n == u.LastPart {
		plain = u.LastSize
	}
	p, _ := tws1.PartOf(int64(n-1)*u.PartSize+plain, u.PartSize, n)
	return p.StoredSize
}

// Direct is the control plane's direct routes (TEEPIN-STORAGE-DESIGN.md sections 20, 21 and 22.2), attached to an
// API, with the storage network faked by Network. A port of fake_direct.py:
//
//   - GET /keys/account: a real X25519 key pair, so wraps really open.
//   - Packs: reserve (idempotency, the pending cap), sign (the manifest checks: exactly the reserved ids, contiguous
//     needles, TWS1 sizes, every wrap opened; a signed manifest never changes), commit (HEAD size and MD5, 202 while
//     storage doesn't show the pack, preconditions per object, a repeat answers the stored result).
//   - Large uploads in parts: create (idempotency, the open-upload cap, the layout, and E2 (b)'s uploads of unknown
//     size), part signs that pin each part's digests, list parts as storage holds them, complete (every part signed,
//     the wrap opened, assembled under If-None-Match, HEAD size and ETag checked), abort.
//   - Read sessions: the three formats, packed objects signed with their needle, blobs in windows (with the TWS1
//     header signed first past the start), staged objects from the API, and refusals inline in a batch.
//   - Empty objects (section 23.5, A2): POST object/empty, a strict JSON body, a put's conditions, and a catalog row
//     of no bytes.
//
// Route names for faults are Route's: "POST packs", "POST pack sign", "POST pack commit", "POST uploads",
// "POST part sign", "GET parts", "POST complete", "DELETE upload", "POST object/read", "POST objects/read",
// "POST object/empty", "GET keys".
//
// Its state is guarded by the API's lock (API.Do).
type Direct struct {
	api *API
	// Network is the storage network.
	Network    *Network
	ownNetwork bool

	AccountID string
	// PrivateKey and PublicKey are the account's X25519 key pair, raw 32 bytes each; a test may swap them.
	PrivateKey, PublicKey []byte
	KeyVersion            int
	// Backend is what a commit names (the real one names the deployment's storage backend).
	Backend string

	Packs      map[string]*Pack
	packIdem   map[string][2]string // idempotency key: request digest, pack id
	Uploads    map[string]*DirectUpload
	uploadIdem map[string][2]string // idempotency key: request digest, upload id

	// Limits, as the control plane's defaults; tests lower them.
	MaxPackBytes     int64
	MaxPackObjects   int
	MaxReservedPacks int
	ReserveTTL       time.Duration
	ReadWindow       int64
	MaxReadBatch     int
	MaxOpenUploads   int
	MaxParts         int64
	MinPartBytes     int64
	UploadTTL        time.Duration

	// PackLimits is the reserve answer's limits (section 28.15): nil sends none, as a server before section 28.
	PackLimits map[string]any

	// Skew is added to the clock (expiries); the storage network has its own.
	Skew time.Duration
	// Metered is the bytes signed for reading, as the control plane meters them (D6).
	Metered int64
	// Erased are keys whose data key has been erased: their reads answer 410 key_erased (inline in a batch).
	Erased map[string]bool
}

// StartDirect attaches direct routes to api, with a storage network of their own unless one is given.
func StartDirect(api *API, network *Network) *Direct {
	own := network == nil
	if own {
		network = StartNetwork()
	}
	priv, pub, err := tws1.GenerateAccountKey()
	if err != nil {
		panic(err)
	}
	d := &Direct{api: api, Network: network, ownNetwork: own, AccountID: newUUID(), PrivateKey: priv, PublicKey: pub,
		KeyVersion: 1, Backend: "fake", Packs: map[string]*Pack{}, packIdem: map[string][2]string{},
		Uploads: map[string]*DirectUpload{}, uploadIdem: map[string][2]string{},
		MaxPackBytes: 64 << 20, MaxPackObjects: 140000, MaxReservedPacks: 64, ReserveTTL: time.Hour,
		ReadWindow: 64 << 20, MaxReadBatch: 1000, MaxOpenUploads: 32, MaxParts: 3000, MinPartBytes: MinPartBytes,
		UploadTTL: 7 * 24 * time.Hour}
	api.Do(func() { api.Direct = d })
	return d
}

// Close closes the storage network, if it was made for these routes.
func (d *Direct) Close() {
	if d.ownNetwork {
		d.Network.Close()
	}
}

func (d *Direct) now() time.Time { return time.Now().Add(d.Skew) }

func (d *Direct) after(t time.Duration) time.Time { return d.now().Add(t) }

// AccountKey is the account key, as GET /keys/account and every reservation answer it.
func (d *Direct) AccountKey() map[string]any {
	return map[string]any{"account_id": d.AccountID, "version": d.KeyVersion, "public_key": b64(d.PublicKey),
		"suite": tws1.Suite}
}

// Unwrap is the data key inside an object's wrap; an error when it doesn't open.
func (d *Direct) Unwrap(objectID string, wrapped []byte) ([]byte, error) {
	acct, ok1 := IDBytes(d.AccountID)
	oid, ok2 := IDBytes(objectID)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("%s is not a UUID", objectID)
	}
	return tws1.UnwrapKey(d.PrivateKey, acct, oid, wrapped)
}

// ReadBack is an object's plaintext, read straight from the storage network with the catalog's key: what any correct
// reader must get.
func (d *Direct) ReadBack(bucket, key string) ([]byte, error) {
	d.api.mu.Lock()
	o := d.api.Buckets[bucket][key]
	d.api.mu.Unlock()
	if o == nil {
		return nil, fmt.Errorf("%s/%s: no such object", bucket, key)
	}
	if !o.Direct() {
		return bytes.Clone(o.Data), nil
	}
	if o.StoredSize == 0 {
		return []byte{}, nil // an empty object: a catalog row, no bytes anywhere
	}
	raw := d.Network.Blob(o.Physical)
	stored := raw
	if o.Location == "packed" {
		oid, _ := IDBytes(o.ID)
		var err error
		if stored, err = tws1.ReadNeedle(raw, o.Offset, oid, o.StoredSize); err != nil {
			return nil, err
		}
	}
	switch o.Format {
	case FormatTWS1:
		dek, err := d.Unwrap(o.ID, o.Wrapped)
		if err != nil {
			return nil, err
		}
		return tws1.Decrypt(dek, stored)
	case FormatServer:
		return OpenServerV1(o.ServerKey, stored, o.AD)
	}
	return stored, nil
}

// OpenPacks are the packs reserved and not yet committed or abandoned. The caller holds the API's lock.
func (d *Direct) OpenPacks() []*Pack {
	var out []*Pack
	for _, p := range d.Packs {
		if p.Status == "reserved" {
			out = append(out, p)
		}
	}
	return out
}

// OpenUploads are the large uploads open or completing. The caller holds the API's lock.
func (d *Direct) OpenUploads() []*DirectUpload {
	var out []*DirectUpload
	for _, u := range d.Uploads {
		if u.Status == "open" || u.Status == "completing" {
			out = append(out, u)
		}
	}
	return out
}

// PutServerObject stores a small object Teepin encrypted when it staged it, now in a server pack: format teepin-v1.
func (d *Direct) PutServerObject(bucket, key string, data []byte) *Object {
	oid := newUUID()
	dek := make([]byte, 32)
	_, _ = rand.Read(dek)
	ad := []byte(fmt.Sprintf("teepin/object/v1/%s/b-%s/%s", d.AccountID, bucket, oid))
	stored := SealServerV1(dek, data, ad)
	before := make([]byte, 100) // another object before it
	_, _ = rand.Read(before)
	id, _ := IDBytes(oid)
	pack, offset := tws1.AppendNeedle(before, id, stored)
	physical := "t/pack-" + b32()
	d.Network.SetBlob(physical, pack)
	o := &Object{Data: bytes.Clone(data), Size: int64(len(data)), ID: oid, Metadata: map[string]string{},
		SHA: sha256hex(data), Updated: wholeSeconds(time.Now()), Location: "packed", Format: FormatServer,
		Physical: physical, StoredSize: int64(len(stored)), Offset: offset, ServerKey: dek, AD: ad}
	d.api.Do(func() { d.api.Bucket(bucket)[key] = o })
	return o
}

// PutPlainBlob stores an object written before encryption, in its own blob: format plain. parts > 0 makes it a
// legacy object uploaded in parts (its ETag "<sha>-parts").
func (d *Direct) PutPlainBlob(bucket, key string, data []byte, parts int) *Object {
	oid := newUUID()
	physical := "t/" + oid
	d.Network.SetBlob(physical, data)
	o := &Object{Data: bytes.Clone(data), Size: int64(len(data)), ID: oid, Metadata: map[string]string{},
		SHA: sha256hex(data), Parts: parts, Updated: wholeSeconds(time.Now()), Location: "blob", Format: FormatPlain,
		Physical: physical, StoredSize: int64(len(data))}
	d.api.Do(func() { d.api.Bucket(bucket)[key] = o })
	return o
}

// --- dispatch ---------------------------------------------------------------------

// route answers a direct route, or nil for the old routes to answer. The API's lock is held.
func (d *Direct) route(ex *exchange, bucket string, seg []string) *reply {
	m := ex.method
	path := strings.Join(seg, "/")
	switch {
	case path == "packs" && m == http.MethodPost:
		return d.reserve(ex, bucket)
	case len(seg) == 3 && seg[0] == "packs" && seg[2] == "sign" && m == http.MethodPost:
		return d.signPack(ex, bucket, seg[1])
	case len(seg) == 3 && seg[0] == "packs" && seg[2] == "commit" && m == http.MethodPost:
		return d.commitPack(bucket, seg[1])
	case len(seg) == 3 && seg[0] == "packs" && seg[2] == "release" && m == http.MethodPost:
		return d.releasePack(bucket, seg[1])
	case path == "object/read" && m == http.MethodPost:
		return d.readOne(ex, bucket)
	case path == "objects/read" && m == http.MethodPost:
		return d.readMany(ex, bucket)
	case path == "object/empty" && m == http.MethodPost:
		return d.putEmpty(ex, bucket)
	case len(seg) >= 2 && seg[0] == "uploads" && d.Uploads[seg[1]] != nil:
		return d.uploadRoute(ex, bucket, seg[1], seg[2:])
	}
	return nil
}

// sweep is what the reservation and upload sweepers do: expired reservations and open uploads are abandoned.
func (d *Direct) sweep() {
	now := d.now()
	for _, p := range d.Packs {
		if p.Status == "reserved" && now.After(p.Expires) {
			p.Status = "abandoned"
		}
	}
	for _, u := range d.Uploads {
		if u.Status == "open" && now.After(u.Expires) {
			u.Status = "abandoned"
			d.Network.AbortMultipart(u.MPU)
		}
	}
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// --- packs ------------------------------------------------------------------------

func (d *Direct) reservation(p *Pack) map[string]any {
	objects := make([]map[string]any, 0, len(p.Slots))
	for _, s := range p.Slots {
		objects = append(objects, map[string]any{"key": s.Key, "object_id": s.ObjectID})
	}
	out := map[string]any{"pack": p.ID, "expires_at": iso(p.Expires), "account_key": d.AccountKey(), "objects": objects}
	if d.PackLimits != nil {
		out["limits"] = d.PackLimits
	}
	return out
}

// releasePack frees a reservation the client gave up (section 28.23).
func (d *Direct) releasePack(bucket, pid string) *reply {
	p, rep := d.pack(bucket, pid)
	if rep != nil {
		return rep
	}
	if p.Status == "sealed" {
		return apiError(http.StatusConflict, "pack_committed", nil, map[string]any{"pack": p.ID})
	}
	p.Status, p.Released = "abandoned", true
	return jsonReply(http.StatusOK, map[string]any{"status": "abandoned"}, nil)
}

func invalid(message string) *reply {
	return refusal(http.StatusBadRequest, "invalid_request", message, nil, nil)
}

func (d *Direct) reserve(ex *exchange, bucket string) *reply {
	req, rep := ex.json()
	if rep != nil {
		return rep
	}
	idem := str(req["idempotency_key"])
	objects, _ := req["objects"].([]any)
	if len(idem) < 1 || len(idem) > 128 {
		return invalid("idempotency_key must be 1 to 128 characters")
	}
	if len(objects) < 1 || len(objects) > d.MaxPackObjects {
		return invalid(fmt.Sprintf("a pack holds 1 to %d objects", d.MaxPackObjects))
	}
	reason, hasReason := req["under_floor"]
	underFloor, _ := reason.(string)
	if hasReason && underFloor != "leftover" && underFloor != "slow_link" && underFloor != "request_limit" {
		return invalid("under_floor must be leftover, slow_link or request_limit")
	}
	keys := map[string]bool{}
	var packed int64
	for i, ov := range objects {
		o, _ := ov.(map[string]any)
		key := str(o["key"])
		if key == "" || keys[key] {
			return invalid(fmt.Sprintf("object %d: a missing or repeated key", i))
		}
		keys[key] = true
		size, ok := integer(o["size"])
		if !ok || size <= 0 {
			return invalid(fmt.Sprintf("object %d: size must be positive", i))
		}
		if size > d.MaxPackBytes {
			return invalid(fmt.Sprintf("object %d: more than a pack holds", i))
		}
		if !hex64.MatchString(str(o["sha256"])) {
			return invalid(fmt.Sprintf("object %d: sha256 must be 64 lowercase hex characters", i))
		}
		packed += tws1.NeedleHeaderSize + tws1.StoredSize(size)
	}
	if packed > d.MaxPackBytes {
		return invalid(fmt.Sprintf("the pack would be %d bytes", packed))
	}
	digest := sha256hex([]byte(canonical(map[string]any{"bucket": bucket, "objects": objects})))
	d.sweep()
	var out map[string]any
	if prior, ok := d.packIdem[idem]; ok {
		if prior[0] != digest {
			return apiError(http.StatusConflict, "idempotency_mismatch", nil, nil)
		}
		p := d.Packs[prior[1]]
		switch p.Status {
		case "abandoned":
			return apiError(http.StatusConflict, "reservation_abandoned", nil, nil)
		case "sealed":
			return apiError(http.StatusConflict, "pack_committed", nil, map[string]any{"pack": p.ID})
		}
		out = d.reservation(p)
	} else {
		if len(d.OpenPacks()) >= d.MaxReservedPacks {
			return apiError(http.StatusTooManyRequests, "too_many_pending_uploads", map[string]string{"Retry-After": "30"}, nil)
		}
		slots := make([]Slot, 0, len(objects))
		for _, ov := range objects {
			o := ov.(map[string]any)
			size, _ := integer(o["size"])
			slots = append(slots, Slot{Key: str(o["key"]), ObjectID: newUUID(), Size: size, SHA256: str(o["sha256"]),
				ContentType: str(o["content_type"]), Metadata: stringMap(o["metadata"]), IfNoneMatch: truthy(o["if_none_match"]),
				IfMatch: stripQuotes(str(o["if_match"]))})
		}
		p := &Pack{ID: newUUID(), Key: "p/" + b32(), Bucket: bucket, Slots: slots, Digest: digest, Status: "reserved",
			Expires: d.after(d.ReserveTTL), UnderFloor: underFloor, Client: ex.header["x-teepin-client"], Packed: packed,
			Auth: ex.header["authorization"]}
		d.Packs[p.ID] = p
		d.packIdem[idem] = [2]string{digest, p.ID}
		out = d.reservation(p)
	}
	return jsonReply(http.StatusCreated, out, nil)
}

func (d *Direct) pack(bucket, pid string) (*Pack, *reply) {
	p := d.Packs[pid]
	if p == nil || p.Bucket != bucket {
		return nil, apiError(http.StatusNotFound, "pack_not_found", nil, nil)
	}
	return p, nil
}

// opens is which of a manifest's wraps open, by object id.
func (d *Direct) opens(objects []any) map[string]bool {
	out := map[string]bool{}
	for _, ov := range objects {
		o, _ := ov.(map[string]any)
		id, ok := o["object_id"].(string)
		if !ok {
			continue
		}
		if _, seen := out[id]; seen {
			continue
		}
		wrapped, err := base64.StdEncoding.DecodeString(str(o["wrapped_key"]))
		if err != nil {
			out[id] = false
			continue
		}
		_, err = d.Unwrap(id, wrapped)
		out[id] = err == nil
	}
	return out
}

func offsetOf(o map[string]any) int64 {
	n, ok := o["offset"].(json.Number)
	if !ok {
		return 0
	}
	f, _ := n.Float64()
	return int64(f)
}

func (d *Direct) checkManifest(p *Pack, req map[string]any) *reply {
	size, ok := integer(req["size"])
	md5s, sha := str(req["md5"]), str(req["sha256"])
	objs, _ := req["objects"].([]any)
	if !ok || !hex32.MatchString(md5s) || !hex64.MatchString(sha) {
		return invalid("size, md5 and sha256 are required")
	}
	slots := map[string]Slot{}
	var want []string
	for _, s := range p.Slots {
		slots[s.ObjectID] = s
		want = append(want, s.ObjectID)
	}
	sort.Strings(want)
	var ids []string
	maps := make([]map[string]any, 0, len(objs))
	for _, ov := range objs {
		o, _ := ov.(map[string]any)
		if o == nil {
			o = map[string]any{}
		}
		maps = append(maps, o)
		id := ""
		if v := o["object_id"]; v != nil {
			id = fmt.Sprint(v)
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if !slices.Equal(ids, want) {
		return invalid("the objects must be exactly the reserved ones")
	}
	opens := d.opens(objs)
	sort.SliceStable(maps, func(i, j int) bool { return offsetOf(maps[i]) < offsetOf(maps[j]) })
	expected := int64(tws1.NeedleHeaderSize)
	var bad []string
	for _, o := range maps {
		id := str(o["object_id"])
		s := slots[id]
		stored, ok := integer(o["stored_size"])
		if !ok || stored != tws1.StoredSize(s.Size) {
			return invalid(fmt.Sprintf("object %s: wrong stored size", id))
		}
		if off, ok := integer(o["offset"]); !ok || off != expected {
			return invalid(fmt.Sprintf("object %s: wrong offset", id))
		}
		expected += stored + tws1.NeedleHeaderSize
		if kv, ok := integer(o["key_version"]); !ok || kv != int64(d.KeyVersion) {
			return invalid("wrapped to another key version")
		}
		if !opens[id] {
			bad = append(bad, id)
		}
	}
	if size != expected-tws1.NeedleHeaderSize {
		return invalid("the pack's size doesn't add up")
	}
	if len(bad) > 0 {
		return refusal(http.StatusBadRequest, "bad_wrapped_key", "wrapped keys don't open: "+strings.Join(bad, ", "), nil, nil)
	}
	return nil
}

func (d *Direct) signPack(ex *exchange, bucket, pid string) *reply {
	req, rep := ex.json()
	if rep != nil {
		return rep
	}
	objects, _ := req["objects"].([]any)
	each := make([]string, 0, len(objects))
	for _, o := range objects {
		each = append(each, canonical(o))
	}
	sort.Strings(each)
	manifest := canonical(map[string]any{"size": req["size"], "md5": req["md5"], "sha256": req["sha256"], "objects": each})
	d.sweep()
	p, rep := d.pack(bucket, pid)
	if rep != nil {
		return rep
	}
	if p.Status != "reserved" {
		return apiError(http.StatusConflict, "reservation_abandoned", nil, nil)
	}
	if p.Manifest != "" {
		if manifest != p.Manifest {
			return apiError(http.StatusConflict, "manifest_mismatch", nil, nil)
		}
	} else {
		if rep := d.checkManifest(p, req); rep != nil {
			return rep
		}
		p.Manifest = manifest
		p.Parsed = req
	}
	p.Expires = later(p.Expires, d.after(30*time.Minute))
	p.Signs++
	size, _ := integer(req["size"])
	md5b, _ := hex.DecodeString(str(req["md5"]))
	headers := map[string]string{
		"content-length":       fmt.Sprint(size),
		"content-md5":          b64(md5b),
		"x-amz-content-sha256": str(req["sha256"]),
		"if-none-match":        "*",
		"content-type":         "application/octet-stream",
		"content-disposition":  "attachment",
		"cache-control":        "no-store",
	}
	return jsonReply(http.StatusOK, d.Network.Sign(http.MethodPut, p.Key, headers, nil, signTTL), nil)
}

func (d *Direct) commitPack(bucket, pid string) *reply {
	d.sweep()
	p, rep := d.pack(bucket, pid)
	if rep != nil {
		return rep
	}
	switch {
	case p.Status == "sealed":
		return jsonReply(http.StatusOK, p.Result, nil)
	case p.Status == "abandoned":
		return apiError(http.StatusConflict, "reservation_abandoned", nil, nil)
	case p.Manifest == "":
		return apiError(http.StatusConflict, "pack_not_signed", nil, nil)
	}
	size, etag, ok := d.Network.Head(p.Key)
	if !ok {
		return jsonReply(http.StatusAccepted, map[string]any{"status": "verifying"}, map[string]string{"Retry-After": "1"})
	}
	req := p.Parsed
	if want, _ := integer(req["size"]); size != want {
		p.Status = "abandoned"
		return apiError(http.StatusConflict, "pack_mismatch", nil, nil)
	}
	if etag != str(req["md5"]) {
		return apiError(http.StatusBadGateway, "pack_unverified", nil, nil)
	}
	objs := d.api.Buckets[bucket]
	if objs == nil {
		return apiError(http.StatusNotFound, "bucket_not_found", nil, nil)
	}
	when := wholeSeconds(d.now())
	signed := map[string]map[string]any{}
	parsed, _ := req["objects"].([]any)
	for _, ov := range parsed {
		o, _ := ov.(map[string]any)
		signed[str(o["object_id"])] = o
	}
	var results []map[string]any
	for _, s := range p.Slots {
		cur := objs[s.Key]
		failed := (s.IfNoneMatch && cur != nil) || (s.IfMatch != "" && (cur == nil || (s.IfMatch != "*" && s.IfMatch != cur.ETag())))
		if failed {
			results = append(results, map[string]any{"key": s.Key, "object_id": s.ObjectID, "result": "failed", "error": "precondition_failed"})
			continue
		}
		o := signed[s.ObjectID]
		stored, _ := integer(o["stored_size"])
		offset, _ := integer(o["offset"])
		kv, _ := integer(o["key_version"])
		wrapped, _ := base64.StdEncoding.DecodeString(str(o["wrapped_key"]))
		objs[s.Key] = &Object{ID: s.ObjectID, Size: s.Size, SHA: s.SHA256, ContentType: s.ContentType, Metadata: s.Metadata,
			Updated: when, ClientEncrypted: true, Location: "packed", Format: FormatTWS1, Physical: p.Key,
			StoredSize: stored, Offset: offset, Wrapped: wrapped, KeyVersion: int(kv)}
		results = append(results, map[string]any{"key": s.Key, "object_id": s.ObjectID, "result": "saved"})
	}
	// In key order, as teepin-core answers (direct_store.go sorts them).
	sort.SliceStable(results, func(i, j int) bool { return str(results[i]["key"]) < str(results[j]["key"]) })
	p.Status = "sealed"
	p.Result = map[string]any{"status": "committed", "committed_at": iso(when), "backend": d.Backend, "objects": results}
	return jsonReply(http.StatusOK, p.Result, nil)
}

// --- empty objects (A2) -------------------------------------------------------------

// putEmpty is POST object/empty: an object of no bytes, its catalog row alone. Anything but one JSON object with only
// its fields, each of its type (null is each one's zero, as Go decodes it), is refused 400, file bytes included;
// then a put's conditions (412) and overwrite.
func (d *Direct) putEmpty(ex *exchange, bucket string) *reply {
	refuse := invalid("the body must be a JSON object with a key and, optionally, content_type, metadata, " +
		"if_none_match and if_match, and nothing else")
	if len(ex.body) > emptyBodyLimit {
		return refuse
	}
	dec := json.NewDecoder(bytes.NewReader(ex.body))
	var req map[string]any
	if err := dec.Decode(&req); err != nil || req == nil {
		return refuse
	}
	if _, err := dec.Token(); err != io.EOF {
		return refuse
	}
	for k, v := range req {
		kind, ok := emptyFields[k]
		if !ok {
			return refuse
		}
		if v == nil {
			continue
		}
		switch kind {
		case "string":
			if _, ok := v.(string); !ok {
				return refuse
			}
		case "boolean":
			if _, ok := v.(bool); !ok {
				return refuse
			}
		case "object":
			m, ok := v.(map[string]any)
			if !ok {
				return refuse
			}
			for _, x := range m {
				if _, ok := x.(string); !ok {
					return refuse
				}
			}
		}
	}
	key := str(req["key"])
	if key == "" {
		return invalid("key is required")
	}
	objs := d.api.Buckets[bucket]
	if objs == nil {
		return apiError(http.StatusNotFound, "bucket_not_found", nil, nil)
	}
	cur := objs[key]
	im := stripQuotes(strings.TrimPrefix(str(req["if_match"]), "W/"))
	if (truthy(req["if_none_match"]) && cur != nil) || (im != "" && (cur == nil || (im != "*" && im != cur.ETag()))) {
		return apiError(http.StatusPreconditionFailed, "precondition_failed", nil, nil)
	}
	id := newUUID()
	o := &Object{ID: id, SHA: EmptySHA256, ContentType: str(req["content_type"]), Metadata: stringMap(req["metadata"]),
		Updated: wholeSeconds(d.now()), Location: "blob", Format: FormatPlain, Physical: "e/" + id}
	objs[key] = o
	return jsonReply(http.StatusOK, d.api.ObjectJSON(key, o), map[string]string{"ETag": `"` + o.ETag() + `"`})
}

// --- reads ----------------------------------------------------------------------------

// signedGet signs a GET of bytes [first, last] of physical, and meters them.
func (d *Direct) signedGet(physical string, first, last int64) map[string]any {
	d.Metered += last - first + 1
	s := d.Network.Sign(http.MethodGet, physical, map[string]string{"range": fmt.Sprintf("bytes=%d-%d", first, last)}, nil, signTTL)
	return map[string]any{"method": s.Method, "url": s.URL, "headers": s.Headers, "expires_at": s.ExpiresAt,
		"prefix": int64(0), "stored_first": first, "stored_last": last}
}

// session is an object's read session. A stored_offset past a blob's end is refused.
func (d *Direct) session(key string, o *Object, off, length int64, includeKey bool) (map[string]any, *reply) {
	version := o.ID
	if version == "" {
		version = "obj-" + key
	}
	s := map[string]any{"key": key, "version": version, "size": o.Size, "etag": o.ETag(), "sha256": o.SHA,
		"content_type": o.ContentType, "metadata": o.Metadata, "uploaded_at": iso(o.Updated), "requests": []any{},
		"key_material": nil}
	if !o.Direct() {
		s["format"], s["source"], s["stored_size"] = FormatPlain, SourceAPI, o.Size
		return s, nil
	}
	s["format"], s["source"], s["stored_size"] = o.Format, SourceStorage, o.StoredSize
	if o.StoredSize > 0 {
		var requests []any
		switch {
		case o.Location == "packed":
			r := d.signedGet(o.Physical, o.Offset-tws1.NeedleHeaderSize, o.Offset+o.StoredSize-1)
			r["prefix"], r["stored_first"], r["stored_last"] = int64(tws1.NeedleHeaderSize), int64(0), o.StoredSize-1
			requests = []any{r}
		case off >= o.StoredSize:
			return nil, invalid(fmt.Sprintf("stored_offset %d is past the object's %d bytes", off, o.StoredSize))
		default:
			window := d.ReadWindow
			if length > 0 {
				window = min(length, d.ReadWindow)
			}
			last := min(off+window, o.StoredSize) - 1
			r := d.signedGet(o.Physical, off, last)
			if o.Format == FormatTWS1 && off > 0 {
				headLast := min(int64(tws1.HeaderSize), o.StoredSize) - 1
				requests = []any{d.signedGet(o.Physical, 0, headLast), r}
			} else {
				requests = []any{r}
			}
		}
		s["requests"] = requests
	}
	if includeKey && o.Format == FormatTWS1 {
		dek, err := d.Unwrap(o.ID, o.Wrapped)
		if err != nil {
			panic(fmt.Sprintf("the catalog's wrap of %s doesn't open: %v", key, err))
		}
		s["key_material"] = b64(dek)
	}
	if includeKey && o.Format == FormatServer {
		s["key_material"] = b64(o.ServerKey)
	}
	if o.Format == FormatServer {
		s["associated_data"] = b64(o.AD)
	}
	return s, nil
}

func (d *Direct) readOne(ex *exchange, bucket string) *reply {
	req, rep := ex.json()
	if rep != nil {
		return rep
	}
	key := str(req["key"])
	o := d.api.Buckets[bucket][key]
	if o == nil {
		return apiError(http.StatusNotFound, "object_not_found", nil, nil)
	}
	if d.Erased[key] {
		return apiError(http.StatusGone, "key_erased", nil, nil)
	}
	s, rep := d.session(key, o, toInt(req["stored_offset"]), toInt(req["stored_length"]), !isFalse(req["include_key"]))
	if rep != nil {
		return rep
	}
	return jsonReply(http.StatusOK, s, nil)
}

// readMany is POST objects/read: a session per key, each refusal inline.
func (d *Direct) readMany(ex *exchange, bucket string) *reply {
	req, rep := ex.json()
	if rep != nil {
		return rep
	}
	keys, _ := req["keys"].([]any)
	if len(keys) < 1 || len(keys) > d.MaxReadBatch {
		return invalid(fmt.Sprintf("a read names 1 to %d objects", d.MaxReadBatch))
	}
	objs := d.api.Buckets[bucket]
	out := make([]any, 0, len(keys))
	for _, kv := range keys {
		k := str(kv)
		o := objs[k]
		if o == nil {
			out = append(out, map[string]any{"key": k, "error": map[string]any{"code": "object_not_found", "error": "object not found"}})
			continue
		}
		if d.Erased[k] {
			out = append(out, map[string]any{"key": k, "error": map[string]any{"code": "key_erased", "error": "the key was erased"}})
			continue
		}
		s, rep := d.session(k, o, 0, 0, !isFalse(req["include_keys"]))
		if rep != nil {
			return rep
		}
		out = append(out, s)
	}
	return jsonReply(http.StatusOK, map[string]any{"objects": out}, nil)
}

// --- large uploads in parts -------------------------------------------------------------

func (d *Direct) uploadJSON(u *DirectUpload) map[string]any {
	var size, count, sha any
	if u.Size >= 0 {
		size = u.Size
	}
	if u.PartCount >= 0 {
		count = u.PartCount
	}
	if u.SHA256 != "" {
		sha = u.SHA256
	}
	return map[string]any{"upload_id": u.ID, "key": u.Key, "content_type": u.ContentType, "metadata": u.Metadata,
		"created_at": iso(u.Created), "expires_at": iso(u.Expires), "direct": true, "size": size,
		"part_size": u.PartSize, "part_count": count, "sha256": sha, "account_key": d.AccountKey()}
}

// createUpload is POST uploads with "direct": true. The API's lock is held.
func (d *Direct) createUpload(bucket string, req map[string]any) *reply {
	idem := str(req["idempotency_key"])
	sizeV, hasSize := req["size"]
	if sizeV == nil {
		hasSize = false
	}
	psize, psOK := integer(req["part_size"])
	// A stream's upload gives neither its size nor its SHA-256 (E2 b); one without the other is refused.
	openEnded := !hasSize && req["sha256"] == nil
	if len(idem) < 1 || len(idem) > 128 {
		return invalid("idempotency_key must be 1 to 128 characters")
	}
	if !openEnded && !hex64.MatchString(str(req["sha256"])) {
		return invalid("sha256 must be 64 lowercase hex characters")
	}
	size, sizeOK := integer(sizeV)
	if !openEnded && (!sizeOK || size <= 0) {
		return invalid("size must be positive, or left out with sha256")
	}
	if !psOK || psize <= 0 || psize%tws1.SegmentSize != 0 || !truthy(req["key"]) {
		return invalid("key and part_size (a multiple of 65,536) are required")
	}
	var first tws1.Part
	var full int64
	if openEnded {
		first, _ = tws1.PartOf(psize, psize, 1)
		second, _ := tws1.PartOf(2*psize, psize, 2)
		full = second.StoredSize
	} else {
		n, err := tws1.PartCount(size, psize)
		if err != nil {
			return invalid(err.Error())
		}
		if n > d.MaxParts {
			return invalid(fmt.Sprintf("%d parts; at most %d", n, d.MaxParts))
		}
		first, _ = tws1.PartOf(size, psize, 1)
		full = d.MinPartBytes
		if n > 1 {
			full = first.StoredSize
		}
	}
	if first.StoredSize > MaxPartBytes {
		return invalid("a part is at most 5 GiB as stored")
	}
	if full < d.MinPartBytes {
		return invalid("every part but the last must be at least 5 MiB as stored")
	}
	spec := map[string]any{}
	for _, k := range []string{"key", "size", "part_size", "sha256", "content_type", "metadata", "if_none_match", "if_match"} {
		spec[k] = req[k]
	}
	digest := sha256hex([]byte(canonical(map[string]any{"bucket": bucket, "upload": spec})))
	d.sweep()
	var out map[string]any
	if prior, ok := d.uploadIdem[idem]; ok {
		if prior[0] != digest {
			return apiError(http.StatusConflict, "idempotency_mismatch", nil, nil)
		}
		u := d.Uploads[prior[1]]
		switch {
		case u.Status == "completed":
			return apiError(http.StatusConflict, "upload_completed", nil, map[string]any{"upload_id": u.ID})
		case u.Status != "open" && u.Status != "completing":
			return apiError(http.StatusConflict, "reservation_abandoned", nil, nil)
		}
		out = d.uploadJSON(u)
	} else {
		// A condition the key's object already fails is refused before any byte moves, as the server does; complete
		// checks it again.
		if cur := d.api.Buckets[bucket][str(req["key"])]; cur != nil {
			im := stripQuotes(str(req["if_match"]))
			if truthy(req["if_none_match"]) || (im != "" && im != "*" && im != cur.ETag()) {
				return apiError(http.StatusPreconditionFailed, "precondition_failed", nil, nil)
			}
		}
		if len(d.OpenUploads()) >= d.MaxOpenUploads {
			return apiError(http.StatusTooManyRequests, "too_many_pending_uploads", map[string]string{"Retry-After": "30"}, nil)
		}
		physical := "o/" + b32()
		u := &DirectUpload{ID: newUUID(), Bucket: bucket, Key: str(req["key"]), Size: -1, PartSize: psize,
			SHA256: str(req["sha256"]), ContentType: str(req["content_type"]), Metadata: stringMap(req["metadata"]),
			IfNoneMatch: truthy(req["if_none_match"]), IfMatch: str(req["if_match"]), Digest: digest,
			MPU: d.Network.CreateMultipart(physical), Physical: physical, Status: "open", Created: time.Now(),
			Expires: d.after(d.UploadTTL), Pins: map[int][2]string{}, PartCount: -1}
		if !openEnded {
			u.Size = size
			u.PartCount, _ = tws1.PartCount(size, psize)
		}
		d.Uploads[u.ID] = u
		d.uploadIdem[idem] = [2]string{digest, u.ID}
		out = d.uploadJSON(u)
	}
	return jsonReply(http.StatusCreated, out, nil)
}

func (d *Direct) uploadRoute(ex *exchange, bucket, uid string, rest []string) *reply {
	u := d.Uploads[uid]
	m := ex.method
	if u.Bucket != bucket {
		return apiError(http.StatusNotFound, "upload_not_found", nil, nil)
	}
	switch {
	case len(rest) == 0 && m == http.MethodDelete:
		return d.abortUpload(u)
	case len(rest) == 1 && rest[0] == "parts" && m == http.MethodGet:
		return d.listUploadParts(u)
	case len(rest) == 3 && rest[0] == "parts" && rest[2] == "sign" && m == http.MethodPost:
		return d.signUploadPart(ex, u, rest[1])
	case len(rest) == 2 && rest[0] == "parts" && m == http.MethodPut:
		return apiError(http.StatusConflict, "upload_is_direct", nil, nil)
	case len(rest) == 1 && rest[0] == "complete" && m == http.MethodPost:
		req, rep := ex.json()
		if rep != nil {
			return rep
		}
		if _, ok := req["wrapped_key"]; !ok {
			return apiError(http.StatusConflict, "upload_is_direct", nil, nil)
		}
		return d.completeUpload(u, req)
	}
	return apiError(http.StatusNotFound, "not_found", nil, nil)
}

func (d *Direct) abortUpload(u *DirectUpload) *reply {
	switch {
	case u.Status == "completing":
		return apiError(http.StatusConflict, "upload_completing", nil, nil)
	case u.Status != "open":
		return apiError(http.StatusNotFound, "upload_not_found", nil, nil)
	}
	u.Status = "aborted"
	d.Network.AbortMultipart(u.MPU)
	return &reply{status: http.StatusNoContent}
}

func (d *Direct) listUploadParts(u *DirectUpload) *reply {
	d.sweep()
	if u.Status != "open" && u.Status != "completing" {
		return apiError(http.StatusNotFound, "upload_not_found", nil, nil)
	}
	landed := map[int]PartInfo{}
	for _, p := range d.Network.ListParts(u.MPU) {
		landed[p.Number] = p
	}
	var nums []int
	for n := range u.Pins {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	parts := []map[string]any{}
	for _, n := range nums {
		stored := u.stored(n)
		md5s := u.Pins[n][0]
		l, ok := landed[n]
		parts = append(parts, map[string]any{"part_number": n, "stored_size": stored, "md5": md5s, "signed": true,
			"landed": ok && l.Size == stored && l.MD5 == md5s})
	}
	out := d.uploadJSON(u)
	out["parts"] = parts
	return jsonReply(http.StatusOK, out, nil)
}

func (d *Direct) signUploadPart(ex *exchange, u *DirectUpload, number string) *reply {
	req, rep := ex.json()
	if rep != nil {
		return rep
	}
	md5s, sha := str(req["md5"]), str(req["sha256"])
	if !hex32.MatchString(md5s) || !hex64.MatchString(sha) {
		return invalid("md5 must be 32 and sha256 64 lowercase hex characters")
	}
	if !wholeNumber.MatchString(number) {
		return refusal(http.StatusBadRequest, "invalid_part", "the part number must be a number", nil, nil)
	}
	n64 := toInt(strings.TrimSpace(number))
	n := int(n64)
	last := truthy(req["last"])
	plain, plainOK := integer(req["plain_size"])
	u.SignBodies = append(u.SignBodies, req)
	if u.PartCount >= 0 {
		if n < 1 || n64 > u.PartCount {
			return invalid(fmt.Sprintf("part %d of %d", n, u.PartCount))
		}
		if last {
			p, _ := tws1.PartOf(u.Size, u.PartSize, n)
			if n64 != u.PartCount || plain != p.PlainSize {
				return refusal(http.StatusBadRequest, "invalid_part", fmt.Sprintf("the last part is %d", u.PartCount), nil, nil)
			}
		}
	} else if n < 1 || n64 > d.MaxParts {
		return refusal(http.StatusBadRequest, "invalid_part", fmt.Sprintf("an upload has parts 1 to %d", d.MaxParts), nil, nil)
	} else if last && !(plainOK && plain >= 1 && plain <= u.PartSize) {
		return refusal(http.StatusBadRequest, "invalid_part", fmt.Sprintf("the last part holds 1 to %d bytes", u.PartSize), nil, nil)
	}
	d.sweep()
	switch {
	case u.Status == "completing":
		return apiError(http.StatusConflict, "upload_completing", nil, nil)
	case u.Status != "open":
		return apiError(http.StatusNotFound, "upload_not_found", nil, nil)
	}
	if pin, ok := u.Pins[n]; ok && (pin[0] != md5s || pin[1] != sha) {
		return apiError(http.StatusConflict, "part_mismatch", nil, nil)
	}
	if u.Size < 0 {
		// The control plane's five rules (section 22.1, E2 b).
		pinned := u.LastPart
		switch {
		case pinned != 0 && n > pinned:
			return refusal(http.StatusBadRequest, "invalid_part", fmt.Sprintf("part %d is above the last part, %d", n, pinned), nil, nil)
		case pinned != 0 && n == pinned && (!last || plain != u.LastSize):
			return refusal(http.StatusConflict, "part_mismatch", fmt.Sprintf("part %d was signed as the last part", n), nil, nil)
		case pinned != 0 && last && n != pinned:
			return refusal(http.StatusConflict, "part_mismatch", fmt.Sprintf("part %d is the last part; an upload has one", pinned), nil, nil)
		case pinned == 0 && last:
			top := 0
			for k := range u.Pins {
				top = max(top, k)
			}
			if top > n {
				return refusal(http.StatusBadRequest, "invalid_part", fmt.Sprintf("part %d is signed, so %d can't be the last", top, n), nil, nil)
			}
			u.LastPart, u.LastSize = n, plain
		}
	}
	u.Pins[n] = [2]string{md5s, sha}
	u.Expires = later(u.Expires, d.after(30*time.Minute))
	md5b, _ := hex.DecodeString(md5s)
	headers := map[string]string{
		"content-length":       fmt.Sprint(u.stored(n)),
		"content-md5":          b64(md5b),
		"x-amz-content-sha256": sha,
	}
	query := map[string]string{"partNumber": fmt.Sprint(n), "uploadId": u.MPU}
	return jsonReply(http.StatusOK, d.Network.Sign(http.MethodPut, u.Physical, headers, query, signTTL), nil)
}

func (d *Direct) completeUpload(u *DirectUpload, req map[string]any) *reply {
	wrapped, _ := base64.StdEncoding.DecodeString(str(req["wrapped_key"]))
	opens := false
	if u.Status == "open" {
		_, err := d.Unwrap(u.ID, wrapped)
		opens = err == nil
	}
	d.sweep()
	switch u.Status {
	case "completed":
		return jsonReply(http.StatusOK, u.Result, map[string]string{"ETag": `"` + str(u.Result["etag"]) + `"`})
	case "aborted", "abandoned":
		if u.PreconditionFailed {
			return apiError(http.StatusPreconditionFailed, "precondition_failed", nil, nil)
		}
		return apiError(http.StatusConflict, "reservation_abandoned", nil, nil)
	}
	if u.Status == "open" {
		if kv, ok := integer(req["key_version"]); !ok || kv != int64(d.KeyVersion) {
			return invalid("wrapped to another key version")
		}
		if !opens {
			return apiError(http.StatusBadRequest, "bad_wrapped_key", nil, nil)
		}
		size, sizeOK := integer(req["size"])
		sha := str(req["sha256"])
		var count, want int64
		if u.Size < 0 {
			if u.LastPart == 0 {
				return refusal(http.StatusBadRequest, "invalid_part", "no part was signed as the last", nil, nil)
			}
			want = int64(u.LastPart-1)*u.PartSize + u.LastSize
			if !sizeOK || size != want {
				return invalid(fmt.Sprintf("size %v isn't what the parts make, %d", req["size"], want))
			}
			if !hex64.MatchString(sha) {
				return invalid("complete needs sha256")
			}
			count = int64(u.LastPart)
		} else {
			if (truthy(req["size"]) && size != u.Size) || (sha != "" && sha != u.SHA256) {
				return invalid("complete must repeat the size and sha256")
			}
			count = u.PartCount
		}
		if int64(len(u.Pins)) != count {
			return refusal(http.StatusBadRequest, "invalid_part", fmt.Sprintf("%d of %d parts were signed", len(u.Pins), count), nil, nil)
		}
		if u.Size < 0 {
			u.Size, u.SHA256, u.PartCount = want, sha, int64(u.LastPart)
		}
		u.Status = "completing"
		u.Wrapped = wrapped
	}
	var parts []PartRef
	var digests []byte
	for n := 1; int64(n) <= u.PartCount; n++ {
		parts = append(parts, PartRef{Number: n, MD5: u.Pins[n][0]})
		b, _ := hex.DecodeString(u.Pins[n][0])
		digests = append(digests, b...)
	}
	d.Network.CompleteMultipart(u.MPU, parts) // "no such upload" or "exists" are settled by the HEAD
	size, etag, ok := d.Network.Head(u.Physical)
	if !ok {
		u.Status = "abandoned"
		return apiError(http.StatusConflict, "reservation_abandoned", nil, nil)
	}
	if size != tws1.StoredSize(u.Size) || etag != fmt.Sprintf("%s-%d", md5hex(digests), u.PartCount) {
		u.Status = "abandoned"
		return apiError(http.StatusConflict, "upload_mismatch", nil, nil)
	}
	objs := d.api.Buckets[u.Bucket]
	if objs == nil {
		return apiError(http.StatusNotFound, "bucket_not_found", nil, nil)
	}
	cur := objs[u.Key]
	if (u.IfNoneMatch && cur != nil) || (u.IfMatch != "" && (cur == nil || (u.IfMatch != "*" && stripQuotes(u.IfMatch) != cur.ETag()))) {
		u.Status = "aborted"
		u.PreconditionFailed = true
		d.Network.DeleteBlob(u.Physical)
		return apiError(http.StatusPreconditionFailed, "precondition_failed", nil, nil)
	}
	o := &Object{ID: u.ID, Size: u.Size, SHA: u.SHA256, ContentType: u.ContentType, Metadata: u.Metadata,
		Parts: int(u.PartCount), Updated: wholeSeconds(d.now()), ClientEncrypted: true, Location: "blob",
		Format: FormatTWS1, Physical: u.Physical, StoredSize: size, Wrapped: u.Wrapped, KeyVersion: d.KeyVersion}
	objs[u.Key] = o
	u.Status = "completed"
	u.Result = d.api.ObjectJSON(u.Key, o)
	return jsonReply(http.StatusOK, u.Result, map[string]string{"ETag": `"` + str(u.Result["etag"]) + `"`})
}
