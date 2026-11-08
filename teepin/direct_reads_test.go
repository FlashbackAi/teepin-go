// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
	"github.com/FlashbackAi/teepin-go/teepin/internal/fakes"
	"github.com/FlashbackAi/teepin-go/teepin/internal/tws1"
)

// Reads straight from the storage network (TEEPIN-STORAGE-DESIGN.md sections 22.5 and 25): read sessions for every
// format and location, each byte verified before it is used, ranges, windows read in parallel, the version pinned,
// and every fault ending in the right bytes or the right error. The Python SDK's tests/test_direct_reads.py and the
// Node SDK's test/reads.test.mjs, ported, against the fakes (internal/fakes), with parts of 128 KiB and read windows
// of 300,000 bytes. The Go SDK doesn't upload directly yet, so objects are stored as a client stores them, through
// the fakes' routes.

var ctx = context.Background()

// --- every format and location -----------------------------------------------------

func TestDirect_APackedObjectReadsThroughItsNeedle(t *testing.T) {
	f, c := newDirect(t)
	data := smallObject(t, f, "small", 5000)
	r, err := c.Storage.GetObject(ctx, "photos", "small", nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, %v", len(got), err)
	}
	if r.Size != int64(len(data)) || r.TotalSize != int64(len(data)) || r.ContentType != "image/png" || r.ETag != sha256Hex(data) {
		t.Fatalf("info %+v", r.ObjectInfo)
	}
	if got, _, err := download(c, "small", nil); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download: %v", err)
	}
	if f.API.HitsOf("GET object/content") != 0 {
		t.Fatal("bytes came through the API")
	}
	reqs := f.Network.RequestsOf("")
	if len(reqs) == 0 {
		t.Fatal("nothing reached storage")
	}
	for _, q := range reqs {
		if b, _ := json.Marshal(q.Header); bytes.Contains(b, []byte(fakes.APIKey)) {
			t.Fatal("the API key reached storage")
		}
	}
}

func TestDirect_ABlobReadsInWindowsEachSegmentVerified(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	if got, err := getBytes(c, "big", nil); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read: %v", err)
	}
	var starts []int64
	for _, g := range gets(f) {
		starts = append(starts, startOf(g))
	}
	slices.Sort(starts)
	if starts[0] != 0 || len(starts) < 4 {
		t.Fatalf("starts %v", starts) // windows, each later one with its header first
	}
	if f.API.HitsOf("GET object/content") != 0 {
		t.Fatal("bytes came through the API")
	}
}

func TestDirect_ADownloadReadsWindowsInParallelAndMetersEachOnce(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	f.API.Do(func() { f.Direct.Metered = 0 })
	if got, _, err := download(c, "big", &DownloadOptions{Concurrency: 4}); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download: %v", err)
	}
	stored := tws1.StoredSize(blobSize)
	var headers int64
	for _, g := range gets(f) {
		if strings.HasPrefix(g.Header["range"], "bytes=0-71") {
			headers += 72
		}
	}
	var metered int64
	f.API.Do(func() { metered = f.Direct.Metered })
	// Every stored byte signed once, plus the first window's tail (signed whole, read up to its last whole segment)
	// and the 72-byte headers of the later windows.
	if metered < stored || metered > stored+headers+readWindow {
		t.Fatalf("metered %d of %d stored", metered, stored)
	}
}

func TestDirect_DownloadFileWritesOnlyVerifiedBytes(t *testing.T) {
	for _, partSize := range []int64{0, 64 * kib, 200 * kib} {
		t.Run(fmt.Sprint(partSize), func(t *testing.T) {
			f, c := newDirect(t)
			data := blob(t, f, "big", blobSize)
			path := filepath.Join(t.TempDir(), "out", "big.mp4")
			obj, err := c.Storage.DownloadFile(ctx, "photos", "big", path, &DownloadOptions{PartSize: partSize})
			if err != nil {
				t.Fatal(err)
			}
			got, _ := os.ReadFile(path)
			if !bytes.Equal(got, data) || obj.Size != blobSize || obj.ChecksumSHA256 != sha256Hex(data) || obj.PartCount != 0 {
				t.Fatalf("%d bytes, %+v", len(got), obj)
			}
			entries, _ := os.ReadDir(filepath.Dir(path))
			if len(entries) != 1 || entries[0].Name() != "big.mp4" {
				t.Fatalf("left %v", entries)
			}
			if st, _ := os.Stat(path); obj.UploadedAt == nil || !st.ModTime().Equal(*obj.UploadedAt) {
				t.Fatalf("mtime %v, uploaded %v", st.ModTime(), obj.UploadedAt)
			}
		})
	}
}

func TestDirect_DownloadStreamWritesInOrder_AndProgressEndsAtTheSize(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	var buf bytes.Buffer
	var mu sync.Mutex
	var last [2]int64
	obj, err := c.Storage.DownloadStream(ctx, "photos", "big", &buf, &DownloadOptions{Progress: func(d, total int64) {
		mu.Lock()
		last = [2]int64{d, total}
		mu.Unlock()
	}})
	if err != nil || !bytes.Equal(buf.Bytes(), data) || last != [2]int64{blobSize, blobSize} || obj.Size != blobSize {
		t.Fatalf("%v: %d bytes, progress %v", err, buf.Len(), last)
	}
	last = [2]int64{}
	if got, _, err := download(c, "big", &DownloadOptions{Progress: func(d, total int64) {
		mu.Lock()
		last = [2]int64{d, total}
		mu.Unlock()
	}}); err != nil || !bytes.Equal(got, data) || last != [2]int64{blobSize, blobSize} {
		t.Fatalf("download: %v, progress %v", err, last)
	}
}

func TestDirect_GetObjectsBodyComesInOrder_ASegmentAtMostEachRead(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	r, err := c.Storage.GetObject(ctx, "photos", "big", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	p := make([]byte, 1<<20)
	var pos int
	for {
		n, err := r.Read(p)
		if n > tws1.SegmentSize {
			t.Fatalf("a read of %d bytes: each piece is handed out as soon as it is verified", n)
		}
		if !bytes.Equal(p[:n], data[pos:pos+n]) {
			t.Fatalf("bytes at %d", pos)
		}
		pos += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if pos != blobSize {
		t.Fatalf("%d bytes", pos)
	}
}

func TestDirect_AServerEncryptedObjectReadsWholeThenRanged(t *testing.T) {
	f, c := newDirect(t)
	data := randomData(3000)
	f.Direct.PutServerObject("photos", "old", data)
	if got, _, err := download(c, "old", nil); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download: %v", err)
	}
	r, err := c.Storage.GetObject(ctx, "photos", "old", &GetObjectOptions{Range: &ByteRange{Start: 100, End: 199}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(got, data[100:200]) || r.Size != 100 || r.TotalSize != 3000 {
		t.Fatalf("%v: %d bytes, %+v", err, len(got), r.ObjectInfo)
	}
}

func TestDirect_ALegacyPlainBlobReadsAsStored(t *testing.T) {
	f, c := newDirect(t)
	data := randomData(700_000)
	f.Direct.PutPlainBlob("photos", "legacy", data, 0)
	if got, _, err := download(c, "legacy", &DownloadOptions{Concurrency: 3}); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download: %v", err)
	}
	if got, err := getBytes(c, "legacy", &GetObjectOptions{Range: &ByteRange{Start: 650_000, End: -1}}); err != nil || !bytes.Equal(got, data[650_000:]) {
		t.Fatalf("range: %v", err)
	}
}

func TestDirect_ALegacyObjectInPartsIsNotCheckedAgainstItsComposite(t *testing.T) {
	f, c := newDirect(t)
	data := randomData(5000)
	o := f.Direct.PutPlainBlob("photos", "legacy", data, 3)
	f.API.Do(func() { o.SHA = strings.Repeat("0", 64) }) // a composite: no whole-object SHA-256 to check
	got, obj, err := download(c, "legacy", nil)
	if err != nil || !bytes.Equal(got, data) || obj.PartCount != 3 {
		t.Fatalf("%v, %+v", err, obj)
	}
}

// A staged object's bytes are on the API, which no longer serves them (section 23.5): every read says so, and none
// falls back to the API's content route.
func TestDirect_AStagedObjectIsAnError_NeverReadThroughTheAPI(t *testing.T) {
	f, c := newDirect(t)
	f.API.PutObject("photos", "staged", []byte("still staged"), "")
	if _, err := getBytes(c, "staged", nil); !errors.Is(err, ErrObjectStaged) || !strings.Contains(err.Error(), "on the API: staged (") {
		t.Fatalf("get: %v", err)
	}
	if _, _, err := download(c, "staged", nil); !errors.Is(err, ErrObjectStaged) {
		t.Fatalf("download: %v", err)
	}
	if _, err := c.Storage.DownloadStream(ctx, "photos", "staged", io.Discard, nil); !errors.Is(err, ErrObjectStaged) {
		t.Fatalf("stream: %v", err)
	}
	if _, err := c.Storage.OpenObject(ctx, "photos", "staged", nil); !errors.Is(err, ErrObjectStaged) {
		t.Fatalf("open: %v", err)
	}
	if f.API.HitsOf("GET object/content") != 0 || len(gets(f)) != 0 {
		t.Fatal("a staged object was read")
	}
}

func TestDirect_AnEmptyObjectReadsAsNothing(t *testing.T) {
	f, c := newDirect(t)
	// Stored as the SDKs store an empty object: a catalog row through the A2 route, no bytes anywhere (section 23.5).
	if a, err := f.Call(http.MethodPost, "/buckets/photos/object/empty", map[string]any{"key": "empty"}, nil); err != nil || a.Status != http.StatusOK {
		t.Fatalf("A2: %v %v", a, err)
	}
	if got, err := getBytes(c, "empty", nil); err != nil || len(got) != 0 {
		t.Fatalf("get: %d %v", len(got), err)
	}
	got, obj, err := download(c, "empty", nil)
	if err != nil || len(got) != 0 || obj.ETag != fakes.EmptySHA256 || obj.Size != 0 {
		t.Fatalf("download: %d %v %+v", len(got), err, obj)
	}
	var buf bytes.Buffer
	if _, err := c.Storage.DownloadStream(ctx, "photos", "empty", &buf, nil); err != nil || buf.Len() != 0 {
		t.Fatalf("stream: %d %v", buf.Len(), err)
	}
	file, err := c.Storage.OpenObject(ctx, "photos", "empty", nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := file.ReadAt(make([]byte, 10), 0); file.Size() != 0 || n != 0 || err != io.EOF {
		t.Fatalf("%d %v", n, err)
	}
	file.Close()
	if len(gets(f)) != 0 {
		t.Fatal("an empty object was fetched")
	}
}

// --- ranges --------------------------------------------------------------------------

func TestDirect_ARangeOfABlob(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	for _, rg := range [][2]int64{
		{0, 0},
		{0, 65535},
		{65535, 65536},
		{65536, 131071},
		{299_000, 600_001}, // across windows
		{blobSize - 1, -1},
		{123_456, -1},
		{5, blobSize + 1000}, // an end past the object is its end
	} {
		t.Run(fmt.Sprint(rg), func(t *testing.T) {
			want := data[rg[0]:]
			if rg[1] >= 0 && rg[1]+1 < blobSize {
				want = data[rg[0] : rg[1]+1]
			}
			r, err := c.Storage.GetObject(ctx, "photos", "big", &GetObjectOptions{Range: &ByteRange{Start: rg[0], End: rg[1]}})
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(r)
			r.Close()
			if err != nil || !bytes.Equal(got, want) || r.Size != int64(len(want)) || r.TotalSize != blobSize {
				t.Fatalf("%v: %d bytes of %d, %+v", err, len(got), len(want), r.ObjectInfo)
			}
		})
	}
}

func TestDirect_ARangeSignsOnlyWhatItReads(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	f.API.Do(func() { f.Direct.Metered = 0 })
	if _, err := getBytes(c, "big", &GetObjectOptions{Range: &ByteRange{Start: 900_000, End: 900_099}}); err != nil {
		t.Fatal(err)
	}
	var metered int64
	f.API.Do(func() { metered = f.Direct.Metered })
	if metered >= 3*65552 {
		t.Fatalf("metered %d: its segment and header, not the windows before it", metered)
	}
}

func TestDirect_ARangePastTheEndIsRefused(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	_, err := c.Storage.GetObject(ctx, "photos", "big", &GetObjectOptions{Range: &ByteRange{Start: blobSize, End: -1}})
	var ae *APIError
	if !errors.As(err, &ae) || ae.StatusCode != http.StatusRequestedRangeNotSatisfiable || ae.Code != "invalid_range" {
		t.Fatal(err)
	}
}

func TestDirect_ReadingInSmallPieces(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	for _, n := range []int{7777, 10_000} {
		r, err := c.Storage.GetObject(ctx, "photos", "big", nil)
		if err != nil {
			t.Fatal(err)
		}
		var got []byte
		p := make([]byte, n)
		for {
			k, err := r.Read(p)
			if k > n {
				t.Fatalf("read %d into %d", k, n)
			}
			got = append(got, p[:k]...)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		r.Close()
		if !bytes.Equal(got, data) {
			t.Fatalf("in pieces of %d: %d bytes", n, len(got))
		}
	}
}

// --- conditions and details ---------------------------------------------------------------

func TestDirect_IfNoneMatchSignsNothingOnAMatch(t *testing.T) {
	f, c := newDirect(t)
	data := smallObject(t, f, "small", 5000)
	if _, err := c.Storage.GetObject(ctx, "photos", "small", &GetObjectOptions{IfNoneMatch: sha256Hex(data)}); !errors.Is(err, ErrNotModified) {
		t.Fatal(err)
	}
	if f.API.HitsOf("POST object/read") != 0 {
		t.Fatal("a match signed a read")
	}
	if got, err := getBytes(c, "small", &GetObjectOptions{IfNoneMatch: "other"}); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
}

func TestDirect_IfMatchIsCheckedBeforeAnyByteIsFetched(t *testing.T) {
	f, c := newDirect(t)
	smallObject(t, f, "small", 5000)
	if _, err := c.Storage.GetObject(ctx, "photos", "small", &GetObjectOptions{IfMatch: strings.Repeat("0", 64)}); !errors.Is(err, ErrObjectChanged) {
		t.Fatal(err)
	}
	if len(gets(f)) != 0 {
		t.Fatal("bytes were fetched")
	}
}

func TestDirect_HeadObjectIsBuiltFromTheObjectsDetails(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	info, err := c.Storage.HeadObject(ctx, "photos", "big")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != blobSize || info.TotalSize != blobSize || info.ContentType != "video/mp4" || info.ETag != sha256Hex(data) {
		t.Fatalf("%+v", info)
	}
	if info.LastModified.IsZero() || info.LastModified.Nanosecond() != 0 {
		t.Fatalf("last modified %v", info.LastModified)
	}
	if f.API.HitsOf("HEAD object/content") != 0 {
		t.Fatal("HEAD went to the content route")
	}
	// Last modified is the object's update time, or, when it has none, its upload time, to the second.
	updated := map[string]any{"key": "big", "size_bytes": 1, "etag": "e", "updated_at": "2026-02-03T04:05:06.789Z",
		"uploaded_at": "2026-01-01T00:00:00Z"}
	uploaded := map[string]any{"key": "big", "size_bytes": 1, "etag": "e", "uploaded_at": "2026-01-01T00:00:00Z"}
	f.API.Do(func() {
		f.API.Canned["GET object"] = []fakes.Canned{{Status: 200, Body: updated}, {Status: 200, Body: uploaded}}
	})
	for _, want := range []string{"2026-02-03T04:05:06Z", "2026-01-01T00:00:00Z"} {
		info, err := c.Storage.HeadObject(ctx, "photos", "big")
		if err != nil || info.LastModified.Format(time.RFC3339) != want {
			t.Fatalf("last modified %v, want %s: %v", info.LastModified, want, err)
		}
	}
}

// --- the version pinned ---------------------------------------------------------------------

func TestDirect_AReplacedObjectStopsADownload(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	var once sync.Once
	engine(c).Hooks.Window = func(s0 *direct.Session, off, length int64) (int64, int64) {
		once.Do(func() { f.API.Do(func() { f.API.Bucket("photos")["big"].ID = beef }) })
		return off, length
	}
	if _, _, err := download(c, "big", &DownloadOptions{Concurrency: 1}); !errors.Is(err, ErrObjectChanged) {
		t.Fatal(err)
	}
}

// --- faults ---------------------------------------------------------------------------------

func TestDirect_AWindowThatBreaksOffGoesOnFromItsLastVerifiedSegment(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	f.Network.Do(func() { f.Network.Cut = 1 })
	if got, _, err := download(c, "big", &DownloadOptions{Concurrency: 1}); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
	var resumed []int64
	for _, g := range gets(f) {
		if s := startOf(g); s > 0 && (s-72)%65552 == 0 {
			resumed = append(resumed, s)
		}
	}
	if len(resumed) == 0 || slices.Min(resumed) >= readWindow {
		t.Fatalf("resumed at %v: inside the first window, not from its start", resumed)
	}
}

func TestDirect_PassingFailuresAreRetried(t *testing.T) {
	for _, fault := range []string{"fail", "drop", "reject", "lose"} {
		t.Run(fault, func(t *testing.T) {
			f, c := newDirect(t)
			data := blob(t, f, "big", blobSize)
			table := map[string]map[string]int{"fail": f.Network.Fail, "drop": f.Network.Drop, "reject": f.Network.Reject, "lose": f.Network.Lose}[fault]
			f.Network.Do(func() { table["GET"] = 2 })
			if got, _, err := download(c, "big", nil); err != nil || !bytes.Equal(got, data) {
				t.Fatal(err)
			}
			f.Network.Do(func() {
				if table["GET"] != 0 {
					t.Errorf("%d faults left", table["GET"])
				}
			})
		})
	}
}

// Step 8's addition to the fault matrix (section 22.9): a response that stops half way and drops its connection, on
// each format and each way of reading, is fetched again from the last verified byte.
func TestDirect_AReadCutOffHalfWayStillReturnsTheRightBytes(t *testing.T) {
	for _, format := range []string{"packed", "blob"} {
		for _, way := range []string{"getObject", "range", "download", "openObject"} {
			t.Run(format+"/"+way, func(t *testing.T) {
				f, c := newDirect(t)
				key := "small"
				var data []byte
				if format == "packed" {
					data = smallObject(t, f, key, 5000)
				} else {
					key = "big"
					data = blob(t, f, key, blobSize)
				}
				f.Network.Do(func() { f.Network.Cut = 1 })
				var got []byte
				var err error
				switch way {
				case "getObject":
					got, err = getBytes(c, key, nil)
				case "range":
					a, b := int64(len(data)/3), int64(len(data)*2/3)
					got, err = getBytes(c, key, &GetObjectOptions{Range: &ByteRange{Start: a, End: b}})
					data = data[a : b+1]
				case "download":
					got, _, err = download(c, key, &DownloadOptions{Concurrency: 1})
				default:
					var file *ObjectFile
					if file, err = c.Storage.OpenObject(ctx, "photos", key, nil); err == nil {
						got = make([]byte, file.Size())
						_, err = file.ReadAt(got, 0)
						file.Close()
					}
				}
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("%v: %d bytes of %d", err, len(got), len(data))
				}
				f.Network.Do(func() {
					if f.Network.Cut != 0 {
						t.Error("the cut didn't happen")
					}
				})
			})
		}
	}
}

func TestDirect_AFlippedByteIsRetriedOnce(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	f.Network.Do(func() { f.Network.Rot = 1 })
	if got, err := getBytes(c, "big", nil); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
	small := smallObject(t, f, "small", 5000)
	f.Network.Do(func() { f.Network.Rot = 1 })
	if got, _, err := download(c, "small", nil); err != nil || !bytes.Equal(got, small) {
		t.Fatal(err)
	}
}

// A check that fails again where it failed is never retried past once: flipped bytes in two places with good ones
// between are two passing faults, but a damaged segment fails for good.
func TestDirect_DamageInStorageIsADataIntegrityError(t *testing.T) {
	f, c := newDirect(t)
	var failures atomic.Int32
	engine(c).Hooks.Failed = func(err error) {
		if failures.Add(1) >= 10 {
			t.Error("a damaged segment retried without end")
		}
	}
	blob(t, f, "big", blobSize)
	f.Damage("photos", "big", 500_000)
	if _, err := getBytes(c, "big", nil); reasonOf(err) != "segment" {
		t.Fatal(err)
	}
	if n := failures.Load(); n != 2 {
		t.Fatalf("%d failures: the damaged segment, and once more from it", n)
	}
	smallObject(t, f, "small", 5000)
	f.Network.Do(func() { f.Network.Rot = 2 })
	if _, _, err := download(c, "small", nil); reasonOf(err) != "needle" {
		t.Fatal(err)
	}
}

// The commitment is checked before any segment is used (section 22.5's order), from the first window's header or a
// later window's own: damage there is a commitment that doesn't match, not a segment.
func TestDirect_ADamagedKeyCommitmentIsRefused(t *testing.T) {
	for _, rg := range []*ByteRange{nil, {Start: 900_000, End: 900_099}} {
		f, c := newDirect(t)
		blob(t, f, "big", blobSize)
		f.Damage("photos", "big", 60)
		if _, err := getBytes(c, "big", &GetObjectOptions{Range: rg}); reasonOf(err) != "commitment" {
			t.Fatalf("range %v: %v", rg, err)
		}
	}
}

func TestDirect_BytesFlippedInTwoWindowsAreTwoPassingFaults(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	f.Network.Do(func() { f.Network.Rot = 2 })
	if got, err := getBytes(c, "big", nil); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
}

func TestDirect_AFailedDownloadLeavesNoFile(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	f.Damage("photos", "big", 500_000)
	dir := filepath.Join(t.TempDir(), "dl")
	var die *DataIntegrityError
	if _, err := c.Storage.DownloadFile(ctx, "photos", "big", filepath.Join(dir, "big.mp4"), &DownloadOptions{Concurrency: 1}); !errors.As(err, &die) {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("left %v", entries)
	}
}

func TestDirect_AWrongChecksumIsTriedOnceMoreThenRefused(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	f.API.Do(func() { f.API.Bucket("photos")["big"].SHA = strings.Repeat("0", 64) })
	var firsts atomic.Int32
	engine(c).Hooks.Session = func(bucket, key string) { firsts.Add(1) }
	_, _, err := download(c, "big", &DownloadOptions{Concurrency: 4})
	if !errors.Is(err, ErrChecksumMismatch) || reasonOf(err) != "checksum" {
		t.Fatal(err)
	}
	if firsts.Load() != 2 {
		t.Fatalf("%d first sessions: the whole download, then once more", firsts.Load())
	}
	if _, err := getBytes(c, "big", nil); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatal(err)
	}
	smallObject(t, f, "small", 5000) // a packed object, checked whole
	f.API.Do(func() { f.API.Bucket("photos")["small"].SHA = strings.Repeat("0", 64) })
	if _, _, err := download(c, "small", nil); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatal(err)
	}
	if _, err := getBytes(c, "small", nil); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatal(err)
	}
	want, _ := f.Direct.ReadBack("photos", "small")
	if got, err := getBytes(c, "small", &GetObjectOptions{Range: &ByteRange{Start: 0, End: 99}}); err != nil || !bytes.Equal(got, want[:100]) {
		t.Fatalf("a range isn't checked against the whole SHA-256: %v", err)
	}
}

func TestDirect_BytesStorageNoLongerHasAreSignedOnceMoreThenNotFound(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	f.Network.DeleteBlob(f.API.Object("photos", "big").Physical)
	if _, _, err := download(c, "big", &DownloadOptions{Concurrency: 1}); !IsNotFound(err) {
		t.Fatal(err)
	}
	if n := f.API.HitsOf("POST object/read"); n != 2 {
		t.Fatalf("%d sessions: the first, and one more after the 404", n)
	}
	if engine(c).Keys.Len() != 0 {
		t.Fatal("the cached key was kept")
	}
}

// A server pack compacted between the session and the fetch: the 404 is signed again, and the new session points at
// the new place.
func TestDirect_AMovedPackIsFoundBySigningAgain(t *testing.T) {
	f, c := newDirect(t)
	data := smallObject(t, f, "small", 5000)
	o := f.API.Object("photos", "small")
	old := o.Physical
	f.Network.SetBlob("p/moved", f.Network.Blob(old))
	var moved atomic.Bool
	engine(c).Hooks.Open = func(ctx context.Context, r *direct.Signed) error {
		if moved.CompareAndSwap(false, true) {
			f.Network.DeleteBlob(old)
			f.API.Do(func() { o.Physical = "p/moved" })
		}
		return nil
	}
	if got, _, err := download(c, "small", nil); err != nil || !bytes.Equal(got, data) || !moved.Load() {
		t.Fatal(err)
	}
}

func TestDirect_ASignatureAboutToExpireIsSignedAgain(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	var seen atomic.Int32
	engine(c).Hooks.Fresh = func(r *direct.Signed, fresh bool) bool {
		return seen.Add(1) > 1 && fresh // the first one is about to expire
	}
	if got, _, err := download(c, "big", &DownloadOptions{Concurrency: 1}); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
	if seen.Load() < 2 || f.API.HitsOf("POST object/read") < 2 {
		t.Fatalf("%d checks, %d sessions", seen.Load(), f.API.HitsOf("POST object/read"))
	}
}

func TestDirect_SignaturesThatAlwaysExpireSayToCheckTheClock(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	var seen atomic.Int32
	engine(c).Hooks.Fresh = func(*direct.Signed, bool) bool {
		seen.Add(1)
		return false
	}
	if _, _, err := download(c, "big", &DownloadOptions{Concurrency: 1}); err == nil || !strings.Contains(err.Error(), "clock") {
		t.Fatal(err)
	}
	if int(seen.Load()) != c.maxRetries+1 {
		t.Fatalf("signed %d times: as often as a passing failure is tried, no more", seen.Load())
	}
}

func TestDirect_AWindowSignedFromElsewhereIsRefused(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	engine(c).Hooks.Window = func(s0 *direct.Session, off, length int64) (int64, int64) { return off + 1, length }
	if _, err := getBytes(c, "big", nil); reasonOf(err) != "window" {
		t.Fatal(err)
	}
}

func TestDirect_ADownloadSplitsIntoTheServersWindows(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "big", blobSize)
	var mu sync.Mutex
	var ranges [][2]int64
	engine(c).Hooks.Plaintext = func(s *direct.Session, dek []byte, a, b int64) {
		mu.Lock()
		ranges = append(ranges, [2]int64{a, b})
		mu.Unlock()
	}
	if got, _, err := download(c, "big", &DownloadOptions{Concurrency: 4}); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
	var want [][2]int64
	for a := int64(0); a < blobSize; a += plainWindow {
		want = append(want, [2]int64{a, min(a+plainWindow, blobSize)})
	}
	slices.SortFunc(ranges, func(x, y [2]int64) int { return int(x[0] - y[0]) })
	if !slices.Equal(ranges, want) {
		t.Fatalf("ranges %v", ranges)
	}
}

func TestDirect_ADeploymentWithoutDirectReadsIsALoudError(t *testing.T) {
	api := fakes.StartAPI() // no direct routes: as a deployment with direct uploads off
	defer api.Close()
	api.PutObject("photos", "k", []byte("x"), "")
	c := NewClient(Config{BaseURL: api.URL, APIKey: fakes.APIKey, StateDir: t.TempDir()})
	c.sleep = func(ctx context.Context, d time.Duration) error { return ctx.Err() }
	_, err := c.Storage.GetObject(ctx, "photos", "k", nil)
	var ae *APIError
	if !errors.As(err, &ae) || ae.StatusCode != http.StatusNotImplemented || api.HitsOf("GET object/content") != 0 {
		t.Fatalf("never a silent fallback: %v", err)
	}
}

// A 401 or 403 means the signature failed, not the key: the window is signed again and the cached key kept
// (section 22.5, as corrected by profile 1's M5 review).
func TestDirect_ARefusedSignatureIsSignedAgainKeepingTheCachedKey(t *testing.T) {
	f, c := newDirect(t)
	data := smallObject(t, f, "small", 5000)
	calls := includeKeyCalls(c)
	f.Network.Do(func() { f.Network.Reject["GET"] = 1 })
	if got, _, err := download(c, "small", nil); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
	if got, _, err := download(c, "small", nil); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
	if got := calls(); !slices.Equal(got, []bool{true, false, false}) || engine(c).Keys.Len() != 1 {
		t.Fatalf("include_key %v, %d keys held", got, engine(c).Keys.Len())
	}
}

// A window read again from the start must carry the header the read began with: another one is refused, even
// though the segments would still open with the header first read.
func TestDirect_AHeaderThatChangesBetweenWindowsIsRefused(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	f.Network.Do(func() { f.Network.Cut = 1 }) // the first window breaks off inside segment 0, after the header
	var once sync.Once
	engine(c).Hooks.Failed = func(err error) { once.Do(func() { f.Damage("photos", "big", 10) }) }
	_, err := getBytes(c, "big", &GetObjectOptions{Range: &ByteRange{Start: 0, End: 0}})
	if reasonOf(err) != "header" {
		t.Fatal(err)
	}
}

// A signature within a minute of its expiry is never used (section 22.5): here every one the control plane signs is,
// as on a computer whose clock runs 14.5 minutes fast.
func TestDirect_ASignatureWithinAMinuteOfItsExpiryIsNeverUsed(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	f.Network.Do(func() { f.Network.Skew = -(15*time.Minute - 30*time.Second) })
	if _, err := getBytes(c, "big", nil); err == nil || !strings.Contains(err.Error(), "clock") {
		t.Fatal(err)
	}
	if len(gets(f)) != 0 {
		t.Fatal("a signature about to expire was used")
	}
}

// A window that holds no whole segment would be asked for again without end: it is refused as a bad window.
func TestDirect_AWindowHoldingNoWholeSegmentIsRefused(t *testing.T) {
	f, c := newDirect(t)
	blob(t, f, "big", blobSize)
	engine(c).Hooks.Window = func(s0 *direct.Session, off, length int64) (int64, int64) { return off, 1 }
	done := make(chan error, 1)
	go func() {
		_, err := getBytes(c, "big", &GetObjectOptions{Range: &ByteRange{Start: 900_000, End: 900_099}})
		done <- err
	}()
	select {
	case err := <-done:
		if reasonOf(err) != "window" {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a window with no whole segment was asked for without end")
	}
}

// An object that ends on a segment boundary: its last segment is full, and still the one marked last.
func TestDirect_ABlobOfWholeSegmentsReads(t *testing.T) {
	f, c := newDirect(t)
	data := blob(t, f, "whole", 4*tws1.SegmentSize)
	if got, err := getBytes(c, "whole", nil); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
	last := int64(len(data))
	if got, err := getBytes(c, "whole", &GetObjectOptions{Range: &ByteRange{Start: last - 10, End: -1}}); err != nil || !bytes.Equal(got, data[last-10:]) {
		t.Fatal(err)
	}
	if got, _, err := download(c, "whole", &DownloadOptions{Concurrency: 2}); err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
}
