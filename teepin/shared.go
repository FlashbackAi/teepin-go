// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"sync"

	"github.com/FlashbackAi/teepin-go/teepin/internal/direct"
)

// One direct-upload engine per process (TEEPIN-STORAGE-DESIGN.md section 28.23).
//
// Packs fill only across the saves one engine sees. A program that makes a client per goroutine or per request would
// otherwise have an engine each, and so a queue each of small leftovers. So every client in this process with the same
// API key, the same API address and the same upload settings shares one engine: its queue, adaptive limit, speed
// meter, memory cap and packs in flight.
//
// The engine sends through a client of its own, built from the first client's settings, so closing one client never
// touches the connections of another's saves. Each client holds an uploadHold. Close lets go of it, and so does the
// client's being garbage collected (runtime.AddCleanup); the last hold closes the engine's client. The registry is
// keyed by a SHA-256 digest and never holds the key.
//
// A client given its own Config.HTTPClient doesn't share: its caller chose that transport (a proxy, certificates), so
// its saves keep an engine of their own, as before.

type sharedUploads struct {
	up     *direct.Uploads
	client *Client // the engine's own
	refs   int
	close  func() // closes the engine's own client (tests watch it)
}

var (
	sharedMu sync.Mutex
	shared   = map[string]*sharedUploads{}
)

// uploadsKey is the registry key of c's upload engine: SHA-256 over the key, the address and every setting the engine
// reads, each prefixed with its length.
func uploadsKey(c *Client) string {
	h := sha256.New()
	for _, p := range []string{
		c.apiKey,
		c.baseURL,
		c.projectID,
		c.stateDir,
		strconv.FormatInt(int64(c.uploadWait), 10),
		strconv.Itoa(c.uploadConcurrency),
		strconv.FormatInt(int64(c.timeout), 10),
		strconv.Itoa(c.maxRetries),
		c.userAgent,
	} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// uploadHold is a client's hold on a shared engine. release is idempotent; the last one closes the engine's client.
type uploadHold struct {
	key   string
	entry *sharedUploads
}

func (h *uploadHold) release() {
	sharedMu.Lock()
	e := h.entry
	h.entry = nil
	last := false
	if e != nil {
		e.refs--
		if e.refs == 0 {
			last = true
			if shared[h.key] == e {
				delete(shared, h.key)
			}
		}
	}
	sharedMu.Unlock()
	if last {
		e.close()
	}
}

// holdUploads is the engine c's uploads go through, and c's hold on it (nil for an engine of its own).
func holdUploads(c *Client) (*direct.Uploads, *uploadHold) {
	if c.ownHTTP {
		return uploadEngine(c), nil
	}
	key := uploadsKey(c)
	sharedMu.Lock()
	defer sharedMu.Unlock()
	e := shared[key]
	if e == nil {
		ec := c.engineClient()
		e = &sharedUploads{up: uploadEngine(ec), client: ec, close: func() {
			_ = ec.Close()
			ec.httpClient.CloseIdleConnections()
		}}
		shared[key] = e
	}
	e.refs++
	return e.up, &uploadHold{key: key, entry: e}
}

// engineClient is a client with c's settings and transport defaults, for a shared upload engine to send through.
func (c *Client) engineClient() *Client {
	ec := NewClient(Config{BaseURL: c.baseURL, APIKey: c.apiKey, ProjectID: c.projectID, Timeout: c.timeout,
		StateDir: c.stateDir, UploadConcurrency: c.uploadConcurrency})
	// The resolved values, as they are: Config's zero means "the default", and these may be 0 or none.
	ec.userAgent, ec.maxRetries, ec.uploadWait, ec.keyCacheTTL = c.userAgent, c.maxRetries, c.uploadWait, c.keyCacheTTL
	ec.sleep = c.sleep // the first client's hook (tests skip the waits)
	return ec
}

// sharedUploadsCount is how many shared engines this process holds (tests).
func sharedUploadsCount() int {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	return len(shared)
}
