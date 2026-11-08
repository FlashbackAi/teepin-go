// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

const (
	// DefaultLinkExpiry is how long a share link works unless
	// CreateLinkOptions.ExpiresIn says otherwise.
	DefaultLinkExpiry = 15 * time.Minute
	// MaxLinkExpiry is the longest a share link may work.
	MaxLinkExpiry = 24 * time.Hour
)

// The dispositions of a share link.
const (
	// LinkInline: the page shows the object when it can.
	LinkInline = "inline"
	// LinkAttachment: the page only downloads it.
	LinkAttachment = "attachment"
)

// CreateLinkOptions are the options of CreateLink.
type CreateLinkOptions struct {
	// ExpiresIn is how long the link works: 1 second to MaxLinkExpiry (24
	// hours), in whole seconds (a fraction is dropped). Zero means the
	// default, DefaultLinkExpiry (15 minutes); a negative value is refused.
	ExpiresIn time.Duration
	// Disposition is LinkInline (the default) or LinkAttachment.
	Disposition string
}

// ShareLink is a share link to one object.
type ShareLink struct {
	// URL is the console page anyone with the link opens without signing in.
	URL string `json:"url"`
	// ExpiresAt is when it stops working.
	ExpiresAt time.Time `json:"expires_at"`
}

// CreateLink makes a share link to one object (TEEPIN-STORAGE-DESIGN.md
// section 26): a page on the console that anyone with the link opens without
// signing in, and that decrypts the object in their browser, so its bytes
// never pass through Teepin's servers. The object must exist. Links are for
// browsers; programs read objects with GetObject and the rest. There are no
// upload links.
//
// ExpiresIn and Disposition are checked here first: a value out of range is
// an error, and nothing is sent.
func (s *StorageService) CreateLink(ctx context.Context, bucket, key string, opts *CreateLinkOptions) (*ShareLink, error) {
	var o CreateLinkOptions
	if opts != nil {
		o = *opts
	}
	if o.ExpiresIn == 0 {
		o.ExpiresIn = DefaultLinkExpiry
	}
	if o.ExpiresIn < time.Second || o.ExpiresIn > MaxLinkExpiry {
		return nil, fmt.Errorf("teepin: a link's ExpiresIn is 1 second to %s, not %s", MaxLinkExpiry, o.ExpiresIn)
	}
	if o.Disposition == "" {
		o.Disposition = LinkInline
	}
	if o.Disposition != LinkInline && o.Disposition != LinkAttachment {
		return nil, errors.New(`teepin: a link's Disposition is "inline" or "attachment"`)
	}
	body := map[string]any{"key": key, "ttl_seconds": int64(o.ExpiresIn / time.Second), "disposition": o.Disposition}
	var link ShareLink
	r := &request{method: http.MethodPost, path: bucketPath(bucket) + "/object/link", json: body, mode: retrySafe}
	if err := s.client.call(ctx, r, &link); err != nil {
		return nil, err
	}
	return &link, nil
}
