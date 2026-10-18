// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package teepin

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// The rules of uploads in parts.
const (
	// MinPartSize is the smallest part but the last.
	MinPartSize = 5 << 20
	// MaxPartSize is the largest part.
	MaxPartSize = 5 << 30
	// MaxParts is the most parts of one upload.
	MaxParts = 10000
	// MaxObjectSize is the largest object.
	MaxObjectSize = 5 << 40
	// MaxPutSize is the largest object PutObject sends in one request.
	MaxPutSize = 5 << 30
)

// Upload is an upload in parts that has not been completed.
type Upload struct {
	ID          string            `json:"upload_id"`
	Key         string            `json:"key"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	// ExpiresAt is when it is cancelled if not completed (7 days).
	ExpiresAt time.Time `json:"expires_at"`
	// Parts is filled by ListParts.
	Parts []Part `json:"parts,omitempty"`
}

// Part is a part the server has received.
type Part struct {
	Number int   `json:"part_number"`
	Size   int64 `json:"size_bytes"`
	// ETag is the MD5 of the part, in hex; CompleteUpload needs it.
	ETag       string    `json:"etag"`
	UploadedAt time.Time `json:"uploaded_at"`
}

// CompletedPart names a part for CompleteUpload.
type CompletedPart struct {
	Number int    `json:"part_number"`
	ETag   string `json:"etag"`
}

// CreateUploadOptions are the options of CreateUpload.
type CreateUploadOptions struct {
	ContentType string
	Metadata    map[string]string
}

func uploadPath(bucket, id string) string {
	return bucketPath(bucket) + "/uploads/" + url.PathEscape(id)
}

// CreateUpload starts an upload in parts. Upload does all of it for you.
func (s *StorageService) CreateUpload(ctx context.Context, bucket, key string, opts *CreateUploadOptions) (*Upload, error) {
	body := map[string]any{"key": key}
	if opts != nil {
		if opts.ContentType != "" {
			body["content_type"] = opts.ContentType
		}
		if len(opts.Metadata) > 0 {
			body["metadata"] = opts.Metadata
		}
	}
	var up Upload
	if err := s.client.call(ctx, &request{method: http.MethodPost, path: bucketPath(bucket) + "/uploads", json: body, mode: retryUnsent}, &up); err != nil {
		return nil, err
	}
	return &up, nil
}

// UploadPart sends part number (1 to 10000) of an upload, reading size bytes
// from body. Sending a number again replaces that part. It is retried after
// a passing failure, rereading body from the start.
func (s *StorageService) UploadPart(ctx context.Context, bucket, uploadID string, number int, body io.ReaderAt, size int64) (*Part, error) {
	return s.uploadPart(ctx, bucket, uploadID, number, body, size, nil)
}

// uploadPart is UploadPart with a hook told of each attempt's reader, so a
// progress count can be undone when an attempt fails.
func (s *StorageService) uploadPart(ctx context.Context, bucket, uploadID string, number int, body io.ReaderAt, size int64,
	attempt func() io.Reader) (*Part, error) {
	r := &request{method: http.MethodPut, path: uploadPath(bucket, uploadID) + "/parts/" + strconv.Itoa(number),
		size: size, transfer: true, mode: retrySafe}
	r.body = func() (io.Reader, error) {
		if attempt != nil {
			return attempt(), nil
		}
		return io.NewSectionReader(body, 0, size), nil
	}
	var p Part
	if err := s.client.call(ctx, r, &p); err != nil {
		return nil, err
	}
	p.ETag = unquoteETag(p.ETag)
	return &p, nil
}

// ListParts returns an upload and the parts received so far, in order.
func (s *StorageService) ListParts(ctx context.Context, bucket, uploadID string) (*Upload, error) {
	var up Upload
	if err := s.client.call(ctx, &request{method: http.MethodGet, path: uploadPath(bucket, uploadID) + "/parts"}, &up); err != nil {
		return nil, err
	}
	for i := range up.Parts {
		up.Parts[i].ETag = unquoteETag(up.Parts[i].ETag)
	}
	return &up, nil
}

// ListUploads lists a bucket's open uploads under prefix, oldest first.
func (s *StorageService) ListUploads(ctx context.Context, bucket, prefix string) ([]Upload, error) {
	q := url.Values{}
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	var resp struct {
		Uploads []Upload `json:"uploads"`
	}
	if err := s.client.call(ctx, &request{method: http.MethodGet, path: bucketPath(bucket) + "/uploads", query: q}, &resp); err != nil {
		return nil, err
	}
	return resp.Uploads, nil
}

// CompleteUpload assembles the parts named, in ascending order, into the
// object. Parts not named are dropped.
func (s *StorageService) CompleteUpload(ctx context.Context, bucket, uploadID string, parts []CompletedPart) (*Object, error) {
	r := &request{method: http.MethodPost, path: uploadPath(bucket, uploadID) + "/complete",
		json: map[string]any{"parts": parts}, mode: retryUnsent, transfer: true}
	var obj Object
	if err := s.client.call(ctx, r, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

// AbortUpload cancels an upload and drops its parts.
func (s *StorageService) AbortUpload(ctx context.Context, bucket, uploadID string) error {
	r := &request{method: http.MethodDelete, path: uploadPath(bucket, uploadID)}
	err := s.client.call(ctx, r, nil)
	if err != nil && r.attempt > 0 && ErrorCode(err) == "upload_not_found" {
		return nil
	}
	return err
}
