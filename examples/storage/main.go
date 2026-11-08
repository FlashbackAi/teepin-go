// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

// Command storage shows Teepin Storage from Go: make a bucket, upload a file
// of any size with progress, list, download (each byte verified on this machine)
// and clean up.
//
//	TEEPIN_API_KEY=tpk_... go run ./examples/storage ./some-file
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/FlashbackAi/teepin-go/teepin"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: storage FILE")
	}
	file := os.Args[1]
	// Reads TEEPIN_API_KEY, and TEEPIN_BASE_URL (default https://api.teepin.com).
	client := teepin.NewClient(teepin.Config{})
	ctx := context.Background()
	const bucket = "sdk-example-bucket"

	if _, err := client.Storage.CreateBucket(ctx, bucket); err != nil && !teepin.IsConflict(err) {
		log.Fatal(err)
	}

	// The file is encrypted on this machine and sent straight to the storage
	// network. Files above 16 MiB go in parallel parts, journaled, so a run
	// cut off (a crash, a lost network) is finished by ResumeUploads.
	key := "examples/" + filepath.Base(file)
	obj, err := client.Storage.UploadFile(ctx, bucket, key, file, &teepin.UploadOptions{
		Progress: func(done, total int64) {
			fmt.Printf("\ruploading %d of %d bytes", done, total)
		},
	})
	fmt.Println()
	var unavailable *teepin.StorageUnavailableError
	switch {
	case teepin.IsNoCredit(err):
		log.Fatal("add credit in the console to store files")
	case errors.As(err, &unavailable):
		log.Fatal("storage is unavailable; run the example again later, or call ResumeUploads to finish the upload")
	case err != nil:
		log.Fatal(err)
	}
	fmt.Printf("stored %s: %d bytes, ETag %s\n", obj.Key, obj.Size, obj.ETag)

	for o, err := range client.Storage.Objects(ctx, bucket, "examples/") {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %-40s %12d\n", o.Key, o.Size)
	}

	out := filepath.Join(os.TempDir(), "teepin-example-"+filepath.Base(file))
	if _, err := client.Storage.DownloadFile(ctx, bucket, key, out, nil); err != nil {
		if errors.Is(err, teepin.ErrChecksumMismatch) {
			log.Fatal("the download did not match the stored checksum")
		}
		log.Fatal(err)
	}
	fmt.Println("downloaded to", out)

	if err := client.Storage.DeleteObject(ctx, bucket, key); err != nil {
		log.Fatal(err)
	}
}
