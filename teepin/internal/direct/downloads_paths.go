// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
	"github.com/FlashbackAi/teepin-go/teepin/internal/journal"
)

// Batch downloads' rules on paths (TEEPIN-STORAGE-DESIGN.md sections 19.15 and 22.5), a port of teepin-python's
// _downloads.py and teepin-node's downloads-plan.ts:
//
//   - which key can be a path under the destination: every part of it a file name; on Windows also without the
//     characters Windows refuses, a device name (with any extension) or a trailing dot or space;
//   - which keys would land on one file: the same path, or paths that differ only by case where names ignore case;
//   - that no link under the destination carries a write out of it.
//
// Links: Python and Node resolve every link under the destination and refuse only one that leads out of it. Go's
// filepath.EvalSymlinks no longer follows Windows' junctions (Go 1.23), so this port is stricter, the same on every
// OS: no folder between the destination and a target may be a link of any kind (a symbolic link, a junction or another
// reparse point), wherever it leads, and a part file may never be one.

// PartSuffix is added to a target's path for the file a download is written into, renamed into place only when whole.
const PartSuffix = ".teepin-part"

// windowsNames is whether Windows' rules on names apply here.
var windowsNames = runtime.GOOS == "windows"

// winBad reports a character Windows doesn't allow in a file name.
func winBad(r rune) bool { return strings.ContainsRune(`<>:"|?*\`, r) || (r >= 1 && r < 32) }

// winReserved reports one of Windows' device names: a file named one of these, with any extension, is the device.
func winReserved(name string) bool {
	switch name {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}
	if len(name) < 4 || (!strings.HasPrefix(name, "COM") && !strings.HasPrefix(name, "LPT")) {
		return false
	}
	switch name[3:] {
	case "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "¹", "²", "³":
		return true
	}
	return false
}

// PartProblem is why one part of a key's path can't be a file name ("" when it can). On Windows also: its forbidden
// characters, a device name with or without an extension, and a trailing dot or space, which Windows drops, so the
// file would land under another name than the key's.
func PartProblem(part string, windows bool) string {
	if part == "" || part == "." || part == ".." {
		return "an empty, '.' or '..' part"
	}
	if strings.ContainsRune(part, 0) {
		return "a NUL character"
	}
	if windows {
		if strings.IndexFunc(part, winBad) >= 0 {
			return "a character Windows doesn't allow in a file name"
		}
		if strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
			return "a name ending in a dot or a space, which Windows would drop"
		}
		base, _, _ := strings.Cut(part, ".")
		if winReserved(strings.ToUpper(strings.TrimRight(base, " "))) {
			return "a name Windows keeps for a device"
		}
	}
	return ""
}

// keyParts is a key's path split into its parts, each checked by PartProblem.
func keyParts(rel string, windows bool) ([]string, error) {
	parts := strings.Split(rel, "/")
	for _, p := range parts {
		if why := PartProblem(p, windows); why != "" {
			return nil, errs.Unsafe("%q isn't a safe path under the destination: %s", rel, why)
		}
	}
	return parts, nil
}

// within reports whether p is root or under it (both absolute and clean).
func within(root, p string) bool {
	rel, err := filepath.Rel(root, p)
	return err == nil && (rel == "." || filepath.IsLocal(rel))
}

// targetPath is where a key's path rel goes under root: an UnsafePathError when it isn't a safe path there.
func targetPath(root, rel string) (string, error) {
	parts, err := keyParts(rel, windowsNames)
	if err != nil {
		return "", err
	}
	p := filepath.Join(append([]string{root}, parts...)...)
	if p == root || !within(root, p) {
		return "", errs.Unsafe("%q isn't a safe path under the destination", rel)
	}
	if err := checkInside(root, p); err != nil {
		return "", err
	}
	return p, nil
}

// isLink reports a link of any kind: a symbolic link, or on Windows a junction or another reparse point (which Go
// reports as irregular).
func isLink(fi fs.FileInfo) bool { return fi.Mode()&(fs.ModeSymlink|fs.ModeIrregular) != 0 }

// checkInside is an UnsafePathError unless no folder between root and path's folder is a link, and path's part file
// isn't one: a link under the destination must never carry a write outside it. Checked before anything is signed,
// again before a part file is made, and again before it is renamed, since links can appear meanwhile. A folder not
// there yet ends the check (making it makes a real folder); a file where a folder goes is left for the write to fail.
func checkInside(root, path string) error {
	rel, err := filepath.Rel(root, filepath.Dir(path))
	if err != nil || !(rel == "." || filepath.IsLocal(rel)) {
		return errs.Unsafe("%q isn't a safe path under the destination", path)
	}
	leads := func() error {
		return errs.Unsafe("%q isn't a safe path under the destination: a link leads out of it", path)
	}
	cur := root
	if rel != "." {
		for _, name := range strings.Split(rel, string(filepath.Separator)) {
			cur = filepath.Join(cur, name)
			fi, err := os.Lstat(cur)
			if errors.Is(err, fs.ErrNotExist) {
				break
			}
			if err != nil {
				return err
			}
			if isLink(fi) {
				return leads()
			}
			if !fi.IsDir() {
				break
			}
		}
	}
	if fi, err := os.Lstat(path + PartSuffix); err == nil && isLink(fi) {
		return leads()
	}
	return nil
}

// CaseInsensitive reports whether names under dir ignore case (Windows, macOS's default volumes), found by trying;
// true when it can't be told, the safe side. dir is made if it isn't there.
func CaseInsensitive(dir string) bool {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return true
	}
	f, err := os.CreateTemp(dir, ".teepin-case-")
	if err != nil {
		return true
	}
	probe := f.Name()
	_ = f.Close()
	defer func() { _ = os.Remove(probe) }()
	_, err = os.Lstat(filepath.Join(dir, strings.ToUpper(filepath.Base(probe))))
	if err == nil {
		return true
	}
	return !errors.Is(err, fs.ErrNotExist)
}

// fold is a path as names that ignore case see it (upper, then lower case, a rune at a time, as NTFS and APFS compare
// names), or as it is. Python and Node fold more: full case folding (a sharp s and "SS" are one) after NFC
// normalization, which the standard library lacks. Two keys they'd call one file and this calls two are still never
// mixed: where the file system does take them for one file, the second's part file replaces the first's, whose
// identity check then fails it; and a rename onto a file this batch already renamed into place for another key fails
// the later key (batch.place), so both are never reported saved.
func fold(path string, insensitive bool) string {
	if !insensitive {
		return path
	}
	return strings.ToLower(strings.ToUpper(path))
}

// DownloadPair is one key of a batch download and its path under the destination, "/"-separated.
type DownloadPair struct {
	Key string
	Rel string
}

// planEntry is one key of a plan: where it goes and, when that's decided before anything is signed, its result.
type planEntry struct {
	key, path string
	result    *DownloadResult
}

// planPaths turns pairs into the plan: folder markers skipped, keys that aren't safe paths failed, and a key whose file
// another key already takes failed (after the rest, so it never stands in for the first key's result).
func planPaths(root string, pairs []DownloadPair, insensitive bool) (entries []*planEntry, collisions []DownloadResult) {
	seen := map[string]string{} // folded path -> the key going there
	shown := func(rel string) string { return root + string(filepath.Separator) + rel }
	for _, pr := range pairs {
		if pr.Rel == "" || strings.HasSuffix(pr.Rel, "/") {
			p := shown(pr.Rel)
			entries = append(entries, &planEntry{key: pr.Key, path: p,
				result: &DownloadResult{Key: pr.Key, Path: p, Status: DownloadSkipped}})
			continue
		}
		p, err := targetPath(root, pr.Rel)
		if err != nil {
			s := shown(pr.Rel)
			entries = append(entries, &planEntry{key: pr.Key, path: s,
				result: &DownloadResult{Key: pr.Key, Path: s, Status: DownloadFailed, Err: err}})
			continue
		}
		norm := fold(p, insensitive)
		if other, ok := seen[norm]; ok {
			if other == pr.Key {
				continue // the same key twice
			}
			collisions = append(collisions, DownloadResult{Key: pr.Key, Path: p, Status: DownloadFailed,
				Err: errs.Unsafe("%q and %q both go to %s", pr.Key, other, p)})
			continue
		}
		seen[norm] = pr.Key
		entries = append(entries, &planEntry{key: pr.Key, path: p})
	}
	return entries, collisions
}

// rmPart removes a part file this SDK left, only while no link leads its path out of root and it is a regular file
// (removing a name never touches another name's data, but a link is left alone all the same).
func rmPart(root, path string) {
	if checkInside(root, path) != nil {
		return
	}
	part := path + PartSuffix
	if fi, err := os.Lstat(part); err == nil && fi.Mode().IsRegular() {
		_ = journalRemove(part)
	}
}

// journalRemove deletes a file, tried again while Windows has it open elsewhere.
func journalRemove(path string) error {
	err := journal.Retry(func() error { return os.Remove(path) })
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
