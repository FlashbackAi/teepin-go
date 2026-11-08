// Copyright 2026 TEEPIN Project
// Licensed under the Apache License, Version 2.0

package direct

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FlashbackAi/teepin-go/teepin/internal/errs"
)

// The rules on names a batch download's keys meet (downloads_paths.go), as teepin-python's and teepin-node's tests
// state them.

func TestPartProblem_EmptyDotAndNULAreRefusedEverywhere(t *testing.T) {
	for _, p := range []string{"", ".", "..", "a\x00b"} {
		for _, windows := range []bool{false, true} {
			if PartProblem(p, windows) == "" {
				t.Errorf("%q (windows %v) was allowed", p, windows)
			}
		}
	}
}

func TestPartProblem_WindowsDeviceNamesAreRefusedWithAnyExtensionOnWindowsOnly(t *testing.T) {
	for _, p := range []string{"CON", "con", "Aux.txt", "COM1.log", "lpt9", "NUL.tar.gz", "PRN", "COM0", "LPT².x", "CON .txt"} {
		if why := PartProblem(p, true); !strings.Contains(why, "device") {
			t.Errorf("%q: %q", p, why)
		}
		if why := PartProblem(p, false); why != "" {
			t.Errorf("%q is an ordinary name elsewhere: %q", p, why)
		}
	}
}

func TestPartProblem_CharactersWindowsRefusesAreRefused(t *testing.T) {
	for _, c := range []string{"<", ">", ":", `"`, "|", "?", "*", `\`, "\x01", "\x1f"} {
		if why := PartProblem("a"+c+"b", true); !strings.Contains(why, "character") {
			t.Errorf("%q: %q", c, why)
		}
	}
}

func TestPartProblem_ANameWindowsWouldCutShortIsRefused(t *testing.T) {
	for _, p := range []string{"report.", "name ", "a..", "x. "} {
		if why := PartProblem(p, true); !strings.Contains(why, "dot or a space") {
			t.Errorf("%q: %q", p, why)
		}
		if PartProblem(p, false) != "" {
			t.Errorf("%q is fine elsewhere", p)
		}
	}
}

func TestPartProblem_NamesLikeDeviceNamesAreFine(t *testing.T) {
	for _, p := range []string{"CONSOLE", "con-fig", "COM10", "LPTX", ".hidden", "a.b.c", "x.CON"} {
		if why := PartProblem(p, true); why != "" {
			t.Errorf("%q: %q", p, why)
		}
	}
}

func TestPlan_KeysThatDifferByCaseAreTwoFilesWhereNamesKeepCase(t *testing.T) {
	root := t.TempDir()
	entries, collisions := planPaths(root, []DownloadPair{{"A.txt", "A.txt"}, {"a.txt", "a.txt"}}, false)
	if len(entries) != 2 || len(collisions) != 0 || entries[0].result != nil || entries[1].result != nil {
		t.Fatalf("%+v %+v", entries, collisions)
	}
	entries, collisions = planPaths(root, []DownloadPair{{"A.txt", "A.txt"}, {"a.txt", "a.txt"}}, true)
	if len(entries) != 1 || len(collisions) != 1 || collisions[0].Key != "a.txt" {
		t.Fatalf("%+v %+v", entries, collisions)
	}
}

func TestPlan_FoldsCaseRuneByRune(t *testing.T) {
	if fold("ÉTÉ/Ǆ", true) != fold("été/ǆ", true) {
		t.Fatal("names that differ only by case are one where names ignore case")
	}
	if fold("A", false) == fold("a", false) {
		t.Fatal("not folded where names keep case")
	}
}

func TestPlan_UnsafeKeysFailAndMarkersAreSkipped(t *testing.T) {
	root := t.TempDir()
	entries, _ := planPaths(root, []DownloadPair{{"../x", "../x"}, {"d/", "d/"}, {"ok", "ok"}}, false)
	if r := entries[0].result; r == nil || r.Status != DownloadFailed || !errors.Is(r.Err, errs.ErrUnsafePath) {
		t.Fatalf("%+v", entries[0])
	}
	if r := entries[1].result; r == nil || r.Status != DownloadSkipped {
		t.Fatalf("%+v", entries[1])
	}
	if entries[2].result != nil || entries[2].path != filepath.Join(root, "ok") {
		t.Fatalf("%+v", entries[2])
	}
}

func TestDownloadPassing_SaysWhichErrorsKeepAnObject(t *testing.T) {
	keep := []error{&errs.APIError{StatusCode: 401}, &errs.APIError{StatusCode: 403}, &errs.APIError{StatusCode: 402},
		&errs.APIError{StatusCode: 429}, &errs.APIError{StatusCode: 503}, &errs.ConnError{Err: errors.New("reset")},
		&errs.StorageUnavailableError{Message: "down"}, &errs.ConnError{Err: errors.New("x")}}
	for _, err := range keep {
		if !downloadPassing(err) {
			t.Errorf("%v should be kept", err)
		}
	}
	final := []error{&errs.APIError{StatusCode: 404}, &errs.APIError{StatusCode: 410}, errs.ErrObjectChanged,
		errs.Checksum("x"), errs.Unsafe("x"), errs.Integrity(errs.ReasonTruncated, "short")}
	for _, err := range final {
		if downloadPassing(err) {
			t.Errorf("%v should be final", err)
		}
	}
}
