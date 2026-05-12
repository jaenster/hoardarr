package par2

// Regression test for task #121: PAR2 repair fails on obfuscated
// releases because the NZB filenames and the PAR2-recorded filenames
// belong to two independent obfuscation layers (e.g. NZB:
// "b96a1bf4716de96e565004e786c283ce.10" vs PAR2:
// "8TMnqVXYerVDFiwYeD33oWTdeSMg2.part10.rar"). The intended fix is to
// resolve mismatches via FileDesc.MD516k (MD5 of first 16 KB) — a
// content-addressed lookup that survives renames.
//
// This file drives the parser directly on the real PAR2 captured from
// the live container (job 38 / Frasier.S02E04) so we can verify what
// the MD516k digests actually are, and what bytes the matcher should
// be hashing.

import (
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestObfuscated_DumpFileDesc parses the real PAR2 and prints every
// FileDesc entry so we can see what filenames + digests the recovery
// set expects.
func TestObfuscated_DumpFileDesc(t *testing.T) {
	par2Path := filepath.Join("..", "..", "..", "testdata", "repair-bug-job38", "main.par2")
	if _, err := os.Stat(par2Path); err != nil {
		t.Skipf("fixture not present: %v (pull /volume1/Media/downloads/nzb/incomplete-hoardarr/38/1258.tmp -> %s)", err, par2Path)
	}
	set, err := parseFiles([]string{par2Path})
	if err != nil {
		t.Fatalf("parseFiles: %v", err)
	}
	t.Logf("set_id=%x slice_size=%d files=%d recovery_slices=%d",
		set.SetID, set.SliceSize, len(set.Files), len(set.RecoverySlices))
	for _, f := range set.Files {
		t.Logf("  par2 file: name=%q size=%d md516k=%x slices=%d",
			f.Name, f.Size, f.MD516k, len(f.Slices))
	}
}

// TestObfuscated_MD516kMatchesData walks every PAR2 FileDesc and tries
// to find a matching .tmp file in the fixture dir by MD5-of-first-16KB.
// Reports each file as either matched (NZB filename) or unmatched.
// Fails loudly if zero matches — that's the actual repair-time bug.
func TestObfuscated_MD516kMatchesData(t *testing.T) {
	fixtureDir := filepath.Join("..", "..", "..", "testdata", "repair-bug-job38")
	par2Path := filepath.Join(fixtureDir, "main.par2")
	if _, err := os.Stat(par2Path); err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	set, err := parseFiles([]string{par2Path})
	if err != nil {
		t.Fatalf("parseFiles: %v", err)
	}

	// Enumerate .tmp files in the fixture dir.
	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	type dataFile struct {
		path     string
		filename string
		md516k   [16]byte
	}
	var files []dataFile
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || filepath.Ext(name) == ".par2" {
			continue
		}
		p := filepath.Join(fixtureDir, name)
		buf, err := readFirstN(p, 16384)
		if err != nil {
			t.Logf("read %s: %v (skipping)", p, err)
			continue
		}
		files = append(files, dataFile{
			path: p, filename: name, md516k: md5.Sum(buf),
		})
	}
	if len(files) == 0 {
		t.Skip("no .tmp data files in fixture dir — pull a few from /volume1/Media/downloads/nzb/incomplete-hoardarr/38/")
	}

	matches := 0
	for _, pf := range set.Files {
		var hit string
		for _, df := range files {
			if df.md516k == pf.MD516k {
				hit = df.filename
				break
			}
		}
		if hit != "" {
			matches++
			t.Logf("MATCH: par2 %q (%x) ↔ disk %q", pf.Name, pf.MD516k, hit)
		} else {
			t.Logf("NO MATCH: par2 %q expected md516k=%x", pf.Name, pf.MD516k)
		}
	}
	t.Logf("summary: %d of %d FileDesc entries matched a data file by MD516k", matches, len(set.Files))

	if matches == 0 && len(files) > 0 {
		// Show the first-16KB digest we computed for one of the data
		// files so we can see what's in the fixture vs PAR2.
		t.Logf("computed md516k of disk files:")
		for _, df := range files {
			t.Logf("  %q → md516k=%x", df.filename, df.md516k)
		}
		t.Fatalf("zero MD516k matches — confirms task #121 root cause; first 16KB of downloaded files does NOT hash to PAR2's MD516k")
	}
}

// TestObfuscated_BuildMD516kIndexProductionPath drives the actual
// production helper (buildMD516kIndex + md5First16k) to confirm it
// returns the same digests as the test helper. If this asserts equal
// counts but the live container shows zero matches, the issue is
// that the deployed image isn't running the new code (cache stale).
func TestObfuscated_BuildMD516kIndexProductionPath(t *testing.T) {
	fixtureDir := filepath.Join("..", "..", "..", "testdata", "repair-bug-job38")
	par2Path := filepath.Join(fixtureDir, "main.par2")
	if _, err := os.Stat(par2Path); err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	set, err := parseFiles([]string{par2Path})
	if err != nil {
		t.Fatalf("parseFiles: %v", err)
	}

	dataPaths := map[string]string{}
	entries, _ := os.ReadDir(fixtureDir)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || filepath.Ext(name) == ".par2" {
			continue
		}
		dataPaths[name] = filepath.Join(fixtureDir, name)
	}

	idx := buildMD516kIndex(dataPaths)
	t.Logf("buildMD516kIndex: %d entries from %d data paths", len(idx), len(dataPaths))

	matches := 0
	for _, pf := range set.Files {
		if p, ok := idx[pf.MD516k]; ok {
			t.Logf("PRODUCTION MATCH: par2 %q ↔ %s", pf.Name, p)
			matches++
		}
	}
	t.Logf("production-path matches: %d", matches)
	if matches == 0 && len(dataPaths) > 0 {
		t.Fatalf("buildMD516kIndex matched zero files — bug in the production helper")
	}
}

func readFirstN(path string, n int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, n)
	r, err := f.Read(buf)
	if err != nil && r == 0 {
		return nil, fmt.Errorf("read: %w", err)
	}
	return buf[:r], nil
}
