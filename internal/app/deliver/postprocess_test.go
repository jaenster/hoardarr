package deliver

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newQuietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func writeFile(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDeobfuscateRename_RenamesObfuscatedLargest(t *testing.T) {
	dir := t.TempDir()
	// 12 MiB obfuscated main file (above min size, way above ratio).
	writeFile(t, filepath.Join(dir, "abcdef1234567890abcdef1234567890.mkv"), 12*1024*1024)
	// Tiny sidecar that won't compete for largest.
	writeFile(t, filepath.Join(dir, "info.txt"), 200)

	newPath, err := deobfuscateRename(dir, "Great.Movie.2020", newQuietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if newPath == "" {
		t.Fatal("expected rename, got none")
	}
	if filepath.Base(newPath) != "Great.Movie.2020.mkv" {
		t.Fatalf("renamed to %q, expected Great.Movie.2020.mkv", filepath.Base(newPath))
	}
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("renamed file missing: %v", err)
	}
}

func TestDeobfuscateRename_SkipsHandNamedLargest(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "Some.Movie.2020.1080p.x264.mkv"), 12*1024*1024)

	newPath, err := deobfuscateRename(dir, "Other.Name", newQuietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if newPath != "" {
		t.Fatalf("unexpected rename to %q on hand-named file", newPath)
	}
}

func TestDeobfuscateRename_SkipsDiscStructure(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "VIDEO_TS"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "VIDEO_TS", "abcdef1234567890abcdef1234567890.vob"), 12*1024*1024)

	newPath, err := deobfuscateRename(dir, "Original.Movie", newQuietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if newPath != "" {
		t.Fatalf("disc structure should block rename, got %q", newPath)
	}
}

func TestDeobfuscateRename_SkipsWhenComparableSiblings(t *testing.T) {
	dir := t.TempDir()
	// Two roughly equal data files — looks like an episode pack.
	writeFile(t, filepath.Join(dir, "abcdef1234567890abcdef1234567890.mkv"), 12*1024*1024)
	writeFile(t, filepath.Join(dir, "fedcba0987654321fedcba0987654321.mkv"), 11*1024*1024)

	newPath, err := deobfuscateRename(dir, "Series.Pack", newQuietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if newPath != "" {
		t.Fatalf("comparable-sized siblings should block rename, got %q", newPath)
	}
}

func TestDeobfuscateRename_SkipsTooSmall(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "abcdef1234567890abcdef1234567890.mkv"), 5*1024*1024) // under 10 MiB

	newPath, err := deobfuscateRename(dir, "Tiny", newQuietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if newPath != "" {
		t.Fatalf("under-min-size file should not be renamed")
	}
}

func TestRemoveSamples_DeletesMatching(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "movie.mkv"), 100)
	writeFile(t, filepath.Join(dir, "movie-sample.mkv"), 50)
	writeFile(t, filepath.Join(dir, "Sample", "preview.mkv"), 50)
	writeFile(t, filepath.Join(dir, "proof.png"), 20)
	writeFile(t, filepath.Join(dir, "subs", "movie.srt"), 30)

	if err := removeSamples(dir, newQuietLogger()); err != nil {
		t.Fatal(err)
	}
	// Movie + srt should survive.
	mustExist(t, filepath.Join(dir, "movie.mkv"))
	mustExist(t, filepath.Join(dir, "subs", "movie.srt"))
	// Samples + proof deleted; empty Sample dir swept.
	mustNotExist(t, filepath.Join(dir, "movie-sample.mkv"))
	mustNotExist(t, filepath.Join(dir, "Sample", "preview.mkv"))
	mustNotExist(t, filepath.Join(dir, "Sample"))
	mustNotExist(t, filepath.Join(dir, "proof.png"))
}

func TestRemoveSamples_RefusesToNukeAllFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "sample-pack-01.mkv"), 100)
	writeFile(t, filepath.Join(dir, "sample-pack-02.mkv"), 100)

	if err := removeSamples(dir, newQuietLogger()); err != nil {
		t.Fatal(err)
	}
	mustExist(t, filepath.Join(dir, "sample-pack-01.mkv"))
	mustExist(t, filepath.Join(dir, "sample-pack-02.mkv"))
}

func TestCollapseSingleFolder_FlattensNestedRelease(t *testing.T) {
	dir := t.TempDir()
	inner := filepath.Join(dir, "Release.Inner.Name")
	writeFile(t, filepath.Join(inner, "movie.mkv"), 100)
	writeFile(t, filepath.Join(inner, "info.nfo"), 50)

	if err := collapseSingleFolder(dir, newQuietLogger()); err != nil {
		t.Fatal(err)
	}
	mustExist(t, filepath.Join(dir, "movie.mkv"))
	mustExist(t, filepath.Join(dir, "info.nfo"))
	mustNotExist(t, inner)
}

func TestCollapseSingleFolder_SkipsWhenMultipleSubdirs(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a", "f.mkv"), 100)
	writeFile(t, filepath.Join(dir, "b", "g.mkv"), 100)

	if err := collapseSingleFolder(dir, newQuietLogger()); err != nil {
		t.Fatal(err)
	}
	mustExist(t, filepath.Join(dir, "a", "f.mkv"))
	mustExist(t, filepath.Join(dir, "b", "g.mkv"))
}

func TestCollapseSingleFolder_SkipsWhenTopLevelFile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "top.mkv"), 100)
	writeFile(t, filepath.Join(dir, "inner", "nested.mkv"), 100)

	if err := collapseSingleFolder(dir, newQuietLogger()); err != nil {
		t.Fatal(err)
	}
	mustExist(t, filepath.Join(dir, "top.mkv"))
	mustExist(t, filepath.Join(dir, "inner", "nested.mkv"))
}

func mustExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

func mustNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("expected %s to be gone", path)
	} else if !strings.Contains(err.Error(), "no such file") {
		t.Fatalf("unexpected stat error for %s: %v", path, err)
	}
}
