package download

import (
	"testing"

	"github.com/jaenster/hoardarr/internal/adapter/nzb"
	"github.com/jaenster/hoardarr/internal/domain/download"
)

// TestBuildFiles_DedupesDuplicateSegments mirrors the failure mode
// reported by a Sonarr grab on 2026-05-17 where a real-world NZB
// contained two <segment number="..."> entries with the same number
// within one file. Pre-fix, the SQLite INSERT later hit
// "UNIQUE constraint failed: segments.file_id, segments.seq_index"
// and the whole AddJob call returned 400 to Sonarr.
//
// Post-fix: silent dedupe at parse-time mirrors SABnzbd's behaviour;
// the first occurrence wins.
func TestBuildFiles_DedupesDuplicateSegments(t *testing.T) {
	doc := &nzb.Document{
		Files: []nzb.File{
			{
				Filename: "release.part01.rar",
				Segments: []nzb.Segment{
					{Number: 1, MessageID: "first@host", Bytes: 700_000},
					{Number: 2, MessageID: "second@host", Bytes: 700_000},
					// Duplicate number 1 — second poster's re-upload.
					// Same number, different message-id. Drop.
					{Number: 1, MessageID: "first-reposted@host", Bytes: 700_000},
					{Number: 3, MessageID: "third@host", Bytes: 700_000},
					// Duplicate message-id (different number). Also drop.
					{Number: 4, MessageID: "third@host", Bytes: 700_000},
				},
			},
		},
	}

	files, total := buildFiles(doc)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	got := files[0].Segments
	if len(got) != 3 {
		t.Errorf("expected 3 deduped segments, got %d: %v", len(got), segNums(got))
	}
	// Expected order: 1, 2, 3 (first occurrence each).
	wantNums := []int{1, 2, 3}
	for i, w := range wantNums {
		if got[i].SeqIndex != w {
			t.Errorf("segment[%d].SeqIndex = %d; want %d", i, got[i].SeqIndex, w)
		}
	}
	if got[0].MessageID != "first@host" {
		t.Errorf("first-wins not honoured: got message_id %q", got[0].MessageID)
	}
	// Size totals from the 3 unique segments only.
	const wantSize = 3 * 700_000
	if files[0].SizeBytes != wantSize {
		t.Errorf("SizeBytes = %d; want %d", files[0].SizeBytes, wantSize)
	}
	if total != wantSize {
		t.Errorf("total = %d; want %d", total, wantSize)
	}
}

// TestBuildFiles_DropsEmptyFile sanity-checks the pre-existing
// "no segments = skip" branch since the dedupe pass above touches
// the same loop.
func TestBuildFiles_DropsEmptyFile(t *testing.T) {
	doc := &nzb.Document{
		Files: []nzb.File{
			{Filename: "empty.rar", Segments: nil},
			{Filename: "good.rar", Segments: []nzb.Segment{
				{Number: 1, MessageID: "a@h", Bytes: 100},
			}},
		},
	}
	files, _ := buildFiles(doc)
	if len(files) != 1 {
		t.Fatalf("expected 1 file (empty skipped), got %d", len(files))
	}
	if files[0].Filename != "good.rar" {
		t.Errorf("got file %q; expected good.rar", files[0].Filename)
	}
}

func segNums(segs []download.NewSegmentParams) []int {
	out := make([]int, len(segs))
	for i, s := range segs {
		out[i] = s.SeqIndex
	}
	return out
}
