package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

func newJobRepo(t *testing.T) (*JobRepo, *DB) {
	t.Helper()
	db := openMigratedDB(t)
	return NewJobRepo(db), db
}

func mkSavedJob(t *testing.T, repo *JobRepo) *download.Job {
	t.Helper()
	j, err := download.NewJob(download.NewJobParams{
		NZBHash:    "hash-" + t.Name(),
		Name:       "release",
		Category:   "tv",
		QueueOrder: time.Now().UnixNano(),
		NZBBlob:    []byte("<nzb/>"),
		Files: []download.NewFileParams{
			{
				Filename:  "file.r00",
				SizeBytes: 1000,
				Segments: []download.NewSegmentParams{
					{SeqIndex: 1, MessageID: "msg1@host", Bytes: 600},
					{SeqIndex: 2, MessageID: "msg2@host", Bytes: 400},
				},
			},
		},
	}, time.UnixMilli(1).UTC())
	if err != nil {
		t.Fatalf("NewJob: %v", err)
	}
	if err := repo.Save(context.Background(), j); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if j.ID() == 0 {
		t.Fatal("ID not assigned")
	}
	return j
}

func TestJobRepo_SaveAndLoad(t *testing.T) {
	repo, _ := newJobRepo(t)
	j := mkSavedJob(t, repo)

	got, err := repo.ByID(context.Background(), j.ID())
	if err != nil {
		t.Fatalf("ByID: %v", err)
	}
	if got.Name() != "release" {
		t.Errorf("name = %q", got.Name())
	}
	if got.Category() != "tv" {
		t.Errorf("category = %q", got.Category())
	}
	if got.TotalBytes() != 1000 {
		t.Errorf("total bytes = %d; want 1000", got.TotalBytes())
	}
	files := got.Files()
	if len(files) != 1 {
		t.Fatalf("files = %d; want 1", len(files))
	}
	if files[0].Filename() != "file.r00" {
		t.Errorf("filename = %q", files[0].Filename())
	}
	segs := files[0].Segments()
	if len(segs) != 2 {
		t.Fatalf("segments = %d; want 2", len(segs))
	}
	if segs[0].MessageID() != "msg1@host" {
		t.Errorf("seg[0] msgid = %q", segs[0].MessageID())
	}
}

func TestJobRepo_ByNZBHash(t *testing.T) {
	repo, _ := newJobRepo(t)
	j := mkSavedJob(t, repo)
	got, err := repo.ByNZBHash(context.Background(), j.NZBHash())
	if err != nil {
		t.Fatalf("ByNZBHash: %v", err)
	}
	if got.ID() != j.ID() {
		t.Errorf("ID = %d; want %d", got.ID(), j.ID())
	}
}

func TestJobRepo_DuplicateHashRejected(t *testing.T) {
	repo, _ := newJobRepo(t)
	mkSavedJob(t, repo)

	dup, _ := download.NewJob(download.NewJobParams{
		NZBHash: "hash-" + t.Name(),
		Name:    "dup",
		Files: []download.NewFileParams{
			{Filename: "x", Segments: []download.NewSegmentParams{{SeqIndex: 1, MessageID: "x@h", Bytes: 1}}},
		},
	}, time.Now())
	if err := repo.Save(context.Background(), dup); err == nil {
		t.Fatal("expected unique-violation error on duplicate hash")
	}
}

func TestJobRepo_UpdateSegmentBatch(t *testing.T) {
	repo, _ := newJobRepo(t)
	j := mkSavedJob(t, repo)

	segIDs := []download.SegmentID{}
	for _, f := range j.Files() {
		for _, s := range f.Segments() {
			segIDs = append(segIDs, s.ID())
		}
	}
	if len(segIDs) != 2 {
		t.Fatalf("seg count = %d; want 2", len(segIDs))
	}

	updates := []download.SegmentUpdate{
		{SegmentID: segIDs[0], State: download.SegmentStateDone, Attempts: 1, FileOffset: 0},
		{SegmentID: segIDs[1], State: download.SegmentStateMissing, Attempts: 3, LastError: "all servers 430"},
	}
	if err := repo.UpdateSegmentBatch(context.Background(), updates); err != nil {
		t.Fatalf("UpdateSegmentBatch: %v", err)
	}

	got, _ := repo.ByID(context.Background(), j.ID())
	segs := got.Files()[0].Segments()
	if segs[0].State() != download.SegmentStateDone {
		t.Errorf("seg[0] state = %s", segs[0].State())
	}
	if segs[1].State() != download.SegmentStateMissing {
		t.Errorf("seg[1] state = %s", segs[1].State())
	}
	if segs[1].LastError() != "all servers 430" {
		t.Errorf("seg[1] last_error = %q", segs[1].LastError())
	}
}

func TestJobRepo_Delete(t *testing.T) {
	repo, db := newJobRepo(t)
	j := mkSavedJob(t, repo)

	if err := repo.Delete(context.Background(), j.ID()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := repo.ByID(context.Background(), j.ID())
	if !errors.Is(err, download.ErrJobNotFound) {
		t.Errorf("post-delete ByID = %v; want ErrJobNotFound", err)
	}

	// Cascade: files and segments should also be gone.
	var n int
	if err := db.QueryRowCtx(context.Background(), `SELECT COUNT(*) FROM files`).Scan(&n); err != nil {
		t.Fatalf("count files: %v", err)
	}
	if n != 0 {
		t.Errorf("files after cascade = %d; want 0", n)
	}
	if err := db.QueryRowCtx(context.Background(), `SELECT COUNT(*) FROM segments`).Scan(&n); err != nil {
		t.Fatalf("count segments: %v", err)
	}
	if n != 0 {
		t.Errorf("segments after cascade = %d; want 0", n)
	}
}

func TestJobRepo_ListAndActive(t *testing.T) {
	repo, _ := newJobRepo(t)
	j1 := mkSavedJob(t, repo)
	// Create a second job with different hash.
	j2, _ := download.NewJob(download.NewJobParams{
		NZBHash: "hash-other-" + t.Name(),
		Name:    "other",
		QueueOrder: time.Now().UnixNano() + 1,
		NZBBlob: []byte("<nzb/>"),
		Files: []download.NewFileParams{
			{Filename: "y", Segments: []download.NewSegmentParams{{SeqIndex: 1, MessageID: "y@h", Bytes: 1}}},
		},
	}, time.Now())
	if err := repo.Save(context.Background(), j2); err != nil {
		t.Fatalf("Save j2: %v", err)
	}

	all, err := repo.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("List count = %d; want 2", len(all))
	}

	active, err := repo.Active(context.Background())
	if err != nil {
		t.Fatalf("Active: %v", err)
	}
	if len(active) != 2 {
		t.Errorf("Active count = %d; want 2 (both queued)", len(active))
	}
	_ = j1
}
