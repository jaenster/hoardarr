package download

import (
	"testing"
	"time"
)

func mkJob(t *testing.T) *Job {
	t.Helper()
	j, err := NewJob(NewJobParams{
		NZBHash: "abcdef",
		Name:    "release",
		Files: []NewFileParams{
			{
				Filename:  "file.r00",
				SizeBytes: 1000,
				Segments: []NewSegmentParams{
					{SeqIndex: 1, MessageID: "msg1@host", Bytes: 600},
					{SeqIndex: 2, MessageID: "msg2@host", Bytes: 400},
				},
			},
			{
				Filename:  "file.par2",
				SizeBytes: 200,
				IsPar2:    true,
				Segments: []NewSegmentParams{
					{SeqIndex: 1, MessageID: "par2@host", Bytes: 200},
				},
			},
		},
	}, time.UnixMilli(1).UTC())
	if err != nil {
		t.Fatalf("NewJob: %v", err)
	}
	j.SetID(7)
	return j
}

func TestNewJob_EmitsJobCreated(t *testing.T) {
	j := mkJob(t)
	evts := j.PullEvents()
	if len(evts) != 1 {
		t.Fatalf("events = %d; want 1", len(evts))
	}
	jc, ok := evts[0].(JobCreated)
	if !ok {
		t.Fatalf("event %T; want JobCreated", evts[0])
	}
	if jc.ID != 7 {
		t.Errorf("ID = %d; want 7 (after SetID)", jc.ID)
	}
	if jc.TotalBytes != 1200 {
		t.Errorf("TotalBytes = %d; want 1200", jc.TotalBytes)
	}
}

func TestMarkSegmentDispatched(t *testing.T) {
	j := mkJob(t)
	_ = j.PullEvents()

	// Patch segment ids for in-memory test (mimicking repo Save).
	id := SegmentID(100)
	for _, f := range j.files {
		for _, s := range f.segments {
			s.SetID(id)
			id++
		}
	}

	segs := j.PendingSegments()
	if len(segs) != 3 {
		t.Fatalf("PendingSegments = %d; want 3", len(segs))
	}

	if err := j.MarkSegmentDispatched(100, time.Now()); err != nil {
		t.Fatalf("MarkSegmentDispatched: %v", err)
	}
	if got := j.files[0].segments[0].State(); got != SegmentStateInflight {
		t.Errorf("state = %s; want inflight", got)
	}
	// Re-dispatching the same segment should fail.
	if err := j.MarkSegmentDispatched(100, time.Now()); err == nil {
		t.Error("expected error re-dispatching inflight segment")
	}
}

func TestMarkSegmentDone_UpdatesProgress(t *testing.T) {
	j := mkJob(t)
	_ = j.PullEvents()

	id := SegmentID(100)
	for _, f := range j.files {
		for _, s := range f.segments {
			s.SetID(id)
			id++
		}
	}

	now := time.Now()
	if err := j.MarkSegmentDone(SegmentResult{SegmentID: 100, BytesOnDisk: 600}, now); err != nil {
		t.Fatalf("MarkSegmentDone 100: %v", err)
	}
	if j.DoneBytes() != 600 {
		t.Errorf("DoneBytes = %d; want 600", j.DoneBytes())
	}
	// File 0 has 2 segments; only 1 done so far → file not complete.
	if j.files[0].State() == FileStateComplete {
		t.Error("file 0 should not yet be complete")
	}

	if err := j.MarkSegmentDone(SegmentResult{SegmentID: 101, BytesOnDisk: 400}, now); err != nil {
		t.Fatalf("MarkSegmentDone 101: %v", err)
	}
	if j.files[0].State() != FileStateComplete {
		t.Error("file 0 should be complete after both segments")
	}

	// File 1 par2 not done yet — job not complete.
	if j.State() == JobStateDownloadComplete {
		t.Error("job should not be complete with par2 still pending")
	}

	if err := j.MarkSegmentDone(SegmentResult{SegmentID: 102, BytesOnDisk: 200}, now); err != nil {
		t.Fatalf("MarkSegmentDone 102: %v", err)
	}
	if j.State() != JobStateDownloadComplete {
		t.Errorf("state = %s; want download_complete", j.State())
	}
}

func TestMarkSegmentMissing_StillCompletesJob(t *testing.T) {
	j := mkJob(t)
	_ = j.PullEvents()

	id := SegmentID(100)
	for _, f := range j.files {
		for _, s := range f.segments {
			s.SetID(id)
			id++
		}
	}

	now := time.Now()
	_ = j.MarkSegmentDone(SegmentResult{SegmentID: 100, BytesOnDisk: 600}, now)
	_ = j.MarkSegmentDone(SegmentResult{SegmentID: 101, BytesOnDisk: 400}, now)
	_ = j.MarkSegmentMissing(102, now)

	if j.State() != JobStateDownloadComplete {
		t.Errorf("state = %s; want download_complete after all resolve (with missing)", j.State())
	}
	// Verify the JobDownloadComplete event reports MissingSegments.
	for _, e := range j.PullEvents() {
		if jdc, ok := e.(JobDownloadComplete); ok {
			if jdc.MissingSegments != 1 {
				t.Errorf("MissingSegments = %d; want 1", jdc.MissingSegments)
			}
			return
		}
	}
	t.Error("no JobDownloadComplete event emitted")
}

func TestResetInflightToPending(t *testing.T) {
	j := mkJob(t)
	_ = j.PullEvents()

	id := SegmentID(100)
	for _, f := range j.files {
		for _, s := range f.segments {
			s.SetID(id)
			id++
		}
	}
	_ = j.MarkSegmentDispatched(100, time.Now())
	_ = j.MarkSegmentDispatched(101, time.Now())

	n := j.ResetInflightToPending()
	if n != 2 {
		t.Errorf("reset count = %d; want 2", n)
	}
	for _, s := range j.files[0].segments {
		if s.State() != SegmentStatePending {
			t.Errorf("seg %d state = %s; want pending", s.id, s.State())
		}
	}
}
