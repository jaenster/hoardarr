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

	segs := j.PendingSegments(time.Now())
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

func TestPendingSegments_SkipsDeferredRecoveryVols(t *testing.T) {
	j, err := NewJob(NewJobParams{
		NZBHash: "deadbeef",
		Name:    "release",
		Files: []NewFileParams{
			{
				Filename:  "release.bin",
				SizeBytes: 1000,
				Segments:  []NewSegmentParams{{SeqIndex: 1, MessageID: "data@host", Bytes: 1000}},
			},
			{
				Filename:      "release.par2",
				SizeBytes:     200,
				IsPar2:        true,
				IsRecoveryVol: false,
				Segments:      []NewSegmentParams{{SeqIndex: 1, MessageID: "idx@host", Bytes: 200}},
			},
			{
				Filename:      "release.vol000+01.par2",
				SizeBytes:     500,
				IsPar2:        true,
				IsRecoveryVol: true,
				Segments:      []NewSegmentParams{{SeqIndex: 1, MessageID: "vol0@host", Bytes: 500}},
			},
			{
				Filename:      "release.vol001+02.par2",
				SizeBytes:     500,
				IsPar2:        true,
				IsRecoveryVol: true,
				Segments:      []NewSegmentParams{{SeqIndex: 1, MessageID: "vol1@host", Bytes: 500}},
			},
		},
		DeferRecoveryVols: true,
	}, time.UnixMilli(1).UTC())
	if err != nil {
		t.Fatalf("NewJob: %v", err)
	}

	pending := j.PendingSegments(time.Now())
	if len(pending) != 2 {
		t.Fatalf("pending = %d; want 2 (data + index, not vols)", len(pending))
	}

	if !j.HasDeferredRecoveryVols() {
		t.Error("HasDeferredRecoveryVols = false; want true")
	}
}

func TestRequestRecoveryVols_RevealsHiddenSegments(t *testing.T) {
	j, _ := NewJob(NewJobParams{
		NZBHash: "deadbeef",
		Name:    "release",
		Files: []NewFileParams{
			{
				Filename:  "release.bin",
				SizeBytes: 1000,
				Segments:  []NewSegmentParams{{SeqIndex: 1, MessageID: "data@host", Bytes: 1000}},
			},
			{
				Filename:      "release.vol000+01.par2",
				SizeBytes:     500,
				IsPar2:        true,
				IsRecoveryVol: true,
				Segments:      []NewSegmentParams{{SeqIndex: 1, MessageID: "vol0@host", Bytes: 500}},
			},
		},
		DeferRecoveryVols: true,
	}, time.UnixMilli(1).UTC())
	j.SetID(42)
	_ = j.PullEvents()

	// Simulate the post-first-download state: the data + index are done,
	// job is in download_complete.
	id := SegmentID(100)
	for _, f := range j.files {
		for _, s := range f.segments {
			s.SetID(id)
			id++
		}
	}
	_ = j.MarkSegmentDone(SegmentResult{SegmentID: 100, BytesOnDisk: 1000}, time.UnixMilli(10).UTC())
	if j.State() != JobStateDownloadComplete {
		t.Fatalf("state = %s; want download_complete", j.State())
	}
	_ = j.PullEvents()

	// Request vols → transitions back to downloading, vol segment becomes visible.
	if err := j.RequestRecoveryVols(time.UnixMilli(20).UTC()); err != nil {
		t.Fatalf("RequestRecoveryVols: %v", err)
	}
	if j.State() != JobStateDownloading {
		t.Errorf("state after request = %s; want downloading", j.State())
	}
	pending := j.PendingSegments(time.Now())
	if len(pending) != 1 {
		t.Fatalf("pending after request = %d; want 1 (the vol)", len(pending))
	}
	if pending[0].MessageID() != "vol0@host" {
		t.Errorf("revealed segment = %s; want vol0@host", pending[0].MessageID())
	}

	// Second call should refuse — flag already flipped.
	if err := j.RequestRecoveryVols(time.UnixMilli(30).UTC()); err == nil {
		t.Error("second RequestRecoveryVols returned nil; want error")
	}
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

// TestPendingSegments_FiltersByNextRetryAt covers the durable-retry
// query semantics: a pending segment with next_retry_at in the future
// is hidden until that instant has passed.
func TestPendingSegments_FiltersByNextRetryAt(t *testing.T) {
	t0 := time.UnixMilli(1_000_000).UTC()
	j, err := NewJob(NewJobParams{
		NZBHash: "deadbeef",
		Name:    "release",
		Files: []NewFileParams{
			{
				Filename:  "f.bin",
				SizeBytes: 100,
				Segments: []NewSegmentParams{
					{SeqIndex: 1, MessageID: "ready@host", Bytes: 50},
					{SeqIndex: 2, MessageID: "deferred@host", Bytes: 50},
				},
			},
		},
	}, t0)
	if err != nil {
		t.Fatalf("NewJob: %v", err)
	}
	id := SegmentID(100)
	for _, f := range j.files {
		for _, s := range f.segments {
			s.SetID(id)
			id++
		}
	}

	// Defer the second segment 1 minute into the future.
	future := t0.Add(time.Minute)
	if err := j.MarkSegmentForRetry(101, future, "conn-limit"); err != nil {
		t.Fatalf("MarkSegmentForRetry: %v", err)
	}

	got := j.PendingSegments(t0)
	if len(got) != 1 {
		t.Fatalf("ready segments = %d; want 1 (deferred one is hidden)", len(got))
	}
	if got[0].ID() != 100 {
		t.Errorf("returned wrong seg: %d", got[0].ID())
	}

	// After the window elapses, both are ready.
	got = j.PendingSegments(future.Add(time.Second))
	if len(got) != 2 {
		t.Fatalf("ready after window = %d; want 2", len(got))
	}

	// NextRetryReadyAt reports the exact instant the deferred segment
	// becomes eligible.
	if got := j.NextRetryReadyAt(t0); !got.Equal(future) {
		t.Errorf("NextRetryReadyAt = %v; want %v", got, future)
	}
	// After all segments are ready, NextRetryReadyAt returns zero.
	if got := j.NextRetryReadyAt(future.Add(time.Second)); !got.IsZero() {
		t.Errorf("NextRetryReadyAt after window = %v; want zero", got)
	}
}

func TestMarkSegmentForRetry_IncrementsAttemptsAndPersistsError(t *testing.T) {
	t0 := time.UnixMilli(0).UTC()
	j, _ := NewJob(NewJobParams{
		NZBHash: "x", Name: "x",
		Files: []NewFileParams{{
			Filename: "f.bin", SizeBytes: 10,
			Segments: []NewSegmentParams{{SeqIndex: 1, MessageID: "m@host", Bytes: 10}},
		}},
	}, t0)
	id := SegmentID(1)
	for _, f := range j.files {
		for _, s := range f.segments {
			s.SetID(id)
			id++
		}
	}

	at := t0.Add(30 * time.Second)
	if err := j.MarkSegmentForRetry(1, at, "boom"); err != nil {
		t.Fatalf("MarkSegmentForRetry: %v", err)
	}
	_, s := j.SegmentByID(1)
	if s.Attempts() != 1 {
		t.Errorf("Attempts = %d; want 1", s.Attempts())
	}
	if s.LastError() != "boom" {
		t.Errorf("LastError = %q; want boom", s.LastError())
	}
	if !s.NextRetryAt().Equal(at) {
		t.Errorf("NextRetryAt = %v; want %v", s.NextRetryAt(), at)
	}
	if s.State() != SegmentStatePending {
		t.Errorf("state = %s; want pending", s.State())
	}
}
