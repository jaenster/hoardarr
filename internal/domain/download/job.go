// Package download is the bounded context that owns NZB-driven download
// jobs. The aggregate root is Job; it has File entities, which have
// Segment entities. All mutation flows through Job's methods so domain
// invariants are enforced and events are recorded.
//
// The orchestrator (in internal/app/download) consumes the JobCreated
// event, dispatches segment fetches to the NNTP pool, and feeds
// completion results back into the Job aggregate via methods like
// MarkSegmentDone.
package download

import (
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
)

// JobID identifies a Job aggregate. Allocated by the persistence layer.
type JobID int64

// Job is the aggregate root. It owns Files which own Segments.
type Job struct {
	id         JobID
	nzbHash    string
	name       string
	category   string
	priority   int
	queueOrder int64
	state      JobState

	totalBytes  int64
	doneBytes   int64
	failedBytes int64

	addedAt    time.Time
	startedAt  time.Time
	finishedAt time.Time

	errorMsg string
	nzbBlob  []byte

	files []*File

	events []event.Event
}

// NewJobParams gathers the inputs to construct a fresh Job.
type NewJobParams struct {
	NZBHash    string
	Name       string
	Category   string
	Priority   int
	QueueOrder int64
	NZBBlob    []byte
	Files      []NewFileParams
}

// NewJob constructs a fresh queued Job. The nzb bytes are persisted
// (small, ~tens of KB) so the user can re-queue from history later
// without keeping the original file path. Records JobCreated with the
// (zero) ID; SetID patches that event before publish.
func NewJob(p NewJobParams, now time.Time) (*Job, error) {
	if p.NZBHash == "" {
		return nil, errors.New("download: nzb_hash required")
	}
	if p.Name == "" {
		return nil, errors.New("download: name required")
	}
	if len(p.Files) == 0 {
		return nil, errors.New("download: at least one file required")
	}

	j := &Job{
		nzbHash:    p.NZBHash,
		name:       p.Name,
		category:   p.Category,
		priority:   p.Priority,
		queueOrder: p.QueueOrder,
		state:      JobStateQueued,
		addedAt:    now,
		nzbBlob:    append([]byte(nil), p.NZBBlob...),
	}
	for _, fp := range p.Files {
		f := newFile(fp)
		j.files = append(j.files, f)
		j.totalBytes += f.sizeBytes
	}

	j.events = append(j.events, JobCreated{
		ID:         0,
		Name:       p.Name,
		Category:   p.Category,
		TotalBytes: j.totalBytes,
		At:         now,
	})
	return j, nil
}

// HydrateJobParams is what the repository hands back when loading a
// row tree. Files are pre-populated with their segments.
type HydrateJobParams struct {
	ID          JobID
	NZBHash     string
	Name        string
	Category    string
	Priority    int
	QueueOrder  int64
	State       JobState
	TotalBytes  int64
	DoneBytes   int64
	FailedBytes int64
	AddedAt     time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
	ErrorMsg    string
	NZBBlob     []byte
	Files       []*File
}

// HydrateJob reconstructs a Job from persistence. No events are
// emitted.
func HydrateJob(p HydrateJobParams) *Job {
	return &Job{
		id:          p.ID,
		nzbHash:     p.NZBHash,
		name:        p.Name,
		category:    p.Category,
		priority:    p.Priority,
		queueOrder:  p.QueueOrder,
		state:       p.State,
		totalBytes:  p.TotalBytes,
		doneBytes:   p.DoneBytes,
		failedBytes: p.FailedBytes,
		addedAt:     p.AddedAt,
		startedAt:   p.StartedAt,
		finishedAt:  p.FinishedAt,
		errorMsg:    p.ErrorMsg,
		nzbBlob:     append([]byte(nil), p.NZBBlob...),
		files:       p.Files,
	}
}

// Accessors.
func (j *Job) ID() JobID         { return j.id }
func (j *Job) NZBHash() string   { return j.nzbHash }
func (j *Job) Name() string      { return j.name }
func (j *Job) Category() string  { return j.category }
func (j *Job) Priority() int     { return j.priority }
func (j *Job) QueueOrder() int64 { return j.queueOrder }
func (j *Job) State() JobState   { return j.state }
func (j *Job) TotalBytes() int64 { return j.totalBytes }
func (j *Job) DoneBytes() int64  { return j.doneBytes }
func (j *Job) FailedBytes() int64 { return j.failedBytes }
func (j *Job) AddedAt() time.Time { return j.addedAt }
func (j *Job) StartedAt() time.Time { return j.startedAt }
func (j *Job) FinishedAt() time.Time { return j.finishedAt }
func (j *Job) ErrorMsg() string  { return j.errorMsg }
func (j *Job) NZBBlob() []byte   { return append([]byte(nil), j.nzbBlob...) }
func (j *Job) Files() []*File {
	out := make([]*File, len(j.files))
	copy(out, j.files)
	return out
}

// SetID is called by the repository after a successful insert.
// Patches the pending JobCreated event with the real id and pushes
// the id down into the file/segment children.
func (j *Job) SetID(id JobID) {
	j.id = id
	for i := range j.events {
		if e, ok := j.events[i].(JobCreated); ok && e.ID == 0 {
			e.ID = id
			j.events[i] = e
		}
	}
	for _, f := range j.files {
		f.SetJobID(id)
	}
}

// SetQueueOrder rewrites the queue-order position. No event is
// emitted — reorder is a UI-driven concern, the orchestrator picks up
// the new ordering on its next dispatch tick via the repository.
//
// Terminal jobs aren't filtered here; QueueService.Reorder is
// responsible for not feeding terminal IDs into the operation.
func (j *Job) SetQueueOrder(order int64) {
	j.queueOrder = order
}

// PullEvents returns and clears the pending event list.
func (j *Job) PullEvents() []event.Event {
	out := j.events
	j.events = nil
	return out
}

// MarkStarted transitions queued → downloading. No-op if already in
// downloading or a later state. Records JobStarted.
func (j *Job) MarkStarted(now time.Time) {
	if j.state != JobStateQueued {
		return
	}
	j.state = JobStateDownloading
	j.startedAt = now
	j.events = append(j.events, JobStarted{ID: j.id, At: now})
}

// Pause transitions downloading|queued → paused. The orchestrator
// observes the JobPaused event and stops dispatching segments for
// this job. Already-paused / terminal-state calls are no-ops.
func (j *Job) Pause(now time.Time) {
	switch j.state {
	case JobStateQueued, JobStateDownloading:
		j.state = JobStatePaused
		j.events = append(j.events, JobPaused{ID: j.id, At: now})
	}
}

// Resume transitions paused → downloading (or queued if no work has
// started yet). The orchestrator observes the JobResumed event and
// re-enables segment dispatch. Non-paused calls are no-ops.
func (j *Job) Resume(now time.Time) {
	if j.state != JobStatePaused {
		return
	}
	if j.startedAt.IsZero() {
		j.state = JobStateQueued
	} else {
		j.state = JobStateDownloading
	}
	j.events = append(j.events, JobResumed{ID: j.id, At: now})
}

// MarkRemoved is called by the application service before deleting
// the row. It records JobRemoved on the aggregate so the bus delivers
// the event in the same tx as the DELETE.
func (j *Job) MarkRemoved(now time.Time) {
	j.events = append(j.events, JobRemoved{ID: j.id, At: now})
}

// SegmentByID looks up a segment within the aggregate. Returns nil if
// the id is not part of this job.
func (j *Job) SegmentByID(id SegmentID) (*File, *Segment) {
	for _, f := range j.files {
		for _, s := range f.segments {
			if s.id == id {
				return f, s
			}
		}
	}
	return nil, nil
}

// SegmentResult is the outcome of one fetch attempt fed back into the
// aggregate. The orchestrator calls MarkSegmentDone, MarkSegmentMissing,
// or MarkSegmentFailed depending on the result.
type SegmentResult struct {
	SegmentID  SegmentID
	BytesOnDisk int64 // decoded bytes written
	FileOffset int64
}

// MarkSegmentDone records a successful fetch + decode + write. The
// segment moves to done; doneBytes increases; if the segment was the
// last pending one for its file the file moves to complete and a
// FileCompleted event is emitted; if it was the last for the job, the
// job moves to download_complete and JobDownloadComplete is emitted.
func (j *Job) MarkSegmentDone(r SegmentResult, now time.Time) error {
	f, s := j.SegmentByID(r.SegmentID)
	if s == nil {
		return fmt.Errorf("segment %d not in job %d", r.SegmentID, j.id)
	}
	if s.state == SegmentStateDone {
		return nil
	}
	s.state = SegmentStateDone
	s.lastError = ""
	s.fileOffset = r.FileOffset
	j.doneBytes += r.BytesOnDisk
	f.segmentsDone++

	j.events = append(j.events, SegmentCompleted{
		JobID:     j.id,
		FileID:    f.id,
		SegmentID: s.id,
		Bytes:     r.BytesOnDisk,
		At:        now,
	})

	if f.segmentsDone >= f.segmentCount && f.state != FileStateComplete {
		f.state = FileStateComplete
		j.events = append(j.events, FileCompleted{
			JobID: j.id, FileID: f.id, Filename: f.filename, At: now,
		})
	}

	if j.allSegmentsResolved() {
		j.completeDownloadPhase(now)
	}
	return nil
}

// MarkSegmentMissing records "every server returned 430". The segment
// is terminal-missing for download purposes; PAR2 may rescue the file.
func (j *Job) MarkSegmentMissing(segID SegmentID, now time.Time) error {
	f, s := j.SegmentByID(segID)
	if s == nil {
		return fmt.Errorf("segment %d not in job %d", segID, j.id)
	}
	if s.state.IsTerminal() {
		return nil
	}
	s.state = SegmentStateMissing
	s.lastError = "article missing on all servers"
	j.failedBytes += s.bytes
	j.events = append(j.events, SegmentMissing{
		JobID: j.id, FileID: f.id, SegmentID: s.id, At: now,
	})
	if j.allSegmentsResolved() {
		j.completeDownloadPhase(now)
	}
	return nil
}

// MarkSegmentFailed records a non-430 failure that exhausted the retry
// budget (network, parse, CRC). Like Missing, it's terminal.
func (j *Job) MarkSegmentFailed(segID SegmentID, errMsg string, now time.Time) error {
	f, s := j.SegmentByID(segID)
	if s == nil {
		return fmt.Errorf("segment %d not in job %d", segID, j.id)
	}
	if s.state.IsTerminal() {
		return nil
	}
	s.state = SegmentStateFailed
	s.lastError = errMsg
	j.failedBytes += s.bytes
	j.events = append(j.events, SegmentFailed{
		JobID: j.id, FileID: f.id, SegmentID: s.id, Err: errMsg, At: now,
	})
	if j.allSegmentsResolved() {
		j.completeDownloadPhase(now)
	}
	return nil
}

// MarkSegmentDispatched flips a pending segment to inflight and records
// the attempt. Returns an error if the segment is not pending.
func (j *Job) MarkSegmentDispatched(segID SegmentID, now time.Time) error {
	_, s := j.SegmentByID(segID)
	if s == nil {
		return fmt.Errorf("segment %d not in job %d", segID, j.id)
	}
	if s.state != SegmentStatePending {
		return fmt.Errorf("segment %d not pending: %s", segID, s.state)
	}
	s.state = SegmentStateInflight
	s.attempts++
	j.events = append(j.events, SegmentDispatched{
		JobID:     j.id,
		SegmentID: s.id,
		MessageID: s.messageID,
		Attempt:   s.attempts,
		At:        now,
	})
	return nil
}

// ResetInflightToPending is called at startup by the orchestrator to
// reset any segment left mid-fetch by a crash. No event is emitted —
// this is recovery hygiene, not a domain event.
func (j *Job) ResetInflightToPending() int {
	n := 0
	for _, f := range j.files {
		for _, s := range f.segments {
			if s.state == SegmentStateInflight {
				s.state = SegmentStatePending
				n++
			}
		}
	}
	return n
}

// PendingSegments returns segments awaiting dispatch. Useful for the
// orchestrator's per-job loop.
func (j *Job) PendingSegments() []*Segment {
	var out []*Segment
	for _, f := range j.files {
		for _, s := range f.segments {
			if s.state == SegmentStatePending {
				out = append(out, s)
			}
		}
	}
	return out
}

// allSegmentsResolved reports whether every segment has reached a
// terminal state.
func (j *Job) allSegmentsResolved() bool {
	for _, f := range j.files {
		for _, s := range f.segments {
			if !s.state.IsTerminal() {
				return false
			}
		}
	}
	return true
}

// completeDownloadPhase transitions the job to download_complete and
// records the appropriate event. If at least one file's segments all
// went done, the download succeeded; otherwise the orchestrator may
// still treat it as failed depending on PAR2 outcome (M3+).
func (j *Job) completeDownloadPhase(now time.Time) {
	j.state = JobStateDownloadComplete
	j.events = append(j.events, JobDownloadComplete{
		JobID:           j.id,
		MissingSegments: j.countMissingSegments(),
		At:              now,
	})
}

// MarkCompleted finalises the Job after delivery (or extract+deliver)
// has finished moving files into complete/. Idempotent: if the Job is
// already in a terminal state, no event is emitted.
func (j *Job) MarkCompleted(now time.Time) {
	if j.state == JobStateCompleted {
		return
	}
	j.state = JobStateCompleted
	j.finishedAt = now
	j.events = append(j.events, JobCompleted{
		JobID: j.id,
		At:    now,
	})
}

// MarkFailed transitions the Job to a terminal failed state with the
// given reason. Idempotent.
func (j *Job) MarkFailed(reason string, now time.Time) {
	if j.state == JobStateFailed {
		return
	}
	j.state = JobStateFailed
	j.errorMsg = reason
	j.finishedAt = now
	j.events = append(j.events, JobFailed{
		JobID: j.id,
		Err:   reason,
		At:    now,
	})
}

func (j *Job) countMissingSegments() int {
	n := 0
	for _, f := range j.files {
		for _, s := range f.segments {
			if s.state == SegmentStateMissing || s.state == SegmentStateFailed {
				n++
			}
		}
	}
	return n
}
