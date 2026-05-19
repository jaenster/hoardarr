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
	// source is the requesting client (e.g. "Sonarr/4.0.5"). Captured
	// from the HTTP User-Agent on the upload endpoint; empty for
	// manual uploads. Lets the UI and webhook subscribers attribute
	// jobs to their *arr origin.
	source string

	totalBytes  int64
	doneBytes   int64
	failedBytes int64

	addedAt    time.Time
	startedAt  time.Time
	finishedAt time.Time

	errorMsg string
	nzbBlob  []byte

	// fetchRecoveryVols gates whether the orchestrator picks up
	// recovery-volume PAR2 files (those matching <base>.vol###+##.par2)
	// during normal download. When false, those files' segments are
	// hidden from PendingSegments and excluded from the
	// "all segments resolved" check, so JobDownloadComplete fires after
	// just the data + index PAR2 have landed. The repair worker flips
	// this to true (via RequestRecoveryVols) when par2.Repair finds
	// insufficient slices and there are deferred files to fetch.
	fetchRecoveryVols bool

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
	Source     string // requesting client UA (e.g. "Sonarr/4.x"); empty for manual
	NZBBlob    []byte
	Files      []NewFileParams
	// DeferRecoveryVols defers per-slice PAR2 recovery files
	// (`<base>.vol###+##.par2`) until repair needs them. The Job is
	// constructed with fetch_recovery_vols=false when this is true.
	// Default false → legacy behaviour: fetch every file eagerly.
	DeferRecoveryVols bool
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
		nzbHash:           p.NZBHash,
		name:              p.Name,
		category:          p.Category,
		priority:          p.Priority,
		queueOrder:        p.QueueOrder,
		source:            p.Source,
		state:             JobStateQueued,
		addedAt:           now,
		nzbBlob:           append([]byte(nil), p.NZBBlob...),
		fetchRecoveryVols: !p.DeferRecoveryVols,
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
	Source      string
	State       JobState
	TotalBytes  int64
	DoneBytes   int64
	FailedBytes int64
	AddedAt     time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
	ErrorMsg          string
	NZBBlob           []byte
	Files             []*File
	FetchRecoveryVols bool
}

// HydrateJob reconstructs a Job from persistence. No events are
// emitted.
func HydrateJob(p HydrateJobParams) *Job {
	return &Job{
		id:                p.ID,
		nzbHash:           p.NZBHash,
		name:              p.Name,
		category:          p.Category,
		priority:          p.Priority,
		queueOrder:        p.QueueOrder,
		source:            p.Source,
		state:             p.State,
		totalBytes:        p.TotalBytes,
		doneBytes:         p.DoneBytes,
		failedBytes:       p.FailedBytes,
		addedAt:           p.AddedAt,
		startedAt:         p.StartedAt,
		finishedAt:        p.FinishedAt,
		errorMsg:          p.ErrorMsg,
		nzbBlob:           append([]byte(nil), p.NZBBlob...),
		files:             p.Files,
		fetchRecoveryVols: p.FetchRecoveryVols,
	}
}

// Accessors.
func (j *Job) ID() JobID         { return j.id }
func (j *Job) NZBHash() string   { return j.nzbHash }
func (j *Job) Name() string      { return j.name }
func (j *Job) Category() string  { return j.category }
func (j *Job) Priority() int     { return j.priority }
func (j *Job) QueueOrder() int64 { return j.queueOrder }
func (j *Job) Source() string    { return j.source }
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

// MarkWaitingForServer parks the job because no usable NNTP server is
// currently configured. The orchestrator runner exits on this transition
// and the orchestrator's server-added handler will call ResumeFromWait
// once a server appears. Idempotent; ignored if the job is in any other
// state (paused/terminal/etc.).
func (j *Job) MarkWaitingForServer(reason string, now time.Time) {
	switch j.state {
	case JobStateQueued, JobStateDownloading:
	default:
		return
	}
	j.state = JobStateWaitingForServer
	j.events = append(j.events, JobWaitingForServer{
		JobID:  j.id,
		Reason: reason,
		At:     now,
	})
}

// ResumeFromWait undoes MarkWaitingForServer when a usable server
// becomes available. Goes back to queued (the orchestrator's normal
// path will flip to downloading once dispatch starts). Emits JobResumed
// so existing SSE / runner subscribers pick it up without a new topic.
// No-op outside waiting_for_server.
func (j *Job) ResumeFromWait(now time.Time) {
	if j.state != JobStateWaitingForServer {
		return
	}
	j.state = JobStateQueued
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

// AbortIfHopeless transitions the job to JobStateFailed when the
// failed-bytes ratio exceeds `threshold` (0.0–1.0). SABnzbd's
// fail_hopeless: stop burning bandwidth on a release that's already
// beyond PAR2's ability to repair. Returns true if the transition
// fired. Threshold ≤ 0 or ≥ 1 disables the check.
//
// Only fires during the download phase (queued / downloading /
// waiting_for_server). Once download_complete is reached the verify
// pipeline decides fate.
func (j *Job) AbortIfHopeless(threshold float64, now time.Time) bool {
	if threshold <= 0 || threshold >= 1 {
		return false
	}
	switch j.state {
	case JobStateQueued, JobStateDownloading, JobStateWaitingForServer:
	default:
		return false
	}
	if j.totalBytes <= 0 {
		return false
	}
	ratio := float64(j.failedBytes) / float64(j.totalBytes)
	if ratio < threshold {
		return false
	}
	reason := fmt.Sprintf("download aborted: %d%% missing exceeds %d%% threshold",
		int(ratio*100+0.5), int(threshold*100+0.5))
	j.state = JobStateFailed
	j.errorMsg = reason
	j.finishedAt = now
	j.events = append(j.events,
		JobDownloadFailed{JobID: j.id, Err: reason, At: now},
		JobFailed{JobID: j.id, Err: reason, At: now},
	)
	return true
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

// ResetFailedToPending flips every `failed` and `missing` segment back
// to `pending`, clearing per-segment error + attempt state so the
// orchestrator picks them up fresh. The operator-facing trigger is
// the RetryFailedSegments command; the typical workflow is "the
// release was temporarily unavailable; please try again".
//
// File-level state is recomputed by reset of segment counters at the
// repo layer the next time we save, so we don't touch File state here.
//
// Returns the number of segments reset.
func (j *Job) ResetFailedToPending() int {
	n := 0
	for _, f := range j.files {
		for _, s := range f.segments {
			if s.state == SegmentStateFailed || s.state == SegmentStateMissing {
				s.state = SegmentStatePending
				s.attempts = 0
				s.lastError = ""
				n++
			}
		}
	}
	// If the job itself terminated, kick it back to queued so the
	// orchestrator considers it again. Active jobs keep their state.
	switch j.state {
	case JobStateFailed, JobStateAborted:
		j.state = JobStateQueued
		j.finishedAt = time.Time{}
		j.errorMsg = ""
	}
	return n
}

// PendingSegments returns segments awaiting dispatch whose retry
// window has elapsed (`next_retry_at <= now`). Segments whose
// `next_retry_at` is still in the future are *deferred* — they remain
// in state=pending in the DB but are intentionally hidden so the
// orchestrator's worker pool doesn't burn capacity re-fetching
// articles whose servers just rejected them for being over-conns.
// Callers wanting to know whether deferred work exists (and when) ask
// NextRetryReadyAt separately.
//
// When fetch_recovery_vols is false, segments belonging to recovery-
// vol files are hidden — repair will flip the flag (via
// RequestRecoveryVols) and a re-entry of the orchestrator picks them
// up at that point.
func (j *Job) PendingSegments(now time.Time) []*Segment {
	var out []*Segment
	for _, f := range j.files {
		if f.isRecoveryVol && !j.fetchRecoveryVols {
			continue
		}
		for _, s := range f.segments {
			if s.state != SegmentStatePending {
				continue
			}
			// Zero next_retry_at means "never been retried" → ready.
			// Otherwise compare: the segment is ready when its retry
			// window has elapsed.
			if !s.nextRetryAt.IsZero() && s.nextRetryAt.After(now) {
				continue
			}
			out = append(out, s)
		}
	}
	return out
}

// NextRetryReadyAt returns the earliest instant at which a currently
// deferred pending segment becomes ready (i.e. `next_retry_at > now`
// today, but `<= now` after the returned time has passed). Returns
// the zero time if nothing is deferred — the orchestrator interprets
// that as "no deferred work left, you can exit".
//
// Recovery-vol gating mirrors PendingSegments so the orchestrator
// doesn't sleep waiting for vols it isn't supposed to fetch.
func (j *Job) NextRetryReadyAt(now time.Time) time.Time {
	var earliest time.Time
	for _, f := range j.files {
		if f.isRecoveryVol && !j.fetchRecoveryVols {
			continue
		}
		for _, s := range f.segments {
			if s.state != SegmentStatePending {
				continue
			}
			if s.nextRetryAt.IsZero() || !s.nextRetryAt.After(now) {
				continue
			}
			if earliest.IsZero() || s.nextRetryAt.Before(earliest) {
				earliest = s.nextRetryAt
			}
		}
	}
	return earliest
}

// MarkSegmentForRetry flips a segment back to pending with a future
// next_retry_at, so the orchestrator's poll loop will skip it until
// `at` has passed — even across a process restart. Used when a fetch
// attempt failed with a transient error (conn-limit, 5xx, network
// hiccup) and we've decided to defer rather than burn the segment's
// remaining retry budget right now.
//
// Returns an error if the id isn't part of this job. The segment must
// not already be in a terminal state — terminal-state segments don't
// re-enter the retry queue (use MarkSegmentMissing/MarkSegmentFailed
// for those instead).
func (j *Job) MarkSegmentForRetry(segID SegmentID, at time.Time, errMsg string) error {
	_, s := j.SegmentByID(segID)
	if s == nil {
		return fmt.Errorf("segment %d not in job %d", segID, j.id)
	}
	if s.state.IsTerminal() {
		return nil
	}
	s.MarkPendingRetry(at, errMsg)
	return nil
}

// allSegmentsResolved reports whether every segment has reached a
// terminal state. Recovery-vol segments are ignored when the job has
// not opted in to fetching them (mirrors PendingSegments).
func (j *Job) allSegmentsResolved() bool {
	for _, f := range j.files {
		if f.isRecoveryVol && !j.fetchRecoveryVols {
			continue
		}
		for _, s := range f.segments {
			if !s.state.IsTerminal() {
				return false
			}
		}
	}
	return true
}

// FetchRecoveryVols reports whether this Job's orchestrator should
// pick up recovery-volume segments. Initially set from the runtime
// "defer recovery vols" knob (inverted) at job creation; toggled to
// true by the repair worker via RequestRecoveryVols.
func (j *Job) FetchRecoveryVols() bool { return j.fetchRecoveryVols }

// HasDeferredRecoveryVols reports whether the Job still has recovery-
// vol files whose segments haven't been fetched (because
// fetch_recovery_vols=false). Used by the repair worker to decide
// whether on-demand fetching is even possible.
func (j *Job) HasDeferredRecoveryVols() bool {
	if j.fetchRecoveryVols {
		return false
	}
	for _, f := range j.files {
		if !f.isRecoveryVol {
			continue
		}
		for _, s := range f.segments {
			if s.state == SegmentStatePending {
				return true
			}
		}
	}
	return false
}

// RequestRecoveryVols flips fetch_recovery_vols=true and transitions
// the Job back to JobStateDownloading so the orchestrator picks up
// the deferred segments. Emits RecoveryVolsRequested. Returns an
// error if there's nothing to fetch (caller should fail the repair
// outright in that case).
func (j *Job) RequestRecoveryVols(now time.Time) error {
	if j.fetchRecoveryVols {
		return errors.New("download: recovery vols already requested")
	}
	if !j.HasDeferredRecoveryVols() {
		return errors.New("download: no deferred recovery vol segments")
	}
	j.fetchRecoveryVols = true
	// Reopen the active phase so the orchestrator's per-job runner
	// will pick up the now-visible pending segments. Anything past
	// download_complete (verifying/repairing/etc.) is moved back to
	// downloading; terminal states reject the request.
	switch j.state {
	case JobStateDownloadComplete, JobStateVerifying, JobStateRepairing:
		j.state = JobStateDownloading
	case JobStateCompleted, JobStateFailed, JobStateAborted:
		return fmt.Errorf("download: cannot request recovery vols from %s", j.state)
	}
	j.events = append(j.events, RecoveryVolsRequested{
		JobID: j.id,
		At:    now,
	})
	return nil
}

// completeDownloadPhase transitions the job to its post-download state.
//
// If nothing got through (doneBytes == 0), the download failed outright:
// PAR2 can't reconstruct anything from zero successful bytes, so we go
// straight to JobStateFailed and emit JobDownloadFailed. This covers the
// "no enabled pools" and "all-segments-missing" cases that would otherwise
// leave the job stuck in download_complete looking misleadingly green.
//
// Otherwise the job moves to download_complete and the verify worker
// decides fate from there (M3a/b: verify + repair).
func (j *Job) completeDownloadPhase(now time.Time) {
	if j.doneBytes == 0 {
		reason := "download failed: no segments retrieved"
		j.state = JobStateFailed
		j.errorMsg = reason
		j.finishedAt = now
		j.events = append(j.events, JobDownloadFailed{
			JobID: j.id,
			Err:   reason,
			At:    now,
		})
		j.events = append(j.events, JobFailed{
			JobID: j.id,
			Err:   reason,
			At:    now,
		})
		return
	}
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
