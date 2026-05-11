package download

import (
	"strconv"
	"time"
)

// Topic prefix for download-context events.
const TopicPrefix = "download."

func jobAggregateID(id JobID) string {
	return strconv.FormatInt(int64(id), 10)
}

// JobCreated — emitted when a fresh Job is persisted.
type JobCreated struct {
	ID         JobID     `json:"id"`
	Name       string    `json:"name"`
	Category   string    `json:"category"`
	TotalBytes int64     `json:"total_bytes"`
	At         time.Time `json:"at"`
}

func (e JobCreated) Topic() string         { return TopicPrefix + "job.created" }
func (e JobCreated) AggregateID() string   { return jobAggregateID(e.ID) }
func (e JobCreated) OccurredAt() time.Time { return e.At }

// JobStarted — first transition from queued to downloading.
type JobStarted struct {
	ID JobID     `json:"id"`
	At time.Time `json:"at"`
}

func (e JobStarted) Topic() string         { return TopicPrefix + "job.started" }
func (e JobStarted) AggregateID() string   { return jobAggregateID(e.ID) }
func (e JobStarted) OccurredAt() time.Time { return e.At }

// JobPaused — user pauses the queue.
type JobPaused struct {
	ID JobID     `json:"id"`
	At time.Time `json:"at"`
}

func (e JobPaused) Topic() string         { return TopicPrefix + "job.paused" }
func (e JobPaused) AggregateID() string   { return jobAggregateID(e.ID) }
func (e JobPaused) OccurredAt() time.Time { return e.At }

// JobResumed — user resumes the queue.
type JobResumed struct {
	ID JobID     `json:"id"`
	At time.Time `json:"at"`
}

func (e JobResumed) Topic() string         { return TopicPrefix + "job.resumed" }
func (e JobResumed) AggregateID() string   { return jobAggregateID(e.ID) }
func (e JobResumed) OccurredAt() time.Time { return e.At }

// JobRemoved — user deletes the job.
type JobRemoved struct {
	ID JobID     `json:"id"`
	At time.Time `json:"at"`
}

func (e JobRemoved) Topic() string         { return TopicPrefix + "job.removed" }
func (e JobRemoved) AggregateID() string   { return jobAggregateID(e.ID) }
func (e JobRemoved) OccurredAt() time.Time { return e.At }

// SegmentDispatched — a worker accepted this segment for fetch.
// MessageID is included so the UI can show "fetching <msg-id>" on
// the active job without a follow-up lookup. Empty for legacy
// events emitted before the field was added.
type SegmentDispatched struct {
	JobID     JobID     `json:"job_id"`
	SegmentID SegmentID `json:"segment_id"`
	MessageID string    `json:"message_id,omitempty"`
	Attempt   int       `json:"attempt"`
	At        time.Time `json:"at"`
}

func (e SegmentDispatched) Topic() string         { return TopicPrefix + "segment.dispatched" }
func (e SegmentDispatched) AggregateID() string   { return jobAggregateID(e.JobID) }
func (e SegmentDispatched) OccurredAt() time.Time { return e.At }

// SegmentCompleted — segment fetched, decoded, and persisted to disk.
type SegmentCompleted struct {
	JobID     JobID     `json:"job_id"`
	FileID    FileID    `json:"file_id"`
	SegmentID SegmentID `json:"segment_id"`
	Bytes     int64     `json:"bytes"`
	At        time.Time `json:"at"`
}

func (e SegmentCompleted) Topic() string         { return TopicPrefix + "segment.completed" }
func (e SegmentCompleted) AggregateID() string   { return jobAggregateID(e.JobID) }
func (e SegmentCompleted) OccurredAt() time.Time { return e.At }

// SegmentMissing — every configured server returned 430 for this
// article. Will be re-investigated by the verify/repair pipeline.
type SegmentMissing struct {
	JobID     JobID     `json:"job_id"`
	FileID    FileID    `json:"file_id"`
	SegmentID SegmentID `json:"segment_id"`
	At        time.Time `json:"at"`
}

func (e SegmentMissing) Topic() string         { return TopicPrefix + "segment.missing" }
func (e SegmentMissing) AggregateID() string   { return jobAggregateID(e.JobID) }
func (e SegmentMissing) OccurredAt() time.Time { return e.At }

// SegmentFailed — non-430 retry budget exhausted (network, decode, CRC).
type SegmentFailed struct {
	JobID     JobID     `json:"job_id"`
	FileID    FileID    `json:"file_id"`
	SegmentID SegmentID `json:"segment_id"`
	Err       string    `json:"err"`
	At        time.Time `json:"at"`
}

func (e SegmentFailed) Topic() string         { return TopicPrefix + "segment.failed" }
func (e SegmentFailed) AggregateID() string   { return jobAggregateID(e.JobID) }
func (e SegmentFailed) OccurredAt() time.Time { return e.At }

// FileCompleted — every segment of this file is terminal-done.
type FileCompleted struct {
	JobID    JobID     `json:"job_id"`
	FileID   FileID    `json:"file_id"`
	Filename string    `json:"filename"`
	At       time.Time `json:"at"`
}

func (e FileCompleted) Topic() string         { return TopicPrefix + "file.completed" }
func (e FileCompleted) AggregateID() string   { return jobAggregateID(e.JobID) }
func (e FileCompleted) OccurredAt() time.Time { return e.At }

// JobDownloadComplete — every segment in the job has resolved (done,
// missing, or failed). Triggers verify in M3.
type JobDownloadComplete struct {
	JobID           JobID     `json:"job_id"`
	MissingSegments int       `json:"missing_segments"`
	At              time.Time `json:"at"`
}

func (e JobDownloadComplete) Topic() string         { return TopicPrefix + "job.download_complete" }
func (e JobDownloadComplete) AggregateID() string   { return jobAggregateID(e.JobID) }
func (e JobDownloadComplete) OccurredAt() time.Time { return e.At }

// JobDownloadFailed — fatal error during download (e.g. all servers
// disabled, disk full). Triggers history with error state.
type JobDownloadFailed struct {
	JobID JobID     `json:"job_id"`
	Err   string    `json:"err"`
	At    time.Time `json:"at"`
}

func (e JobDownloadFailed) Topic() string         { return TopicPrefix + "job.download_failed" }
func (e JobDownloadFailed) AggregateID() string   { return jobAggregateID(e.JobID) }
func (e JobDownloadFailed) OccurredAt() time.Time { return e.At }

// JobCompleted — terminal success: files have been verified and
// moved into complete/<category>/<release>/. Emitted by MarkCompleted.
type JobCompleted struct {
	JobID JobID     `json:"job_id"`
	At    time.Time `json:"at"`
}

func (e JobCompleted) Topic() string         { return TopicPrefix + "job.completed" }
func (e JobCompleted) AggregateID() string   { return jobAggregateID(e.JobID) }
func (e JobCompleted) OccurredAt() time.Time { return e.At }

// JobFailed — terminal failure. Distinct from JobDownloadFailed:
// JobDownloadFailed covers download-phase errors only; JobFailed is
// any post-processing service deciding the Job will not recover.
type JobFailed struct {
	JobID JobID     `json:"job_id"`
	Err   string    `json:"err"`
	At    time.Time `json:"at"`
}

func (e JobFailed) Topic() string         { return TopicPrefix + "job.failed" }
func (e JobFailed) AggregateID() string   { return jobAggregateID(e.JobID) }
func (e JobFailed) OccurredAt() time.Time { return e.At }
