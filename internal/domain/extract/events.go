package extract

import (
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

// TopicPrefix scopes all events of this bounded context.
const TopicPrefix = "extract."

type ExtractQueued struct {
	ID        ExtractID      `json:"id"`
	JobID     download.JobID `json:"job_id"`
	TargetDir string         `json:"target_dir"`
	At        time.Time      `json:"at"`
}

func (e ExtractQueued) Topic() string         { return TopicPrefix + "queued" }
func (e ExtractQueued) AggregateID() string   { return aggID(e.ID) }
func (e ExtractQueued) OccurredAt() time.Time { return e.At }

type ExtractStarted struct {
	ID    ExtractID      `json:"id"`
	JobID download.JobID `json:"job_id"`
	At    time.Time      `json:"at"`
}

func (e ExtractStarted) Topic() string         { return TopicPrefix + "started" }
func (e ExtractStarted) AggregateID() string   { return aggID(e.ID) }
func (e ExtractStarted) OccurredAt() time.Time { return e.At }

type ExtractComplete struct {
	ID        ExtractID      `json:"id"`
	JobID     download.JobID `json:"job_id"`
	TargetDir string         `json:"target_dir"`
	At        time.Time      `json:"at"`
}

func (e ExtractComplete) Topic() string         { return TopicPrefix + "complete" }
func (e ExtractComplete) AggregateID() string   { return aggID(e.ID) }
func (e ExtractComplete) OccurredAt() time.Time { return e.At }

type ExtractFailed struct {
	ID    ExtractID      `json:"id"`
	JobID download.JobID `json:"job_id"`
	Err   string         `json:"err"`
	At    time.Time      `json:"at"`
}

func (e ExtractFailed) Topic() string         { return TopicPrefix + "failed" }
func (e ExtractFailed) AggregateID() string   { return aggID(e.ID) }
func (e ExtractFailed) OccurredAt() time.Time { return e.At }
