package deliver

import (
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

// TopicPrefix scopes all events of this bounded context.
const TopicPrefix = "deliver."

type DeliveryQueued struct {
	ID        DeliveryID     `json:"id"`
	JobID     download.JobID `json:"job_id"`
	TargetDir string         `json:"target_dir"`
	At        time.Time      `json:"at"`
}

func (e DeliveryQueued) Topic() string         { return TopicPrefix + "queued" }
func (e DeliveryQueued) AggregateID() string   { return aggID(e.ID) }
func (e DeliveryQueued) OccurredAt() time.Time { return e.At }

type DeliveryStarted struct {
	ID    DeliveryID     `json:"id"`
	JobID download.JobID `json:"job_id"`
	At    time.Time      `json:"at"`
}

func (e DeliveryStarted) Topic() string         { return TopicPrefix + "started" }
func (e DeliveryStarted) AggregateID() string   { return aggID(e.ID) }
func (e DeliveryStarted) OccurredAt() time.Time { return e.At }

type DeliveryComplete struct {
	ID        DeliveryID     `json:"id"`
	JobID     download.JobID `json:"job_id"`
	TargetDir string         `json:"target_dir"`
	At        time.Time      `json:"at"`
}

func (e DeliveryComplete) Topic() string         { return TopicPrefix + "complete" }
func (e DeliveryComplete) AggregateID() string   { return aggID(e.ID) }
func (e DeliveryComplete) OccurredAt() time.Time { return e.At }

type DeliveryFailed struct {
	ID    DeliveryID     `json:"id"`
	JobID download.JobID `json:"job_id"`
	Err   string         `json:"err"`
	At    time.Time      `json:"at"`
}

func (e DeliveryFailed) Topic() string         { return TopicPrefix + "failed" }
func (e DeliveryFailed) AggregateID() string   { return aggID(e.ID) }
func (e DeliveryFailed) OccurredAt() time.Time { return e.At }

// DeliverySkipped — emitted when an archive job is observed; the
// extract worker (M4 RAR) takes over from here and emits its own
// completion event.
type DeliverySkipped struct {
	ID    DeliveryID     `json:"id"`
	JobID download.JobID `json:"job_id"`
	At    time.Time      `json:"at"`
}

func (e DeliverySkipped) Topic() string         { return TopicPrefix + "skipped" }
func (e DeliverySkipped) AggregateID() string   { return aggID(e.ID) }
func (e DeliverySkipped) OccurredAt() time.Time { return e.At }
