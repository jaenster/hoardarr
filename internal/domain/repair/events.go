package repair

import (
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

// TopicPrefix scopes all events of this bounded context.
const TopicPrefix = "repair."

type RepairQueued struct {
	ID    RepairID       `json:"id"`
	JobID download.JobID `json:"job_id"`
	At    time.Time      `json:"at"`
}

func (e RepairQueued) Topic() string         { return TopicPrefix + "queued" }
func (e RepairQueued) AggregateID() string   { return aggID(e.ID) }
func (e RepairQueued) OccurredAt() time.Time { return e.At }

type RepairStarted struct {
	ID    RepairID       `json:"id"`
	JobID download.JobID `json:"job_id"`
	At    time.Time      `json:"at"`
}

func (e RepairStarted) Topic() string         { return TopicPrefix + "started" }
func (e RepairStarted) AggregateID() string   { return aggID(e.ID) }
func (e RepairStarted) OccurredAt() time.Time { return e.At }

// RepairOK fires after reconstruction + post-repair MD5 check both
// succeeded for every previously-damaged file. The verify worker
// picks this up to re-emit VerifyOK so deliver/extract take over.
type RepairOK struct {
	ID    RepairID       `json:"id"`
	JobID download.JobID `json:"job_id"`
	At    time.Time      `json:"at"`
}

func (e RepairOK) Topic() string         { return TopicPrefix + "ok" }
func (e RepairOK) AggregateID() string   { return aggID(e.ID) }
func (e RepairOK) OccurredAt() time.Time { return e.At }

type RepairFailed struct {
	ID    RepairID       `json:"id"`
	JobID download.JobID `json:"job_id"`
	Err   string         `json:"err"`
	At    time.Time      `json:"at"`
}

func (e RepairFailed) Topic() string         { return TopicPrefix + "failed" }
func (e RepairFailed) AggregateID() string   { return aggID(e.ID) }
func (e RepairFailed) OccurredAt() time.Time { return e.At }
