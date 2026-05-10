package verify

import (
	"strconv"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
)

// Topic prefix for the verify bounded context.
const TopicPrefix = "verify."

func aggID(id VerifySetID) string {
	return strconv.FormatInt(int64(id), 10)
}

// VerifyStarted fires when a worker transitions a VerifySet from
// pending → verifying. UI subscribers light up "Verifying" status.
type VerifyStarted struct {
	ID    VerifySetID    `json:"id"`
	JobID download.JobID `json:"job_id"`
	At    time.Time      `json:"at"`
}

func (e VerifyStarted) Topic() string         { return TopicPrefix + "started" }
func (e VerifyStarted) AggregateID() string   { return aggID(e.ID) }
func (e VerifyStarted) OccurredAt() time.Time { return e.At }

// VerifyOK — every file's checksum matched. The extract worker (M4)
// subscribes to this to start unrar.
type VerifyOK struct {
	ID    VerifySetID    `json:"id"`
	JobID download.JobID `json:"job_id"`
	At    time.Time      `json:"at"`
}

func (e VerifyOK) Topic() string         { return TopicPrefix + "ok" }
func (e VerifyOK) AggregateID() string   { return aggID(e.ID) }
func (e VerifyOK) OccurredAt() time.Time { return e.At }

// RepairNeeded — at least one file's checksum disagreed. M3b's repair
// worker subscribes and starts the GF(2^16) reconstruction.
type RepairNeeded struct {
	ID          VerifySetID    `json:"id"`
	JobID       download.JobID `json:"job_id"`
	FailedFiles []string       `json:"failed_files"`
	At          time.Time      `json:"at"`
}

func (e RepairNeeded) Topic() string         { return TopicPrefix + "repair_needed" }
func (e RepairNeeded) AggregateID() string   { return aggID(e.ID) }
func (e RepairNeeded) OccurredAt() time.Time { return e.At }

// VerifyFailed — couldn't run verification at all (e.g. PAR2 files
// themselves missing or unparseable). Terminal.
type VerifyFailed struct {
	ID    VerifySetID    `json:"id"`
	JobID download.JobID `json:"job_id"`
	Err   string         `json:"err"`
	At    time.Time      `json:"at"`
}

func (e VerifyFailed) Topic() string         { return TopicPrefix + "failed" }
func (e VerifyFailed) AggregateID() string   { return aggID(e.ID) }
func (e VerifyFailed) OccurredAt() time.Time { return e.At }
