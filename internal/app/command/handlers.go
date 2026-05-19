package command

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntptest"
	"github.com/jaenster/hoardarr/internal/domain/download"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
)

// HealthRefresher is the slice of app/health.Service this handler
// needs. Defined here so the dependency stays narrow.
type HealthRefresher interface {
	Refresh()
}

// PingHandler is the minimal smoke-test command — useful to verify
// the worker is alive and the UI plumbing renders Completed/Failed
// correctly. Sleeps briefly so the operator sees the "running" state
// transition rather than a sub-frame flicker.
func PingHandler() Handler {
	return func(ctx context.Context, _ []byte) error {
		select {
		case <-time.After(150 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// HealthRecheckHandler is a useful built-in: triggers an immediate
// re-run of all registered health checks. Equivalent to clicking the
// per-banner refresh button but available from the Commands UI so
// the operator can issue it from anywhere.
func HealthRecheckHandler(h HealthRefresher) Handler {
	return func(ctx context.Context, _ []byte) error {
		h.Refresh()
		// brief settle so the snapshot lands before the command marks
		// completed (otherwise the UI may show the command done but
		// the banner unchanged).
		select {
		case <-time.After(200 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// --- RetryFailedSegments ---------------------------------------------

// JobRepository is the slice of the download repo the RetryFailedSegments
// handler needs. Narrow port; tests can pass a fake.
type JobRepository interface {
	ByID(ctx context.Context, id download.JobID) (*download.Job, error)
	Save(ctx context.Context, j *download.Job) error
}

// RetrySegmentsBody is the JSON shape submitted with the command.
type RetrySegmentsBody struct {
	JobID int64 `json:"job_id"`
}

// RetryFailedSegmentsHandler flips a job's failed + missing segments
// back to pending and clears their per-segment retry state. The
// orchestrator's pending-segments query picks them up on its next
// tick, so the operator gets "kick the wheel" semantics: useful when
// a release was temporarily unavailable and the operator wants a
// fresh attempt without re-uploading the NZB.
//
// If the job had already terminated (failed/aborted), this also flips
// it back to queued so the orchestrator considers it again.
func RetryFailedSegmentsHandler(repo JobRepository, logger *slog.Logger) Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, body []byte) error {
		var b RetrySegmentsBody
		if len(body) > 0 {
			if err := json.Unmarshal(body, &b); err != nil {
				return fmt.Errorf("parse body: %w", err)
			}
		}
		if b.JobID <= 0 {
			return fmt.Errorf("job_id required")
		}
		j, err := repo.ByID(ctx, download.JobID(b.JobID))
		if err != nil {
			return fmt.Errorf("load job %d: %w", b.JobID, err)
		}
		if j == nil {
			return fmt.Errorf("job %d not found", b.JobID)
		}
		n := j.ResetFailedToPending()
		if n == 0 {
			logger.Info("retry: nothing to reset", "job_id", b.JobID, "state", j.State())
			return nil
		}
		if err := repo.Save(ctx, j); err != nil {
			return fmt.Errorf("save job: %w", err)
		}
		logger.Info("retry: reset failed/missing segments", "job_id", b.JobID, "count", n)
		return nil
	}
}

// --- ReprobeAllServers -----------------------------------------------

// ServerLister is the slice of the server repo this handler needs.
type ServerLister interface {
	List(ctx context.Context) ([]*domainserver.UsenetServer, error)
}

// ReprobeAllServersHandler runs an NNTP handshake against every
// enabled server and logs the result. Surfaces failures via the
// command's error field so the operator sees them in the Commands
// table; per-server detail goes to the log file.
func ReprobeAllServersHandler(repo ServerLister, logger *slog.Logger) Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, _ []byte) error {
		servers, err := repo.List(ctx)
		if err != nil {
			return fmt.Errorf("list servers: %w", err)
		}
		var probed, failed int
		var failureNames []string
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, s := range servers {
			if !s.Enabled() {
				continue
			}
			wg.Add(1)
			go func(s *domainserver.UsenetServer) {
				defer wg.Done()
				res := nntptest.Probe(ctx, nntptest.Params{
					Host:     s.Host(),
					Port:     s.Port(),
					TLS:      s.TLS(),
					Username: s.Username(),
					Password: s.Password(),
				})
				mu.Lock()
				probed++
				if !res.OK {
					failed++
					failureNames = append(failureNames, s.Name())
				}
				mu.Unlock()
				if res.OK {
					logger.Info("reprobe: ok", "server", s.Name(), "elapsed", res.Elapsed)
				} else {
					logger.Warn("reprobe: failed", "server", s.Name(), "err", res.Err)
				}
			}(s)
		}
		wg.Wait()
		if failed > 0 {
			return fmt.Errorf("%d/%d servers failed: %v", failed, probed, failureNames)
		}
		logger.Info("reprobe: all servers ok", "count", probed)
		return nil
	}
}

// --- Pause/Resume all -------------------------------------------------

// QueueAdmin is the slice of QueueService the bulk handlers need.
type QueueAdmin interface {
	Active(ctx context.Context) ([]*download.Job, error)
	PauseJob(ctx context.Context, id download.JobID) error
	ResumeJob(ctx context.Context, id download.JobID) error
}

// PauseAllHandler pauses every active job in one shot.
func PauseAllHandler(q QueueAdmin, logger *slog.Logger) Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, _ []byte) error {
		jobs, err := q.Active(ctx)
		if err != nil {
			return fmt.Errorf("list active: %w", err)
		}
		var paused int
		for _, j := range jobs {
			if j.State() == download.JobStateDownloading || j.State() == download.JobStateQueued {
				if err := q.PauseJob(ctx, j.ID()); err != nil {
					logger.Warn("pause-all: skip", "job_id", int64(j.ID()), "err", err)
					continue
				}
				paused++
			}
		}
		logger.Info("pause-all: paused jobs", "count", paused)
		return nil
	}
}

// ResumeAllHandler is the inverse — resumes everything paused.
func ResumeAllHandler(q QueueAdmin, logger *slog.Logger) Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, _ []byte) error {
		jobs, err := q.Active(ctx)
		if err != nil {
			return fmt.Errorf("list active: %w", err)
		}
		var resumed int
		for _, j := range jobs {
			if j.State() == download.JobStatePaused {
				if err := q.ResumeJob(ctx, j.ID()); err != nil {
					logger.Warn("resume-all: skip", "job_id", int64(j.ID()), "err", err)
					continue
				}
				resumed++
			}
		}
		logger.Info("resume-all: resumed jobs", "count", resumed)
		return nil
	}
}
