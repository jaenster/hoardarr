package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	appdownload "github.com/jaenster/hoardarr/internal/app/download"
)

// cmdDownload runs one NZB to download_complete and exits.
//
// Workflow:
//  1. Load config + bootstrap App.
//  2. Read the NZB file.
//  3. AddJob (parses, dedupes, persists, emits JobCreated).
//  4. Pick the highest-priority enabled server from the registry.
//  5. Build NNTP pool + PoolFetcher + Orchestrator.
//  6. Orchestrator.Run drives the job to completion.
//  7. Print a brief summary.
func cmdDownload(args []string, logger *slog.Logger) error {
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	configPath := fs.String("config", "./config.toml", "path to config.toml")
	category := fs.String("cat", "", "category (default: empty)")
	priority := fs.Int("priority", 0, "queue priority (lower = higher)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("download: <nzb-path> required")
	}
	nzbPath := rest[0]

	app, err := loadApp(*configPath, logger)
	if err != nil {
		return err
	}
	defer func() { _ = app.Shutdown() }()

	servers, err := app.ServerService.ListEnabled(context.Background())
	if err != nil {
		return fmt.Errorf("list servers: %w", err)
	}
	if len(servers) == 0 {
		return errors.New("download: no enabled servers configured (use `hoardarr server add`)")
	}
	srv := servers[0] // M1: highest priority only

	nzb, err := os.Open(nzbPath)
	if err != nil {
		return fmt.Errorf("open nzb: %w", err)
	}
	defer nzb.Close()

	jobID, err := app.AddJobService.AddJob(context.Background(), appdownload.AddJobCmd{
		NZB:      nzb,
		Category: *category,
		Priority: *priority,
	})
	if err != nil {
		if errors.Is(err, appdownload.ErrDuplicateNZB) {
			logger.Info("nzb already queued; resuming", "job_id", jobID)
		} else {
			return err
		}
	} else {
		logger.Info("nzb queued", "job_id", jobID)
	}

	pool := nntp.NewPool(srv, nntp.PoolOptions{Logger: logger})
	defer pool.Close()
	fetcher := appdownload.NewPoolFetcher(pool)

	jobDir := filepath.Join(app.Cfg.Paths.IncompleteDir)
	orch := appdownload.NewOrchestrator(
		app.JobRepo, fetcher, app.Bus, app.TxMgr,
		srv.ID(), srv.MaxConns(), jobDir,
		appdownload.OrchestratorOptions{Logger: logger},
	)
	if err := orch.Run(context.Background(), jobID); err != nil {
		return fmt.Errorf("orchestrator: %w", err)
	}

	final, err := app.JobRepo.ByID(context.Background(), jobID)
	if err != nil {
		return err
	}
	logger.Info("download complete",
		"job_id", final.ID(),
		"state", final.State(),
		"done_bytes", final.DoneBytes(),
		"total_bytes", final.TotalBytes(),
		"failed_bytes", final.FailedBytes(),
	)
	return nil
}
