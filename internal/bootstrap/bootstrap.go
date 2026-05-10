// Package bootstrap is hoardarr's composition root.
//
// It is the only place that imports concrete adapter packages and wires
// them to domain ports. main() does nothing but parse flags, load config,
// install signal handlers, and call into bootstrap.
//
// The split exists so command-line entry stays tiny and testable, and so
// alternative entry points (e.g. an in-process integration-test harness)
// can wire the same App without duplicating logic.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/api/rest"
	"github.com/jaenster/hoardarr/internal/api/sse"
	appdownload "github.com/jaenster/hoardarr/internal/app/download"
	appserver "github.com/jaenster/hoardarr/internal/app/server"
	"github.com/jaenster/hoardarr/internal/config"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
	"github.com/jaenster/hoardarr/internal/server"
)

// App is the wired-together hoardarr runtime. Construct via Build, run
// via Run, tear down via Shutdown.
type App struct {
	Cfg    config.Config
	Logger *slog.Logger

	DB    *sqlite.DB
	TxMgr *sqlite.TxManager
	Bus   *sqlite.OutboxBus

	ServerRepo *sqlite.ServerRepo
	JobRepo    *sqlite.JobRepo

	ServerService *appserver.Service
	AddJobService *appdownload.AddJobService
	QueueService  *appdownload.QueueService

	Pools        map[domainserver.ServerID]*nntp.Pool
	Orchestrator *appdownload.OrchestratorService

	LiveHub *sse.Hub

	HTTP       *server.Server
	HTTPServer *http.Server

	shutdownOnce sync.Once
	shutdownErr  error
}

// Build wires the runtime: ensures data directories exist, opens the
// SQLite database, applies migrations, constructs the transaction
// manager and outbox event bus, builds NNTP pools for enabled servers,
// constructs the orchestrator service, and prepares the HTTP server.
//
// On error, all partially-initialised resources are released before
// returning. Callers may pass the returned App to Run.
func Build(ctx context.Context, cfg config.Config, frontendFS fs.FS, logger *slog.Logger) (*App, error) {
	if logger == nil {
		logger = slog.Default()
	}

	if err := ensureDirs(cfg); err != nil {
		return nil, err
	}

	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if err := db.Migrate(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	txm := sqlite.NewTxManager(db)
	bus := sqlite.NewOutboxBus(db, sqlite.OutboxOptions{Logger: logger})

	serverRepo := sqlite.NewServerRepo(db)
	jobRepo := sqlite.NewJobRepo(db)

	serverService := appserver.New(serverRepo, bus, txm, nil)
	addJobService := appdownload.NewAddJobService(jobRepo, bus, txm, nil)
	queueService := appdownload.NewQueueService(appdownload.QueueServiceParams{
		Repo:          jobRepo,
		Bus:           bus,
		TxManager:     txm,
		Logger:        logger,
		IncompleteDir: cfg.Paths.IncompleteDir,
	})

	pools, err := buildPools(ctx, serverRepo, logger)
	if err != nil {
		_ = bus.Close()
		_ = db.Close()
		return nil, fmt.Errorf("build pools: %w", err)
	}

	orch := appdownload.NewOrchestratorService(appdownload.OrchestratorServiceParams{
		Repo:          jobRepo,
		Bus:           bus,
		TxManager:     txm,
		Pools:         pools,
		IncompleteDir: cfg.Paths.IncompleteDir,
		Logger:        logger,
	})

	categoryRepo := sqlite.NewCategoryRepo(db)
	liveHub, err := sse.NewHub(bus, sse.DefaultTopics, logger)
	if err != nil {
		closePools(pools)
		_ = bus.Close()
		_ = db.Close()
		return nil, fmt.Errorf("live hub: %w", err)
	}

	srv := server.New(cfg, logger, frontendFS)
	srv.MountREST(&rest.Handlers{
		Queue:      queueService,
		AddJob:     addJobService,
		Servers:    serverService,
		Categories: categoryRepo,
		Logger:     logger,
	})
	srv.MountSSE(liveHub)
	httpSrv := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}

	return &App{
		Cfg:           cfg,
		Logger:        logger,
		DB:            db,
		TxMgr:         txm,
		Bus:           bus,
		ServerRepo:    serverRepo,
		JobRepo:       jobRepo,
		ServerService: serverService,
		AddJobService: addJobService,
		QueueService:  queueService,
		Pools:         pools,
		Orchestrator:  orch,
		LiveHub:       liveHub,
		HTTP:          srv,
		HTTPServer:    httpSrv,
	}, nil
}

func closePools(pools map[domainserver.ServerID]*nntp.Pool) {
	for _, p := range pools {
		_ = p.Close()
	}
}

// Run starts the orchestrator service and the HTTP listener, blocking
// until ctx is cancelled or HTTP fails.
//
// On ctx cancellation, Run calls Shutdown and returns its error.
// On HTTP failure (other than ErrServerClosed), Run returns the
// failure; the caller decides whether to call Shutdown.
func (a *App) Run(ctx context.Context) error {
	a.Logger.Info(
		"hoardarr starting",
		"listen", a.Cfg.Server.Listen,
		"data_dir", a.Cfg.Server.DataDir,
		"db", a.Cfg.Storage.SQLite.Path,
	)

	if err := a.Orchestrator.Start(ctx); err != nil {
		return fmt.Errorf("start orchestrator: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		if err := a.HTTPServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http listen: %w", err)
	case <-ctx.Done():
		return a.Shutdown()
	}
}

// Shutdown stops in this order: HTTP server, orchestrator service,
// NNTP pools, event bus, database. The first error encountered is
// recorded and returned, but all steps are attempted.
//
// Idempotent: only the first call performs work; later calls return
// the same error.
func (a *App) Shutdown() error {
	a.shutdownOnce.Do(func() {
		a.Logger.Info("hoardarr stopping")

		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := a.HTTPServer.Shutdown(sctx); err != nil {
			a.shutdownErr = fmt.Errorf("http shutdown: %w", err)
		}
		if a.LiveHub != nil {
			if err := a.LiveHub.Close(); err != nil && a.shutdownErr == nil {
				a.shutdownErr = fmt.Errorf("live hub close: %w", err)
			}
		}
		if err := a.Orchestrator.Stop(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("orchestrator stop: %w", err)
		}
		for id, p := range a.Pools {
			if err := p.Close(); err != nil && a.shutdownErr == nil {
				a.shutdownErr = fmt.Errorf("pool close (server %d): %w", id, err)
			}
		}
		if err := a.Bus.Close(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("bus close: %w", err)
		}
		if err := a.DB.Close(); err != nil && a.shutdownErr == nil {
			a.shutdownErr = fmt.Errorf("db close: %w", err)
		}
	})
	return a.shutdownErr
}

func ensureDirs(cfg config.Config) error {
	dirs := []string{
		cfg.Server.DataDir,
		cfg.Paths.IncompleteDir,
		cfg.Paths.CompleteDir,
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("create dir %q: %w", d, err)
		}
	}
	return nil
}

func openDB(ctx context.Context, cfg config.Config) (*sqlite.DB, error) {
	switch cfg.Storage.Backend {
	case "sqlite":
		db, err := sqlite.Open(ctx, cfg.Storage.SQLite.Path, sqlite.Options{})
		if err != nil {
			return nil, fmt.Errorf("open sqlite: %w", err)
		}
		return db, nil
	default:
		return nil, fmt.Errorf("unsupported storage.backend %q", cfg.Storage.Backend)
	}
}

// buildPools opens an nntp.Pool for every enabled server in the
// registry. Empty result is fine — orchestrator tolerates "no servers
// yet" and just sits idle.
func buildPools(ctx context.Context, repo *sqlite.ServerRepo, logger *slog.Logger) (map[domainserver.ServerID]*nntp.Pool, error) {
	servers, err := repo.ListEnabled(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[domainserver.ServerID]*nntp.Pool, len(servers))
	for _, s := range servers {
		out[s.ID()] = nntp.NewPool(s, nntp.PoolOptions{Logger: logger})
	}
	return out, nil
}
