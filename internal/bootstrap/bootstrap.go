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

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/config"
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

	Server     *server.Server
	HTTPServer *http.Server

	shutdownOnce sync.Once
	shutdownErr  error
}

// Build wires the runtime: ensures data directories exist, opens the
// SQLite database, applies migrations, constructs the transaction
// manager and outbox event bus, and prepares the HTTP server.
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

	srv := server.New(cfg, logger, frontendFS)
	httpSrv := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}

	return &App{
		Cfg:        cfg,
		Logger:     logger,
		DB:         db,
		TxMgr:      txm,
		Bus:        bus,
		Server:     srv,
		HTTPServer: httpSrv,
	}, nil
}

// Run blocks until ctx is cancelled or the HTTP server fails to listen.
//
// On ctx cancellation, Run calls Shutdown and returns its error.
// On HTTP failure (other than ErrServerClosed), Run returns the failure
// without calling Shutdown — the caller decides what to do.
func (a *App) Run(ctx context.Context) error {
	a.Logger.Info(
		"hoardarr starting",
		"listen", a.Cfg.Server.Listen,
		"data_dir", a.Cfg.Server.DataDir,
		"db", a.Cfg.Storage.SQLite.Path,
	)

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

// Shutdown stops the HTTP server, closes the event bus, and closes the
// database in that order. The first error encountered is recorded and
// returned, but all steps are attempted.
//
// Shutdown is safe to call multiple times — only the first call performs
// work; subsequent calls return the same error from the first call.
func (a *App) Shutdown() error {
	a.shutdownOnce.Do(func() {
		a.Logger.Info("hoardarr stopping")

		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := a.HTTPServer.Shutdown(sctx); err != nil {
			a.shutdownErr = fmt.Errorf("http shutdown: %w", err)
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
