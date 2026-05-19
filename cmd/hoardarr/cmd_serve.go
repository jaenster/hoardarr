package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/jaenster/hoardarr"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	"github.com/jaenster/hoardarr/internal/logfile"
	"github.com/jaenster/hoardarr/internal/loghub"
)

// cmdServe runs the long-lived HTTP server. This is the default
// behaviour when no subcommand is provided.
func cmdServe(args []string, logger *slog.Logger) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", "./config.toml", "path to config.toml")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.LoadOrCreate(*configPath)
	if err != nil {
		return err
	}
	// Apply the config-requested log level to the process-wide LevelVar.
	// Config.Validate already restricts the set, so this is a total map.
	switch cfg.Server.LogLevel {
	case "debug":
		logLevel.Set(slog.LevelDebug)
	case "warn":
		logLevel.Set(slog.LevelWarn)
	case "error":
		logLevel.Set(slog.LevelError)
	default:
		logLevel.Set(slog.LevelInfo)
	}
	logger.Info("config loaded", "path", *configPath, "log_level", cfg.Server.LogLevel)

	// Tee slog output to <data_dir>/logs/hoardarr.log so operators can
	// download a real log file post-hoc (the loghub ring buffer only
	// holds the most recent ~1k lines). Failures here are non-fatal —
	// we keep going with stdout-only logging.
	logDir := filepath.Join(cfg.Server.DataDir, "logs")
	logWriter, err := logfile.Open(logDir)
	if err != nil {
		logger.Warn("logfile: file logging disabled", "err", err)
	} else {
		stdoutH := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
		fileH := slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: logLevel})
		base := newMultiHandler(stdoutH, fileH)
		logger = slog.New(loghub.NewHandler(base, logHub))
		slog.SetDefault(logger)
		defer logWriter.Close()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	v, c, d := buildInfo()
	app, err := bootstrap.Build(ctx, cfg, hoardarr.FrontendFS, logger,
		bootstrap.WithLogHub(logHub),
		bootstrap.WithConfigPath(*configPath),
		bootstrap.WithBuildInfo(v, c, d),
		bootstrap.WithLogDir(logDir),
	)
	if err != nil {
		return err
	}
	logger.Info("hoardarr", "version", v, "commit", c, "build_date", d)
	return app.Run(ctx)
}

// multiHandler fans a slog record out to several handlers. Used to
// tee logs to both stdout (operator's terminal) and the rotating
// file (post-hoc download).
type multiHandler struct {
	hs []slog.Handler
}

func newMultiHandler(hs ...slog.Handler) slog.Handler {
	return &multiHandler{hs: hs}
}

func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m.hs {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	var firstErr error
	for _, h := range m.hs {
		if !h.Enabled(ctx, r.Level) {
			continue
		}
		if err := h.Handle(ctx, r.Clone()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	hs := make([]slog.Handler, len(m.hs))
	for i, h := range m.hs {
		hs[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{hs: hs}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	hs := make([]slog.Handler, len(m.hs))
	for i, h := range m.hs {
		hs[i] = h.WithGroup(name)
	}
	return &multiHandler{hs: hs}
}

// silence unused-import warnings if logfile isn't reachable elsewhere
var _ io.Writer = (*logfile.Writer)(nil)
