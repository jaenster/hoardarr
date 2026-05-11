package main

import (
	"context"
	"flag"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/jaenster/hoardarr"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := bootstrap.Build(ctx, cfg, hoardarr.FrontendFS, logger,
		bootstrap.WithLogHub(logHub),
		bootstrap.WithConfigPath(*configPath),
	)
	if err != nil {
		return err
	}
	return app.Run(ctx)
}
