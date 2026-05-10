// hoardarr — Go reimplementation of SABnzbd with a Sonarr/Radarr-style UI.
//
// This entry point is intentionally tiny. All wiring lives in
// internal/bootstrap. Tests construct App directly via bootstrap.Build.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jaenster/hoardarr"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
)

func main() {
	configPath := flag.String("config", "./config.toml", "path to config.toml")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.LoadOrCreate(*configPath)
	if err != nil {
		logger.Error("load config", "path", *configPath, "err", err)
		os.Exit(1)
	}
	logger.Info("config loaded", "path", *configPath)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := bootstrap.Build(ctx, cfg, hoardarr.FrontendFS, logger)
	if err != nil {
		logger.Error("bootstrap", "err", err)
		os.Exit(1)
	}

	if err := app.Run(ctx); err != nil {
		logger.Error("run", "err", err)
		os.Exit(1)
	}
}
