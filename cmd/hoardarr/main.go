package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jaenster/hoardarr"
	"github.com/jaenster/hoardarr/internal/config"
	"github.com/jaenster/hoardarr/internal/server"
)

func main() {
	cfg := config.Default()
	flag.StringVar(&cfg.Listen, "listen", cfg.Listen, "address to listen on")
	flag.StringVar(&cfg.DataDir, "data", cfg.DataDir, "data directory")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		logger.Error("create data dir", "err", err)
		os.Exit(1)
	}

	srv := server.New(cfg, logger, hoardarr.FrontendFS)
	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("hoardarr listening", "addr", cfg.Listen)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listen", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown", "err", err)
		os.Exit(1)
	}
}
