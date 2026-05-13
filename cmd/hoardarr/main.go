// hoardarr — Go reimplementation of SABnzbd with a Sonarr/Radarr-style UI.
//
// Subcommand dispatch:
//
//	hoardarr [serve]                      run the HTTP server (default)
//	hoardarr server add --name X ...      add a Usenet provider
//	hoardarr server list
//	hoardarr server rm <id>
//	hoardarr download <nzb-path>          fetch one NZB to completion
//
// All subcommands accept --config (default ./config.toml).
package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/jaenster/hoardarr/internal/loghub"
)

// logLevel is a process-global LevelVar so the daemon can adjust its
// effective slog level once config has loaded — without rebuilding
// the handler or any of its child loggers.
var logLevel = new(slog.LevelVar) // starts at INFO

// logHub mirrors every record into an in-memory ring buffer so the
// System page's Logs tab can show recent lines + tail live.
var logHub = loghub.New(1024)

func main() {
	base := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	logger := slog.New(loghub.NewHandler(base, logHub))
	slog.SetDefault(logger)

	args := os.Args[1:]
	cmd := ""
	if len(args) > 0 {
		cmd = args[0]
	}

	var err error
	switch cmd {
	case "", "serve":
		// Strip "serve" if present so cmdServe sees only flags.
		rest := args
		if cmd == "serve" {
			rest = args[1:]
		}
		err = cmdServe(rest, logger)
	case "server":
		err = cmdServer(args[1:], logger)
	case "download":
		err = cmdDownload(args[1:], logger)
	case "version", "--version", "-v":
		err = cmdVersion(args[1:])
	case "healthcheck":
		err = cmdHealthcheck(args[1:])
	case "-h", "--help", "help":
		printUsage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", cmd)
		printUsage()
		os.Exit(2)
	}

	if err != nil {
		logger.Error("hoardarr", "err", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `hoardarr — usenet downloader

Usage:
  hoardarr [serve] [--config ./config.toml]
  hoardarr server add --name X --host Y --port Z [--user U --pass P --conns N --priority N --no-tls]
  hoardarr server list
  hoardarr server rm <id>
  hoardarr download <nzb-path>
  hoardarr version

Run 'hoardarr <subcommand> --help' for subcommand-specific flags.`)
}
