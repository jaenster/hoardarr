package main

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Build-time variables injected via -ldflags by the Makefile / Dockerfile
// / CI. Defaults are placeholders for `go run` / unflagged `go build`.
//
//	-ldflags "-X main.version=v0.1.0 -X main.commit=abc123 -X main.buildDate=2026-05-13T20:00:00Z"
//
// We deliberately don't fall back to runtime/debug.ReadBuildInfo for the
// commit because that pulls in module-graph noise on dev builds; the
// ldflags inject is the single source of truth for the binary's
// self-identification.
var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

// versionString returns the human-readable identification line we print
// for `hoardarr version` and serve as the User-Agent / Server header
// (when we add one).
func versionString() string {
	c := commit
	if len(c) > 12 && c != "unknown" {
		c = c[:12]
	}
	return fmt.Sprintf("hoardarr %s (commit %s, built %s, %s/%s, %s)",
		version, c, buildDate, runtime.GOOS, runtime.GOARCH, runtime.Version())
}

// buildInfo exposes the version metadata to other parts of the
// binary (the system status REST endpoint, the eventual /metrics
// build_info gauge, the SAB version mode if we ever decide to stop
// lying about being SABnzbd 3.7.2).
func buildInfo() (vers, com, date string) {
	v := version
	if v == "dev" {
		if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "(devel)" && bi.Main.Version != "" {
			v = bi.Main.Version
		}
	}
	return v, commit, buildDate
}

func cmdVersion(_ []string) error {
	fmt.Println(versionString())
	return nil
}
