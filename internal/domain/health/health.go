// Package health is the bounded context that owns operator-facing
// health checks — Sonarr-style banners that surface "your download
// client is unreachable", "no usenet servers configured", "disk space
// low", etc., before they manifest as silent stalled jobs.
//
// A check is a pure function that takes a snapshot of state and
// returns zero or more Issues. The app/health service composes a
// registry of checks, runs them on a tick, and exposes the current
// set so REST + SSE can render banners.
package health

import "context"

// Severity discriminates a soft warning (operator should be aware)
// from a hard error (something is actively broken). UI treats them
// differently — errors get the alarm-red banner, warnings amber.
type Severity string

const (
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"
)

// Issue is one finding from a check function.
type Issue struct {
	// Source identifies the check that produced this issue. Stable
	// across runs so the UI can dedupe / animate transitions cleanly.
	Source string `json:"source"`

	Severity Severity `json:"severity"`

	// Message is the short operator-facing description. Renders in
	// the banner verbatim.
	Message string `json:"message"`

	// DocsURL is an optional link to a wiki / docs page explaining
	// the issue and the recommended fix. Empty when there's nothing
	// useful to link to.
	DocsURL string `json:"docs_url,omitempty"`
}

// CheckFunc is the unit of work the health service composes. Each
// check is responsible for its own data fetches; it must not panic.
// A nil/empty return means "all good, nothing to surface".
type CheckFunc func(ctx context.Context) []Issue
