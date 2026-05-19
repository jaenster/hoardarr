package health

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jaenster/hoardarr/internal/app/diskspace"
	domainhealth "github.com/jaenster/hoardarr/internal/domain/health"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
)

// ServerRepo is the slice of the server bounded context's repository
// that the health checks need — narrow port so the dependency stays
// shallow.
type ServerRepo interface {
	List(ctx context.Context) ([]*domainserver.UsenetServer, error)
}

// ServersConfiguredCheck surfaces:
//   - error  when no usenet servers are configured at all (downloads
//            can't start)
//   - warning when servers exist but every one is disabled (operator
//            probably forgot to flip the toggle back on)
func ServersConfiguredCheck(repo ServerRepo) domainhealth.CheckFunc {
	return func(ctx context.Context) []domainhealth.Issue {
		list, err := repo.List(ctx)
		if err != nil {
			return []domainhealth.Issue{{
				Source:   "ServersConfiguredCheck",
				Severity: domainhealth.SeverityError,
				Message:  fmt.Sprintf("Could not list usenet servers: %v", err),
			}}
		}
		if len(list) == 0 {
			return []domainhealth.Issue{{
				Source:   "ServersConfiguredCheck",
				Severity: domainhealth.SeverityError,
				Message:  "No usenet servers configured. Add one under Settings → Servers.",
			}}
		}
		enabled := 0
		for _, s := range list {
			if s.Enabled() {
				enabled++
			}
		}
		if enabled == 0 {
			return []domainhealth.Issue{{
				Source:   "ServersEnabledCheck",
				Severity: domainhealth.SeverityWarning,
				Message:  "All usenet servers are disabled. Downloads will not run.",
			}}
		}
		return nil
	}
}

// DirWritableCheck verifies that a configured directory exists and
// the running process can write to it. Catches the classic
// PUID/PGID-mismatch / volume-not-mounted footguns before they show
// up as cryptic "rename: permission denied" errors in the orchestrator.
//
// label is the human-facing name ("Incomplete directory") used in the
// issue message; path is the absolute disk path to probe.
func DirWritableCheck(source, label, path string) domainhealth.CheckFunc {
	return func(ctx context.Context) []domainhealth.Issue {
		if path == "" {
			return []domainhealth.Issue{{
				Source:   source,
				Severity: domainhealth.SeverityError,
				Message:  label + " is not configured.",
			}}
		}
		info, err := os.Stat(path)
		if err != nil {
			return []domainhealth.Issue{{
				Source:   source,
				Severity: domainhealth.SeverityError,
				Message:  fmt.Sprintf("%s (%s) is unreachable: %v", label, path, err),
			}}
		}
		if !info.IsDir() {
			return []domainhealth.Issue{{
				Source:   source,
				Severity: domainhealth.SeverityError,
				Message:  fmt.Sprintf("%s (%s) is not a directory.", label, path),
			}}
		}
		probe := filepath.Join(path, ".hoardarr-health-probe")
		f, err := os.Create(probe)
		if err != nil {
			return []domainhealth.Issue{{
				Source:   source,
				Severity: domainhealth.SeverityError,
				Message:  fmt.Sprintf("%s (%s) is not writable by hoardarr: %v", label, path, err),
			}}
		}
		_ = f.Close()
		_ = os.Remove(probe)
		return nil
	}
}

// DiskSpaceCheck returns a warning when any configured path has less
// than thresholdBytes free. Reaches into the diskspace package so the
// statfs logic stays in one place. Unreachable paths are skipped here
// because DirWritableCheck already surfaces them as errors.
func DiskSpaceCheck(sources []diskspace.Source, thresholdBytes int64) domainhealth.CheckFunc {
	return func(ctx context.Context) []domainhealth.Issue {
		var out []domainhealth.Issue
		for _, e := range diskspace.Snapshot(sources) {
			if !e.Reachable {
				continue
			}
			if e.FreeBytes < thresholdBytes {
				out = append(out, domainhealth.Issue{
					Source:   "DiskSpace_" + e.Label,
					Severity: domainhealth.SeverityWarning,
					Message: fmt.Sprintf(
						"%s (%s) has only %s free of %s — downloads may stall.",
						e.Label, e.Path,
						humanBytes(e.FreeBytes), humanBytes(e.TotalBytes),
					),
				})
			}
		}
		return out
	}
}

// humanBytes is a tiny base-1024 formatter for health messages.
// (Frontend has its own; we don't want to depend on it server-side.)
func humanBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%dB", n)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	v := float64(n) / 1024
	u := 0
	for v >= 1024 && u < len(units)-1 {
		v /= 1024
		u++
	}
	return fmt.Sprintf("%.1f%s", v, units[u])
}
