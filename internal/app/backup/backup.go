// Package backup creates clean copies of hoardarr's SQLite database
// under <data_dir>/backups/. Triggered by a recurring scheduled task
// (weekly by default) or on-demand via a Command, so the operator
// has rollback-able snapshots if a migration goes sideways or
// settings get corrupted.
//
// SQLite `VACUUM INTO` is the right primitive: produces a defrag'd
// copy with no torn pages and no need to coordinate with writers
// (it locks briefly, but a per-week tick during idle time is the
// expected pattern).
//
// Retention is bounded by count, not age — keeps the operator's
// last N regardless of how often Backup() ran.
package backup

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// DBExecutor is the slice of *sqlite.DB the backup needs. Defined
// here so the package doesn't depend on the adapter type directly.
type DBExecutor interface {
	ExecCtx(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Service holds the snapshot directory + retention policy. Run()
// is the unit of work; the schedule handler is built off it.
type Service struct {
	db      DBExecutor
	dir     string
	retain  int
}

// New constructs the service. Caller must MkdirAll dir before first
// use; the service does it lazily on each Run as a belt-and-braces.
func New(db DBExecutor, dir string, retain int) *Service {
	if retain <= 0 {
		retain = 14
	}
	return &Service{db: db, dir: dir, retain: retain}
}

// Run produces a fresh backup file and prunes old ones beyond the
// retention bound. Idempotent — calling twice produces two files.
// The filename embeds the UTC timestamp so the lexical sort matches
// chronological order.
func (s *Service) Run(ctx context.Context) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("backup: mkdir %q: %w", s.dir, err)
	}
	stamp := time.Now().UTC().Format("2006-01-02T15-04-05Z")
	name := fmt.Sprintf("hoardarr-%s.db", stamp)
	path := filepath.Join(s.dir, name)
	// VACUUM INTO produces a clean compact copy. Quoted single-quotes
	// is the only path-passing form SQLite accepts; we don't bother
	// param-binding because path is operator-controlled, not user
	// input. (Migration-time check at the entry point if that ever
	// changes.)
	q := fmt.Sprintf("VACUUM INTO '%s'", strings.ReplaceAll(path, "'", "''"))
	if _, err := s.db.ExecCtx(ctx, q); err != nil {
		return fmt.Errorf("backup: VACUUM INTO: %w", err)
	}
	s.prune()
	return nil
}

// prune deletes rotated files past the retention bound, oldest-first.
func (s *Service) prune() {
	files := s.list()
	if len(files) <= s.retain {
		return
	}
	for _, f := range files[s.retain:] {
		_ = os.Remove(filepath.Join(s.dir, f.Name))
	}
}

// FileInfo is one row of the list response.
type FileInfo struct {
	Name      string    `json:"name"`
	Size      int64     `json:"size_bytes"`
	CreatedAt time.Time `json:"created_at"`
}

// List returns every backup file in dir, newest-first.
func (s *Service) List() []FileInfo {
	return s.list()
}

func (s *Service) list() []FileInfo {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	out := make([]FileInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if !strings.HasPrefix(n, "hoardarr-") || !strings.HasSuffix(n, ".db") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, FileInfo{
			Name:      n,
			Size:      info.Size(),
			CreatedAt: info.ModTime().UTC(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out
}

// SafePath validates a filename and returns its absolute path inside
// dir. Same shape as logfile.SafePath — reject anything that isn't
// a hoardarr-prefixed .db and reject traversal characters.
func (s *Service) SafePath(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("backup: name required")
	}
	if !strings.HasPrefix(name, "hoardarr-") || !strings.HasSuffix(name, ".db") {
		return "", fmt.Errorf("backup: %q is not a hoardarr backup", name)
	}
	clean := filepath.Clean(name)
	if clean != name || strings.ContainsAny(clean, `/\`) {
		return "", fmt.Errorf("backup: traversal in %q", name)
	}
	return filepath.Join(s.dir, clean), nil
}

// Dir returns the backup directory; useful for tests.
func (s *Service) Dir() string { return s.dir }
