package sqlite

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migration is one forward-only schema change.
type migration struct {
	Version int    // monotonic, parsed from filename prefix NNN
	Name    string // human-readable name (filename without prefix and .sql)
	SQL     string // file contents
}

// Migrate applies all pending migrations to db.
//
// Migrations are SQL files embedded at internal/adapter/sqlite/migrations/.
// File naming: NNN_description.sql (NNN is the zero-padded integer version).
// Files are applied in version order. Each file runs inside its own
// transaction; partial failure leaves the DB at the previous version.
//
// A bookkeeping table schema_migrations records applied versions.
//
// This is forward-only by design. To revert a schema change, write a new
// migration that undoes it.
func (db *DB) Migrate(ctx context.Context) error {
	if err := db.ensureMigrationsTable(ctx); err != nil {
		return err
	}

	applied, err := db.appliedVersions(ctx)
	if err != nil {
		return fmt.Errorf("read applied versions: %w", err)
	}

	all, err := loadMigrations()
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}

	for _, m := range all {
		if applied[m.Version] {
			continue
		}
		if err := db.applyMigration(ctx, m); err != nil {
			return fmt.Errorf("apply migration %03d_%s: %w", m.Version, m.Name, err)
		}
	}
	return nil
}

func (db *DB) ensureMigrationsTable(ctx context.Context) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INTEGER PRIMARY KEY,
			name       TEXT NOT NULL,
			applied_at INTEGER NOT NULL
		)
	`)
	return err
}

func (db *DB) appliedVersions(ctx context.Context) (map[int]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

func (db *DB) applyMigration(ctx context.Context, m migration) error {
	t, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := t.ExecContext(ctx, m.SQL); err != nil {
		_ = t.Rollback()
		return err
	}
	if _, err := t.ExecContext(ctx,
		`INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, unixepoch('now') * 1000)`,
		m.Version, m.Name,
	); err != nil {
		_ = t.Rollback()
		return err
	}
	return t.Commit()
}

// loadMigrations reads and parses every embedded migration file, sorted
// ascending by version.
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, err
	}
	var out []migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		v, label, err := parseMigrationFilename(name)
		if err != nil {
			return nil, fmt.Errorf("filename %q: %w", name, err)
		}
		body, err := fs.ReadFile(migrationFS, "migrations/"+name)
		if err != nil {
			return nil, err
		}
		out = append(out, migration{Version: v, Name: label, SQL: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })

	// Detect duplicate versions early — programmer error, not user error.
	for i := 1; i < len(out); i++ {
		if out[i].Version == out[i-1].Version {
			return nil, fmt.Errorf("duplicate migration version %d (%s, %s)",
				out[i].Version, out[i-1].Name, out[i].Name)
		}
	}
	return out, nil
}

func parseMigrationFilename(name string) (int, string, error) {
	// Expected: NNN_description.sql
	base := strings.TrimSuffix(name, ".sql")
	idx := strings.Index(base, "_")
	if idx < 0 {
		return 0, "", fmt.Errorf("missing underscore separator")
	}
	v, err := strconv.Atoi(base[:idx])
	if err != nil {
		return 0, "", fmt.Errorf("parse version: %w", err)
	}
	return v, base[idx+1:], nil
}
