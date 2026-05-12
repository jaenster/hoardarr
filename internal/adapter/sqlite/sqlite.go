// Package sqlite provides the SQLite implementation of hoardarr's
// persistence ports.
//
// SQLite is one of potentially several backing stores. The domain layer
// only sees repository ports defined in internal/domain/<context>/ports.go;
// nothing in the domain references sqlite. A future Postgres adapter
// would live in internal/adapter/postgres/ and implement the same ports
// — bootstrap selects which to wire based on config.
//
// Driver: modernc.org/sqlite (pure Go, no cgo). Registered as "sqlite".
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver, registers itself as "sqlite"
)

// DB wraps *sql.DB with hoardarr-specific helpers (transaction-aware
// Exec/Query). Use Open to construct.
type DB struct {
	*sql.DB
	path string
}

// Path returns the on-disk DB file path the connection was opened against.
// Useful for diagnostics and tests.
func (db *DB) Path() string { return db.path }

// Options tune Open's behaviour. Zero values are sensible defaults.
type Options struct {
	// MaxOpenConns caps the connection pool. SQLite serializes writes,
	// so a high number of writer connections is not useful — but readers
	// can fan out. Default: 16.
	MaxOpenConns int

	// MaxIdleConns caps idle conns. Default: same as MaxOpenConns.
	MaxIdleConns int

	// ConnMaxLifetime caps per-conn lifetime. Zero means no limit.
	ConnMaxLifetime time.Duration

	// BusyTimeout is the SQLite busy-timeout (in ms) applied via PRAGMA.
	// Default: 5000 (5 seconds).
	BusyTimeout int

	// CacheSizeKB sets PRAGMA cache_size. Negative kibibytes per SQLite
	// convention. Default: 64 MiB (-65536).
	CacheSizeKB int
}

func (o Options) withDefaults() Options {
	if o.MaxOpenConns == 0 {
		// SQLite + WAL allows many concurrent readers and exactly one
		// writer. The original design pinned the pool to 1 conn to
		// dodge SQLITE_BUSY_SNAPSHOT (517) on the SELECT-then-UPDATE
		// pattern inside a transaction, but the cost is severe:
		// under live load every HTTP handler queues behind whatever
		// the orchestrator's segment drainer or outbox dispatcher
		// happens to be doing, so /api/v1/queue and /system/status
		// can take seconds even when their queries cost microseconds.
		// pprof on the live container showed 70+ goroutines blocked
		// in database/sql.(*DB).conn — the single conn was the
		// bottleneck, not query cost.
		//
		// With WAL + busy_timeout the only path to a real
		// SQLITE_BUSY_SNAPSHOT failure is a single tx that opens a
		// read snapshot and then upgrades to a write *after* another
		// tx has committed. We catch that case in tx_manager.go and
		// retry the whole InTx closure. Reads stay autocommit and
		// scale freely.
		o.MaxOpenConns = 8
	}
	if o.MaxIdleConns == 0 {
		o.MaxIdleConns = o.MaxOpenConns
	}
	if o.BusyTimeout == 0 {
		o.BusyTimeout = 5000
	}
	if o.CacheSizeKB == 0 {
		o.CacheSizeKB = -65536
	}
	return o
}

// Open opens (or creates) the SQLite DB at path and applies hoardarr's
// standard pragmas (WAL, synchronous=NORMAL, foreign_keys=ON, busy_timeout,
// cache_size).
//
// path may be ":memory:" for in-memory tests.
//
// On any pragma failure, the DB is closed before returning.
func Open(ctx context.Context, path string, opts Options) (*DB, error) {
	if path == "" {
		return nil, errors.New("sqlite.Open: path is empty")
	}
	opts = opts.withDefaults()

	if path != ":memory:" {
		if err := ensureParentDir(path); err != nil {
			return nil, fmt.Errorf("ensure parent dir: %w", err)
		}
	}

	// Build a DSN that embeds per-connection pragmas so every fresh
	// connection from the sql.DB pool starts with the right settings.
	// Without this, sql.DB hands out a new connection that hasn't seen
	// our applyPragmas() and ignores busy_timeout, leading to spurious
	// SQLITE_BUSY errors under contention.
	dsn := buildDSN(path, opts)

	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	conn.SetMaxOpenConns(opts.MaxOpenConns)
	conn.SetMaxIdleConns(opts.MaxIdleConns)
	conn.SetConnMaxLifetime(opts.ConnMaxLifetime)

	db := &DB{DB: conn, path: path}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	// applyPragmas runs additional one-shot setup that the DSN doesn't
	// cover (e.g. for in-memory DBs which skip WAL).
	if err := db.applyPragmas(ctx, opts); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply pragmas: %w", err)
	}

	return db, nil
}

// buildDSN composes a modernc/sqlite DSN with per-connection pragmas.
// Every new conn the pool dials picks up these settings; the matching
// applyPragmas pass is a defence-in-depth.
func buildDSN(path string, opts Options) string {
	if path == ":memory:" {
		return path
	}
	pragmas := []string{
		"_pragma=journal_mode(wal)",
		"_pragma=synchronous(normal)",
		"_pragma=foreign_keys(on)",
		fmt.Sprintf("_pragma=busy_timeout(%d)", opts.BusyTimeout),
		fmt.Sprintf("_pragma=cache_size(%d)", opts.CacheSizeKB),
	}
	return "file:" + path + "?" + joinAmpersand(pragmas)
}

func joinAmpersand(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "&"
		}
		out += p
	}
	return out
}

func (db *DB) applyPragmas(ctx context.Context, opts Options) error {
	pragmas := []string{
		// In-memory DBs don't benefit from WAL and SQLite errors with
		// "cannot change journal mode in WAL" — skip in that case.
		"PRAGMA foreign_keys = ON",
		"PRAGMA synchronous = NORMAL",
		fmt.Sprintf("PRAGMA busy_timeout = %d", opts.BusyTimeout),
		fmt.Sprintf("PRAGMA cache_size = %d", opts.CacheSizeKB),
	}
	if db.path != ":memory:" {
		// WAL only applies to file-backed DBs.
		pragmas = append([]string{"PRAGMA journal_mode = WAL"}, pragmas...)
	}
	for _, p := range pragmas {
		if _, err := db.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
	}
	return nil
}

// ensureParentDir creates the directory containing path if absent.
func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}
