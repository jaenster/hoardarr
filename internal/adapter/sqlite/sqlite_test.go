package sqlite

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

func countEmbeddedMigrations(t *testing.T) int {
	t.Helper()
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			n++
		}
	}
	return n
}

func openTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(context.Background(), filepath.Join(dir, "t.db"), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestOpen_AppliesPragmas(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	var jm string
	if err := db.QueryRowCtx(ctx, "PRAGMA journal_mode").Scan(&jm); err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if jm != "wal" {
		t.Errorf("journal_mode = %q; want wal", jm)
	}

	var fk int
	if err := db.QueryRowCtx(ctx, "PRAGMA foreign_keys").Scan(&fk); err != nil {
		t.Fatalf("query foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Errorf("foreign_keys = %d; want 1", fk)
	}

	var bt int
	if err := db.QueryRowCtx(ctx, "PRAGMA busy_timeout").Scan(&bt); err != nil {
		t.Fatalf("query busy_timeout: %v", err)
	}
	if bt != 5000 {
		t.Errorf("busy_timeout = %d; want 5000", bt)
	}
}

func TestOpen_RejectsEmptyPath(t *testing.T) {
	if _, err := Open(context.Background(), "", Options{}); err == nil {
		t.Fatal("Open(\"\") returned nil; want error")
	}
}

func TestMigrate_AppliesAndIsIdempotent(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate (first): %v", err)
	}

	// Verify outbox table exists.
	var name string
	if err := db.QueryRowCtx(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name='outbox'`,
	).Scan(&name); err != nil {
		t.Fatalf("outbox not created: %v", err)
	}

	// Re-running should be a no-op.
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate (second): %v", err)
	}

	// Re-running should be a no-op — we just check the row count is
	// stable (matches the number of embedded migration files), not a
	// hard-coded value, so this test doesn't churn every time we add
	// a migration.
	want := countEmbeddedMigrations(t)
	var n int
	if err := db.QueryRowCtx(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if n != want {
		t.Errorf("schema_migrations row count = %d; want %d", n, want)
	}
}

func TestTxManager_Commit(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	m := NewTxManager(db)

	err := m.InTx(ctx, func(ctx context.Context) error {
		_, err := db.ExecCtx(ctx,
			`INSERT INTO outbox(id, topic, aggregate_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)`,
			[]byte("0123456789ABCDEF"), "test.topic", "agg1", int64(1), []byte("{}"),
		)
		return err
	})
	if err != nil {
		t.Fatalf("InTx commit: %v", err)
	}

	var n int
	if err := db.QueryRowCtx(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("row count after commit = %d; want 1", n)
	}
}

func TestTxManager_Rollback(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	m := NewTxManager(db)
	wantErr := errors.New("user-fn error")

	err := m.InTx(ctx, func(ctx context.Context) error {
		if _, e := db.ExecCtx(ctx,
			`INSERT INTO outbox(id, topic, aggregate_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)`,
			[]byte("0123456789ABCDEF"), "test.topic", "agg1", int64(1), []byte("{}"),
		); e != nil {
			t.Fatalf("insert: %v", e)
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("InTx returned %v; want wraps wantErr", err)
	}

	var n int
	if err := db.QueryRowCtx(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("row count after rollback = %d; want 0", n)
	}
}

func TestTxManager_Nested(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	m := NewTxManager(db)

	err := m.InTx(ctx, func(ctx context.Context) error {
		// Inner InTx should join the outer; outer commit applies inner write.
		return m.InTx(ctx, func(ctx context.Context) error {
			_, err := db.ExecCtx(ctx,
				`INSERT INTO outbox(id, topic, aggregate_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)`,
				[]byte("0123456789ABCDEF"), "test", "x", int64(1), []byte("{}"),
			)
			return err
		})
	})
	if err != nil {
		t.Fatalf("InTx nested: %v", err)
	}

	var n int
	if err := db.QueryRowCtx(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("row count after nested commit = %d; want 1", n)
	}
}

func TestTxFromContext_NoTx(t *testing.T) {
	if got := TxFromContext(context.Background()); got != nil {
		t.Errorf("TxFromContext on plain ctx = %v; want nil", got)
	}
}

func TestParseMigrationFilename(t *testing.T) {
	cases := []struct {
		in        string
		wantV     int
		wantLabel string
		wantErr   bool
	}{
		{"001_outbox.sql", 1, "outbox", false},
		{"042_some_long_name.sql", 42, "some_long_name", false},
		{"abc_no_version.sql", 0, "", true},
		{"no_underscore_at_start.sql", 0, "underscore_at_start", true},
		{"missing-underscore.sql", 0, "", true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			v, label, err := parseMigrationFilename(c.in)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v; wantErr = %v", err, c.wantErr)
			}
			if c.wantErr {
				return
			}
			if v != c.wantV || label != c.wantLabel {
				t.Errorf("got (%d, %q); want (%d, %q)", v, label, c.wantV, c.wantLabel)
			}
		})
	}
}
