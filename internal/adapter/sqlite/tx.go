package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/tx"
)

// maxSnapshotRetries caps the number of times InTx will reopen on a
// SQLITE_BUSY_SNAPSHOT (517). With MaxOpenConns > 1 it's possible for
// a tx to open a read snapshot, then attempt a write after another
// connection has already advanced the DB. busy_timeout doesn't help
// here — the right answer is roll back, re-begin, replay the closure.
// Five retries with a tiny stagger is plenty in practice.
const maxSnapshotRetries = 5

// txKey is the unexported context-key type used to attach an active
// *sql.Tx to a context. Repos and the outbox bus extract via TxFromContext
// to participate in the ambient transaction.
type ctxKey struct{}

var txKey ctxKey

// TxManager is the SQLite implementation of tx.TransactionManager.
//
// It begins a transaction on InTx entry, attaches the *sql.Tx to the
// context, and commits or rolls back based on fn's return.
//
// Nested InTx calls within the same context join the existing transaction
// (savepoints not used in v0.1).
type TxManager struct {
	db *DB
}

// Compile-time check that TxManager satisfies the domain port.
var _ tx.TransactionManager = (*TxManager)(nil)

// NewTxManager returns a TransactionManager backed by db.
func NewTxManager(db *DB) *TxManager {
	return &TxManager{db: db}
}

// InTx runs fn inside a transaction.
//
// If ctx already carries a *sql.Tx, fn is called with the same ctx and
// the existing transaction is reused (joined). Errors from joined fn
// calls propagate up; the outer InTx still controls commit/rollback.
//
// If ctx has no transaction, a fresh one is begun. fn returning nil
// commits; non-nil rollbacks. Panics in fn rollback and re-panic.
func (m *TxManager) InTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if existing := TxFromContext(ctx); existing != nil {
		return fn(ctx)
	}
	var lastErr error
	for attempt := 0; attempt < maxSnapshotRetries; attempt++ {
		err := m.runTx(ctx, fn)
		if err == nil {
			return nil
		}
		if !isSnapshotConflict(err) {
			return err
		}
		lastErr = err
		// Tiny backoff so we don't spin if another writer is bursty.
		select {
		case <-time.After(time.Duration(attempt+1) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("intx: snapshot conflict after %d retries: %w", maxSnapshotRetries, lastErr)
}

// runTx is one transaction attempt. Separated so InTx can replay on
// SQLITE_BUSY_SNAPSHOT.
func (m *TxManager) runTx(ctx context.Context, fn func(ctx context.Context) error) error {
	sqlTx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = sqlTx.Rollback()
			panic(p)
		}
	}()
	if err := fn(context.WithValue(ctx, txKey, sqlTx)); err != nil {
		if rbErr := sqlTx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return fmt.Errorf("rollback after %v: %w", err, rbErr)
		}
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// isSnapshotConflict matches SQLITE_BUSY_SNAPSHOT (extended code 517).
// modernc.org/sqlite surfaces the error code in the error string.
func isSnapshotConflict(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "SQLITE_BUSY_SNAPSHOT") ||
		strings.Contains(s, "database is locked (517)") ||
		strings.Contains(s, "(517)")
}

// TxFromContext returns the *sql.Tx attached to ctx, or nil if none.
//
// Used by repository implementations and the outbox bus to bind their
// queries to the ambient transaction. If nil, the caller should fall
// back to db-level Exec/Query (single-statement, auto-committed).
func TxFromContext(ctx context.Context) *sql.Tx {
	v, _ := ctx.Value(txKey).(*sql.Tx)
	return v
}

// ExecContext runs the query inside the ambient transaction if one is
// in ctx, otherwise directly on the DB. Repositories should call this
// instead of *sql.DB.ExecContext to be tx-aware.
func (db *DB) ExecCtx(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if t := TxFromContext(ctx); t != nil {
		return t.ExecContext(ctx, query, args...)
	}
	return db.ExecContext(ctx, query, args...)
}

// QueryCtx runs the query inside the ambient tx if present.
func (db *DB) QueryCtx(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if t := TxFromContext(ctx); t != nil {
		return t.QueryContext(ctx, query, args...)
	}
	return db.QueryContext(ctx, query, args...)
}

// QueryRowCtx runs the single-row query inside the ambient tx if present.
func (db *DB) QueryRowCtx(ctx context.Context, query string, args ...any) *sql.Row {
	if t := TxFromContext(ctx); t != nil {
		return t.QueryRowContext(ctx, query, args...)
	}
	return db.QueryRowContext(ctx, query, args...)
}
