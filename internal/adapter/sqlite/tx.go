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
// retryable busy error. Two flavours surface from modernc.org/sqlite:
//
//   - SQLITE_BUSY_SNAPSHOT (517): a tx opened a read snapshot, then
//     attempted a write after another connection advanced the DB.
//     busy_timeout doesn't help here; only roll back, re-begin, replay.
//
//   - SQLITE_BUSY (5): two connections raced for the write lock and
//     the loser's busy-timeout countdown ran out. With MaxOpenConns >
//     1 and bursty writers (outbox dispatcher + orchestrator + REST
//     mutating endpoints) this still happens occasionally even with a
//     5s timeout; the retry-then-roll-back-and-replay pattern is the
//     same as for 517.
//
// 20 retries with exponential backoff (1ms * 2^attempt, capped at
// 250ms) absorbs the bursty contention pattern we see under CI's
// -race overhead, where multiple tests in the same process are
// hammering the DB concurrently and the writer set turns over fast
// enough that 5 retries can hit 5 consecutive collisions. Worst-case
// total stagger is ~2.5s, well below the 10m go-test timeout and
// the 30s HTTP-client timeout that's the actual outer bound.
const maxSnapshotRetries = 20

// txKey is the unexported context-key type used to attach an active
// *sql.Tx to a context. Repos and the outbox bus extract via TxFromContext
// to participate in the ambient transaction.
type ctxKey struct{}

var txKey ctxKey

// txState carries the *sql.Tx plus a list of post-commit hooks. Hooks
// fire AFTER the outer TX commits successfully — used to defer
// observable side-effects (e.g. waking outbox dispatchers) so they
// only happen when other readers can see the inserted rows. A
// rolled-back TX runs no hooks.
type txState struct {
	tx          *sql.Tx
	commitHooks []func()
}

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
		if !isRetryableBusy(err) {
			return err
		}
		lastErr = err
		// Exponential backoff with a 250ms cap. Spinning every
		// attempt+1 ms (the previous linear scheme) doesn't give the
		// other writer enough room to finish on a slow runner.
		delay := time.Duration(1<<attempt) * time.Millisecond
		if delay > 250*time.Millisecond {
			delay = 250 * time.Millisecond
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("intx: busy after %d retries: %w", maxSnapshotRetries, lastErr)
}

// runTx is one transaction attempt. Separated so InTx can replay on
// SQLITE_BUSY_SNAPSHOT / SQLITE_BUSY.
func (m *TxManager) runTx(ctx context.Context, fn func(ctx context.Context) error) error {
	sqlTx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	state := &txState{tx: sqlTx}
	defer func() {
		if p := recover(); p != nil {
			_ = sqlTx.Rollback()
			panic(p)
		}
	}()
	if err := fn(context.WithValue(ctx, txKey, state)); err != nil {
		if rbErr := sqlTx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return fmt.Errorf("rollback after %v: %w", err, rbErr)
		}
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	// Fire post-commit hooks. We deliberately run them after a
	// successful commit so observers (e.g. outbox dispatchers being
	// nudged) see the just-inserted rows when they query.
	for _, hook := range state.commitHooks {
		hook()
	}
	return nil
}

// isRetryableBusy matches the two flavours of busy / lock-contention
// modernc.org/sqlite surfaces: plain SQLITE_BUSY (5) and the snapshot-
// conflict variant SQLITE_BUSY_SNAPSHOT (517). Both clear with a tx
// replay; neither benefits from blindly waiting (517 never times out,
// and a 5 that already exhausted busy_timeout won't relax on its own).
func isRetryableBusy(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	// 517 — snapshot conflict.
	if strings.Contains(s, "SQLITE_BUSY_SNAPSHOT") ||
		strings.Contains(s, "(517)") {
		return true
	}
	// 5 — plain SQLITE_BUSY (write-write race after busy_timeout).
	if strings.Contains(s, "database is locked (5)") ||
		strings.Contains(s, "SQLITE_BUSY") {
		return true
	}
	return false
}

// TxFromContext returns the *sql.Tx attached to ctx, or nil if none.
//
// Used by repository implementations and the outbox bus to bind their
// queries to the ambient transaction. If nil, the caller should fall
// back to db-level Exec/Query (single-statement, auto-committed).
func TxFromContext(ctx context.Context) *sql.Tx {
	if st, ok := ctx.Value(txKey).(*txState); ok && st != nil {
		return st.tx
	}
	return nil
}

// OnTxCommit registers fn to run after the ambient transaction
// commits successfully. If ctx carries no transaction, fn runs
// immediately. fn does not run if the transaction is rolled back.
//
// Used for side-effects that observers (other goroutines) must not
// see before the TX is durable — most notably the outbox bus waking
// up dispatcher goroutines, which would otherwise SELECT and find
// nothing while the INSERT is still pending in an uncommitted TX.
func OnTxCommit(ctx context.Context, fn func()) {
	if st, ok := ctx.Value(txKey).(*txState); ok && st != nil {
		st.commitHooks = append(st.commitHooks, fn)
		return
	}
	fn()
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
