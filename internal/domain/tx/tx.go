// Package tx defines the unit-of-work abstraction used to coordinate
// repository writes and event publication into a single atomic operation.
//
// Application services that mutate state should always run inside a
// TransactionManager.InTx block so that:
//
//   - Multiple repository writes commit atomically.
//   - Domain events written to the outbox commit in the same transaction
//     as the state change that produced them. This is the load-bearing
//     guarantee of the transactional-outbox pattern.
//
// This package is pure domain — no SQL, no specific persistence concerns.
// Adapters (e.g. internal/adapter/sqlite) provide concrete implementations
// that propagate their transaction handle through the context.
package tx

import "context"

// TransactionManager runs fn inside a transaction.
//
// On entry, it begins a transaction and attaches a backend-specific handle
// to the returned context. Repositories and the event bus extract that
// handle from the context to participate in the same transaction.
//
// If fn returns nil, the transaction commits and any nested operations
// (including outbox writes) become visible. If fn returns a non-nil error
// or panics, the transaction is rolled back and no partial state is
// observable.
//
// Nested InTx calls within the same context join the existing transaction
// (savepoints are not used in v0.1; nested calls are flat).
//
// Implementations:
//
//   - internal/adapter/sqlite: backed by *sql.Tx, propagated via context key.
//   - internal/adapter/eventbus/memory provides a no-op TransactionManager
//     for use in tests that don't need real persistence.
type TransactionManager interface {
	InTx(ctx context.Context, fn func(ctx context.Context) error) error
}
