package server

import (
	"context"
	"errors"
)

// Repository persists UsenetServer aggregates. Implementations live
// in adapter packages (e.g. internal/adapter/sqlite/repo_server.go).
//
// Save assigns an ID via UsenetServer.SetID when the aggregate is
// being inserted for the first time (id == 0). Subsequent saves
// match by ID.
//
// All operations participate in the ambient transaction (see
// internal/domain/tx) when one is in scope.
type Repository interface {
	Save(ctx context.Context, s *UsenetServer) error
	ByID(ctx context.Context, id ServerID) (*UsenetServer, error)
	ByName(ctx context.Context, name string) (*UsenetServer, error)
	List(ctx context.Context) ([]*UsenetServer, error)

	// ListEnabled returns enabled servers ordered by priority ascending
	// (lower priority number = higher priority). The download
	// orchestrator calls this when assembling its dispatch order.
	ListEnabled(ctx context.Context) ([]*UsenetServer, error)

	// Delete removes the row identified by id. Returns ErrNotFound if
	// no such row exists; idempotency is the caller's responsibility.
	Delete(ctx context.Context, id ServerID) error
}

// ErrNotFound is returned by Repository methods when the requested
// aggregate doesn't exist.
var ErrNotFound = errors.New("server: not found")
