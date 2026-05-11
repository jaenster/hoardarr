// Package server provides application services for the server bounded
// context: Add, Update, Enable/Disable, Remove use cases.
//
// Each use case wraps repository calls and event publication in a
// transaction. Domain events written here flow through the outbox to
// any registered subscribers.
package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
	domain "github.com/jaenster/hoardarr/internal/domain/server"
	"github.com/jaenster/hoardarr/internal/domain/tx"
)

// Service composes the dependencies for server use cases.
type Service struct {
	repo domain.Repository
	bus  event.Bus
	tx   tx.TransactionManager
	now  func() time.Time
}

// New constructs a Service. now defaults to time.Now if nil.
func New(repo domain.Repository, bus event.Bus, txm tx.TransactionManager, now func() time.Time) *Service {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{repo: repo, bus: bus, tx: txm, now: now}
}

// AddCmd is the command shape for adding a server. Mirrors
// domain.NewParams but suitable for serialisation from the CLI / HTTP
// layer.
type AddCmd struct {
	Name     string
	Host     string
	Port     int
	TLS      *bool
	Username string
	Password string
	MaxConns int
	Priority int

	// Optional multi-server fields. Backup defaults false. BillingMode
	// "" means "flat" (the common case). QuotaBytes 0 means "unknown
	// or unlimited" (no auto-disable).
	Backup               bool
	BillingMode          domain.BillingMode
	QuotaBytes           int64
	BandwidthBytesPerSec int64
}

// Add creates a new server and persists it. Returns the assigned ID.
//
// Returns ErrNameTaken if a server with the given name already exists.
func (s *Service) Add(ctx context.Context, cmd AddCmd) (domain.ServerID, error) {
	var id domain.ServerID
	err := s.tx.InTx(ctx, func(ctx context.Context) error {
		// Pre-check for duplicate name to give a stable error type.
		if _, err := s.repo.ByName(ctx, cmd.Name); err == nil {
			return ErrNameTaken
		} else if !errors.Is(err, domain.ErrNotFound) {
			return err
		}

		agg, err := domain.New(domain.NewParams{
			Name:                 cmd.Name,
			Host:                 cmd.Host,
			Port:                 cmd.Port,
			TLS:                  cmd.TLS,
			Username:             cmd.Username,
			Password:             cmd.Password,
			MaxConns:             cmd.MaxConns,
			Priority:             cmd.Priority,
			Backup:               cmd.Backup,
			BillingMode:          cmd.BillingMode,
			QuotaBytes:           cmd.QuotaBytes,
			BandwidthBytesPerSec: cmd.BandwidthBytesPerSec,
		}, s.now())
		if err != nil {
			return err
		}
		if err := s.repo.Save(ctx, agg); err != nil {
			return fmt.Errorf("save: %w", err)
		}
		id = agg.ID()
		return s.bus.Publish(ctx, agg.PullEvents()...)
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// UpdateCmd specifies which fields to mutate. Only non-nil fields are
// applied. ID is required.
type UpdateCmd struct {
	ID                   domain.ServerID
	Host                 *string
	Port                 *int
	TLS                  *bool
	Username             *string
	Password             *string
	MaxConns             *int
	Priority             *int
	Backup               *bool
	BillingMode          *domain.BillingMode
	QuotaBytes           *int64
	BandwidthBytesPerSec *int64
}

// Update applies the given mutations to the server identified by ID.
func (s *Service) Update(ctx context.Context, cmd UpdateCmd) error {
	return s.tx.InTx(ctx, func(ctx context.Context) error {
		agg, err := s.repo.ByID(ctx, cmd.ID)
		if err != nil {
			return err
		}
		if err := agg.Update(domain.UpdateParams{
			Host:                 cmd.Host,
			Port:                 cmd.Port,
			TLS:                  cmd.TLS,
			Username:             cmd.Username,
			Password:             cmd.Password,
			MaxConns:             cmd.MaxConns,
			Priority:             cmd.Priority,
			Backup:               cmd.Backup,
			BillingMode:          cmd.BillingMode,
			QuotaBytes:           cmd.QuotaBytes,
			BandwidthBytesPerSec: cmd.BandwidthBytesPerSec,
		}, s.now()); err != nil {
			return err
		}
		if err := s.repo.Save(ctx, agg); err != nil {
			return fmt.Errorf("save: %w", err)
		}
		return s.bus.Publish(ctx, agg.PullEvents()...)
	})
}

// SetEnabled toggles the soft-enable flag.
func (s *Service) SetEnabled(ctx context.Context, id domain.ServerID, enabled bool) error {
	return s.tx.InTx(ctx, func(ctx context.Context) error {
		agg, err := s.repo.ByID(ctx, id)
		if err != nil {
			return err
		}
		agg.SetEnabled(enabled, s.now())
		if err := s.repo.Save(ctx, agg); err != nil {
			return fmt.Errorf("save: %w", err)
		}
		return s.bus.Publish(ctx, agg.PullEvents()...)
	})
}

// Remove deletes the server identified by id. Emits ServerRemoved.
func (s *Service) Remove(ctx context.Context, id domain.ServerID) error {
	return s.tx.InTx(ctx, func(ctx context.Context) error {
		// Confirm existence so the event carries a real id and we
		// surface a stable ErrNotFound rather than racy "rows = 0".
		if _, err := s.repo.ByID(ctx, id); err != nil {
			return err
		}
		if err := s.repo.Delete(ctx, id); err != nil {
			return err
		}
		return s.bus.Publish(ctx, domain.ServerRemoved{ID: id, At: s.now()})
	})
}

// Get returns the server by id.
func (s *Service) Get(ctx context.Context, id domain.ServerID) (*domain.UsenetServer, error) {
	return s.repo.ByID(ctx, id)
}

// List returns all servers.
func (s *Service) List(ctx context.Context) ([]*domain.UsenetServer, error) {
	return s.repo.List(ctx)
}

// ListEnabled returns enabled servers ordered by priority ascending.
func (s *Service) ListEnabled(ctx context.Context) ([]*domain.UsenetServer, error) {
	return s.repo.ListEnabled(ctx)
}

// ErrNameTaken is returned by Add when the name conflicts with an
// existing server.
var ErrNameTaken = errors.New("server: name already taken")
