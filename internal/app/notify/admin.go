package notify

// Admin: CRUD use-cases for managing Subscription rows. Separate from
// Service (which handles delivery) so the dispatch path and the
// management path don't share a struct full of unrelated fields.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/notify"
	"github.com/jaenster/hoardarr/internal/domain/tx"
)

// ErrNameTaken is returned by Admin.Add when a subscription with the
// same display name already exists.
var ErrNameTaken = errors.New("notify: name already taken")

// Admin is the management surface — what the REST handlers call.
type Admin struct {
	repo notify.Repository
	bus  event.Bus
	txm  tx.TransactionManager
	now  func() time.Time
}

// NewAdmin wires Admin.
func NewAdmin(repo notify.Repository, bus event.Bus, txm tx.TransactionManager, now func() time.Time) *Admin {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Admin{repo: repo, bus: bus, txm: txm, now: now}
}

// AddCmd is the input to Add.
type AddCmd struct {
	Name   string
	Kind   notify.Kind
	URL    string
	Topics []string
	Secret string
}

// Add persists a new subscription and emits SubscriptionAdded.
func (a *Admin) Add(ctx context.Context, cmd AddCmd) (notify.SubscriptionID, error) {
	var id notify.SubscriptionID
	err := a.txm.InTx(ctx, func(ctx context.Context) error {
		// Pre-check for duplicate name (the UNIQUE constraint also
		// catches it, but a domain-level check gives a clean error).
		list, err := a.repo.List(ctx)
		if err != nil {
			return err
		}
		for _, s := range list {
			if s.Name() == cmd.Name {
				return ErrNameTaken
			}
		}
		sub, err := notify.New(notify.NewParams{
			Name:   cmd.Name,
			Kind:   cmd.Kind,
			URL:    cmd.URL,
			Topics: cmd.Topics,
			Secret: cmd.Secret,
		}, a.now())
		if err != nil {
			return err
		}
		if err := a.repo.Save(ctx, sub); err != nil {
			return fmt.Errorf("save: %w", err)
		}
		id = sub.ID()
		return a.bus.Publish(ctx, sub.PullEvents()...)
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// Remove deletes the subscription and emits SubscriptionRemoved.
func (a *Admin) Remove(ctx context.Context, id notify.SubscriptionID) error {
	return a.txm.InTx(ctx, func(ctx context.Context) error {
		sub, err := a.repo.ByID(ctx, id)
		if err != nil {
			return err
		}
		sub.MarkRemoved(a.now())
		if err := a.repo.Delete(ctx, id); err != nil {
			return err
		}
		return a.bus.Publish(ctx, sub.PullEvents()...)
	})
}

// SetEnabled toggles the enabled flag.
func (a *Admin) SetEnabled(ctx context.Context, id notify.SubscriptionID, enabled bool) error {
	return a.txm.InTx(ctx, func(ctx context.Context) error {
		sub, err := a.repo.ByID(ctx, id)
		if err != nil {
			return err
		}
		sub.SetEnabled(enabled, a.now())
		evts := sub.PullEvents()
		if len(evts) == 0 {
			return nil
		}
		if err := a.repo.Save(ctx, sub); err != nil {
			return err
		}
		return a.bus.Publish(ctx, evts...)
	})
}

// List returns all subscriptions, including disabled ones.
func (a *Admin) List(ctx context.Context) ([]*notify.Subscription, error) {
	return a.repo.List(ctx)
}
