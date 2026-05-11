package notify

import (
	"context"
	"errors"

	"github.com/jaenster/hoardarr/internal/domain/event"
)

// Repository persists Subscription aggregates.
type Repository interface {
	Save(ctx context.Context, s *Subscription) error
	ByID(ctx context.Context, id SubscriptionID) (*Subscription, error)
	List(ctx context.Context) ([]*Subscription, error)
	Delete(ctx context.Context, id SubscriptionID) error
}

// ErrNotFound — no row matches the lookup.
var ErrNotFound = errors.New("notify: not found")

// Sender is the adapter port for delivering one event to one
// subscriber. Implementations: adapter/notify/webhook (HTTP POST),
// adapter/notify/discord (Discord-specific shape), etc.
//
// Send is synchronous from the caller's point of view: it returns
// after the consumer accepted (2xx) or all retries failed. The notify
// service spawns one goroutine per (sub, event) pair so a slow target
// doesn't block sibling subs.
type Sender interface {
	Send(ctx context.Context, sub *Subscription, env event.Envelope) error
}
