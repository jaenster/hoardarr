// Package router dispatches each delivery to the right adapter based
// on the Subscription's Kind. notify.Service treats this as a single
// Sender and remains adapter-agnostic.
package router

import (
	"context"
	"fmt"

	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/notify"
)

// Router implements notify.Sender by delegating to a per-Kind sender.
// Unknown Kinds return an error (the domain rejects them at creation
// time, so this is purely a safety net).
type Router struct {
	byKind map[notify.Kind]notify.Sender
}

var _ notify.Sender = (*Router)(nil)

// New constructs a Router from a kind→sender map. The caller owns
// the inputs; Router keeps references.
func New(byKind map[notify.Kind]notify.Sender) *Router {
	return &Router{byKind: byKind}
}

// Send delegates to the adapter for sub.Kind().
func (r *Router) Send(ctx context.Context, sub *notify.Subscription, env event.Envelope) error {
	s, ok := r.byKind[sub.Kind()]
	if !ok {
		return fmt.Errorf("notify router: no sender for kind %q", sub.Kind())
	}
	return s.Send(ctx, sub, env)
}
