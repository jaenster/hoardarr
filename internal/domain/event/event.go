// Package event defines the domain-level event abstractions used by every
// bounded context in hoardarr.
//
// All inter-context communication flows through the Bus. A bounded context
// publishes domain events when its aggregates change state, and other
// contexts subscribe to topics they care about. There are no direct calls
// between contexts.
//
// This package is pure domain — no framework, persistence, or adapter
// dependencies beyond the standard library and uuid generation.
package event

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event is the contract every domain event satisfies.
//
// Concrete event types live in the bounded context that produces them
// (e.g. download.JobCreated, verify.RepairNeeded). They are immutable
// value types — once created, they are never mutated. The Bus serializes
// them via JSON for delivery to subscribers and for outbox persistence.
type Event interface {
	// Topic uniquely identifies the event kind. Convention:
	//   "<context>.<aggregate>.<verb>"
	// e.g. "download.job.created", "verify.repair.needed".
	Topic() string

	// AggregateID is the identifier of the aggregate the event is about,
	// rendered as a stable string. Used for replay-by-aggregate queries
	// and partitioning. The exact ID format is owned by the producing
	// context.
	AggregateID() string

	// OccurredAt is the wall-clock time the event happened, as observed
	// by the producer. Set once at construction; never updated.
	OccurredAt() time.Time
}

// Envelope wraps an Event with delivery metadata. The Bus exposes
// Envelopes to handlers; producers only ever construct Events.
type Envelope struct {
	// ID is a UUIDv7 (time-ordered) assigned by the Bus at publish time.
	ID uuid.UUID

	// Topic, AggregateID, OccurredAt mirror the Event fields and are
	// duplicated here so handlers and persistence can index without
	// re-deserializing the payload.
	Topic       string
	AggregateID string
	OccurredAt  time.Time

	// Payload is the JSON-encoded Event body. Handlers unmarshal into
	// the concrete event type they expect for the topic.
	Payload json.RawMessage

	// Attempts is the 1-based delivery attempt count. First delivery
	// is Attempts == 1. Incremented on retry.
	Attempts int
}

// Handler processes a delivered Envelope. Returning a non-nil error
// signals delivery failure: the Bus will retry per its retry policy
// (typically exponential backoff up to a cap). Handlers MUST be
// idempotent — at-least-once delivery is the contract.
//
// The ctx passed to a Handler carries delivery deadlines and is
// cancelled if the Bus is shutting down. Handlers should respect
// cancellation and return promptly.
type Handler func(ctx context.Context, env Envelope) error

// Bus publishes events and delivers them to subscribers.
//
// Implementations:
//
//   - adapter/eventbus/memory: in-process pub/sub for tests.
//   - adapter/eventbus/outbox: SQLite-backed transactional outbox for
//     production. Events published via this bus are written to the
//     outbox table inside the surrounding transaction (see domain/tx
//     for transaction propagation), so state changes and event emission
//     are atomic.
type Bus interface {
	// Publish records events for delivery. For the outbox bus, ctx must
	// carry an active transaction (see tx.TransactionManager). For the
	// memory bus, ctx is informational only.
	//
	// Returns the first error encountered. On error, no partial state is
	// observed (writes happen as part of the surrounding tx).
	Publish(ctx context.Context, evts ...Event) error

	// Subscribe registers a Handler for events matching topic. The name
	// is a stable identifier for this subscription, used for outbox
	// per-subscriber delivery tracking. Subscribing the same name twice
	// is an error.
	//
	// Topic matching: exact match on Event.Topic(). Wildcards and prefix
	// matching are not supported in v0.1; if needed later they will be
	// added as a separate API.
	//
	// The returned Subscription is the lifecycle handle. Calling Close
	// stops delivery to this handler; in-flight deliveries are allowed
	// to complete.
	Subscribe(name string, topic string, handler Handler) (Subscription, error)
}

// Subscription is the lifecycle handle for a registered Handler.
type Subscription interface {
	Name() string
	Topic() string
	Close() error
}
