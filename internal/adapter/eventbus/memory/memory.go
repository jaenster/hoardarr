// Package memory is an in-process implementation of domain/event.Bus.
//
// It is intended for tests, fixtures, and ephemeral situations where
// durability is not required. Delivery is synchronous: Publish iterates
// subscribers and invokes their handlers in the calling goroutine. This
// makes tests deterministic — no goroutine schedule races, no need for
// "wait for delivery" helpers.
//
// For production, use internal/adapter/sqlite's outbox bus instead. See
// the canonical hoardarr plan for the rationale (transactional outbox,
// at-least-once, replayable).
package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jaenster/hoardarr/internal/domain/event"
)

// Bus is an in-process synchronous Bus.
//
// Multiple subscriptions may exist per topic. Publish delivers to all
// matching subscribers in subscription-name order (lexicographic) for
// determinism. The first handler error stops delivery to remaining
// subscribers and is returned to the publisher.
type Bus struct {
	mu     sync.RWMutex
	byName map[string]*subscription            // unique name index
	byTopic map[string]map[string]*subscription // topic -> name -> sub
}

// Compile-time check that Bus satisfies the domain port.
var _ event.Bus = (*Bus)(nil)

// New returns an empty Bus.
func New() *Bus {
	return &Bus{
		byName:  make(map[string]*subscription),
		byTopic: make(map[string]map[string]*subscription),
	}
}

// Publish delivers each event to every subscriber of its topic.
//
// Events are processed in order. Within an event, subscribers are
// invoked in lexicographic name order. The first handler error is
// returned and stops further delivery (for that event and subsequent
// events).
//
// The memory bus does not record delivery state. Re-publishing replays.
func (b *Bus) Publish(ctx context.Context, evts ...event.Event) error {
	for _, e := range evts {
		env, err := makeEnvelope(e)
		if err != nil {
			return fmt.Errorf("envelope %q: %w", e.Topic(), err)
		}
		subs := b.snapshotSubsForTopic(env.Topic)
		for _, sub := range subs {
			if err := sub.handler(ctx, env); err != nil {
				return fmt.Errorf("subscriber %q: %w", sub.name, err)
			}
		}
	}
	return nil
}

// Subscribe registers handler under a unique name for the given topic.
// Subscribing twice with the same name (across all topics) returns
// ErrDuplicateSubscription.
func (b *Bus) Subscribe(name string, topic string, handler event.Handler) (event.Subscription, error) {
	if name == "" {
		return nil, errors.New("subscription name must not be empty")
	}
	if topic == "" {
		return nil, errors.New("topic must not be empty")
	}
	if handler == nil {
		return nil, errors.New("handler must not be nil")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.byName[name]; exists {
		return nil, fmt.Errorf("%w: %q", ErrDuplicateSubscription, name)
	}
	sub := &subscription{
		bus:     b,
		name:    name,
		topic:   topic,
		handler: handler,
	}
	b.byName[name] = sub
	if b.byTopic[topic] == nil {
		b.byTopic[topic] = make(map[string]*subscription)
	}
	b.byTopic[topic][name] = sub
	return sub, nil
}

// snapshotSubsForTopic returns a stable, lexicographically-ordered slice
// of subscriptions matching topic. Returned slice is safe to iterate
// without holding the lock.
func (b *Bus) snapshotSubsForTopic(topic string) []*subscription {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m := b.byTopic[topic]
	if len(m) == 0 {
		return nil
	}
	out := make([]*subscription, 0, len(m))
	for _, s := range m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

func (b *Bus) remove(sub *subscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.byName, sub.name)
	if topicSubs := b.byTopic[sub.topic]; topicSubs != nil {
		delete(topicSubs, sub.name)
		if len(topicSubs) == 0 {
			delete(b.byTopic, sub.topic)
		}
	}
}

// ErrDuplicateSubscription is returned by Subscribe when a name is reused.
var ErrDuplicateSubscription = errors.New("duplicate subscription name")

// subscription is one registered Handler.
type subscription struct {
	bus     *Bus
	name    string
	topic   string
	handler event.Handler
}

func (s *subscription) Name() string  { return s.name }
func (s *subscription) Topic() string { return s.topic }
func (s *subscription) Close() error {
	s.bus.remove(s)
	return nil
}

// makeEnvelope wraps a domain event in a delivery envelope. The Payload
// is the JSON marshaling of the concrete event struct.
func makeEnvelope(e event.Event) (event.Envelope, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return event.Envelope{}, err
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return event.Envelope{}, err
	}
	occurred := e.OccurredAt()
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	return event.Envelope{
		ID:          id,
		Topic:       e.Topic(),
		AggregateID: e.AggregateID(),
		OccurredAt:  occurred,
		Payload:     payload,
		Attempts:    1,
	}, nil
}
