// Package notify owns the bounded context for outbound notifications:
// webhook subscribers today, Discord / Slack / Pushover later (each as
// its own adapter implementing the Sender port).
//
// A Subscription captures one consumer's interest in a topic set. The
// notify service tails the bus, fans events out to matching subs, and
// the adapter does the delivery. The outbox guarantees at-least-once
// across hoardarr restarts; the per-sub retry policy guards against
// transient subscriber-side failures within a single dispatch.
package notify

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
)

// SubscriptionID identifies a Subscription aggregate.
type SubscriptionID int64

// Kind identifies the delivery adapter that handles the subscription.
// Today only "webhook" exists; "discord" / "slack" / "pushover" are
// adapter-shaped extensions for later without schema churn.
type Kind string

const (
	KindWebhook Kind = "webhook"
	KindDiscord Kind = "discord"
	KindSlack   Kind = "slack"
)

// ValidKind reports whether k is one of the supported delivery
// adapters. Used by the domain constructor and by REST validation.
func ValidKind(k Kind) bool {
	switch k {
	case KindWebhook, KindDiscord, KindSlack:
		return true
	default:
		return false
	}
}

// Subscription is the aggregate root.
type Subscription struct {
	id        SubscriptionID
	name      string
	kind      Kind
	url       string
	topics    []string
	secret    string
	enabled   bool

	// Operational telemetry. These move via Mark* methods so they
	// stay inside the aggregate boundary; the service updates them
	// post-delivery.
	lastSuccessAt time.Time
	lastErrorAt   time.Time
	lastError     string

	createdAt time.Time
	updatedAt time.Time

	events []event.Event
}

// NewParams gathers required fields.
type NewParams struct {
	Name    string
	Kind    Kind
	URL     string
	Topics  []string
	Secret  string // optional; used as HMAC key when non-empty
}

// New constructs a Subscription with validation. The returned aggregate
// has a pending SubscriptionAdded event; call PullEvents after the
// repo Save succeeds.
func New(p NewParams, now time.Time) (*Subscription, error) {
	name := strings.TrimSpace(p.Name)
	if name == "" {
		return nil, errors.New("notify: name required")
	}
	if len(name) > 128 {
		return nil, errors.New("notify: name too long (max 128)")
	}
	kind := p.Kind
	if kind == "" {
		kind = KindWebhook
	}
	if !ValidKind(kind) {
		return nil, fmt.Errorf("notify: unknown subscription kind %q", kind)
	}
	if err := validateURL(p.URL); err != nil {
		return nil, err
	}
	topics, err := normaliseTopics(p.Topics)
	if err != nil {
		return nil, err
	}

	s := &Subscription{
		name:      name,
		kind:      kind,
		url:       p.URL,
		topics:    topics,
		secret:    p.Secret,
		enabled:   true,
		createdAt: now,
		updatedAt: now,
	}
	s.events = append(s.events, SubscriptionAdded{
		ID: 0, Name: name, Kind: string(kind), URL: p.URL, Topics: topics, At: now,
	})
	return s, nil
}

// HydrateParams is the snapshot the repository hands back.
type HydrateParams struct {
	ID            SubscriptionID
	Name          string
	Kind          Kind
	URL           string
	Topics        []string
	Secret        string
	Enabled       bool
	LastSuccessAt time.Time
	LastErrorAt   time.Time
	LastError     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Hydrate reconstructs without emitting events.
func Hydrate(p HydrateParams) *Subscription {
	return &Subscription{
		id:            p.ID,
		name:          p.Name,
		kind:          p.Kind,
		url:           p.URL,
		topics:        p.Topics,
		secret:        p.Secret,
		enabled:       p.Enabled,
		lastSuccessAt: p.LastSuccessAt,
		lastErrorAt:   p.LastErrorAt,
		lastError:     p.LastError,
		createdAt:     p.CreatedAt,
		updatedAt:     p.UpdatedAt,
	}
}

// Accessors.
func (s *Subscription) ID() SubscriptionID    { return s.id }
func (s *Subscription) Name() string           { return s.name }
func (s *Subscription) Kind() Kind             { return s.kind }
func (s *Subscription) URL() string            { return s.url }
func (s *Subscription) Topics() []string       { return s.topics }
func (s *Subscription) Secret() string         { return s.secret }
func (s *Subscription) Enabled() bool          { return s.enabled }
func (s *Subscription) LastSuccessAt() time.Time { return s.lastSuccessAt }
func (s *Subscription) LastErrorAt() time.Time   { return s.lastErrorAt }
func (s *Subscription) LastError() string         { return s.lastError }
func (s *Subscription) CreatedAt() time.Time      { return s.createdAt }
func (s *Subscription) UpdatedAt() time.Time      { return s.updatedAt }

// MatchesTopic reports whether this subscription is interested in
// the given event topic. Topics are matched exactly OR by trailing
// wildcard: "deliver.*" matches any topic starting with "deliver.".
func (s *Subscription) MatchesTopic(topic string) bool {
	for _, t := range s.topics {
		if t == topic {
			return true
		}
		if strings.HasSuffix(t, "*") && strings.HasPrefix(topic, t[:len(t)-1]) {
			return true
		}
	}
	return false
}

// SetID is called by the repository after INSERT.
func (s *Subscription) SetID(id SubscriptionID) {
	s.id = id
	for i := range s.events {
		if e, ok := s.events[i].(SubscriptionAdded); ok && e.ID == 0 {
			e.ID = id
			s.events[i] = e
		}
	}
}

// PullEvents drains buffered events.
func (s *Subscription) PullEvents() []event.Event {
	out := s.events
	s.events = nil
	return out
}

// SetEnabled flips the active flag. Emits SubscriptionEnabled or
// SubscriptionDisabled.
func (s *Subscription) SetEnabled(enabled bool, now time.Time) {
	if s.enabled == enabled {
		return
	}
	s.enabled = enabled
	s.updatedAt = now
	if enabled {
		s.events = append(s.events, SubscriptionEnabled{ID: s.id, At: now})
	} else {
		s.events = append(s.events, SubscriptionDisabled{ID: s.id, At: now})
	}
}

// MarkDeliverySuccess updates the operational telemetry after a
// successful delivery. No event is emitted — this is bookkeeping that
// only affects the Settings UI's "last success" display.
func (s *Subscription) MarkDeliverySuccess(now time.Time) {
	s.lastSuccessAt = now
	s.lastError = ""
	s.updatedAt = now
}

// MarkDeliveryFailure records a delivery failure for UI display.
func (s *Subscription) MarkDeliveryFailure(reason string, now time.Time) {
	s.lastErrorAt = now
	s.lastError = reason
	s.updatedAt = now
}

// MarkRemoved is called by the application service immediately before
// deleting the row, so a SubscriptionRemoved event commits in the
// same tx and downstream listeners see it.
func (s *Subscription) MarkRemoved(now time.Time) {
	s.events = append(s.events, SubscriptionRemoved{ID: s.id, Name: s.name, At: now})
}

// --- validation helpers ---------------------------------------------

func validateURL(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return errors.New("notify: url required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	switch u.Scheme {
	case "http", "https":
		// ok
	default:
		return errors.New("notify: url scheme must be http or https")
	}
	if u.Host == "" {
		return errors.New("notify: url must include a host")
	}
	return nil
}

// normaliseTopics dedupes + sorts the topic list and rejects empties.
// Wildcards (trailing "*") are kept verbatim.
func normaliseTopics(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, errors.New("notify: at least one topic required")
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, t := range in {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil, errors.New("notify: at least one non-empty topic required")
	}
	sort.Strings(out)
	return out, nil
}
