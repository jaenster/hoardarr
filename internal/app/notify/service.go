// Package notify is the application layer for outbound notifications.
//
// Architecture: the bus delivers events for a fixed curated set of
// "user-meaningful" topics (job completed/failed, deliver complete,
// repair OK/failed, etc.); the service fans each event out to every
// active Subscription that matches the topic.
//
// We subscribe to a curated set rather than every bus topic because
// fine-grained internal events (segment.completed fires per segment;
// queue.* fires on pause/resume) are noise for a webhook consumer and
// would amplify a busy job into thousands of HTTP requests.
//
// Subscription cache: the service maintains an in-memory snapshot of
// active subscriptions, refreshed on every subscription-management
// event (added/updated/removed/enabled/disabled) so we don't hit the
// DB on every dispatched event. Stale by at most one event tick.
package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/notify"
	"github.com/jaenster/hoardarr/internal/domain/tx"
)

// SubscribableTopics is the curated set of bus topics that the notify
// service will forward to webhook subscribers. Internal high-volume
// segment / file events are intentionally excluded.
var SubscribableTopics = []string{
	"download.job.created",
	"download.job.download_complete",
	"download.job.download_failed",
	"download.job.completed",
	"download.job.failed",
	"verify.ok",
	"verify.repair_needed",
	"verify.failed",
	"repair.ok",
	"repair.failed",
	"deliver.complete",
	"deliver.failed",
	"extract.complete",
	"extract.failed",
}

// Service drives webhook (and future) deliveries.
type Service struct {
	repo   notify.Repository
	jobs   download.JobRepository // optional; enables payload enrichment
	sender notify.Sender
	bus    event.Bus
	txm    tx.TransactionManager
	logger *slog.Logger
	now    func() time.Time

	// cache of active subs, refreshed on notify.* admin events.
	cacheMu sync.RWMutex
	cache   []*notify.Subscription

	subs []event.Subscription

	wg      sync.WaitGroup
	rootCtx context.Context
	cancel  context.CancelFunc
	started bool
	mu      sync.Mutex
}

// ServiceParams gathers dependencies.
type ServiceParams struct {
	Repo      notify.Repository
	Sender    notify.Sender
	Bus       event.Bus
	TxManager tx.TransactionManager
	Logger    *slog.Logger
	Now       func() time.Time
	// Jobs enables payload enrichment: when an event carries a
	// job_id, the dispatched envelope is augmented with a job
	// snapshot. Optional — nil disables enrichment.
	Jobs download.JobRepository
}

// New constructs a Service.
func New(p ServiceParams) *Service {
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	if p.Now == nil {
		p.Now = func() time.Time { return time.Now().UTC() }
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	return &Service{
		repo:    p.Repo,
		jobs:    p.Jobs,
		sender:  p.Sender,
		bus:     p.Bus,
		txm:     p.TxManager,
		logger:  p.Logger,
		now:     p.Now,
		rootCtx: rootCtx,
		cancel:  cancel,
	}
}

// Start loads the initial sub cache and registers bus subscriptions:
// one per curated topic (forwards to dispatch), plus one each on the
// admin events that invalidate the cache.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	s.rootCtx, s.cancel = context.WithCancel(context.Background())

	if err := s.refreshCache(ctx); err != nil {
		return fmt.Errorf("initial sub cache: %w", err)
	}

	for _, topic := range SubscribableTopics {
		t := topic
		sub, err := s.bus.Subscribe("notify-"+t, t, func(ctx context.Context, env event.Envelope) error {
			return s.onEvent(ctx, env)
		})
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", t, err)
		}
		s.subs = append(s.subs, sub)
	}

	for _, t := range []string{
		"notify.subscription.added",
		"notify.subscription.enabled",
		"notify.subscription.disabled",
		"notify.subscription.removed",
	} {
		topic := t
		sub, err := s.bus.Subscribe("notify-cache-"+t, topic, func(ctx context.Context, _ event.Envelope) error {
			return s.refreshCache(ctx)
		})
		if err != nil {
			return fmt.Errorf("subscribe cache invalidator %s: %w", topic, err)
		}
		s.subs = append(s.subs, sub)
	}

	s.started = true
	s.logger.Info("notify service started", "topics", len(SubscribableTopics))
	return nil
}

// Stop closes subscriptions and waits for in-flight deliveries.
func (s *Service) Stop() error {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = false
	for _, sub := range s.subs {
		_ = sub.Close()
	}
	s.subs = nil
	s.mu.Unlock()
	s.cancel()
	s.wg.Wait()
	s.logger.Info("notify service stopped")
	return nil
}

// RefreshCache forces an immediate reload of the active sub set.
// Callers: Admin (after Add/SetEnabled/Remove) so the dispatch path
// sees fresh state without waiting for the async bus invalidator;
// Start (initial load); the bus invalidator handlers themselves.
func (s *Service) RefreshCache(ctx context.Context) error {
	return s.refreshCache(ctx)
}

// refreshCache reloads the active subscription set. Called from Start
// and from admin-event handlers.
func (s *Service) refreshCache(ctx context.Context) error {
	all, err := s.repo.List(ctx)
	if err != nil {
		return err
	}
	active := make([]*notify.Subscription, 0, len(all))
	for _, sub := range all {
		if sub.Enabled() {
			active = append(active, sub)
		}
	}
	s.cacheMu.Lock()
	s.cache = active
	s.cacheMu.Unlock()
	return nil
}

// activeSubs returns a copy of the current cache.
func (s *Service) activeSubs() []*notify.Subscription {
	s.cacheMu.RLock()
	out := make([]*notify.Subscription, len(s.cache))
	copy(out, s.cache)
	s.cacheMu.RUnlock()
	return out
}

// onEvent fans the envelope out to every matching active subscription.
// Per-subscription dispatch is a goroutine so a slow subscriber
// doesn't block siblings.
//
// Payload enrichment: if the event carries a job_id, the envelope's
// payload is rewritten to include a job snapshot (name, category,
// state, total/done bytes, file count, source). Subscribers thus
// don't need a follow-up GET to react sensibly — "deliver.complete"
// arrives with everything a Discord embed or shell webhook needs.
func (s *Service) onEvent(ctx context.Context, env event.Envelope) error {
	enriched := s.enrichEnvelope(ctx, env)
	for _, sub := range s.activeSubs() {
		if !sub.MatchesTopic(enriched.Topic) {
			continue
		}
		s.wg.Add(1)
		go s.dispatch(s.rootCtx, sub, enriched)
	}
	_ = ctx
	return nil
}

// enrichEnvelope returns env with Payload rewritten to include a
// snapshot of the relevant Job aggregate when the original payload
// carried a job_id. Falls back to the unmodified envelope when:
//   - the payload doesn't decode as JSON
//   - no job_id field is present
//   - the job repo lookup fails
//
// The original event fields are preserved under "event"; the job
// snapshot goes under "job"; the topic + envelope id stay at top
// level so HMAC consumers don't have to relearn the shape.
func (s *Service) enrichEnvelope(ctx context.Context, env event.Envelope) event.Envelope {
	if s.jobs == nil {
		return env
	}
	var raw map[string]any
	if err := json.Unmarshal(env.Payload, &raw); err != nil {
		return env
	}
	rawID, ok := raw["job_id"]
	if !ok {
		return env
	}
	id, ok := asJobID(rawID)
	if !ok || id == 0 {
		return env
	}
	j, err := s.jobs.ByID(ctx, id)
	if err != nil {
		return env
	}
	merged := map[string]any{
		"event": raw,
		"job": map[string]any{
			"id":           int64(j.ID()),
			"name":         j.Name(),
			"category":     j.Category(),
			"state":        string(j.State()),
			"source":       j.Source(),
			"total_bytes":  j.TotalBytes(),
			"done_bytes":   j.DoneBytes(),
			"failed_bytes": j.FailedBytes(),
			"file_count":   len(j.Files()),
			"added_at":     j.AddedAt().Format("2006-01-02T15:04:05Z07:00"),
		},
	}
	body, err := json.Marshal(merged)
	if err != nil {
		return env
	}
	out := env
	out.Payload = body
	return out
}

// asJobID extracts a domain JobID from a JSON-decoded number. JSON
// unmarshal yields float64 for numbers in interface{}, so a direct
// type assertion won't work.
func asJobID(v any) (download.JobID, bool) {
	switch n := v.(type) {
	case float64:
		return download.JobID(int64(n)), true
	case int64:
		return download.JobID(n), true
	case int:
		return download.JobID(n), true
	default:
		return 0, false
	}
}

// dispatch invokes the Sender for one (sub, event) pair and updates
// the per-sub telemetry. Errors are logged and recorded; the bus
// always sees this as a successful handler return because retry at
// the bus level would duplicate deliveries (the Sender handles its
// own retry budget).
func (s *Service) dispatch(ctx context.Context, sub *notify.Subscription, env event.Envelope) {
	defer s.wg.Done()
	err := s.sender.Send(ctx, sub, env)
	if err != nil {
		s.logger.Warn("notify: delivery failed",
			"sub_id", sub.ID(), "topic", env.Topic, "err", err)
		_ = s.recordOutcome(ctx, sub.ID(), false, err.Error())
		return
	}
	_ = s.recordOutcome(ctx, sub.ID(), true, "")
}

// recordOutcome persists last_success / last_error for the sub. We
// keep this in a separate tx (no bus event) because there's no
// downstream subscriber interested in the per-delivery telemetry;
// it's purely UI display.
func (s *Service) recordOutcome(ctx context.Context, id notify.SubscriptionID, ok bool, msg string) error {
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		sub, err := s.repo.ByID(ctx, id)
		if err != nil {
			return err
		}
		now := s.now()
		if ok {
			sub.MarkDeliverySuccess(now)
		} else {
			sub.MarkDeliveryFailure(msg, now)
		}
		return s.repo.Save(ctx, sub)
	})
}

// TestPayload is what /api/v1/subscriptions/{id}/test sends as the
// event payload. Documented here so consumers know what to expect.
type TestPayload struct {
	Note string `json:"note"`
}

// Test sends a synthetic event to one subscription using the Sender
// directly (does not flow through the bus). Used by Settings UI to
// verify a fresh subscription works.
func (s *Service) Test(ctx context.Context, id notify.SubscriptionID) error {
	sub, err := s.repo.ByID(ctx, id)
	if err != nil {
		return err
	}
	payload, _ := json.Marshal(TestPayload{Note: "hoardarr test event"})
	env := event.Envelope{
		Topic:       "notify.test",
		AggregateID: "test",
		OccurredAt:  s.now(),
		Payload:     payload,
	}
	if err := s.sender.Send(ctx, sub, env); err != nil {
		_ = s.recordOutcome(ctx, id, false, err.Error())
		return err
	}
	_ = s.recordOutcome(ctx, id, true, "")
	return nil
}
