package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jaenster/hoardarr/internal/domain/event"
)

// OutboxBus is the SQLite-backed implementation of domain/event.Bus.
//
// Publish writes events to the `outbox` table inside the ambient
// transaction (taken from ctx via TxFromContext). It also writes one
// `outbox_subs` row per currently-registered subscription so each
// subscriber's dispatcher can claim the event independently.
//
// Each subscription has its own dispatcher goroutine that polls
// `outbox_subs` for pending rows, calls the handler, and either marks
// delivered or schedules a retry with exponential backoff.
//
// Delivery contract: at-least-once. Handlers must be idempotent.
type OutboxBus struct {
	db     *DB
	logger *slog.Logger

	pollInterval        time.Duration
	batchSize           int
	backoffBase         time.Duration
	backoffMax          time.Duration
	maxDeliveryAttempts int
	pruneRetention      time.Duration
	pruneInterval       time.Duration

	now func() time.Time // injectable for tests

	mu      sync.RWMutex
	byName  map[string]*outboxSub
	byTopic map[string]map[string]*outboxSub

	stopMu       sync.Mutex
	stopped      bool
	prunerOnce   sync.Once
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

// Compile-time check.
var _ event.Bus = (*OutboxBus)(nil)

// OutboxOptions tunes dispatcher behaviour. Zero values are sensible.
type OutboxOptions struct {
	// PollInterval is how often each subscription's dispatcher wakes up
	// to look for pending rows. Default: 250ms.
	PollInterval time.Duration

	// BatchSize is the maximum events fetched per dispatcher tick.
	// Default: 64.
	BatchSize int

	// BackoffBase is the first-retry delay. Subsequent retries double up
	// to BackoffMax. Default: 1s.
	BackoffBase time.Duration

	// BackoffMax caps the per-retry delay. Default: 10m.
	BackoffMax time.Duration

	// MaxDeliveryAttempts is the cap on per-event retries. Once a
	// row's attempts reach this count it stops being picked up by the
	// dispatcher (it's "poisoned" and parked). The row stays in
	// outbox_subs with delivered_at NULL and last_error populated, so
	// an operator can inspect and either clear the row (manual retry)
	// or accept the loss. Default: 20.
	MaxDeliveryAttempts int

	// PruneRetention is how long delivered outbox_subs rows are kept
	// before the background pruner deletes them. Older rows are
	// reaped together with any outbox rows that have no remaining
	// outbox_subs references. Default 1 hour — long enough to debug
	// recent activity, short enough to keep the tables lean.
	//
	// Without pruning the bus accretes one row per (subscription,
	// event) forever; the dispatcher's working set then grows with
	// total events processed, which on a busy installation reaches
	// millions per day and drags every query through huge indexes.
	PruneRetention time.Duration

	// PruneInterval is how often the background pruner runs.
	// Default 1 minute.
	PruneInterval time.Duration

	// Logger is the slog used for dispatcher diagnostics. Defaults to
	// slog.Default().
	Logger *slog.Logger

	// Now overrides the time source (for tests). Defaults to time.Now.
	Now func() time.Time
}

func (o OutboxOptions) withDefaults() OutboxOptions {
	if o.PollInterval == 0 {
		o.PollInterval = 250 * time.Millisecond
	}
	if o.BatchSize == 0 {
		o.BatchSize = 64
	}
	if o.BackoffBase == 0 {
		o.BackoffBase = 1 * time.Second
	}
	if o.BackoffMax == 0 {
		o.BackoffMax = 10 * time.Minute
	}
	if o.MaxDeliveryAttempts == 0 {
		o.MaxDeliveryAttempts = 20
	}
	if o.PruneRetention == 0 {
		o.PruneRetention = 1 * time.Hour
	}
	if o.PruneInterval == 0 {
		o.PruneInterval = 1 * time.Minute
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Now == nil {
		o.Now = func() time.Time { return time.Now().UTC() }
	}
	return o
}

// NewOutboxBus constructs an OutboxBus over the given DB. The bus does
// not start any goroutines until Subscribe is called.
//
// Callers must call Close to stop dispatchers cleanly on shutdown.
func NewOutboxBus(db *DB, opts OutboxOptions) *OutboxBus {
	opts = opts.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &OutboxBus{
		db:                  db,
		logger:              opts.Logger,
		pollInterval:        opts.PollInterval,
		batchSize:           opts.BatchSize,
		backoffBase:         opts.BackoffBase,
		backoffMax:          opts.BackoffMax,
		maxDeliveryAttempts: opts.MaxDeliveryAttempts,
		pruneRetention:      opts.PruneRetention,
		pruneInterval:       opts.PruneInterval,
		now:                 opts.Now,
		byName:              make(map[string]*outboxSub),
		byTopic:             make(map[string]map[string]*outboxSub),
		ctx:                 ctx,
		cancel:              cancel,
	}
}

// pruneLoop is the background reaper. Deletes delivered outbox_subs
// rows older than pruneRetention and orphaned outbox rows that no
// subscription still references. Lazy-started on first Subscribe so
// tests that build an OutboxBus without ever subscribing don't see
// a stray goroutine.
func (b *OutboxBus) pruneLoop() {
	defer b.wg.Done()
	// Run once on entry — clears any stale backlog from a previous
	// run before the periodic ticker kicks in.
	b.prune()
	t := time.NewTicker(b.pruneInterval)
	defer t.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-t.C:
			b.prune()
		}
	}
}

// prune deletes outbox_subs rows in two cases:
//
//   1. delivered_at IS NOT NULL AND delivered_at < cutoff — they've
//      been delivered and are past retention.
//
//   2. delivered_at IS NULL AND occurred_at < stale-cutoff — they've
//      been pending for so long they're effectively garbage. This is
//      what reaps the orphan rows from a prior bug where Publish
//      created sub rows for subscriptions that didn't match the
//      event's topic; without this, those rows live forever and
//      bloat every dispatcher's scan.
//
// Each DELETE is bounded so the first post-restart sweep doesn't lock
// SQLite for seconds. modernc.org/sqlite is built without
// SQLITE_ENABLE_UPDATE_DELETE_LIMIT so we use the rowid-subquery
// workaround.
func (b *OutboxBus) prune() {
	cutoff := b.now().Add(-b.pruneRetention).UnixMilli()
	// Stale-undelivered cutoff is 24x retention by default — generous
	// so we never reap something a slow subscriber could still
	// deliver, tight enough that misrouted rows from a prior buggy
	// publish path don't accumulate forever.
	staleCutoff := b.now().Add(-24 * b.pruneRetention).UnixMilli()

	// Phase 1: delivered rows past retention.
	for {
		res, err := b.db.ExecContext(b.ctx, `
			DELETE FROM outbox_subs
			WHERE rowid IN (
				SELECT rowid FROM outbox_subs
				WHERE delivered_at IS NOT NULL AND delivered_at < ?
				LIMIT 5000
			)
		`, cutoff)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			b.logger.Warn("outbox prune delivered", "err", err)
			return
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			break
		}
	}

	// Phase 2: stale-undelivered. Joins outbox to get occurred_at; the
	// outbox_subs.event_id index from migration 015 makes the join
	// O(matched rows) instead of a scan.
	for {
		res, err := b.db.ExecContext(b.ctx, `
			DELETE FROM outbox_subs
			WHERE rowid IN (
				SELECT s.rowid FROM outbox_subs s
				JOIN outbox o ON o.id = s.event_id
				WHERE s.delivered_at IS NULL AND o.occurred_at < ?
				LIMIT 5000
			)
		`, staleCutoff)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			b.logger.Warn("outbox prune stale-undelivered", "err", err)
			return
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			break
		}
	}
	// Note: we deliberately don't sweep orphan outbox rows here.
	// The LEFT JOIN-style query is O(outbox × outbox_subs) without
	// a covering index — on the live container that took the writer
	// lock long enough to wedge every other transaction with
	// SQLITE_BUSY. Subs-only prune is what keeps the dispatcher's
	// working set lean; outbox rows are tiny and accumulate slowly,
	// and the upcoming durable-scheduler task can age them out on
	// a much cheaper "id <= cutoff" query (UUID v7 ids are time-
	// ordered, so a single index lookup answers it).
}

// Publish persists each event to the outbox along with a per-subscriber
// pending row, all inside the ambient transaction.
//
// If ctx has no tx (TxFromContext returns nil), Publish opens a single
// short-lived transaction itself. Application code should normally call
// Publish inside an InTx block so state changes and event emission
// commit atomically.
func (b *OutboxBus) Publish(ctx context.Context, evts ...event.Event) error {
	if len(evts) == 0 {
		return nil
	}
	if b.stopped {
		return errors.New("outbox bus: closed")
	}

	// Snapshot the topic→subs map under the read lock so the lookup
	// inside `publish` is a plain map read. The dispatch query filters
	// by o.topic = sub.topic anyway, so creating outbox_subs rows for
	// subscriptions that don't match the event's topic is pure waste —
	// they sit undelivered forever (no match in dispatcher's SELECT)
	// and bloat the table by 50-100×. The old code created one row per
	// sub-per-event regardless of topic; on the live container that
	// turned every published event into ~70 garbage rows.
	subsByTopic := b.snapshotSubsByTopic()

	publish := func(ctx context.Context) error {
		now := b.now().UnixMilli()
		for _, e := range evts {
			id, err := uuid.NewV7()
			if err != nil {
				return fmt.Errorf("uuid: %w", err)
			}
			payload, err := json.Marshal(e)
			if err != nil {
				return fmt.Errorf("marshal %q: %w", e.Topic(), err)
			}
			occurred := e.OccurredAt().UnixMilli()
			if e.OccurredAt().IsZero() {
				occurred = now
			}
			if _, err := b.db.ExecCtx(ctx,
				`INSERT INTO outbox(id, topic, aggregate_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)`,
				id[:], e.Topic(), e.AggregateID(), occurred, payload,
			); err != nil {
				return fmt.Errorf("insert outbox: %w", err)
			}
			// Only create sub rows for subscriptions that subscribed to
			// this event's topic. If no one is listening the event still
			// lands in outbox (for EventsByJob replay queries) but skips
			// the per-sub fan-out entirely.
			for name := range subsByTopic[e.Topic()] {
				if _, err := b.db.ExecCtx(ctx,
					`INSERT INTO outbox_subs(subscription, event_id, attempts) VALUES (?, ?, 0)`,
					name, id[:],
				); err != nil {
					return fmt.Errorf("insert outbox_subs(%s): %w", name, err)
				}
			}
		}
		return nil
	}

	if TxFromContext(ctx) != nil {
		if err := publish(ctx); err != nil {
			return err
		}
	} else {
		txm := NewTxManager(b.db)
		if err := txm.InTx(ctx, publish); err != nil {
			return err
		}
	}

	// Wake matching dispatchers so they pick up the new rows quickly.
	b.wakeMatching(evts)
	return nil
}

// Subscribe registers handler under name and starts a dispatcher
// goroutine for it. Each name must be unique across the bus.
func (b *OutboxBus) Subscribe(name string, topic string, handler event.Handler) (event.Subscription, error) {
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
	if _, exists := b.byName[name]; exists {
		b.mu.Unlock()
		return nil, fmt.Errorf("duplicate subscription: %q", name)
	}
	sub := &outboxSub{
		bus:     b,
		name:    name,
		topic:   topic,
		handler: handler,
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	b.byName[name] = sub
	if b.byTopic[topic] == nil {
		b.byTopic[topic] = make(map[string]*outboxSub)
	}
	b.byTopic[topic][name] = sub
	b.mu.Unlock()

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.dispatchLoop(sub)
	}()

	// Lazy-start the background pruner on the first Subscribe. Tests
	// that build an OutboxBus without subscribing don't pay the cost.
	b.prunerOnce.Do(func() {
		b.wg.Add(1)
		go b.pruneLoop()
	})

	return sub, nil
}

// Close stops all dispatcher goroutines and blocks until they return.
// EventsByJob returns outbox events touching the given job_id,
// ordered by occurred_at ascending. "Touching" is defined as:
//
//   - For download-domain events the aggregate is the job itself or
//     a segment/file under it; we match by the job_id field embedded
//     in the JSON payload.
//   - For verify / repair / deliver / extract events the aggregate
//     id is the per-context row id, but the payload always carries
//     job_id.
//
// SQLite's json_extract is fine here — the outbox is at most tens of
// thousands of rows over the daemon's life and we only filter to one
// job's worth (a few hundred rows). If this becomes hot we'll add
// either a denormalised job_id column or a separate index, but for
// v0.1 the json scan is negligible.
//
// Returns events with their delivered/retry metadata stripped — this
// is the operator-facing "what happened to my job" timeline, not the
// per-subscriber delivery audit.
func (b *OutboxBus) EventsByJob(ctx context.Context, jobID int64) ([]event.Envelope, error) {
	rows, err := b.db.QueryCtx(ctx, `
		SELECT id, topic, aggregate_id, occurred_at, payload
		FROM outbox
		WHERE json_extract(payload, '$.job_id') = ?
		ORDER BY occurred_at ASC, id ASC
	`, jobID)
	if err != nil {
		return nil, fmt.Errorf("query outbox by job: %w", err)
	}
	defer rows.Close()
	var out []event.Envelope
	for rows.Next() {
		var (
			idBlob   []byte
			topic    string
			aggID    string
			occurred int64
			payload  []byte
		)
		if err := rows.Scan(&idBlob, &topic, &aggID, &occurred, &payload); err != nil {
			return nil, err
		}
		var id uuid.UUID
		copy(id[:], idBlob)
		out = append(out, event.Envelope{
			ID:          id,
			Topic:       topic,
			AggregateID: aggID,
			OccurredAt:  time.UnixMilli(occurred).UTC(),
			Payload:     payload,
		})
	}
	return out, rows.Err()
}

func (b *OutboxBus) Close() error {
	b.stopMu.Lock()
	if b.stopped {
		b.stopMu.Unlock()
		return nil
	}
	b.stopped = true
	b.stopMu.Unlock()

	b.cancel()
	b.wg.Wait()
	return nil
}

// snapshotSubsByTopic returns a deep-enough copy of the topic→subs map
// so Publish can iterate it without holding the lock. The inner map is
// keyed by subscription name; the values aren't used (the dispatcher
// already has the *outboxSub via b.byName). Returned map is safe to
// keep — it's a copy.
func (b *OutboxBus) snapshotSubsByTopic() map[string]map[string]struct{} {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]map[string]struct{}, len(b.byTopic))
	for topic, subs := range b.byTopic {
		inner := make(map[string]struct{}, len(subs))
		for name := range subs {
			inner[name] = struct{}{}
		}
		out[topic] = inner
	}
	return out
}

func (b *OutboxBus) snapshotSubNames() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.byName))
	for n := range b.byName {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (b *OutboxBus) wakeMatching(evts []event.Event) {
	topics := make(map[string]struct{}, len(evts))
	for _, e := range evts {
		topics[e.Topic()] = struct{}{}
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for topic := range topics {
		for _, sub := range b.byTopic[topic] {
			select {
			case sub.wake <- struct{}{}:
			default:
			}
		}
	}
}

func (b *OutboxBus) remove(sub *outboxSub) {
	b.mu.Lock()
	delete(b.byName, sub.name)
	if topicSubs := b.byTopic[sub.topic]; topicSubs != nil {
		delete(topicSubs, sub.name)
		if len(topicSubs) == 0 {
			delete(b.byTopic, sub.topic)
		}
	}
	b.mu.Unlock()
}

// dispatchLoop is one subscription's poller. It wakes on tick or on a
// nudge from Publish, processes a batch of pending rows, and loops.
func (b *OutboxBus) dispatchLoop(sub *outboxSub) {
	t := time.NewTicker(b.pollInterval)
	defer t.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-sub.done:
			return
		case <-sub.wake:
		case <-t.C:
		}
		// Drain until the table is empty (or we hit batchSize-of-empty).
		for {
			n, err := b.processBatch(sub)
			if err != nil {
				if errors.Is(err, context.Canceled) || isSQLiteBusy(err) {
					// Shutdown or transient lock — next tick retries.
					b.logger.Debug("outbox dispatch interrupted",
						"subscription", sub.name, "err", err)
				} else {
					b.logger.Error("outbox dispatch", "subscription", sub.name, "err", err)
				}
				break
			}
			if n == 0 {
				break
			}
		}
	}
}

// processBatch fetches up to batchSize pending rows for sub and delivers
// each. Returns the number of rows attempted (regardless of success).
//
// Rows whose attempts have hit MaxDeliveryAttempts are skipped — they
// stay in the table with last_error populated for operator inspection.
func (b *OutboxBus) processBatch(sub *outboxSub) (int, error) {
	rows, err := b.db.QueryContext(b.ctx, `
		SELECT s.event_id, s.attempts, o.topic, o.aggregate_id, o.occurred_at, o.payload
		FROM outbox_subs s
		JOIN outbox o ON o.id = s.event_id
		WHERE s.subscription = ?
		  AND s.delivered_at IS NULL
		  AND s.attempts < ?
		  AND o.topic = ?
		  AND (s.next_retry_at IS NULL OR s.next_retry_at <= ?)
		ORDER BY s.event_id
		LIMIT ?
	`, sub.name, b.maxDeliveryAttempts, sub.topic, b.now().UnixMilli(), b.batchSize)
	if err != nil {
		return 0, fmt.Errorf("query pending: %w", err)
	}

	type item struct {
		id          []byte
		attempts    int
		topic       string
		aggregateID string
		occurredAt  int64
		payload     []byte
	}
	var batch []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.id, &it.attempts, &it.topic, &it.aggregateID, &it.occurredAt, &it.payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan: %w", err)
		}
		batch = append(batch, it)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}

	for _, it := range batch {
		eid, err := uuid.FromBytes(it.id)
		if err != nil {
			b.logger.Error("outbox: corrupt event id", "raw", it.id, "err", err)
			continue
		}
		env := event.Envelope{
			ID:          eid,
			Topic:       it.topic,
			AggregateID: it.aggregateID,
			OccurredAt:  time.UnixMilli(it.occurredAt).UTC(),
			Payload:     append(json.RawMessage(nil), it.payload...),
			Attempts:    it.attempts + 1,
		}
		err = b.invokeHandler(sub, env)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return len(batch), nil
			}
			b.markFailure(sub, it.id, env.Attempts, err)
			continue
		}
		b.markDelivered(sub, it.id)
	}
	return len(batch), nil
}

// invokeHandler runs sub.handler with a deferred recover. A panicking
// handler would otherwise kill the dispatcher goroutine and silently
// stop delivery for that subscription. Treat panic-as-failure so the
// retry/backoff/poison machinery applies uniformly.
func (b *OutboxBus) invokeHandler(sub *outboxSub, env event.Envelope) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
			b.logger.Error("outbox: handler panic recovered",
				"subscription", sub.name, "topic", env.Topic, "panic", r)
		}
	}()
	return sub.handler(b.ctx, env)
}

func (b *OutboxBus) markDelivered(sub *outboxSub, id []byte) {
	if _, err := b.db.ExecContext(b.ctx, `
		UPDATE outbox_subs
		SET delivered_at = ?, last_error = NULL, next_retry_at = NULL
		WHERE subscription = ? AND event_id = ?
	`, b.now().UnixMilli(), sub.name, id); err != nil {
		// SQLITE_BUSY (transient WAL contention) and context.Canceled
		// (shutdown) are recoverable — next dispatcher tick retries.
		// Anything else is a real error.
		if isSQLiteBusy(err) || errors.Is(err, context.Canceled) {
			b.logger.Debug("outbox: mark delivered interrupted; will retry",
				"subscription", sub.name, "err", err)
			return
		}
		b.logger.Error("outbox: mark delivered", "subscription", sub.name, "err", err)
	}
}

func (b *OutboxBus) markFailure(sub *outboxSub, id []byte, attempts int, handlerErr error) {
	delay := b.backoff(attempts)
	next := b.now().Add(delay).UnixMilli()
	if _, err := b.db.ExecContext(b.ctx, `
		UPDATE outbox_subs
		SET attempts = ?, last_error = ?, next_retry_at = ?
		WHERE subscription = ? AND event_id = ?
	`, attempts, handlerErr.Error(), next, sub.name, id); err != nil {
		b.logger.Error("outbox: mark failure", "subscription", sub.name, "err", err)
	}
}

// backoff computes the retry delay for the given attempt count. Attempt 1
// waits BackoffBase; each subsequent attempt doubles up to BackoffMax.
func (b *OutboxBus) backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := b.backoffBase
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= b.backoffMax {
			return b.backoffMax
		}
	}
	return d
}

// outboxSub is one registered subscription.
type outboxSub struct {
	bus      *OutboxBus
	name     string
	topic    string
	handler  event.Handler
	wake     chan struct{}
	done     chan struct{}
	doneOnce sync.Once
}

func (s *outboxSub) Name() string  { return s.name }
func (s *outboxSub) Topic() string { return s.topic }

// Close stops this subscription's dispatcher and removes it from the
// bus's routing maps. Idempotent.
func (s *outboxSub) Close() error {
	s.doneOnce.Do(func() {
		close(s.done)
		s.bus.remove(s)
	})
	return nil
}

// Ensure imports stay live for future error-typed comparisons.
var _ = sql.ErrNoRows

// isSQLiteBusy returns true when err is a SQLITE_BUSY (5) — the
// transient lock-contention condition that resolves on retry.
func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "database is locked")
}
