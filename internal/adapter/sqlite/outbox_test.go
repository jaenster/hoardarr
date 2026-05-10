package sqlite

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
)

// outboxTestEvent is a minimal event.Event for outbox tests.
type outboxTestEvent struct {
	T  string `json:"topic"`
	A  string `json:"agg"`
	V  string `json:"v"`
	at time.Time
}

func (e outboxTestEvent) Topic() string         { return e.T }
func (e outboxTestEvent) AggregateID() string   { return e.A }
func (e outboxTestEvent) OccurredAt() time.Time { return e.at }

func newOutboxEvt(topic, agg, v string, at time.Time) outboxTestEvent {
	return outboxTestEvent{T: topic, A: agg, V: v, at: at}
}

// quietLogger silences slog output during tests so we don't drown in
// expected-error logs.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func openMigratedDB(t *testing.T) *DB {
	t.Helper()
	db := openTestDB(t)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return db
}

func TestOutbox_Publish_PersistsEvent(t *testing.T) {
	db := openMigratedDB(t)
	bus := NewOutboxBus(db, OutboxOptions{Logger: quietLogger()})
	t.Cleanup(func() { _ = bus.Close() })

	ctx := context.Background()
	now := time.UnixMilli(1_700_000_000_000).UTC()
	if err := bus.Publish(ctx, newOutboxEvt("test.topic", "agg-1", "hello", now)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var n int
	if err := db.QueryRowCtx(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("outbox row count = %d; want 1", n)
	}

	var topic, agg string
	var occurred int64
	if err := db.QueryRowCtx(ctx, `SELECT topic, aggregate_id, occurred_at FROM outbox`).Scan(&topic, &agg, &occurred); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if topic != "test.topic" {
		t.Errorf("topic = %q", topic)
	}
	if agg != "agg-1" {
		t.Errorf("agg = %q", agg)
	}
	if occurred != now.UnixMilli() {
		t.Errorf("occurred = %d; want %d", occurred, now.UnixMilli())
	}
}

func TestOutbox_Publish_InsidesAmbientTx(t *testing.T) {
	db := openMigratedDB(t)
	bus := NewOutboxBus(db, OutboxOptions{Logger: quietLogger()})
	t.Cleanup(func() { _ = bus.Close() })
	txm := NewTxManager(db)
	ctx := context.Background()

	wantErr := errors.New("user-rollback")
	err := txm.InTx(ctx, func(ctx context.Context) error {
		if err := bus.Publish(ctx, newOutboxEvt("a.b.c", "x", "v", time.Time{})); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("InTx error = %v; want wraps wantErr", err)
	}
	var n int
	if err := db.QueryRowCtx(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("outbox row count = %d; want 0 after rollback", n)
	}
}

func TestOutbox_Subscribe_DeliversEvent(t *testing.T) {
	db := openMigratedDB(t)
	bus := NewOutboxBus(db, OutboxOptions{
		Logger:       quietLogger(),
		PollInterval: 20 * time.Millisecond,
	})
	t.Cleanup(func() { _ = bus.Close() })

	var got atomic.Int32
	delivered := make(chan event.Envelope, 1)
	if _, err := bus.Subscribe("worker", "test.topic", func(_ context.Context, env event.Envelope) error {
		got.Add(1)
		delivered <- env
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(context.Background(), newOutboxEvt("test.topic", "agg-7", "p", time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case env := <-delivered:
		if env.Topic != "test.topic" {
			t.Errorf("topic = %q", env.Topic)
		}
		if env.AggregateID != "agg-7" {
			t.Errorf("agg = %q", env.AggregateID)
		}
		if env.Attempts != 1 {
			t.Errorf("attempts = %d; want 1 on first delivery", env.Attempts)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("handler not called; got=%d", got.Load())
	}

	// Wait for delivery row to be marked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var deliveredAt sql_NullInt64
		err := db.QueryRowCtx(context.Background(),
			`SELECT delivered_at FROM outbox_subs WHERE subscription = ?`, "worker",
		).Scan(&deliveredAt)
		if err == nil && deliveredAt.Valid {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("delivered_at never set")
}

// sql_NullInt64 alias avoids importing database/sql in the test file
// header for one type. We re-declare here.
type sql_NullInt64 struct {
	Int64 int64
	Valid bool
}

func (n *sql_NullInt64) Scan(v any) error {
	if v == nil {
		n.Valid = false
		return nil
	}
	switch x := v.(type) {
	case int64:
		n.Int64 = x
		n.Valid = true
	case []byte:
		// number as bytes; not expected here
	}
	return nil
}

func TestOutbox_Subscribe_RetriesOnHandlerError(t *testing.T) {
	db := openMigratedDB(t)
	bus := NewOutboxBus(db, OutboxOptions{
		Logger:       quietLogger(),
		PollInterval: 10 * time.Millisecond,
		BackoffBase:  20 * time.Millisecond,
		BackoffMax:   100 * time.Millisecond,
	})
	t.Cleanup(func() { _ = bus.Close() })

	var calls atomic.Int32
	done := make(chan struct{})
	var doneOnce sync.Once
	if _, err := bus.Subscribe("worker", "topic", func(_ context.Context, _ event.Envelope) error {
		c := calls.Add(1)
		if c < 3 {
			return errors.New("transient")
		}
		doneOnce.Do(func() { close(done) })
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(context.Background(), newOutboxEvt("topic", "x", "v", time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-done:
		if calls.Load() < 3 {
			t.Errorf("calls = %d; want >=3", calls.Load())
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("handler not called 3 times; calls=%d", calls.Load())
	}
}

func TestOutbox_Backoff_DoublesUpToCap(t *testing.T) {
	bus := &OutboxBus{
		backoffBase: 1 * time.Second,
		backoffMax:  10 * time.Second,
	}
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, 1 * time.Second},
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 10 * time.Second}, // capped
		{20, 10 * time.Second},
	}
	for _, c := range cases {
		got := bus.backoff(c.attempts)
		if got != c.want {
			t.Errorf("backoff(%d) = %v; want %v", c.attempts, got, c.want)
		}
	}
}

func TestOutbox_Subscribe_RejectsDuplicate(t *testing.T) {
	db := openMigratedDB(t)
	bus := NewOutboxBus(db, OutboxOptions{Logger: quietLogger()})
	t.Cleanup(func() { _ = bus.Close() })

	if _, err := bus.Subscribe("a", "t1", func(_ context.Context, _ event.Envelope) error { return nil }); err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}
	if _, err := bus.Subscribe("a", "t2", func(_ context.Context, _ event.Envelope) error { return nil }); err == nil {
		t.Fatal("duplicate subscription accepted")
	}
}

// A handler that always errors must not pin the dispatcher in a
// retry loop forever. After MaxDeliveryAttempts, the row is parked
// and the dispatcher stops trying it.
func TestOutbox_PoisonMessage_ParksAfterMaxAttempts(t *testing.T) {
	db := openMigratedDB(t)
	bus := NewOutboxBus(db, OutboxOptions{
		Logger:              quietLogger(),
		PollInterval:        5 * time.Millisecond,
		BackoffBase:         1 * time.Millisecond,
		BackoffMax:          5 * time.Millisecond,
		MaxDeliveryAttempts: 3,
	})
	t.Cleanup(func() { _ = bus.Close() })

	var calls atomic.Int32
	if _, err := bus.Subscribe("poison", "topic", func(_ context.Context, _ event.Envelope) error {
		calls.Add(1)
		return errors.New("always fails")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(context.Background(), newOutboxEvt("topic", "x", "v", time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Wait long enough that all retries have happened. Stable state:
	// calls.Load() should equal MaxDeliveryAttempts.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && calls.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}

	// Give the dispatcher more polls to confirm no more calls happen
	// past the cap.
	time.Sleep(200 * time.Millisecond)
	final := calls.Load()
	if final != 3 {
		t.Errorf("calls = %d; want exactly MaxDeliveryAttempts (3)", final)
	}
}

// A handler that panics must not kill the dispatcher. Recovery treats
// the panic as a regular error and applies the normal retry path.
func TestOutbox_HandlerPanic_Recovered(t *testing.T) {
	db := openMigratedDB(t)
	bus := NewOutboxBus(db, OutboxOptions{
		Logger:              quietLogger(),
		PollInterval:        5 * time.Millisecond,
		BackoffBase:         1 * time.Millisecond,
		BackoffMax:          5 * time.Millisecond,
		MaxDeliveryAttempts: 5,
	})
	t.Cleanup(func() { _ = bus.Close() })

	var calls atomic.Int32
	done := make(chan struct{})
	var doneOnce sync.Once

	if _, err := bus.Subscribe("panicker", "topic", func(_ context.Context, _ event.Envelope) error {
		c := calls.Add(1)
		if c <= 2 {
			panic("intentional test panic")
		}
		doneOnce.Do(func() { close(done) })
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(context.Background(), newOutboxEvt("topic", "x", "v", time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-done:
		// Recovered panics + eventual success: dispatcher survived.
	case <-time.After(2 * time.Second):
		t.Fatalf("dispatcher did not recover from panic; calls=%d", calls.Load())
	}
}

func TestOutbox_Close_StopsDispatchers(t *testing.T) {
	db := openMigratedDB(t)
	bus := NewOutboxBus(db, OutboxOptions{
		Logger:       quietLogger(),
		PollInterval: 10 * time.Millisecond,
	})

	if _, err := bus.Subscribe("a", "t", func(_ context.Context, _ event.Envelope) error { return nil }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	doneCh := make(chan error, 1)
	go func() { doneCh <- bus.Close() }()
	select {
	case err := <-doneCh:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s")
	}
}
