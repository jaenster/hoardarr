package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
)

// testEvent is a minimal event.Event for tests.
type testEvent struct {
	T  string    `json:"topic"`
	A  string    `json:"agg"`
	At time.Time `json:"at"`
	V  string    `json:"v"`
}

func (e testEvent) Topic() string          { return e.T }
func (e testEvent) AggregateID() string    { return e.A }
func (e testEvent) OccurredAt() time.Time  { return e.At }

func newEvt(topic, agg, v string) testEvent {
	return testEvent{T: topic, A: agg, At: time.Now().UTC(), V: v}
}

func TestPublish_DeliversToMatchingTopic(t *testing.T) {
	bus := New()
	var got []event.Envelope
	var mu sync.Mutex
	_, err := bus.Subscribe("subA", "test.topic", func(_ context.Context, env event.Envelope) error {
		mu.Lock()
		got = append(got, env)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(context.Background(), newEvt("test.topic", "agg1", "hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := bus.Publish(context.Background(), newEvt("other.topic", "agg2", "should-not-deliver")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("got %d envelopes; want 1", len(got))
	}
	if got[0].Topic != "test.topic" {
		t.Errorf("topic = %q; want test.topic", got[0].Topic)
	}
	if got[0].AggregateID != "agg1" {
		t.Errorf("agg = %q; want agg1", got[0].AggregateID)
	}
	if len(got[0].Payload) == 0 {
		t.Errorf("payload empty")
	}
}

func TestSubscribe_RejectsDuplicate(t *testing.T) {
	bus := New()
	if _, err := bus.Subscribe("name", "topic.a", func(_ context.Context, _ event.Envelope) error { return nil }); err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}
	_, err := bus.Subscribe("name", "topic.b", func(_ context.Context, _ event.Envelope) error { return nil })
	if !errors.Is(err, ErrDuplicateSubscription) {
		t.Errorf("err = %v; want ErrDuplicateSubscription", err)
	}
}

func TestSubscribe_RejectsBadInputs(t *testing.T) {
	bus := New()
	if _, err := bus.Subscribe("", "t", func(_ context.Context, _ event.Envelope) error { return nil }); err == nil {
		t.Error("empty name accepted")
	}
	if _, err := bus.Subscribe("n", "", func(_ context.Context, _ event.Envelope) error { return nil }); err == nil {
		t.Error("empty topic accepted")
	}
	if _, err := bus.Subscribe("n", "t", nil); err == nil {
		t.Error("nil handler accepted")
	}
}

func TestPublish_DeliversToMultipleSubsInNameOrder(t *testing.T) {
	bus := New()
	var order []string
	mk := func(name string) event.Handler {
		return func(_ context.Context, _ event.Envelope) error {
			order = append(order, name)
			return nil
		}
	}
	// Insert out of order to ensure ordering is by name not insert.
	if _, err := bus.Subscribe("zebra", "t", mk("zebra")); err != nil {
		t.Fatalf("Subscribe zebra: %v", err)
	}
	if _, err := bus.Subscribe("apple", "t", mk("apple")); err != nil {
		t.Fatalf("Subscribe apple: %v", err)
	}
	if _, err := bus.Subscribe("mango", "t", mk("mango")); err != nil {
		t.Fatalf("Subscribe mango: %v", err)
	}

	if err := bus.Publish(context.Background(), newEvt("t", "x", "v")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	want := []string{"apple", "mango", "zebra"}
	if len(order) != len(want) {
		t.Fatalf("order = %v; want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("order[%d] = %q; want %q", i, order[i], want[i])
		}
	}
}

func TestPublish_HandlerErrorPropagates(t *testing.T) {
	bus := New()
	wantErr := errors.New("boom")
	if _, err := bus.Subscribe("a", "t", func(_ context.Context, _ event.Envelope) error { return wantErr }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	err := bus.Publish(context.Background(), newEvt("t", "x", "v"))
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v; want wraps wantErr", err)
	}
}

func TestSubscription_Close(t *testing.T) {
	bus := New()
	calls := 0
	sub, err := bus.Subscribe("a", "t", func(_ context.Context, _ event.Envelope) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), newEvt("t", "x", "v")); err != nil {
		t.Fatalf("Publish 1: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := bus.Publish(context.Background(), newEvt("t", "x", "v")); err != nil {
		t.Fatalf("Publish 2: %v", err)
	}
	if calls != 1 {
		t.Errorf("handler calls = %d; want 1", calls)
	}

	// After close, name should be reusable.
	if _, err := bus.Subscribe("a", "t", func(_ context.Context, _ event.Envelope) error { return nil }); err != nil {
		t.Errorf("re-Subscribe after Close: %v", err)
	}
}
