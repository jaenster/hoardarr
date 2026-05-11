package loghub

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestHub_SnapshotEmpty(t *testing.T) {
	h := New(8)
	if got := h.Snapshot(); len(got) != 0 {
		t.Errorf("empty Snapshot len = %d; want 0", len(got))
	}
}

func TestHub_FillUnderCap(t *testing.T) {
	h := New(8)
	for i := 0; i < 3; i++ {
		h.publish(Entry{Message: "msg", Time: time.Unix(int64(i), 0)})
	}
	snap := h.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("Snapshot len = %d; want 3", len(snap))
	}
	if snap[0].Time.Unix() != 0 || snap[2].Time.Unix() != 2 {
		t.Errorf("ordering wrong: %v", snap)
	}
}

func TestHub_WrapDropsOldest(t *testing.T) {
	const cap = 4
	h := New(cap)
	// Push 6 entries; only the last 4 should survive.
	for i := 0; i < 6; i++ {
		h.publish(Entry{Message: "m", Time: time.Unix(int64(i), 0)})
	}
	snap := h.Snapshot()
	if len(snap) != cap {
		t.Fatalf("Snapshot len = %d; want %d", len(snap), cap)
	}
	// Oldest surviving entry should be index 2.
	if snap[0].Time.Unix() != 2 {
		t.Errorf("oldest after wrap = %d; want 2", snap[0].Time.Unix())
	}
	if snap[3].Time.Unix() != 5 {
		t.Errorf("newest = %d; want 5", snap[3].Time.Unix())
	}
}

func TestHub_SubscriberReceivesNewEntries(t *testing.T) {
	h := New(4)
	ch, cancel := h.Subscribe()
	defer cancel()
	go h.publish(Entry{Message: "hello"})
	select {
	case got := <-ch:
		if got.Message != "hello" {
			t.Errorf("got %q; want hello", got.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber didn't receive entry")
	}
}

func TestHub_SubscriberCloseStopsDelivery(t *testing.T) {
	h := New(4)
	ch, cancel := h.Subscribe()
	cancel()
	// Channel must be closed after cancel.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel didn't close channel")
	}
}

func TestHandler_TeesToBaseAndHub(t *testing.T) {
	var stdoutBuf bytes.Buffer
	base := slog.NewTextHandler(&stdoutBuf, &slog.HandlerOptions{Level: slog.LevelInfo})
	h := New(8)
	logger := slog.New(NewHandler(base, h))

	logger.Info("test message", "key", "value")

	if stdoutBuf.Len() == 0 {
		t.Error("base handler did not receive the record")
	}

	snap := h.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("hub snapshot len = %d; want 1", len(snap))
	}
	if snap[0].Message != "test message" {
		t.Errorf("hub message = %q", snap[0].Message)
	}
	if snap[0].Attrs["key"] != "value" {
		t.Errorf("hub attrs = %v; want key=value", snap[0].Attrs)
	}
}

func TestHandler_RespectsBaseLevel(t *testing.T) {
	var sink bytes.Buffer
	base := slog.NewTextHandler(&sink, &slog.HandlerOptions{Level: slog.LevelWarn})
	h := New(8)
	logger := slog.New(NewHandler(base, h))

	logger.DebugContext(context.Background(), "noisy")
	logger.Info("also-ignored")
	logger.Warn("important")

	snap := h.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("hub snapshot len = %d; want 1 (only warn)", len(snap))
	}
	if snap[0].Message != "important" {
		t.Errorf("expected warn-level entry; got %q", snap[0].Message)
	}
}
