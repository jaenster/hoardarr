// Package sse provides Server-Sent Events delivery of download events
// to connected UI clients.
//
// Architecture: a Hub subscribes once per topic on the outbox event
// bus. Each connected SSE client gets a buffered channel from the hub.
// When an envelope arrives via the bus, the hub fans it out to every
// client channel; slow consumers are dropped (best-effort delivery).
//
// This decouples durable event delivery (outbox bus → orchestrator,
// future webhooks) from ephemeral live-UI updates. The hub is in-memory;
// its subscribers do not write outbox_subs rows.
package sse

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/jaenster/hoardarr/internal/domain/event"
)

// DefaultTopics is the topic set the Hub forwards by default —
// everything the UI needs to render queue + per-job progress.
var DefaultTopics = []string{
	"download.job.created",
	"download.job.started",
	"download.job.paused",
	"download.job.resumed",
	"download.job.removed",
	"download.job.download_complete",
	"download.job.download_failed",
	"download.job.completed",
	"download.job.failed",
	"download.segment.dispatched",
	"download.segment.completed",
	"download.segment.missing",
	"download.segment.failed",
	"download.file.completed",
	"verify.started",
	"verify.ok",
	"verify.repair_needed",
	"verify.failed",
	"deliver.queued",
	"deliver.started",
	"deliver.complete",
	"deliver.skipped",
	"deliver.failed",
	"extract.queued",
	"extract.started",
	"extract.complete",
	"extract.failed",
	"server.usenet.added",
	"server.usenet.updated",
	"server.usenet.enabled",
	"server.usenet.disabled",
	"server.usenet.removed",
}

// Hub broadcasts event envelopes to subscribed SSE clients.
type Hub struct {
	logger *slog.Logger

	mu      sync.Mutex
	clients map[uuid.UUID]chan event.Envelope

	subs []event.Subscription
}

// NewHub constructs a Hub subscribed to the given topics on bus. Pass
// DefaultTopics for the standard hoardarr UI surface.
//
// The returned hub maintains its bus subscriptions until Close.
func NewHub(bus event.Bus, topics []string, logger *slog.Logger) (*Hub, error) {
	if logger == nil {
		logger = slog.Default()
	}
	h := &Hub{
		logger:  logger,
		clients: make(map[uuid.UUID]chan event.Envelope),
	}
	subs := make([]event.Subscription, 0, len(topics))
	for _, t := range topics {
		name := "sse-hub:" + t
		sub, err := bus.Subscribe(name, t, h.onEvent)
		if err != nil {
			for _, prior := range subs {
				_ = prior.Close()
			}
			return nil, fmt.Errorf("hub subscribe %s: %w", t, err)
		}
		subs = append(subs, sub)
	}
	h.subs = subs
	return h, nil
}

func (h *Hub) onEvent(_ context.Context, env event.Envelope) error {
	h.broadcast(env)
	return nil
}

// Subscribe returns a unique id and a buffered receive-only channel.
// The Hub fans envelopes into this channel; on slow consumer (channel
// full) the event is dropped — UI updates are best-effort. Bumping
// buffer size raises the slowness tolerance.
//
// Caller MUST call Unsubscribe(id) when done; otherwise the channel
// stays in the hub map forever.
func (h *Hub) Subscribe(buffer int) (uuid.UUID, <-chan event.Envelope) {
	if buffer < 1 {
		buffer = 64
	}
	id := uuid.New()
	ch := make(chan event.Envelope, buffer)
	h.mu.Lock()
	h.clients[id] = ch
	h.mu.Unlock()
	return id, ch
}

// Unsubscribe removes the client and closes its channel. Idempotent.
func (h *Hub) Unsubscribe(id uuid.UUID) {
	h.mu.Lock()
	ch, ok := h.clients[id]
	if ok {
		delete(h.clients, id)
	}
	h.mu.Unlock()
	if ok {
		close(ch)
	}
}

// Close drops all bus subscriptions and disconnects every client.
func (h *Hub) Close() error {
	for _, sub := range h.subs {
		_ = sub.Close()
	}
	h.subs = nil

	h.mu.Lock()
	clients := h.clients
	h.clients = make(map[uuid.UUID]chan event.Envelope)
	h.mu.Unlock()
	for _, ch := range clients {
		close(ch)
	}
	return nil
}

// broadcast pushes env to every connected client; drops on full
// channels. Holding the mutex during broadcast is fine because each
// per-client send is non-blocking.
func (h *Hub) broadcast(env event.Envelope) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.clients {
		select {
		case ch <- env:
		default:
			// slow consumer; drop. SSE is best-effort.
		}
	}
}
