// Package loghub is a process-global slog handler that mirrors every
// log record into an in-memory ring buffer and broadcasts to live
// SSE subscribers. The original handler keeps writing to stdout
// unchanged — this is a tee, not a replacement.
//
// Why a ring buffer: the System page "Logs" tab needs to show the
// last N lines on load and tail new lines after that. Reading from
// stdout would require disk persistence and seek logic; an in-memory
// ring is a few hundred KB of RAM and a few microseconds per log
// record. Older entries fall off the tail when the ring wraps.
package loghub

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Entry is one log record in serialisable form. Time is RFC3339-Nano;
// Attrs is a flat key/value list (string values; complex types
// stringified at capture time).
type Entry struct {
	Time    time.Time         `json:"time"`
	Level   string            `json:"level"`
	Message string            `json:"message"`
	Attrs   map[string]string `json:"attrs,omitempty"`
}

// Hub owns the ring buffer + subscriber set. Construct one with New
// and pass it to NewHandler to bridge slog.
type Hub struct {
	cap      int
	mu       sync.RWMutex
	ring     []Entry
	head     int  // next write index
	wrapped  bool // true once we've filled the ring once

	subsMu sync.Mutex
	subs   map[chan Entry]struct{}
}

// New constructs a Hub with the given capacity. Default 1024 when
// capacity is <= 0.
func New(capacity int) *Hub {
	if capacity <= 0 {
		capacity = 1024
	}
	return &Hub{
		cap:  capacity,
		ring: make([]Entry, capacity),
		subs: make(map[chan Entry]struct{}),
	}
}

// publish appends an entry to the ring and broadcasts to subscribers.
// Slow subscribers (channel full) miss entries — that's fine, this is
// a "tail" view, not an audit log.
func (h *Hub) publish(e Entry) {
	h.mu.Lock()
	h.ring[h.head] = e
	h.head = (h.head + 1) % h.cap
	if h.head == 0 {
		h.wrapped = true
	}
	h.mu.Unlock()

	h.subsMu.Lock()
	for ch := range h.subs {
		select {
		case ch <- e:
		default:
		}
	}
	h.subsMu.Unlock()
}

// Snapshot returns the current ring contents oldest → newest. Used by
// /api/v1/system/logs for the initial paint on the Logs tab.
func (h *Hub) Snapshot() []Entry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.wrapped {
		// Ring not yet full — just the live prefix.
		out := make([]Entry, h.head)
		copy(out, h.ring[:h.head])
		return out
	}
	out := make([]Entry, h.cap)
	copy(out, h.ring[h.head:])
	copy(out[h.cap-h.head:], h.ring[:h.head])
	return out
}

// Subscribe registers a channel that receives new entries. The
// returned cancel removes the subscription and is safe to call any
// number of times. Channel buffer of 16 is enough to absorb a small
// burst without dropping; under sustained pressure entries are
// dropped silently.
func (h *Hub) Subscribe() (chan Entry, func()) {
	ch := make(chan Entry, 16)
	h.subsMu.Lock()
	h.subs[ch] = struct{}{}
	h.subsMu.Unlock()
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.subsMu.Lock()
			delete(h.subs, ch)
			h.subsMu.Unlock()
			close(ch)
		})
	}
	return ch, cancel
}

// Handler is a slog.Handler that mirrors records into Hub. Compose
// with a base handler that takes care of formatted output:
//
//	base := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
//	h := loghub.NewHandler(base, hub)
//	slog.SetDefault(slog.New(h))
type Handler struct {
	base slog.Handler
	hub  *Hub

	attrs []slog.Attr
	group string
}

// NewHandler wires a Hub-mirroring slog.Handler over base.
func NewHandler(base slog.Handler, hub *Hub) *Handler {
	return &Handler{base: base, hub: hub}
}

// Enabled defers to base — we don't want loghub to enable levels the
// base would skip.
func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(ctx, level)
}

// Handle forwards the record to base and publishes to the hub.
func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	if err := h.base.Handle(ctx, r); err != nil {
		return err
	}
	entry := Entry{
		Time:    r.Time,
		Level:   r.Level.String(),
		Message: r.Message,
	}
	// Collect attributes flat. slog.Record.Attrs walks them in order;
	// we apply any WithAttrs prefix first.
	attrs := map[string]string{}
	for _, a := range h.attrs {
		attrs[fullKey(h.group, a.Key)] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs[fullKey(h.group, a.Key)] = a.Value.String()
		return true
	})
	if len(attrs) > 0 {
		entry.Attrs = attrs
	}
	h.hub.publish(entry)
	return nil
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &Handler{
		base:  h.base.WithAttrs(attrs),
		hub:   h.hub,
		attrs: append(append([]slog.Attr(nil), h.attrs...), attrs...),
		group: h.group,
	}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{
		base:  h.base.WithGroup(name),
		hub:   h.hub,
		attrs: h.attrs,
		group: joinGroup(h.group, name),
	}
}

func fullKey(group, key string) string {
	if group == "" {
		return key
	}
	return group + "." + key
}

func joinGroup(a, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}

// Format renders an entry into a one-line human-readable string. Used
// by the SSE endpoint and by tests.
func (e Entry) Format() string {
	var sb strings.Builder
	sb.WriteString(e.Time.Format("15:04:05.000"))
	sb.WriteString(" ")
	sb.WriteString(e.Level)
	sb.WriteString(" ")
	sb.WriteString(e.Message)
	for k, v := range e.Attrs {
		sb.WriteString(" ")
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(v)
	}
	return sb.String()
}

// LevelName converts a slog.Level into the canonical lower-case label
// used by the UI filter dropdown.
func LevelName(lvl slog.Level) string {
	return strings.ToLower(fmt.Sprintf("%s", lvl))
}
