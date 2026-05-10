package sse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
)

// Handler returns the SSE endpoint for /api/v1/queue/stream.
//
// Each connection: subscribes to the Hub, pumps envelopes into the
// response stream as `event: <topic>\ndata: <json>\n\n` blocks.
// A heartbeat comment fires every 15s so reverse proxies don't time
// out idle connections.
//
// Disconnect: when the request context is cancelled (client closed
// the tab, network dropped, the server is shutting down), the handler
// returns and the hub subscription is released.
func Handler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no") // nginx
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		id, ch := hub.Subscribe(256)
		defer hub.Unsubscribe(id)

		// Send a hello so the client knows it's connected.
		_, _ = fmt.Fprintf(w, ": connected %s\n\n", id)
		flusher.Flush()

		heartbeat := time.NewTicker(15 * time.Second)
		defer heartbeat.Stop()

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case env, ok := <-ch:
				if !ok {
					return
				}
				if err := writeSSEEvent(w, env); err != nil {
					return
				}
				flusher.Flush()
			case <-heartbeat.C:
				if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

// writeSSEEvent renders one envelope as an SSE message. The event
// type is the envelope topic; data is the JSON-encoded envelope.
func writeSSEEvent(w http.ResponseWriter, env event.Envelope) error {
	if _, err := fmt.Fprintf(w, "event: %s\n", env.Topic); err != nil {
		return err
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", body); err != nil {
		return err
	}
	return nil
}
