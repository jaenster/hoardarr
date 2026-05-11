package discord

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/notify"
)

func TestSend_ShapeAndStatus(t *testing.T) {
	var (
		body        []byte
		contentType string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		contentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	sub, err := notify.New(notify.NewParams{
		Name:   "discord-test",
		Kind:   notify.KindDiscord,
		URL:    srv.URL,
		Topics: []string{"deliver.complete"},
	}, time.Now())
	if err != nil {
		t.Fatalf("notify.New: %v", err)
	}

	env := event.Envelope{
		Topic:       "deliver.complete",
		AggregateID: "42",
		OccurredAt:  time.Unix(1_700_000_000, 0).UTC(),
		Payload:     []byte(`{"name":"release.bin","job_id":42}`),
	}

	s := NewWithClient(srv.Client())
	if err := s.Send(context.Background(), sub, env); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if contentType != "application/json" {
		t.Errorf("Content-Type = %q; want application/json", contentType)
	}

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, body)
	}
	embeds, ok := got["embeds"].([]any)
	if !ok || len(embeds) != 1 {
		t.Fatalf("embeds shape wrong: %v", got["embeds"])
	}
	embed := embeds[0].(map[string]any)
	if embed["title"] != "Delivered" {
		t.Errorf("embed.title = %v; want Delivered", embed["title"])
	}
	if embed["description"] != "release.bin" {
		t.Errorf("embed.description = %v; want release.bin", embed["description"])
	}
	color, _ := embed["color"].(float64)
	if int(color) != colorOK {
		t.Errorf("embed.color = %v; want %d (ok)", color, colorOK)
	}
}

func TestSend_FailingStatusReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sub, _ := notify.New(notify.NewParams{
		Name: "d", Kind: notify.KindDiscord, URL: srv.URL,
		Topics: []string{"deliver.complete"},
	}, time.Now())
	if err := NewWithClient(srv.Client()).Send(context.Background(), sub, event.Envelope{Topic: "deliver.complete"}); err == nil {
		t.Error("expected error for 500")
	}
}
