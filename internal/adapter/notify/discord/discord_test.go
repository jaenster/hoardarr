package discord

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/notify/render"
	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/notify"
)

// TestSend_RichEmbed exercises the Sonarr-style embed shape: title is
// the cleaned release name, description leads with the bold verb and a
// code-blocked raw release, fields are a 2-col grid sourced from the
// hydrated job snapshot.
func TestSend_RichEmbed(t *testing.T) {
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

	// Enriched envelope shape, as the notify service produces it after
	// looking up the job.
	payload := []byte(`{
		"event": {"job_id": 42},
		"job": {
			"id": 42,
			"name": "Foo.Bar.S01E02.1080p.WEB-DL.H264-RLSGRP",
			"category": "tv",
			"state": "completed",
			"source": "Sonarr/4.0.0",
			"total_bytes": 5368709120,
			"file_count": 4
		}
	}`)
	env := event.Envelope{
		Topic:       "deliver.complete",
		AggregateID: "42",
		OccurredAt:  time.Unix(1_700_000_000, 0).UTC(),
		Payload:     payload,
	}

	if err := NewWithClient(srv.Client()).Send(context.Background(), sub, env); err != nil {
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

	if title, _ := embed["title"].(string); !strings.Contains(title, "Foo Bar S01E02") {
		t.Errorf("title should be cleaned release; got %q", title)
	}
	desc, _ := embed["description"].(string)
	if !strings.Contains(desc, "**Delivered**") {
		t.Errorf("description should bold the verb; got %q", desc)
	}
	if !strings.Contains(desc, "Foo.Bar.S01E02.1080p.WEB-DL.H264-RLSGRP") {
		t.Errorf("description should include raw release in code block; got %q", desc)
	}
	if color, _ := embed["color"].(float64); int(color) != render.OutcomeOK.Color() {
		t.Errorf("colour = %v; want OK (%d)", color, render.OutcomeOK.Color())
	}

	// Field grid: confirm the keys we care about are present and the
	// debug "Topic"/"Aggregate" fields are NOT.
	fields, _ := embed["fields"].([]any)
	names := map[string]string{}
	for _, f := range fields {
		fm := f.(map[string]any)
		names[fm["name"].(string)] = fm["value"].(string)
	}
	for _, want := range []string{"Source", "Category", "Size", "Files", "Quality", "State"} {
		if _, ok := names[want]; !ok {
			t.Errorf("missing field %q in embed", want)
		}
	}
	for _, debug := range []string{"Topic", "Aggregate"} {
		if _, ok := names[debug]; ok {
			t.Errorf("debug field %q should no longer be emitted", debug)
		}
	}
	if names["Source"] != "Sonarr" {
		t.Errorf("Source = %q; want Sonarr", names["Source"])
	}
	if names["Size"] != "5.00 GB" {
		t.Errorf("Size = %q; want 5.00 GB", names["Size"])
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

// TestSend_FailureSurfacesError checks that an Error field is appended
// (non-inline) when the topic ends in .failed and the payload carries
// an err / fail_message.
func TestSend_FailureSurfacesError(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	sub, _ := notify.New(notify.NewParams{
		Name: "d", Kind: notify.KindDiscord, URL: srv.URL,
		Topics: []string{"deliver.failed"},
	}, time.Now())

	env := event.Envelope{
		Topic:       "deliver.failed",
		AggregateID: "9",
		OccurredAt:  time.Unix(1_700_000_000, 0).UTC(),
		Payload:     []byte(`{"event":{"err":"target filesystem is read-only"},"job":{"name":"X-G","state":"failed"}}`),
	}
	if err := NewWithClient(srv.Client()).Send(context.Background(), sub, env); err != nil {
		t.Fatalf("Send: %v", err)
	}
	var got map[string]any
	_ = json.Unmarshal(body, &got)
	embed := got["embeds"].([]any)[0].(map[string]any)

	if int(embed["color"].(float64)) != render.OutcomeFail.Color() {
		t.Errorf("colour should be Fail")
	}
	fields, _ := embed["fields"].([]any)
	var hasError bool
	for _, f := range fields {
		fm := f.(map[string]any)
		if fm["name"] == "Error" {
			hasError = true
			if !strings.Contains(fm["value"].(string), "read-only") {
				t.Errorf("Error field should carry message; got %v", fm["value"])
			}
			if fm["inline"].(bool) {
				t.Errorf("Error field should be non-inline")
			}
		}
	}
	if !hasError {
		t.Errorf("Error field missing on .failed event")
	}
}
