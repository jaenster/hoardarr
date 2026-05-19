package slack

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/notify"
)

// TestSend_BlockKitShape covers the new Sonarr-style layout: a header
// block (verb + cleaned release), a section with the bold verb and a
// code-blocked raw release, a fields section with Source/Size/etc, and
// a context footer.
func TestSend_BlockKitShape(t *testing.T) {
	var (
		body        []byte
		contentType string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		contentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sub, err := notify.New(notify.NewParams{
		Name: "slack-test", Kind: notify.KindSlack, URL: srv.URL,
		Topics: []string{"deliver.complete"},
	}, time.Now())
	if err != nil {
		t.Fatalf("notify.New: %v", err)
	}

	payload := []byte(`{
		"event": {"job_id": 42},
		"job": {
			"name": "Foo.Bar.S01E02.1080p.WEB-DL-RLSGRP",
			"category": "tv",
			"state": "completed",
			"source": "Radarr/5.0",
			"total_bytes": 1073741824,
			"file_count": 3
		}
	}`)
	env := event.Envelope{
		Topic: "deliver.complete", AggregateID: "42",
		OccurredAt: time.Unix(1_700_000_000, 0).UTC(),
		Payload:    payload,
	}

	if err := NewWithClient(srv.Client()).Send(context.Background(), sub, env); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if contentType != "application/json" {
		t.Errorf("Content-Type = %q", contentType)
	}

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, body)
	}
	if fb, _ := got["text"].(string); !strings.Contains(fb, "Delivered") {
		t.Errorf("fallback text should mention the verb; got %q", fb)
	}
	blocks, _ := got["blocks"].([]any)
	if len(blocks) < 3 {
		t.Fatalf("expected at least 3 blocks (header/section/context); got %d", len(blocks))
	}
	header := blocks[0].(map[string]any)
	if header["type"] != "header" {
		t.Errorf("first block should be header; got %v", header["type"])
	}
	headerText := header["text"].(map[string]any)["text"].(string)
	if !strings.Contains(headerText, "Foo Bar S01E02") {
		t.Errorf("header should contain cleaned release; got %q", headerText)
	}

	// Find the fields-section: type=section, has "fields" key.
	var foundFields bool
	for _, b := range blocks {
		bm := b.(map[string]any)
		if bm["type"] != "section" {
			continue
		}
		fs, ok := bm["fields"].([]any)
		if !ok {
			continue
		}
		foundFields = true
		labels := map[string]bool{}
		for _, f := range fs {
			fm := f.(map[string]any)
			labels[fm["text"].(string)] = true
		}
		// We expect labels in the form "*Source*\nRadarr".
		mustContain := []string{"*Source*", "*Category*", "*Size*", "*Files*"}
		for _, want := range mustContain {
			found := false
			for k := range labels {
				if strings.HasPrefix(k, want) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("fields section missing key starting with %q", want)
			}
		}
	}
	if !foundFields {
		t.Errorf("fields-section block not found")
	}
}

func TestSend_FailingStatusReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sub, _ := notify.New(notify.NewParams{
		Name: "s", Kind: notify.KindSlack, URL: srv.URL,
		Topics: []string{"deliver.complete"},
	}, time.Now())
	if err := NewWithClient(srv.Client()).Send(context.Background(), sub, event.Envelope{Topic: "deliver.complete"}); err == nil {
		t.Error("expected error for 500")
	}
}
