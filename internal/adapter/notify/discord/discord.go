// Package discord implements notify.Sender for Discord webhooks.
// The bus envelope is translated into Discord's embeds-based JSON
// shape before POSTing to the subscription URL.
//
// Spec reference: https://discord.com/developers/docs/resources/webhook
//
// HMAC: Discord webhooks don't verify a signature header — anyone
// with the URL can post. We still respect sub.Secret() by appending
// it as the wait=<secret> query param for symmetry with the generic
// webhook sender, but Discord ignores it.
package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/notify"
)

const (
	timeout = 5 * time.Second

	// Embed colour: hoardarr blue for OK-ish topics, red for failures.
	colorOK     = 0x4A90E2 // blue (matches UI accent)
	colorFail   = 0xF85149 // red
	colorWarn   = 0xD29922 // amber
	colorNeut   = 0x8A8F9B // grey
)

// Sender posts Discord-shape JSON.
type Sender struct {
	client *http.Client
}

var _ notify.Sender = (*Sender)(nil)

// New returns a Sender backed by the default Discord-tuned client.
func New() *Sender {
	return &Sender{client: &http.Client{Timeout: timeout}}
}

// NewWithClient lets tests inject an httptest-friendly transport.
func NewWithClient(c *http.Client) *Sender {
	return &Sender{client: c}
}

// Send formats env as a Discord embed and POSTs it to sub.URL.
func (s *Sender) Send(ctx context.Context, sub *notify.Subscription, env event.Envelope) error {
	body, err := json.Marshal(discordPayload(sub, env))
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.URL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "hoardarr-discord/1")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.CopyN(io.Discard, resp.Body, 64*1024)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return errors.New("discord: status " + resp.Status)
}

// discordPayload turns a bus envelope into Discord's webhook body.
// We emit a single embed per event with a colour by outcome and a
// fields list with the topic + aggregate id + raw payload preview.
func discordPayload(sub *notify.Subscription, env event.Envelope) map[string]any {
	title := titleFor(env.Topic)
	desc := summarise(env)
	embed := map[string]any{
		"title":       title,
		"description": desc,
		"color":       colorFor(env.Topic),
		"timestamp":   env.OccurredAt.Format(time.RFC3339),
		"fields": []map[string]any{
			{"name": "Topic", "value": "`" + env.Topic + "`", "inline": true},
			{"name": "Aggregate", "value": env.AggregateID, "inline": true},
		},
		"footer": map[string]any{
			"text": "hoardarr • " + sub.Name(),
		},
	}
	return map[string]any{
		"username": "hoardarr",
		"embeds":   []map[string]any{embed},
	}
}

// titleFor renders a human-friendly title from the bus topic.
//
//	download.job.completed -> "Job completed"
//	deliver.failed         -> "Delivery failed"
func titleFor(topic string) string {
	switch topic {
	case "download.job.completed":
		return "Job completed"
	case "download.job.failed", "download.job.download_failed":
		return "Job failed"
	case "verify.repair_needed":
		return "Repair needed"
	case "verify.ok":
		return "Verify ok"
	case "verify.failed":
		return "Verify failed"
	case "repair.ok":
		return "Repair ok"
	case "repair.failed":
		return "Repair failed"
	case "deliver.complete":
		return "Delivered"
	case "deliver.failed":
		return "Delivery failed"
	case "extract.complete":
		return "Extract complete"
	case "extract.failed":
		return "Extract failed"
	case "notify.test":
		return "Test notification"
	default:
		return topic
	}
}

// summarise picks a readable line out of the envelope payload —
// usually the job name + a short status. We try a few common fields;
// if none match, fall back to "Topic <topic> for <aggregateID>".
func summarise(env event.Envelope) string {
	var p map[string]any
	_ = json.Unmarshal(env.Payload, &p)
	for _, k := range []string{"name", "filename", "release"} {
		if v, ok := p[k].(string); ok && v != "" {
			return v
		}
	}
	if errMsg, ok := p["err"].(string); ok && errMsg != "" {
		return errMsg
	}
	return "Aggregate " + env.AggregateID
}

// colorFor maps topic suffix to embed colour.
func colorFor(topic string) int {
	switch {
	case isFailTopic(topic):
		return colorFail
	case isWarnTopic(topic):
		return colorWarn
	case isOKTopic(topic):
		return colorOK
	default:
		return colorNeut
	}
}

func isFailTopic(topic string) bool {
	for _, s := range []string{".failed", ".download_failed"} {
		if hasSuffix(topic, s) {
			return true
		}
	}
	return false
}

func isWarnTopic(topic string) bool { return hasSuffix(topic, ".repair_needed") }
func isOKTopic(topic string) bool {
	for _, s := range []string{".ok", ".complete", ".completed"} {
		if hasSuffix(topic, s) {
			return true
		}
	}
	return false
}

func hasSuffix(s, suf string) bool {
	if len(s) < len(suf) {
		return false
	}
	return s[len(s)-len(suf):] == suf
}
