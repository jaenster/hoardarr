// Package slack implements notify.Sender for Slack incoming webhooks.
// The bus envelope is normalised by internal/adapter/notify/render and
// poured into Slack's block-kit shape.
//
// Spec reference: https://api.slack.com/messaging/webhooks
//
// We use the "blocks" shape because it renders nicer than `text` for
// structured events, and `attachments` is deprecated.
package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/notify/render"
	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/notify"
)

const timeout = 5 * time.Second

// Sender posts Slack-shape JSON.
type Sender struct {
	client *http.Client
}

var _ notify.Sender = (*Sender)(nil)

// New returns a Sender backed by the default Slack-tuned client.
func New() *Sender {
	return &Sender{client: &http.Client{Timeout: timeout}}
}

// NewWithClient lets tests inject an httptest-friendly transport.
func NewWithClient(c *http.Client) *Sender {
	return &Sender{client: c}
}

// Send formats env as Slack blocks and POSTs it to sub.URL.
func (s *Sender) Send(ctx context.Context, sub *notify.Subscription, env event.Envelope) error {
	body, err := json.Marshal(buildPayload(sub, env))
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.URL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "hoardarr-slack/1")

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
	return errors.New("slack: status " + resp.Status)
}

// buildPayload turns a bus envelope into Slack's webhook body. We emit
// header + section (with the verb + release) + a 2-col fields section
// + a context footer. Slack doesn't support per-message colour without
// the deprecated `attachments` shape, so the colour comes through as a
// leading emoji in the header.
func buildPayload(sub *notify.Subscription, env event.Envelope) map[string]any {
	v := render.From(env)

	title := v.CleanTitle
	if title == "" {
		title = v.Verb
	}
	header := outcomeEmoji(v.Outcome) + " " + title

	// `text` is the fallback for notifications / mobile push (block_kit
	// docs require it for accessibility). Render a one-line summary.
	fallback := fmt.Sprintf("%s — %s", v.Verb, v.Release)
	if v.Release == "" {
		fallback = v.Verb
	}

	blocks := []map[string]any{
		{
			"type": "header",
			"text": map[string]any{
				"type": "plain_text",
				"text": truncate(header, 150),
			},
		},
		{
			"type": "section",
			"text": map[string]any{
				"type": "mrkdwn",
				"text": sectionBody(v),
			},
		},
	}
	if fields := slackFields(v); len(fields) > 0 {
		blocks = append(blocks, map[string]any{
			"type":   "section",
			"fields": fields,
		})
	}
	if v.ErrorMsg != "" {
		blocks = append(blocks, map[string]any{
			"type": "section",
			"text": map[string]any{
				"type": "mrkdwn",
				"text": "*Error*\n```" + truncate(v.ErrorMsg, 1000) + "```",
			},
		})
	}
	blocks = append(blocks, map[string]any{
		"type": "context",
		"elements": []map[string]any{
			{"type": "mrkdwn", "text": "`" + env.Topic + "`"},
			{"type": "mrkdwn", "text": "hoardarr • " + sub.Name() + " • " + env.OccurredAt.Format(time.RFC3339)},
		},
	})

	return map[string]any{
		"text":   fallback,
		"blocks": blocks,
	}
}

// sectionBody is the main section: bold verb, then the raw release in
// a code block for copyability.
func sectionBody(v render.View) string {
	body := "*" + v.Verb + "*"
	if v.Release != "" {
		body += "\n```" + v.Release + "```"
	}
	return body
}

// slackFields returns the 2-col grid of Source/Category/Size/etc as
// Slack mrkdwn fields. Empty values are skipped.
func slackFields(v render.View) []map[string]any {
	type kv struct{ key, val string }
	pairs := []kv{
		{"Source", v.Source},
		{"Category", v.Category},
		{"Size", v.SizeHuman},
		{"Files", fileCountStr(v.FileCount)},
		{"Quality", v.Quality},
		{"State", v.State},
	}
	out := make([]map[string]any, 0, len(pairs))
	for _, p := range pairs {
		if p.val == "" {
			continue
		}
		out = append(out, map[string]any{
			"type": "mrkdwn",
			"text": "*" + p.key + "*\n" + p.val,
		})
	}
	return out
}

func fileCountStr(n int) string {
	if n <= 0 {
		return ""
	}
	return strconv.Itoa(n)
}

// outcomeEmoji is the leading marker used in the Slack header. Slack
// has no per-message colour without `attachments`, so emoji is the
// next-best signalling channel.
func outcomeEmoji(o render.Outcome) string {
	switch o {
	case render.OutcomeOK:
		return "✅"
	case render.OutcomeFail:
		return "❌"
	case render.OutcomeWarn:
		return "⚠️"
	default:
		return "ℹ️"
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
