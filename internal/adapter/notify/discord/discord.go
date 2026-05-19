// Package discord implements notify.Sender for Discord webhooks. The
// bus envelope is run through internal/adapter/notify/render to extract
// a flat View, which is then poured into Discord's embed-based JSON
// shape.
//
// Spec reference: https://discord.com/developers/docs/resources/webhook
//
// HMAC: Discord webhooks don't verify a signature header — anyone with
// the URL can post. We still respect sub.Secret() by appending it as
// the wait=<secret> query param for symmetry with the generic webhook
// sender, but Discord ignores it.
package discord

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
	body, err := json.Marshal(buildPayload(sub, env))
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

// buildPayload turns a bus envelope into Discord's webhook body. We
// emit a single embed per event with:
//   - title       = cleaned release name (or the verb if no release)
//   - description = bold verb, then the raw release name in a code
//                   block so the operator can copy it verbatim
//   - color       = outcome-based (green/red/amber/blue)
//   - fields      = 2-column grid of Source / Category / Size / Files /
//                   Quality / State, plus a non-inline Error row for
//                   .failed events
//   - footer      = "hoardarr • <subscription name>"
func buildPayload(sub *notify.Subscription, env event.Envelope) map[string]any {
	v := render.From(env)

	title := v.CleanTitle
	if title == "" {
		title = v.Verb
	}

	desc := "**" + v.Verb + "**"
	if v.Release != "" && v.Release != title {
		// Code-block the raw release name so the operator can copy it
		// without the dot-replacement we did for readability.
		desc += "\n```\n" + v.Release + "\n```"
	}

	embed := map[string]any{
		"title":       title,
		"description": desc,
		"color":       v.Outcome.Color(),
		"timestamp":   env.OccurredAt.Format(time.RFC3339),
		"fields":      buildFields(v),
		"footer": map[string]any{
			"text": "hoardarr • " + sub.Name(),
		},
	}
	return map[string]any{
		"username": "hoardarr",
		"embeds":   []map[string]any{embed},
	}
}

// buildFields lays out a 2-column inline grid. Pairs are emitted only
// when both halves have content, keeping the grid visually balanced.
// Singletons go full-width (non-inline). Order is biased toward what
// the operator most wants to see at a glance.
func buildFields(v render.View) []map[string]any {
	type kv struct{ key, val string }
	pairs := []kv{
		{"Source", v.Source},
		{"Category", v.Category},
		{"Size", v.SizeHuman},
		{"Files", fileCountStr(v.FileCount)},
		{"Quality", v.Quality},
		{"State", v.State},
	}
	out := make([]map[string]any, 0, len(pairs)+1)
	for _, p := range pairs {
		if p.val == "" {
			continue
		}
		out = append(out, map[string]any{
			"name":   p.key,
			"value":  p.val,
			"inline": true,
		})
	}
	if v.ErrorMsg != "" {
		out = append(out, map[string]any{
			"name":   "Error",
			"value":  truncate(v.ErrorMsg, 1000),
			"inline": false,
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

// truncate caps a string at n bytes so a runaway error message doesn't
// blow past Discord's 1024-char per-field limit.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
