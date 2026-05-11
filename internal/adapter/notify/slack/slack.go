// Package slack implements notify.Sender for Slack incoming webhooks.
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
	"time"

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
	body, err := json.Marshal(slackPayload(sub, env))
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

func slackPayload(sub *notify.Subscription, env event.Envelope) map[string]any {
	title := titleFor(env.Topic)
	body := summarise(env)
	// `text` is fallback for notifications / mobile push (block_kit
	// docs require it for accessibility).
	fallback := fmt.Sprintf("%s — %s", title, body)
	return map[string]any{
		"text": fallback,
		"blocks": []map[string]any{
			{
				"type": "section",
				"text": map[string]any{
					"type": "mrkdwn",
					"text": fmt.Sprintf("*%s*\n%s", title, body),
				},
			},
			{
				"type": "context",
				"elements": []map[string]any{
					{"type": "mrkdwn", "text": "`" + env.Topic + "`"},
					{"type": "mrkdwn", "text": "hoardarr • " + sub.Name()},
				},
			},
		},
	}
}

// titleFor / summarise duplicate Discord's mappings; copying inline
// is cheaper than a notify-helpers package given there are only two
// adapters that need this today.
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
