// Package webhook implements notify.Sender as HTTP POST.
//
// Each event becomes one POST to the subscriber's URL. The body is the
// bus Envelope serialised as JSON. If the Subscription has a secret,
// the body is signed with HMAC-SHA256 and the digest is sent as
// X-Hoardarr-Signature: sha256=<hex>. Consumers can authenticate the
// payload by recomputing the HMAC with the shared secret.
//
// Retry policy: 3 attempts with exponential backoff (200ms, 400ms,
// 800ms). Network errors and 5xx are retried; 4xx (other than 408 /
// 429) terminate immediately — those are client misconfigurations.
//
// Timeout per attempt: 5s. The outer ctx gates the whole call.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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
	// SignatureHeader is where consumers find the HMAC-SHA256 digest
	// when the subscription has a secret.
	SignatureHeader = "X-Hoardarr-Signature"

	// EventHeader carries the bus topic for routing on the consumer
	// side without forcing them to parse the JSON body.
	EventHeader = "X-Hoardarr-Event"

	// DeliveryHeader carries the envelope ID so consumers can dedupe
	// across at-least-once retries.
	DeliveryHeader = "X-Hoardarr-Delivery"

	defaultTimeout = 5 * time.Second
	maxAttempts    = 3
	baseBackoff    = 200 * time.Millisecond
)

// Sender is the HTTP-POST notify.Sender.
type Sender struct {
	client *http.Client
}

// Compile-time port check.
var _ notify.Sender = (*Sender)(nil)

// New constructs a Sender with a sensible HTTP client.
func New() *Sender {
	return &Sender{
		client: &http.Client{Timeout: defaultTimeout},
	}
}

// NewWithClient lets tests inject an httptest-friendly transport.
func NewWithClient(c *http.Client) *Sender {
	return &Sender{client: c}
}

// Send delivers env to sub.URL.
func (s *Sender) Send(ctx context.Context, sub *notify.Subscription, env event.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	var signature string
	if sub.Secret() != "" {
		mac := hmac.New(sha256.New, []byte(sub.Secret()))
		mac.Write(body)
		signature = "sha256=" + hex.EncodeToString(mac.Sum(nil))
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.attempt(ctx, sub.URL(), body, env, signature)
		if err == nil {
			return nil
		}
		lastErr = err
		// 4xx (except 408 / 429) → don't retry. The retryable predicate
		// inside attempt() distinguishes.
		var nrErr nonRetryableError
		if errors.As(err, &nrErr) {
			return err
		}
		if attempt < maxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(baseBackoff << (attempt - 1)):
			}
		}
	}
	return fmt.Errorf("webhook: %d attempts failed: %w", maxAttempts, lastErr)
}

// attempt does one HTTP POST. Returns non-nil on transport error or
// non-2xx response. Wraps 4xx (other than 408 / 429) in
// nonRetryableError so the caller stops looping.
func (s *Sender) attempt(ctx context.Context, url string, body []byte, env event.Envelope, signature string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(EventHeader, env.Topic)
	req.Header.Set(DeliveryHeader, env.ID.String())
	req.Header.Set("User-Agent", "hoardarr-webhook/1")
	if signature != "" {
		req.Header.Set(SignatureHeader, signature)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		// Drain to enable keep-alive reuse. 64 KiB cap defends against
		// a misbehaving consumer that streams forever.
		_, _ = io.CopyN(io.Discard, resp.Body, 64*1024)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if isRetryableStatus(resp.StatusCode) {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nonRetryableError{status: resp.StatusCode}
}

type nonRetryableError struct{ status int }

func (e nonRetryableError) Error() string {
	return fmt.Sprintf("webhook: non-retryable status %d", e.status)
}

func isRetryableStatus(code int) bool {
	if code == http.StatusRequestTimeout || code == http.StatusTooManyRequests {
		return true
	}
	return code >= 500 && code <= 599
}
