package events

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// WebhookStore abstracts the webhook persistence operations needed by the
// deliverer, keeping it decoupled from the concrete storage layer.
type WebhookStore interface {
	ListActiveForEvent(ctx context.Context, namespaceID uuid.UUID, event string) ([]model.Webhook, error)
	RecordFailure(ctx context.Context, id uuid.UUID) error
	RecordSuccess(ctx context.Context, id uuid.UUID) error
}

// WebhookDeliverer subscribes to all events on the bus and delivers them to
// matching webhooks via HTTP POST with HMAC-SHA256 signing and retries.
type WebhookDeliverer struct {
	bus        EventBus
	store      WebhookStore
	client     *http.Client
	maxRetries int
	timeout    time.Duration

	cancel func()
}

// DelivererOption configures a WebhookDeliverer.
type DelivererOption func(*WebhookDeliverer)

// WithHTTPClient sets the HTTP client used for webhook delivery.
func WithHTTPClient(client *http.Client) DelivererOption {
	return func(d *WebhookDeliverer) {
		d.client = client
	}
}

// WithMaxRetries sets the maximum number of delivery attempts (default 3).
func WithMaxRetries(n int) DelivererOption {
	return func(d *WebhookDeliverer) {
		d.maxRetries = n
	}
}

// WithTimeout sets the per-request timeout (default 10s).
func WithTimeout(t time.Duration) DelivererOption {
	return func(d *WebhookDeliverer) {
		d.timeout = t
	}
}

// NewWebhookDeliverer creates a new deliverer that listens on the bus and
// dispatches events to matching webhooks.
func NewWebhookDeliverer(bus EventBus, store WebhookStore, opts ...DelivererOption) *WebhookDeliverer {
	d := &WebhookDeliverer{
		bus:        bus,
		store:      store,
		client:     &http.Client{},
		maxRetries: 3,
		timeout:    10 * time.Second,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Start subscribes to all events and begins delivering webhooks in a
// background goroutine. It blocks until the context is cancelled or Stop is
// called.
func (d *WebhookDeliverer) Start(ctx context.Context) error {
	ch, unsub, err := d.bus.Subscribe(ctx, "")
	if err != nil {
		return fmt.Errorf("webhook deliverer subscribe: %w", err)
	}
	d.cancel = unsub

	go d.loop(ctx, ch)
	return nil
}

// Stop cancels the subscription and stops the delivery loop.
func (d *WebhookDeliverer) Stop() error {
	if d.cancel != nil {
		d.cancel()
		d.cancel = nil
	}
	return nil
}

func (d *WebhookDeliverer) loop(ctx context.Context, ch <-chan Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			d.handleEvent(ctx, event)
		}
	}
}

func (d *WebhookDeliverer) handleEvent(ctx context.Context, event Event) {
	nsID, ok := parseNamespaceID(event.Scope)
	if !ok {
		return
	}

	webhooks, err := d.store.ListActiveForEvent(ctx, nsID, event.Type)
	if err != nil {
		log.Printf("webhook deliverer: list webhooks for event %s: %v", event.Type, err)
		return
	}

	for i := range webhooks {
		d.deliver(ctx, &webhooks[i], event)
	}
}

func (d *WebhookDeliverer) deliver(ctx context.Context, wh *model.Webhook, event Event) {
	body, err := json.Marshal(event)
	if err != nil {
		log.Printf("webhook deliverer: marshal event %s: %v", event.ID, err)
		return
	}

	var lastErr error
	for attempt := 0; attempt < d.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}

		lastErr = d.sendRequest(ctx, wh, event, body)
		if lastErr == nil {
			if recordErr := d.store.RecordSuccess(ctx, wh.ID); recordErr != nil {
				log.Printf("webhook deliverer: record success for %s: %v", wh.ID, recordErr)
			}
			return
		}
	}

	log.Printf("webhook deliverer: delivery failed after %d attempts for webhook %s: %v", d.maxRetries, wh.ID, lastErr)
	if recordErr := d.store.RecordFailure(ctx, wh.ID); recordErr != nil {
		log.Printf("webhook deliverer: record failure for %s: %v", wh.ID, recordErr)
	}
}

func (d *WebhookDeliverer) sendRequest(ctx context.Context, wh *model.Webhook, event Event, body []byte) error {
	reqCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, wh.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-NRAM-Event", event.Type)
	req.Header.Set("X-NRAM-Delivery", event.ID)

	if wh.Secret != nil && *wh.Secret != "" {
		sig := ComputeHMACSHA256(*wh.Secret, body)
		req.Header.Set("X-NRAM-Signature", "sha256="+sig)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("webhook returned status %d", resp.StatusCode)
}

// ComputeHMACSHA256 computes the hex-encoded HMAC-SHA256 of the given body
// using the provided secret key.
func ComputeHMACSHA256(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// parseNamespaceID extracts a UUID from event scopes of the form
// "prefix:<uuid>". Returns the parsed UUID and true on success, or uuid.Nil
// and false if the scope does not contain a valid UUID component.
func parseNamespaceID(scope string) (uuid.UUID, bool) {
	idx := strings.LastIndex(scope, ":")
	if idx < 0 || idx == len(scope)-1 {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(scope[idx+1:])
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}
