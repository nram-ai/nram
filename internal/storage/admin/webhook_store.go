package admin

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// WebhookAdminStore implements api.WebhookAdminStore by wrapping WebhookRepo.
type WebhookAdminStore struct {
	webhookRepo *storage.WebhookRepo
}

// NewWebhookAdminStore creates a new WebhookAdminStore.
func NewWebhookAdminStore(webhookRepo *storage.WebhookRepo) *WebhookAdminStore {
	return &WebhookAdminStore{webhookRepo: webhookRepo}
}

func (s *WebhookAdminStore) CountWebhooks(ctx context.Context) (int, error) {
	return s.webhookRepo.CountAll(ctx)
}

func (s *WebhookAdminStore) ListWebhooks(ctx context.Context, limit, offset int) ([]model.Webhook, error) {
	return s.webhookRepo.ListAllPaged(ctx, limit, offset)
}

func (s *WebhookAdminStore) CreateWebhook(ctx context.Context, url, scope string, events []string, secret *string, active bool) (*model.Webhook, error) {
	webhook := &model.Webhook{
		URL:    url,
		Scope:  scope,
		Events: events,
		Secret: secret,
		Active: active,
	}
	if err := s.webhookRepo.Create(ctx, webhook); err != nil {
		return nil, fmt.Errorf("create webhook: %w", err)
	}
	return webhook, nil
}

func (s *WebhookAdminStore) GetWebhook(ctx context.Context, id uuid.UUID) (*model.Webhook, error) {
	return s.webhookRepo.GetByID(ctx, id)
}

func (s *WebhookAdminStore) UpdateWebhook(ctx context.Context, id uuid.UUID, url, scope string, events []string, secret *string, active bool) (*model.Webhook, error) {
	webhook, err := s.webhookRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	webhook.URL = url
	webhook.Scope = scope
	webhook.Events = events
	if secret != nil {
		webhook.Secret = secret
	}
	webhook.Active = active

	if err := s.webhookRepo.Update(ctx, webhook); err != nil {
		return nil, err
	}
	return webhook, nil
}

func (s *WebhookAdminStore) DeleteWebhook(ctx context.Context, id uuid.UUID) error {
	return s.webhookRepo.Delete(ctx, id)
}

func (s *WebhookAdminStore) TestWebhook(ctx context.Context, id uuid.UUID) (*api.WebhookTestResult, error) {
	webhook, err := s.webhookRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// Fire a test POST to the webhook URL.
	start := time.Now()
	resp, err := http.Post(webhook.URL, "application/json", nil)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return &api.WebhookTestResult{
			Success:   false,
			Message:   fmt.Sprintf("connection failed: %v", err),
			LatencyMs: latency,
		}, nil
	}
	defer resp.Body.Close()

	return &api.WebhookTestResult{
		Success:    resp.StatusCode >= 200 && resp.StatusCode < 300,
		StatusCode: resp.StatusCode,
		Message:    resp.Status,
		LatencyMs:  latency,
	}, nil
}
