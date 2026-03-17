package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// enrichmentAdminRequest creates a request with administrator auth context.
func enrichmentAdminRequest(method, url string, body *bytes.Buffer) *http.Request {
	if body == nil {
		body = bytes.NewBuffer(nil)
	}
	req := httptest.NewRequest(method, url, body)
	ac := &auth.AuthContext{
		UserID: uuid.New(),
		Role:   auth.RoleAdministrator,
	}
	return req.WithContext(auth.WithContext(req.Context(), ac))
}

// --- mock EnrichmentAdminStore ---

type mockEnrichmentAdminStore struct {
	queueStatus    *EnrichmentQueueStatus
	queueStatusErr error
	retryCount     int
	retryErr       error
	retryIDs       []uuid.UUID // captured from last RetryFailed call
	paused         bool
	setPausedErr   error
	isPausedErr    error
}

func (m *mockEnrichmentAdminStore) QueueStatus(_ context.Context) (*EnrichmentQueueStatus, error) {
	return m.queueStatus, m.queueStatusErr
}

func (m *mockEnrichmentAdminStore) RetryFailed(_ context.Context, ids []uuid.UUID) (int, error) {
	m.retryIDs = ids
	return m.retryCount, m.retryErr
}

func (m *mockEnrichmentAdminStore) SetPaused(_ context.Context, paused bool) error {
	m.paused = paused
	return m.setPausedErr
}

func (m *mockEnrichmentAdminStore) IsPaused(_ context.Context) (bool, error) {
	return m.paused, m.isPausedErr
}

// --- tests ---

func TestEnrichmentQueueStatus(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	itemID := uuid.New()
	memID := uuid.New()

	store := &mockEnrichmentAdminStore{
		queueStatus: &EnrichmentQueueStatus{
			Counts: EnrichmentQueueCounts{
				Pending:    5,
				Processing: 2,
				Completed:  100,
				Failed:     3,
			},
			Items: []EnrichmentQueueItem{
				{
					ID:        itemID,
					MemoryID:  memID,
					Status:    "pending",
					Attempts:  0,
					CreatedAt: now,
				},
			},
			Paused: false,
		},
	}

	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/enrichment/queue", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp EnrichmentQueueStatus
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Counts.Pending != 5 {
		t.Errorf("expected pending 5, got %d", resp.Counts.Pending)
	}
	if resp.Counts.Processing != 2 {
		t.Errorf("expected processing 2, got %d", resp.Counts.Processing)
	}
	if resp.Counts.Completed != 100 {
		t.Errorf("expected completed 100, got %d", resp.Counts.Completed)
	}
	if resp.Counts.Failed != 3 {
		t.Errorf("expected failed 3, got %d", resp.Counts.Failed)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	if resp.Items[0].ID != itemID {
		t.Errorf("expected item ID %s, got %s", itemID, resp.Items[0].ID)
	}
	if resp.Paused {
		t.Error("expected paused to be false")
	}
}

func TestEnrichmentRetryAll(t *testing.T) {
	store := &mockEnrichmentAdminStore{retryCount: 7}

	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{Store: store})

	req := enrichmentAdminRequest(http.MethodPost, "/v1/admin/enrichment/retry", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]int
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp["retried"] != 7 {
		t.Errorf("expected retried 7, got %d", resp["retried"])
	}

	if len(store.retryIDs) != 0 {
		t.Errorf("expected empty IDs for retry-all, got %d", len(store.retryIDs))
	}
}

func TestEnrichmentRetrySpecificIDs(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	store := &mockEnrichmentAdminStore{retryCount: 2}

	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{Store: store})

	bodyBytes, _ := json.Marshal(enrichmentRetryRequest{IDs: []uuid.UUID{id1, id2}})
	req := enrichmentAdminRequest(http.MethodPost, "/v1/admin/enrichment/retry", bytes.NewBuffer(bodyBytes))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]int
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp["retried"] != 2 {
		t.Errorf("expected retried 2, got %d", resp["retried"])
	}

	if len(store.retryIDs) != 2 {
		t.Fatalf("expected 2 IDs, got %d", len(store.retryIDs))
	}
	if store.retryIDs[0] != id1 || store.retryIDs[1] != id2 {
		t.Errorf("unexpected retry IDs: %v", store.retryIDs)
	}
}

func TestEnrichmentPauseWorkers(t *testing.T) {
	store := &mockEnrichmentAdminStore{}

	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{Store: store})

	req := enrichmentAdminRequest(http.MethodPost, "/v1/admin/enrichment/pause", bytes.NewBufferString(`{"paused":true}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]bool
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !resp["paused"] {
		t.Error("expected paused true in response")
	}

	if !store.paused {
		t.Error("expected store.paused to be true")
	}
}

func TestEnrichmentResumeWorkers(t *testing.T) {
	store := &mockEnrichmentAdminStore{paused: true}

	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{Store: store})

	req := enrichmentAdminRequest(http.MethodPost, "/v1/admin/enrichment/pause", bytes.NewBufferString(`{"paused":false}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]bool
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp["paused"] {
		t.Error("expected paused false in response")
	}

	if store.paused {
		t.Error("expected store.paused to be false")
	}
}

func TestEnrichmentQueueStatusStoreError(t *testing.T) {
	store := &mockEnrichmentAdminStore{
		queueStatusErr: errors.New("database down"),
	}

	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/enrichment/queue", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestEnrichmentRetryStoreError(t *testing.T) {
	store := &mockEnrichmentAdminStore{
		retryErr: errors.New("database down"),
	}

	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{Store: store})

	req := enrichmentAdminRequest(http.MethodPost, "/v1/admin/enrichment/retry", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestEnrichmentUnknownSubPath(t *testing.T) {
	store := &mockEnrichmentAdminStore{}

	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/enrichment/unknown", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestEnrichmentRootReturnsQueueStatus(t *testing.T) {
	store := &mockEnrichmentAdminStore{
		queueStatus: &EnrichmentQueueStatus{
			Counts: EnrichmentQueueCounts{
				Pending:    1,
				Processing: 0,
				Completed:  50,
				Failed:     0,
			},
			Items:  []EnrichmentQueueItem{},
			Paused: true,
		},
	}

	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/enrichment", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp EnrichmentQueueStatus
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Counts.Pending != 1 {
		t.Errorf("expected pending 1, got %d", resp.Counts.Pending)
	}
	if !resp.Paused {
		t.Error("expected paused to be true")
	}
}
