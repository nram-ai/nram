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
	"github.com/nram-ai/nram/internal/provider"
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

func (m *mockEnrichmentAdminStore) QueueStatus(_ context.Context, _ QueueListParams) (*EnrichmentQueueStatus, error) {
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

// --- backfill-extracted-fact-paraphrase ---

func TestEnrichmentBackfillExtractedFactParaphrase_NotWired_503(t *testing.T) {
	// Deployments that do not wire the service expose a 503 so the UI button
	// can render "not available" without a 404.
	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{
		Store: &mockEnrichmentAdminStore{},
	})

	req := enrichmentAdminRequest(http.MethodPost,
		"/v1/admin/enrichment/backfill-extracted-fact-paraphrase",
		bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when backfill not wired, got %d", w.Code)
	}
}

func TestEnrichmentBackfillExtractedFactParaphrase_NonAdmin_403(t *testing.T) {
	// Admin gate must reject non-administrator callers, matching the
	// retry/pause/backfill-augmentation paths.
	called := false
	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{
		Store: &mockEnrichmentAdminStore{},
		BackfillExtractedFactParaphrase: func(_ context.Context, _ uuid.UUID, _ bool, _ int) (int, int, error) {
			called = true
			return 0, 0, nil
		},
	})

	// Plain request with no admin auth context.
	req := httptest.NewRequest(http.MethodPost,
		"/v1/admin/enrichment/backfill-extracted-fact-paraphrase",
		bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-admin, got %d", w.Code)
	}
	if called {
		t.Error("backfill function must not be invoked when admin gate rejects")
	}
}

func TestEnrichmentBackfillExtractedFactParaphrase_MethodNotPost_400(t *testing.T) {
	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{
		Store: &mockEnrichmentAdminStore{},
		BackfillExtractedFactParaphrase: func(_ context.Context, _ uuid.UUID, _ bool, _ int) (int, int, error) {
			return 0, 0, nil
		},
	})

	req := enrichmentAdminRequest(http.MethodGet,
		"/v1/admin/enrichment/backfill-extracted-fact-paraphrase", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-POST, got %d", w.Code)
	}
}

func TestEnrichmentBackfillExtractedFactParaphrase_BadJSON_400(t *testing.T) {
	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{
		Store: &mockEnrichmentAdminStore{},
		BackfillExtractedFactParaphrase: func(_ context.Context, _ uuid.UUID, _ bool, _ int) (int, int, error) {
			return 0, 0, nil
		},
	})

	req := enrichmentAdminRequest(http.MethodPost,
		"/v1/admin/enrichment/backfill-extracted-fact-paraphrase",
		bytes.NewBufferString(`not json`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed JSON, got %d", w.Code)
	}
}

func TestEnrichmentBackfillExtractedFactParaphrase_DryRun_200(t *testing.T) {
	// Dry-run reports the candidate count without enqueueing.
	var capturedDry bool
	var capturedProj uuid.UUID
	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{
		Store: &mockEnrichmentAdminStore{},
		BackfillExtractedFactParaphrase: func(_ context.Context, projectID uuid.UUID, dryRun bool, _ int) (int, int, error) {
			capturedDry = dryRun
			capturedProj = projectID
			return 42, 0, nil
		},
	})

	body := `{"dry_run": true}`
	req := enrichmentAdminRequest(http.MethodPost,
		"/v1/admin/enrichment/backfill-extracted-fact-paraphrase",
		bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", w.Code, w.Body.String())
	}
	if !capturedDry {
		t.Error("dry_run flag not forwarded to backfill closure")
	}
	if capturedProj != uuid.Nil {
		t.Errorf("expected zero ProjectID when omitted, got %s", capturedProj)
	}

	var resp enrichmentBackfillAugmentResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.CandidateCount != 42 {
		t.Errorf("CandidateCount = %d, want 42", resp.CandidateCount)
	}
	if resp.Enqueued != 0 {
		t.Errorf("dry_run should not enqueue; got %d", resp.Enqueued)
	}
	if !resp.DryRun {
		t.Error("DryRun flag not echoed in response")
	}
}

func TestEnrichmentBackfillExtractedFactParaphrase_Execute_200(t *testing.T) {
	projID := uuid.New()
	var capturedProj uuid.UUID
	var capturedLimit int
	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{
		Store: &mockEnrichmentAdminStore{},
		BackfillExtractedFactParaphrase: func(_ context.Context, projectID uuid.UUID, _ bool, limit int) (int, int, error) {
			capturedProj = projectID
			capturedLimit = limit
			return 10, 10, nil
		},
	})

	body, _ := json.Marshal(map[string]any{
		"project_id": projID.String(),
		"limit":      50,
	})
	req := enrichmentAdminRequest(http.MethodPost,
		"/v1/admin/enrichment/backfill-extracted-fact-paraphrase",
		bytes.NewBuffer(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", w.Code, w.Body.String())
	}
	if capturedProj != projID {
		t.Errorf("ProjectID forwarding broken: got %s, want %s", capturedProj, projID)
	}
	if capturedLimit != 50 {
		t.Errorf("Limit forwarding broken: got %d, want 50", capturedLimit)
	}

	var resp enrichmentBackfillAugmentResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.CandidateCount != 10 || resp.Enqueued != 10 {
		t.Errorf("counts wrong: candidates=%d enqueued=%d", resp.CandidateCount, resp.Enqueued)
	}
}

func TestEnrichmentBackfillExtractedFactParaphrase_ServiceError_500(t *testing.T) {
	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{
		Store: &mockEnrichmentAdminStore{},
		BackfillExtractedFactParaphrase: func(_ context.Context, _ uuid.UUID, _ bool, _ int) (int, int, error) {
			return 0, 0, errors.New("downstream blew up")
		},
	})

	req := enrichmentAdminRequest(http.MethodPost,
		"/v1/admin/enrichment/backfill-extracted-fact-paraphrase",
		bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on service error, got %d", w.Code)
	}
}

// --- test-prompt dedicated-provider coverage ---

// capturingLLMProvider records the Model on the last completion request and
// echoes a model back in the response, so a test can assert which model the
// handler asked the provider to use. When the request Model is empty it echoes
// a sentinel, simulating a real provider falling back to its default model.
type capturingLLMProvider struct {
	gotModel string
	content  string
}

func (c *capturingLLMProvider) Complete(_ context.Context, req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	c.gotModel = req.Model
	respModel := req.Model
	if respModel == "" {
		respModel = "default-fact-model"
	}
	return &provider.CompletionResponse{Content: c.content, Model: respModel}, nil
}

func (c *capturingLLMProvider) Name() string     { return "mock" }
func (c *capturingLLMProvider) Models() []string { return []string{"mock"} }

// TestEnrichmentTestPrompt_AugmentUsesDedicatedProvider verifies the augment
// Test surface runs against the query-augmentation provider slot and sends an
// empty model, so the slot's own model is used (the slot falls back to the fact
// provider when unconfigured, per Registry.GetQueryAugment). The capturing
// provider echoes "default-fact-model" for an empty request model, standing in
// for the provider's configured model.
func TestEnrichmentTestPrompt_AugmentUsesDedicatedProvider(t *testing.T) {
	capLLM := &capturingLLMProvider{content: `["how does x work?", "what is x?"]`}
	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{
		Store:                       &mockEnrichmentAdminStore{},
		QueryAugmentProvider:        func() provider.LLMProvider { return capLLM },
		QueryAugmentSystemPromptDef: func(_ context.Context) string { return "You are a query augmenter. Output a JSON array of strings." },
	})

	body := `{"type":"augment","sample_input":"some memory content"}`
	req := enrichmentAdminRequest(http.MethodPost, "/v1/admin/enrichment/test-prompt", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", w.Code, w.Body.String())
	}
	if capLLM.gotModel != "" {
		t.Errorf("augment test must send an empty model so the slot supplies it; got %q", capLLM.gotModel)
	}
	var resp enrichmentTestPromptResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Model != "default-fact-model" {
		t.Errorf("response model = %q, want the provider's own model surfaced", resp.Model)
	}
}

// TestEnrichmentTestPrompt_IngestionUsesDedicatedProvider verifies the ingestion
// Test surface runs against the ingestion-decision provider slot with an empty
// model.
func TestEnrichmentTestPrompt_IngestionUsesDedicatedProvider(t *testing.T) {
	capLLM := &capturingLLMProvider{content: `{"operation":"ADD","target_id":null,"rationale":"distinct"}`}
	h := NewAdminEnrichmentHandler(EnrichmentAdminConfig{
		Store:             &mockEnrichmentAdminStore{},
		IngestionProvider: func() provider.LLMProvider { return capLLM },
		IngestionSystemPromptDefault: func(_ context.Context) string {
			return "You are an ingestion decision engine. Output JSON only."
		},
	})

	body := `{"type":"ingestion","sample_input":"a brand new memory"}`
	req := enrichmentAdminRequest(http.MethodPost, "/v1/admin/enrichment/test-prompt", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", w.Code, w.Body.String())
	}
	if capLLM.gotModel != "" {
		t.Errorf("ingestion test must send an empty model so the slot supplies it; got %q", capLLM.gotModel)
	}
	var resp enrichmentTestPromptResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The canned object response must parse into the decision shape.
	parsed, ok := resp.Parsed.(map[string]any)
	if !ok || parsed["operation"] != "ADD" {
		t.Errorf("expected parsed ingestion decision with operation ADD, got %#v", resp.Parsed)
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
