package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

// --- mock UsageStore ---

type mockUsageStore struct {
	report    *UsageReport
	err       error
	lastFilter UsageFilter
}

func (m *mockUsageStore) QueryUsage(_ context.Context, filter UsageFilter) (*UsageReport, error) {
	m.lastFilter = filter
	return m.report, m.err
}

// --- helpers ---

func defaultUsageReport() *UsageReport {
	return &UsageReport{
		Groups: []UsageGroup{
			{Key: "enrich", TokensInput: 1000, TokensOutput: 500, CallCount: 10},
			{Key: "search", TokensInput: 200, TokensOutput: 100, CallCount: 5},
		},
		Totals: UsageTotals{
			TokensInput:  1200,
			TokensOutput: 600,
			CallCount:    15,
		},
	}
}

// --- tests ---

func TestAdminUsageDefaultParams(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/usage", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp UsageReport
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Groups) != 2 {
		t.Errorf("expected 2 groups, got %d", len(resp.Groups))
	}
	if resp.Totals.TokensInput != 1200 {
		t.Errorf("expected tokens_input 1200, got %d", resp.Totals.TokensInput)
	}
	if resp.Totals.TokensOutput != 600 {
		t.Errorf("expected tokens_output 600, got %d", resp.Totals.TokensOutput)
	}
	if resp.Totals.CallCount != 15 {
		t.Errorf("expected call_count 15, got %d", resp.Totals.CallCount)
	}

	// Default group_by should be "operation".
	if store.lastFilter.GroupBy != "operation" {
		t.Errorf("expected group_by operation, got %q", store.lastFilter.GroupBy)
	}
	if store.lastFilter.OrgID != nil {
		t.Error("expected OrgID to be nil")
	}
	if store.lastFilter.UserID != nil {
		t.Error("expected UserID to be nil")
	}
	if store.lastFilter.ProjectID != nil {
		t.Error("expected ProjectID to be nil")
	}
	if store.lastFilter.From != nil {
		t.Error("expected From to be nil")
	}
	if store.lastFilter.To != nil {
		t.Error("expected To to be nil")
	}
}

func TestAdminUsageAllFilters(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	orgID := uuid.New()
	userID := uuid.New()
	projectID := uuid.New()
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	url := "/v1/admin/usage?org=" + orgID.String() +
		"&user=" + userID.String() +
		"&project=" + projectID.String() +
		"&from=" + from.Format(time.RFC3339) +
		"&to=" + to.Format(time.RFC3339) +
		"&group_by=user"

	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if store.lastFilter.OrgID == nil || *store.lastFilter.OrgID != orgID {
		t.Errorf("expected OrgID %s, got %v", orgID, store.lastFilter.OrgID)
	}
	if store.lastFilter.UserID == nil || *store.lastFilter.UserID != userID {
		t.Errorf("expected UserID %s, got %v", userID, store.lastFilter.UserID)
	}
	if store.lastFilter.ProjectID == nil || *store.lastFilter.ProjectID != projectID {
		t.Errorf("expected ProjectID %s, got %v", projectID, store.lastFilter.ProjectID)
	}
	if store.lastFilter.From == nil || !store.lastFilter.From.Equal(from) {
		t.Errorf("expected From %v, got %v", from, store.lastFilter.From)
	}
	if store.lastFilter.To == nil || !store.lastFilter.To.Equal(to) {
		t.Errorf("expected To %v, got %v", to, store.lastFilter.To)
	}
	if store.lastFilter.GroupBy != "user" {
		t.Errorf("expected group_by user, got %q", store.lastFilter.GroupBy)
	}
}

func TestAdminUsageGroupByModel(t *testing.T) {
	store := &mockUsageStore{
		report: &UsageReport{
			Groups: []UsageGroup{
				{Key: "gpt-4.1-nano", TokensInput: 800, TokensOutput: 400, CallCount: 8},
			},
			Totals: UsageTotals{TokensInput: 800, TokensOutput: 400, CallCount: 8},
		},
	}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/usage?group_by=model", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp UsageReport
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(resp.Groups))
	}
	if resp.Groups[0].Key != "gpt-4.1-nano" {
		t.Errorf("expected group key gpt-4.1-nano, got %q", resp.Groups[0].Key)
	}
	if store.lastFilter.GroupBy != "model" {
		t.Errorf("expected group_by model, got %q", store.lastFilter.GroupBy)
	}
}

func TestAdminUsageInvalidGroupBy(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/usage?group_by=invalid", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error envelope")
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}

func TestAdminUsageStoreError(t *testing.T) {
	store := &mockUsageStore{err: errors.New("database timeout")}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/usage", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error envelope")
	}
	if resp.Error.Code != "internal_error" {
		t.Errorf("expected code internal_error, got %q", resp.Error.Code)
	}
}

func TestAdminUsageWrongMethod(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/usage", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error envelope")
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}

func TestAdminUsageFromToDates(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	from := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 2, 28, 23, 59, 59, 0, time.UTC)

	url := "/v1/admin/usage?from=" + from.Format(time.RFC3339) + "&to=" + to.Format(time.RFC3339)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if store.lastFilter.From == nil || !store.lastFilter.From.Equal(from) {
		t.Errorf("expected From %v, got %v", from, store.lastFilter.From)
	}
	if store.lastFilter.To == nil || !store.lastFilter.To.Equal(to) {
		t.Errorf("expected To %v, got %v", to, store.lastFilter.To)
	}
	// No UUID filters should be set.
	if store.lastFilter.OrgID != nil {
		t.Error("expected OrgID to be nil")
	}
	if store.lastFilter.UserID != nil {
		t.Error("expected UserID to be nil")
	}
	if store.lastFilter.ProjectID != nil {
		t.Error("expected ProjectID to be nil")
	}
	// Default group_by.
	if store.lastFilter.GroupBy != "operation" {
		t.Errorf("expected group_by operation, got %q", store.lastFilter.GroupBy)
	}
}

func TestAdminUsageInvalidUUIDIgnored(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewAdminUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/usage?org=not-a-uuid", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Invalid UUID should be silently ignored — OrgID remains nil.
	if store.lastFilter.OrgID != nil {
		t.Errorf("expected OrgID to be nil for invalid UUID, got %v", store.lastFilter.OrgID)
	}
}
