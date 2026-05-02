package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestSystemUsage_NoScopeFilter is the regression test for the system-tier
// token-usage bug: before the per-tier handler split, /v1/admin/system/usage
// reused the self-tier handler and silently filtered to the calling admin's
// own user. The handler must now query unscoped (system-wide).
func TestSystemUsage_NoScopeFilter(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewSystemUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/system/usage", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if store.lastFilter.OrgID != nil {
		t.Errorf("system-tier must not scope by OrgID, got %v", store.lastFilter.OrgID)
	}
	if store.lastFilter.UserID != nil {
		t.Errorf("system-tier must not scope by UserID, got %v", store.lastFilter.UserID)
	}
	if store.lastFilter.GroupBy != "operation" {
		t.Errorf("expected default group_by=operation, got %q", store.lastFilter.GroupBy)
	}
}

func TestSystemUsage_PassesGroupByAndDates(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewSystemUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet,
		"/v1/admin/system/usage?group_by=org&from=2026-01-01T00:00:00Z&to=2026-12-31T23:59:59Z&success_only=true",
		nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if store.lastFilter.GroupBy != "org" {
		t.Errorf("group_by: got %q want org", store.lastFilter.GroupBy)
	}
	if store.lastFilter.From == nil || store.lastFilter.To == nil {
		t.Errorf("expected from/to parsed, got from=%v to=%v", store.lastFilter.From, store.lastFilter.To)
	}
	if store.lastFilter.SuccessOnly == nil || !*store.lastFilter.SuccessOnly {
		t.Errorf("expected success_only=true, got %v", store.lastFilter.SuccessOnly)
	}

	var resp UsageReport
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Totals.TokensInput != 1200 {
		t.Errorf("totals not propagated: %+v", resp.Totals)
	}
}

func TestSystemUsage_RejectsInvalidGroupBy(t *testing.T) {
	store := &mockUsageStore{report: defaultUsageReport()}
	h := NewSystemUsageHandler(UsageConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/system/usage?group_by=password", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}
