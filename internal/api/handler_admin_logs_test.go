package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

type fakeLogStore struct {
	entries    []model.LogEntry
	lastFilter storage.LogFilter
	lastLimit  int
	lastOffset int
	components []string
}

func (f *fakeLogStore) List(_ context.Context, flt storage.LogFilter, limit, offset int) ([]model.LogEntry, error) {
	f.lastFilter = flt
	f.lastLimit = limit
	f.lastOffset = offset
	return f.entries, nil
}

func (f *fakeLogStore) Count(_ context.Context, flt storage.LogFilter) (int, error) {
	f.lastFilter = flt
	return len(f.entries), nil
}

func (f *fakeLogStore) Components(_ context.Context) ([]string, error) {
	return f.components, nil
}

func sampleLogEntries() []model.LogEntry {
	return []model.LogEntry{
		{
			Timestamp: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
			Level:     model.LogLevelError,
			Component: "enrichment",
			Message:   "extraction failed",
			Attrs:     json.RawMessage(`{"job":"j2"}`),
		},
	}
}

func TestAdminLogs_List(t *testing.T) {
	store := &fakeLogStore{entries: sampleLogEntries()}
	h := NewAdminLogsHandler(LogAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/logs?level=error,warn&component=enrichment&search=fail&limit=25", nil)
	rec := httptest.NewRecorder()
	h(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp model.PaginatedResponse[model.LogEntry]
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Pagination.Total != 1 || len(resp.Data) != 1 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	// attrs survive as a structured object.
	var attrs map[string]any
	if err := json.Unmarshal(resp.Data[0].Attrs, &attrs); err != nil || attrs["job"] != "j2" {
		t.Fatalf("attrs not structured: %v %s", err, resp.Data[0].Attrs)
	}
	// Filters parsed into the storage filter.
	if len(store.lastFilter.Levels) != 2 || store.lastFilter.Component != "enrichment" || store.lastFilter.Search != "fail" {
		t.Fatalf("filter not parsed: %+v", store.lastFilter)
	}
	if store.lastLimit != 25 {
		t.Fatalf("limit not applied: %d", store.lastLimit)
	}
}

func TestAdminLogs_ListLimitCap(t *testing.T) {
	store := &fakeLogStore{}
	h := NewAdminLogsHandler(LogAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/logs?limit=99999", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if store.lastLimit != logsMaxLimit {
		t.Fatalf("limit should be capped at %d, got %d", logsMaxLimit, store.lastLimit)
	}
}

func TestAdminLogs_ExportCSV(t *testing.T) {
	store := &fakeLogStore{entries: sampleLogEntries()}
	h := NewAdminLogsHandler(LogAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/logs/export?format=csv", nil)
	rec := httptest.NewRecorder()
	h(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/csv") {
		t.Fatalf("expected text/csv, got %q", ct)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, "attachment;") || !strings.Contains(cd, ".csv") {
		t.Fatalf("expected csv attachment, got %q", cd)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "ts,level,component,message,attrs") {
		t.Fatalf("missing CSV header: %s", body)
	}
	if !strings.Contains(body, "extraction failed") || !strings.Contains(body, `{""job"":""j2""}`) {
		t.Fatalf("CSV body missing row/attrs: %s", body)
	}
}

func TestAdminLogs_Facets(t *testing.T) {
	store := &fakeLogStore{components: []string{"dreaming", "enrichment"}}
	h := NewAdminLogsHandler(LogAdminConfig{Store: store})

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/logs/facets", nil)
	rec := httptest.NewRecorder()
	h(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp logFacetsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Levels) != 4 || len(resp.Components) != 2 {
		t.Fatalf("unexpected facets: %+v", resp)
	}
}

func TestAdminLogs_ExportBadFormat(t *testing.T) {
	store := &fakeLogStore{}
	h := NewAdminLogsHandler(LogAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/logs/export?format=xml", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad format, got %d", rec.Code)
	}
}
