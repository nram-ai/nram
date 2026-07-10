package api

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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

// ListKeyset mirrors the real repo: it returns up to limit rows ordered
// ts DESC, id DESC, strictly after cursor. It uses string id comparison to
// match the SQLite text ordering the real store falls back to.
func (f *fakeLogStore) ListKeyset(_ context.Context, flt storage.LogFilter, cursor *storage.LogCursor, limit int) ([]model.LogEntry, error) {
	f.lastFilter = flt
	sorted := make([]model.LogEntry, len(f.entries))
	copy(sorted, f.entries)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].Timestamp.Equal(sorted[j].Timestamp) {
			return sorted[i].Timestamp.After(sorted[j].Timestamp)
		}
		return sorted[i].ID.String() > sorted[j].ID.String()
	})
	start := 0
	if cursor != nil {
		for start < len(sorted) {
			e := sorted[start]
			atOrBefore := e.Timestamp.After(cursor.TS) ||
				(e.Timestamp.Equal(cursor.TS) && e.ID.String() >= cursor.ID.String())
			if !atOrBefore {
				break
			}
			start++
		}
	}
	end := min(start+limit, len(sorted))
	return sorted[start:end], nil
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

// TestAdminLogs_ExportStreamsAllRows seeds more rows than a single export page
// and asserts both formats return every row (no 50k-style cap, no truncation
// header) by paging through the keyset cursor.
func TestAdminLogs_ExportStreamsAllRows(t *testing.T) {
	const n = 3*logsExportPageSize + 137 // spans several pages plus a partial tail
	entries := make([]model.LogEntry, n)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	for i := range entries {
		entries[i] = model.LogEntry{
			ID:        uuid.New(),
			Timestamp: base.Add(time.Duration(i) * time.Millisecond),
			Level:     model.LogLevelInfo,
			Component: "test",
			Message:   fmt.Sprintf("row %d", i),
			Attrs:     json.RawMessage(`{}`),
		}
	}
	store := &fakeLogStore{entries: entries}
	h := NewAdminLogsHandler(LogAdminConfig{Store: store})

	// CSV: every row plus the header, and no truncation signal.
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/logs/export?format=csv", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("csv: expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("X-Truncated"); got != "" {
		t.Fatalf("csv: unexpected X-Truncated header %q", got)
	}
	records, err := csv.NewReader(strings.NewReader(rec.Body.String())).ReadAll()
	if err != nil {
		t.Fatalf("csv: parse: %v", err)
	}
	if len(records) != n+1 {
		t.Fatalf("csv: want %d data rows + header, got %d records", n, len(records))
	}

	// JSON: valid array framing across pages, every row present, ts DESC order.
	req = httptest.NewRequest(http.MethodGet, "/v1/admin/logs/export?format=json", nil)
	rec = httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("json: expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("X-Truncated"); got != "" {
		t.Fatalf("json: unexpected X-Truncated header %q", got)
	}
	var got []model.LogEntry
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("json: decode: %v", err)
	}
	if len(got) != n {
		t.Fatalf("json: want %d rows, got %d", n, len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp.After(got[i-1].Timestamp) {
			t.Fatalf("json: rows not ts DESC at index %d", i)
		}
	}
}
