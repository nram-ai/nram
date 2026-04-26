package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/service"
)

// --- mock export service ---

type mockExportService struct {
	exportFn     func(ctx context.Context, req *service.ExportRequest) (*service.ExportData, error)
	exportNDJSON func(ctx context.Context, req *service.ExportRequest, w io.Writer) error
}

func (m *mockExportService) Export(ctx context.Context, req *service.ExportRequest) (*service.ExportData, error) {
	if m.exportFn != nil {
		return m.exportFn(ctx, req)
	}
	return &service.ExportData{
		Version:       "1.0",
		ExportedAt:    time.Now(),
		Project:       service.ExportProject{ID: req.ProjectID, Name: "test", Slug: "test"},
		Memories:      []service.ExportMemory{},
		Entities:      []service.ExportEntity{},
		Relationships: []service.ExportRelationship{},
		Stats:         service.ExportStats{},
	}, nil
}

func (m *mockExportService) ExportNDJSON(ctx context.Context, req *service.ExportRequest, w io.Writer) error {
	if m.exportNDJSON != nil {
		return m.exportNDJSON(ctx, req, w)
	}
	rec := map[string]interface{}{
		"type": "project",
		"data": map[string]interface{}{"id": req.ProjectID.String(), "name": "test", "slug": "test"},
	}
	return json.NewEncoder(w).Encode(rec)
}

// --- mock import service ---

type mockImportService struct {
	importFn func(ctx context.Context, req *service.ImportRequest) (*service.ImportResponse, error)
}

func (m *mockImportService) Import(ctx context.Context, req *service.ImportRequest) (*service.ImportResponse, error) {
	if m.importFn != nil {
		return m.importFn(ctx, req)
	}
	return &service.ImportResponse{
		Imported:  5,
		Skipped:   0,
		Errors:    []service.ImportError{},
		LatencyMs: 42,
	}, nil
}

// --- helpers ---

func newExportRouter(handler http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Get("/v1/projects/{project_id}/memories/export", handler)
	return r
}

func newImportRouter(handler http.HandlerFunc) *chi.Mux {
	r := chi.NewRouter()
	r.Post("/v1/projects/{project_id}/memories/import", handler)
	return r
}

// --- export tests ---

func TestExportHandler_JSONSuccess(t *testing.T) {
	svc := &mockExportService{}
	router := newExportRouter(NewExportHandler(svc))

	projectID := uuid.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/"+projectID.String()+"/memories/export", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	ct := w.Header().Get("Content-Type")
	if ct != "application/json; charset=utf-8" {
		t.Errorf("expected application/json content-type, got %q", ct)
	}

	var data service.ExportData
	if err := json.NewDecoder(w.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if data.Version != "1.0" {
		t.Errorf("expected version 1.0, got %s", data.Version)
	}
	if data.Project.ID != projectID {
		t.Errorf("expected project ID %s, got %s", projectID, data.Project.ID)
	}
}

func TestExportHandler_NDJSONQueryParam(t *testing.T) {
	svc := &mockExportService{}
	router := newExportRouter(NewExportHandler(svc))

	projectID := uuid.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/"+projectID.String()+"/memories/export?format=ndjson", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	ct := w.Header().Get("Content-Type")
	if ct != "application/x-ndjson" {
		t.Errorf("expected application/x-ndjson content-type, got %q", ct)
	}
}

func TestExportHandler_NDJSONAcceptHeader(t *testing.T) {
	svc := &mockExportService{}
	router := newExportRouter(NewExportHandler(svc))

	projectID := uuid.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/"+projectID.String()+"/memories/export", nil)
	req.Header.Set("Accept", "application/x-ndjson")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	ct := w.Header().Get("Content-Type")
	if ct != "application/x-ndjson" {
		t.Errorf("expected application/x-ndjson content-type, got %q", ct)
	}
}

func TestExportHandler_InvalidProjectID(t *testing.T) {
	svc := &mockExportService{}
	router := newExportRouter(NewExportHandler(svc))

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/not-a-uuid/memories/export", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "bad_request" {
		t.Errorf("expected bad_request error code, got %+v", envelope.Error)
	}
}

func TestExportHandler_InvalidFormat(t *testing.T) {
	svc := &mockExportService{}
	router := newExportRouter(NewExportHandler(svc))

	projectID := uuid.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/"+projectID.String()+"/memories/export?format=csv", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestExportHandler_ServiceError(t *testing.T) {
	svc := &mockExportService{
		exportFn: func(ctx context.Context, req *service.ExportRequest) (*service.ExportData, error) {
			return nil, fmt.Errorf("project not found: record does not exist")
		},
	}
	router := newExportRouter(NewExportHandler(svc))

	projectID := uuid.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/"+projectID.String()+"/memories/export", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d: %s", w.Code, w.Body.String())
	}
}

// --- import tests ---

func TestImportHandler_NRAMSuccess(t *testing.T) {
	svc := &mockImportService{}
	router := newImportRouter(NewImportHandler(svc))

	projectID := uuid.New()
	body := bytes.NewBufferString(`{"version":"1.0","memories":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID.String()+"/memories/import", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	ct := w.Header().Get("Content-Type")
	if ct != "application/json; charset=utf-8" {
		t.Errorf("expected application/json content-type, got %q", ct)
	}

	var resp service.ImportResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Imported != 5 {
		t.Errorf("expected 5 imported, got %d", resp.Imported)
	}
}

func TestImportHandler_Mem0Success(t *testing.T) {
	var receivedFormat service.ImportFormat
	svc := &mockImportService{
		importFn: func(ctx context.Context, req *service.ImportRequest) (*service.ImportResponse, error) {
			receivedFormat = req.Format
			return &service.ImportResponse{
				Imported:  3,
				Skipped:   0,
				Errors:    []service.ImportError{},
				LatencyMs: 10,
			}, nil
		},
	}
	router := newImportRouter(NewImportHandler(svc))

	projectID := uuid.New()
	body := bytes.NewBufferString(`{"results":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID.String()+"/memories/import?format=mem0", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}
	if receivedFormat != service.ImportFormatMem0 {
		t.Errorf("expected format mem0, got %s", receivedFormat)
	}
}

func TestImportHandler_InvalidFormat(t *testing.T) {
	svc := &mockImportService{}
	router := newImportRouter(NewImportHandler(svc))

	projectID := uuid.New()
	body := bytes.NewBufferString(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID.String()+"/memories/import?format=csv", body)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "bad_request" {
		t.Errorf("expected bad_request error code, got %+v", envelope.Error)
	}
}

func TestImportHandler_InvalidProjectID(t *testing.T) {
	svc := &mockImportService{}
	router := newImportRouter(NewImportHandler(svc))

	body := bytes.NewBufferString(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/not-a-uuid/memories/import", body)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}

	var envelope errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode error: %v", err)
	}
	if envelope.Error == nil || envelope.Error.Code != "bad_request" {
		t.Errorf("expected bad_request error code, got %+v", envelope.Error)
	}
}

func TestImportHandler_ServiceError(t *testing.T) {
	svc := &mockImportService{
		importFn: func(ctx context.Context, req *service.ImportRequest) (*service.ImportResponse, error) {
			return nil, fmt.Errorf("project not found: no such project")
		},
	}
	router := newImportRouter(NewImportHandler(svc))

	projectID := uuid.New()
	body := bytes.NewBufferString(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/"+projectID.String()+"/memories/import", body)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestExportHandler_PassesIncludeSupersededFlag(t *testing.T) {
	var got *service.ExportRequest
	svc := &mockExportService{
		exportFn: func(_ context.Context, req *service.ExportRequest) (*service.ExportData, error) {
			got = req
			return &service.ExportData{
				Version: "1.0", Project: service.ExportProject{ID: req.ProjectID, Slug: "test"},
				Memories: []service.ExportMemory{}, Entities: []service.ExportEntity{}, Relationships: []service.ExportRelationship{},
			}, nil
		},
	}
	router := newExportRouter(NewExportHandler(svc))
	projectID := uuid.New()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/"+projectID.String()+"/memories/export", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("default request: %d %s", w.Code, w.Body.String())
	}
	if got == nil || got.IncludeSuperseded {
		t.Errorf("default should keep IncludeSuperseded=false; got %+v", got)
	}

	got = nil
	req = httptest.NewRequest(http.MethodGet,
		"/v1/projects/"+projectID.String()+"/memories/export?include_superseded=true", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("include request: %d %s", w.Code, w.Body.String())
	}
	if got == nil || !got.IncludeSuperseded {
		t.Errorf("include_superseded=true should set IncludeSuperseded; got %+v", got)
	}
}
