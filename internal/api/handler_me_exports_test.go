package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// --- stub service ---

type stubMeExportSvc struct {
	enqueueErr     error
	enqueued       *model.ExportJob
	getResult      *model.ExportJob
	getErr         error
	listResult     []model.ExportJob
	listErr        error
	deleteErr      error
	openReader     io.ReadCloser
	openJob        *model.ExportJob
	openErr        error
	lastEnqueueReq service.EnqueueRequest
}

func (s *stubMeExportSvc) Enqueue(_ context.Context, req service.EnqueueRequest) (*model.ExportJob, error) {
	s.lastEnqueueReq = req
	if s.enqueueErr != nil {
		return nil, s.enqueueErr
	}
	return s.enqueued, nil
}
func (s *stubMeExportSvc) GetForUser(_ context.Context, _, _ uuid.UUID) (*model.ExportJob, error) {
	return s.getResult, s.getErr
}
func (s *stubMeExportSvc) ListForUser(_ context.Context, _ uuid.UUID, _, _ int) ([]model.ExportJob, error) {
	return s.listResult, s.listErr
}
func (s *stubMeExportSvc) DeleteForUser(_ context.Context, _, _ uuid.UUID) error {
	return s.deleteErr
}
func (s *stubMeExportSvc) OpenArtifact(_ context.Context, _, _ uuid.UUID) (io.ReadCloser, *model.ExportJob, error) {
	return s.openReader, s.openJob, s.openErr
}

func authedRequest(t *testing.T, method, target string, body io.Reader) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	ac := &auth.AuthContext{UserID: uuid.New(), Role: auth.RoleMember}
	return req.WithContext(auth.WithContext(req.Context(), ac))
}

// --- list ---

func TestMeExports_List_Unauthorized(t *testing.T) {
	h := NewMeExportHandlers(&stubMeExportSvc{}).List
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest("GET", "/v1/me/exports", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestMeExports_List_OK(t *testing.T) {
	jobs := []model.ExportJob{{ID: uuid.New(), Scope: model.ExportScopeAccount, Status: model.ExportStatusPending}}
	svc := &stubMeExportSvc{listResult: jobs}
	h := NewMeExportHandlers(svc).List

	w := httptest.NewRecorder()
	h(w, authedRequest(t, "GET", "/v1/me/exports", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", w.Code, w.Body.String())
	}
	var resp struct {
		Data []model.ExportJob `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 job, got %d", len(resp.Data))
	}
}

// --- create ---

func TestMeExports_Create_Account_OK(t *testing.T) {
	enq := &model.ExportJob{ID: uuid.New(), Scope: model.ExportScopeAccount, Status: model.ExportStatusPending}
	svc := &stubMeExportSvc{enqueued: enq}
	h := NewMeExportHandlers(svc).List

	body := bytes.NewBufferString(`{"scope":"account"}`)
	w := httptest.NewRecorder()
	h(w, authedRequest(t, "POST", "/v1/me/exports", body))
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", w.Code, w.Body.String())
	}
	if svc.lastEnqueueReq.Scope != model.ExportScopeAccount {
		t.Fatalf("expected scope account, got %q", svc.lastEnqueueReq.Scope)
	}
}

func TestMeExports_Create_BadJSON(t *testing.T) {
	h := NewMeExportHandlers(&stubMeExportSvc{}).List
	w := httptest.NewRecorder()
	h(w, authedRequest(t, "POST", "/v1/me/exports", bytes.NewBufferString(`{nope`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestMeExports_Create_RateLimited(t *testing.T) {
	svc := &stubMeExportSvc{enqueueErr: service.ErrExportJobRateLimited}
	h := NewMeExportHandlers(svc).List
	w := httptest.NewRecorder()
	h(w, authedRequest(t, "POST", "/v1/me/exports", bytes.NewBufferString(`{"scope":"account"}`)))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", w.Code)
	}
}

func TestMeExports_Create_AlreadyRunning(t *testing.T) {
	svc := &stubMeExportSvc{enqueueErr: service.ErrExportJobAlreadyRunning}
	h := NewMeExportHandlers(svc).List
	w := httptest.NewRecorder()
	h(w, authedRequest(t, "POST", "/v1/me/exports", bytes.NewBufferString(`{"scope":"account"}`)))
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
}

func TestMeExports_Create_BadScope(t *testing.T) {
	svc := &stubMeExportSvc{enqueueErr: service.ErrExportJobBadRequest}
	h := NewMeExportHandlers(svc).List
	w := httptest.NewRecorder()
	h(w, authedRequest(t, "POST", "/v1/me/exports", bytes.NewBufferString(`{"scope":"bogus"}`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- item GET / DELETE ---

func TestMeExportItem_Get_OtherUserIsNotFound(t *testing.T) {
	svc := &stubMeExportSvc{getErr: sql.ErrNoRows}
	h := NewMeExportHandlers(svc).Item

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("job_id", uuid.New().String())
	r := authedRequest(t, "GET", "/v1/me/exports/x", nil).WithContext(context.WithValue(context.Background(), chi.RouteCtxKey, rctx))
	// re-attach auth after the chi context swap.
	ac := &auth.AuthContext{UserID: uuid.New(), Role: auth.RoleMember}
	r = r.WithContext(auth.WithContext(r.Context(), ac))

	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d (%s)", w.Code, w.Body.String())
	}
}

func TestMeExportItem_Get_OK(t *testing.T) {
	job := &model.ExportJob{ID: uuid.New(), Scope: model.ExportScopeAccount, Status: model.ExportStatusSucceeded}
	svc := &stubMeExportSvc{getResult: job}
	h := NewMeExportHandlers(svc).Item

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("job_id", job.ID.String())
	r := authedRequest(t, "GET", "/v1/me/exports/x", nil)
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", w.Code, w.Body.String())
	}
}

func TestMeExportItem_Delete_OK(t *testing.T) {
	svc := &stubMeExportSvc{}
	h := NewMeExportHandlers(svc).Item
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("job_id", uuid.New().String())
	r := authedRequest(t, "DELETE", "/v1/me/exports/x", nil)
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
}

// --- download ---

func TestMeExportDownload_NotReady(t *testing.T) {
	job := &model.ExportJob{ID: uuid.New(), Status: model.ExportStatusPending}
	svc := &stubMeExportSvc{openErr: sql.ErrNoRows, openJob: job}
	h := NewMeExportHandlers(svc).Download

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("job_id", job.ID.String())
	r := authedRequest(t, "GET", "/v1/me/exports/x/download", nil)
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409 (not ready), got %d (%s)", w.Code, w.Body.String())
	}
}

func TestMeExportDownload_Missing(t *testing.T) {
	svc := &stubMeExportSvc{openErr: sql.ErrNoRows} // no job at all
	h := NewMeExportHandlers(svc).Download

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("job_id", uuid.New().String())
	r := authedRequest(t, "GET", "/v1/me/exports/x/download", nil)
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestMeExportDownload_Streams(t *testing.T) {
	body := []byte("PK\x03\x04 fake-zip")
	bytesLen := int64(len(body))
	job := &model.ExportJob{
		ID:            uuid.New(),
		Status:        model.ExportStatusSucceeded,
		ArtifactBytes: &bytesLen,
	}
	svc := &stubMeExportSvc{
		openReader: io.NopCloser(bytes.NewReader(body)),
		openJob:    job,
	}
	h := NewMeExportHandlers(svc).Download

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("job_id", job.ID.String())
	r := authedRequest(t, "GET", "/v1/me/exports/x/download", nil)
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/zip" {
		t.Errorf("expected Content-Type=application/zip, got %q", got)
	}
	if got := w.Header().Get("Content-Disposition"); !strings.Contains(got, "attachment;") || !strings.Contains(got, ".zip") {
		t.Errorf("expected attachment+.zip in Content-Disposition, got %q", got)
	}
	if !bytes.Equal(w.Body.Bytes(), body) {
		t.Errorf("expected body byte-equal; got %d bytes", w.Body.Len())
	}
}

// Compile-time check on the interface shape.
var _ MeExportService = (*stubMeExportSvc)(nil)

// Sentinel use of errors.Is so the import isn't dropped if a future
// refactor inlines all the error checks.
var _ = errors.Is