package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeVMStore struct {
	dryResult    *VectorMigrationResult
	dryErr       error
	startErr     error
	dryCalled    bool
	startCalled  bool
	gotDirection string
	gotBatch     int
}

func (f *fakeVMStore) DryRun(_ context.Context, direction string, batchSize int) (*VectorMigrationResult, error) {
	f.dryCalled = true
	f.gotDirection = direction
	f.gotBatch = batchSize
	return f.dryResult, f.dryErr
}

func (f *fakeVMStore) Start(_ context.Context, direction string, batchSize int) error {
	f.startCalled = true
	f.gotDirection = direction
	f.gotBatch = batchSize
	return f.startErr
}

func callVM(t *testing.T, cfg VectorMigrationAdminConfig, method string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		req = httptest.NewRequest(method, "/v1/admin/vector-migration", bytes.NewReader(b))
	} else {
		req = httptest.NewRequest(method, "/v1/admin/vector-migration", nil)
	}
	w := httptest.NewRecorder()
	NewAdminVectorMigrationHandler(cfg)(w, req)
	return w
}

func TestVectorMigrationHandler_MethodNotPost(t *testing.T) {
	store := &fakeVMStore{dryResult: &VectorMigrationResult{}}
	w := callVM(t, VectorMigrationAdminConfig{Store: store}, http.MethodGet, nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("GET: status = %d, want 400", w.Code)
	}
	if store.dryCalled || store.startCalled {
		t.Error("store should not be called for a non-POST request")
	}
}

func TestVectorMigrationHandler_NilStore(t *testing.T) {
	w := callVM(t, VectorMigrationAdminConfig{Store: nil}, http.MethodPost, map[string]any{"direction": "to_qdrant"})
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("nil store: status = %d, want 503", w.Code)
	}
}

func TestVectorMigrationHandler_InvalidDirection(t *testing.T) {
	store := &fakeVMStore{dryResult: &VectorMigrationResult{}}
	w := callVM(t, VectorMigrationAdminConfig{Store: store}, http.MethodPost, map[string]any{"direction": "sideways"})
	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid direction: status = %d, want 400", w.Code)
	}
	if store.dryCalled || store.startCalled {
		t.Error("store should not be called for an invalid direction")
	}
}

func TestVectorMigrationHandler_DryRunQdrantNotConfigured(t *testing.T) {
	store := &fakeVMStore{dryErr: ErrVectorMigrationQdrantNotConfigured}
	w := callVM(t, VectorMigrationAdminConfig{Store: store}, http.MethodPost, map[string]any{"direction": "to_qdrant", "dry_run": true})
	if w.Code != http.StatusBadRequest {
		t.Errorf("qdrant not configured: status = %d, want 400", w.Code)
	}
}

func TestVectorMigrationHandler_DryRunSuccess(t *testing.T) {
	store := &fakeVMStore{dryResult: &VectorMigrationResult{
		Direction:   VectorMigrationToQdrant,
		DryRun:      true,
		MemoryCount: 7,
		EntityCount: 3,
	}}
	w := callVM(t, VectorMigrationAdminConfig{Store: store}, http.MethodPost, map[string]any{
		"direction": "to_qdrant", "dry_run": true, "batch_size": 250,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if !store.dryCalled || store.gotDirection != "to_qdrant" || store.gotBatch != 250 {
		t.Errorf("store args: dryCalled=%v direction=%q batch=%d", store.dryCalled, store.gotDirection, store.gotBatch)
	}
	if store.startCalled {
		t.Error("Start should not be called for a dry run")
	}
	var got VectorMigrationResult
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.MemoryCount != 7 || got.EntityCount != 3 || !got.DryRun {
		t.Errorf("response = %+v, want memory=7 entity=3 dryRun=true", got)
	}
}

func TestVectorMigrationHandler_StartAccepted(t *testing.T) {
	store := &fakeVMStore{}
	w := callVM(t, VectorMigrationAdminConfig{Store: store}, http.MethodPost, map[string]any{
		"direction": "from_qdrant", "dry_run": false, "batch_size": 500,
	})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	if !store.startCalled || store.gotDirection != "from_qdrant" || store.gotBatch != 500 {
		t.Errorf("store args: startCalled=%v direction=%q batch=%d", store.startCalled, store.gotDirection, store.gotBatch)
	}
	if store.dryCalled {
		t.Error("DryRun should not be called for a real migration")
	}
}

func TestVectorMigrationHandler_StartInProgress(t *testing.T) {
	store := &fakeVMStore{startErr: ErrMigrationInProgress}
	w := callVM(t, VectorMigrationAdminConfig{Store: store}, http.MethodPost, map[string]any{"direction": "to_qdrant"})
	if w.Code != http.StatusConflict {
		t.Errorf("already running: status = %d, want 409", w.Code)
	}
}

func TestVectorMigrationHandler_StartQdrantNotConfigured(t *testing.T) {
	store := &fakeVMStore{startErr: ErrVectorMigrationQdrantNotConfigured}
	w := callVM(t, VectorMigrationAdminConfig{Store: store}, http.MethodPost, map[string]any{"direction": "to_qdrant"})
	if w.Code != http.StatusBadRequest {
		t.Errorf("start not configured: status = %d, want 400", w.Code)
	}
}
