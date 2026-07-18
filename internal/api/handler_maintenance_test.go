package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nram-ai/nram/internal/maintenance"
)

func getMaintenanceStatus(t *testing.T, h http.HandlerFunc) maintenanceStatusResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/v1/maintenance/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp maintenanceStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp
}

func TestMaintenanceStatusHandler(t *testing.T) {
	reg := maintenance.NewRegistry(nil)
	h := NewMaintenanceStatusHandler(reg)

	if resp := getMaintenanceStatus(t, h); resp.Active || len(resp.Operations) != 0 {
		t.Fatalf("inactive: active=%v ops=%d", resp.Active, len(resp.Operations))
	}

	end := reg.Begin("op", "Label", "hello")
	resp := getMaintenanceStatus(t, h)
	if !resp.Active || len(resp.Operations) != 1 {
		t.Fatalf("active: active=%v ops=%d", resp.Active, len(resp.Operations))
	}
	if resp.Operations[0].Label != "Label" || resp.Operations[0].Message != "hello" {
		t.Fatalf("op payload = %+v", resp.Operations[0])
	}

	end()
	if resp := getMaintenanceStatus(t, h); resp.Active {
		t.Fatal("expected inactive after op ended")
	}
}

func TestMaintenanceStatusHandlerNilRegistry(t *testing.T) {
	h := NewMaintenanceStatusHandler(nil)
	if resp := getMaintenanceStatus(t, h); resp.Active || len(resp.Operations) != 0 {
		t.Fatalf("nil registry should report inactive, got active=%v ops=%d", resp.Active, len(resp.Operations))
	}
}
