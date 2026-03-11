package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// --- mock DatabaseAdminStore ---

type mockDatabaseAdminStore struct {
	info      *DatabaseInfo
	infoErr   error
	testRes   *ConnectionTestResult
	testErr   error
	migrateRes *MigrationStatus
	migrateErr error

	// capture args
	testURL    string
	migrateURL string
}

func (m *mockDatabaseAdminStore) GetDatabaseInfo(_ context.Context) (*DatabaseInfo, error) {
	return m.info, m.infoErr
}

func (m *mockDatabaseAdminStore) TestConnection(_ context.Context, url string) (*ConnectionTestResult, error) {
	m.testURL = url
	return m.testRes, m.testErr
}

func (m *mockDatabaseAdminStore) TriggerMigration(_ context.Context, url string) (*MigrationStatus, error) {
	m.migrateURL = url
	return m.migrateRes, m.migrateErr
}

// --- tests ---

func TestAdminDatabaseGetInfoSQLite(t *testing.T) {
	store := &mockDatabaseAdminStore{
		info: &DatabaseInfo{
			Backend: "sqlite",
			Version: "3.45.0",
			SQLite: &SQLiteInfo{
				FilePath: "/data/nram.db",
				FileSize: 1048576,
			},
			DataCounts: DataCounts{
				Memories:      100,
				Entities:      50,
				Projects:      3,
				Users:         2,
				Organizations: 1,
			},
		},
	}

	h := NewAdminDatabaseHandler(DatabaseAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/database", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp DatabaseInfo
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Backend != "sqlite" {
		t.Errorf("expected backend sqlite, got %q", resp.Backend)
	}
	if resp.Version != "3.45.0" {
		t.Errorf("expected version 3.45.0, got %q", resp.Version)
	}
	if resp.SQLite == nil {
		t.Fatal("expected sqlite info to be non-nil")
	}
	if resp.SQLite.FilePath != "/data/nram.db" {
		t.Errorf("expected file_path /data/nram.db, got %q", resp.SQLite.FilePath)
	}
	if resp.SQLite.FileSize != 1048576 {
		t.Errorf("expected file_size 1048576, got %d", resp.SQLite.FileSize)
	}
	if resp.Postgres != nil {
		t.Error("expected postgres info to be nil for sqlite backend")
	}
	if resp.DataCounts.Memories != 100 {
		t.Errorf("expected 100 memories, got %d", resp.DataCounts.Memories)
	}
	if resp.DataCounts.Entities != 50 {
		t.Errorf("expected 50 entities, got %d", resp.DataCounts.Entities)
	}
	if resp.DataCounts.Projects != 3 {
		t.Errorf("expected 3 projects, got %d", resp.DataCounts.Projects)
	}
	if resp.DataCounts.Users != 2 {
		t.Errorf("expected 2 users, got %d", resp.DataCounts.Users)
	}
	if resp.DataCounts.Organizations != 1 {
		t.Errorf("expected 1 organization, got %d", resp.DataCounts.Organizations)
	}
}

func TestAdminDatabaseGetInfoPostgres(t *testing.T) {
	store := &mockDatabaseAdminStore{
		info: &DatabaseInfo{
			Backend: "postgres",
			Version: "16.2",
			Postgres: &PostgresInfo{
				Host:            "localhost",
				Database:        "nram",
				PgvectorVersion: "0.7.0",
				ActiveConns:     5,
				IdleConns:       3,
				MaxConns:        20,
			},
			DataCounts: DataCounts{
				Memories:      500,
				Entities:      200,
				Projects:      10,
				Users:         5,
				Organizations: 2,
			},
		},
	}

	h := NewAdminDatabaseHandler(DatabaseAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/database", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp DatabaseInfo
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Backend != "postgres" {
		t.Errorf("expected backend postgres, got %q", resp.Backend)
	}
	if resp.Version != "16.2" {
		t.Errorf("expected version 16.2, got %q", resp.Version)
	}
	if resp.SQLite != nil {
		t.Error("expected sqlite info to be nil for postgres backend")
	}
	if resp.Postgres == nil {
		t.Fatal("expected postgres info to be non-nil")
	}
	if resp.Postgres.Host != "localhost" {
		t.Errorf("expected host localhost, got %q", resp.Postgres.Host)
	}
	if resp.Postgres.Database != "nram" {
		t.Errorf("expected database nram, got %q", resp.Postgres.Database)
	}
	if resp.Postgres.PgvectorVersion != "0.7.0" {
		t.Errorf("expected pgvector 0.7.0, got %q", resp.Postgres.PgvectorVersion)
	}
	if resp.Postgres.ActiveConns != 5 {
		t.Errorf("expected 5 active conns, got %d", resp.Postgres.ActiveConns)
	}
	if resp.Postgres.IdleConns != 3 {
		t.Errorf("expected 3 idle conns, got %d", resp.Postgres.IdleConns)
	}
	if resp.Postgres.MaxConns != 20 {
		t.Errorf("expected 20 max conns, got %d", resp.Postgres.MaxConns)
	}
	if resp.DataCounts.Memories != 500 {
		t.Errorf("expected 500 memories, got %d", resp.DataCounts.Memories)
	}
}

func TestAdminDatabaseGetInfoStoreError(t *testing.T) {
	store := &mockDatabaseAdminStore{
		infoErr: errors.New("database failure"),
	}

	h := NewAdminDatabaseHandler(DatabaseAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/database", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "internal_error" {
		t.Errorf("expected code internal_error, got %q", resp.Error.Code)
	}
}

func TestAdminDatabaseTestConnectionSuccess(t *testing.T) {
	store := &mockDatabaseAdminStore{
		testRes: &ConnectionTestResult{
			Success:           true,
			Message:           "connection successful",
			PgvectorInstalled: true,
			LatencyMs:         12,
		},
	}

	h := NewAdminDatabaseHandler(DatabaseAdminConfig{Store: store})
	body := `{"url":"postgres://user:pass@localhost:5432/nram"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/database/test", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp ConnectionTestResult
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !resp.Success {
		t.Error("expected success true")
	}
	if resp.Message != "connection successful" {
		t.Errorf("expected message 'connection successful', got %q", resp.Message)
	}
	if !resp.PgvectorInstalled {
		t.Error("expected pgvector_installed true")
	}
	if resp.LatencyMs != 12 {
		t.Errorf("expected latency 12, got %d", resp.LatencyMs)
	}
	if store.testURL != "postgres://user:pass@localhost:5432/nram" {
		t.Errorf("expected URL postgres://user:pass@localhost:5432/nram, got %q", store.testURL)
	}
}

func TestAdminDatabaseTestConnectionMissingURL(t *testing.T) {
	store := &mockDatabaseAdminStore{}

	h := NewAdminDatabaseHandler(DatabaseAdminConfig{Store: store})
	body := `{"url":""}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/database/test", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}

func TestAdminDatabaseTestConnectionStoreError(t *testing.T) {
	store := &mockDatabaseAdminStore{
		testErr: errors.New("connection refused"),
	}

	h := NewAdminDatabaseHandler(DatabaseAdminConfig{Store: store})
	body := `{"url":"postgres://user:pass@localhost:5432/nram"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/database/test", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "internal_error" {
		t.Errorf("expected code internal_error, got %q", resp.Error.Code)
	}
}

func TestAdminDatabaseTriggerMigrationSuccess(t *testing.T) {
	store := &mockDatabaseAdminStore{
		migrateRes: &MigrationStatus{
			Status:  "started",
			Message: "migration initiated",
		},
	}

	h := NewAdminDatabaseHandler(DatabaseAdminConfig{Store: store})
	body := `{"url":"postgres://user:pass@localhost:5432/nram"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/database/migrate", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp MigrationStatus
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Status != "started" {
		t.Errorf("expected status started, got %q", resp.Status)
	}
	if resp.Message != "migration initiated" {
		t.Errorf("expected message 'migration initiated', got %q", resp.Message)
	}
	if store.migrateURL != "postgres://user:pass@localhost:5432/nram" {
		t.Errorf("expected URL postgres://user:pass@localhost:5432/nram, got %q", store.migrateURL)
	}
}

func TestAdminDatabaseTriggerMigrationMissingURL(t *testing.T) {
	store := &mockDatabaseAdminStore{}

	h := NewAdminDatabaseHandler(DatabaseAdminConfig{Store: store})
	body := `{"url":"  "}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/database/migrate", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}

func TestAdminDatabaseTriggerMigrationStoreError(t *testing.T) {
	store := &mockDatabaseAdminStore{
		migrateErr: errors.New("migration failed: table already exists"),
	}

	h := NewAdminDatabaseHandler(DatabaseAdminConfig{Store: store})
	body := `{"url":"postgres://user:pass@localhost:5432/nram"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/database/migrate", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "internal_error" {
		t.Errorf("expected code internal_error, got %q", resp.Error.Code)
	}
}

func TestAdminDatabaseUnknownSubPath(t *testing.T) {
	store := &mockDatabaseAdminStore{}

	h := NewAdminDatabaseHandler(DatabaseAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/database/unknown", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}

func TestAdminDatabaseWrongMethodOnRoot(t *testing.T) {
	store := &mockDatabaseAdminStore{}

	h := NewAdminDatabaseHandler(DatabaseAdminConfig{Store: store})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/database", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	var resp errorEnvelope
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %q", resp.Error.Code)
	}
}
