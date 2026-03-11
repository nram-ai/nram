package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// DatabaseAdminStore abstracts storage operations for the database admin API.
type DatabaseAdminStore interface {
	GetDatabaseInfo(ctx context.Context) (*DatabaseInfo, error)
	TestConnection(ctx context.Context, url string) (*ConnectionTestResult, error)
	TriggerMigration(ctx context.Context, url string) (*MigrationStatus, error)
}

// DatabaseAdminConfig holds the dependencies for the database admin handler.
type DatabaseAdminConfig struct {
	Store DatabaseAdminStore
}

// DatabaseInfo describes the current database backend and its statistics.
type DatabaseInfo struct {
	Backend    string        `json:"backend"`
	Version    string        `json:"version"`
	SQLite     *SQLiteInfo   `json:"sqlite,omitempty"`
	Postgres   *PostgresInfo `json:"postgres,omitempty"`
	DataCounts DataCounts    `json:"data_counts"`
}

// SQLiteInfo holds SQLite-specific database information.
type SQLiteInfo struct {
	FilePath string `json:"file_path"`
	FileSize int64  `json:"file_size_bytes"`
}

// PostgresInfo holds PostgreSQL-specific database information.
type PostgresInfo struct {
	Host            string `json:"host"`
	Database        string `json:"database"`
	PgvectorVersion string `json:"pgvector_version,omitempty"`
	ActiveConns     int    `json:"active_connections"`
	IdleConns       int    `json:"idle_connections"`
	MaxConns        int    `json:"max_connections"`
}

// DataCounts holds row counts for the main data tables.
type DataCounts struct {
	Memories      int `json:"memories"`
	Entities      int `json:"entities"`
	Projects      int `json:"projects"`
	Users         int `json:"users"`
	Organizations int `json:"organizations"`
}

// ConnectionTestResult holds the outcome of a PostgreSQL connection test.
type ConnectionTestResult struct {
	Success           bool   `json:"success"`
	Message           string `json:"message"`
	PgvectorInstalled bool   `json:"pgvector_installed"`
	LatencyMs         int64  `json:"latency_ms"`
}

// MigrationStatus holds the outcome of a SQLite-to-Postgres migration request.
type MigrationStatus struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// databaseURLRequest is the request body for connection test and migration endpoints.
type databaseURLRequest struct {
	URL string `json:"url"`
}

// NewAdminDatabaseHandler returns an http.HandlerFunc that dispatches database
// admin requests based on method and sub-path.
//
// Routes:
//   - GET  /database         — current backend info and stats
//   - POST /database/test    — test a Postgres connection URL
//   - POST /database/migrate — trigger SQLite-to-Postgres migration
func NewAdminDatabaseHandler(cfg DatabaseAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract sub-path after "/database".
		sub := extractDatabaseSubPath(r.URL.Path)

		switch {
		case sub == "" && r.Method == http.MethodGet:
			handleGetDatabaseInfo(w, r, cfg)
		case sub == "test" && r.Method == http.MethodPost:
			handleTestConnection(w, r, cfg)
		case sub == "migrate" && r.Method == http.MethodPost:
			handleTriggerMigration(w, r, cfg)
		default:
			WriteError(w, ErrBadRequest("invalid database endpoint or method"))
		}
	}
}

// extractDatabaseSubPath returns the portion of the URL path after "/database",
// stripped of leading and trailing slashes. For example:
//
//	"/v1/admin/database"         → ""
//	"/v1/admin/database/test"    → "test"
//	"/v1/admin/database/migrate" → "migrate"
func extractDatabaseSubPath(path string) string {
	const marker = "/database"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return ""
	}
	sub := path[idx+len(marker):]
	return strings.Trim(sub, "/")
}

// handleGetDatabaseInfo handles GET /database — returns current backend info and statistics.
func handleGetDatabaseInfo(w http.ResponseWriter, r *http.Request, cfg DatabaseAdminConfig) {
	info, err := cfg.Store.GetDatabaseInfo(r.Context())
	if err != nil {
		WriteError(w, ErrInternal(err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, info)
}

// handleTestConnection handles POST /database/test — tests a Postgres connection URL.
func handleTestConnection(w http.ResponseWriter, r *http.Request, cfg DatabaseAdminConfig) {
	var body databaseURLRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	body.URL = strings.TrimSpace(body.URL)
	if body.URL == "" {
		WriteError(w, ErrBadRequest("url is required"))
		return
	}

	result, err := cfg.Store.TestConnection(r.Context(), body.URL)
	if err != nil {
		WriteError(w, ErrInternal(err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// handleTriggerMigration handles POST /database/migrate — triggers SQLite-to-Postgres migration.
func handleTriggerMigration(w http.ResponseWriter, r *http.Request, cfg DatabaseAdminConfig) {
	var body databaseURLRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	body.URL = strings.TrimSpace(body.URL)
	if body.URL == "" {
		WriteError(w, ErrBadRequest("url is required"))
		return
	}

	status, err := cfg.Store.TriggerMigration(r.Context(), body.URL)
	if err != nil {
		WriteError(w, ErrInternal(err.Error()))
		return
	}

	writeJSON(w, http.StatusAccepted, status)
}
