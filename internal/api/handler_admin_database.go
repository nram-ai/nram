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
	Preflight(ctx context.Context, url string) (*PreflightReport, error)
	ResetTarget(ctx context.Context, url, mode string) (*ResetResult, error)
	MigrationAudit(ctx context.Context) (*MigrationAudit, error)
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
	Vectors       int `json:"vectors"`
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
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Stats   *MigrationStats `json:"stats,omitempty"`
}

// MigrationStats describes what a completed migration accomplished.
// Inserted maps table → rows successfully written. SkippedOrphans maps
// "table.column" → rows dropped because their FK parent was missing.
// SkippedUpdates maps "table.column" → second-pass column updates skipped
// (e.g. memories.superseded_by pointing at a dropped target).
type MigrationStats struct {
	Inserted       map[string]int `json:"inserted,omitempty"`
	SkippedOrphans map[string]int `json:"skipped_orphans,omitempty"`
	SkippedUpdates map[string]int `json:"skipped_updates,omitempty"`
}

// PreflightReport summarizes all pre-migration checks against a target Postgres URL.
type PreflightReport struct {
	OK     bool             `json:"ok"`
	Checks []PreflightCheck `json:"checks"`
}

// PreflightCheck is a single pre-migration check result with an optional remediation hint.
type PreflightCheck struct {
	Name        string         `json:"name"`
	Status      string         `json:"status"`
	Message     string         `json:"message"`
	Remediation string         `json:"remediation,omitempty"`
	TableCounts map[string]int `json:"table_counts,omitempty"`
}

// Preflight status constants.
const (
	PreflightStatusOK    = "ok"
	PreflightStatusWarn  = "warn"
	PreflightStatusError = "error"
)

// ResetResult describes the outcome of a target-Postgres reset request.
type ResetResult struct {
	Status        string   `json:"status"`
	Message       string   `json:"message"`
	Mode          string   `json:"mode"`
	TablesDropped []string `json:"tables_dropped,omitempty"`
}

// Reset modes.
const (
	ResetModeTruncate   = "truncate"
	ResetModeDropSchema = "drop_schema"
)

// MigrationAudit reports orphan-row counts per foreign-key relationship in the
// SQLite source database. It is a read-only scan used before a SQLite→Postgres
// migration: any count > 0 flags rows that would violate a Postgres FK constraint
// at INSERT time. The migrator itself drops orphans on read once this is approved.
type MigrationAudit struct {
	Backend      string        `json:"backend"`
	TotalOrphans int           `json:"total_orphans"`
	Orphans      []OrphanCount `json:"orphans"`
	Errors       []AuditError  `json:"errors,omitempty"`
}

// OrphanCount is a single FK-level orphan report.
type OrphanCount struct {
	Table      string `json:"table"`
	Column     string `json:"column"`
	References string `json:"references"`
	Count      int    `json:"count"`
}

// AuditError records a FK relationship that could not be audited (e.g. because
// an expected table does not exist on the source). Not fatal — reported alongside
// the orphan counts so the operator can decide.
type AuditError struct {
	Table   string `json:"table"`
	Column  string `json:"column"`
	Message string `json:"message"`
}

// databaseURLRequest is the request body for connection test and migration endpoints.
type databaseURLRequest struct {
	URL string `json:"url"`
}

// resetRequest is the request body for POST /database/reset.
type resetRequest struct {
	URL  string `json:"url"`
	Mode string `json:"mode"`
}

// NewAdminDatabaseHandler returns an http.HandlerFunc that dispatches database
// admin requests based on method and sub-path.
//
// Routes:
//   - GET  /database           — current backend info and stats
//   - POST /database/test      — test a Postgres connection URL
//   - POST /database/migrate   — trigger SQLite-to-Postgres migration
//   - POST /database/preflight — preflight checks against a target Postgres URL
//   - POST /database/reset     — reset a target Postgres DB's nram tables
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
		case sub == "preflight" && r.Method == http.MethodPost:
			handlePreflight(w, r, cfg)
		case sub == "reset" && r.Method == http.MethodPost:
			handleReset(w, r, cfg)
		case sub == "migration-audit" && r.Method == http.MethodGet:
			handleMigrationAudit(w, r, cfg)
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

// handlePreflight handles POST /database/preflight — runs pre-migration checks against a target URL.
func handlePreflight(w http.ResponseWriter, r *http.Request, cfg DatabaseAdminConfig) {
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

	report, err := cfg.Store.Preflight(r.Context(), body.URL)
	if err != nil {
		WriteError(w, ErrInternal(err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, report)
}

// handleReset handles POST /database/reset — wipes nram tables from the target DB.
// Mode must be explicitly supplied (no default) so the caller makes the choice each time.
func handleReset(w http.ResponseWriter, r *http.Request, cfg DatabaseAdminConfig) {
	var body resetRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	body.URL = strings.TrimSpace(body.URL)
	body.Mode = strings.TrimSpace(body.Mode)
	if body.URL == "" {
		WriteError(w, ErrBadRequest("url is required"))
		return
	}
	if body.Mode != ResetModeTruncate && body.Mode != ResetModeDropSchema {
		WriteError(w, ErrBadRequest("mode must be 'truncate' or 'drop_schema'"))
		return
	}

	result, err := cfg.Store.ResetTarget(r.Context(), body.URL, body.Mode)
	if err != nil {
		WriteError(w, ErrInternal(err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// handleMigrationAudit handles GET /database/migration-audit — scans the SQLite
// source for orphan FK rows that would break a Postgres migration.
func handleMigrationAudit(w http.ResponseWriter, r *http.Request, cfg DatabaseAdminConfig) {
	audit, err := cfg.Store.MigrationAudit(r.Context())
	if err != nil {
		WriteError(w, ErrInternal(err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, audit)
}
