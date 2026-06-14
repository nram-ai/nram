package api

import (
	"context"
	"encoding/csv"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// LogAdminStore is the storage surface the admin logs handler needs.
// *storage.LogEntryRepo satisfies it.
type LogAdminStore interface {
	List(ctx context.Context, f storage.LogFilter, limit, offset int) ([]model.LogEntry, error)
	Count(ctx context.Context, f storage.LogFilter) (int, error)
	Components(ctx context.Context) ([]string, error)
}

// LogAdminConfig holds the dependencies for the admin logs handler.
type LogAdminConfig struct {
	Store LogAdminStore
}

const (
	logsDefaultLimit = 100
	logsMaxLimit     = 500
	// logsExportMax bounds an export so a single request cannot load the whole
	// rolling window into memory. When hit, the response is truncated and a
	// header + log line flag it rather than silently capping.
	logsExportMax = 50000
)

// logFacetsResponse lists the values available for the Logs page filter
// dropdowns: the fixed level set and the distinct components seen so far.
type logFacetsResponse struct {
	Levels     []string `json:"levels"`
	Components []string `json:"components"`
}

// NewAdminLogsHandler returns the handler for /v1/admin/logs. It is mounted
// inside the /v1/admin route group, which already requires RoleAdministrator
// (the instance-operator tier, above org_owner), so the diagnostic log stream
// is never exposed to a tenant admin. The stream is global and not tenant
// scoped.
//
// Routes:
//   - GET /v1/admin/logs                    paginated, filtered list
//   - GET /v1/admin/logs/export?format=csv  filtered export (csv | json)
//   - GET /v1/admin/logs/facets             filter dropdown values
func NewAdminLogsHandler(cfg LogAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}
		switch extractSubPath(r.URL.Path, "/logs") {
		case "":
			handleAdminListLogs(w, r, cfg.Store)
		case "export":
			handleAdminExportLogs(w, r, cfg.Store)
		case "facets":
			handleAdminLogFacets(w, r, cfg.Store)
		default:
			WriteError(w, ErrNotFound("not found"))
		}
	}
}

// parseLogFilter builds a storage.LogFilter from the request query parameters.
// level accepts a comma-separated set or repeated values; from/to are RFC3339.
func parseLogFilter(r *http.Request) storage.LogFilter {
	q := r.URL.Query()
	f := storage.LogFilter{
		Component: strings.TrimSpace(q.Get("component")),
		Search:    strings.TrimSpace(q.Get("search")),
		AttrKey:   strings.TrimSpace(q.Get("attr_key")),
		AttrValue: q.Get("attr_value"),
	}
	for _, raw := range q["level"] {
		for lvl := range strings.SplitSeq(raw, ",") {
			if lvl = strings.TrimSpace(lvl); lvl != "" {
				f.Levels = append(f.Levels, lvl)
			}
		}
	}
	f.From = parseRFC3339Param(q.Get("from"))
	f.To = parseRFC3339Param(q.Get("to"))
	return f
}

// parseRFC3339Param parses an optional RFC3339 query value, returning nil when
// absent or unparseable.
func parseRFC3339Param(v string) *time.Time {
	if v = strings.TrimSpace(v); v == "" {
		return nil
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return &t
	}
	return nil
}

func handleAdminListLogs(w http.ResponseWriter, r *http.Request, store LogAdminStore) {
	limit := parseIntParam(r, "limit", logsDefaultLimit)
	if limit < 0 {
		limit = logsDefaultLimit
	}
	limit = min(limit, logsMaxLimit)
	offset := max(parseIntParam(r, "offset", 0), 0)

	f := parseLogFilter(r)
	total, err := store.Count(r.Context(), f)
	if err != nil {
		WriteError(w, ErrInternal("failed to count logs"))
		return
	}
	entries, err := store.List(r.Context(), f, limit, offset)
	if err != nil {
		WriteError(w, ErrInternal("failed to list logs"))
		return
	}
	if entries == nil {
		entries = []model.LogEntry{}
	}
	writeJSON(w, http.StatusOK, model.PaginatedResponse[model.LogEntry]{
		Data: entries,
		Pagination: model.Pagination{
			Total:  total,
			Limit:  limit,
			Offset: offset,
		},
	})
}

func handleAdminLogFacets(w http.ResponseWriter, r *http.Request, store LogAdminStore) {
	components, err := store.Components(r.Context())
	if err != nil {
		WriteError(w, ErrInternal("failed to list log components"))
		return
	}
	if components == nil {
		components = []string{}
	}
	writeJSON(w, http.StatusOK, logFacetsResponse{
		Levels:     []string{model.LogLevelDebug, model.LogLevelInfo, model.LogLevelWarn, model.LogLevelError},
		Components: components,
	})
}

func handleAdminExportLogs(w http.ResponseWriter, r *http.Request, store LogAdminStore) {
	format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if format == "" {
		format = "json"
	}
	if format != "json" && format != "csv" {
		WriteError(w, ErrBadRequest("format must be json or csv"))
		return
	}

	f := parseLogFilter(r)
	// Fetch one past the cap so truncation is detectable.
	entries, err := store.List(r.Context(), f, logsExportMax+1, 0)
	if err != nil {
		WriteError(w, ErrInternal("failed to export logs"))
		return
	}
	truncated := len(entries) > logsExportMax
	if truncated {
		entries = entries[:logsExportMax]
		w.Header().Set("X-Truncated", "true")
	}

	stamp := time.Now().UTC().Format("20060102-150405")
	if format == "json" {
		w.Header().Set("Content-Disposition", `attachment; filename="logs-`+stamp+`.json"`)
		writeJSON(w, http.StatusOK, entries)
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="logs-`+stamp+`.csv"`)
	w.WriteHeader(http.StatusOK)
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"ts", "level", "component", "message", "attrs", "project_id", "namespace_id", "user_id"})
	for _, e := range entries {
		_ = cw.Write([]string{
			e.Timestamp.UTC().Format(time.RFC3339Nano),
			e.Level,
			e.Component,
			e.Message,
			string(e.Attrs),
			uuidPtrString(e.ProjectID),
			uuidPtrString(e.NamespaceID),
			uuidPtrString(e.UserID),
		})
	}
	cw.Flush()
}

func uuidPtrString(id *uuid.UUID) string {
	if id == nil {
		return ""
	}
	return id.String()
}
