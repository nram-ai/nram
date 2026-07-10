package api

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"log/slog"
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
	ListKeyset(ctx context.Context, f storage.LogFilter, cursor *storage.LogCursor, limit int) ([]model.LogEntry, error)
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
	// logsExportPageSize is how many rows the streaming export pulls per DB
	// page. The export pages through the whole result set with a keyset cursor
	// and writes each page straight to the response, so peak memory is one page
	// regardless of how large the filtered window is.
	logsExportPageSize = 5000
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
	// Fetch the first page before writing any status so an early DB error still
	// surfaces as a real 500. Once the first byte of the body is written the
	// response is committed, so a later-page error can only be logged and the
	// body cut short.
	first, err := store.ListKeyset(r.Context(), f, nil, logsExportPageSize)
	if err != nil {
		WriteError(w, ErrInternal("failed to export logs"))
		return
	}

	stamp := time.Now().UTC().Format("20060102-150405")
	if format == "json" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Content-Disposition", `attachment; filename="logs-`+stamp+`.json"`)
		streamLogsJSON(w, r.Context(), store, f, first)
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="logs-`+stamp+`.csv"`)
	streamLogsCSV(w, r.Context(), store, f, first)
}

// exportLogPages walks the keyset-paged result set, invoking emit for every row
// and afterPage once per page (the flush point). The first page is fetched by
// the caller (so an early error can still set a 500); subsequent pages are
// pulled here. A mid-stream DB error is logged and ends the walk, since the
// response body is already committed by then.
func exportLogPages(ctx context.Context, store LogAdminStore, f storage.LogFilter, first []model.LogEntry, emit func(model.LogEntry), afterPage func()) {
	batch := first
	for {
		for _, e := range batch {
			emit(e)
		}
		if afterPage != nil {
			afterPage()
		}
		if len(batch) < logsExportPageSize {
			return
		}
		last := batch[len(batch)-1]
		next, err := store.ListKeyset(ctx, f, &storage.LogCursor{TS: last.Timestamp, ID: last.ID}, logsExportPageSize)
		if err != nil {
			slog.Error("api: log export paging failed mid-stream", "err", err)
			return
		}
		batch = next
	}
}

func streamLogsCSV(w http.ResponseWriter, ctx context.Context, store LogAdminStore, f storage.LogFilter, first []model.LogEntry) {
	w.WriteHeader(http.StatusOK)
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"ts", "level", "component", "message", "attrs", "project_id", "namespace_id", "user_id"})
	flusher, _ := w.(http.Flusher)
	emit := func(e model.LogEntry) {
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
	exportLogPages(ctx, store, f, first, emit, func() {
		cw.Flush()
		if flusher != nil {
			flusher.Flush()
		}
	})
	cw.Flush()
}

func streamLogsJSON(w http.ResponseWriter, ctx context.Context, store LogAdminStore, f storage.LogFilter, first []model.LogEntry) {
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	_, _ = w.Write([]byte("["))
	firstRow := true
	emit := func(e model.LogEntry) {
		row, err := json.Marshal(e)
		if err != nil {
			slog.Error("api: log export row marshal failed", "err", err)
			return
		}
		if !firstRow {
			_, _ = w.Write([]byte(","))
		}
		firstRow = false
		_, _ = w.Write(row)
	}
	exportLogPages(ctx, store, f, first, emit, func() {
		if flusher != nil {
			flusher.Flush()
		}
	})
	_, _ = w.Write([]byte("]"))
}

func uuidPtrString(id *uuid.UUID) string {
	if id == nil {
		return ""
	}
	return id.String()
}
