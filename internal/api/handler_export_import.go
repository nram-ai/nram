package api

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/service"
)

// ExportServicer abstracts the export service for handler use.
type ExportServicer interface {
	Export(ctx context.Context, req *service.ExportRequest) (*service.ExportData, error)
	ExportNDJSON(ctx context.Context, req *service.ExportRequest, w io.Writer) error
}

// ImportServicer abstracts the import service for handler use.
type ImportServicer interface {
	Import(ctx context.Context, req *service.ImportRequest) (*service.ImportResponse, error)
}

// NewExportHandler returns an http.HandlerFunc that serves
// GET /v1/projects/{project_id}/memories/export.
// Supports JSON (default) and NDJSON formats via the ?format= query parameter
// or the Accept header.
func NewExportHandler(svc ExportServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		// Determine export format from query param, then Accept header, default JSON.
		format := service.ExportFormatJSON
		if qf := r.URL.Query().Get("format"); qf != "" {
			switch strings.ToLower(qf) {
			case "json":
				format = service.ExportFormatJSON
			case "ndjson":
				format = service.ExportFormatNDJSON
			default:
				WriteError(w, ErrBadRequest("invalid format: must be \"json\" or \"ndjson\""))
				return
			}
		} else if strings.Contains(r.Header.Get("Accept"), "application/x-ndjson") {
			format = service.ExportFormatNDJSON
		}

		req := &service.ExportRequest{
			ProjectID: projectID,
			Format:    format,
		}

		if format == service.ExportFormatNDJSON {
			w.Header().Set("Content-Type", "application/x-ndjson")
			if err := svc.ExportNDJSON(r.Context(), req, w); err != nil {
				msg := err.Error()
				switch {
				case strings.Contains(msg, "not found"):
					WriteError(w, ErrNotFound(msg))
				default:
					WriteError(w, ErrInternal(msg))
				}
			}
			return
		}

		data, err := svc.Export(r.Context(), req)
		if err != nil {
			msg := err.Error()
			switch {
			case strings.Contains(msg, "not found"):
				WriteError(w, ErrNotFound(msg))
			default:
				WriteError(w, ErrInternal(msg))
			}
			return
		}

		writeJSON(w, http.StatusOK, data)
	}
}

// NewImportHandler returns an http.HandlerFunc that serves
// POST /v1/projects/{project_id}/memories/import.
// Supports nram (default), mem0, and zep import formats via the ?format= query parameter.
func NewImportHandler(svc ImportServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		// Determine import format, default to nram.
		format := service.ImportFormatNRAM
		if qf := r.URL.Query().Get("format"); qf != "" {
			switch strings.ToLower(qf) {
			case "nram":
				format = service.ImportFormatNRAM
			case "mem0":
				format = service.ImportFormatMem0
			case "zep":
				format = service.ImportFormatZep
			default:
				WriteError(w, ErrBadRequest("invalid format: must be \"nram\", \"mem0\", or \"zep\""))
				return
			}
		}

		req := &service.ImportRequest{
			ProjectID: projectID,
			Format:    format,
			Data:      r.Body,
		}

		resp, err := svc.Import(r.Context(), req)
		if err != nil {
			msg := err.Error()
			switch {
			case strings.Contains(msg, "not found"):
				WriteError(w, ErrNotFound(msg))
			case strings.Contains(msg, "is required"),
				strings.Contains(msg, "unsupported import format"):
				WriteError(w, ErrBadRequest(msg))
			default:
				WriteError(w, ErrInternal(msg))
			}
			return
		}

		writeJSON(w, http.StatusOK, resp)
	}
}
