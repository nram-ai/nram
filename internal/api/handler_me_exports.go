package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// MeExportService is the surface required by the /v1/me/exports handlers.
// Defined here as a small interface so handler tests can stub the worker
// pipeline.
type MeExportService interface {
	Enqueue(ctx context.Context, req service.EnqueueRequest) (*model.ExportJob, error)
	GetForUser(ctx context.Context, userID, jobID uuid.UUID) (*model.ExportJob, error)
	ListForUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]model.ExportJob, error)
	DeleteForUser(ctx context.Context, userID, jobID uuid.UUID) error
	OpenArtifact(ctx context.Context, userID, jobID uuid.UUID) (io.ReadCloser, *model.ExportJob, error)
}

// MeExportHandlers bundles the three handlers that back the /v1/me/exports
// route family. Router uses the named fields directly so the chi route
// patterns (root GET+POST, item GET+DELETE, /download GET) stay legible
// at the mount site.
type MeExportHandlers struct {
	List     http.HandlerFunc
	Item     http.HandlerFunc
	Download http.HandlerFunc
}

// createExportRequest is the wire shape for POST /v1/me/exports.
type createExportRequest struct {
	Scope             string  `json:"scope"`
	ProjectID         *string `json:"project_id,omitempty"`
	Format            string  `json:"format,omitempty"`
	IncludeSuperseded bool    `json:"include_superseded,omitempty"`
}

// NewMeExportHandlers wires the three handler functions for the
// /v1/me/exports* route family from a single service dep. Returning a
// struct keeps the wire-up at the call site to one line instead of three
// near-identical config-passing factories.
func NewMeExportHandlers(svc MeExportService) MeExportHandlers {
	return MeExportHandlers{
		List:     newMeExportsListCreate(svc),
		Item:     newMeExportItem(svc),
		Download: newMeExportDownload(svc),
	}
}

// newMeExportsListCreate returns the GET+POST handler at /v1/me/exports.
// GET lists the caller's exports; POST enqueues a new one.
func newMeExportsListCreate(svc MeExportService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleMeExportsList(w, r, svc, ac.UserID)
		case http.MethodPost:
			handleMeExportsCreate(w, r, svc, ac.UserID)
		default:
			WriteError(w, ErrBadRequest("method not allowed"))
		}
	}
}

func handleMeExportsList(w http.ResponseWriter, r *http.Request, svc MeExportService, userID uuid.UUID) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}
	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	jobs, err := svc.ListForUser(r.Context(), userID, limit, offset)
	if err != nil {
		WriteError(w, ErrInternal("failed to list export jobs"))
		return
	}
	if jobs == nil {
		jobs = []model.ExportJob{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": jobs})
}

func handleMeExportsCreate(w http.ResponseWriter, r *http.Request, svc MeExportService, userID uuid.UUID) {
	var body createExportRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	req := service.EnqueueRequest{
		UserID:            userID,
		Scope:             strings.ToLower(strings.TrimSpace(body.Scope)),
		Format:            strings.ToLower(strings.TrimSpace(body.Format)),
		IncludeSuperseded: body.IncludeSuperseded,
	}
	if body.ProjectID != nil && *body.ProjectID != "" {
		pid, err := uuid.Parse(*body.ProjectID)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}
		req.ProjectID = &pid
	}

	job, err := svc.Enqueue(r.Context(), req)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrExportJobBadRequest):
			WriteError(w, ErrBadRequest(err.Error()))
		case errors.Is(err, service.ErrExportJobAlreadyRunning):
			WriteError(w, ErrConflict(err.Error()))
		case errors.Is(err, service.ErrExportJobRateLimited):
			WriteError(w, ErrRateLimited(err.Error()))
		default:
			WriteError(w, ErrInternal("failed to enqueue export job"))
		}
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

// newMeExportItem returns the GET+DELETE handler at
// /v1/me/exports/{job_id}.
func newMeExportItem(svc MeExportService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}
		jobID, err := uuid.Parse(chi.URLParam(r, "job_id"))
		if err != nil {
			WriteError(w, ErrBadRequest("invalid job_id: must be a valid UUID"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			job, err := svc.GetForUser(r.Context(), ac.UserID, jobID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					WriteError(w, ErrNotFound("export job not found"))
					return
				}
				WriteError(w, ErrInternal("failed to load export job"))
				return
			}
			writeJSON(w, http.StatusOK, job)
		case http.MethodDelete:
			if err := svc.DeleteForUser(r.Context(), ac.UserID, jobID); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					WriteError(w, ErrNotFound("export job not found"))
					return
				}
				WriteError(w, ErrInternal("failed to delete export job"))
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			WriteError(w, ErrBadRequest("method not allowed"))
		}
	}
}

// newMeExportDownload returns the GET handler at
// /v1/me/exports/{job_id}/download. Streams the artifact file inline.
func newMeExportDownload(svc MeExportService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}
		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}
		jobID, err := uuid.Parse(chi.URLParam(r, "job_id"))
		if err != nil {
			WriteError(w, ErrBadRequest("invalid job_id: must be a valid UUID"))
			return
		}

		f, job, err := svc.OpenArtifact(r.Context(), ac.UserID, jobID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if job != nil && job.Status != model.ExportStatusSucceeded {
					WriteError(w, ErrConflict("export job is not in succeeded state"))
					return
				}
				WriteError(w, ErrNotFound("export job artifact not found"))
				return
			}
			WriteError(w, ErrInternal("failed to open artifact"))
			return
		}
		defer f.Close()

		filename := "nram-export-" + jobID.String() + ".zip"
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
		if job.ArtifactBytes != nil {
			w.Header().Set("Content-Length", strconv.FormatInt(*job.ArtifactBytes, 10))
		}
		// Stream the artifact. Errors after the header has been written
		// cannot be converted into a JSON error; the client sees a
		// truncated body, and io.Copy reports the partial-write count
		// which we ignore — the caller can retry on a non-200 status
		// only, which is too late here.
		_, _ = io.Copy(w, f)
	}
}
