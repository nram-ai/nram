package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/service"
)

// EnrichServicer defines the interface for the enrich service used by the handler.
type EnrichServicer interface {
	Enrich(ctx context.Context, req *service.EnrichRequest) (*service.EnrichResponse, error)
}

// enrichRequestBody represents the JSON body for the enrich endpoint.
type enrichRequestBody struct {
	IDs      []uuid.UUID `json:"ids,omitempty"`
	All      bool        `json:"all,omitempty"`
	Priority int         `json:"priority,omitempty"`
}

// NewEnrichHandler returns an http.HandlerFunc that accepts a POST request to
// enrich memories within a project. It delegates to the given EnrichServicer.
func NewEnrichHandler(svc EnrichServicer, bus events.EventBus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse and validate project_id from the URL.
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		// Decode request body.
		var body enrichRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		// Validate that at least one of ids or all is specified.
		if len(body.IDs) == 0 && !body.All {
			WriteError(w, ErrBadRequest("at least one of ids or all must be specified"))
			return
		}

		// Build service request, mapping ids -> MemoryIDs.
		req := &service.EnrichRequest{
			ProjectID: projectID,
			MemoryIDs: body.IDs,
			All:       body.All,
			Priority:  body.Priority,
		}

		// Call the service.
		resp, err := svc.Enrich(r.Context(), req)
		if err != nil {
			msg := err.Error()

			scope := "project:" + projectID.String()
			for _, id := range body.IDs {
				events.Emit(r.Context(), bus, events.EnrichmentFailed, scope, map[string]string{
					"memory_id":  id.String(),
					"project_id": projectID.String(),
					"error":      msg,
				})
			}
			if body.All {
				events.Emit(r.Context(), bus, events.EnrichmentFailed, scope, map[string]string{
					"project_id": projectID.String(),
					"error":      msg,
				})
			}

			switch {
			case strings.Contains(msg, "not found"):
				WriteError(w, ErrNotFound(msg))
			case strings.Contains(msg, "is required"),
				strings.Contains(msg, "must be specified"):
				WriteError(w, ErrBadRequest(msg))
			default:
				WriteError(w, ErrInternal(msg))
			}
			return
		}

		scope := "project:" + projectID.String()
		for _, id := range body.IDs {
			events.Emit(r.Context(), bus, events.MemoryEnriched, scope, map[string]string{
				"memory_id":  id.String(),
				"project_id": projectID.String(),
			})
		}
		if body.All && resp.Queued > 0 {
			events.Emit(r.Context(), bus, events.MemoryEnriched, scope, map[string]string{
				"project_id": projectID.String(),
			})
		}

		writeJSON(w, http.StatusOK, resp)
	}
}
