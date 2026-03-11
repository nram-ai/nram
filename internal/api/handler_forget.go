package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"context"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/service"
)

// ForgetServicer defines the interface for forget/delete operations, allowing mocking in tests.
type ForgetServicer interface {
	Forget(ctx context.Context, req *service.ForgetRequest) (*service.ForgetResponse, error)
}

// bulkForgetRequestBody represents the JSON body for the bulk forget endpoint.
type bulkForgetRequestBody struct {
	IDs  []uuid.UUID `json:"ids"`
	Tags []string    `json:"tags"`
	Hard bool        `json:"hard"`
}

// mapForgetError converts a service error to an appropriate API error response.
func mapForgetError(w http.ResponseWriter, err error) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "project_id is required"),
		strings.Contains(msg, "at least one of"):
		WriteError(w, ErrBadRequest(msg))
	case strings.Contains(msg, "not found"):
		WriteError(w, ErrNotFound(msg))
	default:
		WriteError(w, ErrInternal(msg))
	}
}

// NewBulkForgetHandler returns an http.HandlerFunc that accepts a POST request to
// bulk forget (delete) memories within a project by IDs or tag filters.
func NewBulkForgetHandler(svc ForgetServicer, bus events.EventBus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse and validate project_id from the URL.
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		// Decode request body.
		var body bulkForgetRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		// Validate that at least one filter is present.
		if len(body.IDs) == 0 && len(body.Tags) == 0 {
			WriteError(w, ErrBadRequest("at least one of ids or tags is required"))
			return
		}

		// Build service request, mapping API field names to service field names.
		req := &service.ForgetRequest{
			ProjectID:  projectID,
			MemoryIDs:  body.IDs,
			Tags:       body.Tags,
			HardDelete: body.Hard,
		}

		// Populate caller identity from auth context.
		if ac := auth.FromContext(r.Context()); ac != nil {
			uid := ac.UserID
			req.UserID = &uid
		}

		// Call the service.
		resp, err := svc.Forget(r.Context(), req)
		if err != nil {
			mapForgetError(w, err)
			return
		}

		scope := "project:" + projectID.String()
		if len(body.IDs) > 0 {
			for _, id := range body.IDs {
				events.Emit(r.Context(), bus, events.MemoryDeleted, scope, map[string]string{
					"memory_id":  id.String(),
					"project_id": projectID.String(),
				})
			}
		} else {
			for i := 0; i < resp.Deleted; i++ {
				events.Emit(r.Context(), bus, events.MemoryDeleted, scope, map[string]string{
					"project_id": projectID.String(),
				})
			}
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

// NewDeleteHandler returns an http.HandlerFunc that accepts a DELETE request to
// soft-delete a single memory by its ID within a project.
func NewDeleteHandler(svc ForgetServicer, bus events.EventBus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse and validate project_id from the URL.
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		// Parse and validate memory id from the URL.
		memoryIDStr := chi.URLParam(r, "id")
		memoryID, err := uuid.Parse(memoryIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid memory id: must be a valid UUID"))
			return
		}

		// Build service request: single memory, always soft delete.
		req := &service.ForgetRequest{
			ProjectID:  projectID,
			MemoryID:   &memoryID,
			HardDelete: false,
		}

		// Populate caller identity from auth context.
		if ac := auth.FromContext(r.Context()); ac != nil {
			uid := ac.UserID
			req.UserID = &uid
		}

		// Call the service.
		resp, err := svc.Forget(r.Context(), req)
		if err != nil {
			mapForgetError(w, err)
			return
		}

		events.Emit(r.Context(), bus, events.MemoryDeleted, "project:"+projectID.String(), map[string]string{
			"memory_id":  memoryID.String(),
			"project_id": projectID.String(),
		})

		writeJSON(w, http.StatusOK, resp)
	}
}
