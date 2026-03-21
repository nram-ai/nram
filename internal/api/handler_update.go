package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/service"
)

// UpdateServicer defines the interface for update operations, allowing mocking in tests.
type UpdateServicer interface {
	Update(ctx context.Context, req *service.UpdateRequest) (*service.UpdateResponse, error)
}

// updateRequestBody represents the JSON body for the update memory endpoint.
type updateRequestBody struct {
	Content  *string          `json:"content,omitempty"`
	Tags     *[]string        `json:"tags,omitempty"`
	Metadata *json.RawMessage `json:"metadata,omitempty"`
}

// NewUpdateHandler returns an http.HandlerFunc that accepts a PUT request to
// update an existing memory within a project. It delegates to the given UpdateServicer.
func NewUpdateHandler(svc UpdateServicer, bus events.EventBus) http.HandlerFunc {
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

		// Decode request body.
		var body updateRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		// Build service request.
		req := &service.UpdateRequest{
			ProjectID: projectID,
			MemoryID:  memoryID,
			Content:   body.Content,
			Tags:      body.Tags,
			Metadata:  body.Metadata,
		}

		// Populate caller identity from auth context.
		if ac := auth.FromContext(r.Context()); ac != nil {
			uid := ac.UserID
			req.UserID = &uid
			if ac.OrgID != uuid.Nil {
				oid := ac.OrgID
				req.OrgID = &oid
			}
			req.APIKeyID = ac.APIKeyID
		}

		// Call the service.
		resp, err := svc.Update(r.Context(), req)
		if err != nil {
			mapUpdateError(w, err)
			return
		}

		events.Emit(r.Context(), bus, events.MemoryUpdated, "project:"+projectID.String(), map[string]string{
			"memory_id":  memoryID.String(),
			"project_id": projectID.String(),
		})

		writeJSON(w, http.StatusOK, resp)
	}
}

// mapUpdateError converts a service error to an appropriate API error response.
func mapUpdateError(w http.ResponseWriter, err error) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "is required"),
		strings.Contains(msg, "at least one of"),
		strings.Contains(msg, "does not belong to"):
		WriteError(w, ErrBadRequest(msg))
	case strings.Contains(msg, "not found"):
		WriteError(w, ErrNotFound(msg))
	default:
		WriteError(w, ErrInternal(msg))
	}
}
