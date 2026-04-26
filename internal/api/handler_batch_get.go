package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/service"
)

// BatchGetServicer defines the interface for batch memory retrieval.
type BatchGetServicer interface {
	BatchGet(ctx context.Context, req *service.BatchGetRequest) (*service.BatchGetResponse, error)
}

// batchGetRequestBody represents the JSON body for the batch get endpoint.
type batchGetRequestBody struct {
	IDs []uuid.UUID `json:"ids"`
}

// NewBatchGetHandler returns an http.HandlerFunc that accepts a POST request to
// retrieve multiple memories by ID within a project.
func NewBatchGetHandler(svc BatchGetServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse and validate project_id from the URL.
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		// Decode request body.
		var body batchGetRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		// Validate IDs non-empty.
		if len(body.IDs) == 0 {
			WriteError(w, ErrBadRequest("ids must be non-empty"))
			return
		}

		// Build service request.
		req := &service.BatchGetRequest{
			ProjectID:         projectID,
			IDs:               body.IDs,
			IncludeSuperseded: queryParamBool(r, includeSupersededParam),
		}

		// Call the service.
		resp, err := svc.BatchGet(r.Context(), req)
		if err != nil {
			msg := err.Error()
			switch {
			case strings.Contains(msg, "project_id is required"),
				strings.Contains(msg, "ids must be non-empty"):
				WriteError(w, ErrBadRequest(msg))
			case strings.Contains(msg, "not found"):
				WriteError(w, ErrNotFound(msg))
			default:
				WriteError(w, ErrInternal(msg))
			}
			return
		}

		writeJSON(w, http.StatusOK, resp)
	}
}
