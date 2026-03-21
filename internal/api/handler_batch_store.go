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

// BatchStoreServicer abstracts the batch store service for testability.
type BatchStoreServicer interface {
	BatchStore(ctx context.Context, req *service.BatchStoreRequest) (*service.BatchStoreResponse, error)
}

// batchStoreRequestBody represents the JSON body for the batch store endpoint.
type batchStoreRequestBody struct {
	Items   []service.BatchStoreItem `json:"items"`
	Options storeBodyOpts            `json:"options"`
}

// NewBatchStoreHandler returns an http.HandlerFunc that accepts a POST request to
// create multiple memories in batch within a project.
func NewBatchStoreHandler(svc BatchStoreServicer, bus events.EventBus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse and validate project_id from the URL.
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		// Decode request body.
		var body batchStoreRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		// Validate items non-empty.
		if len(body.Items) == 0 {
			WriteError(w, ErrBadRequest("items must not be empty"))
			return
		}

		// Build service request.
		req := &service.BatchStoreRequest{
			ProjectID: projectID,
			Items:     body.Items,
			Options: service.StoreOptions{
				Enrich:  body.Options.Enrich,
				Extract: body.Options.Extract,
				TTL:     body.Options.TTL,
			},
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
		resp, err := svc.BatchStore(r.Context(), req)
		if err != nil {
			msg := err.Error()
			switch {
			case strings.Contains(msg, "items must not be empty"),
				strings.Contains(msg, "project_id is required"),
				strings.Contains(msg, "too many items"),
				strings.Contains(msg, "invalid TTL"):
				WriteError(w, ErrBadRequest(msg))
			case strings.Contains(msg, "not found"):
				WriteError(w, ErrNotFound(msg))
			default:
				WriteError(w, ErrInternal(msg))
			}
			return
		}

		scope := "project:" + projectID.String()
		for i := 0; i < resp.MemoriesCreated; i++ {
			events.Emit(r.Context(), bus, events.MemoryCreated, scope, map[string]string{
				"project_id": projectID.String(),
			})
		}

		writeJSON(w, http.StatusCreated, resp)
	}
}
