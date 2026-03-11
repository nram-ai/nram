package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/service"
)

// storeRequestBody represents the JSON body for the store memory endpoint.
type storeRequestBody struct {
	Content  string          `json:"content"`
	Source   string          `json:"source"`
	Tags     []string        `json:"tags"`
	Metadata json.RawMessage `json:"metadata"`
	Options  storeBodyOpts   `json:"options"`
}

type storeBodyOpts struct {
	Enrich  bool   `json:"enrich"`
	Extract bool   `json:"extract"`
	TTL     string `json:"ttl"`
}

// NewStoreHandler returns an http.HandlerFunc that accepts a POST request to
// create a new memory within a project. It delegates to the given StoreService.
func NewStoreHandler(svc *service.StoreService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse and validate project_id from the URL.
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		// Decode request body.
		var body storeRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		// Validate required fields.
		if body.Content == "" {
			WriteError(w, ErrBadRequest("content is required"))
			return
		}

		// Build service request.
		req := &service.StoreRequest{
			ProjectID: projectID,
			Content:   body.Content,
			Source:    body.Source,
			Tags:      body.Tags,
			Metadata:  body.Metadata,
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
			req.APIKeyID = ac.APIKeyID
		}

		// Call the service.
		resp, err := svc.Store(r.Context(), req)
		if err != nil {
			// Map known error patterns to appropriate HTTP status codes.
			msg := err.Error()
			switch {
			case strings.Contains(msg, "content is required"),
				strings.Contains(msg, "project_id is required"),
				strings.Contains(msg, "invalid TTL"),
				strings.Contains(msg, "enrich and extract cannot both be true"):
				WriteError(w, ErrBadRequest(msg))
			case strings.Contains(msg, "not found"):
				WriteError(w, ErrNotFound(msg))
			default:
				WriteError(w, ErrInternal(msg))
			}
			return
		}

		writeJSON(w, http.StatusCreated, resp)
	}
}
