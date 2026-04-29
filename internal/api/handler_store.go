package api

import (
	"encoding/json"
	"math"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/service"
)

// storeRequestBody represents the JSON body for the store memory endpoint.
//
// Importance is the only [0,1] memory-score field a caller can seed at store
// time; Confidence is deliberately not accepted because it is an internal
// signal driven by reinforcement, decay, and contradiction haircuts. A
// client-supplied confidence would let callers game ranking. Fields that
// look like Confidence in the request body are silently ignored — same as
// any other unknown JSON key — which preserves the contract.
type storeRequestBody struct {
	Content    string          `json:"content"`
	Source     string          `json:"source"`
	Tags       []string        `json:"tags"`
	Importance *float64        `json:"importance,omitempty"`
	Metadata   json.RawMessage `json:"metadata"`
	Options    storeBodyOpts   `json:"options"`
}

type storeBodyOpts struct {
	Enrich  bool   `json:"enrich"`
	Extract bool   `json:"extract"`
	TTL     string `json:"ttl"`
}

// NewStoreHandler returns an http.HandlerFunc that accepts a POST request to
// create a new memory within a project. It delegates to the given StoreService.
func NewStoreHandler(svc *service.StoreService, bus events.EventBus) http.HandlerFunc {
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
		if body.Importance != nil {
			v := *body.Importance
			if math.IsNaN(v) || math.IsInf(v, 0) {
				WriteError(w, ErrBadRequest("importance: must be finite"))
				return
			}
			if v < 0 || v > 1 {
				WriteError(w, ErrBadRequest("importance: must be in [0.0, 1.0]"))
				return
			}
		}

		// Build service request.
		req := &service.StoreRequest{
			ProjectID:  projectID,
			Content:    body.Content,
			Source:     body.Source,
			Tags:       body.Tags,
			Importance: body.Importance,
			Metadata:   body.Metadata,
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

		events.Emit(r.Context(), bus, events.MemoryCreated, "project:"+projectID.String(), map[string]string{
			"memory_id":  resp.ID.String(),
			"project_id": projectID.String(),
			"source":     body.Source,
		})

		writeJSON(w, http.StatusCreated, resp)
	}
}
