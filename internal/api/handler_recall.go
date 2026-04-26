package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// UserReader provides read access to user records for user-scoped handlers.
type UserReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.User, error)
}

// recallRequestBody represents the JSON body for recall endpoints.
type recallRequestBody struct {
	Query                string   `json:"query"`
	Limit                int      `json:"limit"`
	Threshold            float64  `json:"threshold"`
	Tags                 []string `json:"tags"`
	IncludeGraph         bool     `json:"include_graph"`
	GraphDepth           int      `json:"graph_depth"`
	IncludeLowNovelty    bool     `json:"include_low_novelty"`
	DiversifyByTagPrefix string   `json:"diversify_by_tag_prefix"`
}

// RecallServicer defines the interface for recall operations, allowing mocking in tests.
type RecallServicer interface {
	Recall(ctx context.Context, req *service.RecallRequest) (*service.RecallResponse, error)
}

// mapRecallError converts a service error to an appropriate API error response.
func mapRecallError(w http.ResponseWriter, err error) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "query is required"),
		strings.Contains(msg, "project_id is required"):
		WriteError(w, ErrBadRequest(msg))
	case strings.Contains(msg, "not found"):
		WriteError(w, ErrNotFound(msg))
	default:
		WriteError(w, ErrInternal(msg))
	}
}

// NewRecallHandler returns an http.HandlerFunc for project-scoped memory recall.
// It expects project_id as a URL parameter.
func NewRecallHandler(svc RecallServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}

		var body recallRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		if strings.TrimSpace(body.Query) == "" {
			WriteError(w, ErrBadRequest("query is required"))
			return
		}

		req := &service.RecallRequest{
			ProjectID:            projectID,
			Query:                body.Query,
			Limit:                body.Limit,
			Threshold:            body.Threshold,
			Tags:                 body.Tags,
			IncludeGraph:         body.IncludeGraph,
			GraphDepth:           body.GraphDepth,
			IncludeLowNovelty:    body.IncludeLowNovelty,
			DiversifyByTagPrefix: body.DiversifyByTagPrefix,
		}

		if ac := auth.FromContext(r.Context()); ac != nil {
			uid := ac.UserID
			req.UserID = &uid
			req.APIKeyID = ac.APIKeyID
		}

		resp, err := svc.Recall(r.Context(), req)
		if err != nil {
			mapRecallError(w, err)
			return
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

// NewMeRecallHandler returns an http.HandlerFunc for user-scoped memory recall.
// It looks up the authenticated user's namespace and searches across all projects.
func NewMeRecallHandler(svc RecallServicer, users UserReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body recallRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		if strings.TrimSpace(body.Query) == "" {
			WriteError(w, ErrBadRequest("query is required"))
			return
		}

		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}

		user, err := users.GetByID(r.Context(), ac.UserID)
		if err != nil {
			WriteError(w, ErrNotFound("user not found"))
			return
		}

		req := &service.RecallRequest{
			Query:                body.Query,
			Limit:                body.Limit,
			Threshold:            body.Threshold,
			Tags:                 body.Tags,
			IncludeGraph:         body.IncludeGraph,
			GraphDepth:           body.GraphDepth,
			IncludeLowNovelty:    body.IncludeLowNovelty,
			DiversifyByTagPrefix: body.DiversifyByTagPrefix,
			NamespaceID:          &user.NamespaceID,
		}

		uid := ac.UserID
		req.UserID = &uid
		req.APIKeyID = ac.APIKeyID

		resp, err := svc.Recall(r.Context(), req)
		if err != nil {
			mapRecallError(w, err)
			return
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

