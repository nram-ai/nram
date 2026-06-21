package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// AskServicer is the ask synthesis surface the REST handlers drive. The concrete
// *service.AskService satisfies it; the interface keeps the handlers mockable.
type AskServicer interface {
	Ask(ctx context.Context, req *service.AskRequest) (*service.AskResponse, error)
}

// AskProjectReader resolves a project by ID so the project-scoped ask endpoint
// can turn its path UUID into the slug the ask service expects.
type AskProjectReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// askRequestBody is the JSON body for the ask endpoints. project is honored
// only on the user-scoped endpoint (the project-scoped route takes the project
// from the path).
type askRequestBody struct {
	Query   string `json:"query"`
	Project string `json:"project"`
}

// writeAskError maps an ask service error to an HTTP response.
func writeAskError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrAskProviderUnconfigured):
		WriteError(w, &APIError{
			Code:    "provider_unconfigured",
			Message: "the ask synthesis provider is not configured",
			Status:  http.StatusServiceUnavailable,
		})
	default:
		WriteError(w, ErrInternal("ask failed: "+err.Error()))
	}
}

// NewMeAskHandler returns the user-scoped ask endpoint. With no project in the
// body it synthesizes over the caller's wide aperture (every owned project plus
// global and about_me); with a project slug it narrows to that project plus
// global and about_me.
func NewMeAskHandler(svc AskServicer, users UserReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, ac, ok := decodeAskRequest(w, r)
		if !ok {
			return
		}
		user, err := users.GetByID(r.Context(), ac.UserID)
		if err != nil {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		uid := ac.UserID
		resp, err := svc.Ask(r.Context(), &service.AskRequest{
			Query:            body.Query,
			ProjectSlug:      strings.TrimSpace(body.Project),
			OwnerNamespaceID: user.NamespaceID,
			OrgID:            ac.OrgID,
			UserID:           &uid,
			APIKeyID:         ac.APIKeyID,
		})
		if err != nil {
			writeAskError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

// NewAskHandler returns the project-scoped ask endpoint. It resolves the path
// project_id to its slug and narrows the synthesis to that project plus global
// and about_me.
func NewAskHandler(svc AskServicer, users UserReader, projects AskProjectReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		projectID, err := uuid.Parse(chi.URLParam(r, "project_id"))
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}
		body, ac, ok := decodeAskRequest(w, r)
		if !ok {
			return
		}
		user, err := users.GetByID(r.Context(), ac.UserID)
		if err != nil {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		project, err := projects.GetByID(r.Context(), projectID)
		if err != nil || project == nil {
			WriteError(w, ErrNotFound("project not found"))
			return
		}
		uid := ac.UserID
		resp, err := svc.Ask(r.Context(), &service.AskRequest{
			Query:            body.Query,
			ProjectSlug:      project.Slug,
			OwnerNamespaceID: user.NamespaceID,
			OrgID:            ac.OrgID,
			UserID:           &uid,
			APIKeyID:         ac.APIKeyID,
		})
		if err != nil {
			writeAskError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

// decodeAskRequest parses and validates the shared ask request body and pulls
// the auth context. It writes the error response and returns ok=false on any
// failure.
func decodeAskRequest(w http.ResponseWriter, r *http.Request) (askRequestBody, *auth.AuthContext, bool) {
	var body askRequestBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
		return body, nil, false
	}
	if strings.TrimSpace(body.Query) == "" {
		WriteError(w, ErrBadRequest("query is required"))
		return body, nil, false
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return body, nil, false
	}
	return body, ac, true
}
