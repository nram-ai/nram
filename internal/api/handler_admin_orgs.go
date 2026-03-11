package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// OrgStore abstracts storage for admin org operations.
type OrgStore interface {
	ListOrgs(ctx context.Context) ([]model.Organization, error)
	CreateOrg(ctx context.Context, name, slug string) (*model.Organization, error)
	GetOrg(ctx context.Context, id uuid.UUID) (*model.Organization, error)
	UpdateOrg(ctx context.Context, id uuid.UUID, name, slug string, settings json.RawMessage) (*model.Organization, error)
	DeleteOrg(ctx context.Context, id uuid.UUID) error
}

// OrgAdminConfig holds the dependencies for the admin orgs handler.
type OrgAdminConfig struct {
	Store OrgStore
}

// createOrgRequest is the JSON body for POST /v1/admin/orgs.
type createOrgRequest struct {
	Name string `json:"name"`
	Slug string `json:"slug"`
}

// updateOrgRequest is the JSON body for PUT /v1/admin/orgs/{id}.
type updateOrgRequest struct {
	Name     string          `json:"name"`
	Slug     string          `json:"slug"`
	Settings json.RawMessage `json:"settings"`
}

// NewAdminOrgsHandler returns an http.HandlerFunc that handles admin
// organization CRUD operations. It dispatches internally based on HTTP
// method and URL path.
func NewAdminOrgsHandler(cfg OrgAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract sub-path after "/orgs".
		path := r.URL.Path
		idx := strings.LastIndex(path, "/orgs")
		sub := ""
		if idx >= 0 {
			sub = path[idx+5:] // after "/orgs"
		}
		sub = strings.TrimPrefix(sub, "/")

		if sub == "" {
			// Collection routes: GET (list) or POST (create).
			switch r.Method {
			case http.MethodGet:
				handleListOrgs(w, r, cfg.Store)
			case http.MethodPost:
				handleCreateOrg(w, r, cfg.Store)
			default:
				WriteError(w, ErrBadRequest("method not allowed"))
			}
			return
		}

		// Item routes: GET, PUT, DELETE on /orgs/{id}.
		id, err := uuid.Parse(sub)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid organization id"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleGetOrg(w, r, cfg.Store, id)
		case http.MethodPut:
			handleUpdateOrg(w, r, cfg.Store, id)
		case http.MethodDelete:
			handleDeleteOrg(w, r, cfg.Store, id)
		default:
			WriteError(w, ErrBadRequest("method not allowed"))
		}
	}
}

// isOrgNotFound returns true if the error represents a not-found condition.
func isOrgNotFound(err error) bool {
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	return strings.Contains(err.Error(), "not found")
}

func handleListOrgs(w http.ResponseWriter, r *http.Request, store OrgStore) {
	orgs, err := store.ListOrgs(r.Context())
	if err != nil {
		WriteError(w, ErrInternal("failed to list organizations"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"data": orgs,
	})
}

func handleCreateOrg(w http.ResponseWriter, r *http.Request, store OrgStore) {
	var body createOrgRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body"))
		return
	}

	if strings.TrimSpace(body.Name) == "" {
		WriteError(w, ErrBadRequest("name is required"))
		return
	}
	if strings.TrimSpace(body.Slug) == "" {
		WriteError(w, ErrBadRequest("slug is required"))
		return
	}

	org, err := store.CreateOrg(r.Context(), body.Name, body.Slug)
	if err != nil {
		WriteError(w, ErrInternal("failed to create organization"))
		return
	}

	writeJSON(w, http.StatusCreated, org)
}

func handleGetOrg(w http.ResponseWriter, r *http.Request, store OrgStore, id uuid.UUID) {
	org, err := store.GetOrg(r.Context(), id)
	if err != nil {
		if isOrgNotFound(err) {
			WriteError(w, ErrNotFound("organization not found"))
			return
		}
		WriteError(w, ErrInternal("failed to get organization"))
		return
	}

	writeJSON(w, http.StatusOK, org)
}

func handleUpdateOrg(w http.ResponseWriter, r *http.Request, store OrgStore, id uuid.UUID) {
	var body updateOrgRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body"))
		return
	}

	org, err := store.UpdateOrg(r.Context(), id, body.Name, body.Slug, body.Settings)
	if err != nil {
		if isOrgNotFound(err) {
			WriteError(w, ErrNotFound("organization not found"))
			return
		}
		WriteError(w, ErrInternal("failed to update organization"))
		return
	}

	writeJSON(w, http.StatusOK, org)
}

func handleDeleteOrg(w http.ResponseWriter, r *http.Request, store OrgStore, id uuid.UUID) {
	err := store.DeleteOrg(r.Context(), id)
	if err != nil {
		if isOrgNotFound(err) {
			WriteError(w, ErrNotFound("organization not found"))
			return
		}
		WriteError(w, ErrInternal("failed to delete organization"))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
