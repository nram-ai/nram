package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// ShareTokenManager is the subset of *service.ShareTokenService consumed by
// the self-service share handlers. Defined as an interface so tests can pass
// a stub.
type ShareTokenManager interface {
	Create(ctx context.Context, req service.CreateShareRequest) (*service.CreateShareResult, error)
	ListByOwner(ctx context.Context, ownerUserID uuid.UUID) ([]model.ShareToken, error)
	ListGrants(ctx context.Context, shareID uuid.UUID) ([]model.ShareTokenGrant, error)
	ListGrantsByOwner(ctx context.Context, ownerUserID uuid.UUID) (map[uuid.UUID][]model.ShareTokenGrant, error)
	ListBindings(ctx context.Context, shareID uuid.UUID) ([]model.OAuthClient, error)
	SetGrants(ctx context.Context, ownerUserID, shareID uuid.UUID, grants []model.ShareTokenGrant) error
	Revoke(ctx context.Context, ownerUserID, shareID uuid.UUID) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.ShareToken, error)
}

// ProjectOwnership resolves the owning namespace of a project. The share
// handlers use it to verify that every grant in a create/edit request points
// at a project the caller actually owns — without this check, a user could
// mint shares listing arbitrary project UUIDs (foreign tenants, deleted
// projects, random UUIDs) and pollute their own UI / leak project IDs.
type ProjectOwnership interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// UserNamespaceLookup resolves the caller's owning namespace so the handlers
// can compare it against the namespace of each grant's project.
type UserNamespaceLookup interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.User, error)
}

// shareCreateRequest is the POST /v1/me/shares body. Grants are required;
// an empty list is rejected at the service layer.
type shareCreateRequest struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	IsOneShot   bool              `json:"is_one_shot,omitempty"`
	ExpiresAt   time.Time         `json:"expires_at"`
	Grants      []shareGrantInput `json:"grants"`
}

// shareGrantInput pairs a project_id with a permission tier on the wire.
type shareGrantInput struct {
	ProjectID  uuid.UUID `json:"project_id"`
	Permission string    `json:"permission"`
}

// shareCreateResponse is returned on create. Secret is the wire-format
// nram_s_<hex> string; the recipient must capture it on the create
// response because it is never re-issued.
type shareCreateResponse struct {
	Share  shareDetailResponse `json:"share"`
	Secret string              `json:"secret"`
}

// shareDetailResponse is the JSON representation of a share + its grants.
// Used by the list endpoint (omitting Bindings), the detail endpoint (with
// Bindings populated), and the create response.
type shareDetailResponse struct {
	ID          uuid.UUID                 `json:"id"`
	Name        string                    `json:"name"`
	Description string                    `json:"description,omitempty"`
	TokenPrefix string                    `json:"token_prefix"`
	IsOneShot   bool                      `json:"is_one_shot"`
	ExpiresAt   time.Time                 `json:"expires_at"`
	ConsumedAt  *time.Time                `json:"consumed_at,omitempty"`
	CreatedAt   time.Time                 `json:"created_at"`
	LastUsedAt  *time.Time                `json:"last_used_at,omitempty"`
	UseCount    int                       `json:"use_count"`
	RevokedAt   *time.Time                `json:"revoked_at,omitempty"`
	Grants      []model.ShareTokenGrant   `json:"grants"`
	Bindings    []shareDetailBindingEntry `json:"bindings,omitempty"`
}

// shareDetailBindingEntry is the per-OAuth-client subset surfaced on the
// detail endpoint. Secrets are never returned.
type shareDetailBindingEntry struct {
	ID        uuid.UUID `json:"id"`
	ClientID  string    `json:"client_id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// NewMeSharesHandler returns an http.HandlerFunc for GET (list) and POST
// (create) at /v1/me/shares. projects and users are used to validate that
// every grant in a create body points at a project owned by the caller.
func NewMeSharesHandler(mgr ShareTokenManager, projects ProjectOwnership, users UserNamespaceLookup) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleListShares(w, r, mgr)
		case http.MethodPost:
			handleCreateShare(w, r, mgr, projects, users)
		default:
			WriteError(w, ErrBadRequest("method not allowed"))
		}
	}
}

func handleListShares(w http.ResponseWriter, r *http.Request, mgr ShareTokenManager) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrForbidden("unauthorized"))
		return
	}
	shares, err := mgr.ListByOwner(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to list shares"))
		return
	}
	grantsByShare, err := mgr.ListGrantsByOwner(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to list share grants"))
		return
	}
	out := make([]shareDetailResponse, 0, len(shares))
	for _, s := range shares {
		out = append(out, toShareDetail(s, grantsByShare[s.ID], nil))
	}
	writeJSON(w, http.StatusOK, map[string]any{"shares": out})
}

func handleCreateShare(w http.ResponseWriter, r *http.Request, mgr ShareTokenManager, projects ProjectOwnership, users UserNamespaceLookup) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrForbidden("unauthorized"))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, shareRequestMaxBodyBytes)
	var body shareCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}
	if strings.TrimSpace(body.Name) == "" {
		WriteError(w, ErrBadRequest("name is required"))
		return
	}
	if body.ExpiresAt.IsZero() {
		WriteError(w, ErrBadRequest("expires_at is required"))
		return
	}
	if len(body.Grants) == 0 {
		WriteError(w, ErrBadRequest("at least one grant is required"))
		return
	}

	caller, uerr := users.GetByID(r.Context(), ac.UserID)
	if uerr != nil {
		WriteError(w, ErrInternal("failed to resolve caller namespace"))
		return
	}

	grants := make([]model.ShareTokenGrant, 0, len(body.Grants))
	seenProjects := make(map[uuid.UUID]struct{}, len(body.Grants))
	for _, g := range body.Grants {
		perm := model.SharePermission(g.Permission)
		if !perm.Valid() {
			WriteError(w, ErrBadRequest("invalid permission "+g.Permission))
			return
		}
		if _, dup := seenProjects[g.ProjectID]; dup {
			WriteError(w, ErrBadRequest("duplicate project_id in grants"))
			return
		}
		seenProjects[g.ProjectID] = struct{}{}
		project, perr := projects.GetByID(r.Context(), g.ProjectID)
		if perr != nil {
			WriteError(w, ErrBadRequest("unknown project_id "+g.ProjectID.String()))
			return
		}
		if project.OwnerNamespaceID != caller.NamespaceID {
			WriteError(w, ErrForbidden("project "+g.ProjectID.String()+" is not owned by the caller"))
			return
		}
		grants = append(grants, model.ShareTokenGrant{
			ProjectID:  g.ProjectID,
			Permission: perm,
		})
	}

	result, err := mgr.Create(r.Context(), service.CreateShareRequest{
		OwnerUserID: ac.UserID,
		Name:        body.Name,
		Description: body.Description,
		IsOneShot:   body.IsOneShot,
		ExpiresAt:   body.ExpiresAt,
		Grants:      grants,
	})
	if err != nil {
		WriteError(w, ErrBadRequest(err.Error()))
		return
	}

	writeJSON(w, http.StatusCreated, shareCreateResponse{
		Share:  toShareDetail(*result.Share, grants, nil),
		Secret: result.RawSecret,
	})
}

// shareRequestMaxBodyBytes bounds POST/PATCH bodies on the self-service
// share endpoints so an authenticated client cannot exhaust the request
// goroutine by streaming a multi-gigabyte grants payload.
const shareRequestMaxBodyBytes int64 = 1 << 20 // 1 MiB

// NewMeShareItemHandler returns an http.HandlerFunc for GET (detail), PATCH
// (replace grants), and DELETE (revoke) at /v1/me/shares/{id}.
func NewMeShareItemHandler(mgr ShareTokenManager, projects ProjectOwnership, users UserNamespaceLookup) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrForbidden("unauthorized"))
			return
		}
		idStr := chi.URLParam(r, "id")
		shareID, err := uuid.Parse(idStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid share id"))
			return
		}

		share, err := mgr.GetByID(r.Context(), shareID)
		if err != nil {
			WriteError(w, ErrNotFound("share not found"))
			return
		}
		if share.OwnerUserID != ac.UserID {
			WriteError(w, ErrForbidden("share not owned by caller"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			grants, gerr := mgr.ListGrants(r.Context(), shareID)
			if gerr != nil {
				WriteError(w, ErrInternal("failed to load grants"))
				return
			}
			bindings, berr := mgr.ListBindings(r.Context(), shareID)
			if berr != nil {
				WriteError(w, ErrInternal("failed to load bindings"))
				return
			}
			writeJSON(w, http.StatusOK, toShareDetail(*share, grants, bindings))
		case http.MethodPatch:
			handlePatchShareGrants(w, r, mgr, projects, users, ac.UserID, shareID)
		case http.MethodDelete:
			if err := mgr.Revoke(r.Context(), ac.UserID, shareID); err != nil {
				WriteError(w, ErrInternal("failed to revoke share"))
				return
			}
			writeJSON(w, http.StatusOK, map[string]bool{"revoked": true})
		default:
			WriteError(w, ErrBadRequest("method not allowed"))
		}
	}
}

func handlePatchShareGrants(w http.ResponseWriter, r *http.Request, mgr ShareTokenManager, projects ProjectOwnership, users UserNamespaceLookup, ownerUserID, shareID uuid.UUID) {
	r.Body = http.MaxBytesReader(w, r.Body, shareRequestMaxBodyBytes)
	var body struct {
		Grants []shareGrantInput `json:"grants"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}
	if len(body.Grants) == 0 {
		WriteError(w, ErrBadRequest("at least one grant is required (use DELETE to revoke)"))
		return
	}
	caller, uerr := users.GetByID(r.Context(), ownerUserID)
	if uerr != nil {
		WriteError(w, ErrInternal("failed to resolve caller namespace"))
		return
	}
	grants := make([]model.ShareTokenGrant, 0, len(body.Grants))
	seenProjects := make(map[uuid.UUID]struct{}, len(body.Grants))
	for _, g := range body.Grants {
		perm := model.SharePermission(g.Permission)
		if !perm.Valid() {
			WriteError(w, ErrBadRequest("invalid permission "+g.Permission))
			return
		}
		if _, dup := seenProjects[g.ProjectID]; dup {
			WriteError(w, ErrBadRequest("duplicate project_id in grants"))
			return
		}
		seenProjects[g.ProjectID] = struct{}{}
		project, perr := projects.GetByID(r.Context(), g.ProjectID)
		if perr != nil {
			WriteError(w, ErrBadRequest("unknown project_id "+g.ProjectID.String()))
			return
		}
		if project.OwnerNamespaceID != caller.NamespaceID {
			WriteError(w, ErrForbidden("project "+g.ProjectID.String()+" is not owned by the caller"))
			return
		}
		grants = append(grants, model.ShareTokenGrant{
			ShareTokenID: shareID,
			ProjectID:    g.ProjectID,
			Permission:   perm,
		})
	}
	if err := mgr.SetGrants(r.Context(), ownerUserID, shareID, grants); err != nil {
		if errors.Is(err, service.ErrShareNotOwned) {
			WriteError(w, ErrForbidden("share not owned by caller"))
			return
		}
		WriteError(w, ErrBadRequest(err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"updated": true})
}

// toShareDetail projects a ShareToken + its grants (and optional bindings)
// to the wire shape. Bindings is omitted when nil.
func toShareDetail(s model.ShareToken, grants []model.ShareTokenGrant, bindings []model.OAuthClient) shareDetailResponse {
	out := shareDetailResponse{
		ID:          s.ID,
		Name:        s.Name,
		Description: s.Description,
		TokenPrefix: s.TokenPrefix,
		IsOneShot:   s.IsOneShot,
		ExpiresAt:   s.ExpiresAt,
		ConsumedAt:  s.ConsumedAt,
		CreatedAt:   s.CreatedAt,
		LastUsedAt:  s.LastUsedAt,
		UseCount:    s.UseCount,
		RevokedAt:   s.RevokedAt,
		Grants:      grants,
	}
	if bindings != nil {
		out.Bindings = make([]shareDetailBindingEntry, 0, len(bindings))
		for _, b := range bindings {
			out.Bindings = append(out.Bindings, shareDetailBindingEntry{
				ID:        b.ID,
				ClientID:  b.ClientID,
				Name:      b.Name,
				CreatedAt: b.CreatedAt,
			})
		}
	}
	return out
}
