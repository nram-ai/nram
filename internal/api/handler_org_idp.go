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
)

// OrgIdPStore defines the storage operations needed by the org-scoped IdP handler.
type OrgIdPStore interface {
	ListIdPsByOrg(ctx context.Context, orgID uuid.UUID) ([]model.OAuthIdPConfig, error)
	CreateIdP(ctx context.Context, idp *model.OAuthIdPConfig) error
	UpdateIdPByOrg(ctx context.Context, idp *model.OAuthIdPConfig, orgID uuid.UUID) error
	GetIdPByID(ctx context.Context, id uuid.UUID) (*model.OAuthIdPConfig, error)
	DeleteIdPByOrg(ctx context.Context, id, orgID uuid.UUID) error
}

// NewOrgIdPHandler returns an http.HandlerFunc that handles IdP configuration
// for an organization. Org owners can list, create, and delete IdP configs
// scoped to their own organization.
func NewOrgIdPHandler(store OrgIdPStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orgIDStr := chi.URLParam(r, "org_id")
		orgID, err := uuid.Parse(orgIDStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid org_id"})
			return
		}

		// Verify the caller belongs to this org (unless administrator).
		info := auth.FromContext(r.Context())
		if info.Role != string(auth.RoleAdministrator) && info.OrgID != orgID {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
			return
		}

		// Extract sub-path after "/idp".
		subPath := ""
		idx := strings.Index(r.URL.Path, "/idp")
		if idx >= 0 {
			subPath = r.URL.Path[idx+len("/idp"):]
		}
		subPath = strings.TrimSuffix(subPath, "/")

		switch {
		case r.Method == http.MethodGet && subPath == "":
			orgIdPList(w, r, store, orgID)
		case r.Method == http.MethodPost && subPath == "":
			orgIdPCreate(w, r, store, orgID)
		case r.Method == http.MethodPut && subPath != "":
			idStr := strings.TrimPrefix(subPath, "/")
			orgIdPUpdate(w, r, store, orgID, idStr)
		case r.Method == http.MethodDelete && subPath != "":
			idStr := strings.TrimPrefix(subPath, "/")
			orgIdPDelete(w, r, store, orgID, idStr)
		default:
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		}
	}
}

func orgIdPList(w http.ResponseWriter, r *http.Request, store OrgIdPStore, orgID uuid.UUID) {
	configs, err := store.ListIdPsByOrg(r.Context(), orgID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, configs)
}

type orgCreateIdPRequest struct {
	ProviderType   string   `json:"provider_type"`
	ClientID       string   `json:"client_id"`
	ClientSecret   string   `json:"client_secret"`
	IssuerURL      *string  `json:"issuer_url,omitempty"`
	AuthorizeURL   *string  `json:"authorize_url,omitempty"`
	TokenURL       *string  `json:"token_url,omitempty"`
	UserinfoURL    *string  `json:"userinfo_url,omitempty"`
	AllowedDomains []string `json:"allowed_domains,omitempty"`
	AutoProvision  bool     `json:"auto_provision"`
	DefaultRole    string   `json:"default_role,omitempty"`
}

func orgIdPCreate(w http.ResponseWriter, r *http.Request, store OrgIdPStore, orgID uuid.UUID) {
	var req orgCreateIdPRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	// Validate required fields.
	if req.ProviderType != "oidc" && req.ProviderType != "saml" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provider_type must be 'oidc' or 'saml'"})
		return
	}
	if req.ClientID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "client_id is required"})
		return
	}
	if req.ClientSecret == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "client_secret is required"})
		return
	}

	defaultRole := req.DefaultRole
	if defaultRole == "" {
		defaultRole = "member"
	}

	idp := &model.OAuthIdPConfig{
		OrgID:          &orgID,
		ProviderType:   req.ProviderType,
		ClientID:       req.ClientID,
		ClientSecret:   req.ClientSecret,
		IssuerURL:      req.IssuerURL,
		AuthorizeURL:   req.AuthorizeURL,
		TokenURL:       req.TokenURL,
		UserinfoURL:    req.UserinfoURL,
		AllowedDomains: req.AllowedDomains,
		AutoProvision:  req.AutoProvision,
		DefaultRole:    defaultRole,
	}

	if err := store.CreateIdP(r.Context(), idp); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, idp)
}

func orgIdPUpdate(w http.ResponseWriter, r *http.Request, store OrgIdPStore, orgID uuid.UUID, idStr string) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}

	var body UpdateIdPRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	idp, err := store.GetIdPByID(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "idp config not found"})
		return
	}

	// Verify it belongs to the caller's org.
	if idp.OrgID == nil || *idp.OrgID != orgID {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "idp config not found"})
		return
	}

	if body.ClientID != nil {
		idp.ClientID = *body.ClientID
	}
	if body.ClientSecret != nil {
		idp.ClientSecret = *body.ClientSecret
	}
	if body.IssuerURL != nil {
		idp.IssuerURL = body.IssuerURL
	}
	if body.AuthorizeURL != nil {
		idp.AuthorizeURL = body.AuthorizeURL
	}
	if body.TokenURL != nil {
		idp.TokenURL = body.TokenURL
	}
	if body.UserinfoURL != nil {
		idp.UserinfoURL = body.UserinfoURL
	}
	if body.AllowedDomains != nil {
		idp.AllowedDomains = body.AllowedDomains
	}
	if body.AutoProvision != nil {
		idp.AutoProvision = *body.AutoProvision
	}
	if body.DefaultRole != nil {
		idp.DefaultRole = *body.DefaultRole
	}

	if err := store.UpdateIdPByOrg(r.Context(), idp, orgID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "idp config not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, idp)
}

func orgIdPDelete(w http.ResponseWriter, r *http.Request, store OrgIdPStore, orgID uuid.UUID, idStr string) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}

	if err := store.DeleteIdPByOrg(r.Context(), id, orgID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "idp config not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
