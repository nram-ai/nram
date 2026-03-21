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
	AllowedDomains []string `json:"allowed_domains,omitempty"`
	AutoProvision  bool     `json:"auto_provision"`
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

	idp := &model.OAuthIdPConfig{
		OrgID:          &orgID,
		ProviderType:   req.ProviderType,
		ClientID:       req.ClientID,
		ClientSecret:   req.ClientSecret,
		IssuerURL:      req.IssuerURL,
		AllowedDomains: req.AllowedDomains,
		AutoProvision:  req.AutoProvision,
	}

	if err := store.CreateIdP(r.Context(), idp); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, idp)
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
