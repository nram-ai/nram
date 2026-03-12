package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// OAuthAdminStore abstracts storage operations for the OAuth admin API.
type OAuthAdminStore interface {
	ListAllClients(ctx context.Context) ([]model.OAuthClient, error)
	CreateClient(ctx context.Context, name string, redirectURIs []string, clientType string) (*model.OAuthClient, string, error)
	DeleteClient(ctx context.Context, id uuid.UUID) error
	ListIdPs(ctx context.Context) ([]model.OAuthIdPConfig, error)
	CreateIdP(ctx context.Context, req CreateIdPRequest) (*model.OAuthIdPConfig, error)
	DeleteIdP(ctx context.Context, id uuid.UUID) error
}

// OAuthAdminConfig holds the dependencies for the OAuth admin handler.
type OAuthAdminConfig struct {
	Store OAuthAdminStore
}

// CreateIdPRequest is the parsed request body for POST /oauth/idp.
type CreateIdPRequest struct {
	OrgID          string   `json:"org_id"`
	ProviderType   string   `json:"provider_type"`
	ClientID       string   `json:"client_id"`
	ClientSecret   string   `json:"client_secret"`
	IssuerURL      *string  `json:"issuer_url"`
	AllowedDomains []string `json:"allowed_domains"`
	AutoProvision  bool     `json:"auto_provision"`
}

// oauthClientResponse is the JSON shape returned to the UI for an OAuth client.
type oauthClientResponse struct {
	ID           uuid.UUID `json:"id"`
	Name         string    `json:"name"`
	ClientID     string    `json:"client_id"`
	Type         string    `json:"type"`
	ClientType   string    `json:"client_type"`
	RedirectURIs []string  `json:"redirect_uris"`
	CreatedAt    string    `json:"created_at"`
}

// oauthClientCreatedResponse includes the client_secret (only for confidential).
type oauthClientCreatedResponse struct {
	ID           uuid.UUID `json:"id"`
	Name         string    `json:"name"`
	ClientID     string    `json:"client_id"`
	Type         string    `json:"type"`
	ClientType   string    `json:"client_type"`
	RedirectURIs []string  `json:"redirect_uris"`
	CreatedAt    string    `json:"created_at"`
	ClientSecret *string   `json:"client_secret,omitempty"`
}

// oauthCreateClientRequest is the request body for POST /oauth/clients.
type oauthCreateClientRequest struct {
	Name         string   `json:"name"`
	RedirectURIs []string `json:"redirect_uris"`
	ClientType   string   `json:"client_type"`
}

// NewAdminOAuthHandler returns an http.HandlerFunc that dispatches OAuth
// admin requests based on method and sub-path under /oauth.
//
// Routes:
//   - GET    /oauth/clients       — list all OAuth clients
//   - POST   /oauth/clients       — create client
//   - DELETE  /oauth/clients/{id}  — delete client by PK
//   - GET    /oauth/idp           — list IdP configs
//   - POST   /oauth/idp           — create IdP config
//   - DELETE  /oauth/idp/{id}      — delete IdP config
func NewAdminOAuthHandler(cfg OAuthAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resource, id := extractOAuthSubPath(r.URL.Path)

		switch resource {
		case "clients":
			if id == "" {
				switch r.Method {
				case http.MethodGet:
					handleAdminListOAuthClients(w, r, cfg)
				case http.MethodPost:
					handleAdminCreateOAuthClient(w, r, cfg)
				default:
					WriteError(w, ErrBadRequest("method not allowed"))
				}
				return
			}
			parsed, err := uuid.Parse(id)
			if err != nil {
				WriteError(w, ErrBadRequest("invalid client id"))
				return
			}
			if r.Method == http.MethodDelete {
				handleDeleteOAuthClient(w, r, cfg, parsed)
				return
			}
			WriteError(w, ErrBadRequest("method not allowed"))

		case "idp":
			if id == "" {
				switch r.Method {
				case http.MethodGet:
					handleListIdPs(w, r, cfg)
				case http.MethodPost:
					handleCreateIdP(w, r, cfg)
				default:
					WriteError(w, ErrBadRequest("method not allowed"))
				}
				return
			}
			parsed, err := uuid.Parse(id)
			if err != nil {
				WriteError(w, ErrBadRequest("invalid idp id"))
				return
			}
			if r.Method == http.MethodDelete {
				handleDeleteIdP(w, r, cfg, parsed)
				return
			}
			WriteError(w, ErrBadRequest("method not allowed"))

		default:
			WriteError(w, ErrBadRequest("unknown oauth sub-path"))
		}
	}
}

// extractOAuthSubPath parses the URL path after "/oauth" into a resource
// segment ("clients" or "idp") and an optional ID segment.
func extractOAuthSubPath(path string) (resource, id string) {
	const marker = "/oauth"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return "", ""
	}
	rest := path[idx+len(marker):]
	rest = strings.TrimPrefix(rest, "/")
	if rest == "" {
		return "", ""
	}
	parts := strings.SplitN(rest, "/", 3)
	resource = parts[0]
	if len(parts) > 1 {
		id = parts[1]
	}
	return resource, id
}

// toClientResponse converts a model.OAuthClient to the UI-expected JSON shape.
func toClientResponse(c model.OAuthClient) oauthClientResponse {
	clientType := "confidential"
	if c.ClientSecret == nil || *c.ClientSecret == "" {
		clientType = "public"
	}
	regType := "manual"
	if c.AutoRegistered {
		regType = "auto"
	}
	return oauthClientResponse{
		ID:           c.ID,
		Name:         c.Name,
		ClientID:     c.ClientID,
		Type:         regType,
		ClientType:   clientType,
		RedirectURIs: c.RedirectURIs,
		CreatedAt:    c.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
}

// handleAdminListOAuthClients handles GET /oauth/clients.
func handleAdminListOAuthClients(w http.ResponseWriter, r *http.Request, cfg OAuthAdminConfig) {
	clients, err := cfg.Store.ListAllClients(r.Context())
	if err != nil {
		WriteError(w, mapOAuthError(err))
		return
	}

	result := make([]oauthClientResponse, len(clients))
	for i, c := range clients {
		result[i] = toClientResponse(c)
	}

	writeJSON(w, http.StatusOK, result)
}

// handleAdminCreateOAuthClient handles POST /oauth/clients.
func handleAdminCreateOAuthClient(w http.ResponseWriter, r *http.Request, cfg OAuthAdminConfig) {
	var body oauthCreateClientRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	body.Name = strings.TrimSpace(body.Name)
	if body.Name == "" {
		WriteError(w, ErrBadRequest("name is required"))
		return
	}

	if body.ClientType == "" {
		body.ClientType = "confidential"
	}
	if body.ClientType != "public" && body.ClientType != "confidential" {
		WriteError(w, ErrBadRequest("client_type must be public or confidential"))
		return
	}

	if body.RedirectURIs == nil {
		body.RedirectURIs = []string{}
	}

	client, secret, err := cfg.Store.CreateClient(r.Context(), body.Name, body.RedirectURIs, body.ClientType)
	if err != nil {
		WriteError(w, mapOAuthError(err))
		return
	}

	cr := toClientResponse(*client)
	resp := oauthClientCreatedResponse{
		ID:           cr.ID,
		Name:         cr.Name,
		ClientID:     cr.ClientID,
		Type:         cr.Type,
		ClientType:   body.ClientType,
		RedirectURIs: cr.RedirectURIs,
		CreatedAt:    cr.CreatedAt,
	}
	if body.ClientType == "confidential" && secret != "" {
		resp.ClientSecret = &secret
	}

	writeJSON(w, http.StatusCreated, resp)
}

// handleDeleteOAuthClient handles DELETE /oauth/clients/{id}.
func handleDeleteOAuthClient(w http.ResponseWriter, r *http.Request, cfg OAuthAdminConfig, id uuid.UUID) {
	if err := cfg.Store.DeleteClient(r.Context(), id); err != nil {
		WriteError(w, mapOAuthError(err))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleListIdPs handles GET /oauth/idp.
func handleListIdPs(w http.ResponseWriter, r *http.Request, cfg OAuthAdminConfig) {
	idps, err := cfg.Store.ListIdPs(r.Context())
	if err != nil {
		WriteError(w, mapOAuthError(err))
		return
	}

	writeJSON(w, http.StatusOK, idps)
}

// handleCreateIdP handles POST /oauth/idp.
func handleCreateIdP(w http.ResponseWriter, r *http.Request, cfg OAuthAdminConfig) {
	var body CreateIdPRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	body.ProviderType = strings.TrimSpace(body.ProviderType)
	if body.ProviderType == "" {
		WriteError(w, ErrBadRequest("provider_type is required"))
		return
	}

	body.ClientID = strings.TrimSpace(body.ClientID)
	if body.ClientID == "" {
		WriteError(w, ErrBadRequest("client_id is required"))
		return
	}

	body.ClientSecret = strings.TrimSpace(body.ClientSecret)
	if body.ClientSecret == "" {
		WriteError(w, ErrBadRequest("client_secret is required"))
		return
	}

	if body.AllowedDomains == nil {
		body.AllowedDomains = []string{}
	}

	idp, err := cfg.Store.CreateIdP(r.Context(), body)
	if err != nil {
		WriteError(w, mapOAuthError(err))
		return
	}

	writeJSON(w, http.StatusCreated, idp)
}

// handleDeleteIdP handles DELETE /oauth/idp/{id}.
func handleDeleteIdP(w http.ResponseWriter, r *http.Request, cfg OAuthAdminConfig, id uuid.UUID) {
	if err := cfg.Store.DeleteIdP(r.Context(), id); err != nil {
		WriteError(w, mapOAuthError(err))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// mapOAuthError maps store errors to appropriate API errors.
func mapOAuthError(err error) *APIError {
	msg := err.Error()
	if strings.Contains(msg, "not found") {
		return ErrNotFound(msg)
	}
	return ErrInternal(msg)
}
