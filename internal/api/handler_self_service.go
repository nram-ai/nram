package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"context"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// APIKeyManager defines operations on API keys needed by self-service handlers.
type APIKeyManager interface {
	Create(ctx context.Context, key *model.APIKey) (string, error)
	ListByUser(ctx context.Context, userID uuid.UUID) ([]model.APIKey, error)
	Revoke(ctx context.Context, id uuid.UUID) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.APIKey, error)
}

// OAuthClientManager defines operations on OAuth clients needed by self-service handlers.
type OAuthClientManager interface {
	CreateClient(ctx context.Context, client *model.OAuthClient) error
	ListClientsByUser(ctx context.Context, userID uuid.UUID) ([]model.OAuthClient, error)
	DeleteClient(ctx context.Context, clientID string) error
	GetClientByID(ctx context.Context, clientID string) (*model.OAuthClient, error)
}

// createAPIKeyRequest is the JSON body for POST /v1/me/api-keys.
type createAPIKeyRequest struct {
	Name      string      `json:"name"`
	Scopes    []uuid.UUID `json:"scopes"`
	ExpiresAt *time.Time  `json:"expires_at"`
}

// createAPIKeyResponse is returned on successful API key creation.
type createAPIKeyResponse struct {
	ID        uuid.UUID `json:"id"`
	Key       string    `json:"key"`
	KeyPrefix string    `json:"key_prefix"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// createOAuthClientRequest is the JSON body for POST /v1/me/oauth-clients.
type createOAuthClientRequest struct {
	Name         string   `json:"name"`
	RedirectURIs []string `json:"redirect_uris"`
	GrantTypes   []string `json:"grant_types"`
}

// createOAuthClientResponse is returned on successful OAuth client creation.
type createOAuthClientResponse struct {
	ID           uuid.UUID `json:"id"`
	ClientID     string    `json:"client_id"`
	ClientSecret *string   `json:"client_secret,omitempty"`
	Name         string    `json:"name"`
	CreatedAt    time.Time `json:"created_at"`
}

// NewMeAPIKeysHandler returns an http.HandlerFunc that handles
// GET /v1/me/api-keys (list) and POST /v1/me/api-keys (create).
func NewMeAPIKeysHandler(keys APIKeyManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleListAPIKeys(w, r, keys)
		case http.MethodPost:
			handleCreateAPIKey(w, r, keys)
		default:
			w.Header().Set("Allow", "GET, POST")
			WriteError(w, &APIError{
				Code:    "method_not_allowed",
				Message: "method not allowed",
				Status:  http.StatusMethodNotAllowed,
			})
		}
	}
}

func handleListAPIKeys(w http.ResponseWriter, r *http.Request, keys APIKeyManager) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}

	result, err := keys.ListByUser(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to list api keys"))
		return
	}

	if result == nil {
		result = []model.APIKey{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"data": result,
	})
}

func handleCreateAPIKey(w http.ResponseWriter, r *http.Request, keys APIKeyManager) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}

	var body createAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
		return
	}

	if strings.TrimSpace(body.Name) == "" {
		WriteError(w, ErrBadRequest("name is required"))
		return
	}

	key := &model.APIKey{
		UserID:    ac.UserID,
		Name:      body.Name,
		Scopes:    body.Scopes,
		ExpiresAt: body.ExpiresAt,
	}

	rawKey, err := keys.Create(r.Context(), key)
	if err != nil {
		WriteError(w, ErrInternal("failed to create api key"))
		return
	}

	writeJSON(w, http.StatusCreated, createAPIKeyResponse{
		ID:        key.ID,
		Key:       rawKey,
		KeyPrefix: key.KeyPrefix,
		Name:      key.Name,
		CreatedAt: key.CreatedAt,
	})
}

// NewMeAPIKeyRevokeHandler returns an http.HandlerFunc that handles
// DELETE /v1/me/api-keys/{id}.
func NewMeAPIKeyRevokeHandler(keys APIKeyManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}

		idStr := chi.URLParam(r, "id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid id: must be a valid UUID"))
			return
		}

		existing, err := keys.GetByID(r.Context(), id)
		if err != nil {
			WriteError(w, ErrNotFound("api key not found"))
			return
		}

		if existing.UserID != ac.UserID {
			WriteError(w, ErrForbidden("api key does not belong to you"))
			return
		}

		if err := keys.Revoke(r.Context(), id); err != nil {
			WriteError(w, ErrInternal("failed to revoke api key"))
			return
		}

		writeJSON(w, http.StatusOK, map[string]bool{"revoked": true})
	}
}

// NewMeOAuthClientsHandler returns an http.HandlerFunc that handles
// GET /v1/me/oauth-clients (list) and POST /v1/me/oauth-clients (create).
func NewMeOAuthClientsHandler(clients OAuthClientManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleListOAuthClients(w, r, clients)
		case http.MethodPost:
			handleCreateOAuthClient(w, r, clients)
		default:
			w.Header().Set("Allow", "GET, POST")
			WriteError(w, &APIError{
				Code:    "method_not_allowed",
				Message: "method not allowed",
				Status:  http.StatusMethodNotAllowed,
			})
		}
	}
}

func handleListOAuthClients(w http.ResponseWriter, r *http.Request, clients OAuthClientManager) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}

	result, err := clients.ListClientsByUser(r.Context(), ac.UserID)
	if err != nil {
		WriteError(w, ErrInternal("failed to list oauth clients"))
		return
	}

	if result == nil {
		result = []model.OAuthClient{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"data": result,
	})
}

func handleCreateOAuthClient(w http.ResponseWriter, r *http.Request, clients OAuthClientManager) {
	ac := auth.FromContext(r.Context())
	if ac == nil {
		WriteError(w, ErrUnauthorized("authentication required"))
		return
	}

	var body createOAuthClientRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
		return
	}

	if strings.TrimSpace(body.Name) == "" {
		WriteError(w, ErrBadRequest("name is required"))
		return
	}

	clientID, err := generateClientID()
	if err != nil {
		WriteError(w, ErrInternal("failed to generate client id"))
		return
	}

	clientSecret, err := generateClientSecret()
	if err != nil {
		WriteError(w, ErrInternal("failed to generate client secret"))
		return
	}

	client := &model.OAuthClient{
		ClientID:     clientID,
		ClientSecret: &clientSecret,
		Name:         body.Name,
		RedirectURIs: body.RedirectURIs,
		GrantTypes:   body.GrantTypes,
		UserID:       &ac.UserID,
		OrgID:        &ac.OrgID,
	}

	if err := clients.CreateClient(r.Context(), client); err != nil {
		WriteError(w, ErrInternal("failed to create oauth client"))
		return
	}

	writeJSON(w, http.StatusCreated, createOAuthClientResponse{
		ID:           client.ID,
		ClientID:     client.ClientID,
		ClientSecret: &clientSecret,
		Name:         client.Name,
		CreatedAt:    client.CreatedAt,
	})
}

// NewMeOAuthClientRevokeHandler returns an http.HandlerFunc that handles
// DELETE /v1/me/oauth-clients/{id}.
func NewMeOAuthClientRevokeHandler(clients OAuthClientManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}

		idStr := chi.URLParam(r, "id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid id: must be a valid UUID"))
			return
		}

		// Look up all clients for this user and verify ownership by matching the UUID.
		userClients, err := clients.ListClientsByUser(r.Context(), ac.UserID)
		if err != nil {
			WriteError(w, ErrInternal("failed to verify oauth client ownership"))
			return
		}

		var target *model.OAuthClient
		for i := range userClients {
			if userClients[i].ID == id {
				target = &userClients[i]
				break
			}
		}

		if target == nil {
			WriteError(w, ErrNotFound("oauth client not found"))
			return
		}

		if err := clients.DeleteClient(r.Context(), target.ClientID); err != nil {
			WriteError(w, ErrInternal("failed to delete oauth client"))
			return
		}

		writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
	}
}

// PasswordChanger defines operations needed for self-service password change.
type PasswordChanger interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.User, error)
	UpdatePassword(ctx context.Context, id uuid.UUID, newHash string) error
}

// changePasswordRequest is the JSON body for POST /v1/me/password.
type changePasswordRequest struct {
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
}

// NewMeChangePasswordHandler returns an http.HandlerFunc that handles
// POST /v1/me/password (self-service password change).
func NewMeChangePasswordHandler(repo PasswordChanger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			WriteError(w, &APIError{
				Code:    "method_not_allowed",
				Message: "method not allowed",
				Status:  http.StatusMethodNotAllowed,
			})
			return
		}

		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}

		var body changePasswordRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid request body: "+err.Error()))
			return
		}

		if body.CurrentPassword == "" {
			WriteError(w, ErrBadRequest("current_password is required"))
			return
		}

		if len(body.NewPassword) < 8 {
			WriteError(w, ErrBadRequest("new_password must be at least 8 characters"))
			return
		}

		user, err := repo.GetByID(r.Context(), ac.UserID)
		if err != nil {
			WriteError(w, ErrInternal("failed to load user"))
			return
		}

		if user.PasswordHash == nil {
			WriteError(w, ErrBadRequest("account does not have a password set (external identity provider)"))
			return
		}

		if !storage.VerifyPassword(*user.PasswordHash, body.CurrentPassword) {
			WriteError(w, ErrBadRequest("current password is incorrect"))
			return
		}

		newHash, err := storage.HashPassword(body.NewPassword)
		if err != nil {
			WriteError(w, ErrInternal("failed to hash new password"))
			return
		}

		if err := repo.UpdatePassword(r.Context(), ac.UserID, newHash); err != nil {
			WriteError(w, ErrInternal("failed to update password"))
			return
		}

		writeJSON(w, http.StatusOK, map[string]bool{"changed": true})
	}
}

// generateClientID creates a random client ID with the "nram_" prefix.
func generateClientID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "nram_" + hex.EncodeToString(b), nil
}

// generateClientSecret creates a random client secret.
func generateClientSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
