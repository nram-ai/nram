package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// UserAdminStore abstracts storage for admin user operations.
type UserAdminStore interface {
	ListUsers(ctx context.Context) ([]model.User, error)
	CreateUser(ctx context.Context, email, displayName, password, role string, orgID uuid.UUID) (*model.User, error)
	GetUser(ctx context.Context, id uuid.UUID) (*model.User, error)
	UpdateUser(ctx context.Context, id uuid.UUID, displayName, role string, settings json.RawMessage) (*model.User, error)
	DeleteUser(ctx context.Context, id uuid.UUID) error
	CountAdmins(ctx context.Context) (int, error)
	ListAPIKeys(ctx context.Context, userID uuid.UUID) ([]model.APIKey, error)
	GenerateAPIKey(ctx context.Context, userID uuid.UUID, name string, scopes []uuid.UUID, expiresAt *time.Time) (*model.APIKey, string, error)
	RevokeAPIKey(ctx context.Context, keyID uuid.UUID) error
}

// UserAdminConfig holds the dependencies for the admin users handler.
type UserAdminConfig struct {
	Store UserAdminStore
}

// createUserRequest is the JSON body for POST /v1/admin/users.
type createUserRequest struct {
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	Password    string `json:"password"`
	Role        string `json:"role"`
	OrgID       string `json:"org_id"`
}

// updateUserRequest is the JSON body for PUT /v1/admin/users/{id}.
type updateUserRequest struct {
	DisplayName string          `json:"display_name"`
	Role        string          `json:"role"`
	Settings    json.RawMessage `json:"settings"`
}

// generateAPIKeyRequest is the JSON body for POST /v1/admin/users/{id}/api-keys.
type generateAPIKeyRequest struct {
	Name      string      `json:"name"`
	Scopes    []uuid.UUID `json:"scopes"`
	ExpiresAt *time.Time  `json:"expires_at"`
}

// generateAPIKeyResponse is returned on successful admin API key generation.
type generateAPIKeyResponse struct {
	APIKey *model.APIKey `json:"api_key"`
	RawKey string        `json:"raw_key"`
}

// validUserRoles contains the set of valid role values for user creation and update.
var validUserRoles = map[string]bool{
	"administrator": true,
	"org_owner":     true,
	"member":        true,
	"readonly":      true,
	"service":       true,
}

// NewAdminUsersHandler returns an http.HandlerFunc that handles admin
// user CRUD operations and user API key management. It dispatches
// internally based on HTTP method and URL sub-path.
func NewAdminUsersHandler(cfg UserAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract sub-path after "/users".
		path := r.URL.Path
		idx := strings.LastIndex(path, "/users")
		sub := ""
		if idx >= 0 {
			sub = path[idx+6:] // after "/users"
		}
		sub = strings.TrimPrefix(sub, "/")

		// "" → collection (GET list, POST create)
		if sub == "" {
			switch r.Method {
			case http.MethodGet:
				handleAdminListUsers(w, r, cfg.Store)
			case http.MethodPost:
				handleAdminCreateUser(w, r, cfg.Store)
			default:
				WriteError(w, ErrBadRequest("method not allowed"))
			}
			return
		}

		// Split sub-path: could be "{id}", "{id}/api-keys", or "{id}/api-keys/{key_id}"
		parts := strings.SplitN(sub, "/", 4)

		userID, err := uuid.Parse(parts[0])
		if err != nil {
			WriteError(w, ErrBadRequest("invalid user id"))
			return
		}

		// "{id}" → GET, PUT, DELETE user
		if len(parts) == 1 {
			switch r.Method {
			case http.MethodGet:
				handleAdminGetUser(w, r, cfg.Store, userID)
			case http.MethodPut:
				handleAdminUpdateUser(w, r, cfg.Store, userID)
			case http.MethodDelete:
				handleAdminDeleteUser(w, r, cfg.Store, userID)
			default:
				WriteError(w, ErrBadRequest("method not allowed"))
			}
			return
		}

		// Expect "api-keys" as the second segment.
		if parts[1] != "api-keys" {
			WriteError(w, ErrNotFound("not found"))
			return
		}

		// "{id}/api-keys" → GET list keys, POST generate key
		if len(parts) == 2 {
			switch r.Method {
			case http.MethodGet:
				handleAdminListAPIKeys(w, r, cfg.Store, userID)
			case http.MethodPost:
				handleAdminGenerateAPIKey(w, r, cfg.Store, userID)
			default:
				WriteError(w, ErrBadRequest("method not allowed"))
			}
			return
		}

		// "{id}/api-keys/{key_id}" → DELETE revoke key
		if len(parts) == 3 {
			keyID, err := uuid.Parse(parts[2])
			if err != nil {
				WriteError(w, ErrBadRequest("invalid api key id"))
				return
			}

			if r.Method == http.MethodDelete {
				handleAdminRevokeAPIKey(w, r, cfg.Store, keyID)
				return
			}
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		WriteError(w, ErrNotFound("not found"))
	}
}

// isUserNotFound returns true if the error represents a not-found condition.
func isUserNotFound(err error) bool {
	if errors.Is(err, sql.ErrNoRows) {
		return true
	}
	return strings.Contains(err.Error(), "not found")
}

func handleAdminListUsers(w http.ResponseWriter, r *http.Request, store UserAdminStore) {
	users, err := store.ListUsers(r.Context())
	if err != nil {
		WriteError(w, ErrInternal("failed to list users"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"data": users,
	})
}

func handleAdminCreateUser(w http.ResponseWriter, r *http.Request, store UserAdminStore) {
	var body createUserRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body"))
		return
	}

	if strings.TrimSpace(body.Email) == "" {
		WriteError(w, ErrBadRequest("email is required"))
		return
	}

	if len(body.Password) < 8 {
		WriteError(w, ErrBadRequest("password must be at least 8 characters"))
		return
	}

	if !validUserRoles[body.Role] {
		WriteError(w, ErrBadRequest("role must be one of: administrator, org_owner, member, readonly, service"))
		return
	}

	orgID := uuid.Nil
	if body.OrgID != "" {
		var err error
		orgID, err = uuid.Parse(body.OrgID)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid org_id"))
			return
		}
	}

	user, err := store.CreateUser(r.Context(), body.Email, body.DisplayName, body.Password, body.Role, orgID)
	if err != nil {
		WriteError(w, ErrInternal("failed to create user"))
		return
	}

	writeJSON(w, http.StatusCreated, user)
}

func handleAdminGetUser(w http.ResponseWriter, r *http.Request, store UserAdminStore, id uuid.UUID) {
	user, err := store.GetUser(r.Context(), id)
	if err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		WriteError(w, ErrInternal("failed to get user"))
		return
	}

	writeJSON(w, http.StatusOK, user)
}

func handleAdminUpdateUser(w http.ResponseWriter, r *http.Request, store UserAdminStore, id uuid.UUID) {
	var body updateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body"))
		return
	}

	if body.Role != "" && !validUserRoles[body.Role] {
		WriteError(w, ErrBadRequest("role must be one of: administrator, org_owner, member, readonly, service"))
		return
	}

	user, err := store.UpdateUser(r.Context(), id, body.DisplayName, body.Role, body.Settings)
	if err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		WriteError(w, ErrInternal("failed to update user"))
		return
	}

	writeJSON(w, http.StatusOK, user)
}

func handleAdminDeleteUser(w http.ResponseWriter, r *http.Request, store UserAdminStore, id uuid.UUID) {
	// Check if the user is an admin and if they are the last one.
	user, err := store.GetUser(r.Context(), id)
	if err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		WriteError(w, ErrInternal("failed to get user"))
		return
	}

	if user.Role == "administrator" {
		count, err := store.CountAdmins(r.Context())
		if err != nil {
			WriteError(w, ErrInternal("failed to count administrators"))
			return
		}
		if count <= 1 {
			WriteError(w, ErrConflict("cannot delete the last administrator"))
			return
		}
	}

	if err := store.DeleteUser(r.Context(), id); err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		WriteError(w, ErrInternal("failed to delete user"))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleAdminListAPIKeys(w http.ResponseWriter, r *http.Request, store UserAdminStore, userID uuid.UUID) {
	keys, err := store.ListAPIKeys(r.Context(), userID)
	if err != nil {
		WriteError(w, ErrInternal("failed to list api keys"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"data": keys,
	})
}

func handleAdminGenerateAPIKey(w http.ResponseWriter, r *http.Request, store UserAdminStore, userID uuid.UUID) {
	var body generateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body"))
		return
	}

	if strings.TrimSpace(body.Name) == "" {
		WriteError(w, ErrBadRequest("name is required"))
		return
	}

	key, rawKey, err := store.GenerateAPIKey(r.Context(), userID, body.Name, body.Scopes, body.ExpiresAt)
	if err != nil {
		WriteError(w, ErrInternal("failed to generate api key"))
		return
	}

	writeJSON(w, http.StatusCreated, generateAPIKeyResponse{
		APIKey: key,
		RawKey: rawKey,
	})
}

func handleAdminRevokeAPIKey(w http.ResponseWriter, r *http.Request, store UserAdminStore, keyID uuid.UUID) {
	err := store.RevokeAPIKey(r.Context(), keyID)
	if err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("api key not found"))
			return
		}
		WriteError(w, ErrInternal("failed to revoke api key"))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
