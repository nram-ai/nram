package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// OrgUserStore abstracts user operations scoped to an organization.
type OrgUserStore interface {
	CountUsersByOrg(ctx context.Context, orgID uuid.UUID) (int, error)
	ListUsersByOrg(ctx context.Context, orgID uuid.UUID, limit, offset int) ([]model.User, error)
	CreateUser(ctx context.Context, email, displayName, password, role string, orgID uuid.UUID) (*model.User, error)
	GetUser(ctx context.Context, id uuid.UUID) (*model.User, error)
	UpdateUser(ctx context.Context, id uuid.UUID, displayName, role string, settings json.RawMessage) (*model.User, error)
	DeleteUser(ctx context.Context, id uuid.UUID) error
	CountAdmins(ctx context.Context) (int, error)
	CountAPIKeys(ctx context.Context, userID uuid.UUID) (int, error)
	ListAPIKeys(ctx context.Context, userID uuid.UUID, limit, offset int) ([]model.APIKey, error)
	GenerateAPIKey(ctx context.Context, userID uuid.UUID, name string, scopes []uuid.UUID, expiresAt *time.Time) (*model.APIKey, string, error)
	RevokeAPIKey(ctx context.Context, keyID uuid.UUID) error
}

// OrgUserConfig holds the dependencies for org-scoped user management.
type OrgUserConfig struct {
	Store OrgUserStore
}

// NewOrgUsersHandler returns an http.HandlerFunc that handles org-scoped
// user CRUD operations. The {org_id} is extracted from the URL and validated
// by OrgAccessMiddleware before this handler is called.
func NewOrgUsersHandler(cfg OrgUserConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orgIDStr := chi.URLParam(r, "org_id")
		orgID, err := uuid.Parse(orgIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid org_id"))
			return
		}

		// Extract sub-path after "/users".
		path := r.URL.Path
		idx := strings.LastIndex(path, "/users")
		sub := ""
		if idx >= 0 {
			sub = path[idx+6:]
		}
		sub = strings.TrimPrefix(sub, "/")

		if sub == "" {
			switch r.Method {
			case http.MethodGet:
				handleOrgListUsers(w, r, cfg.Store, orgID)
			case http.MethodPost:
				handleOrgCreateUser(w, r, cfg.Store, orgID)
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

		if len(parts) == 1 {
			switch r.Method {
			case http.MethodGet:
				handleOrgGetUser(w, r, cfg.Store, orgID, userID)
			case http.MethodPut:
				handleOrgUpdateUser(w, r, cfg.Store, orgID, userID)
			case http.MethodDelete:
				handleOrgDeleteUser(w, r, cfg.Store, orgID, userID)
			default:
				WriteError(w, ErrBadRequest("method not allowed"))
			}
			return
		}

		if parts[1] != "api-keys" {
			WriteError(w, ErrNotFound("not found"))
			return
		}

		if len(parts) == 2 {
			switch r.Method {
			case http.MethodGet:
				handleOrgListAPIKeys(w, r, cfg.Store, orgID, userID)
			case http.MethodPost:
				handleOrgGenerateAPIKey(w, r, cfg.Store, orgID, userID)
			default:
				WriteError(w, ErrBadRequest("method not allowed"))
			}
			return
		}

		if len(parts) == 3 {
			keyID, err := uuid.Parse(parts[2])
			if err != nil {
				WriteError(w, ErrBadRequest("invalid api key id"))
				return
			}
			if r.Method == http.MethodDelete {
				handleOrgRevokeAPIKey(w, r, cfg.Store, orgID, userID, keyID)
				return
			}
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		WriteError(w, ErrNotFound("not found"))
	}
}

func handleOrgListUsers(w http.ResponseWriter, r *http.Request, store OrgUserStore, orgID uuid.UUID) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			limit = n
		}
	}
	if limit > 200 {
		limit = 200
	}

	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	total, err := store.CountUsersByOrg(r.Context(), orgID)
	if err != nil {
		WriteError(w, ErrInternal("failed to count users"))
		return
	}

	users, err := store.ListUsersByOrg(r.Context(), orgID, limit, offset)
	if err != nil {
		WriteError(w, ErrInternal("failed to list users"))
		return
	}
	if users == nil {
		users = []model.User{}
	}

	writeJSON(w, http.StatusOK, model.PaginatedResponse[model.User]{
		Data: users,
		Pagination: model.Pagination{
			Total:  total,
			Limit:  limit,
			Offset: offset,
		},
	})
}

func handleOrgCreateUser(w http.ResponseWriter, r *http.Request, store OrgUserStore, orgID uuid.UUID) {
	ac := auth.FromContext(r.Context())

	var body createUserRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body"))
		return
	}

	body.Email = strings.TrimSpace(body.Email)
	if body.Email == "" {
		WriteError(w, ErrBadRequest("email is required"))
		return
	}
	if !isValidEmail(body.Email) {
		WriteError(w, ErrBadRequest("invalid email address"))
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

	// Org owners cannot escalate to administrator.
	if ac != nil && ac.Role != auth.RoleAdministrator && body.Role == auth.RoleAdministrator {
		WriteError(w, ErrForbidden("cannot assign administrator role"))
		return
	}

	user, err := store.CreateUser(r.Context(), body.Email, body.DisplayName, body.Password, body.Role, orgID)
	if err != nil {
		WriteError(w, ErrInternal("failed to create user"))
		return
	}

	writeJSON(w, http.StatusCreated, user)
}

func handleOrgGetUser(w http.ResponseWriter, r *http.Request, store OrgUserStore, orgID uuid.UUID, userID uuid.UUID) {
	user, err := store.GetUser(r.Context(), userID)
	if err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		WriteError(w, ErrInternal("failed to get user"))
		return
	}

	// Ensure the user belongs to the requested org.
	if user.OrgID != orgID {
		WriteError(w, ErrNotFound("user not found"))
		return
	}

	keys, err := store.ListAPIKeys(r.Context(), userID, 200, 0)
	if err != nil {
		WriteError(w, ErrInternal("failed to list api keys"))
		return
	}
	if keys == nil {
		keys = []model.APIKey{}
	}

	type userDetailResponse struct {
		*model.User
		APIKeys []model.APIKey `json:"api_keys"`
	}
	writeJSON(w, http.StatusOK, userDetailResponse{User: user, APIKeys: keys})
}

func handleOrgUpdateUser(w http.ResponseWriter, r *http.Request, store OrgUserStore, orgID uuid.UUID, userID uuid.UUID) {
	ac := auth.FromContext(r.Context())

	// Verify the target user belongs to this org.
	existing, err := store.GetUser(r.Context(), userID)
	if err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		WriteError(w, ErrInternal("failed to get user"))
		return
	}
	if existing.OrgID != orgID {
		WriteError(w, ErrNotFound("user not found"))
		return
	}

	var body updateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body"))
		return
	}

	if body.Role != "" && !validUserRoles[body.Role] {
		WriteError(w, ErrBadRequest("role must be one of: administrator, org_owner, member, readonly, service"))
		return
	}

	// Org owners cannot escalate to administrator.
	if ac != nil && ac.Role != auth.RoleAdministrator && body.Role == auth.RoleAdministrator {
		WriteError(w, ErrForbidden("cannot assign administrator role"))
		return
	}

	user, err := store.UpdateUser(r.Context(), userID, body.DisplayName, body.Role, body.Settings)
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

func handleOrgDeleteUser(w http.ResponseWriter, r *http.Request, store OrgUserStore, orgID uuid.UUID, userID uuid.UUID) {
	// Verify the target user belongs to this org.
	user, err := store.GetUser(r.Context(), userID)
	if err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		WriteError(w, ErrInternal("failed to get user"))
		return
	}
	if user.OrgID != orgID {
		WriteError(w, ErrNotFound("user not found"))
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

	if err := store.DeleteUser(r.Context(), userID); err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		WriteError(w, ErrInternal("failed to delete user"))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleOrgListAPIKeys(w http.ResponseWriter, r *http.Request, store OrgUserStore, orgID uuid.UUID, userID uuid.UUID) {
	// Verify the target user belongs to this org.
	user, err := store.GetUser(r.Context(), userID)
	if err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		WriteError(w, ErrInternal("failed to get user"))
		return
	}
	if user.OrgID != orgID {
		WriteError(w, ErrNotFound("user not found"))
		return
	}

	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			limit = n
		}
	}
	if limit > 200 {
		limit = 200
	}

	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	total, err := store.CountAPIKeys(r.Context(), userID)
	if err != nil {
		WriteError(w, ErrInternal("failed to count api keys"))
		return
	}

	keys, err := store.ListAPIKeys(r.Context(), userID, limit, offset)
	if err != nil {
		WriteError(w, ErrInternal("failed to list api keys"))
		return
	}
	if keys == nil {
		keys = []model.APIKey{}
	}

	writeJSON(w, http.StatusOK, model.PaginatedResponse[model.APIKey]{
		Data: keys,
		Pagination: model.Pagination{
			Total:  total,
			Limit:  limit,
			Offset: offset,
		},
	})
}

func handleOrgGenerateAPIKey(w http.ResponseWriter, r *http.Request, store OrgUserStore, orgID uuid.UUID, userID uuid.UUID) {
	// Verify the target user belongs to this org.
	user, err := store.GetUser(r.Context(), userID)
	if err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		WriteError(w, ErrInternal("failed to get user"))
		return
	}
	if user.OrgID != orgID {
		WriteError(w, ErrNotFound("user not found"))
		return
	}

	var body generateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid request body"))
		return
	}

	if strings.TrimSpace(body.Label) == "" {
		WriteError(w, ErrBadRequest("label is required"))
		return
	}

	key, rawKey, err := store.GenerateAPIKey(r.Context(), userID, body.Label, body.Scopes, body.ExpiresAt)
	if err != nil {
		WriteError(w, ErrInternal("failed to generate api key"))
		return
	}

	resp := generateAPIKeyResponse{
		ID:        key.ID.String(),
		Key:       rawKey,
		Label:     key.Name,
		Prefix:    key.KeyPrefix,
		CreatedAt: key.CreatedAt.Format(time.RFC3339),
	}
	if key.ExpiresAt != nil {
		s := key.ExpiresAt.Format(time.RFC3339)
		resp.ExpiresAt = &s
	}
	resp.Scopes = make([]string, len(key.Scopes))
	for i, s := range key.Scopes {
		resp.Scopes[i] = s.String()
	}
	writeJSON(w, http.StatusCreated, resp)
}

func handleOrgRevokeAPIKey(w http.ResponseWriter, r *http.Request, store OrgUserStore, orgID uuid.UUID, userID uuid.UUID, keyID uuid.UUID) {
	// Verify the target user belongs to this org before revoking.
	user, err := store.GetUser(r.Context(), userID)
	if err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("user not found"))
			return
		}
		WriteError(w, ErrInternal("failed to get user"))
		return
	}
	if user.OrgID != orgID {
		WriteError(w, ErrNotFound("user not found"))
		return
	}

	if err := store.RevokeAPIKey(r.Context(), keyID); err != nil {
		if isUserNotFound(err) {
			WriteError(w, ErrNotFound("api key not found"))
			return
		}
		WriteError(w, ErrInternal("failed to revoke api key"))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
