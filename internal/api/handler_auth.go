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
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// AuthUserRepo defines the user repository methods needed by the auth handlers.
type AuthUserRepo interface {
	GetByEmail(ctx context.Context, email string) (*model.User, error)
	UpdateLastLogin(ctx context.Context, userID uuid.UUID) error
}

// AuthConfig holds dependencies for the auth handlers.
type AuthConfig struct {
	UserRepo  AuthUserRepo
	JWTSecret []byte
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token string    `json:"token"`
	User  loginUser `json:"user"`
}

type loginUser struct {
	ID          uuid.UUID `json:"id"`
	Email       string    `json:"email"`
	DisplayName string    `json:"display_name"`
	Role        string    `json:"role"`
	OrgID       uuid.UUID `json:"org_id"`
}

type lookupRequest struct {
	Email string `json:"email"`
}

type lookupResponse struct {
	Method string `json:"method"`
}

// NewLoginHandler returns an http.HandlerFunc that authenticates a user by
// email and password and returns a signed JWT.
func NewLoginHandler(cfg AuthConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		var req loginRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteError(w, ErrBadRequest("invalid request body"))
			return
		}

		req.Email = strings.TrimSpace(req.Email)
		req.Password = strings.TrimSpace(req.Password)

		if req.Email == "" || req.Password == "" {
			WriteError(w, ErrBadRequest("email and password are required"))
			return
		}

		user, err := cfg.UserRepo.GetByEmail(r.Context(), req.Email)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				WriteError(w, ErrUnauthorized("invalid credentials"))
				return
			}
			WriteError(w, ErrInternal("failed to look up user"))
			return
		}

		if user.DisabledAt != nil {
			WriteError(w, ErrUnauthorized("account disabled"))
			return
		}

		if user.PasswordHash == nil {
			WriteError(w, ErrUnauthorized("no local credentials"))
			return
		}

		if !storage.VerifyPassword(*user.PasswordHash, req.Password) {
			WriteError(w, ErrUnauthorized("invalid credentials"))
			return
		}

		token, err := auth.GenerateJWT(user.ID, user.Role, cfg.JWTSecret, 24*time.Hour)
		if err != nil {
			WriteError(w, ErrInternal("failed to generate token"))
			return
		}

		// Best-effort update of last_login timestamp.
		_ = cfg.UserRepo.UpdateLastLogin(r.Context(), user.ID)

		writeJSON(w, http.StatusOK, loginResponse{
			Token: token,
			User: loginUser{
				ID:          user.ID,
				Email:       user.Email,
				DisplayName: user.DisplayName,
				Role:        user.Role,
				OrgID:       user.OrgID,
			},
		})
	}
}

// NewLookupHandler returns an http.HandlerFunc that checks which authentication
// method is available for a given email address.
func NewLookupHandler(cfg AuthConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		var req lookupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteError(w, ErrBadRequest("invalid request body"))
			return
		}

		req.Email = strings.TrimSpace(req.Email)
		if req.Email == "" {
			WriteError(w, ErrBadRequest("email is required"))
			return
		}

		user, err := cfg.UserRepo.GetByEmail(r.Context(), req.Email)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusOK, lookupResponse{Method: "unknown"})
				return
			}
			WriteError(w, ErrInternal("failed to look up user"))
			return
		}

		if user.PasswordHash != nil {
			writeJSON(w, http.StatusOK, lookupResponse{Method: "local"})
			return
		}

		writeJSON(w, http.StatusOK, lookupResponse{Method: "idp"})
	}
}
