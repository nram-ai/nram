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

// AuthIdPRepo defines the IdP repository methods needed by the auth handlers.
type AuthIdPRepo interface {
	ListIdPsByOrg(ctx context.Context, orgID uuid.UUID) ([]model.OAuthIdPConfig, error)
	ListIdPs(ctx context.Context) ([]model.OAuthIdPConfig, error)
}

// AuthConfig holds dependencies for the auth handlers.
type AuthConfig struct {
	UserRepo  AuthUserRepo
	IdPRepo   AuthIdPRepo
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
	Method string  `json:"method"`
	IdPID  *string `json:"idp_id,omitempty"`
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

		token, err := auth.GenerateJWT(user.ID, user.OrgID, user.Role, cfg.JWTSecret, 24*time.Hour)
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
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			WriteError(w, ErrInternal("failed to look up user"))
			return
		}

		// Known user with a password → local login.
		if user != nil && user.PasswordHash != nil {
			writeJSON(w, http.StatusOK, lookupResponse{Method: "local"})
			return
		}

		// Try to find an IdP for this user. If the user exists, check their
		// org's IdP configs. If the user doesn't exist, scan all IdP configs
		// for one whose allowed_domains matches the email domain.
		if idpID := resolveIdPForEmail(r.Context(), cfg, user, req.Email); idpID != "" {
			writeJSON(w, http.StatusOK, lookupResponse{Method: "idp", IdPID: &idpID})
			return
		}

		// Known user without password and no IdP configured — still report idp
		// so the frontend doesn't offer password login.
		if user != nil {
			writeJSON(w, http.StatusOK, lookupResponse{Method: "idp"})
			return
		}

		writeJSON(w, http.StatusOK, lookupResponse{Method: "unknown"})
	}
}

// resolveIdPForEmail finds the best IdP config for the given email. For known
// users it checks their org's IdP configs first. For unknown users (or if no
// org-scoped IdP matched) it scans all IdP configs by email domain.
func resolveIdPForEmail(ctx context.Context, cfg AuthConfig, user *model.User, email string) string {
	if cfg.IdPRepo == nil {
		return ""
	}

	domain := emailDomain(email)

	// If the user exists, prefer their org's IdP.
	if user != nil && user.OrgID != uuid.Nil {
		idps, err := cfg.IdPRepo.ListIdPsByOrg(ctx, user.OrgID)
		if err == nil {
			for _, idp := range idps {
				if matchesDomain(idp, domain) {
					return idp.ID.String()
				}
			}
		}
	}

	// Scan all IdP configs for a domain match.
	idps, err := cfg.IdPRepo.ListIdPs(ctx)
	if err != nil {
		return ""
	}
	for _, idp := range idps {
		if matchesDomain(idp, domain) {
			return idp.ID.String()
		}
	}

	return ""
}

// matchesDomain returns true if the IdP config accepts the given email domain.
// An empty allowed_domains list means all domains are accepted.
func matchesDomain(idp model.OAuthIdPConfig, domain string) bool {
	if len(idp.AllowedDomains) == 0 {
		return true
	}
	for _, d := range idp.AllowedDomains {
		if strings.EqualFold(d, domain) {
			return true
		}
	}
	return false
}

func emailDomain(email string) string {
	parts := strings.SplitN(email, "@", 2)
	if len(parts) != 2 {
		return ""
	}
	return strings.ToLower(parts[1])
}
