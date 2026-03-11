package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/nram-ai/nram/internal/model"
)

// SetupStore abstracts storage operations for the setup wizard.
type SetupStore interface {
	// IsSetupComplete checks system_meta for the setup_complete key.
	IsSetupComplete(ctx context.Context) (bool, error)
	// CompleteSetup creates org, user, API key, and marks setup complete.
	// Returns the created user and raw API key string.
	CompleteSetup(ctx context.Context, email, password string) (*model.User, string, error)
	// Backend returns "sqlite" or "postgres".
	Backend() string
}

// SetupConfig holds dependencies for setup handlers.
type SetupConfig struct {
	Store SetupStore
}

type setupStatusResponse struct {
	SetupComplete bool   `json:"setup_complete"`
	Backend       string `json:"backend"`
}

type setupRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type setupResponse struct {
	User    *model.User `json:"user"`
	APIKey  string      `json:"api_key"`
	Message string      `json:"message"`
}

// NewAdminSetupStatusHandler returns an http.HandlerFunc that reports whether
// initial setup has been completed.
func NewAdminSetupStatusHandler(cfg SetupConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		complete, err := cfg.Store.IsSetupComplete(r.Context())
		if err != nil {
			WriteError(w, ErrInternal("failed to check setup status"))
			return
		}

		writeJSON(w, http.StatusOK, setupStatusResponse{
			SetupComplete: complete,
			Backend:       cfg.Store.Backend(),
		})
	}
}

// NewAdminSetupHandler returns an http.HandlerFunc that performs initial setup
// by creating the first administrator account, default organization, and an
// initial API key.
func NewAdminSetupHandler(cfg SetupConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Check if setup is already complete.
		complete, err := cfg.Store.IsSetupComplete(ctx)
		if err != nil {
			WriteError(w, ErrInternal("failed to check setup status"))
			return
		}
		if complete {
			WriteError(w, ErrConflict("setup already complete"))
			return
		}

		// Decode request body.
		var req setupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteError(w, ErrBadRequest("invalid JSON body"))
			return
		}

		// Validate fields.
		if req.Email == "" {
			WriteError(w, ErrBadRequest("email is required"))
			return
		}
		if len(req.Password) < 8 {
			WriteError(w, ErrBadRequest("password must be at least 8 characters"))
			return
		}

		// Perform setup.
		user, apiKey, err := cfg.Store.CompleteSetup(ctx, req.Email, req.Password)
		if err != nil {
			WriteError(w, ErrInternal("failed to complete setup"))
			return
		}

		writeJSON(w, http.StatusCreated, setupResponse{
			User:    user,
			APIKey:  apiKey,
			Message: "Setup complete. Store this API key — it will not be shown again.",
		})
	}
}
