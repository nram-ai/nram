package api

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
)

// WebAuthnCredManager defines the credential repository methods used by the
// passkey management handlers.
type WebAuthnCredManager interface {
	ListByUser(ctx context.Context, userID uuid.UUID) ([]model.WebAuthnCredential, error)
	Delete(ctx context.Context, id uuid.UUID) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.WebAuthnCredential, error)
}

// AuthPasskeyChecker checks if a user has registered passkeys. Used by the
// lookup handler to indicate passkey availability.
type AuthPasskeyChecker interface {
	HasCredentials(ctx context.Context, userID uuid.UUID) (bool, error)
}

// NewMePasskeysListHandler returns a handler that lists the authenticated
// user's passkeys. GET /v1/me/passkeys
func NewMePasskeysListHandler(creds WebAuthnCredManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("unauthorized"))
			return
		}

		list, err := creds.ListByUser(r.Context(), ac.UserID)
		if err != nil {
			WriteError(w, ErrInternal("failed to list passkeys"))
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"data": list,
		})
	}
}

// NewMePasskeyDeleteHandler returns a handler that deletes a passkey owned by
// the authenticated user. DELETE /v1/me/passkeys/{id}
func NewMePasskeyDeleteHandler(creds WebAuthnCredManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("unauthorized"))
			return
		}

		idStr := chi.URLParam(r, "id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid passkey ID"))
			return
		}

		// Verify ownership before deleting.
		cred, err := creds.GetByID(r.Context(), id)
		if err != nil {
			WriteError(w, ErrBadRequest("passkey not found"))
			return
		}
		if cred.UserID != ac.UserID {
			WriteError(w, ErrBadRequest("passkey not found"))
			return
		}

		if err := creds.Delete(r.Context(), id); err != nil {
			WriteError(w, ErrInternal("failed to delete passkey"))
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}
