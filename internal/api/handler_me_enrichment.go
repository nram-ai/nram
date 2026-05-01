package api

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// MeEnrichmentStore narrows the EnrichmentAdminStore surface to just the
// self-scoped read used by NewSelfEnrichmentHandler.
type MeEnrichmentStore interface {
	SelfQueueStatus(ctx context.Context, userNamespaceID uuid.UUID) (*EnrichmentQueueStatus, error)
}

// MeEnrichmentConfig wires NewSelfEnrichmentHandler.
type MeEnrichmentConfig struct {
	Store MeEnrichmentStore
	Users UserGetter
}

// NewSelfEnrichmentHandler returns the read-only self-tier enrichment
// queue handler at /v1/me/enrichment. Returns the caller's own queue
// items + counts (filtered by namespace prefix). Write operations
// (retry, pause, test-prompt) remain admin-only at /v1/admin/enrichment.
func NewSelfEnrichmentHandler(cfg MeEnrichmentConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}

		ac := auth.FromContext(r.Context())
		if ac == nil {
			WriteError(w, ErrUnauthorized("authentication required"))
			return
		}

		user, err := cfg.Users.GetByID(r.Context(), ac.UserID)
		if err != nil {
			WriteError(w, ErrInternal("failed to resolve user"))
			return
		}

		status, err := cfg.Store.SelfQueueStatus(r.Context(), user.NamespaceID)
		if err != nil {
			WriteError(w, ErrInternal("failed to get enrichment queue status"))
			return
		}

		if status.Items == nil {
			status.Items = []EnrichmentQueueItem{}
		}

		writeJSON(w, http.StatusOK, status)
	}
}
