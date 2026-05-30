package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// MeEnrichmentStore narrows the EnrichmentAdminStore surface to the
// self-scoped operations /v1/me/enrichment exposes.
type MeEnrichmentStore interface {
	SelfQueueStatus(ctx context.Context, userNamespaceID uuid.UUID, params QueueListParams) (*EnrichmentQueueStatus, error)
	SelfRetryFailed(ctx context.Context, userNamespacePath string, ids []uuid.UUID) (int, error)
}

// MeEnrichmentConfig wires NewSelfEnrichmentHandler.
type MeEnrichmentConfig struct {
	Store      MeEnrichmentStore
	Users      UserGetter
	Namespaces MeDreamNamespaceLookup
}

// NewSelfEnrichmentHandler returns the self-tier enrichment handler at
// /v1/me/enrichment. Sub-paths:
//
//	GET  /            — queue status (counts + items) scoped to caller
//	GET  /queue       — alias for /
//	POST /retry       — retry failed jobs the caller owns
//
// Pause/resume remain admin-only at /v1/admin/enrichment.
func NewSelfEnrichmentHandler(cfg MeEnrichmentConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		sub := extractSubPath(r.URL.Path, "/me/enrichment")
		switch sub {
		case "", "queue":
			handleMeEnrichmentQueue(w, r, cfg, user.NamespaceID)
		case "retry":
			handleMeEnrichmentRetry(w, r, cfg, user.NamespaceID)
		default:
			WriteError(w, ErrBadRequest("unknown enrichment sub-path"))
		}
	}
}

func handleMeEnrichmentQueue(w http.ResponseWriter, r *http.Request, cfg MeEnrichmentConfig, userNS uuid.UUID) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	status, err := cfg.Store.SelfQueueStatus(r.Context(), userNS, parseQueueListParams(r))
	if err != nil {
		WriteError(w, ErrInternal("failed to get enrichment queue status"))
		return
	}
	if status.Items == nil {
		status.Items = []EnrichmentQueueItem{}
	}
	writeJSON(w, http.StatusOK, status)
}

func handleMeEnrichmentRetry(w http.ResponseWriter, r *http.Request, cfg MeEnrichmentConfig, userNS uuid.UUID) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	if cfg.Namespaces == nil {
		WriteError(w, ErrInternal("namespace lookup unavailable"))
		return
	}
	ns, err := cfg.Namespaces.GetByID(r.Context(), userNS)
	if err != nil || ns == nil {
		WriteError(w, ErrInternal("failed to resolve user namespace"))
		return
	}
	var body enrichmentRetryRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}
	retried, err := cfg.Store.SelfRetryFailed(r.Context(), ns.Path, body.IDs)
	if err != nil {
		WriteError(w, ErrInternal("failed to retry enrichment jobs"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"retried": retried})
}
