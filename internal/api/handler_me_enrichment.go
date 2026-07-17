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
	SelfClearFailed(ctx context.Context, userNamespacePath string, olderThanDays int) (int64, error)
}

// MeEnrichmentConfig wires NewSelfEnrichmentHandler.
type MeEnrichmentConfig struct {
	Store      MeEnrichmentStore
	Users      UserGetter
	Namespaces MeDreamNamespaceLookup
	// ReExtractMemories re-extracts an explicit set of the caller's memories,
	// scoped to the caller's namespace prefix. Nil disables the endpoint.
	ReExtractMemories func(ctx context.Context, namespacePrefix string, memoryIDs []uuid.UUID) (EnrichmentReExtractResult, error)
}

// NewSelfEnrichmentHandler returns the self-tier enrichment handler at
// /v1/me/enrichment. Sub-paths:
//
//	GET  /:            queue status (counts + items) scoped to caller
//	GET  /queue:       alias for /
//	POST /retry:       retry failed jobs the caller owns
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
		case "clear-failed":
			handleMeEnrichmentClearFailed(w, r, cfg, user.NamespaceID)
		case "re-extract":
			handleMeEnrichmentReExtract(w, r, cfg, user.NamespaceID)
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

// handleMeEnrichmentClearFailed handles POST /me/enrichment/clear-failed: delete
// the caller's failed enrichment jobs (scoped to the caller's namespace path
// prefix). older_than_days 0 clears all in scope. Returns rows deleted.
func handleMeEnrichmentClearFailed(w http.ResponseWriter, r *http.Request, cfg MeEnrichmentConfig, userNS uuid.UUID) {
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
	var body enrichmentClearRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}
	deleted, err := cfg.Store.SelfClearFailed(r.Context(), ns.Path, body.OlderThanDays)
	if err != nil {
		WriteError(w, ErrInternal("failed to clear failed enrichment jobs"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"deleted": deleted})
}

// handleMeEnrichmentReExtract handles POST /me/enrichment/re-extract: re-extract
// an explicit set of the caller's memories, scoped to the caller's namespace
// path prefix so IDs outside the caller's namespace are silently dropped.
func handleMeEnrichmentReExtract(w http.ResponseWriter, r *http.Request, cfg MeEnrichmentConfig, userNS uuid.UUID) {
	if cfg.Namespaces == nil {
		WriteError(w, ErrInternal("namespace lookup unavailable"))
		return
	}
	ns, err := cfg.Namespaces.GetByID(r.Context(), userNS)
	if err != nil || ns == nil {
		WriteError(w, ErrInternal("failed to resolve user namespace"))
		return
	}
	reExtractMemoriesHandler(w, r, ns.Path, cfg.ReExtractMemories)
}
