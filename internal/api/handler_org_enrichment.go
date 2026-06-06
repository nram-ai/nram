package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// OrgEnrichmentStore narrows the admin store surface to the org-scoped
// operations /v1/orgs/{orgId}/enrichment needs.
type OrgEnrichmentStore interface {
	OrgQueueStatus(ctx context.Context, orgID uuid.UUID, params QueueListParams) (*EnrichmentQueueStatus, error)
	OrgRetryFailed(ctx context.Context, orgID uuid.UUID, ids []uuid.UUID) (int, error)
}

// OrgEnrichmentConfig wires NewOrgEnrichmentHandler.
type OrgEnrichmentConfig struct {
	Store OrgEnrichmentStore
}

// NewOrgEnrichmentHandler returns the org-tier enrichment handler at
// /v1/orgs/{org_id}/enrichment. Authorization: caller must be administrator
// or org_owner of {org_id}. Sub-paths:
//
//	GET  /:          queue status (counts + items) scoped to org
//	GET  /queue:     alias for /
//	POST /retry:     retry failed jobs in the org (all or by ids)
func NewOrgEnrichmentHandler(cfg OrgEnrichmentConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orgID, ok := OrgScope(r)
		if !ok {
			WriteError(w, ErrBadRequest("invalid org_id"))
			return
		}
		ac := auth.FromContext(r.Context())
		if !requireOrgOwner(ac, *orgID) {
			WriteError(w, ErrForbidden("org_owner role required for this org"))
			return
		}

		sub := extractSubPath(r.URL.Path, "/enrichment")
		switch sub {
		case "", "queue":
			handleOrgEnrichmentQueue(w, r, cfg, *orgID)
		case "retry":
			handleOrgEnrichmentRetry(w, r, cfg, *orgID)
		default:
			WriteError(w, ErrBadRequest("unknown enrichment sub-path"))
		}
	}
}

func handleOrgEnrichmentQueue(w http.ResponseWriter, r *http.Request, cfg OrgEnrichmentConfig, orgID uuid.UUID) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	status, err := cfg.Store.OrgQueueStatus(r.Context(), orgID, parseQueueListParams(r))
	if err != nil {
		WriteError(w, ErrInternal("failed to get enrichment queue status"))
		return
	}
	if status.Items == nil {
		status.Items = []EnrichmentQueueItem{}
	}
	writeJSON(w, http.StatusOK, status)
}

func handleOrgEnrichmentRetry(w http.ResponseWriter, r *http.Request, cfg OrgEnrichmentConfig, orgID uuid.UUID) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	var body enrichmentRetryRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}
	retried, err := cfg.Store.OrgRetryFailed(r.Context(), orgID, body.IDs)
	if err != nil {
		WriteError(w, ErrInternal("failed to retry enrichment jobs"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"retried": retried})
}
