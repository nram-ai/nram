package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// EnrichmentAdminStore abstracts storage and worker management operations
// for the enrichment admin API.
type EnrichmentAdminStore interface {
	// QueueStatus returns counts by status and recent queue items.
	QueueStatus(ctx context.Context) (*EnrichmentQueueStatus, error)
	// RetryFailed retries failed enrichment jobs. If ids is nil/empty, retries all failed.
	RetryFailed(ctx context.Context, ids []uuid.UUID) (int, error)
	// SetPaused pauses or resumes enrichment workers.
	SetPaused(ctx context.Context, paused bool) error
	// IsPaused returns whether enrichment workers are paused.
	IsPaused(ctx context.Context) (bool, error)
}

// EnrichmentAdminConfig holds the dependencies for the enrichment admin handler.
type EnrichmentAdminConfig struct {
	Store EnrichmentAdminStore
}

// EnrichmentQueueStatus is the response for GET /enrichment/queue.
type EnrichmentQueueStatus struct {
	Counts EnrichmentQueueCounts `json:"counts"`
	Items  []EnrichmentQueueItem `json:"items"`
	Paused bool                  `json:"paused"`
}

// EnrichmentQueueCounts contains the count of items in each queue state.
type EnrichmentQueueCounts struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Completed  int `json:"completed"`
	Failed     int `json:"failed"`
}

// EnrichmentQueueItem describes a single item in the enrichment queue.
type EnrichmentQueueItem struct {
	ID        uuid.UUID `json:"id"`
	MemoryID  uuid.UUID `json:"memory_id"`
	Status    string    `json:"status"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// enrichmentRetryRequest is the request body for POST /enrichment/retry.
type enrichmentRetryRequest struct {
	IDs []uuid.UUID `json:"ids"`
}

// enrichmentPauseRequest is the request body for POST /enrichment/pause.
type enrichmentPauseRequest struct {
	Paused bool `json:"paused"`
}

// NewAdminEnrichmentHandler returns an http.HandlerFunc that dispatches enrichment
// admin requests based on method and sub-path under /enrichment.
//
// Routes:
//   - GET  /enrichment         — queue status (convenience alias)
//   - GET  /enrichment/queue   — queue status with counts and recent items
//   - POST /enrichment/retry   — retry failed jobs (all or specific IDs)
//   - POST /enrichment/pause   — pause or resume enrichment workers
func NewAdminEnrichmentHandler(cfg EnrichmentAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sub := extractEnrichmentSubPath(r.URL.Path)

		switch sub {
		case "", "queue":
			handleEnrichmentQueue(w, r, cfg)
		case "retry":
			handleEnrichmentRetry(w, r, cfg)
		case "pause":
			handleEnrichmentPause(w, r, cfg)
		default:
			WriteError(w, ErrBadRequest("unknown enrichment sub-path"))
		}
	}
}

// extractEnrichmentSubPath returns the portion of the URL path after "/enrichment".
// For example, "/v1/admin/enrichment/queue" returns "queue".
func extractEnrichmentSubPath(path string) string {
	const marker = "/enrichment"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return ""
	}
	rest := path[idx+len(marker):]
	rest = strings.TrimPrefix(rest, "/")
	return rest
}

// handleEnrichmentQueue handles GET /enrichment and GET /enrichment/queue.
func handleEnrichmentQueue(w http.ResponseWriter, r *http.Request, cfg EnrichmentAdminConfig) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	status, err := cfg.Store.QueueStatus(r.Context())
	if err != nil {
		WriteError(w, ErrInternal("failed to get enrichment queue status"))
		return
	}

	writeJSON(w, http.StatusOK, status)
}

// handleEnrichmentRetry handles POST /enrichment/retry.
func handleEnrichmentRetry(w http.ResponseWriter, r *http.Request, cfg EnrichmentAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var body enrichmentRetryRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	retried, err := cfg.Store.RetryFailed(r.Context(), body.IDs)
	if err != nil {
		WriteError(w, ErrInternal("failed to retry enrichment jobs"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]int{"retried": retried})
}

// handleEnrichmentPause handles POST /enrichment/pause.
func handleEnrichmentPause(w http.ResponseWriter, r *http.Request, cfg EnrichmentAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var body enrichmentPauseRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	if err := cfg.Store.SetPaused(r.Context(), body.Paused); err != nil {
		WriteError(w, ErrInternal("failed to set enrichment pause state"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"paused": body.Paused})
}
