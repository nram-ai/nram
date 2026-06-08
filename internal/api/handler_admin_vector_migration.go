package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

// ErrVectorMigrationQdrantNotConfigured is returned by the store when a
// migration is requested but no Qdrant address is configured. The handler maps
// it to 400 so the UI can prompt the operator to configure Qdrant first.
var ErrVectorMigrationQdrantNotConfigured = errors.New("qdrant is not configured: set qdrant.addr before migrating vectors")

// ErrMigrationInProgress is returned when a migration is requested while one is
// already running. The handler maps it to 409.
var ErrMigrationInProgress = errors.New("a migration is already in progress")

// Vector migration directions.
const (
	VectorMigrationToQdrant   = "to_qdrant"
	VectorMigrationFromQdrant = "from_qdrant"
)

// VectorMigrationAdminStore abstracts the vector migration operation for the
// admin API. Implementations copy vectors between the SQL primary store and
// Qdrant; they read the source read-only and never delete. DryRun counts
// synchronously; Start launches a real migration in the background and streams
// progress over the event bus.
type VectorMigrationAdminStore interface {
	DryRun(ctx context.Context, direction string, batchSize int) (*VectorMigrationResult, error)
	Start(ctx context.Context, direction string, batchSize int) error
}

// VectorMigrationAdminConfig holds the dependencies for the vector migration handler.
type VectorMigrationAdminConfig struct {
	Store VectorMigrationAdminStore
}

// VectorMigrationDimStat is a per-(kind, dimension) source-vs-destination count
// so a partial copy is visible to the operator.
type VectorMigrationDimStat struct {
	Kind        string `json:"kind"`
	Dimension   int    `json:"dimension"`
	SourceCount int    `json:"source_count"`
	DestCount   int    `json:"dest_count"`
}

// VectorMigrationResult is the response body for POST /admin/vector-migration.
type VectorMigrationResult struct {
	Direction   string                   `json:"direction"`
	DryRun      bool                     `json:"dry_run"`
	MemoryCount int                      `json:"memory_count"`
	EntityCount int                      `json:"entity_count"`
	Mismatch    bool                     `json:"mismatch"`
	Verify      []VectorMigrationDimStat `json:"verify"`
}

// vectorMigrationRequest is the request body for POST /admin/vector-migration.
type vectorMigrationRequest struct {
	Direction string `json:"direction"`
	DryRun    bool   `json:"dry_run"`
	BatchSize int    `json:"batch_size"`
}

// NewAdminVectorMigrationHandler returns an http.HandlerFunc for
// POST /admin/vector-migration. It copies memory and entity vectors between the
// SQL primary store and Qdrant in the requested direction. A dry run reports
// source counts without writing.
func NewAdminVectorMigrationHandler(cfg VectorMigrationAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}
		if cfg.Store == nil {
			http.Error(w, "vector migration not available in this deployment", http.StatusServiceUnavailable)
			return
		}

		var body vectorMigrationRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			WriteError(w, ErrBadRequest("invalid JSON body"))
			return
		}
		if body.Direction != VectorMigrationToQdrant && body.Direction != VectorMigrationFromQdrant {
			WriteError(w, ErrBadRequest("direction must be 'to_qdrant' or 'from_qdrant'"))
			return
		}

		// Dry run is a synchronous count and returns the result inline.
		if body.DryRun {
			result, err := cfg.Store.DryRun(r.Context(), body.Direction, body.BatchSize)
			if err != nil {
				if errors.Is(err, ErrVectorMigrationQdrantNotConfigured) {
					WriteError(w, ErrBadRequest(err.Error()))
					return
				}
				WriteError(w, ErrInternal(err.Error()))
				return
			}
			writeJSON(w, http.StatusOK, result)
			return
		}

		// Real migrations run in the background; progress and the terminal
		// result stream over /v1/events under the vector-migration scope.
		if err := cfg.Store.Start(r.Context(), body.Direction, body.BatchSize); err != nil {
			switch {
			case errors.Is(err, ErrVectorMigrationQdrantNotConfigured):
				WriteError(w, ErrBadRequest(err.Error()))
			case errors.Is(err, ErrMigrationInProgress):
				WriteError(w, ErrConflict(err.Error()))
			default:
				WriteError(w, ErrInternal(err.Error()))
			}
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"status": "started", "direction": body.Direction})
	}
}
