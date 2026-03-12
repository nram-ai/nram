package api

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/nram-ai/nram/internal/storage"
)

// SetupChecker provides a cached check for whether initial setup is complete.
// It uses an atomic bool so the check is lock-free after the first successful
// database query.
type SetupChecker struct {
	done    atomic.Bool
	checked atomic.Bool
	db      storage.DB
}

// NewSetupChecker creates a SetupChecker that queries the given database on
// its first call to IsComplete, then caches the result.
func NewSetupChecker(db storage.DB) *SetupChecker {
	return &SetupChecker{db: db}
}

// IsComplete returns true if initial setup has been completed. The first
// invocation reads from the database; subsequent calls return the cached value.
func (sc *SetupChecker) IsComplete(ctx context.Context) bool {
	if sc.done.Load() {
		return true
	}

	if sc.checked.Load() {
		return false
	}

	if sc.db == nil {
		return false
	}

	val, err := storage.GetSystemMeta(ctx, sc.db, "setup_complete")
	if err != nil {
		return false
	}

	sc.checked.Store(true)
	if val == "true" {
		sc.done.Store(true)
		return true
	}
	return false
}

// MarkComplete flips the cached flag so subsequent calls to IsComplete return
// true without a database round-trip.
func (sc *SetupChecker) MarkComplete() {
	sc.done.Store(true)
	sc.checked.Store(true)
}

// SetupGuardMiddleware returns middleware that rejects requests with 503 when
// initial setup has not been completed. The isComplete function is called on
// every request and should use a cached value for performance.
func SetupGuardMiddleware(isComplete func(ctx context.Context) bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !isComplete(r.Context()) {
				WriteError(w, &APIError{
					Code:    "setup_required",
					Message: "Complete setup at the admin UI",
					Status:  http.StatusServiceUnavailable,
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
