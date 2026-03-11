package events

import "github.com/jackc/pgx/v5/pgxpool"

// NewEventBus creates an EventBus appropriate for the given storage backend.
// For "postgres" it requires a non-nil pgxpool.Pool; for all other backends
// (including "sqlite") it returns a purely in-memory bus.
func NewEventBus(backend string, pool *pgxpool.Pool) EventBus {
	if backend == "postgres" && pool != nil {
		return NewPostgresBus(pool)
	}
	return NewMemoryBus()
}
