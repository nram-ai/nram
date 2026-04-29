package events

import "github.com/jackc/pgx/v5/pgxpool"

// NewEventBus creates an EventBus appropriate for the given storage backend.
// For "postgres" it requires a non-nil pgxpool.Pool; for all other backends
// (including "sqlite") it returns a purely in-memory bus. subscriberBuf and
// replayCap are forwarded to the underlying MemoryBus; cmd/server/main.go
// resolves both from the SettingsService cascade. Zero or negative falls
// through to package-internal floors so misuse cannot create an unusable bus.
func NewEventBus(backend string, pool *pgxpool.Pool, subscriberBuf, replayCap int) EventBus {
	if backend == "postgres" && pool != nil {
		return NewPostgresBus(pool, subscriberBuf, replayCap)
	}
	return NewMemoryBus(subscriberBuf, replayCap)
}
