// Package maintenance provides a small, backend-agnostic registry of in-flight
// maintenance operations that degrade server performance while they run (for
// example a SQLite VACUUM). An operation raises the flag via Begin and clears
// it via the returned closure; both transitions emit a maintenance event on the
// shared EventBus so the admin UI can flip a banner instantly, while Snapshot
// exposes the current state for a cheap poll endpoint.
//
// Snapshot is a pure in-memory read and never touches the database, so the
// status endpoint stays responsive even while a VACUUM holds SQLite's exclusive
// lock.
package maintenance

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/nram-ai/nram/internal/events"
)

// Op describes a single active maintenance operation.
type Op struct {
	ID      string    `json:"id"`
	Label   string    `json:"label"`
	Message string    `json:"message"`
	Since   time.Time `json:"since"`
}

// Registry tracks active maintenance operations and announces transitions on an
// EventBus. The zero value is not usable; construct with NewRegistry. All
// methods are safe for concurrent use.
type Registry struct {
	bus events.EventBus
	mu  sync.RWMutex
	ops map[string]Op
}

// NewRegistry creates a Registry that announces transitions on bus. A nil bus
// is allowed (events.Emit is nil-safe), which disables the instant-flip events
// but leaves Snapshot fully functional.
func NewRegistry(bus events.EventBus) *Registry {
	return &Registry{bus: bus, ops: make(map[string]Op)}
}

// Begin marks the operation identified by id as active and returns a function
// that ends it. Both the start and the end emit a maintenance event on the bus.
// The returned end function is idempotent, so callers can safely defer it and
// also call it explicitly. Re-using an id replaces any prior entry.
func (r *Registry) Begin(id, label, message string) func() {
	op := Op{ID: id, Label: label, Message: message, Since: time.Now().UTC()}

	r.mu.Lock()
	r.ops[id] = op
	r.mu.Unlock()

	events.Emit(context.Background(), r.bus, events.MaintenanceStarted, events.EventScopeMaintenance, op)

	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.ops, id)
			r.mu.Unlock()
			events.Emit(context.Background(), r.bus, events.MaintenanceEnded, events.EventScopeMaintenance, op)
		})
	}
}

// Snapshot reports whether any operation is active and returns a copy of the
// active operations, oldest first. It never touches the database.
func (r *Registry) Snapshot() (active bool, ops []Op) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ops) == 0 {
		return false, nil
	}

	ops = make([]Op, 0, len(r.ops))
	for _, op := range r.ops {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].Since.Before(ops[j].Since) })
	return true, ops
}
