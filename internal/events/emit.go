package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// Emit publishes an event to the given EventBus. If bus is nil the call is a
// no-op, making event emission fully optional. Errors are logged but never
// propagated because events are best-effort.
func Emit(ctx context.Context, bus EventBus, eventType, scope string, data any) {
	if bus == nil {
		return
	}

	raw, err := json.Marshal(sanitizeForJSON(data))
	if err != nil {
		slog.Warn("events: failed to marshal event data", "event_type", eventType, "err", err)
		return
	}

	event := Event{
		ID:        uuid.New().String(),
		Type:      eventType,
		Scope:     scope,
		Data:      raw,
		Timestamp: time.Now().UTC(),
	}

	if err := bus.Publish(ctx, event); err != nil {
		slog.Warn("events: failed to publish event", "event_type", eventType, "err", err)
	}
}

// EmitProjectUpdated publishes a ProjectUpdated event for the given project.
// It is the single source of truth for the event's scope and payload: the
// dreaming DirtyTracker consumes it (scope "project:<id>" plus a project_id
// payload it can parse) to mark the project dirty so the
// project_description_sync phase reconciles its backing memory. Safe with a
// nil bus.
func EmitProjectUpdated(ctx context.Context, bus EventBus, projectID uuid.UUID) {
	Emit(ctx, bus, ProjectUpdated, "project:"+projectID.String(), map[string]string{
		"project_id": projectID.String(),
	})
}
