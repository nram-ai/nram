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
