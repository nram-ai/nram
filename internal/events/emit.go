package events

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
)

// Emit publishes an event to the given EventBus. If bus is nil the call is a
// no-op, making event emission fully optional. Errors are logged but never
// propagated because events are best-effort.
func Emit(ctx context.Context, bus EventBus, eventType, scope string, data interface{}) {
	if bus == nil {
		return
	}

	raw, err := json.Marshal(data)
	if err != nil {
		log.Printf("events: failed to marshal event data for %s: %v", eventType, err)
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
		log.Printf("events: failed to publish %s event: %v", eventType, err)
	}
}
