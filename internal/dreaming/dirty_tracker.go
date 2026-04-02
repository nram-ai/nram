// Package dreaming provides the background "dreaming" system that
// cross-references memories within a project to improve the knowledge graph.
package dreaming

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// DirtyTracker subscribes to memory lifecycle events on the EventBus and
// marks projects as dirty when user-originated changes occur. Dream-originated
// changes (source = "dream") are ignored to prevent feedback loops.
type DirtyTracker struct {
	bus       events.EventBus
	dirtyRepo *storage.DreamDirtyRepo

	cancel func()
}

// NewDirtyTracker creates a new DirtyTracker.
func NewDirtyTracker(bus events.EventBus, dirtyRepo *storage.DreamDirtyRepo) *DirtyTracker {
	return &DirtyTracker{
		bus:       bus,
		dirtyRepo: dirtyRepo,
	}
}

// Start subscribes to memory events and begins processing in a background goroutine.
func (dt *DirtyTracker) Start(ctx context.Context) error {
	ch, unsub, err := dt.bus.Subscribe(ctx, "")
	if err != nil {
		return err
	}
	dt.cancel = unsub

	go dt.loop(ctx, ch)
	return nil
}

// Stop cancels the subscription and stops the tracker.
func (dt *DirtyTracker) Stop() {
	if dt.cancel != nil {
		dt.cancel()
		dt.cancel = nil
	}
}

func (dt *DirtyTracker) loop(ctx context.Context, ch <-chan events.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			dt.handleEvent(ctx, event)
		}
	}
}

func (dt *DirtyTracker) handleEvent(ctx context.Context, event events.Event) {
	// Only care about memory lifecycle events.
	switch event.Type {
	case events.MemoryCreated, events.MemoryUpdated, events.MemoryDeleted:
	default:
		return
	}

	// Parse event data to check source field.
	var data map[string]string
	if err := json.Unmarshal(event.Data, &data); err != nil {
		slog.Warn("dreaming: dirty tracker failed to parse event data", "err", err)
		return
	}

	// Skip dream-originated changes to prevent feedback loops.
	if data["source"] == model.DreamSource {
		return
	}

	// Extract project ID from event data or scope.
	projectIDStr := data["project_id"]
	if projectIDStr == "" {
		// Try to extract from scope (format: "project:<uuid>").
		projectIDStr = parseProjectIDFromScope(event.Scope)
	}
	if projectIDStr == "" {
		return
	}

	projectID, err := uuid.Parse(projectIDStr)
	if err != nil {
		return
	}

	if err := dt.dirtyRepo.MarkDirty(ctx, projectID); err != nil {
		slog.Error("dreaming: dirty tracker failed to mark project dirty",
			"project_id", projectID, "err", err)
	}
}

func parseProjectIDFromScope(scope string) string {
	if strings.HasPrefix(scope, "project:") {
		return strings.TrimPrefix(scope, "project:")
	}
	return ""
}
