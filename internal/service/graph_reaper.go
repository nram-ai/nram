package service

import (
	"context"

	"github.com/google/uuid"
)

// GraphReaper removes knowledge-graph data whose sourcing memory is gone and
// keeps entity mention_count consistent with surviving provenance. It is the
// shared engine behind three call sites: the forget hard-delete path and the
// supersede path (which reap one memory's exclusively-sourced footprint), and
// the lifecycle sweep / console repair (which reap the accumulated backlog of
// lost-provenance edges). All operations are safe to run repeatedly — a
// lost-provenance edge can never be tied back to a live memory, so reaping it
// never destroys live-sourced data.
type GraphReaper interface {
	// ReapMemoryFootprint deletes the relationships a single memory
	// exclusively sourced and recomputes the affected entities' mention_count
	// from surviving provenance. Returns the number of entities whose counts
	// were recomputed. Call before a hard delete (or after a supersede) so the
	// FK ON DELETE SET NULL does not first erase the provenance link.
	ReapMemoryFootprint(ctx context.Context, namespaceID, memoryID uuid.UUID) (int, error)
	// ReapLostProvenance deletes every lost-provenance edge (batched) and then
	// recomputes every entity's mention_count. Returns edges deleted.
	ReapLostProvenance(ctx context.Context) (int64, error)
	// CountLostProvenance reports how many lost-provenance edges exist, for the
	// console graph-health display.
	CountLostProvenance(ctx context.Context) (int64, error)
}

// relationshipReaper is the relationship-repo surface GraphReaper needs.
type relationshipReaper interface {
	DeleteBySourceMemory(ctx context.Context, namespaceID, memoryID uuid.UUID) ([]uuid.UUID, error)
	DeleteByLostProvenance(ctx context.Context, limit int) (int64, error)
	CountLostProvenance(ctx context.Context) (int64, error)
}

// entityRecomputer is the entity-repo surface GraphReaper needs.
type entityRecomputer interface {
	RecomputeMentionCounts(ctx context.Context, ids []uuid.UUID) (int64, error)
}

// lostProvenanceBatch bounds one DeleteByLostProvenance statement so the reap
// loop never holds a lock over the whole (potentially large) backlog at once.
const lostProvenanceBatch = 1000

type graphReaperAdapter struct {
	relationships relationshipReaper
	entities      entityRecomputer
}

// NewGraphReaper wires the relationship and entity repos into a GraphReaper.
func NewGraphReaper(relationships relationshipReaper, entities entityRecomputer) GraphReaper {
	return &graphReaperAdapter{relationships: relationships, entities: entities}
}

func (a *graphReaperAdapter) ReapMemoryFootprint(ctx context.Context, namespaceID, memoryID uuid.UUID) (int, error) {
	affected, err := a.relationships.DeleteBySourceMemory(ctx, namespaceID, memoryID)
	if err != nil {
		return 0, err
	}
	if len(affected) > 0 {
		if _, err := a.entities.RecomputeMentionCounts(ctx, affected); err != nil {
			return len(affected), err
		}
	}
	return len(affected), nil
}

func (a *graphReaperAdapter) ReapLostProvenance(ctx context.Context) (int64, error) {
	var total int64
	for {
		n, err := a.relationships.DeleteByLostProvenance(ctx, lostProvenanceBatch)
		if err != nil {
			return total, err
		}
		total += n
		if n < lostProvenanceBatch {
			break
		}
	}
	if total > 0 {
		if _, err := a.entities.RecomputeMentionCounts(ctx, nil); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (a *graphReaperAdapter) CountLostProvenance(ctx context.Context) (int64, error) {
	return a.relationships.CountLostProvenance(ctx)
}
