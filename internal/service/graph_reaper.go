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
// lost-provenance edges). All operations are safe to run repeatedly: a
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
	// recomputes mention_count for exactly the entities those reaped edges
	// touched (not the whole table). Returns edges deleted.
	ReapLostProvenance(ctx context.Context) (int64, error)
	// RecomputeAllMentionCounts re-derives mention_count for every entity across
	// all namespaces from surviving live provenance. This is the full,
	// whole-table self-heal; the operator-triggered RepairGraph runs it after
	// reaping so a deliberate repair re-normalizes the entire graph. Returns
	// entities updated.
	RecomputeAllMentionCounts(ctx context.Context) (int64, error)
	// CountLostProvenance reports how many lost-provenance edges exist, for the
	// console graph-health display.
	CountLostProvenance(ctx context.Context) (int64, error)
}

// relationshipReaper is the relationship-repo surface GraphReaper needs.
type relationshipReaper interface {
	DeleteBySourceMemory(ctx context.Context, namespaceID, memoryID uuid.UUID) ([]uuid.UUID, error)
	DeleteByLostProvenance(ctx context.Context, limit int) ([]uuid.UUID, int64, error)
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
	seen := make(map[uuid.UUID]struct{})
	var affected []uuid.UUID
	for {
		endpoints, n, err := a.relationships.DeleteByLostProvenance(ctx, lostProvenanceBatch)
		if err != nil {
			return total, err
		}
		total += n
		for _, id := range endpoints {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				affected = append(affected, id)
			}
		}
		if n < lostProvenanceBatch {
			break
		}
	}
	// Recompute only the entities the reaped edges touched: deleting an edge can
	// only change its two endpoints' counts, so a scoped recompute is exact. A
	// nil/empty slice would recompute the whole table, so guard on len like
	// ReapMemoryFootprint does.
	if len(affected) > 0 {
		if _, err := a.entities.RecomputeMentionCounts(ctx, affected); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (a *graphReaperAdapter) RecomputeAllMentionCounts(ctx context.Context) (int64, error) {
	return a.entities.RecomputeMentionCounts(ctx, nil)
}

func (a *graphReaperAdapter) CountLostProvenance(ctx context.Context) (int64, error) {
	return a.relationships.CountLostProvenance(ctx)
}
