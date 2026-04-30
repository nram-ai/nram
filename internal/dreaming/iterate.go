package dreaming

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// iterateMemoriesByNamespace streams every non-deleted memory in a namespace
// through fn, one batch at a time. Pagination uses
// reader.ListByNamespace(limit=batchSize, offset=N*batchSize); termination is
// triggered when a batch returns fewer rows than batchSize (or zero rows).
//
// Memory footprint is bounded to one batch — used by phases that visit every
// memory each cycle but do not need the full namespace resident
// simultaneously (pruning's confidence-decay-and-prune pass is the canonical
// case: O(N) per cycle is the design intent, but holding the full N in
// memory is not).
//
// Idempotency requirement: pagination is offset-based, so concurrent
// inserts/deletes on the underlying table can shift rows between batches —
// the iterator may skip a row or surface it twice across consecutive
// batches. Callers MUST be idempotent on a per-row basis. Pruning's three
// row operations (DecayConfidence clamps to floor, the supersede-with-
// zero-access prune is a no-op on already-soft-deleted rows, the dream-
// source low-confidence prune likewise) all satisfy this; missed rows are
// recovered next cycle (which restarts from offset zero).
//
// batchSize <= 0 falls back to 1000. fn returning a non-nil error aborts
// iteration; the error is propagated. Context cancellation between batches
// is honored.
func iterateMemoriesByNamespace(
	ctx context.Context,
	reader MemoryReader,
	namespaceID uuid.UUID,
	batchSize int,
	fn func([]model.Memory) error,
) error {
	if reader == nil {
		return fmt.Errorf("iterateMemoriesByNamespace: nil reader")
	}
	if fn == nil {
		return fmt.Errorf("iterateMemoriesByNamespace: nil callback")
	}
	if batchSize <= 0 {
		batchSize = 1000
	}

	offset := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := reader.ListByNamespace(ctx, namespaceID, batchSize, offset)
		if err != nil {
			return fmt.Errorf("iterate namespace at offset %d: %w", offset, err)
		}
		if len(batch) == 0 {
			return nil
		}
		if err := fn(batch); err != nil {
			return err
		}
		if len(batch) < batchSize {
			return nil
		}
		offset += batchSize
	}
}
