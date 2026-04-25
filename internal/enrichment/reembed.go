package enrichment

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// ReembedEntitiesResult summarizes the outcome of a ReembedAllEntities run.
// Used by both the cascade handler and the operator-facing CLI so progress
// can be surfaced in either context.
type ReembedEntitiesResult struct {
	Total       int
	Reembedded  int
	Skipped     int
	Errors      int
	DurationMs  int64
	LastErrText string
}

// ReembedAllEntities walks every entity across every namespace and
// re-embeds each one's canonical name through the configured embedder,
// upserting the new vector into the entity vector store and stamping the
// entity row with the new dim. Used by the embedding-model switch cascade
// (entities are not driven by the enrichment_queue, so they need their
// own re-embed loop).
//
// Batches are sized to embedInputCap (matches the worker pool's per-call
// cap so we share the same embedder concurrency budget). Within each
// batch a single Embed call returns all vectors; on partial failure the
// batch counts as an error and is skipped (operator can re-run the loop;
// the next pass picks up where this one left off because successfully
// stamped entities have a non-NULL embedding_dim).
//
// Returns counts and a short error description from the last failed
// batch. A non-nil error is returned only on infrastructure failure
// (database unreachable mid-walk); per-batch embed errors are accumulated
// in the result and the loop continues so a single broken batch does not
// halt the entire migration.
func ReembedAllEntities(
	ctx context.Context,
	repo *storage.EntityRepo,
	vectorStore storage.VectorStore,
	embedder provider.EmbeddingProvider,
) (*ReembedEntitiesResult, error) {
	if repo == nil || vectorStore == nil || embedder == nil {
		return nil, fmt.Errorf("reembed entities: repo, vectorStore, and embedder are required")
	}

	start := time.Now()
	result := &ReembedEntitiesResult{}
	pageSize := embedInputCap
	offset := 0

	for {
		entities, err := repo.ListAll(ctx, pageSize, offset)
		if err != nil {
			return result, fmt.Errorf("reembed entities: list page offset=%d: %w", offset, err)
		}
		if len(entities) == 0 {
			break
		}
		result.Total += len(entities)

		inputs := make([]string, 0, len(entities))
		batch := make([]struct {
			id  uuid.UUID
			nid uuid.UUID
		}, 0, len(entities))
		for i := range entities {
			if entities[i].Canonical == "" {
				result.Skipped++
				continue
			}
			inputs = append(inputs, entities[i].Canonical)
			batch = append(batch, struct {
				id  uuid.UUID
				nid uuid.UUID
			}{id: entities[i].ID, nid: entities[i].NamespaceID})
		}
		if len(inputs) == 0 {
			offset += len(entities)
			continue
		}

		embedCtx, cancel := context.WithTimeout(ctx, workerEmbedTimeout)
		resp, embErr := embedder.Embed(embedCtx, &provider.EmbeddingRequest{Input: inputs})
		cancel()
		if embErr != nil || resp == nil || len(resp.Embeddings) != len(inputs) {
			result.Errors += len(inputs)
			if embErr != nil {
				result.LastErrText = embErr.Error()
				slog.Warn("reembed entities: batch embed failed",
					"offset", offset, "batch", len(inputs), "err", embErr)
			} else {
				result.LastErrText = fmt.Sprintf("response length mismatch: got %d want %d",
					len(resp.Embeddings), len(inputs))
				slog.Warn("reembed entities: batch embed length mismatch",
					"offset", offset, "got", len(resp.Embeddings), "want", len(inputs))
			}
			offset += len(entities)
			continue
		}

		// All vectors in a batch share a width — embedders return fixed
		// dim per call. Capture the first non-zero width as the batch
		// dim; any item with a different (or zero) length is skipped.
		batchDim := 0
		for _, vec := range resp.Embeddings {
			if d := len(vec); d > 0 {
				batchDim = d
				break
			}
		}
		if batchDim == 0 {
			result.Skipped += len(inputs)
			offset += len(entities)
			continue
		}

		items := make([]storage.VectorUpsertItem, 0, len(inputs))
		ids := make([]uuid.UUID, 0, len(inputs))
		for i, b := range batch {
			vec := resp.Embeddings[i]
			if len(vec) != batchDim {
				result.Skipped++
				continue
			}
			items = append(items, storage.VectorUpsertItem{
				Kind:        storage.VectorKindEntity,
				ID:          b.id,
				NamespaceID: b.nid,
				Embedding:   vec,
				Dimension:   batchDim,
			})
			ids = append(ids, b.id)
		}
		if len(items) == 0 {
			offset += len(entities)
			continue
		}

		if err := vectorStore.UpsertBatch(ctx, items); err != nil {
			result.Errors += len(items)
			result.LastErrText = err.Error()
			slog.Warn("reembed entities: vector upsert failed",
				"offset", offset, "batch", len(items), "err", err)
			offset += len(entities)
			continue
		}

		if err := repo.UpdateEmbeddingDimBatch(ctx, ids, batchDim); err != nil {
			result.Errors += len(ids)
			result.LastErrText = err.Error()
			slog.Warn("reembed entities: embedding_dim stamp failed",
				"offset", offset, "dim", batchDim, "batch", len(ids), "err", err)
			offset += len(entities)
			continue
		}
		result.Reembedded += len(ids)

		offset += len(entities)
	}

	result.DurationMs = time.Since(start).Milliseconds()
	slog.Info("reembed entities: complete",
		"total", result.Total, "reembedded", result.Reembedded,
		"skipped", result.Skipped, "errors", result.Errors,
		"duration_ms", result.DurationMs)
	return result, nil
}
