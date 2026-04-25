package dreaming

import (
	"context"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
	"github.com/nram-ai/nram/internal/storage/hnsw"
)

const entityMergeCosineThreshold = 0.92

// EntityDedupPhase merges near-duplicate entities within a namespace.
// It compares entities first by canonical/normalized text and known suffix
// variants, then by cosine similarity over their stored entity vectors when
// the vector store is attached and both sides have an embedding_dim recorded.
// Aliases and relationships are retargeted onto the surviving entity.
type EntityDedupPhase struct {
	entities      EntityReader
	entityWriter  EntityWriter
	aliases       EntityAliasWriter
	relationships RelationshipReader
	relWriter     RelationshipWriter
	vectorStore   storage.VectorStore
}

// NewEntityDedupPhase creates a new entity deduplication phase. vectorStore
// may be nil; in that case the phase degrades to text-only matching.
func NewEntityDedupPhase(
	entities EntityReader,
	entityWriter EntityWriter,
	aliases EntityAliasWriter,
	relationships RelationshipReader,
	relWriter RelationshipWriter,
	vectorStore storage.VectorStore,
) *EntityDedupPhase {
	return &EntityDedupPhase{
		entities:      entities,
		entityWriter:  entityWriter,
		aliases:       aliases,
		relationships: relationships,
		relWriter:     relWriter,
		vectorStore:   vectorStore,
	}
}

func (p *EntityDedupPhase) Name() string { return model.DreamPhaseEntityDedup }

func (p *EntityDedupPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (bool, error) {
	entities, err := p.entities.ListByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return false, err
	}

	if len(entities) < 2 {
		return false, nil
	}

	// Group entities by type for more targeted dedup.
	byType := make(map[string][]model.Entity)
	for _, e := range entities {
		byType[e.EntityType] = append(byType[e.EntityType], e)
	}

	for entityType, group := range byType {
		if len(group) < 2 {
			continue
		}

		merged := p.findAndMergeDuplicates(ctx, cycle, group, entityType, logger)
		if merged > 0 {
			slog.Info("dreaming: entity dedup merged entities",
				"type", entityType, "merged", merged, "cycle", cycle.ID)
		}
	}

	return false, nil
}

func (p *EntityDedupPhase) findAndMergeDuplicates(
	ctx context.Context,
	cycle *model.DreamCycle,
	entities []model.Entity,
	entityType string,
	logger *DreamLogWriter,
) int {
	merged := 0

	vectorsByID, normsByID := p.preloadVectors(ctx, entities)
	consumed := make(map[uuid.UUID]bool)

	for i := 0; i < len(entities); i++ {
		if consumed[entities[i].ID] {
			continue
		}
		primary := &entities[i]

		for j := i + 1; j < len(entities); j++ {
			if consumed[entities[j].ID] {
				continue
			}
			candidate := &entities[j]

			if !p.shouldMerge(primary, candidate, vectorsByID, normsByID) {
				continue
			}

			if err := p.mergeEntities(ctx, cycle, primary, candidate, logger); err != nil {
				slog.Error("dreaming: entity merge failed",
					"primary", primary.ID, "candidate", candidate.ID, "err", err)
				continue
			}

			consumed[candidate.ID] = true
			merged++
		}
	}
	return merged
}

// preloadVectors batches GetByIDs once per (kind=entity, dim) and precomputes
// the L2 norm of each loaded vector. Norms are cached so the O(n^2) shouldMerge
// loop computes O(n) norms instead of O(n^2). Returns nil maps when no vector
// store is attached or no entities have a dim recorded; callers degrade to
// text-only matching.
func (p *EntityDedupPhase) preloadVectors(ctx context.Context, entities []model.Entity) (map[uuid.UUID][]float32, map[uuid.UUID]float32) {
	if p.vectorStore == nil {
		return nil, nil
	}

	byDim := make(map[int][]uuid.UUID)
	for _, e := range entities {
		if e.EmbeddingDim == nil {
			continue
		}
		byDim[*e.EmbeddingDim] = append(byDim[*e.EmbeddingDim], e.ID)
	}
	if len(byDim) == 0 {
		return nil, nil
	}

	vecs := make(map[uuid.UUID][]float32)
	norms := make(map[uuid.UUID]float32)
	for dim, ids := range byDim {
		got, err := p.vectorStore.GetByIDs(ctx, storage.VectorKindEntity, ids, dim)
		if err != nil {
			slog.Warn("dreaming: entity vector preload failed; vector fallback unavailable for this dim",
				"dim", dim, "ids", len(ids), "err", err)
			continue
		}
		for k, v := range got {
			vecs[k] = v
			norms[k] = hnsw.Norm(v)
		}
	}
	return vecs, norms
}

// shouldMerge runs text-matching branches first, then falls back to cosine
// similarity over the preloaded vectors.
func (p *EntityDedupPhase) shouldMerge(a, b *model.Entity, vectorsByID map[uuid.UUID][]float32, normsByID map[uuid.UUID]float32) bool {
	if a.Canonical == b.Canonical {
		return true
	}

	normA := normalizeForDedup(a.Canonical)
	normB := normalizeForDedup(b.Canonical)
	if normA == normB {
		return true
	}

	if (strings.Contains(normA, normB) || strings.Contains(normB, normA)) && isVariantSuffix(normA, normB) {
		return true
	}

	// Vector-similarity fallback. A dim mismatch (mid-migration after switching
	// providers) is treated as no-match so we never compare vectors of
	// incompatible shape.
	if a.EmbeddingDim == nil || b.EmbeddingDim == nil {
		return false
	}
	if *a.EmbeddingDim != *b.EmbeddingDim {
		return false
	}
	aVec, aOK := vectorsByID[a.ID]
	bVec, bOK := vectorsByID[b.ID]
	if !aOK || !bOK {
		return false
	}
	sim := hnsw.CosineSimilarityWithNorms(aVec, bVec, normsByID[a.ID], normsByID[b.ID])
	return sim >= entityMergeCosineThreshold
}

// mergeEntities absorbs candidate into primary: creates alias, increments
// mention count, and retargets candidate's relationships.
func (p *EntityDedupPhase) mergeEntities(
	ctx context.Context,
	cycle *model.DreamCycle,
	primary, candidate *model.Entity,
	logger *DreamLogWriter,
) error {
	if err := logger.LogOperation(ctx, model.DreamPhaseEntityDedup,
		model.DreamOpEntityMerged, "entity", candidate.ID, candidate, primary); err != nil {
		return err
	}

	alias := &model.EntityAlias{
		ID:          uuid.New(),
		NamespaceID: primary.NamespaceID,
		EntityID:    primary.ID,
		Alias:       candidate.Name,
		AliasType:   "dream_dedup",
	}
	if err := p.aliases.Create(ctx, alias); err != nil {
		slog.Warn("dreaming: alias creation failed (may already exist)", "err", err)
	}

	primary.MentionCount += candidate.MentionCount
	if err := p.entityWriter.Upsert(ctx, primary); err != nil {
		return err
	}

	rels, err := p.relationships.ListByEntity(ctx, candidate.ID)
	if err != nil {
		return err
	}

	for _, rel := range rels {
		newRel := rel
		if rel.SourceID == candidate.ID {
			newRel.SourceID = primary.ID
		}
		if rel.TargetID == candidate.ID {
			newRel.TargetID = primary.ID
		}

		if newRel.SourceID == newRel.TargetID {
			if err := p.relWriter.Expire(ctx, rel.ID, rel.NamespaceID); err != nil {
				slog.Warn("dreaming: expire self-referential relationship failed", "err", err)
			}
			continue
		}

		if err := p.relWriter.Expire(ctx, rel.ID, rel.NamespaceID); err != nil {
			slog.Warn("dreaming: expire relationship failed", "rel", rel.ID, "err", err)
			continue
		}

		newRel.ID = uuid.New()
		if err := p.relWriter.Create(ctx, &newRel); err != nil {
			slog.Warn("dreaming: create retargeted relationship failed", "err", err)
			continue
		}

		if err := logger.LogOperation(ctx, model.DreamPhaseEntityDedup,
			model.DreamOpRelationshipCreated, "relationship", newRel.ID, nil, &newRel); err != nil {
			slog.Warn("dreaming: log relationship retarget failed", "err", err)
		}
	}

	return nil
}

func normalizeForDedup(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func isVariantSuffix(a, b string) bool {
	longer, shorter := a, b
	if len(b) > len(a) {
		longer, shorter = b, a
	}

	diff := strings.TrimPrefix(longer, shorter)
	if diff == "" {
		return true
	}

	commonSuffixes := []string{"js", "lang", "lib", "framework", "tool", "app", "cli", "sdk"}
	for _, suffix := range commonSuffixes {
		if diff == suffix {
			return true
		}
	}
	return false
}
