package dreaming

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
	"github.com/nram-ai/nram/internal/storage/hnsw"
)

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
	settings      SettingsResolver
}

// NewEntityDedupPhase creates a new entity deduplication phase. vectorStore
// may be nil; in that case the phase degrades to text-only matching. settings
// may be nil; the threshold falls through to the registered default in
// service.settingDefaults.
func NewEntityDedupPhase(
	entities EntityReader,
	entityWriter EntityWriter,
	aliases EntityAliasWriter,
	relationships RelationshipReader,
	relWriter RelationshipWriter,
	vectorStore storage.VectorStore,
	settings SettingsResolver,
) *EntityDedupPhase {
	return &EntityDedupPhase{
		entities:      entities,
		entityWriter:  entityWriter,
		aliases:       aliases,
		relationships: relationships,
		relWriter:     relWriter,
		vectorStore:   vectorStore,
		settings:      settings,
	}
}

func (p *EntityDedupPhase) Name() string { return model.DreamPhaseEntityDedup }

func (p *EntityDedupPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) (PhaseResult, error) {
	entities, err := p.entities.ListByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return PhaseResult{}, err
	}

	if len(entities) < 2 {
		return PhaseResult{}, nil
	}

	// Group entities by type for more targeted dedup.
	byType := make(map[string][]model.Entity)
	for _, e := range entities {
		byType[e.EntityType] = append(byType[e.EntityType], e)
	}

	threshold := service.GetDefaultFloat(service.SettingDreamEntityMergeThreshold)
	if p.settings != nil {
		threshold = p.settings.ResolveFloatWithDefault(ctx,
			service.SettingDreamEntityMergeThreshold, "global")
	}

	for entityType, group := range byType {
		if len(group) < 2 {
			continue
		}

		merged := p.findAndMergeDuplicates(ctx, cycle, group, entityType, logger, threshold)
		if merged > 0 {
			slog.Info("dreaming: entity dedup merged entities",
				"type", entityType, "merged", merged, "cycle", cycle.ID)
		}
	}

	return PhaseResult{}, nil
}

func (p *EntityDedupPhase) findAndMergeDuplicates(
	ctx context.Context,
	cycle *model.DreamCycle,
	entities []model.Entity,
	entityType string,
	logger *DreamLogWriter,
	threshold float64,
) int {
	merged := 0

	vectorsByID, normsByID := p.preloadVectors(ctx, entities)
	consumed := make(map[uuid.UUID]bool)

	tracker := CycleTrackerFromContext(ctx)
	progressStep := progressEmitStep(len(entities))
	progressLabel := "entities[" + entityType + "]"

	for i := 0; i < len(entities); i++ {
		if shouldEmitProgress(i, len(entities), progressStep) {
			if tracker != nil {
				tracker.EmitPhaseProgress(ctx, i+1, len(entities), progressLabel)
			}
			slog.Info("dreaming: entity dedup progress",
				"cycle", cycle.ID, "type", entityType,
				"entity", i+1, "of", len(entities))
		}
		if consumed[entities[i].ID] {
			continue
		}
		primary := &entities[i]

		for j := i + 1; j < len(entities); j++ {
			if consumed[entities[j].ID] {
				continue
			}
			candidate := &entities[j]

			if !p.shouldMerge(primary, candidate, vectorsByID, normsByID, threshold) {
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
// similarity over the preloaded vectors. threshold is the resolved value of
// SettingDreamEntityMergeThreshold for this phase invocation.
func (p *EntityDedupPhase) shouldMerge(a, b *model.Entity, vectorsByID map[uuid.UUID][]float32, normsByID map[uuid.UUID]float32, threshold float64) bool {
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
	return float64(sim) >= threshold
}

// mergeEntities absorbs candidate into primary: retargets candidate's
// relationships, then creates the alias and increments primary's mention
// count. Order matters: relationship migration runs FIRST so a failure
// there does not leave a half-applied merge (alias plus inflated mention
// count without the rel work). EntityMerged is logged only after every
// step succeeds, so rollback never sees a merge record for work that
// did not actually commit.
func (p *EntityDedupPhase) mergeEntities(
	ctx context.Context,
	cycle *model.DreamCycle,
	primary, candidate *model.Entity,
	logger *DreamLogWriter,
) error {
	rels, err := p.relationships.ListByEntity(ctx, candidate.ID)
	if err != nil {
		return err
	}

	// Group expires by their own NamespaceID. ListByEntity has no
	// namespace filter, so a candidate referenced by a cross-namespace
	// shared edge would otherwise have that edge silently survive the
	// merge (BatchExpire filters by namespace_id and would not match).
	// Retargeted rels keep their source rel's namespace_id, so
	// BatchCreate's per-row namespace_id handles them without grouping.
	expireByNS := map[uuid.UUID][]uuid.UUID{}
	newRels := make([]*model.Relationship, 0, len(rels))
	for _, rel := range rels {
		newRel := rel
		if rel.SourceID == candidate.ID {
			newRel.SourceID = primary.ID
		}
		if rel.TargetID == candidate.ID {
			newRel.TargetID = primary.ID
		}

		if newRel.SourceID == newRel.TargetID {
			expireByNS[rel.NamespaceID] = append(expireByNS[rel.NamespaceID], rel.ID)
			continue
		}

		expireByNS[rel.NamespaceID] = append(expireByNS[rel.NamespaceID], rel.ID)
		newRel.ID = uuid.New()
		newRels = append(newRels, &newRel)
	}

	for ns, ids := range expireByNS {
		if _, err := p.relWriter.BatchExpire(ctx, ns, ids); err != nil {
			slog.Warn("dreaming: batch expire relationships failed", "candidate", candidate.ID, "ns", ns, "err", err)
			return fmt.Errorf("entity dedup batch expire: %w", err)
		}
	}

	if len(newRels) > 0 {
		if _, err := p.relWriter.BatchCreate(ctx, newRels); err != nil {
			slog.Warn("dreaming: batch create retargeted relationships failed", "candidate", candidate.ID, "err", err)
			return fmt.Errorf("entity dedup batch create: %w", err)
		}
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

	if err := logger.LogOperation(ctx, model.DreamPhaseEntityDedup, "",
		model.DreamOpEntityMerged, "entity", candidate.ID, candidate, primary); err != nil {
		return err
	}

	// LogOperation only for rels that actually persisted. BatchCreate
	// sets rel.ID to uuid.Nil for rows it skipped via per-row constraint
	// fallback, so checking the sentinel filters out phantom log entries
	// rollback would otherwise chase via BatchDeleteByID.
	for _, rel := range newRels {
		if rel.ID == uuid.Nil {
			continue
		}
		if err := logger.LogOperation(ctx, model.DreamPhaseEntityDedup, "",
			model.DreamOpRelationshipCreated, "relationship", rel.ID, nil, rel); err != nil {
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
