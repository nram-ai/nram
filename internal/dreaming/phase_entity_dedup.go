package dreaming

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
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

	// Hygiene first: delete any entity whose name is degenerate (a wall of text,
	// a whole sentence, or a repetition loop) by the same predicate the
	// write-path guard uses, so a name created before that guard existed
	// self-cleans. Dedup then runs over the survivors.
	entities = p.sweepDegenerateNames(ctx, cycle, entities, logger)

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

// sweepDegenerateNames deletes entities whose name fails the entity-name guard
// predicate and returns the survivors. Gated by dreaming.entity_hygiene_enabled
// (default on); each deletion cascades to the entity's vectors, relationships,
// and aliases (FK ON DELETE CASCADE) and is recorded in the dream log. On a
// delete error the original set is returned unchanged so dedup still runs.
func (p *EntityDedupPhase) sweepDegenerateNames(ctx context.Context, cycle *model.DreamCycle, entities []model.Entity, logger *DreamLogWriter) []model.Entity {
	if p.settings == nil || !p.settings.ResolveBoolWithDefault(ctx, service.SettingDreamEntityHygieneEnabled, "global") {
		return entities
	}
	maxChars := p.settings.ResolveIntWithDefault(ctx, service.SettingExtractionEntityNameMaxChars, "global")
	maxWords := p.settings.ResolveIntWithDefault(ctx, service.SettingExtractionEntityNameMaxWords, "global")
	minRatio := p.settings.ResolveFloatWithDefault(ctx, service.SettingExtractionEntityNameMinDistinctWordRatio, "global")

	kept := make([]model.Entity, 0, len(entities))
	var bad []model.Entity
	var badIDs []uuid.UUID
	for _, e := range entities {
		if service.IsDegenerateEntityName(e.Name, maxChars, maxWords, minRatio) {
			bad = append(bad, e)
			badIDs = append(badIDs, e.ID)
			continue
		}
		kept = append(kept, e)
	}
	if len(badIDs) == 0 {
		return entities
	}

	deleted, err := p.entityWriter.DeleteByIDs(ctx, badIDs)
	if err != nil {
		slog.Warn("dreaming: entity hygiene delete failed",
			"cycle", cycle.ID, "count", len(badIDs), "err", err)
		return entities
	}
	for i := range bad {
		if err := logger.LogOperation(ctx, model.DreamPhaseEntityDedup, "entity_hygiene",
			model.DreamOpEntityDeleted, "entity", bad[i].ID, bad[i], nil); err != nil {
			slog.Warn("dreaming: log entity hygiene delete failed", "err", err)
		}
	}
	slog.Info("dreaming: entity hygiene removed degenerate names",
		"cycle", cycle.ID, "deleted", len(deleted))
	return kept
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

	// Merge toward the higher-mention-count canonical: sorting the group by
	// mention_count desc means the survivor (primary, the lower index) is always
	// the more-established entity. This fixes the inversion where a mangled
	// low-count variant (e.g. "Brandonlehmann") absorbed the real "Brandon".
	slices.SortStableFunc(entities, func(a, b model.Entity) int {
		return b.MentionCount - a.MentionCount
	})

	vectorsByID, normsByID := p.preloadVectors(ctx, entities)
	consumed := make(map[uuid.UUID]bool)
	// memCache memoizes each entity's source-memory set for the name-variant
	// co-occurrence guard, scoped to this dedup group.
	memCache := make(map[uuid.UUID]map[uuid.UUID]struct{})

	tracker := CycleTrackerFromContext(ctx)
	progressStep := progressEmitStep(len(entities))
	progressLabel := "entities[" + entityType + "]"

	for i := range entities {
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

			if !p.shouldMerge(primary, candidate, vectorsByID, normsByID, threshold) &&
				!p.shouldMergeNameVariant(ctx, entityType, primary, candidate, memCache) {
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
	var nilDim []uuid.UUID
	for _, e := range entities {
		if e.EmbeddingDim == nil {
			// The scalar may be NULL while a vector still exists (historical
			// Upsert clobber). Probe for these below rather than skipping them,
			// so a stale scalar can no longer silently disable dedup.
			nilDim = append(nilDim, e.ID)
			continue
		}
		byDim[*e.EmbeddingDim] = append(byDim[*e.EmbeddingDim], e.ID)
	}
	if len(byDim) == 0 && len(nilDim) == 0 {
		return nil, nil
	}

	vecs := make(map[uuid.UUID][]float32)
	norms := make(map[uuid.UUID]float32)
	load := func(dim int, ids []uuid.UUID) {
		if len(ids) == 0 {
			return
		}
		got, err := p.vectorStore.GetByIDs(ctx, storage.VectorKindEntity, ids, dim)
		if err != nil {
			slog.Warn("dreaming: entity vector preload failed; vector fallback unavailable for this dim",
				"dim", dim, "ids", len(ids), "err", err)
			return
		}
		for k, v := range got {
			vecs[k] = v
			norms[k] = hnsw.Norm(v)
		}
	}
	for dim, ids := range byDim {
		load(dim, ids)
	}
	// Recover vectors for entities whose embedding_dim scalar is NULL by probing
	// each known dim; an entity resolves on the dim whose table holds its vector.
	// In practice an instance runs one embedding model, so nearly all resolve on
	// the first probed dim. Drop resolved IDs so later probes shrink to nothing.
	pending := nilDim
	for _, dim := range storage.OrderedVectorDimensions {
		if len(pending) == 0 {
			break
		}
		load(dim, pending)
		next := pending[:0]
		for _, id := range pending {
			if _, ok := vecs[id]; !ok {
				next = append(next, id)
			}
		}
		pending = next
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

	// Vector-similarity fallback. Gate on the actual presence and shape of the
	// preloaded vectors, NOT the entities.embedding_dim scalar: that scalar was
	// historically clobbered to NULL on re-mention while the vector persisted,
	// which silently disabled this branch for the most-mentioned entities.
	// preloadVectors recovers vectors even when the scalar is NULL, so a vector
	// present here is authoritative. Unequal length means incompatible shape
	// (mid-migration after a provider switch); treat as no-match.
	aVec, aOK := vectorsByID[a.ID]
	bVec, bOK := vectorsByID[b.ID]
	if !aOK || !bOK || len(aVec) == 0 || len(aVec) != len(bVec) {
		return false
	}
	sim := hnsw.CosineSimilarityWithNorms(aVec, bVec, normsByID[a.ID], normsByID[b.ID])
	return float64(sim) >= threshold
}

// isTokenPrefixVariant reports whether one canonical name is a strict
// whitespace-token prefix of the other (first-name vs full-name, e.g.
// "brandon" / "brandon lehmann"). Surname-only ("lehmann") does NOT match a
// full name, which keeps the rule biased to the first-name case.
func isTokenPrefixVariant(a, b string) bool {
	ta := strings.Fields(a)
	tb := strings.Fields(b)
	if len(ta) == 0 || len(tb) == 0 || len(ta) == len(tb) {
		return false
	}
	short, long := ta, tb
	if len(ta) > len(tb) {
		short, long = tb, ta
	}
	for i := range short {
		if short[i] != long[i] {
			return false
		}
	}
	return true
}

// shouldMergeNameVariant merges a person whose name is a first-name/full-name
// token-prefix variant of another, guarded by co-occurrence in at least one
// shared source memory so two distinct people who share a first name are not
// merged. Restricted to the person type (entities are pre-grouped by type, so
// the caller passes the group's type) because the prefix heuristic is unsafe on
// code symbols / concepts ("User" vs "User Service"). The cosine path already
// handles cases where the vectors agree; this catches the ones where the short
// and full name embed too far apart to clear the threshold.
func (p *EntityDedupPhase) shouldMergeNameVariant(ctx context.Context, entityType string, a, b *model.Entity, memCache map[uuid.UUID]map[uuid.UUID]struct{}) bool {
	if entityType != "person" {
		return false
	}
	if !isTokenPrefixVariant(a.Canonical, b.Canonical) {
		return false
	}
	return p.coOccur(ctx, a, b, memCache)
}

// coOccur reports whether a and b share at least one source memory among their
// relationships. memCache memoizes each entity's source-memory set within a
// dedup group so the lookup runs at most once per entity.
func (p *EntityDedupPhase) coOccur(ctx context.Context, a, b *model.Entity, memCache map[uuid.UUID]map[uuid.UUID]struct{}) bool {
	am := p.sourceMemories(ctx, a, memCache)
	if len(am) == 0 {
		return false
	}
	for m := range p.sourceMemories(ctx, b, memCache) {
		if _, ok := am[m]; ok {
			return true
		}
	}
	return false
}

func (p *EntityDedupPhase) sourceMemories(ctx context.Context, e *model.Entity, memCache map[uuid.UUID]map[uuid.UUID]struct{}) map[uuid.UUID]struct{} {
	if s, ok := memCache[e.ID]; ok {
		return s
	}
	s := make(map[uuid.UUID]struct{})
	rels, err := p.relationships.ListByEntity(ctx, e.ID, []uuid.UUID{e.NamespaceID})
	if err != nil {
		slog.Warn("dreaming: name-variant co-occurrence lookup failed", "entity", e.ID, "err", err)
		memCache[e.ID] = s
		return s
	}
	for _, r := range rels {
		if r.SourceMemory != nil {
			s[*r.SourceMemory] = struct{}{}
		}
	}
	memCache[e.ID] = s
	return s
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
	_ *model.DreamCycle,
	primary, candidate *model.Entity,
	logger *DreamLogWriter,
) error {
	// candidate and primary are duplicates within one namespace, so the
	// candidate's edges all live in candidate.NamespaceID; the bounded read
	// returns exactly those. Retargeted rels keep their source namespace_id,
	// which equals candidate.NamespaceID, so a single per-namespace expire
	// covers the merge.
	rels, err := p.relationships.ListByEntity(ctx, candidate.ID, []uuid.UUID{candidate.NamespaceID})
	if err != nil {
		return err
	}

	expireIDs := make([]uuid.UUID, 0, len(rels))
	newRels := make([]*model.Relationship, 0, len(rels))
	for _, rel := range rels {
		newRel := rel
		if rel.SourceID == candidate.ID {
			newRel.SourceID = primary.ID
		}
		if rel.TargetID == candidate.ID {
			newRel.TargetID = primary.ID
		}

		expireIDs = append(expireIDs, rel.ID)
		if newRel.SourceID == newRel.TargetID {
			continue
		}
		newRel.ID = uuid.New()
		newRels = append(newRels, &newRel)
	}

	if len(expireIDs) > 0 {
		if _, err := p.relWriter.BatchExpire(ctx, candidate.NamespaceID, expireIDs); err != nil {
			slog.Warn("dreaming: batch expire relationships failed", "candidate", candidate.ID, "ns", candidate.NamespaceID, "err", err)
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
	return slices.Contains(commonSuffixes, diff)
}
