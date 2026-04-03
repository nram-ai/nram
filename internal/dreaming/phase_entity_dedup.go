package dreaming

import (
	"context"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// EntityDedupPhase merges near-duplicate entities within a namespace.
// It uses canonical string comparison and optional vector similarity
// to identify entities that should be merged, then updates aliases
// and retargets relationships.
type EntityDedupPhase struct {
	entities      EntityReader
	entityWriter  EntityWriter
	aliases       EntityAliasWriter
	relationships RelationshipReader
	relWriter     RelationshipWriter
}

// NewEntityDedupPhase creates a new entity deduplication phase.
func NewEntityDedupPhase(
	entities EntityReader,
	entityWriter EntityWriter,
	aliases EntityAliasWriter,
	relationships RelationshipReader,
	relWriter RelationshipWriter,
) *EntityDedupPhase {
	return &EntityDedupPhase{
		entities:      entities,
		entityWriter:  entityWriter,
		aliases:       aliases,
		relationships: relationships,
		relWriter:     relWriter,
	}
}

func (p *EntityDedupPhase) Name() string { return model.DreamPhaseEntityDedup }

func (p *EntityDedupPhase) Execute(ctx context.Context, cycle *model.DreamCycle, budget *TokenBudget, logger *DreamLogWriter) error {
	entities, err := p.entities.ListByNamespace(ctx, cycle.NamespaceID)
	if err != nil {
		return err
	}

	if len(entities) < 2 {
		return nil
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

	return nil
}

func (p *EntityDedupPhase) findAndMergeDuplicates(
	ctx context.Context,
	cycle *model.DreamCycle,
	entities []model.Entity,
	entityType string,
	logger *DreamLogWriter,
) int {
	merged := 0
	// Track which entities have been consumed by merges.
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

			if !p.shouldMerge(primary, candidate) {
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

// shouldMerge determines if two entities are near-duplicates that should be merged.
func (p *EntityDedupPhase) shouldMerge(a, b *model.Entity) bool {
	// Same canonical name = definite merge.
	if a.Canonical == b.Canonical {
		return true
	}

	// Normalize further: remove common separators and compare.
	normA := normalizeForDedup(a.Canonical)
	normB := normalizeForDedup(b.Canonical)
	if normA == normB {
		return true
	}

	// Check if one is a substring/prefix of the other (e.g., "react" vs "reactjs").
	if strings.Contains(normA, normB) || strings.Contains(normB, normA) {
		// Only if the difference is a common suffix like "js", ".js", "lang", etc.
		if isVariantSuffix(normA, normB) {
			return true
		}
	}

	return false
}

// mergeEntities absorbs candidate into primary: creates alias, increments
// mention count, and retargets candidate's relationships.
func (p *EntityDedupPhase) mergeEntities(
	ctx context.Context,
	cycle *model.DreamCycle,
	primary, candidate *model.Entity,
	logger *DreamLogWriter,
) error {
	// Log the merge operation with before states.
	if err := logger.LogOperation(ctx, model.DreamPhaseEntityDedup,
		model.DreamOpEntityMerged, "entity", candidate.ID, candidate, primary); err != nil {
		return err
	}

	// Create alias from candidate name to primary.
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

	// Increment primary mention count.
	primary.MentionCount += candidate.MentionCount
	if err := p.entityWriter.Upsert(ctx, primary); err != nil {
		return err
	}

	// Retarget candidate's relationships to point to primary.
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

		// Skip self-referential relationships that would result from merge.
		if newRel.SourceID == newRel.TargetID {
			if err := p.relWriter.Expire(ctx, rel.ID, rel.NamespaceID); err != nil {
				slog.Warn("dreaming: expire self-referential relationship failed", "err", err)
			}
			continue
		}

		// Expire old relationship.
		if err := p.relWriter.Expire(ctx, rel.ID, rel.NamespaceID); err != nil {
			slog.Warn("dreaming: expire relationship failed", "rel", rel.ID, "err", err)
			continue
		}

		// Create retargeted relationship.
		newRel.ID = uuid.New()
		if err := p.relWriter.Create(ctx, &newRel); err != nil {
			slog.Warn("dreaming: create retargeted relationship failed", "err", err)
			continue
		}

		// Log the retarget so rollback can reverse it.
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
