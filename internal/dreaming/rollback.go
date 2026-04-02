package dreaming

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

// RollbackService reverses all operations performed during a dream cycle
// using the logged before/after state snapshots.
type RollbackService struct {
	logRepo      *storage.DreamLogRepo
	cycleRepo    *storage.DreamCycleRepo
	dirtyRepo    *storage.DreamDirtyRepo
	memWriter    MemoryWriter
	memories     MemoryReader
	relWriter    RelationshipWriter
	entityWriter EntityWriter
	entityReader EntityReader
}

// NewRollbackService creates a new RollbackService.
func NewRollbackService(
	logRepo *storage.DreamLogRepo,
	cycleRepo *storage.DreamCycleRepo,
	dirtyRepo *storage.DreamDirtyRepo,
	memWriter MemoryWriter,
	memories MemoryReader,
	relWriter RelationshipWriter,
	entityWriter EntityWriter,
	entityReader EntityReader,
) *RollbackService {
	return &RollbackService{
		logRepo:      logRepo,
		cycleRepo:    cycleRepo,
		dirtyRepo:    dirtyRepo,
		memWriter:    memWriter,
		memories:     memories,
		relWriter:    relWriter,
		entityWriter: entityWriter,
		entityReader: entityReader,
	}
}

// Rollback reverses all operations in a dream cycle in reverse chronological
// order. The cycle is marked as rolled_back and the project is re-marked dirty.
func (s *RollbackService) Rollback(ctx context.Context, cycleID uuid.UUID) error {
	cycle, err := s.cycleRepo.GetByID(ctx, cycleID)
	if err != nil {
		return fmt.Errorf("rollback: get cycle: %w", err)
	}

	if cycle.Status == model.DreamStatusRolledBack {
		return fmt.Errorf("rollback: cycle %s already rolled back", cycleID)
	}

	entries, err := s.logRepo.ListByCycleReversed(ctx, cycleID)
	if err != nil {
		return fmt.Errorf("rollback: list log entries: %w", err)
	}

	if len(entries) == 0 {
		return s.cycleRepo.MarkRolledBack(ctx, cycleID)
	}

	var rollbackErrors []error
	for _, entry := range entries {
		if err := s.reverseOperation(ctx, &entry); err != nil {
			rollbackErrors = append(rollbackErrors, fmt.Errorf(
				"op=%s target=%s: %w", entry.Operation, entry.TargetID, err))
		}
	}

	if err := s.cycleRepo.MarkRolledBack(ctx, cycleID); err != nil {
		return fmt.Errorf("rollback: mark rolled back: %w", err)
	}

	if err := s.dirtyRepo.MarkDirty(ctx, cycle.ProjectID); err != nil {
		return fmt.Errorf("rollback: re-mark dirty: %w", err)
	}

	if len(rollbackErrors) > 0 {
		return fmt.Errorf("rollback: %d of %d operations failed (first: %w)",
			len(rollbackErrors), len(entries), rollbackErrors[0])
	}

	return nil
}

func (s *RollbackService) reverseOperation(ctx context.Context, entry *model.DreamLog) error {
	switch entry.Operation {
	case model.DreamOpMemoryCreated:
		return s.memWriter.HardDelete(ctx, entry.TargetID)

	case model.DreamOpMemoryDeleted:
		return s.restoreMemory(ctx, entry.BeforeState)

	case model.DreamOpMemorySuperseded, model.DreamOpConfidenceAdjusted:
		return s.restoreMemoryFields(ctx, entry.TargetID, entry.BeforeState)

	case model.DreamOpEntityMerged:
		return s.reverseEntityMerge(ctx, entry)

	case model.DreamOpRelationshipCreated:
		return s.relWriter.DeleteByID(ctx, entry.TargetID)

	case model.DreamOpRelationshipUpdated:
		return s.reverseRelationshipUpdate(ctx, entry)

	case model.DreamOpEntityUpdated:
		return s.reverseEntityUpdate(ctx, entry)

	case model.DreamOpContradictionDetected:
		// Contradiction detection creates lineage entries but doesn't
		// mutate memories or entities. The lineage entry is informational.
		return nil

	default:
		return fmt.Errorf("unknown rollback operation: %s", entry.Operation)
	}
}

// reverseEntityMerge restores a consumed entity from its before_state snapshot.
// before_state = the consumed entity, after_state = the primary entity.
func (s *RollbackService) reverseEntityMerge(ctx context.Context, entry *model.DreamLog) error {
	var consumed model.Entity
	if err := json.Unmarshal(entry.BeforeState, &consumed); err != nil {
		return fmt.Errorf("unmarshal consumed entity: %w", err)
	}

	consumed.UpdatedAt = time.Now().UTC()
	if err := s.entityWriter.Upsert(ctx, &consumed); err != nil {
		return fmt.Errorf("restore consumed entity: %w", err)
	}

	// Restore the primary entity's mention count by subtracting the consumed count.
	var primary model.Entity
	if err := json.Unmarshal(entry.AfterState, &primary); err != nil {
		return fmt.Errorf("unmarshal primary entity: %w", err)
	}

	// Read current state of primary to avoid overwriting other changes.
	currentPrimary, err := s.entityReader.GetByID(ctx, primary.ID)
	if err != nil {
		return fmt.Errorf("read primary entity: %w", err)
	}

	currentPrimary.MentionCount -= consumed.MentionCount
	if currentPrimary.MentionCount < 1 {
		currentPrimary.MentionCount = 1
	}
	currentPrimary.UpdatedAt = time.Now().UTC()
	return s.entityWriter.Upsert(ctx, currentPrimary)
}

// reverseRelationshipUpdate restores a relationship's weight from before_state.
// before_state = {"weight": <old_weight>}
func (s *RollbackService) reverseRelationshipUpdate(ctx context.Context, entry *model.DreamLog) error {
	var fields map[string]interface{}
	if err := json.Unmarshal(entry.BeforeState, &fields); err != nil {
		return fmt.Errorf("unmarshal relationship before state: %w", err)
	}

	weightRaw, ok := fields["weight"]
	if !ok {
		return fmt.Errorf("relationship before_state missing weight field")
	}

	weight, ok := weightRaw.(float64)
	if !ok {
		return fmt.Errorf("relationship weight is not a number")
	}

	return s.relWriter.UpdateWeight(ctx, entry.TargetID, weight)
}

// reverseEntityUpdate restores an entity's mention count from before_state.
// before_state = {"mention_count": <old_count>}
func (s *RollbackService) reverseEntityUpdate(ctx context.Context, entry *model.DreamLog) error {
	var fields map[string]interface{}
	if err := json.Unmarshal(entry.BeforeState, &fields); err != nil {
		return fmt.Errorf("unmarshal entity before state: %w", err)
	}

	countRaw, ok := fields["mention_count"]
	if !ok {
		return fmt.Errorf("entity before_state missing mention_count field")
	}

	countFloat, ok := countRaw.(float64)
	if !ok {
		return fmt.Errorf("entity mention_count is not a number")
	}

	// Read the current entity, set the old mention count, and upsert.
	entity, err := s.entityReader.GetByID(ctx, entry.TargetID)
	if err != nil {
		return fmt.Errorf("read entity for rollback: %w", err)
	}

	entity.MentionCount = int(countFloat)
	entity.UpdatedAt = time.Now().UTC()
	return s.entityWriter.Upsert(ctx, entity)
}

func (s *RollbackService) restoreMemory(ctx context.Context, beforeState json.RawMessage) error {
	var mem model.Memory
	if err := json.Unmarshal(beforeState, &mem); err != nil {
		return fmt.Errorf("unmarshal memory for restore: %w", err)
	}

	mem.DeletedAt = nil
	mem.UpdatedAt = time.Now().UTC()
	return s.memWriter.Create(ctx, &mem)
}

func (s *RollbackService) restoreMemoryFields(ctx context.Context, memID uuid.UUID, beforeState json.RawMessage) error {
	mem, err := s.memories.GetByID(ctx, memID)
	if err != nil {
		return fmt.Errorf("get memory for restore: %w", err)
	}

	var fields map[string]interface{}
	if err := json.Unmarshal(beforeState, &fields); err != nil {
		return fmt.Errorf("unmarshal before state: %w", err)
	}

	if conf, ok := fields["confidence"]; ok {
		if f, ok := conf.(float64); ok {
			mem.Confidence = f
		}
	}

	if sup, ok := fields["superseded_by"]; ok {
		if sup == nil {
			mem.SupersededBy = nil
		}
	}

	mem.UpdatedAt = time.Now().UTC()
	return s.memWriter.Update(ctx, mem)
}
