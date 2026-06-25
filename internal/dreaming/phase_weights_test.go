package dreaming

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// --- Test doubles for the WeightAdjustmentPhase deps ---

type fakeRelationshipReader struct {
	rels []model.Relationship
}

func (f *fakeRelationshipReader) ListByNamespace(_ context.Context, _ uuid.UUID) ([]model.Relationship, error) {
	out := make([]model.Relationship, len(f.rels))
	copy(out, f.rels)
	return out, nil
}
func (f *fakeRelationshipReader) ListByEntity(_ context.Context, _ uuid.UUID, _ []uuid.UUID) ([]model.Relationship, error) {
	return nil, nil
}
func (f *fakeRelationshipReader) TraverseFromEntity(_ context.Context, _ uuid.UUID, _ []uuid.UUID, _, _ int) (storage.TraversalResult, error) {
	return storage.TraversalResult{}, nil
}
func (f *fakeRelationshipReader) FindActiveByTriple(_ context.Context, _, _, _ uuid.UUID, _ string) (*model.Relationship, error) {
	return nil, nil
}
func (f *fakeRelationshipReader) CountActiveByNamespace(_ context.Context, _ uuid.UUID) (int, error) {
	return len(f.rels), nil
}

// recordingRelationshipWriter captures UpdateWeight and Expire calls so tests
// can assert direction (up/down/expired) per relationship. batchExpireCalls
// and batchUpdateWeightCalls count batch invocations so post-migration
// tests can confirm the per-row paths are no longer used.
type recordingRelationshipWriter struct {
	mu                sync.Mutex
	weights           map[uuid.UUID]float64
	expired           map[uuid.UUID]struct{}
	creates           []model.Relationship
	batchExpireCalls  int
	batchUpdateCalls  int
	perRowExpireCalls int
	perRowUpdateCalls int
}

func newRecordingRelationshipWriter() *recordingRelationshipWriter {
	return &recordingRelationshipWriter{
		weights: map[uuid.UUID]float64{},
		expired: map[uuid.UUID]struct{}{},
	}
}

func (r *recordingRelationshipWriter) Create(_ context.Context, rel *model.Relationship) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.creates = append(r.creates, *rel)
	return nil
}
func (r *recordingRelationshipWriter) Reinforce(_ context.Context, _, _ uuid.UUID, _ float64) error {
	return nil
}
func (r *recordingRelationshipWriter) Expire(_ context.Context, id, _ uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expired[id] = struct{}{}
	r.perRowExpireCalls++
	return nil
}
func (r *recordingRelationshipWriter) DeleteByID(_ context.Context, _, _ uuid.UUID) error {
	return nil
}
func (r *recordingRelationshipWriter) UpdateWeight(_ context.Context, id, _ uuid.UUID, w float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.weights[id] = w
	r.perRowUpdateCalls++
	return nil
}
func (r *recordingRelationshipWriter) ExpireLowWeight(_ context.Context, _ uuid.UUID, _ float64) (int64, error) {
	return 0, nil
}

func (r *recordingRelationshipWriter) ExpireLowestNTransitive(_ context.Context, _ uuid.UUID, _ int) (int64, error) {
	return 0, nil
}

func (r *recordingRelationshipWriter) BatchCreate(_ context.Context, rels []*model.Relationship) (model.BatchCreateResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rel := range rels {
		r.creates = append(r.creates, *rel)
	}
	return model.BatchCreateResult{Affected: int64(len(rels))}, nil
}

func (r *recordingRelationshipWriter) BatchExpire(_ context.Context, _ uuid.UUID, ids []uuid.UUID) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batchExpireCalls++
	for _, id := range ids {
		r.expired[id] = struct{}{}
	}
	return int64(len(ids)), nil
}

func (r *recordingRelationshipWriter) BatchReinforce(_ context.Context, _ uuid.UUID, _ []model.ReinforceItem) (int64, error) {
	return 0, nil
}

func (r *recordingRelationshipWriter) BatchUpdateWeight(_ context.Context, _ uuid.UUID, items []model.WeightUpdateItem) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batchUpdateCalls++
	for _, it := range items {
		r.weights[it.ID] = it.Weight
	}
	return int64(len(items)), nil
}

func (r *recordingRelationshipWriter) BatchDeleteByID(_ context.Context, _ uuid.UUID, _ []uuid.UUID) (int64, error) {
	return 0, nil
}

func (r *recordingRelationshipWriter) weightOf(id uuid.UUID) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.weights[id]
	return w, ok
}

func (r *recordingRelationshipWriter) wasExpired(id uuid.UUID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.expired[id]
	return ok
}

type fakeEntityReader struct {
	entities []model.Entity
}

func (f *fakeEntityReader) GetByID(_ context.Context, id uuid.UUID, _ uuid.UUID) (*model.Entity, error) {
	for i := range f.entities {
		if f.entities[i].ID == id {
			return &f.entities[i], nil
		}
	}
	return nil, errors.New("not found")
}
func (f *fakeEntityReader) ListByNamespace(_ context.Context, _ uuid.UUID) ([]model.Entity, error) {
	return f.entities, nil
}
func (f *fakeEntityReader) FindBySimilarity(_ context.Context, _ uuid.UUID, _, _ string, _ int) ([]model.Entity, error) {
	return nil, nil
}

type noopEntityWriter struct{}

func (noopEntityWriter) Upsert(_ context.Context, _ *model.Entity) error { return nil }
func (noopEntityWriter) DeleteByIDs(_ context.Context, _ []uuid.UUID) ([]uuid.UUID, error) {
	return nil, nil
}

// --- Helpers ---

func weightTestPhase(
	rels []model.Relationship,
	memories []model.Memory,
	supportGain float64,
) (*WeightAdjustmentPhase, *recordingRelationshipWriter) {
	relWriter := newRecordingRelationshipWriter()
	settings := &staticDreamSettings{
		floats: map[string]float64{
			service.SettingDreamingWeightSupportGain: supportGain,
		},
	}
	phase := NewWeightAdjustmentPhase(
		&fakeEntityReader{},
		noopEntityWriter{},
		&fakeRelationshipReader{rels: rels},
		relWriter,
		&fakeMemoryReader{list: memories},
		settings,
	)
	return phase, relWriter
}

func weightTestCycle() *model.DreamCycle {
	return &model.DreamCycle{
		ID:          uuid.New(),
		ProjectID:   uuid.New(),
		NamespaceID: uuid.New(),
	}
}

func aliveMemory(id uuid.UUID, confidence float64) model.Memory {
	return model.Memory{
		ID:         id,
		Confidence: confidence,
		CreatedAt:  time.Now().Add(-24 * time.Hour),
		UpdatedAt:  time.Now().Add(-24 * time.Hour),
	}
}

func deletedMemory(id uuid.UUID) model.Memory {
	now := time.Now()
	mem := aliveMemory(id, 1.0)
	mem.DeletedAt = &now
	return mem
}

// --- Tests ---

// TestCalculateWeight_TwoConfidentDirectMemories_LiftsWeight pins the core
// fix: a relationship attested by ≥ 2 high-confidence direct-lineage memories
// must rise rather than decay every cycle. Without the fix this row's
// calculateWeight monotonically multiplied by mem.Confidence and never lifted.
func TestCalculateWeight_TwoConfidentDirectMemories_LiftsWeight(t *testing.T) {
	srcEntity := uuid.New()
	tgtEntity := uuid.New()
	memA := uuid.New()
	memB := uuid.New()

	// Two relationship rows for the same (src, tgt, "knows") triple, each
	// produced by a distinct memory at full confidence. Recent ValidFrom so
	// the 30-day decay branch does NOT fire.
	rels := []model.Relationship{
		mkRel(srcEntity, tgtEntity, "knows", 0.5, &memA),
		mkRel(srcEntity, tgtEntity, "knows", 0.5, &memB),
	}
	mems := []model.Memory{
		aliveMemory(memA, 1.0),
		aliveMemory(memB, 1.0),
	}

	idx, _ := buildSupportIndex(rels)
	memMap := map[uuid.UUID]*model.Memory{memA: &mems[0], memB: &mems[1]}

	phase := &WeightAdjustmentPhase{}
	now := time.Now().UTC()

	// support = 1.0 (memA) + 1.0 (memB) = 2.0
	// gain    = 1 + 0.05*(2.0 - 1) = 1.05
	// new     = 0.5 * 1.05 = 0.525
	w, t1, t2 := phase.calculateWeight(&rels[0], now, memMap, idx, phase.resolveWeightTuning(context.Background()))

	if w <= 0.5 {
		t.Errorf("expected weight to rise from 0.5 with 2 confident direct memories; got %f", w)
	}
	if w > 2.0 {
		t.Errorf("weight must be capped at 2.0; got %f", w)
	}
	if t1 != 2 || t2 != 0 {
		t.Errorf("tier counts: want tier1=2 tier2=0, got tier1=%d tier2=%d", t1, t2)
	}
}

// TestCalculateWeight_SingleConfidentSource_NoLift confirms the design
// threshold: a single Confidence=1.0 source gives support = 1.0, which is
// NOT > 1.0, so no gain is applied. Decay alone runs (and on a fresh row,
// decay is also a no-op). Net effect: weight unchanged. This protects
// against a runaway lift on every recall-sourced row.
func TestCalculateWeight_SingleConfidentSource_NoLift(t *testing.T) {
	src, tgt := uuid.New(), uuid.New()
	mem := uuid.New()
	rels := []model.Relationship{mkRel(src, tgt, "knows", 0.7, &mem)}
	mems := []model.Memory{aliveMemory(mem, 1.0)}

	idx, _ := buildSupportIndex(rels)
	memMap := map[uuid.UUID]*model.Memory{mem: &mems[0]}

	phase := &WeightAdjustmentPhase{}
	w, t1, _ := phase.calculateWeight(&rels[0], time.Now().UTC(), memMap, idx, phase.resolveWeightTuning(context.Background()))

	if w != 0.7 {
		t.Errorf("single confident source should leave weight unchanged; want 0.7, got %f", w)
	}
	if t1 != 1 {
		t.Errorf("tier1: want 1, got %d", t1)
	}
}

// TestCalculateWeight_CoMentionTier2_HalfWeightContribution exercises the
// belt-and-suspenders branch: a memory that produced relationship rows
// touching both endpoints separately (but never the direct edge) should
// contribute half-weight support.
func TestCalculateWeight_CoMentionTier2_HalfWeightContribution(t *testing.T) {
	src, tgt := uuid.New(), uuid.New()
	other := uuid.New()
	memDirect := uuid.New() // Tier 1: produced src↔tgt direct edge
	memCo := uuid.New()     // Tier 2: produced src↔other and tgt↔other rows

	rels := []model.Relationship{
		mkRel(src, tgt, "knows", 0.5, &memDirect),
		mkRel(src, other, "rel_a", 1.0, &memCo),
		mkRel(tgt, other, "rel_b", 1.0, &memCo),
	}
	mems := []model.Memory{aliveMemory(memDirect, 1.0), aliveMemory(memCo, 1.0)}

	idx, _ := buildSupportIndex(rels)
	memMap := map[uuid.UUID]*model.Memory{
		memDirect: &mems[0],
		memCo:     &mems[1],
	}

	phase := &WeightAdjustmentPhase{}
	// support = 1.0 (Tier 1: memDirect) + 0.5*1.0 (Tier 2: memCo) = 1.5
	// gain    = 1 + 0.05*(1.5-1) = 1.025
	w, t1, t2 := phase.calculateWeight(&rels[0], time.Now().UTC(), memMap, idx, phase.resolveWeightTuning(context.Background()))

	if w <= 0.5 {
		t.Errorf("expected co-mention tier to push support over 1.0 and lift weight; got %f", w)
	}
	if t1 != 1 {
		t.Errorf("tier1: want 1, got %d", t1)
	}
	if t2 != 1 {
		t.Errorf("tier2: want 1, got %d", t2)
	}
}

// TestCalculateWeight_DeletedSourceGuard pins the empty-support guard: a
// row whose only source is soft-deleted, with no other supporting memory
// in the namespace, must take the ×0.5 hit so it falls toward the pruning
// floor faster.
func TestCalculateWeight_DeletedSourceGuard(t *testing.T) {
	src, tgt := uuid.New(), uuid.New()
	mem := uuid.New()
	rels := []model.Relationship{mkRel(src, tgt, "knows", 0.6, &mem)}
	mems := []model.Memory{deletedMemory(mem)}

	idx, _ := buildSupportIndex(rels)
	memMap := map[uuid.UUID]*model.Memory{mem: &mems[0]}

	phase := &WeightAdjustmentPhase{}
	w, t1, t2 := phase.calculateWeight(&rels[0], time.Now().UTC(), memMap, idx, phase.resolveWeightTuning(context.Background()))

	// support is 0 (deleted memory filtered out), guard fires: 0.6 * 0.5 = 0.3
	if w >= 0.6 {
		t.Errorf("expected deleted-source guard to reduce weight; got %f from 0.6", w)
	}
	if t1 != 0 || t2 != 0 {
		t.Errorf("deleted source must contribute zero tiers; got tier1=%d tier2=%d", t1, t2)
	}
}

// TestCalculateWeight_OldRelationshipDecays preserves the existing 30-day
// decay loop unchanged.
func TestCalculateWeight_OldRelationshipDecays(t *testing.T) {
	src, tgt := uuid.New(), uuid.New()
	mem := uuid.New()
	rel := mkRel(src, tgt, "knows", 1.0, &mem)
	rel.ValidFrom = time.Now().Add(-365 * 24 * time.Hour) // ~1 year old
	rels := []model.Relationship{rel}
	mems := []model.Memory{aliveMemory(mem, 1.0)}

	idx, _ := buildSupportIndex(rels)
	memMap := map[uuid.UUID]*model.Memory{mem: &mems[0]}

	phase := &WeightAdjustmentPhase{}
	w, _, _ := phase.calculateWeight(&rels[0], time.Now().UTC(), memMap, idx, phase.resolveWeightTuning(context.Background()))

	// 12 30-day periods, capped at 10: 0.95^10 ≈ 0.5987
	if w >= 1.0 || w <= 0.4 {
		t.Errorf("expected decay to drop weight from 1.0 toward 0.6; got %f", w)
	}
}

// TestExecute_DirectionTriad_AppearsInOpCount confirms the runner-visible
// direction counters fire end-to-end. Two rels: one rising (multi-memory
// support), one falling (deleted source guard).
func TestExecute_DirectionTriad_AppearsInOpCount(t *testing.T) {
	src1, tgt1 := uuid.New(), uuid.New() // rising edge
	src2, tgt2 := uuid.New(), uuid.New() // falling edge
	memA, memB := uuid.New(), uuid.New() // direct support for rising edge
	memDead := uuid.New()                // dead source for falling edge

	rels := []model.Relationship{
		mkRel(src1, tgt1, "knows", 0.5, &memA),
		mkRel(src1, tgt1, "knows", 0.5, &memB),
		mkRel(src2, tgt2, "knows", 0.6, &memDead),
	}
	mems := []model.Memory{
		aliveMemory(memA, 1.0),
		aliveMemory(memB, 1.0),
		deletedMemory(memDead),
	}

	phase, relWriter := weightTestPhase(rels, mems, 0.05)

	// Use a logger backed by a nil repo (counted no-op) so OpCount reflects
	// the writes we performed without needing a real DB.
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())
	budget := NewTokenBudget(1<<20, 1024)
	cycle := weightTestCycle()

	result, err := phase.Execute(context.Background(), cycle, budget, logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.HasResidual {
		t.Error("weight_adjustment must never report residual")
	}

	// Rising edge (rels[0] and rels[1]): each got an UpdateWeight call.
	w1Up, ok1 := relWriter.weightOf(rels[0].ID)
	if !ok1 {
		t.Error("rising rel[0] should have produced an UpdateWeight call")
	} else if w1Up <= 0.5 {
		t.Errorf("rising rel[0] weight: want > 0.5, got %f", w1Up)
	}
	// Falling edge (rels[2]): deleted-source guard halves 0.6 → 0.3.
	wDown, okDown := relWriter.weightOf(rels[2].ID)
	if !okDown {
		t.Error("falling rel[2] should have produced an UpdateWeight call")
	} else if wDown >= 0.6 {
		t.Errorf("falling rel[2] weight: want < 0.6, got %f", wDown)
	}
}

// TestExecute_ExpiredRelationshipsSkipped pins the existing skip on
// rel.ValidUntil != nil. Expired rows must not be UpdateWeight'd and must
// not contribute to support indices.
func TestExecute_ExpiredRelationshipsSkipped(t *testing.T) {
	src, tgt := uuid.New(), uuid.New()
	mem := uuid.New()
	rel := mkRel(src, tgt, "knows", 0.5, &mem)
	expired := time.Now().Add(-time.Hour)
	rel.ValidUntil = &expired
	rels := []model.Relationship{rel}
	mems := []model.Memory{aliveMemory(mem, 1.0)}

	phase, relWriter := weightTestPhase(rels, mems, 0.05)
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())
	budget := NewTokenBudget(1<<20, 1024)

	if _, err := phase.Execute(context.Background(), weightTestCycle(), budget, logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if _, ok := relWriter.weightOf(rel.ID); ok {
		t.Error("expired relationship must not be touched by UpdateWeight")
	}
	if relWriter.wasExpired(rel.ID) {
		t.Error("expired relationship must not be re-Expire'd")
	}
}

// TestExecute_DecaysToFloor_ExpiresRelationship pins the expiry pathway:
// when the new weight falls below 0.05, the row is Expire'd rather than
// UpdateWeight'd at near-zero.
func TestExecute_DecaysToFloor_ExpiresRelationship(t *testing.T) {
	src, tgt := uuid.New(), uuid.New()
	mem := uuid.New()
	rel := mkRel(src, tgt, "knows", 0.06, &mem)
	rel.ValidFrom = time.Now().Add(-365 * 24 * time.Hour) // forces 10× 0.95 decay
	rels := []model.Relationship{rel}
	mems := []model.Memory{aliveMemory(mem, 1.0)}

	phase, relWriter := weightTestPhase(rels, mems, 0.05)
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())
	budget := NewTokenBudget(1<<20, 1024)

	if _, err := phase.Execute(context.Background(), weightTestCycle(), budget, logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// 0.06 * 0.95^10 ≈ 0.0359 < 0.05 → expire
	if !relWriter.wasExpired(rel.ID) {
		t.Error("relationship that decayed below 0.05 must be Expire'd")
	}
	if _, ok := relWriter.weightOf(rel.ID); ok {
		t.Error("expiring path must not also write the near-zero weight")
	}
}

// TestExecute_UsesBatchExpireAndBatchUpdate pins the migration: when the
// phase touches multiple relationships, per-row Expire and UpdateWeight
// are not called and the batch methods fire at most once each. Regression
// guard against re-introducing per-row writes inside the loop.
func TestExecute_UsesBatchExpireAndBatchUpdate(t *testing.T) {
	src, tgtA := uuid.New(), uuid.New()
	srcB, tgtB := uuid.New(), uuid.New()
	memA, memB := uuid.New(), uuid.New()

	// Two rels that will get UpdateWeight (rising) and one that will
	// Expire (decayed below floor).
	rising1 := mkRel(src, tgtA, "knows", 0.5, &memA)
	rising2 := mkRel(src, tgtA, "knows", 0.5, &memB)
	decayed := mkRel(srcB, tgtB, "knows", 0.06, &memA)
	decayed.ValidFrom = time.Now().Add(-365 * 24 * time.Hour)

	rels := []model.Relationship{rising1, rising2, decayed}
	mems := []model.Memory{aliveMemory(memA, 1.0), aliveMemory(memB, 1.0)}

	phase, relWriter := weightTestPhase(rels, mems, 0.05)
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())
	budget := NewTokenBudget(1<<20, 1024)

	if _, err := phase.Execute(context.Background(), weightTestCycle(), budget, logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if relWriter.perRowExpireCalls != 0 {
		t.Errorf("per-row Expire called %d times; want 0", relWriter.perRowExpireCalls)
	}
	if relWriter.perRowUpdateCalls != 0 {
		t.Errorf("per-row UpdateWeight called %d times; want 0", relWriter.perRowUpdateCalls)
	}
	if relWriter.batchExpireCalls > 1 {
		t.Errorf("BatchExpire called %d times; want at most 1 per phase invocation", relWriter.batchExpireCalls)
	}
	if relWriter.batchUpdateCalls > 1 {
		t.Errorf("BatchUpdateWeight called %d times; want at most 1 per phase invocation", relWriter.batchUpdateCalls)
	}
}

// --- shared helpers ---

func mkRel(src, tgt uuid.UUID, relation string, weight float64, sourceMem *uuid.UUID) model.Relationship {
	return model.Relationship{
		ID:           uuid.New(),
		NamespaceID:  uuid.New(),
		SourceID:     src,
		TargetID:     tgt,
		Relation:     relation,
		Weight:       weight,
		ValidFrom:    time.Now().Add(-time.Hour),
		SourceMemory: sourceMem,
	}
}
