package dreaming

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// transitiveFakeEntityReader returns a fixed entity list keyed by namespace
// and stubs out the methods the transitive phase does not call.
type transitiveFakeEntityReader struct {
	entities []model.Entity
}

func (f *transitiveFakeEntityReader) GetByID(context.Context, uuid.UUID) (*model.Entity, error) {
	return nil, errors.New("not used by transitive phase")
}
func (f *transitiveFakeEntityReader) ListByNamespace(_ context.Context, _ uuid.UUID) ([]model.Entity, error) {
	return f.entities, nil
}
func (f *transitiveFakeEntityReader) FindBySimilarity(context.Context, uuid.UUID, string, string, int) ([]model.Entity, error) {
	return nil, errors.New("not used by transitive phase")
}

// transitiveFakeRelationshipReader serves the relationships and active-count
// the transitive phase reads on every Execute.
type transitiveFakeRelationshipReader struct {
	rels   []model.Relationship
	active int
}

func (f *transitiveFakeRelationshipReader) ListByNamespace(_ context.Context, _ uuid.UUID) ([]model.Relationship, error) {
	return f.rels, nil
}
func (f *transitiveFakeRelationshipReader) ListByEntity(context.Context, uuid.UUID) ([]model.Relationship, error) {
	return nil, errors.New("not used by transitive phase")
}
func (f *transitiveFakeRelationshipReader) TraverseFromEntity(context.Context, uuid.UUID, int, int) (storage.TraversalResult, error) {
	return storage.TraversalResult{}, errors.New("not used by transitive phase")
}
func (f *transitiveFakeRelationshipReader) FindActiveByTriple(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (*model.Relationship, error) {
	return nil, errors.New("not used by transitive phase")
}
func (f *transitiveFakeRelationshipReader) CountActiveByNamespace(_ context.Context, _ uuid.UUID) (int, error) {
	return f.active, nil
}

// transitiveFakeRelationshipWriter records every Create the transitive
// phase issues so tests can assert which transitive edges were created.
// perRowCreates tracks any per-row Create call (post-migration this
// should stay zero); batchCalls counts how many times BatchCreate fired.
type transitiveFakeRelationshipWriter struct {
	mu            sync.Mutex
	created       []*model.Relationship
	perRowCreates int
	batchCalls    int
}

func (w *transitiveFakeRelationshipWriter) Create(_ context.Context, rel *model.Relationship) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	cp := *rel
	w.created = append(w.created, &cp)
	w.perRowCreates++
	return nil
}
func (w *transitiveFakeRelationshipWriter) Reinforce(context.Context, uuid.UUID, uuid.UUID, float64) error {
	return nil
}
func (w *transitiveFakeRelationshipWriter) Expire(context.Context, uuid.UUID, uuid.UUID) error {
	return nil
}
func (w *transitiveFakeRelationshipWriter) DeleteByID(context.Context, uuid.UUID, uuid.UUID) error {
	return nil
}
func (w *transitiveFakeRelationshipWriter) UpdateWeight(context.Context, uuid.UUID, uuid.UUID, float64) error {
	return nil
}
func (w *transitiveFakeRelationshipWriter) ExpireLowWeight(context.Context, uuid.UUID, float64) (int64, error) {
	return 0, nil
}
func (w *transitiveFakeRelationshipWriter) ExpireLowestNTransitive(context.Context, uuid.UUID, int) (int64, error) {
	return 0, nil
}
func (w *transitiveFakeRelationshipWriter) BatchCreate(_ context.Context, rels []*model.Relationship) (model.BatchCreateResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.batchCalls++
	for _, rel := range rels {
		cp := *rel
		w.created = append(w.created, &cp)
	}
	return model.BatchCreateResult{Affected: int64(len(rels))}, nil
}
func (w *transitiveFakeRelationshipWriter) BatchExpire(context.Context, uuid.UUID, []uuid.UUID) (int64, error) {
	return 0, nil
}
func (w *transitiveFakeRelationshipWriter) BatchReinforce(context.Context, uuid.UUID, []model.ReinforceItem) (int64, error) {
	return 0, nil
}
func (w *transitiveFakeRelationshipWriter) BatchUpdateWeight(context.Context, uuid.UUID, []model.WeightUpdateItem) (int64, error) {
	return 0, nil
}
func (w *transitiveFakeRelationshipWriter) BatchDeleteByID(context.Context, uuid.UUID, []uuid.UUID) (int64, error) {
	return 0, nil
}

// transitiveTestFixture wires a small A→B→C→D chain (3 entities, 2 edges)
// with the supplied weights. Every Execute on it can yield up to 1 new
// transitive edge (A→C) before considering D; the second edge B→C plus a
// C→D edge yields A→D after that. Callers add as many entities and edges
// as the test needs.
func transitiveTestFixture(weight float64) ([]model.Entity, []model.Relationship) {
	a := model.Entity{ID: uuid.New(), Name: "A"}
	b := model.Entity{ID: uuid.New(), Name: "B"}
	c := model.Entity{ID: uuid.New(), Name: "C"}
	d := model.Entity{ID: uuid.New(), Name: "D"}
	now := transitiveTestCycle().NamespaceID // not used as time, just a stable UUID seed
	_ = now
	rels := []model.Relationship{
		{ID: uuid.New(), SourceID: a.ID, TargetID: b.ID, Relation: "knows", Weight: weight},
		{ID: uuid.New(), SourceID: b.ID, TargetID: c.ID, Relation: "knows", Weight: weight},
		{ID: uuid.New(), SourceID: c.ID, TargetID: d.ID, Relation: "knows", Weight: weight},
	}
	return []model.Entity{a, b, c, d}, rels
}

func transitiveTestCycle() *model.DreamCycle {
	return &model.DreamCycle{
		ID:          uuid.New(),
		ProjectID:   uuid.New(),
		NamespaceID: uuid.New(),
	}
}

// transitiveTestSettings constructs a settings stub that resolves the three
// transitive knobs; everything else falls through to registered defaults.
func transitiveTestSettings(minWeight float64, maxPerCycle, hardCap int) *staticDreamSettings {
	return &staticDreamSettings{
		values: map[string]string{},
		floats: map[string]float64{
			service.SettingDreamTransitiveMinWeight: minWeight,
		},
		ints: map[string]int{
			service.SettingDreamTransitiveMaxPerCycle:      maxPerCycle,
			service.SettingDreamTransitiveNamespaceHardCap: hardCap,
		},
	}
}

// TestTransitive_PerCycleCapBinding confirms that when the per-cycle cap is
// the binding constraint (well below hard-cap headroom), the phase reports
// HasResidual=true with ResidualReasonTransitivePerCycleCap. This branch is
// preserved verbatim; operators raising max_per_cycle should still see this
// residual_reason.
func TestTransitive_PerCycleCapBinding(t *testing.T) {
	entities, rels := transitiveTestFixture(0.9)
	cycle := transitiveTestCycle()
	settings := transitiveTestSettings(0.1, 1, 1000) // maxPerCycle=1, hardCap=1000 (huge headroom)

	reader := &transitiveFakeRelationshipReader{rels: rels, active: 10}
	writer := &transitiveFakeRelationshipWriter{}
	phase := NewTransitivePhase(&transitiveFakeEntityReader{entities: entities}, reader, writer, settings)

	result, err := phase.Execute(context.Background(), cycle, nil, NewDreamLogWriter(nil, cycle.ID, cycle.ProjectID))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if !result.HasResidual {
		t.Fatalf("expected HasResidual=true, got false (created=%d)", len(writer.created))
	}
	if result.ResidualReason != ResidualReasonTransitivePerCycleCap {
		t.Errorf("residual_reason = %q, want %q", result.ResidualReason, ResidualReasonTransitivePerCycleCap)
	}
	if len(writer.created) != 1 {
		t.Errorf("created = %d, want exactly 1 (per-cycle cap)", len(writer.created))
	}
}

// TestTransitive_HeadroomBindingDoesNotResidual proves the loop fix: when
// hard-cap headroom is the binding constraint (not max_per_cycle), the phase
// MUST return HasResidual=false so the project dirty flag can clear. The
// informational residual_reason still appears in the phase summary so the
// UI/operator can see why the phase truncated.
func TestTransitive_HeadroomBindingDoesNotResidual(t *testing.T) {
	entities, rels := transitiveTestFixture(0.9)
	cycle := transitiveTestCycle()
	// hardCap=11, active=10 → headroom=1. maxPerCycle=1000 is irrelevant.
	settings := transitiveTestSettings(0.1, 1000, 11)

	reader := &transitiveFakeRelationshipReader{rels: rels, active: 10}
	writer := &transitiveFakeRelationshipWriter{}
	phase := NewTransitivePhase(&transitiveFakeEntityReader{entities: entities}, reader, writer, settings)

	result, err := phase.Execute(context.Background(), cycle, nil, NewDreamLogWriter(nil, cycle.ID, cycle.ProjectID))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.HasResidual {
		t.Fatalf("expected HasResidual=false (headroom-bound must not loop), got true; residual_reason=%q",
			result.ResidualReason)
	}
	if result.ResidualReason != ResidualReasonTransitiveHardCapApproach {
		t.Errorf("residual_reason = %q, want %q (informational)", result.ResidualReason,
			ResidualReasonTransitiveHardCapApproach)
	}
	if len(writer.created) != 1 {
		t.Errorf("created = %d, want exactly 1 (headroom=1)", len(writer.created))
	}
	// ResidualDetail is informational only; assert it carries the active/
	// hard_cap values that the operator needs to diagnose the saturation.
	if result.ResidualDetail == nil {
		t.Fatalf("ResidualDetail = nil, want populated detail map")
	}
	if v, _ := result.ResidualDetail["hard_cap"].(int); v != 11 {
		t.Errorf("ResidualDetail.hard_cap = %v, want 11", result.ResidualDetail["hard_cap"])
	}
	if v, _ := result.ResidualDetail["active"].(int); v != 10 {
		t.Errorf("ResidualDetail.active = %v, want 10", result.ResidualDetail["active"])
	}
}

// TestTransitive_HardCapAlreadyHit is the existing branch at line 76-80:
// when totalActive >= hardCap the phase exits cleanly with no residual.
// Pinning it as a regression guard alongside the new branch.
func TestTransitive_HardCapAlreadyHit(t *testing.T) {
	entities, rels := transitiveTestFixture(0.9)
	cycle := transitiveTestCycle()
	settings := transitiveTestSettings(0.1, 1000, 10)

	reader := &transitiveFakeRelationshipReader{rels: rels, active: 10} // active == hardCap
	writer := &transitiveFakeRelationshipWriter{}
	phase := NewTransitivePhase(&transitiveFakeEntityReader{entities: entities}, reader, writer, settings)

	result, err := phase.Execute(context.Background(), cycle, nil, NewDreamLogWriter(nil, cycle.ID, cycle.ProjectID))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.HasResidual {
		t.Errorf("expected HasResidual=false at hard cap, got true")
	}
	if len(writer.created) != 0 {
		t.Errorf("created = %d, want 0 (phase must no-op at hard cap)", len(writer.created))
	}
}

// TestTransitive_UsesBatchCreateNotPerRow pins the migration: the phase
// must funnel every inferred edge through BatchCreate, with the per-row
// Create method untouched. Regression guard against future code that
// reintroduces per-row writes inside the triple loop.
func TestTransitive_UsesBatchCreateNotPerRow(t *testing.T) {
	entities, rels := transitiveTestFixture(0.9)
	cycle := transitiveTestCycle()
	settings := transitiveTestSettings(0.1, 1000, 1000)

	reader := &transitiveFakeRelationshipReader{rels: rels, active: 10}
	writer := &transitiveFakeRelationshipWriter{}
	phase := NewTransitivePhase(&transitiveFakeEntityReader{entities: entities}, reader, writer, settings)

	if _, err := phase.Execute(context.Background(), cycle, nil, NewDreamLogWriter(nil, cycle.ID, cycle.ProjectID)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if writer.perRowCreates != 0 {
		t.Errorf("per-row Create called %d times; want 0 (BatchCreate is the only path)", writer.perRowCreates)
	}
	if writer.batchCalls != 1 {
		t.Errorf("BatchCreate called %d times; want exactly 1 per phase invocation", writer.batchCalls)
	}
	if len(writer.created) == 0 {
		t.Error("expected at least one transitive edge created via batch")
	}
}

// TestTransitive_NoCandidatesNoBatchCall confirms BatchCreate is not
// called when the triple loop produces zero candidates (e.g. all edges
// already exist or fall below minWeight).
func TestTransitive_NoCandidatesNoBatchCall(t *testing.T) {
	entities, rels := transitiveTestFixture(0.01) // weights too low to produce A→C
	cycle := transitiveTestCycle()
	settings := transitiveTestSettings(0.1, 1000, 1000) // minWeight=0.1 filters all

	reader := &transitiveFakeRelationshipReader{rels: rels, active: 10}
	writer := &transitiveFakeRelationshipWriter{}
	phase := NewTransitivePhase(&transitiveFakeEntityReader{entities: entities}, reader, writer, settings)

	if _, err := phase.Execute(context.Background(), cycle, nil, NewDreamLogWriter(nil, cycle.ID, cycle.ProjectID)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if writer.batchCalls != 0 {
		t.Errorf("BatchCreate called %d times; want 0 when no candidates exist", writer.batchCalls)
	}
	if writer.perRowCreates != 0 {
		t.Errorf("per-row Create called %d times; want 0", writer.perRowCreates)
	}
}
