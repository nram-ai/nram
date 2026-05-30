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

// pressureFakeRelationshipReader exposes a fixed CountActiveByNamespace so
// the pressure-prune branch can observe namespace saturation. The other
// reader methods are stubbed because the pruning phase only calls
// CountActiveByNamespace from the pressure-driven path.
type pressureFakeRelationshipReader struct {
	active int
}

func (f *pressureFakeRelationshipReader) ListByNamespace(context.Context, uuid.UUID) ([]model.Relationship, error) {
	return nil, errors.New("not used by pressure prune")
}
func (f *pressureFakeRelationshipReader) ListByEntity(context.Context, uuid.UUID) ([]model.Relationship, error) {
	return nil, errors.New("not used by pressure prune")
}
func (f *pressureFakeRelationshipReader) TraverseFromEntity(context.Context, uuid.UUID, int, int) (storage.TraversalResult, error) {
	return storage.TraversalResult{}, errors.New("not used by pressure prune")
}
func (f *pressureFakeRelationshipReader) FindActiveByTriple(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (*model.Relationship, error) {
	return nil, errors.New("not used by pressure prune")
}
func (f *pressureFakeRelationshipReader) CountActiveByNamespace(_ context.Context, _ uuid.UUID) (int, error) {
	return f.active, nil
}

// recordingRelWriter captures every ExpireLowestNTransitive call so tests
// can assert both that the call fires and that the N matches the expected
// drain target (totalActive - hard_cap * low_water).
type recordingRelWriter struct {
	mu                sync.Mutex
	expireLowWeightN  []int
	expireTransitiveN []int
	// transitiveResult lets a test inject the int64 returned from
	// ExpireLowestNTransitive — defaults to the int(n) requested.
	transitiveResult *int64
}

func (w *recordingRelWriter) Create(context.Context, *model.Relationship) error { return nil }
func (w *recordingRelWriter) Reinforce(context.Context, uuid.UUID, uuid.UUID, float64) error {
	return nil
}
func (w *recordingRelWriter) Expire(context.Context, uuid.UUID, uuid.UUID) error     { return nil }
func (w *recordingRelWriter) DeleteByID(context.Context, uuid.UUID, uuid.UUID) error { return nil }
func (w *recordingRelWriter) UpdateWeight(context.Context, uuid.UUID, uuid.UUID, float64) error {
	return nil
}
func (w *recordingRelWriter) ExpireLowWeight(_ context.Context, _ uuid.UUID, threshold float64) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Stash the requested threshold via a sentinel int — tests on this
	// branch do not exercise the threshold value, only call presence.
	w.expireLowWeightN = append(w.expireLowWeightN, int(threshold*100))
	return 0, nil
}
func (w *recordingRelWriter) BatchCreate(_ context.Context, rels []*model.Relationship) (model.BatchCreateResult, error) {
	return model.BatchCreateResult{Affected: int64(len(rels))}, nil
}
func (w *recordingRelWriter) BatchExpire(_ context.Context, _ uuid.UUID, ids []uuid.UUID) (int64, error) {
	return int64(len(ids)), nil
}
func (w *recordingRelWriter) BatchReinforce(_ context.Context, _ uuid.UUID, items []model.ReinforceItem) (int64, error) {
	return int64(len(items)), nil
}
func (w *recordingRelWriter) BatchUpdateWeight(_ context.Context, _ uuid.UUID, items []model.WeightUpdateItem) (int64, error) {
	return int64(len(items)), nil
}
func (w *recordingRelWriter) BatchDeleteByID(_ context.Context, _ uuid.UUID, ids []uuid.UUID) (int64, error) {
	return int64(len(ids)), nil
}
func (w *recordingRelWriter) ExpireLowestNTransitive(_ context.Context, _ uuid.UUID, n int) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.expireTransitiveN = append(w.expireTransitiveN, n)
	if w.transitiveResult != nil {
		return *w.transitiveResult, nil
	}
	return int64(n), nil
}

// pressureSettings builds a settings stub with the four knobs the pressure
// branch consults: hard_cap, high_water, low_water, plus the existing
// pruning relationship weight threshold (forced very low so the regular
// pruneRelationships call does not steal records).
func pressureSettings(hardCap int, highWater, lowWater float64) *staticDreamSettings {
	return &staticDreamSettings{
		values: map[string]string{},
		floats: map[string]float64{
			service.SettingDreamTransitiveNamespaceHighWater:       highWater,
			service.SettingDreamTransitiveNamespaceLowWater:        lowWater,
			service.SettingDreamPruningRelationshipWeightThreshold: 0.0001,
		},
		ints: map[string]int{
			service.SettingDreamTransitiveNamespaceHardCap: hardCap,
			// Headroom above any drain target the suite computes so the
			// per-cycle ceiling does not engage; tests that exercise the
			// ceiling set their own batch_size below.
			service.SettingDreamPruningBatchSize: 100000,
		},
	}
}

func pressureCycle() *model.DreamCycle {
	return &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New(), NamespaceID: uuid.New()}
}

// TestPruning_TransitivePressure_BelowHighWater proves no pressure-prune
// fires when the namespace is below the trigger. Floor regression.
func TestPruning_TransitivePressure_BelowHighWater(t *testing.T) {
	// 9000 active, hard_cap 10000, high_water 0.95 → trigger at 9500.
	reader := &pressureFakeRelationshipReader{active: 9000}
	writer := &recordingRelWriter{}
	phase := NewPruningPhase(&fakeMemoryReader{}, &recordingMemoryWriter{}, reader, writer,
		pressureSettings(10000, 0.95, 0.80))
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())

	if _, err := phase.Execute(context.Background(), pressureCycle(), NewTokenBudget(1<<20, 1024), logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(writer.expireTransitiveN) != 0 {
		t.Errorf("expected zero ExpireLowestNTransitive calls below high_water, got %v",
			writer.expireTransitiveN)
	}
}

// TestPruning_TransitivePressure_AboveHighWaterDrains proves the drain
// target is (totalActive - hard_cap * low_water) and that the call fires
// exactly once per cycle.
func TestPruning_TransitivePressure_AboveHighWaterDrains(t *testing.T) {
	// 9700 active, hard_cap 10000, high_water 0.95, low_water 0.80
	// → trigger at 9500, drain to 8000, target = 9700 - 8000 = 1700.
	reader := &pressureFakeRelationshipReader{active: 9700}
	writer := &recordingRelWriter{}
	phase := NewPruningPhase(&fakeMemoryReader{}, &recordingMemoryWriter{}, reader, writer,
		pressureSettings(10000, 0.95, 0.80))
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())

	if _, err := phase.Execute(context.Background(), pressureCycle(), NewTokenBudget(1<<20, 1024), logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(writer.expireTransitiveN) != 1 {
		t.Fatalf("expected exactly 1 ExpireLowestNTransitive call, got %d (%v)",
			len(writer.expireTransitiveN), writer.expireTransitiveN)
	}
	if got, want := writer.expireTransitiveN[0], 1700; got != want {
		t.Errorf("drain target = %d, want %d (9700 active - 8000 low_water_floor)",
			got, want)
	}
}

// TestPruning_TransitivePressure_AtBoundary confirms an exact equal at the
// high-water boundary still triggers. (totalActive >= highThreshold.)
func TestPruning_TransitivePressure_AtBoundary(t *testing.T) {
	// 9500 == 0.95 * 10000 exactly.
	reader := &pressureFakeRelationshipReader{active: 9500}
	writer := &recordingRelWriter{}
	phase := NewPruningPhase(&fakeMemoryReader{}, &recordingMemoryWriter{}, reader, writer,
		pressureSettings(10000, 0.95, 0.80))
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())

	if _, err := phase.Execute(context.Background(), pressureCycle(), NewTokenBudget(1<<20, 1024), logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(writer.expireTransitiveN) != 1 {
		t.Fatalf("expected exactly 1 ExpireLowestNTransitive call at boundary, got %d",
			len(writer.expireTransitiveN))
	}
	if got, want := writer.expireTransitiveN[0], 1500; got != want {
		t.Errorf("drain target = %d, want %d (9500 active - 8000 floor)", got, want)
	}
}

// TestPruning_TransitivePressure_Misconfigured proves the defensive
// no-op path: if low_water >= high_water (which the API validator rejects
// but a manual DB edit can land), the pressure prune bails rather than
// thrashing.
func TestPruning_TransitivePressure_Misconfigured(t *testing.T) {
	reader := &pressureFakeRelationshipReader{active: 9700}
	writer := &recordingRelWriter{}
	// low_water 0.99 >= high_water 0.95 — invalid pair.
	phase := NewPruningPhase(&fakeMemoryReader{}, &recordingMemoryWriter{}, reader, writer,
		pressureSettings(10000, 0.95, 0.99))
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())

	if _, err := phase.Execute(context.Background(), pressureCycle(), NewTokenBudget(1<<20, 1024), logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(writer.expireTransitiveN) != 0 {
		t.Errorf("expected zero ExpireLowestNTransitive calls on misconfigured pair, got %v",
			writer.expireTransitiveN)
	}
}

// TestPruning_TransitivePressure_PerCycleCap proves the drain target is
// clamped to dreaming.pruning.batch_size so a single cycle on a very large
// namespace cannot issue an unbounded UPDATE. With hard_cap=1,000,000 and
// the default high/low waters the raw drain would be 150,000 rows in one
// call; this test pins the protective ceiling that splits the drain
// across cycles.
func TestPruning_TransitivePressure_PerCycleCap(t *testing.T) {
	// 950000 active, hard_cap 1000000, high_water 0.95, low_water 0.80 →
	// raw target = 950000 - 800000 = 150000. With per-cycle cap of 5000,
	// the call should fire with target 5000.
	reader := &pressureFakeRelationshipReader{active: 950000}
	writer := &recordingRelWriter{}
	settings := pressureSettings(1000000, 0.95, 0.80)
	settings.ints[service.SettingDreamPruningBatchSize] = 5000
	phase := NewPruningPhase(&fakeMemoryReader{}, &recordingMemoryWriter{}, reader, writer, settings)
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())

	if _, err := phase.Execute(context.Background(), pressureCycle(), NewTokenBudget(1<<20, 1024), logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(writer.expireTransitiveN) != 1 {
		t.Fatalf("expected exactly 1 ExpireLowestNTransitive call, got %d (%v)",
			len(writer.expireTransitiveN), writer.expireTransitiveN)
	}
	if got, want := writer.expireTransitiveN[0], 5000; got != want {
		t.Errorf("drain target = %d, want %d (capped by batch_size, not raw 150000)", got, want)
	}
}

// TestPruning_TransitivePressure_NilReaderNoOps proves the test-friendly
// fallback: a PruningPhase constructed without a RelationshipReader (the
// established pattern for tests that do not exercise this branch) simply
// skips the pressure prune.
func TestPruning_TransitivePressure_NilReaderNoOps(t *testing.T) {
	writer := &recordingRelWriter{}
	phase := NewPruningPhase(&fakeMemoryReader{}, &recordingMemoryWriter{}, nil, writer,
		pressureSettings(10000, 0.95, 0.80))
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())

	if _, err := phase.Execute(context.Background(), pressureCycle(), NewTokenBudget(1<<20, 1024), logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(writer.expireTransitiveN) != 0 {
		t.Errorf("expected zero ExpireLowestNTransitive calls when reader is nil, got %v",
			writer.expireTransitiveN)
	}
}
