package dreaming

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// --- Doubles for the dreaming interfaces needed by applyConfidenceDecay ---

type fakeMemoryReader struct {
	list []model.Memory
}

func (f *fakeMemoryReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	for i := range f.list {
		if f.list[i].ID == id {
			return &f.list[i], nil
		}
	}
	return nil, fmt.Errorf("not found")
}
func (f *fakeMemoryReader) ListByNamespace(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
	return f.list, nil
}
func (f *fakeMemoryReader) CountByNamespace(_ context.Context, _ uuid.UUID) (int, error) {
	return len(f.list), nil
}

type decayCall struct {
	IDs        []uuid.UUID
	Multiplier float64
	Floor      float64
}

type recordingMemoryWriter struct {
	calls []decayCall
}

func (r *recordingMemoryWriter) Create(_ context.Context, _ *model.Memory) error { return nil }
func (r *recordingMemoryWriter) Update(_ context.Context, _ *model.Memory) error { return nil }
func (r *recordingMemoryWriter) SoftDelete(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}
func (r *recordingMemoryWriter) HardDelete(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}
func (r *recordingMemoryWriter) DecayConfidence(_ context.Context, ids []uuid.UUID, multiplier, floor float64) (int64, error) {
	r.calls = append(r.calls, decayCall{
		IDs:        append([]uuid.UUID(nil), ids...),
		Multiplier: multiplier,
		Floor:      floor,
	})
	return int64(len(ids)), nil
}

type staticDreamSettings struct {
	values map[string]string
	floats map[string]float64
}

func (s *staticDreamSettings) Resolve(_ context.Context, key string, _ string) (string, error) {
	if v, ok := s.values[key]; ok {
		return v, nil
	}
	return "", nil
}
func (s *staticDreamSettings) ResolveFloat(_ context.Context, key string, _ string) (float64, error) {
	if v, ok := s.floats[key]; ok {
		return v, nil
	}
	return 0, errors.New("unused")
}
func (s *staticDreamSettings) ResolveInt(_ context.Context, _ string, _ string) (int, error) {
	return 0, errors.New("unused")
}
func (s *staticDreamSettings) ResolveBool(_ context.Context, key string, _ string) bool {
	v := s.values[key]
	return v == "true" || v == "1"
}

// --- Helpers ---

func decayTestCycle() *model.DreamCycle {
	return &model.DreamCycle{
		ID:          uuid.New(),
		ProjectID:   uuid.New(),
		NamespaceID: uuid.New(),
	}
}

func decayTestMemory(confidence float64, lastAccessed *time.Time, createdAt time.Time) model.Memory {
	return model.Memory{
		ID:           uuid.New(),
		NamespaceID:  uuid.New(),
		Content:      "x",
		Confidence:   confidence,
		LastAccessed: lastAccessed,
		CreatedAt:    createdAt,
		UpdatedAt:    createdAt,
	}
}

// --- Tests ---

// decayPhase builds a PruningPhase with standard test settings and returns it
// alongside the recording writer so assertions can inspect DecayConfidence calls.
func decayPhase(enabled bool) (*PruningPhase, *recordingMemoryWriter) {
	writer := &recordingMemoryWriter{}
	values := map[string]string{}
	if enabled {
		values[service.SettingConfidenceDecayEnabled] = "true"
	}
	settings := &staticDreamSettings{
		values: values,
		floats: map[string]float64{
			service.SettingConfidenceDecayThresholdDays: 14,
			service.SettingConfidenceDecayRatePerCycle:  0.02,
			service.SettingConfidenceFloor:              0.05,
		},
	}
	return NewPruningPhase(&fakeMemoryReader{}, writer, nil, settings), writer
}

func TestApplyConfidenceDecay_DisabledByDefault(t *testing.T) {
	phase, writer := decayPhase(false)
	memories := []model.Memory{decayTestMemory(0.9, nil, time.Now().Add(-365*24*time.Hour))}

	if err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), memories); err != nil {
		t.Fatalf("decay: %v", err)
	}
	if len(writer.calls) != 0 {
		t.Errorf("decay must be a no-op when disabled; got %d calls", len(writer.calls))
	}
}

func TestApplyConfidenceDecay_NilSettings_NoOp(t *testing.T) {
	phase := NewPruningPhase(&fakeMemoryReader{}, &recordingMemoryWriter{}, nil, nil)
	if err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), nil); err != nil {
		t.Fatalf("nil settings must not error: %v", err)
	}
}

func TestApplyConfidenceDecay_EnabledBelowThreshold_Skips(t *testing.T) {
	phase, writer := decayPhase(true)
	recent := time.Now().Add(-5 * 24 * time.Hour) // within 14-day threshold
	memories := []model.Memory{decayTestMemory(0.9, &recent, time.Now().Add(-365*24*time.Hour))}

	if err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), memories); err != nil {
		t.Fatalf("decay: %v", err)
	}
	if len(writer.calls) != 0 {
		t.Errorf("recent memories must not be eligible; got %d decay calls", len(writer.calls))
	}
}

func TestApplyConfidenceDecay_EnabledBeyondThreshold_Decays(t *testing.T) {
	phase, writer := decayPhase(true)
	old := time.Now().Add(-30 * 24 * time.Hour)
	mem := decayTestMemory(0.9, &old, time.Now().Add(-365*24*time.Hour))
	memories := []model.Memory{mem}

	if err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), memories); err != nil {
		t.Fatalf("decay: %v", err)
	}
	if len(writer.calls) != 1 {
		t.Fatalf("want 1 decay call, got %d", len(writer.calls))
	}
	call := writer.calls[0]
	if len(call.IDs) != 1 || call.IDs[0] != mem.ID {
		t.Errorf("decay call ids: want [%s], got %v", mem.ID, call.IDs)
	}
	if call.Multiplier < 0.98-1e-9 || call.Multiplier > 0.98+1e-9 {
		t.Errorf("decay multiplier: want 0.98, got %v", call.Multiplier)
	}
	if call.Floor != 0.05 {
		t.Errorf("decay floor: want 0.05, got %v", call.Floor)
	}
	// Post-decay mirror: caller's slice should see the scaled value so the
	// subsequent prune step reads it without a re-fetch.
	wantConfidence := 0.9 * 0.98
	if memories[0].Confidence < wantConfidence-1e-9 || memories[0].Confidence > wantConfidence+1e-9 {
		t.Errorf("in-memory confidence after decay: want %v, got %v", wantConfidence, memories[0].Confidence)
	}
}

func TestApplyConfidenceDecay_SkipsMemoriesAtOrBelowFloor(t *testing.T) {
	phase, writer := decayPhase(true)
	old := time.Now().Add(-30 * 24 * time.Hour)
	created := time.Now().Add(-365 * 24 * time.Hour)
	memories := []model.Memory{
		decayTestMemory(0.05, &old, created), // at floor
		decayTestMemory(0.01, &old, created), // below floor
	}

	if err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), memories); err != nil {
		t.Fatalf("decay: %v", err)
	}
	if len(writer.calls) != 0 {
		t.Errorf("memories at/below floor must not be decayed; got %d calls", len(writer.calls))
	}
}

func TestApplyConfidenceDecay_SkipsSoftDeleted(t *testing.T) {
	phase, writer := decayPhase(true)
	old := time.Now().Add(-30 * 24 * time.Hour)
	deletedAt := time.Now().Add(-1 * time.Hour)
	mem := decayTestMemory(0.9, &old, time.Now().Add(-365*24*time.Hour))
	mem.DeletedAt = &deletedAt

	if err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), []model.Memory{mem}); err != nil {
		t.Fatalf("decay: %v", err)
	}
	if len(writer.calls) != 0 {
		t.Errorf("soft-deleted memory must be skipped; got %d calls", len(writer.calls))
	}
}

func TestApplyConfidenceDecay_NeverAccessedFallsBackToCreatedAt(t *testing.T) {
	phase, writer := decayPhase(true)
	// last_accessed nil, created_at 30 days ago → eligible via created_at.
	mem := decayTestMemory(0.9, nil, time.Now().Add(-30*24*time.Hour))

	if err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), []model.Memory{mem}); err != nil {
		t.Fatalf("decay: %v", err)
	}
	if len(writer.calls) != 1 {
		t.Errorf("never-accessed but aged memory should be eligible; got %d calls", len(writer.calls))
	}
}

func TestApplyConfidenceDecay_BadSettingsFallBackToDefaults(t *testing.T) {
	writer := &recordingMemoryWriter{}
	settings := &staticDreamSettings{
		values: map[string]string{service.SettingConfidenceDecayEnabled: "true"},
		// floats unset → all fallback to defaults.
	}
	phase := NewPruningPhase(&fakeMemoryReader{}, writer, nil, settings)
	old := time.Now().Add(-30 * 24 * time.Hour)
	memories := []model.Memory{decayTestMemory(0.9, &old, time.Now().Add(-365*24*time.Hour))}

	if err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), memories); err != nil {
		t.Fatalf("decay: %v", err)
	}
	if len(writer.calls) != 1 {
		t.Fatalf("want 1 call with defaults, got %d", len(writer.calls))
	}
	call := writer.calls[0]
	if call.Multiplier < 0.98-1e-9 || call.Multiplier > 0.98+1e-9 {
		t.Errorf("default multiplier: want 0.98, got %v", call.Multiplier)
	}
	if call.Floor != defaultConfidenceFloor {
		t.Errorf("default floor: want %v, got %v", defaultConfidenceFloor, call.Floor)
	}
}
