package dreaming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// --- Doubles for the dreaming interfaces needed by applyConfidenceDecay ---

type listCall struct {
	Limit  int
	Offset int
}

type fakeMemoryReader struct {
	list      []model.Memory
	listCalls []listCall
}

func (f *fakeMemoryReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	for i := range f.list {
		if f.list[i].ID == id {
			return &f.list[i], nil
		}
	}
	return nil, fmt.Errorf("not found")
}
func (f *fakeMemoryReader) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	want := make(map[uuid.UUID]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}
	out := make([]model.Memory, 0, len(ids))
	for i := range f.list {
		if _, ok := want[f.list[i].ID]; ok {
			out = append(out, f.list[i])
		}
	}
	return out, nil
}
func (f *fakeMemoryReader) ListByNamespace(_ context.Context, _ uuid.UUID, limit, offset int) ([]model.Memory, error) {
	f.listCalls = append(f.listCalls, listCall{Limit: limit, Offset: offset})
	if offset >= len(f.list) {
		return []model.Memory{}, nil
	}
	end := offset + limit
	if limit <= 0 || end > len(f.list) {
		end = len(f.list)
	}
	out := make([]model.Memory, end-offset)
	copy(out, f.list[offset:end])
	return out, nil
}
func (f *fakeMemoryReader) ListByNamespaceStale(_ context.Context, _ uuid.UUID, _ string, _ int) ([]model.Memory, error) {
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
func (r *recordingMemoryWriter) UpdateMetadata(_ context.Context, _, _ uuid.UUID, _ json.RawMessage) error {
	return nil
}
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
func (r *recordingMemoryWriter) UpdateEmbeddingDim(_ context.Context, _ uuid.UUID, _ int) error {
	return nil
}
func (r *recordingMemoryWriter) ClearEmbeddingDim(_ context.Context, _, _ uuid.UUID) error {
	return nil
}
func (r *recordingMemoryWriter) UpdateConfidence(_ context.Context, _, _ uuid.UUID, _ float64) error {
	return nil
}
func (r *recordingMemoryWriter) Demote(_ context.Context, _, _ uuid.UUID, _ json.RawMessage) error {
	return nil
}
func (r *recordingMemoryWriter) MarkSupersededBy(_ context.Context, _, _, _ uuid.UUID) error {
	return nil
}

type staticDreamSettings struct {
	values map[string]string
	floats map[string]float64
	ints   map[string]int
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
func (s *staticDreamSettings) ResolveInt(_ context.Context, key string, _ string) (int, error) {
	if v, ok := s.ints[key]; ok {
		return v, nil
	}
	return 0, errors.New("unused")
}
func (s *staticDreamSettings) ResolveBool(_ context.Context, key string, _ string) bool {
	v := s.values[key]
	return v == "true" || v == "1"
}
func (s *staticDreamSettings) ResolveIntWithDefault(ctx context.Context, key, scope string) int {
	if v, ok := s.ints[key]; ok {
		return v
	}
	return service.GetDefaultInt(key)
}
func (s *staticDreamSettings) ResolveFloatWithDefault(ctx context.Context, key, scope string) float64 {
	if v, ok := s.floats[key]; ok {
		return v
	}
	return service.GetDefaultFloat(key)
}
func (s *staticDreamSettings) ResolveDurationSecondsWithDefault(ctx context.Context, key, scope string) time.Duration {
	return time.Duration(s.ResolveIntWithDefault(ctx, key, scope)) * time.Second
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
	return NewPruningPhase(&fakeMemoryReader{}, writer, nil, nil, settings), writer
}

func TestApplyConfidenceDecay_DisabledByDefault(t *testing.T) {
	phase, writer := decayPhase(false)
	memories := []model.Memory{decayTestMemory(0.9, nil, time.Now().Add(-365*24*time.Hour))}

	if _, err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), memories); err != nil {
		t.Fatalf("decay: %v", err)
	}
	if len(writer.calls) != 0 {
		t.Errorf("decay must be a no-op when disabled; got %d calls", len(writer.calls))
	}
}

func TestApplyConfidenceDecay_NilSettings_NoOp(t *testing.T) {
	phase := NewPruningPhase(&fakeMemoryReader{}, &recordingMemoryWriter{}, nil, nil, nil)
	if _, err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), nil); err != nil {
		t.Fatalf("nil settings must not error: %v", err)
	}
}

func TestApplyConfidenceDecay_EnabledBelowThreshold_Skips(t *testing.T) {
	phase, writer := decayPhase(true)
	recent := time.Now().Add(-5 * 24 * time.Hour) // within 14-day threshold
	memories := []model.Memory{decayTestMemory(0.9, &recent, time.Now().Add(-365*24*time.Hour))}

	if _, err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), memories); err != nil {
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

	if _, err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), memories); err != nil {
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

	if _, err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), memories); err != nil {
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

	if _, err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), []model.Memory{mem}); err != nil {
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

	if _, err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), []model.Memory{mem}); err != nil {
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
	phase := NewPruningPhase(&fakeMemoryReader{}, writer, nil, nil, settings)
	old := time.Now().Add(-30 * 24 * time.Hour)
	memories := []model.Memory{decayTestMemory(0.9, &old, time.Now().Add(-365*24*time.Hour))}

	if _, err := phase.applyConfidenceDecay(context.Background(), decayTestCycle(), memories); err != nil {
		t.Fatalf("decay: %v", err)
	}
	if len(writer.calls) != 1 {
		t.Fatalf("want 1 call with defaults, got %d", len(writer.calls))
	}
	call := writer.calls[0]
	if call.Multiplier < 0.98-1e-9 || call.Multiplier > 0.98+1e-9 {
		t.Errorf("default multiplier: want 0.98, got %v", call.Multiplier)
	}
	wantFloor := service.GetDefaultFloat(service.SettingConfidenceFloor)
	if call.Floor != wantFloor {
		t.Errorf("default floor: want %v, got %v", wantFloor, call.Floor)
	}
}

// --- minimal RelationshipWriter stub for Execute integration tests ---

type noopRelWriter struct{}

func (noopRelWriter) Create(_ context.Context, _ *model.Relationship) error      { return nil }
func (noopRelWriter) Reinforce(_ context.Context, _, _ uuid.UUID, _ float64) error { return nil }
func (noopRelWriter) Expire(_ context.Context, _, _ uuid.UUID) error              { return nil }
func (noopRelWriter) DeleteByID(_ context.Context, _, _ uuid.UUID) error          { return nil }
func (noopRelWriter) UpdateWeight(_ context.Context, _, _ uuid.UUID, _ float64) error {
	return nil
}
func (noopRelWriter) ExpireLowWeight(_ context.Context, _ uuid.UUID, _ float64) (int64, error) {
	return 0, nil
}
func (noopRelWriter) ExpireLowestNTransitive(_ context.Context, _ uuid.UUID, _ int) (int64, error) {
	return 0, nil
}

// TestPruning_Execute_StreamsAllBatches confirms 2500 memories paginate
// into three batches at offsets 0/1000/2000, the loop terminates on the
// short final batch, and exactly one phase_summary log entry is emitted.
func TestPruning_Execute_StreamsAllBatches(t *testing.T) {
	const total = 2500

	now := time.Now()
	memories := make([]model.Memory, total)
	for i := range memories {
		memories[i] = model.Memory{
			ID:          uuid.New(),
			NamespaceID: uuid.New(),
			Confidence:  0.9, // above floor; decay disabled so this is moot
			CreatedAt:   now.Add(-time.Duration(i) * time.Hour),
			UpdatedAt:   now.Add(-time.Duration(i) * time.Hour),
			AccessCount: 1, // not zero, so the supersede-zero-access prune cannot fire
		}
	}

	reader := &fakeMemoryReader{list: memories}
	writer := &recordingMemoryWriter{}
	settings := &staticDreamSettings{
		values: map[string]string{}, // decay disabled
		ints:   map[string]int{service.SettingDreamPruningBatchSize: 1000},
	}
	phase := NewPruningPhase(reader, writer, nil, noopRelWriter{}, settings)
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())

	cycle := &model.DreamCycle{
		ID:          uuid.New(),
		ProjectID:   uuid.New(),
		NamespaceID: uuid.New(),
	}

	result, err := phase.Execute(context.Background(), cycle, NewTokenBudget(1<<20, 1024), logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.HasResidual {
		t.Error("pruning must never report residual")
	}

	// 3 batches of 1000+1000+500. The third call short-returns (500 < 1000)
	// and the loop terminates without a fourth probe.
	wantOffsets := []int{0, 1000, 2000}
	if len(reader.listCalls) != len(wantOffsets) {
		t.Fatalf("expected %d ListByNamespace calls, got %d (calls=%+v)",
			len(wantOffsets), len(reader.listCalls), reader.listCalls)
	}
	for i, want := range wantOffsets {
		if reader.listCalls[i].Offset != want {
			t.Errorf("call %d: expected offset %d, got %d", i, want, reader.listCalls[i].Offset)
		}
		if reader.listCalls[i].Limit != 1000 {
			t.Errorf("call %d: expected limit 1000, got %d", i, reader.listCalls[i].Limit)
		}
	}

	// Phase emits exactly one LogOperation: the phase_summary. No memories
	// trip shouldPrune (Confidence=0.9, AccessCount=1, no SupersededBy), no
	// relationships expire (noopRelWriter), and decay is disabled.
	if logger.OpCount() != 1 {
		t.Errorf("expected exactly 1 logged operation (phase_summary), got %d", logger.OpCount())
	}
}

// TestPruning_Execute_BatchSizeFromSetting confirms the streaming chunk
// size honors dreaming.pruning.batch_size when configured.
func TestPruning_Execute_BatchSizeFromSetting(t *testing.T) {
	memories := make([]model.Memory, 750)
	for i := range memories {
		memories[i] = model.Memory{
			ID:          uuid.New(),
			NamespaceID: uuid.New(),
			Confidence:  0.9,
			AccessCount: 1,
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}
	}

	reader := &fakeMemoryReader{list: memories}
	settings := &staticDreamSettings{
		ints: map[string]int{service.SettingDreamPruningBatchSize: 250},
	}
	phase := NewPruningPhase(reader, &recordingMemoryWriter{}, nil, noopRelWriter{}, settings)
	logger := NewDreamLogWriter(nil, uuid.New(), uuid.New())

	cycle := &model.DreamCycle{ID: uuid.New(), ProjectID: uuid.New(), NamespaceID: uuid.New()}
	if _, err := phase.Execute(context.Background(), cycle, NewTokenBudget(1<<20, 1024), logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// 750 / 250 = 3 full batches; loop exits after the third (short-circuit
	// only fires on len(batch) < batchSize, so a 4th probe lands at offset
	// 750 and returns empty).
	if got, want := len(reader.listCalls), 4; got != want {
		t.Errorf("expected %d ListByNamespace calls, got %d", want, got)
	}
	for i, c := range reader.listCalls {
		if c.Limit != 250 {
			t.Errorf("call %d: expected limit 250 (from setting), got %d", i, c.Limit)
		}
	}
}
