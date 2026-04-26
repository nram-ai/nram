package dreaming

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// paraphraseTestVectorStore is a focused VectorStore double for the
// paraphrase dedup tests. searchResults is keyed by anchor ID and
// returned verbatim; vectorsByID feeds GetByIDs.
type paraphraseTestVectorStore struct {
	vectorsByID   map[uuid.UUID][]float32
	searchResults map[uuid.UUID][]storage.VectorSearchResult
	deleted       []uuid.UUID
}

func (f *paraphraseTestVectorStore) Upsert(_ context.Context, _ storage.VectorKind, _ uuid.UUID, _ uuid.UUID, _ []float32, _ int) error {
	return nil
}
func (f *paraphraseTestVectorStore) UpsertBatch(_ context.Context, _ []storage.VectorUpsertItem) error {
	return nil
}
func (f *paraphraseTestVectorStore) Search(_ context.Context, _ storage.VectorKind, embedding []float32, _ uuid.UUID, _ int, _ int) ([]storage.VectorSearchResult, error) {
	// Resolve anchor by exact-match scan.
	for id, v := range f.vectorsByID {
		if vectorsEqual(v, embedding) {
			return f.searchResults[id], nil
		}
	}
	return nil, nil
}
func (f *paraphraseTestVectorStore) GetByIDs(_ context.Context, _ storage.VectorKind, ids []uuid.UUID, _ int) (map[uuid.UUID][]float32, error) {
	out := make(map[uuid.UUID][]float32, len(ids))
	for _, id := range ids {
		if v, ok := f.vectorsByID[id]; ok {
			cp := make([]float32, len(v))
			copy(cp, v)
			out[id] = cp
		}
	}
	return out, nil
}
func (f *paraphraseTestVectorStore) Delete(_ context.Context, _ storage.VectorKind, id uuid.UUID) error {
	f.deleted = append(f.deleted, id)
	return nil
}
func (f *paraphraseTestVectorStore) TruncateAllVectors(_ context.Context) error { return nil }
func (f *paraphraseTestVectorStore) Ping(_ context.Context) error               { return nil }

func vectorsEqual(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func paraphraseDedupSettings() *staticDreamSettings {
	return &staticDreamSettings{
		values: map[string]string{
			service.SettingDreamParaphraseEnabled: "true",
		},
		floats: map[string]float64{
			service.SettingDreamParaphraseThreshold: 0.97,
		},
		ints: map[string]int{
			service.SettingDreamParaphraseCapPerCycle: 500,
			service.SettingDreamParaphraseTopK:        5,
		},
	}
}

func userMemoryWithVector(content string, confidence float64, createdAt time.Time, dim int, namespace uuid.UUID, firstComponent float32) (model.Memory, []float32) {
	src := "user"
	d := dim
	mem := model.Memory{
		ID:           uuid.New(),
		NamespaceID:  namespace,
		Content:      content,
		Source:       &src,
		Confidence:   confidence,
		Metadata:     json.RawMessage(`{}`),
		CreatedAt:    createdAt,
		UpdatedAt:    createdAt,
		EmbeddingDim: &d,
	}
	vec := make([]float32, dim)
	vec[0] = firstComponent
	return mem, vec
}

// TestParaphraseDedupPhase_SupersedesNearDuplicate is the load-bearing
// test: two user-source memories with identical vectors get paired by
// the sweep (not just by contradiction's anchor walk). On a confidence
// tie, the older CreatedAt survives (matches the contradiction-phase
// paraphrase fast-path's intent: prefer the stable historic record over
// a recent rewrite).
func TestParaphraseDedupPhase_SupersedesNearDuplicate(t *testing.T) {
	ns := uuid.New()
	dim := 2
	older, vecOlder := userMemoryWithVector("Mem0 audit found 97.8 percent agreement", 1.0, time.Now().Add(-72*time.Hour), dim, ns, 1.0)
	newer, vecNewer := userMemoryWithVector("Mem0 audit reports 97.8% agreement.", 1.0, time.Now().Add(-1*time.Hour), dim, ns, 2.0)

	reader := &fakeMemoryReader{list: []model.Memory{older, newer}}
	writer := &updatingMemoryWriter{}
	vs := &paraphraseTestVectorStore{
		vectorsByID: map[uuid.UUID][]float32{
			older.ID: vecOlder,
			newer.ID: vecNewer,
		},
		searchResults: map[uuid.UUID][]storage.VectorSearchResult{
			older.ID: {
				{ID: older.ID, Score: 1.0, NamespaceID: ns},
				{ID: newer.ID, Score: 1.0, NamespaceID: ns},
			},
			newer.ID: {
				{ID: newer.ID, Score: 1.0, NamespaceID: ns},
				{ID: older.ID, Score: 1.0, NamespaceID: ns},
			},
		},
	}

	phase := NewParaphraseDedupPhase(reader, writer, vs, vs, nil, paraphraseDedupSettings())
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(writer.updates) == 0 {
		t.Fatalf("expected at least one Update; got 0")
	}

	var loserUpdate *model.Memory
	for i := range writer.updates {
		u := writer.updates[i]
		if u.SupersededBy != nil {
			loserUpdate = &u
			break
		}
	}
	if loserUpdate == nil {
		t.Fatalf("no Update with SupersededBy set; updates=%d", len(writer.updates))
	}
	if loserUpdate.ID != newer.ID {
		t.Errorf("loser should be the newer memory (confidence tie → older survives). got %s, want %s", loserUpdate.ID, newer.ID)
	}
	if *loserUpdate.SupersededBy != older.ID {
		t.Errorf("loser should point to older as winner; got %s, want %s", *loserUpdate.SupersededBy, older.ID)
	}

	purgedLoser := false
	for _, id := range vs.deleted {
		if id == newer.ID {
			purgedLoser = true
			break
		}
	}
	if !purgedLoser {
		t.Errorf("expected vector purge on loser %s; deleted=%v", newer.ID, vs.deleted)
	}
}

// TestParaphraseDedupPhase_StampsSurvivorWithoutMatch confirms that a
// memory the sweep visits but finds no near-paraphrase for is stamped
// so the next cycle skips it.
func TestParaphraseDedupPhase_StampsSurvivorWithoutMatch(t *testing.T) {
	ns := uuid.New()
	dim := 2
	mem, vec := userMemoryWithVector("Unique content with no paraphrases", 1.0, time.Now(), dim, ns, 1.0)

	reader := &fakeMemoryReader{list: []model.Memory{mem}}
	writer := &updatingMemoryWriter{}
	vs := &paraphraseTestVectorStore{
		vectorsByID: map[uuid.UUID][]float32{mem.ID: vec},
		searchResults: map[uuid.UUID][]storage.VectorSearchResult{
			mem.ID: {
				{ID: mem.ID, Score: 1.0, NamespaceID: ns},
			},
		},
	}

	phase := NewParaphraseDedupPhase(reader, writer, vs, vs, nil, paraphraseDedupSettings())
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(writer.updates) != 1 {
		t.Fatalf("expected exactly 1 Update (stamp survivor), got %d", len(writer.updates))
	}
	updated := writer.updates[0]
	if updated.SupersededBy != nil {
		t.Errorf("survivor should not be superseded; got SupersededBy=%v", updated.SupersededBy)
	}
	var meta map[string]interface{}
	_ = json.Unmarshal(updated.Metadata, &meta)
	if _, ok := meta[ParaphraseCheckedStampKey]; !ok {
		t.Errorf("survivor should carry %s stamp; got metadata=%s", ParaphraseCheckedStampKey, string(updated.Metadata))
	}
}

// TestParaphraseDedupPhase_SkipsAlreadyStamped confirms the staleness
// gate: a memory whose paraphrase_checked_at is >= UpdatedAt is skipped
// entirely (no Update).
func TestParaphraseDedupPhase_SkipsAlreadyStamped(t *testing.T) {
	ns := uuid.New()
	dim := 2
	mem, vec := userMemoryWithVector("Content already paraphrase-checked", 1.0, time.Now(), dim, ns, 1.0)
	meta := map[string]interface{}{
		ParaphraseCheckedStampKey: mem.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
	raw, _ := json.Marshal(meta)
	mem.Metadata = raw

	reader := &fakeMemoryReader{list: []model.Memory{mem}}
	writer := &updatingMemoryWriter{}
	vs := &paraphraseTestVectorStore{
		vectorsByID: map[uuid.UUID][]float32{mem.ID: vec},
	}

	phase := NewParaphraseDedupPhase(reader, writer, vs, vs, nil, paraphraseDedupSettings())
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(writer.updates) != 0 {
		t.Errorf("already-stamped memory must not be touched; got %d updates", len(writer.updates))
	}
}

// TestParaphraseDedupPhase_DisabledByZeroSetting confirms the global
// kill switch.
func TestParaphraseDedupPhase_DisabledByZeroSetting(t *testing.T) {
	settings := paraphraseDedupSettings()
	settings.values[service.SettingDreamParaphraseEnabled] = "false"

	ns := uuid.New()
	mem, vec := userMemoryWithVector("anything", 1.0, time.Now(), 2, ns, 1.0)
	reader := &fakeMemoryReader{list: []model.Memory{mem}}
	writer := &updatingMemoryWriter{}
	vs := &paraphraseTestVectorStore{vectorsByID: map[uuid.UUID][]float32{mem.ID: vec}}

	phase := NewParaphraseDedupPhase(reader, writer, vs, vs, func() provider.EmbeddingProvider { return nil }, settings)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(writer.updates) != 0 {
		t.Errorf("disabled phase must be a no-op, got %d updates", len(writer.updates))
	}
}
