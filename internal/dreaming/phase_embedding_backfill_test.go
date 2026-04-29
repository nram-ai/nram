package dreaming

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// fakeMemoryDimRepairer drives the backfill phase with a fixed list of
// divergent rows per dim. The phase queries one dim at a time so the
// double maps dim → rows.
type fakeMemoryDimRepairer struct {
	rowsByDim    map[int][]model.Memory
	queryCalls   int
	lastDim      int
	lastLimit    int
	errOnDim     int
	errToReturn  error
}

func (f *fakeMemoryDimRepairer) FindMemoriesMissingVector(_ context.Context, _ uuid.UUID, dim, limit int) ([]model.Memory, error) {
	f.queryCalls++
	f.lastDim = dim
	f.lastLimit = limit
	if f.errOnDim == dim && f.errToReturn != nil {
		return nil, f.errToReturn
	}
	rows, ok := f.rowsByDim[dim]
	if !ok {
		return nil, nil
	}
	if limit < len(rows) {
		return append([]model.Memory(nil), rows[:limit]...), nil
	}
	return append([]model.Memory(nil), rows...), nil
}

// recordingVectorStore captures Upsert and Delete calls so backfill tests
// can assert what got repaired vs. cleared.
type recordingVectorStore struct {
	upserts    []vectorUpsertRecord
	deletes    []uuid.UUID
	upsertErr  error
}

type vectorUpsertRecord struct {
	ID          uuid.UUID
	NamespaceID uuid.UUID
	Embedding   []float32
	Dimension   int
}

func (r *recordingVectorStore) Upsert(_ context.Context, _ storage.VectorKind, id, ns uuid.UUID, emb []float32, dim int) error {
	if r.upsertErr != nil {
		return r.upsertErr
	}
	cp := append([]float32(nil), emb...)
	r.upserts = append(r.upserts, vectorUpsertRecord{ID: id, NamespaceID: ns, Embedding: cp, Dimension: dim})
	return nil
}
func (r *recordingVectorStore) UpsertBatch(_ context.Context, _ []storage.VectorUpsertItem) error {
	return nil
}
func (r *recordingVectorStore) Search(_ context.Context, _ storage.VectorKind, _ []float32, _ uuid.UUID, _ int, _ int) ([]storage.VectorSearchResult, error) {
	return nil, nil
}
func (r *recordingVectorStore) GetByIDs(_ context.Context, _ storage.VectorKind, _ []uuid.UUID, _ int) (map[uuid.UUID][]float32, error) {
	return nil, nil
}
func (r *recordingVectorStore) Delete(_ context.Context, _ storage.VectorKind, id uuid.UUID) error {
	r.deletes = append(r.deletes, id)
	return nil
}
func (r *recordingVectorStore) TruncateAllVectors(_ context.Context) error { return nil }
func (r *recordingVectorStore) Ping(_ context.Context) error               { return nil }

// failingEmbedder returns an error from every Embed call. Used to drive
// the clearDim fallback path.
type failingEmbedder struct {
	err   error
	calls int
}

func (f *failingEmbedder) Embed(_ context.Context, _ *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	f.calls++
	return nil, f.err
}
func (f *failingEmbedder) Name() string         { return "failing-embedder" }
func (f *failingEmbedder) Models() []string     { return []string{"failing-model"} }
func (f *failingEmbedder) Dimensions() []int    { return []int{384} }
func (f *failingEmbedder) DefaultDimension() int { return 384 }

func backfillSettings(enabled bool, capPerCycle int) *staticDreamSettings {
	values := map[string]string{}
	if enabled {
		values[service.SettingDreamEmbeddingBackfillEnabled] = "true"
	} else {
		values[service.SettingDreamEmbeddingBackfillEnabled] = "false"
	}
	return &staticDreamSettings{
		values: values,
		ints: map[string]int{
			service.SettingDreamEmbeddingBackfillCapPerCycle: capPerCycle,
		},
	}
}

// memWithDim builds a memory whose recorded embedding_dim matches the
// per-dim find query but whose vector is missing — exactly the shape the
// backfill phase repairs.
func memWithDim(content string, dim int, ns uuid.UUID) model.Memory {
	d := dim
	return model.Memory{
		ID:           uuid.New(),
		NamespaceID:  ns,
		Content:      content,
		Confidence:   1.0,
		EmbeddingDim: &d,
		UpdatedAt:    time.Now().UTC(),
	}
}

// TestEmbeddingBackfillPhase_RepairsMissingVectors is the happy path:
// embedder is healthy, every divergent row gets re-embedded and the
// vector store sees an Upsert for each.
func TestEmbeddingBackfillPhase_RepairsMissingVectors(t *testing.T) {
	ns := uuid.New()
	dim := 384
	row := memWithDim("re-embed me", dim, ns)

	repairer := &fakeMemoryDimRepairer{
		rowsByDim: map[int][]model.Memory{dim: {row}},
	}
	writer := &updatingMemoryWriter{}
	vs := &recordingVectorStore{}
	emb := &staticEmbedder{vectors: [][]float32{makeUnitVec(dim)}}

	phase := NewEmbeddingBackfillPhase(
		repairer, writer, vs,
		func() provider.EmbeddingProvider { return emb },
		backfillSettings(true, 200),
	)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	residual, err := phase.Execute(context.Background(), cycle, budget, logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if residual {
		t.Errorf("residual must be false when all rows fit under cap; got true")
	}
	if len(vs.upserts) != 1 {
		t.Fatalf("expected 1 vector Upsert; got %d", len(vs.upserts))
	}
	if vs.upserts[0].ID != row.ID {
		t.Errorf("Upsert target mismatch; got %s, want %s", vs.upserts[0].ID, row.ID)
	}
	if vs.upserts[0].Dimension != dim {
		t.Errorf("Upsert dim mismatch; got %d, want %d", vs.upserts[0].Dimension, dim)
	}
	// Row already had EmbeddingDim = dim; re-embed at same dim should
	// not need a follow-up Update.
	if len(writer.updates) != 0 {
		t.Errorf("expected no Update when re-embed dim matches existing; got %d", len(writer.updates))
	}
}

// TestEmbeddingBackfillPhase_ClearsDimWhenEmbedderUnavailable asserts the
// fallback: with no embedder (or a persistently failing one), the row's
// embedding_dim is cleared so the divergence is closed without requiring
// a healthy embed provider.
func TestEmbeddingBackfillPhase_ClearsDimWhenEmbedderUnavailable(t *testing.T) {
	ns := uuid.New()
	dim := 768
	row := memWithDim("no embedder available", dim, ns)

	repairer := &fakeMemoryDimRepairer{
		rowsByDim: map[int][]model.Memory{dim: {row}},
	}
	writer := &updatingMemoryWriter{}
	vs := &recordingVectorStore{}

	phase := NewEmbeddingBackfillPhase(
		repairer, writer, vs,
		nil, // embedder unavailable
		backfillSettings(true, 200),
	)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(vs.upserts) != 0 {
		t.Errorf("nil embedder must not produce vector Upserts; got %d", len(vs.upserts))
	}
	if len(writer.updates) != 1 {
		t.Fatalf("expected 1 Update (clear EmbeddingDim); got %d", len(writer.updates))
	}
	if writer.updates[0].EmbeddingDim != nil {
		t.Errorf("Update should have cleared EmbeddingDim; got %v", *writer.updates[0].EmbeddingDim)
	}
}

// TestEmbeddingBackfillPhase_ClearsDimOnFailingEmbedder mirrors the
// previous test but exercises the path where the embedder is wired but
// returns an error on every call (e.g. provider 4xx). Same outcome: the
// row's embedding_dim is cleared.
func TestEmbeddingBackfillPhase_ClearsDimOnFailingEmbedder(t *testing.T) {
	ns := uuid.New()
	dim := 768
	row := memWithDim("provider failing", dim, ns)

	repairer := &fakeMemoryDimRepairer{
		rowsByDim: map[int][]model.Memory{dim: {row}},
	}
	writer := &updatingMemoryWriter{}
	vs := &recordingVectorStore{}
	emb := &failingEmbedder{err: errors.New("provider 4xx")}

	phase := NewEmbeddingBackfillPhase(
		repairer, writer, vs,
		func() provider.EmbeddingProvider { return emb },
		backfillSettings(true, 200),
	)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if emb.calls == 0 {
		t.Errorf("expected at least one embed attempt; got 0")
	}
	if len(writer.updates) != 1 {
		t.Fatalf("expected 1 Update (clear EmbeddingDim after embedder failure); got %d", len(writer.updates))
	}
	if writer.updates[0].EmbeddingDim != nil {
		t.Errorf("Update should have cleared EmbeddingDim; got %v", *writer.updates[0].EmbeddingDim)
	}
}

// TestEmbeddingBackfillPhase_RespectsCap asserts the per-cycle cap caps
// total visited rows across all dims and reports residual when more
// candidates remain than the cap allowed.
func TestEmbeddingBackfillPhase_RespectsCap(t *testing.T) {
	ns := uuid.New()
	dim := 384
	rows := []model.Memory{
		memWithDim("a", dim, ns),
		memWithDim("b", dim, ns),
		memWithDim("c", dim, ns),
		memWithDim("d", dim, ns),
		memWithDim("e", dim, ns),
	}
	repairer := &fakeMemoryDimRepairer{
		rowsByDim: map[int][]model.Memory{dim: rows},
	}
	writer := &updatingMemoryWriter{}
	vs := &recordingVectorStore{}
	emb := &staticEmbedder{vectors: [][]float32{makeUnitVec(dim)}}

	phase := NewEmbeddingBackfillPhase(
		repairer, writer, vs,
		func() provider.EmbeddingProvider { return emb },
		backfillSettings(true, 2),
	)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	residual, err := phase.Execute(context.Background(), cycle, budget, logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !residual {
		t.Errorf("expected residual=true when more rows than cap; got false")
	}
	if len(vs.upserts) != 2 {
		t.Errorf("cap=2 must bound vector Upserts; got %d", len(vs.upserts))
	}
}

// TestEmbeddingBackfillPhase_DisabledByZeroSetting asserts the global
// kill switch.
func TestEmbeddingBackfillPhase_DisabledByZeroSetting(t *testing.T) {
	ns := uuid.New()
	dim := 384
	row := memWithDim("anything", dim, ns)

	repairer := &fakeMemoryDimRepairer{
		rowsByDim: map[int][]model.Memory{dim: {row}},
	}
	writer := &updatingMemoryWriter{}
	vs := &recordingVectorStore{}

	phase := NewEmbeddingBackfillPhase(
		repairer, writer, vs,
		nil,
		backfillSettings(false, 200),
	)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if repairer.queryCalls != 0 {
		t.Errorf("disabled phase must not query the repairer; got %d calls", repairer.queryCalls)
	}
	if len(writer.updates) != 0 || len(vs.upserts) != 0 {
		t.Errorf("disabled phase must be a no-op; got %d Updates, %d Upserts", len(writer.updates), len(vs.upserts))
	}
}

// TestEmbeddingBackfillPhase_SyncsDimWhenEmbedderReturnsDifferentDim
// asserts that when the embedder picks a different dim than the row
// recorded (model swap, dim renegotiation), the row's EmbeddingDim is
// updated to match the actual embedding written.
func TestEmbeddingBackfillPhase_SyncsDimWhenEmbedderReturnsDifferentDim(t *testing.T) {
	ns := uuid.New()
	storedDim := 768
	row := memWithDim("dim-shifted", storedDim, ns)

	repairer := &fakeMemoryDimRepairer{
		rowsByDim: map[int][]model.Memory{storedDim: {row}},
	}
	writer := &updatingMemoryWriter{}
	vs := &recordingVectorStore{}
	// Embedder returns 384-d vector even though the row claims 768.
	actualDim := 384
	emb := &staticEmbedder{vectors: [][]float32{makeUnitVec(actualDim)}}

	phase := NewEmbeddingBackfillPhase(
		repairer, writer, vs,
		func() provider.EmbeddingProvider { return emb },
		backfillSettings(true, 200),
	)
	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.Execute(context.Background(), cycle, budget, logger); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(vs.upserts) != 1 {
		t.Fatalf("expected 1 Upsert; got %d", len(vs.upserts))
	}
	if vs.upserts[0].Dimension != actualDim {
		t.Errorf("vector written at wrong dim; got %d, want %d", vs.upserts[0].Dimension, actualDim)
	}
	if len(writer.updates) != 1 {
		t.Fatalf("expected 1 Update to sync EmbeddingDim; got %d", len(writer.updates))
	}
	if writer.updates[0].EmbeddingDim == nil || *writer.updates[0].EmbeddingDim != actualDim {
		t.Errorf("EmbeddingDim sync mismatch; got %v, want %d", writer.updates[0].EmbeddingDim, actualDim)
	}
}

func makeUnitVec(dim int) []float32 {
	v := make([]float32, dim)
	v[0] = 1.0
	return v
}
