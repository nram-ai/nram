package dreaming

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// auditVectorStore embeds recordingVectorStore (for the rest of the
// storage.VectorStore surface) and serves preset vectors from GetByIDs so a
// test can exercise the novelty audit's stored-vector reuse path.
type auditVectorStore struct {
	recordingVectorStore
	byID map[uuid.UUID][]float32
}

func (s *auditVectorStore) GetByIDs(_ context.Context, _ storage.VectorKind, ids []uuid.UUID, _ int) (map[uuid.UUID][]float32, error) {
	out := make(map[uuid.UUID][]float32, len(ids))
	for _, id := range ids {
		if v, ok := s.byID[id]; ok {
			out[id] = v
		}
	}
	return out, nil
}

// recordingAuditEmbedder records every batch of inputs it is asked to embed so
// a test can assert which source contents were (or were not) re-embedded.
type recordingAuditEmbedder struct {
	dim int

	mu     sync.Mutex
	inputs [][]string
}

func (e *recordingAuditEmbedder) Embed(_ context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	e.mu.Lock()
	e.inputs = append(e.inputs, append([]string(nil), req.Input...))
	e.mu.Unlock()
	out := make([][]float32, len(req.Input))
	for i, in := range req.Input {
		out[i] = auditOneHot(e.dim, oneHotIndex(in))
	}
	return &provider.EmbeddingResponse{Embeddings: out}, nil
}

func (e *recordingAuditEmbedder) Name() string      { return "rec-audit" }
func (e *recordingAuditEmbedder) Dimensions() []int { return []int{e.dim} }

func (e *recordingAuditEmbedder) embeddedInputs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var all []string
	for _, batch := range e.inputs {
		all = append(all, batch...)
	}
	return all
}

func auditOneHot(dim, idx int) []float32 {
	v := make([]float32, dim)
	if idx >= 0 && idx < dim {
		v[idx] = 1
	}
	return v
}

// oneHotIndex maps a couple of well-known test strings to one-hot positions so
// "candidate" and "source" collide (cosine 1.0) when both are embedded.
func oneHotIndex(s string) int {
	switch s {
	case "candidate", "source":
		return 0
	default:
		return 1
	}
}

const auditTestDim = 384 // a SupportedVectorDimensions entry

// A non-augmented source whose stored vector matches the audit dimension is
// served from the vector store, so its content is never re-embedded, and the
// verdict is identical to the embed path (cosine 1.0 -> auto-reject).
func TestAuditNovelty_ReusesStoredVectorForNonAugmentedSource(t *testing.T) {
	emb := &recordingAuditEmbedder{dim: auditTestDim}
	llm := &scriptedJudgeLLM{}
	phase := newAuditPhase(emb, llm, noveltySettings(true), &updatingMemoryWriter{}, &fakeMemoryReader{})

	srcID := uuid.New()
	store := &auditVectorStore{byID: map[uuid.UUID][]float32{
		srcID: auditOneHot(auditTestDim, 0), // identical to the candidate
	}}
	phase.AttachVectorStore(store)

	src := model.Memory{ID: srcID, Content: "source", EmbeddingDim: new(auditTestDim)} // AugmentedEmbeddingAt nil

	passed, reason, _, _, err := phase.auditNovelty(context.Background(), llm, nil, "candidate", []model.Memory{src}, 0, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if passed || reason != "embed_high_sim" {
		t.Fatalf("verdict = (passed=%v, reason=%q), want auto-reject (embed_high_sim)", passed, reason)
	}
	if got := emb.embeddedInputs(); slices.Contains(got, "source") {
		t.Fatalf("source content was re-embedded despite a reusable stored vector: %v", got)
	}
	if got := emb.embeddedInputs(); !slices.Contains(got, "candidate") {
		t.Fatalf("candidate must still be embedded fresh; embedded inputs: %v", got)
	}
}

// An augmented source's stored vector is NOT a raw-content embedding, so it is
// re-embedded to keep the cosine comparison consistent with the raw candidate.
func TestAuditNovelty_AugmentedSourceStillReEmbedded(t *testing.T) {
	emb := &recordingAuditEmbedder{dim: auditTestDim}
	llm := &scriptedJudgeLLM{}
	phase := newAuditPhase(emb, llm, noveltySettings(true), &updatingMemoryWriter{}, &fakeMemoryReader{})

	srcID := uuid.New()
	store := &auditVectorStore{byID: map[uuid.UUID][]float32{
		srcID: auditOneHot(auditTestDim, 0),
	}}
	phase.AttachVectorStore(store)

	augAt := time.Now().UTC()
	src := model.Memory{ID: srcID, Content: "source", EmbeddingDim: new(auditTestDim), AugmentedEmbeddingAt: &augAt}

	if _, _, _, _, err := phase.auditNovelty(context.Background(), llm, nil, "candidate", []model.Memory{src}, 0, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := emb.embeddedInputs(); !slices.Contains(got, "source") {
		t.Fatalf("augmented source must be re-embedded; embedded inputs: %v", got)
	}
}

// A stored vector at a different dimension than the audit's cannot be reused,
// so the source falls back to embedding.
func TestAuditNovelty_DimMismatchFallsBackToEmbedding(t *testing.T) {
	emb := &recordingAuditEmbedder{dim: auditTestDim}
	llm := &scriptedJudgeLLM{}
	phase := newAuditPhase(emb, llm, noveltySettings(true), &updatingMemoryWriter{}, &fakeMemoryReader{})

	srcID := uuid.New()
	store := &auditVectorStore{byID: map[uuid.UUID][]float32{
		srcID: auditOneHot(auditTestDim, 0),
	}}
	phase.AttachVectorStore(store)

	src := model.Memory{ID: srcID, Content: "source", EmbeddingDim: new(768)} // != auditTestDim

	if _, _, _, _, err := phase.auditNovelty(context.Background(), llm, nil, "candidate", []model.Memory{src}, 0, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := emb.embeddedInputs(); !slices.Contains(got, "source") {
		t.Fatalf("dimension-mismatched source must be re-embedded; embedded inputs: %v", got)
	}
}
