package enrichment

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// ---------------------------------------------------------------------------
// Mock implementations for conflict tests
// ---------------------------------------------------------------------------

// conflictMockMemoryReader returns pre-configured memories by ID.
type conflictMockMemoryReader struct {
	memories map[uuid.UUID]*model.Memory
	err      error
}

func (m *conflictMockMemoryReader) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	if m.err != nil {
		return nil, m.err
	}
	mem, ok := m.memories[id]
	if !ok {
		return nil, fmt.Errorf("memory not found: %s", id)
	}
	return mem, nil
}

// conflictMockLineageCreator captures lineage records created during detection.
type conflictMockLineageCreator struct {
	records []*model.MemoryLineage
	err     error
}

func (m *conflictMockLineageCreator) Create(_ context.Context, lineage *model.MemoryLineage) error {
	if m.err != nil {
		return m.err
	}
	m.records = append(m.records, lineage)
	return nil
}

// conflictMockLLM returns pre-configured completion responses.
type conflictMockLLM struct {
	responses []string // rotated through on successive calls
	callCount int
	err       error
}

func (m *conflictMockLLM) Complete(_ context.Context, _ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	idx := m.callCount % len(m.responses)
	m.callCount++
	return &provider.CompletionResponse{
		Content: m.responses[idx],
		Model:   "mock-llm",
	}, nil
}

func (m *conflictMockLLM) Name() string      { return "mock-llm" }
func (m *conflictMockLLM) Models() []string   { return []string{"mock-llm"} }

// conflictMockEmbedder returns deterministic embeddings.
type conflictMockEmbedder struct {
	embeddings [][]float32
	err        error
}

func (m *conflictMockEmbedder) Embed(_ context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	out := make([][]float32, len(req.Input))
	for i := range req.Input {
		if i < len(m.embeddings) {
			out[i] = m.embeddings[i]
		} else if len(m.embeddings) > 0 {
			out[i] = m.embeddings[0]
		} else {
			out[i] = []float32{0.1, 0.2, 0.3}
		}
	}
	return &provider.EmbeddingResponse{
		Embeddings: out,
		Model:      "mock-embed",
	}, nil
}

func (m *conflictMockEmbedder) Name() string       { return "mock-embed" }
func (m *conflictMockEmbedder) Dimensions() []int   { return []int{3} }

// ---------------------------------------------------------------------------
// Helper: build a ConflictDetector with mocks
// ---------------------------------------------------------------------------

func buildConflictDetector(
	vs VectorSearcher,
	mr MemoryReader,
	lc LineageCreator,
	llm provider.LLMProvider,
	ep provider.EmbeddingProvider,
	cfg ConflictConfig,
) *ConflictDetector {
	return NewConflictDetector(
		vs, mr, lc,
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return ep },
		cfg,
	)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestConflict_NoConflictsFound(t *testing.T) {
	nsID := uuid.New()
	candidateID := uuid.New()

	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: candidateID, Score: 0.85, NamespaceID: nsID},
		},
	}
	mr := &conflictMockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			candidateID: {ID: candidateID, NamespaceID: nsID, Content: "The sky is blue"},
		},
	}
	lc := &conflictMockLineageCreator{}
	llm := &conflictMockLLM{
		responses: []string{`{"contradicts": false, "explanation": "Both agree"}`},
	}
	ep := &conflictMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	mem := &model.Memory{ID: uuid.New(), NamespaceID: nsID, Content: "The sky appears blue"}

	cd := buildConflictDetector(vs, mr, lc, llm, ep, ConflictConfig{})
	results, err := cd.Detect(context.Background(), mem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 conflicts, got %d", len(results))
	}
	if len(lc.records) != 0 {
		t.Errorf("expected 0 lineage records, got %d", len(lc.records))
	}
}

func TestConflict_Detected(t *testing.T) {
	nsID := uuid.New()
	candidateID := uuid.New()

	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: candidateID, Score: 0.82, NamespaceID: nsID},
		},
	}
	mr := &conflictMockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			candidateID: {ID: candidateID, NamespaceID: nsID, Content: "Alice works at ACME"},
		},
	}
	lc := &conflictMockLineageCreator{}
	llm := &conflictMockLLM{
		responses: []string{`{"contradicts": true, "explanation": "Alice cannot work at both companies"}`},
	}
	ep := &conflictMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	mem := &model.Memory{ID: uuid.New(), NamespaceID: nsID, Content: "Alice works at Globex"}

	cd := buildConflictDetector(vs, mr, lc, llm, ep, ConflictConfig{})
	results, err := cd.Detect(context.Background(), mem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 conflict, got %d", len(results))
	}
	if !results[0].ConflictFound {
		t.Error("expected ConflictFound=true")
	}
	if results[0].ConflictingID != candidateID {
		t.Errorf("ConflictingID = %v, want %v", results[0].ConflictingID, candidateID)
	}
	if results[0].Explanation != "Alice cannot work at both companies" {
		t.Errorf("unexpected explanation: %s", results[0].Explanation)
	}
}

func TestConflict_MultipleConflicts(t *testing.T) {
	nsID := uuid.New()
	candA := uuid.New()
	candB := uuid.New()

	callCount := 0
	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: candA, Score: 0.80, NamespaceID: nsID},
			{ID: candB, Score: 0.75, NamespaceID: nsID},
		},
	}
	mr := &conflictMockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			candA: {ID: candA, NamespaceID: nsID, Content: "Earth is flat"},
			candB: {ID: candB, NamespaceID: nsID, Content: "Earth has no atmosphere"},
		},
	}
	lc := &conflictMockLineageCreator{}
	llm := &conflictMockLLM{
		responses: []string{
			`{"contradicts": true, "explanation": "Shape conflict"}`,
			`{"contradicts": true, "explanation": "Atmosphere conflict"}`,
		},
	}
	_ = callCount
	ep := &conflictMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	mem := &model.Memory{ID: uuid.New(), NamespaceID: nsID, Content: "Earth is a sphere with atmosphere"}

	cd := buildConflictDetector(vs, mr, lc, llm, ep, ConflictConfig{})
	results, err := cd.Detect(context.Background(), mem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 conflicts, got %d", len(results))
	}
	if len(lc.records) != 2 {
		t.Errorf("expected 2 lineage records, got %d", len(lc.records))
	}
}

func TestConflict_NoLLMProvider(t *testing.T) {
	vs := &mockVectorSearcher{}
	mr := &conflictMockMemoryReader{memories: map[uuid.UUID]*model.Memory{}}
	lc := &conflictMockLineageCreator{}
	ep := &conflictMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	mem := &model.Memory{ID: uuid.New(), NamespaceID: uuid.New(), Content: "anything"}

	cd := NewConflictDetector(
		vs, mr, lc,
		func() provider.LLMProvider { return nil },
		func() provider.EmbeddingProvider { return ep },
		ConflictConfig{},
	)
	results, err := cd.Detect(context.Background(), mem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results when no LLM provider, got %v", results)
	}
}

func TestConflict_NoEmbeddingProvider(t *testing.T) {
	vs := &mockVectorSearcher{}
	mr := &conflictMockMemoryReader{memories: map[uuid.UUID]*model.Memory{}}
	lc := &conflictMockLineageCreator{}
	llm := &conflictMockLLM{responses: []string{`{}`}}

	mem := &model.Memory{ID: uuid.New(), NamespaceID: uuid.New(), Content: "anything"}

	cd := NewConflictDetector(
		vs, mr, lc,
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return nil },
		ConflictConfig{},
	)
	results, err := cd.Detect(context.Background(), mem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results when no embedding provider, got %v", results)
	}
}

func TestConflict_NoSimilarMemories(t *testing.T) {
	vs := &mockVectorSearcher{results: nil}
	mr := &conflictMockMemoryReader{memories: map[uuid.UUID]*model.Memory{}}
	lc := &conflictMockLineageCreator{}
	llm := &conflictMockLLM{responses: []string{`{}`}}
	ep := &conflictMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	mem := &model.Memory{ID: uuid.New(), NamespaceID: uuid.New(), Content: "novel content"}

	cd := buildConflictDetector(vs, mr, lc, llm, ep, ConflictConfig{})
	results, err := cd.Detect(context.Background(), mem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 conflicts, got %d", len(results))
	}
}

func TestConflict_MalformedLLMResponse(t *testing.T) {
	nsID := uuid.New()
	candidateID := uuid.New()

	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: candidateID, Score: 0.85, NamespaceID: nsID},
		},
	}
	mr := &conflictMockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			candidateID: {ID: candidateID, NamespaceID: nsID, Content: "some fact"},
		},
	}
	lc := &conflictMockLineageCreator{}
	llm := &conflictMockLLM{
		responses: []string{"this is not valid json at all"},
	}
	ep := &conflictMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	mem := &model.Memory{ID: uuid.New(), NamespaceID: nsID, Content: "another fact"}

	cd := buildConflictDetector(vs, mr, lc, llm, ep, ConflictConfig{})
	results, err := cd.Detect(context.Background(), mem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Malformed response is skipped, not treated as error.
	if len(results) != 0 {
		t.Errorf("expected 0 conflicts for malformed response, got %d", len(results))
	}
}

func TestConflict_LineageRecordCreated(t *testing.T) {
	nsID := uuid.New()
	candidateID := uuid.New()
	memID := uuid.New()

	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: candidateID, Score: 0.90, NamespaceID: nsID},
		},
	}
	mr := &conflictMockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			candidateID: {ID: candidateID, NamespaceID: nsID, Content: "cats are dogs"},
		},
	}
	lc := &conflictMockLineageCreator{}
	llm := &conflictMockLLM{
		responses: []string{`{"contradicts": true, "explanation": "Cats are not dogs"}`},
	}
	ep := &conflictMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	mem := &model.Memory{ID: memID, NamespaceID: nsID, Content: "cats are cats"}

	cd := buildConflictDetector(vs, mr, lc, llm, ep, ConflictConfig{})
	_, err := cd.Detect(context.Background(), mem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(lc.records) != 1 {
		t.Fatalf("expected 1 lineage record, got %d", len(lc.records))
	}

	rec := lc.records[0]
	if rec.MemoryID != memID {
		t.Errorf("lineage MemoryID = %v, want %v", rec.MemoryID, memID)
	}
	if rec.ParentID == nil || *rec.ParentID != candidateID {
		t.Errorf("lineage ParentID = %v, want %v", rec.ParentID, candidateID)
	}
	if rec.Relation != "conflicts_with" {
		t.Errorf("lineage Relation = %q, want %q", rec.Relation, "conflicts_with")
	}

	// Verify context JSON contains the explanation.
	var ctxMap map[string]string
	if err := json.Unmarshal(rec.Context, &ctxMap); err != nil {
		t.Fatalf("failed to unmarshal lineage context: %v", err)
	}
	if ctxMap["explanation"] != "Cats are not dogs" {
		t.Errorf("lineage context explanation = %q, want %q", ctxMap["explanation"], "Cats are not dogs")
	}
}

func TestConflict_SkipsSelfMatch(t *testing.T) {
	nsID := uuid.New()
	memID := uuid.New()

	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: memID, Score: 1.0, NamespaceID: nsID}, // self-match
		},
	}
	mr := &conflictMockMemoryReader{memories: map[uuid.UUID]*model.Memory{}}
	lc := &conflictMockLineageCreator{}
	llm := &conflictMockLLM{responses: []string{`{"contradicts": true, "explanation": "self"}`}}
	ep := &conflictMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	mem := &model.Memory{ID: memID, NamespaceID: nsID, Content: "statement"}

	cd := buildConflictDetector(vs, mr, lc, llm, ep, ConflictConfig{})
	results, err := cd.Detect(context.Background(), mem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 conflicts (self-match skipped), got %d", len(results))
	}
}

func TestConflict_BelowThresholdSkipped(t *testing.T) {
	nsID := uuid.New()
	candidateID := uuid.New()

	vs := &mockVectorSearcher{
		results: []storage.VectorSearchResult{
			{ID: candidateID, Score: 0.50, NamespaceID: nsID}, // below 0.7 threshold
		},
	}
	mr := &conflictMockMemoryReader{
		memories: map[uuid.UUID]*model.Memory{
			candidateID: {ID: candidateID, NamespaceID: nsID, Content: "irrelevant"},
		},
	}
	lc := &conflictMockLineageCreator{}
	llm := &conflictMockLLM{responses: []string{`{"contradicts": true, "explanation": "nope"}`}}
	ep := &conflictMockEmbedder{embeddings: [][]float32{{0.1, 0.2, 0.3}}}

	mem := &model.Memory{ID: uuid.New(), NamespaceID: nsID, Content: "something"}

	cd := buildConflictDetector(vs, mr, lc, llm, ep, ConflictConfig{})
	results, err := cd.Detect(context.Background(), mem)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 conflicts (below threshold), got %d", len(results))
	}
}

// ---------------------------------------------------------------------------
// parseConflictResponse tests
// ---------------------------------------------------------------------------

func TestParseConflictResponse_CleanJSON(t *testing.T) {
	raw := `{"contradicts": true, "explanation": "They disagree"}`
	contradicts, explanation, err := parseConflictResponse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !contradicts {
		t.Error("expected contradicts=true")
	}
	if explanation != "They disagree" {
		t.Errorf("explanation = %q, want %q", explanation, "They disagree")
	}
}

func TestParseConflictResponse_FencedJSON(t *testing.T) {
	// With JSON mode enabled, LLM output should never contain markdown fences.
	// The parser no longer strips fences, so fenced input is treated as invalid.
	raw := "```json\n{\"contradicts\": false, \"explanation\": \"No conflict\"}\n```"
	_, _, err := parseConflictResponse(raw)
	if err == nil {
		t.Error("expected error for markdown-fenced input (JSON mode makes fence stripping unnecessary)")
	}
}

func TestParseConflictResponse_Malformed(t *testing.T) {
	raw := "I'm not JSON"
	_, _, err := parseConflictResponse(raw)
	if err == nil {
		t.Fatal("expected error for malformed input")
	}
}

func TestParseConflictResponse_EmbeddedJSON(t *testing.T) {
	// With JSON mode enabled, LLM output should be pure JSON.
	// The parser no longer extracts JSON from surrounding text.
	raw := "Here is my answer: {\"contradicts\": true, \"explanation\": \"Conflict found\"} some trailing text"
	_, _, err := parseConflictResponse(raw)
	if err == nil {
		t.Error("expected error for text-wrapped input (JSON mode makes extraction unnecessary)")
	}
}

func TestConflictConfig_WithDefaults(t *testing.T) {
	// Zero-value config should get defaults.
	cfg := ConflictConfig{}.withDefaults()
	if cfg.SimilarityThreshold != 0.7 {
		t.Errorf("SimilarityThreshold = %f, want 0.7", cfg.SimilarityThreshold)
	}
	if cfg.TopK != 10 {
		t.Errorf("TopK = %d, want 10", cfg.TopK)
	}

	// Explicit values should be preserved.
	cfg2 := ConflictConfig{SimilarityThreshold: 0.8, TopK: 5}.withDefaults()
	if cfg2.SimilarityThreshold != 0.8 {
		t.Errorf("SimilarityThreshold = %f, want 0.8", cfg2.SimilarityThreshold)
	}
	if cfg2.TopK != 5 {
		t.Errorf("TopK = %d, want 5", cfg2.TopK)
	}
}
