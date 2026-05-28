package enrichment

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// ---------------------------------------------------------------------------
// Mocks specific to the ingestion-decision phase
// ---------------------------------------------------------------------------

type mockSettingsRepo struct {
	mu        sync.Mutex
	overrides map[string]string
}

func newMockSettingsRepo(overrides map[string]string) *mockSettingsRepo {
	if overrides == nil {
		overrides = map[string]string{}
	}
	return &mockSettingsRepo{overrides: overrides}
}

func (m *mockSettingsRepo) Get(_ context.Context, key, scope string) (*model.Setting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.overrides[key]
	if !ok {
		return nil, sql.ErrNoRows
	}
	b, _ := json.Marshal(v)
	return &model.Setting{Key: key, Value: b, Scope: scope}, nil
}

func (m *mockSettingsRepo) Set(_ context.Context, _ *model.Setting) error      { return nil }
func (m *mockSettingsRepo) Delete(_ context.Context, _ string, _ string) error { return nil }
func (m *mockSettingsRepo) ListByScope(_ context.Context, _ string) ([]model.Setting, error) {
	return nil, nil
}

type mockSoftDeleter struct {
	mu      sync.Mutex
	deleted []uuid.UUID
	err     error
}

func (m *mockSoftDeleter) SoftDelete(_ context.Context, id uuid.UUID, _ uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.deleted = append(m.deleted, id)
	return nil
}

// ---------------------------------------------------------------------------
// Harness with the ingestion-decision phase wired in
// ---------------------------------------------------------------------------

type ingestionHarness struct {
	pool     *WorkerPool
	reader   *mockMemoryReader
	updater  *mockMemoryUpdater
	creator  *mockMemoryCreator
	queue    *mockQueueClaimer
	entities *mockEntityUpserter
	rels     *mockRelationshipCreator
	lineage  *mockLineageCreator
	tokens   *mockTokenRecorder
	vectors  *mockVectorWriter
	deleter  *mockSoftDeleter
	dedupVS  *mockVectorSearcher
	settings *service.SettingsService
}

func newIngestionHarness(
	settingsOverrides map[string]string,
	dedupResults []storage.VectorSearchResult,
	factLLM provider.LLMProvider,
	entityLLM provider.LLMProvider,
	ingestionLLM provider.LLMProvider,
	embedProv provider.EmbeddingProvider,
) *ingestionHarness {
	h := &ingestionHarness{
		reader:   newMockMemoryReader(),
		updater:  &mockMemoryUpdater{},
		creator:  &mockMemoryCreator{},
		queue:    newMockQueueClaimer(),
		entities: &mockEntityUpserter{},
		rels:     &mockRelationshipCreator{},
		lineage:  &mockLineageCreator{},
		tokens:   &mockTokenRecorder{},
		vectors:  &mockVectorWriter{},
		deleter:  &mockSoftDeleter{},
		dedupVS:  &mockVectorSearcher{results: dedupResults},
	}

	settingsRepo := newMockSettingsRepo(settingsOverrides)
	h.settings = service.NewSettingsService(settingsRepo)

	dedup := NewDeduplicator(
		h.dedupVS,
		func() provider.EmbeddingProvider { return embedProv },
		h.reader,
		DefaultDeduplicationConfig,
	)

	// Wrap test provider stubs so the middleware writes token_usage rows
	// to h.tokens — matches production registry wiring.
	factFn := provider.WrapLLMForTest(constLLM(factLLM), h.tokens)
	entityFn := provider.WrapLLMForTest(constLLM(entityLLM), h.tokens)
	ingestionFn := provider.WrapLLMForTest(constLLM(ingestionLLM), h.tokens)
	embedFn := provider.WrapEmbeddingForTest(constEmbed(embedProv), h.tokens)

	h.pool = NewWorkerPool(
		WorkerConfig{Workers: 1, PollInterval: 10 * time.Millisecond},
		h.reader, h.updater, h.creator, h.deleter, h.queue,
		h.entities, h.rels, h.lineage, h.vectors,
		factFn, entityFn, embedFn, ingestionFn,
		dedup,
		h.settings,
		nil,
		nil,
	)
	return h
}

// constStringLLM returns the same JSON body for every call.
func constStringLLM(name, body string) *mockLLMProvider {
	return &mockLLMProvider{
		name: name,
		respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
			return &provider.CompletionResponse{
				Content: body,
				Model:   name + "-model",
				Usage:   provider.TokenUsage{PromptTokens: 30, CompletionTokens: 15, TotalTokens: 45},
			}, nil
		},
	}
}

// constEmbedder returns a fixed 3-dim embedding for every input.
func constEmbedder() *mockEmbeddingProvider {
	return &mockEmbeddingProvider{
		name: "embed",
		respond: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
			out := make([][]float32, len(req.Input))
			for i := range req.Input {
				out[i] = []float32{0.1, 0.2, 0.3}
			}
			return &provider.EmbeddingResponse{
				Embeddings: out,
				Model:      "embed-model",
				Usage:      provider.TokenUsage{TotalTokens: 5},
			}, nil
		},
	}
}

// minimalFactLLM returns one trivial fact so the parser accepts the body
// without producing noise relevant to the ingestion-decision tests.
func minimalFactLLM() *mockLLMProvider {
	return constStringLLM("fact", `[{"content":"f","confidence":0.5,"tags":[]}]`)
}

// minimalEntityLLM returns an empty entity result. The parser accepts an
// object with empty arrays; no entities or relationships are produced.
func minimalEntityLLM() *mockLLMProvider {
	return constStringLLM("entity", `{"entities":[],"relationships":[]}`)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestIngestion_Disabled_PhaseSkipped(t *testing.T) {
	// No overrides → enabled defaults to "false".
	h := newIngestionHarness(nil, nil,
		minimalFactLLM(),
		minimalEntityLLM(),
		constStringLLM("ingest", `{"operation":"ADD","target_id":null,"rationale":""}`),
		constEmbedder(),
	)

	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	// No metadata stamp: MarkEnriched is called with metadata=nil so the
	// metadata column is left untouched (only enriched + updated_at flip).
	if len(h.updater.enrichedMarks) != 1 {
		t.Fatalf("expected 1 parent MarkEnriched, got %d", len(h.updater.enrichedMarks))
	}
	if md := h.updater.enrichedMarks[0].metadata; md != nil {
		t.Errorf("expected nil metadata when phase disabled, got %q", string(md))
	}
}

// TestIngestion_SkipsWhenSourceIsDream pins the ingestion-decision half of
// the dream-recursion guard. A memory with Source=DreamSource MUST early-
// return from runIngestionDecision even when Enriched=false and the phase
// is enabled, because the source check (not the Enriched flag) is the
// readable expression of the recursion-prevention contract. Removing the
// Source==DreamSource clause from phase_ingestion.go runIngestionDecision makes this test
// fail — that is the entire point. Sibling test: worker_test.go
// TestProcessJob_SkipsLLMWhenSourceIsDream.
func TestIngestion_SkipsWhenSourceIsDream(t *testing.T) {
	ingestCalls := 0
	ingestLLM := &mockLLMProvider{name: "test-ingest", respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
		ingestCalls++
		return &provider.CompletionResponse{Content: `{"operation":"ADD","target_id":null,"rationale":""}`, Model: "ingest-model"}, nil
	}}
	h := newIngestionHarness(
		map[string]string{
			service.SettingIngestionDecisionEnabled:  "true",
			service.SettingIngestionDecisionShadow:   "false",
			service.SettingExtractedFactGuardEnabled: "false",
		},
		nil,
		minimalFactLLM(),
		minimalEntityLLM(),
		ingestLLM,
		constEmbedder(),
	)

	mem := testMemory()
	// Deliberately Enriched=false so the source check is the only signal
	// gating the ingestion-decision phase. If the early-return dropped the
	// source clause, ingestCalls would be > 0.
	mem.Enriched = false
	src := model.DreamSource
	mem.Source = &src
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	if ingestCalls != 0 {
		t.Errorf("dream-source memory must not invoke the ingestion-decision LLM; got %d calls", ingestCalls)
	}
	// MarkEnriched still runs at the end of the job, but with no
	// ingestion_decision stamp in metadata (the phase early-returned).
	if len(h.updater.enrichedMarks) != 1 {
		t.Fatalf("expected 1 parent MarkEnriched, got %d", len(h.updater.enrichedMarks))
	}
	if md := h.updater.enrichedMarks[0].metadata; md != nil {
		// A dream-source skip should write nil metadata (same as the
		// disabled-phase case), not stamp an ingestion_decision key.
		var meta map[string]any
		if err := json.Unmarshal(md, &meta); err == nil {
			if _, present := meta["ingestion_decision"]; present {
				t.Errorf("dream-source skip must not stamp ingestion_decision metadata; got %q", string(md))
			}
		}
	}
}

func TestIngestion_NoMatches_AddDecisionStampsMetadata(t *testing.T) {
	h := newIngestionHarness(
		map[string]string{
			service.SettingIngestionDecisionEnabled: "true",
			service.SettingIngestionDecisionShadow:  "false",
			// constEmbedder returns the same fixed vector for every input,
			// which would make every extracted fact cosine=1.0 to the
			// parent and trigger the paraphrase guard. This test exercises
			// the ingestion-decision metadata stamp, not the guard.
			service.SettingExtractedFactGuardEnabled: "false",
		},
		nil, // no near matches
		minimalFactLLM(),
		minimalEntityLLM(),
		constStringLLM("ingest", `{"operation":"ADD","target_id":null,"rationale":""}`),
		constEmbedder(),
	)

	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	if len(h.updater.enrichedMarks) != 1 {
		t.Fatalf("expected 1 parent MarkEnriched, got %d", len(h.updater.enrichedMarks))
	}
	md := decodeMetadata(t, h.updater.enrichedMarks[0].metadata)
	if got, _ := md["ingestion_decision"].(string); got != IngestionOpAdd {
		t.Errorf("ingestion_decision = %q, want ADD", got)
	}
	// 2 vector upserts: parent (reused from dedup phase) + 1 child fact.
	if len(h.vectors.vectors) != 2 {
		t.Errorf("expected 2 vector upserts (parent + child), got %d", len(h.vectors.vectors))
	}
	// Parent vector must be present in the upsert set.
	parentSeen := false
	for _, v := range h.vectors.vectors {
		if v.ID == mem.ID {
			parentSeen = true
			break
		}
	}
	if !parentSeen {
		t.Error("parent vector missing from upsert batch")
	}
}

func TestIngestion_Update_WritesLineageAndSupersedesTarget(t *testing.T) {
	target := testMemory()
	target.Content = "existing fact"
	d := 384
	target.EmbeddingDim = &d

	dedupResults := []storage.VectorSearchResult{
		{ID: target.ID, Score: 0.96, NamespaceID: target.NamespaceID},
	}
	updateBody := fmt.Sprintf(`{"operation":"UPDATE","target_id":"%s","rationale":"newer phrasing of same fact"}`, target.ID)
	h := newIngestionHarness(
		map[string]string{
			service.SettingIngestionDecisionEnabled: "true",
			service.SettingIngestionDecisionShadow:  "false",
		},
		dedupResults,
		minimalFactLLM(),
		minimalEntityLLM(),
		constStringLLM("ingest", updateBody),
		constEmbedder(),
	)

	// Both new memory and target memory must be loadable by the worker.
	newMem := testMemory()
	newMem.NamespaceID = target.NamespaceID
	h.reader.byID[newMem.ID] = newMem
	h.reader.byID[target.ID] = target
	job := testJob(newMem.ID, newMem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	// Supersedes lineage edge: child=newMem, parent=target.
	supersedes := lineageWithRelation(h.lineage.created, model.LineageSupersedes)
	if len(supersedes) != 1 {
		t.Fatalf("expected 1 supersedes lineage edge, got %d", len(supersedes))
	}
	lin := supersedes[0]
	if lin.MemoryID != newMem.ID {
		t.Errorf("lineage.MemoryID = %v, want %v", lin.MemoryID, newMem.ID)
	}
	if lin.ParentID == nil || *lin.ParentID != target.ID {
		t.Errorf("lineage.ParentID = %v, want %v", lin.ParentID, target.ID)
	}

	// Target memory marked superseded via the partial MarkSupersededBy
	// helper (race-guarded with "AND superseded_by IS NULL"). Parent
	// memory marked enriched via MarkEnriched with metadata.
	var targetMark *supersedeMark
	for i := range h.updater.supersedeMarks {
		if h.updater.supersedeMarks[i].oldID == target.ID {
			targetMark = &h.updater.supersedeMarks[i]
		}
	}
	if targetMark == nil {
		t.Fatal("expected MarkSupersededBy on the target memory")
	}
	if targetMark.newID != newMem.ID {
		t.Errorf("target MarkSupersededBy newID = %s, want %s", targetMark.newID, newMem.ID)
	}
	purged := slices.Contains(h.vectors.deleted, target.ID)
	if !purged {
		t.Errorf("expected vector purge on superseded target %s; deleted=%v", target.ID, h.vectors.deleted)
	}

	var parentMark *enrichedMark
	for i := range h.updater.enrichedMarks {
		if h.updater.enrichedMarks[i].id == newMem.ID {
			parentMark = &h.updater.enrichedMarks[i]
		}
	}
	if parentMark == nil {
		t.Fatal("expected a MarkEnriched on the parent memory")
	}
	md := decodeMetadata(t, parentMark.metadata)
	if got, _ := md["ingestion_decision"].(string); got != IngestionOpUpdate {
		t.Errorf("ingestion_decision = %q, want UPDATE", got)
	}
	if got, _ := md["ingestion_target_id"].(string); got != target.ID.String() {
		t.Errorf("ingestion_target_id = %q, want %s", got, target.ID)
	}
}

func TestIngestion_Delete_SoftDeletesNewMemoryShortCircuits(t *testing.T) {
	target := testMemory()
	target.Content = "existing fact already covers this"

	dedupResults := []storage.VectorSearchResult{
		{ID: target.ID, Score: 0.99, NamespaceID: target.NamespaceID},
	}
	deleteBody := fmt.Sprintf(`{"operation":"DELETE","target_id":"%s","rationale":"duplicate"}`, target.ID)

	// Fact LLM that would error if called — proves short-circuit skipped it.
	failingFact := &mockLLMProvider{
		name: "fact",
		respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
			t.Error("fact LLM should not be called on DELETE short-circuit")
			return &provider.CompletionResponse{Content: "[]"}, nil
		},
	}

	h := newIngestionHarness(
		map[string]string{
			service.SettingIngestionDecisionEnabled: "true",
			service.SettingIngestionDecisionShadow:  "false",
		},
		dedupResults,
		failingFact,
		minimalEntityLLM(),
		constStringLLM("ingest", deleteBody),
		constEmbedder(),
	)

	newMem := testMemory()
	newMem.NamespaceID = target.NamespaceID
	h.reader.byID[newMem.ID] = newMem
	h.reader.byID[target.ID] = target
	job := testJob(newMem.ID, newMem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	// New memory was soft-deleted.
	if len(h.deleter.deleted) != 1 || h.deleter.deleted[0] != newMem.ID {
		t.Errorf("expected new memory soft-deleted, got %v", h.deleter.deleted)
	}
	// Target was NOT touched (no superseded_by/at update).
	for _, u := range h.updater.updated {
		if u.ID == target.ID {
			t.Errorf("target memory should not have been updated for DELETE, got %+v", u)
		}
	}
	// No lineage edge.
	if len(h.lineage.created) != 0 {
		t.Errorf("expected no lineage on DELETE, got %d", len(h.lineage.created))
	}
	// No children created (short-circuit).
	if len(h.creator.created) != 0 {
		t.Errorf("expected no child memories on DELETE, got %d", len(h.creator.created))
	}
	// No vector upsert for the new memory.
	if len(h.vectors.vectors) != 0 {
		t.Errorf("expected no vector upserts on DELETE, got %d", len(h.vectors.vectors))
	}
	// Queue marked complete.
	if len(h.queue.completed) != 1 {
		t.Errorf("expected job completed, got %v", h.queue.completed)
	}
}

func TestIngestion_MalformedJSON_FallsBackToAdd(t *testing.T) {
	target := testMemory()
	dedupResults := []storage.VectorSearchResult{
		{ID: target.ID, Score: 0.95, NamespaceID: target.NamespaceID},
	}

	// LLM always returns garbage. Both attempts fail to parse → ADD-FALLBACK.
	garbageLLM := &mockLLMProvider{
		name: "ingest",
		respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
			return &provider.CompletionResponse{
				Content: "this is not json at all",
				Model:   "ingest-model",
				Usage:   provider.TokenUsage{TotalTokens: 10},
			}, nil
		},
	}

	h := newIngestionHarness(
		map[string]string{
			service.SettingIngestionDecisionEnabled: "true",
			service.SettingIngestionDecisionShadow:  "false",
		},
		dedupResults,
		minimalFactLLM(),
		minimalEntityLLM(),
		garbageLLM,
		constEmbedder(),
	)

	newMem := testMemory()
	newMem.NamespaceID = target.NamespaceID
	h.reader.byID[newMem.ID] = newMem
	h.reader.byID[target.ID] = target
	job := testJob(newMem.ID, newMem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	if len(h.deleter.deleted) != 0 {
		t.Errorf("ADD-FALLBACK must not soft-delete, got %v", h.deleter.deleted)
	}
	if got := lineageWithRelation(h.lineage.created, model.LineageSupersedes); len(got) != 0 {
		t.Errorf("ADD-FALLBACK must not write supersedes lineage, got %d", len(got))
	}
	parentMark := findEnrichedMark(h.updater.enrichedMarks, newMem.ID)
	if parentMark == nil {
		t.Fatal("expected a MarkEnriched on the new memory")
	}
	md := decodeMetadata(t, parentMark.metadata)
	if got, _ := md["ingestion_decision"].(string); got != IngestionOpAddFallback {
		t.Errorf("ingestion_decision = %q, want ADD-FALLBACK", got)
	}
}

func TestIngestion_ShadowMode_LogsButDoesNotApply(t *testing.T) {
	target := testMemory()
	dedupResults := []storage.VectorSearchResult{
		{ID: target.ID, Score: 0.96, NamespaceID: target.NamespaceID},
	}
	updateBody := fmt.Sprintf(`{"operation":"UPDATE","target_id":"%s","rationale":"r"}`, target.ID)

	h := newIngestionHarness(
		map[string]string{
			service.SettingIngestionDecisionEnabled: "true",
			service.SettingIngestionDecisionShadow:  "true",
		},
		dedupResults,
		minimalFactLLM(),
		minimalEntityLLM(),
		constStringLLM("ingest", updateBody),
		constEmbedder(),
	)

	newMem := testMemory()
	newMem.NamespaceID = target.NamespaceID
	h.reader.byID[newMem.ID] = newMem
	h.reader.byID[target.ID] = target
	job := testJob(newMem.ID, newMem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	// Shadow mode must NOT write a supersedes edge or touch the target.
	if got := lineageWithRelation(h.lineage.created, model.LineageSupersedes); len(got) != 0 {
		t.Errorf("shadow mode must not write supersedes lineage, got %d", len(got))
	}
	for _, u := range h.updater.updated {
		if u.ID == target.ID {
			t.Errorf("shadow mode must not touch target memory, got %+v", u)
		}
	}
	parentMark := findEnrichedMark(h.updater.enrichedMarks, newMem.ID)
	if parentMark == nil {
		t.Fatal("expected a MarkEnriched on the new memory")
	}
	md := decodeMetadata(t, parentMark.metadata)
	if got, _ := md["ingestion_decision"].(string); got != IngestionOpAdd {
		t.Errorf("shadow effective op = %q, want ADD", got)
	}
	if got, _ := md["ingestion_shadow_op"].(string); got != IngestionOpUpdate {
		t.Errorf("ingestion_shadow_op = %q, want UPDATE", got)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func decodeMetadata(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	if len(raw) == 0 {
		return map[string]any{}
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode metadata %q: %v", string(raw), err)
	}
	return out
}

func findEnrichedMark(marks []enrichedMark, id uuid.UUID) *enrichedMark {
	for i := range marks {
		if marks[i].id == id {
			return &marks[i]
		}
	}
	return nil
}

func lineageWithRelation(all []*model.MemoryLineage, relation string) []*model.MemoryLineage {
	out := make([]*model.MemoryLineage, 0, len(all))
	for _, l := range all {
		if l.Relation == relation {
			out = append(out, l)
		}
	}
	return out
}
