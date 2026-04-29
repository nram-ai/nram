package dreaming

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// --- doubles ---

// staticEmbedder returns a fixed slice of vectors regardless of input. The
// caller controls the values so tests can drive every branch of the
// hybrid novelty audit (auto-reject, auto-accept, borderline).
type staticEmbedder struct {
	calls   atomic.Int32
	vectors [][]float32
	err     error
}

func (s *staticEmbedder) Embed(_ context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	s.calls.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	if len(s.vectors) != len(req.Input) {
		// Pad or truncate so the audit's length-equality check still passes
		// when callers only specify a single template vector.
		out := make([][]float32, len(req.Input))
		for i := range out {
			out[i] = s.vectors[i%len(s.vectors)]
		}
		return &provider.EmbeddingResponse{Embeddings: out}, nil
	}
	return &provider.EmbeddingResponse{Embeddings: s.vectors}, nil
}
func (s *staticEmbedder) Name() string { return "static" }
func (s *staticEmbedder) Dimensions() []int {
	if len(s.vectors) == 0 || len(s.vectors[0]) == 0 {
		return []int{0}
	}
	return []int{len(s.vectors[0])}
}

// scriptedJudgeLLM returns a fixed completion content. Used to feed the
// audit's LLM judge with chosen JSON or malformed text to exercise the
// pass/fail/parse-error branches.
type scriptedJudgeLLM struct {
	calls   atomic.Int32
	content string
	usage   provider.TokenUsage
	err     error
}

func (s *scriptedJudgeLLM) Complete(_ context.Context, _ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	s.calls.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	return &provider.CompletionResponse{
		Content: s.content,
		Model:   "test-model",
		Usage:   s.usage,
	}, nil
}
func (s *scriptedJudgeLLM) Name() string     { return "test-llm" }
func (s *scriptedJudgeLLM) Models() []string { return []string{"test-model"} }

// updatingMemoryWriter records each Update call so backfill tests can
// inspect what the consolidation phase wrote back. metadataUpdates is
// recorded separately so tests can distinguish stamp-only writes (which
// must not bump updated_at) from full Updates.
type updatingMemoryWriter struct {
	creates         []*model.Memory
	updates         []model.Memory
	metadataUpdates []metadataUpdateRecord
}

type metadataUpdateRecord struct {
	ID          uuid.UUID
	NamespaceID uuid.UUID
	Metadata    json.RawMessage
}

func (w *updatingMemoryWriter) Create(_ context.Context, mem *model.Memory) error {
	cp := *mem
	w.creates = append(w.creates, &cp)
	return nil
}
func (w *updatingMemoryWriter) Update(_ context.Context, mem *model.Memory) error {
	w.updates = append(w.updates, *mem)
	return nil
}
func (w *updatingMemoryWriter) UpdateMetadata(_ context.Context, id, namespaceID uuid.UUID, metadata json.RawMessage) error {
	cp := append(json.RawMessage(nil), metadata...)
	w.metadataUpdates = append(w.metadataUpdates, metadataUpdateRecord{
		ID:          id,
		NamespaceID: namespaceID,
		Metadata:    cp,
	})
	return nil
}
func (w *updatingMemoryWriter) SoftDelete(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}
func (w *updatingMemoryWriter) HardDelete(_ context.Context, _ uuid.UUID, _ uuid.UUID) error {
	return nil
}
func (w *updatingMemoryWriter) DecayConfidence(_ context.Context, ids []uuid.UUID, _, _ float64) (int64, error) {
	return int64(len(ids)), nil
}

// --- helpers ---

// noveltySettings returns a fully-configured staticDreamSettings for audit
// tests. Callers can mutate the returned struct's ints map to set per-test
// values like SettingDreamNoveltyBackfillPerCycle and judge max tokens.
func noveltySettings(enabled bool) *staticDreamSettings {
	values := map[string]string{
		service.SettingDreamNoveltyJudgePrompt: `{"novel_facts": []} synth=%s sources=%s`,
	}
	if enabled {
		values[service.SettingDreamNoveltyEnabled] = "true"
	}
	return &staticDreamSettings{
		values: values,
		floats: map[string]float64{
			service.SettingDreamNoveltyEmbedHighThreshold: 0.97,
			service.SettingDreamNoveltyEmbedLowThreshold:  0.85,
		},
		ints: map[string]int{},
	}
}

func newAuditPhase(emb provider.EmbeddingProvider, llm provider.LLMProvider, settings SettingsResolver, writer MemoryWriter, reader MemoryReader) *ConsolidationPhase {
	return NewConsolidationPhase(
		reader,
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return emb },
		settings,
	)
}

func dreamMemory(content string, sourceIDs []uuid.UUID) model.Memory {
	src := model.DreamSource
	meta := map[string]interface{}{}
	if len(sourceIDs) > 0 {
		ids := make([]string, len(sourceIDs))
		for i, id := range sourceIDs {
			ids[i] = id.String()
		}
		meta["source_memory_ids"] = ids
	}
	raw, _ := json.Marshal(meta)
	return model.Memory{
		ID:          uuid.New(),
		NamespaceID: uuid.New(),
		Content:     content,
		Source:      &src,
		Confidence:  0.3,
		Metadata:    raw,
	}
}

// --- auditNovelty unit tests ---

func TestAuditNovelty_EmbedHighSim_AutoReject(t *testing.T) {
	emb := &staticEmbedder{vectors: [][]float32{{1, 0, 0}, {1, 0, 0}}}
	llm := &scriptedJudgeLLM{}
	phase := newAuditPhase(emb, llm, noveltySettings(true), &updatingMemoryWriter{}, &fakeMemoryReader{})

	src := model.Memory{ID: uuid.New(), Content: "source"}
	passed, reason, usage, _, err := phase.auditNovelty(context.Background(), llm, nil, "candidate", []model.Memory{src}, 0, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if passed {
		t.Fatalf("expected reject, got pass")
	}
	if reason != "embed_high_sim" {
		t.Fatalf("expected embed_high_sim reason, got %q", reason)
	}
	if usage != nil {
		t.Fatalf("expected nil usage on pre-filter rejection, got %+v", usage)
	}
	if llm.calls.Load() != 0 {
		t.Fatalf("expected no LLM calls on auto-reject, got %d", llm.calls.Load())
	}
}

// TestAuditNovelty_BackfillThresholdOverride_RejectsEarlier verifies that
// passing a more aggressive override pushes a previously-borderline
// similarity into the auto-reject band without reaching the LLM judge.
// This is the load-bearing path for backfill-specific tightening.
func TestAuditNovelty_BackfillThresholdOverride_RejectsEarlier(t *testing.T) {
	// cosine({1,0,0}, {0.95, 0.31, 0}) ≈ 0.95. With the default 0.97 high
	// threshold this would fall through to the LLM judge. With a backfill
	// override of 0.93, it auto-rejects without calling the judge.
	emb := &staticEmbedder{vectors: [][]float32{{1, 0, 0}, {0.95, 0.31, 0}}}
	llm := &scriptedJudgeLLM{}
	phase := newAuditPhase(emb, llm, noveltySettings(true), &updatingMemoryWriter{}, &fakeMemoryReader{})

	src := model.Memory{ID: uuid.New(), Content: "source"}
	passed, reason, usage, _, err := phase.auditNovelty(context.Background(), llm, nil, "candidate", []model.Memory{src}, 0.93, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if passed {
		t.Fatalf("expected reject under backfill override, got pass")
	}
	if reason != "embed_high_sim" {
		t.Fatalf("expected embed_high_sim reason under override, got %q", reason)
	}
	if usage != nil {
		t.Fatalf("expected nil usage on pre-filter rejection, got %+v", usage)
	}
	if llm.calls.Load() != 0 {
		t.Fatalf("expected no LLM calls under auto-reject, got %d", llm.calls.Load())
	}
}

// TestAuditNovelty_OverrideZeroFallsBackToSetting verifies that passing
// embedHighOverride == 0 reverts to the default 0.97 threshold, so
// existing callers are unaffected.
func TestAuditNovelty_OverrideZeroFallsBackToSetting(t *testing.T) {
	// Same similarity as above (~0.95). With override=0 the default 0.97
	// applies, so this falls through to the LLM judge rather than
	// auto-rejecting.
	emb := &staticEmbedder{vectors: [][]float32{{1, 0, 0}, {0.95, 0.31, 0}}}
	llm := &scriptedJudgeLLM{
		content: `{"novel_facts": ["x"]}`,
	}
	phase := newAuditPhase(emb, llm, noveltySettings(true), &updatingMemoryWriter{}, &fakeMemoryReader{})

	src := model.Memory{ID: uuid.New(), Content: "source"}
	passed, reason, _, _, err := phase.auditNovelty(context.Background(), llm, nil, "candidate", []model.Memory{src}, 0, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !passed {
		t.Fatalf("expected LLM judge pass (novel_facts present), got reject")
	}
	if reason != "llm_judge" {
		t.Fatalf("expected llm_judge reason under default threshold, got %q", reason)
	}
	if llm.calls.Load() == 0 {
		t.Fatalf("expected LLM to be consulted when override is 0 and sim is borderline")
	}
}

func TestAuditNovelty_EmbedLowSim_AutoAccept(t *testing.T) {
	emb := &staticEmbedder{vectors: [][]float32{{1, 0, 0}, {0, 1, 0}}}
	llm := &scriptedJudgeLLM{}
	phase := newAuditPhase(emb, llm, noveltySettings(true), &updatingMemoryWriter{}, &fakeMemoryReader{})

	src := model.Memory{ID: uuid.New(), Content: "source"}
	passed, reason, usage, _, err := phase.auditNovelty(context.Background(), llm, nil, "candidate", []model.Memory{src}, 0, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !passed {
		t.Fatalf("expected accept on low similarity, got reject")
	}
	if reason != "embed_low_sim" {
		t.Fatalf("expected embed_low_sim reason, got %q", reason)
	}
	if usage != nil {
		t.Fatalf("expected nil usage on pre-filter accept, got %+v", usage)
	}
	if llm.calls.Load() != 0 {
		t.Fatalf("expected no LLM calls on auto-accept, got %d", llm.calls.Load())
	}
}

func TestAuditNovelty_BorderlineJudgePass(t *testing.T) {
	// cosine of {1,0} vs {0.95,0.31} ≈ 0.95 → between 0.85 and 0.97.
	emb := &staticEmbedder{vectors: [][]float32{{1, 0}, {0.95, 0.31}}}
	llm := &scriptedJudgeLLM{
		content: `{"novel_facts": ["a new fact the source doesn't state"]}`,
		usage:   provider.TokenUsage{PromptTokens: 50, CompletionTokens: 10, TotalTokens: 60},
	}
	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyJudgeMaxTokens] = 256
	phase := newAuditPhase(emb, llm, settings, &updatingMemoryWriter{}, &fakeMemoryReader{})

	src := model.Memory{ID: uuid.New(), Content: "source"}
	passed, reason, usage, _, err := phase.auditNovelty(context.Background(), llm, nil, "candidate", []model.Memory{src}, 0, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !passed {
		t.Fatalf("expected pass when judge reports novel facts, got reject")
	}
	if reason != "llm_judge" {
		t.Fatalf("expected llm_judge reason, got %q", reason)
	}
	if usage == nil || usage.TotalTokens == 0 {
		t.Fatalf("expected non-zero usage on judge call, got %+v", usage)
	}
	if llm.calls.Load() != 1 {
		t.Fatalf("expected exactly one LLM call on borderline, got %d", llm.calls.Load())
	}
}

func TestAuditNovelty_BorderlineJudgeFail(t *testing.T) {
	emb := &staticEmbedder{vectors: [][]float32{{1, 0}, {0.95, 0.31}}}
	llm := &scriptedJudgeLLM{
		content: `{"novel_facts": []}`,
		usage:   provider.TokenUsage{PromptTokens: 50, CompletionTokens: 5, TotalTokens: 55},
	}
	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyJudgeMaxTokens] = 256
	phase := newAuditPhase(emb, llm, settings, &updatingMemoryWriter{}, &fakeMemoryReader{})

	src := model.Memory{ID: uuid.New(), Content: "source"}
	passed, reason, usage, _, _ := phase.auditNovelty(context.Background(), llm, nil, "candidate", []model.Memory{src}, 0, "")
	if passed {
		t.Fatalf("expected reject when judge returns empty novel_facts, got pass")
	}
	if reason != "llm_judge" {
		t.Fatalf("expected llm_judge reason, got %q", reason)
	}
	if usage == nil {
		t.Fatalf("expected non-nil usage on judge call")
	}
}

func TestAuditNovelty_EmbedError_FailClosed(t *testing.T) {
	emb := &staticEmbedder{vectors: [][]float32{{0}}, err: errors.New("embedder down")}
	llm := &scriptedJudgeLLM{}
	phase := newAuditPhase(emb, llm, noveltySettings(true), &updatingMemoryWriter{}, &fakeMemoryReader{})

	src := model.Memory{ID: uuid.New(), Content: "source"}
	passed, reason, _, _, err := phase.auditNovelty(context.Background(), llm, nil, "candidate", []model.Memory{src}, 0, "")
	if passed {
		t.Fatalf("embedding error must fail closed (reject), got pass")
	}
	if reason != "embed_error" {
		t.Fatalf("expected embed_error reason, got %q", reason)
	}
	if err == nil {
		t.Fatalf("expected non-nil error to propagate")
	}
	if llm.calls.Load() != 0 {
		t.Fatalf("LLM judge must not run when embedder fails, got %d calls", llm.calls.Load())
	}
}

func TestAuditNovelty_JudgeParseError_FailClosed(t *testing.T) {
	emb := &staticEmbedder{vectors: [][]float32{{1, 0}, {0.9, 0.4}}}
	llm := &scriptedJudgeLLM{
		content: "definitely not json",
		usage:   provider.TokenUsage{PromptTokens: 30, CompletionTokens: 5, TotalTokens: 35},
	}
	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyJudgeMaxTokens] = 256
	phase := newAuditPhase(emb, llm, settings, &updatingMemoryWriter{}, &fakeMemoryReader{})

	src := model.Memory{ID: uuid.New(), Content: "source"}
	passed, reason, usage, _, err := phase.auditNovelty(context.Background(), llm, nil, "candidate", []model.Memory{src}, 0, "")
	if passed {
		t.Fatalf("parse error must fail closed (reject), got pass")
	}
	if reason != "judge_parse_error" {
		t.Fatalf("expected judge_parse_error reason, got %q", reason)
	}
	if err != nil {
		t.Fatalf("parse errors are reported via reason, not err; got err=%v", err)
	}
	if usage == nil {
		t.Fatalf("usage must be recorded on parse-error path so the call still costs the budget")
	}
}

func TestAuditNovelty_NoSources_Rejected(t *testing.T) {
	phase := newAuditPhase(nil, &scriptedJudgeLLM{}, noveltySettings(true), &updatingMemoryWriter{}, &fakeMemoryReader{})
	passed, reason, _, _, _ := phase.auditNovelty(context.Background(), &scriptedJudgeLLM{}, nil, "candidate", nil, 0, "")
	if passed {
		t.Fatalf("audit with zero sources cannot verify novelty and must reject")
	}
	if reason != "no_sources" {
		t.Fatalf("expected no_sources reason, got %q", reason)
	}
}

// --- backfill (auditExistingDreams) tests ---

func TestAuditExistingDreams_DemotesDuplicateAndStampsNovel(t *testing.T) {
	srcA := model.Memory{ID: uuid.New(), Content: "source A content"}
	srcB := model.Memory{ID: uuid.New(), Content: "source B content"}

	dupDream := dreamMemory("near duplicate of source A", []uuid.UUID{srcA.ID})
	novelDream := dreamMemory("genuinely new content", []uuid.UUID{srcB.ID})

	reader := &fakeMemoryReader{list: []model.Memory{srcA, srcB, dupDream, novelDream}}
	writer := &updatingMemoryWriter{}

	// Embedder vectors keyed by call: dup audit gets identical vectors,
	// novel audit gets orthogonal. We control this by switching vectors
	// between calls via a per-call closure embedder.
	type embedCall struct {
		vectors [][]float32
	}
	callIdx := 0
	scripts := []embedCall{
		// dup synthesis: candidate + 1 source, both identical
		{vectors: [][]float32{{1, 0}, {1, 0}}},
		// novel synthesis: orthogonal vectors
		{vectors: [][]float32{{1, 0}, {0, 1}}},
	}
	emb := &scriptedEmbedder{
		next: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
			s := scripts[callIdx]
			callIdx++
			out := make([][]float32, len(req.Input))
			for i := range out {
				out[i] = s.vectors[i]
			}
			return &provider.EmbeddingResponse{Embeddings: out}, nil
		},
		dim: 2,
	}

	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyBackfillPerCycle] = 50
	phase := newAuditPhase(emb, &scriptedJudgeLLM{}, settings, writer, reader)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: dupDream.NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.AuditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, reader.list, settings.ints[service.SettingDreamNoveltyBackfillPerCycle]); err != nil {
		t.Fatalf("auditExistingDreams returned error: %v", err)
	}

	// Demote takes the full Update path (real state change); stamp-only
	// audits go through UpdateMetadata so updated_at stays intact and
	// the next cycle does not re-audit.
	if len(writer.updates) != 1 {
		t.Fatalf("expected 1 Update call (demote), got %d", len(writer.updates))
	}
	if len(writer.metadataUpdates) != 1 {
		t.Fatalf("expected 1 UpdateMetadata call (stamp-only audit), got %d", len(writer.metadataUpdates))
	}

	demoted := &writer.updates[0]
	if demoted.ID != dupDream.ID {
		t.Fatalf("Update target should be duplicate dream %s, got %s", dupDream.ID, demoted.ID)
	}
	stampRecord := writer.metadataUpdates[0]
	if stampRecord.ID != novelDream.ID {
		t.Fatalf("UpdateMetadata target should be novel dream %s, got %s", novelDream.ID, stampRecord.ID)
	}

	if demoted.Confidence != 0 {
		t.Errorf("duplicate dream confidence should be 0, got %v", demoted.Confidence)
	}
	if demoted.EmbeddingDim != nil {
		t.Errorf("duplicate dream EmbeddingDim should be cleared on demote, got %v", *demoted.EmbeddingDim)
	}
	if !isLowNoveltyJSON(demoted.Metadata) {
		t.Errorf("duplicate dream metadata.low_novelty must be true; got %s", string(demoted.Metadata))
	}
	if !hasAuditMarker(demoted.Metadata) {
		t.Errorf("duplicate dream missing novelty_audited_at marker")
	}

	if isLowNoveltyJSON(stampRecord.Metadata) {
		t.Errorf("novel dream must not be flagged low_novelty")
	}
	if !hasAuditMarker(stampRecord.Metadata) {
		t.Errorf("novel dream missing novelty_audited_at marker")
	}
}

func TestAuditExistingDreams_RespectsPerCycleCap(t *testing.T) {
	src := model.Memory{ID: uuid.New(), Content: "shared source"}
	memories := []model.Memory{src}
	for i := 0; i < 5; i++ {
		memories = append(memories, dreamMemory("dream "+string(rune('A'+i)), []uuid.UUID{src.ID}))
	}

	emb := &staticEmbedder{vectors: [][]float32{{0, 1}, {1, 0}}} // orthogonal ⇒ auto-accept
	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyBackfillPerCycle] = 3
	writer := &updatingMemoryWriter{}
	reader := &fakeMemoryReader{list: memories}
	phase := newAuditPhase(emb, &scriptedJudgeLLM{}, settings, writer, reader)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: memories[0].NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.AuditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, memories, settings.ints[service.SettingDreamNoveltyBackfillPerCycle]); err != nil {
		t.Fatalf("auditExistingDreams returned error: %v", err)
	}
	// Auto-accept dreams take the stamp-only UpdateMetadata path; the
	// per-cycle cap covers visited memories regardless of which write
	// path each one takes.
	totalWrites := len(writer.updates) + len(writer.metadataUpdates)
	if totalWrites != 3 {
		t.Fatalf("backfill cap=3 should produce exactly 3 writes, got %d (Update=%d, UpdateMetadata=%d)",
			totalWrites, len(writer.updates), len(writer.metadataUpdates))
	}
}

func TestAuditExistingDreams_SkipsAlreadyStamped(t *testing.T) {
	src := model.Memory{ID: uuid.New(), Content: "shared source"}
	dream := dreamMemory("already audited", []uuid.UUID{src.ID})

	// Pre-stamp the audit marker.
	meta := map[string]interface{}{
		"source_memory_ids":  []interface{}{src.ID.String()},
		"novelty_audited_at": "2026-04-01T00:00:00Z",
	}
	raw, _ := json.Marshal(meta)
	dream.Metadata = raw

	memories := []model.Memory{src, dream}
	emb := &staticEmbedder{vectors: [][]float32{{1, 0}, {1, 0}}}
	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyBackfillPerCycle] = 50
	writer := &updatingMemoryWriter{}
	reader := &fakeMemoryReader{list: memories}
	phase := newAuditPhase(emb, &scriptedJudgeLLM{}, settings, writer, reader)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: dream.NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	_, _ = phase.AuditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, memories, settings.ints[service.SettingDreamNoveltyBackfillPerCycle])

	if len(writer.updates) != 0 || len(writer.metadataUpdates) != 0 {
		t.Fatalf("already-audited dream must not be touched, got %d updates / %d metadata updates",
			len(writer.updates), len(writer.metadataUpdates))
	}
	if emb.calls.Load() != 0 {
		t.Fatalf("audit must short-circuit before embedding when marker exists, got %d embed calls", emb.calls.Load())
	}
}

func TestAuditExistingDreams_DisabledByZeroCap(t *testing.T) {
	src := model.Memory{ID: uuid.New(), Content: "src"}
	dream := dreamMemory("d", []uuid.UUID{src.ID})

	emb := &staticEmbedder{vectors: [][]float32{{1, 0}, {1, 0}}}
	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyBackfillPerCycle] = 0
	writer := &updatingMemoryWriter{}
	reader := &fakeMemoryReader{list: []model.Memory{src, dream}}
	phase := newAuditPhase(emb, &scriptedJudgeLLM{}, settings, writer, reader)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: dream.NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)
	_, _ = phase.AuditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, []model.Memory{src, dream}, settings.ints[service.SettingDreamNoveltyBackfillPerCycle])

	if len(writer.updates) != 0 {
		t.Fatalf("backfill must be a no-op when per-cycle cap is 0, got %d updates", len(writer.updates))
	}
}

func TestAuditExistingDreams_OrphanGetsDemoted(t *testing.T) {
	srcStr := model.DreamSource
	orphan := model.Memory{
		ID:          uuid.New(),
		NamespaceID: uuid.New(),
		Content:     "orphan with no lineage",
		Source:      &srcStr,
		Confidence:  0.5,
		Metadata:    json.RawMessage(`{}`),
	}

	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyBackfillPerCycle] = 50
	writer := &updatingMemoryWriter{}
	reader := &fakeMemoryReader{list: []model.Memory{orphan}}
	phase := newAuditPhase(nil, &scriptedJudgeLLM{}, settings, writer, reader)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: orphan.NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)
	_, _ = phase.AuditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, []model.Memory{orphan}, settings.ints[service.SettingDreamNoveltyBackfillPerCycle])

	if len(writer.updates) != 1 {
		t.Fatalf("orphan dream must be demoted, got %d updates", len(writer.updates))
	}
	if writer.updates[0].Confidence != 0 {
		t.Errorf("orphan dream confidence should be 0, got %v", writer.updates[0].Confidence)
	}
	if !isLowNoveltyJSON(writer.updates[0].Metadata) {
		t.Errorf("orphan dream must be marked low_novelty; got %s", string(writer.updates[0].Metadata))
	}
}

// --- helpers used only by tests ---

func isLowNoveltyJSON(raw json.RawMessage) bool {
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	v, ok := m["low_novelty"].(bool)
	return ok && v
}

func hasAuditMarker(raw json.RawMessage) bool {
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	_, ok := m["novelty_audited_at"]
	return ok
}

// recordingVectorPurger is a VectorPurger stub that records every memory
// id passed to Delete. Returning no error on all calls.
type recordingVectorPurger struct {
	deleted []uuid.UUID
}

func (p *recordingVectorPurger) Delete(_ context.Context, _ storage.VectorKind, id uuid.UUID) error {
	p.deleted = append(p.deleted, id)
	return nil
}

func containsUUID(ids []uuid.UUID, target uuid.UUID) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

// TestAuditExistingDreams_DemotePurgesVector asserts that demoting a dream
// via the novelty backfill also drops its vector. This is the load-bearing
// hook for keeping recall from traversing entries excluded by isLowNovelty.
func TestAuditExistingDreams_DemotePurgesVector(t *testing.T) {
	srcA := model.Memory{ID: uuid.New(), Content: "source A content"}
	dupDream := dreamMemory("near duplicate of source A", []uuid.UUID{srcA.ID})

	reader := &fakeMemoryReader{list: []model.Memory{srcA, dupDream}}
	writer := &updatingMemoryWriter{}

	emb := &scriptedEmbedder{
		next: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
			out := make([][]float32, len(req.Input))
			for i := range out {
				out[i] = []float32{1, 0}
			}
			return &provider.EmbeddingResponse{Embeddings: out}, nil
		},
		dim: 2,
	}

	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyBackfillPerCycle] = 10
	phase := newAuditPhase(emb, &scriptedJudgeLLM{}, settings, writer, reader)

	purger := &recordingVectorPurger{}
	phase.AttachVectorPurger(purger)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: dupDream.NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.AuditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, reader.list, settings.ints[service.SettingDreamNoveltyBackfillPerCycle]); err != nil {
		t.Fatalf("auditExistingDreams error: %v", err)
	}

	if !containsUUID(purger.deleted, dupDream.ID) {
		t.Errorf("expected demoted dream id %s in purger.deleted, got %v", dupDream.ID, purger.deleted)
	}
}

// TestAuditExistingDreams_PassDoesNotPurgeVector asserts that a dream that
// passes the novelty audit retains its vector — stamping only, no purge.
func TestAuditExistingDreams_PassDoesNotPurgeVector(t *testing.T) {
	srcA := model.Memory{ID: uuid.New(), Content: "source A content"}
	novelDream := dreamMemory("genuinely new content", []uuid.UUID{srcA.ID})

	reader := &fakeMemoryReader{list: []model.Memory{srcA, novelDream}}
	writer := &updatingMemoryWriter{}

	emb := &scriptedEmbedder{
		next: func(req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
			// Orthogonal vectors → low sim → auto-accept path.
			return &provider.EmbeddingResponse{Embeddings: [][]float32{{1, 0}, {0, 1}}}, nil
		},
		dim: 2,
	}

	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyBackfillPerCycle] = 10
	phase := newAuditPhase(emb, &scriptedJudgeLLM{}, settings, writer, reader)

	purger := &recordingVectorPurger{}
	phase.AttachVectorPurger(purger)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: novelDream.NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.AuditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, reader.list, settings.ints[service.SettingDreamNoveltyBackfillPerCycle]); err != nil {
		t.Fatalf("auditExistingDreams error: %v", err)
	}

	if containsUUID(purger.deleted, novelDream.ID) {
		t.Errorf("pass should not purge vector; got %v", purger.deleted)
	}
}

// TestSupersedeOriginals_PurgesOriginalVectors asserts that when a synthesis
// supersedes its source memories, the source vectors are purged AND the
// originals' embedding_dim is cleared so the row state matches the
// vector store. The synthesis itself retains its vector (that's the one
// recall should surface).
func TestSupersedeOriginals_PurgesOriginalVectors(t *testing.T) {
	d := 4
	srcA := model.Memory{ID: uuid.New(), Content: "source A", EmbeddingDim: &d}
	srcB := model.Memory{ID: uuid.New(), Content: "source B", EmbeddingDim: &d}

	// Build a synthesis whose metadata lists srcA and srcB as source memories.
	src := model.DreamSource
	meta, _ := json.Marshal(map[string]interface{}{
		"source_memory_ids": []string{srcA.ID.String(), srcB.ID.String()},
	})
	synthesis := &model.Memory{
		ID:         uuid.New(),
		Source:     &src,
		Confidence: 0.9,
		Metadata:   meta,
	}

	reader := &fakeMemoryReader{list: []model.Memory{srcA, srcB}}
	writer := &updatingMemoryWriter{}
	phase := newAuditPhase(nil, &scriptedJudgeLLM{}, noveltySettings(true), writer, reader)

	purger := &recordingVectorPurger{}
	phase.AttachVectorPurger(purger)

	cycle := &model.DreamCycle{ID: uuid.New()}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})

	phase.supersedeOriginals(context.Background(), cycle, synthesis, logger)

	if !containsUUID(purger.deleted, srcA.ID) {
		t.Errorf("supersede should purge srcA vector; got %v", purger.deleted)
	}
	if !containsUUID(purger.deleted, srcB.ID) {
		t.Errorf("supersede should purge srcB vector; got %v", purger.deleted)
	}
	if containsUUID(purger.deleted, synthesis.ID) {
		t.Errorf("synthesis vector should NOT be purged; got %v", purger.deleted)
	}

	// Both originals should have EmbeddingDim cleared in the persisted
	// Update so the row state matches the vector store.
	for _, id := range []uuid.UUID{srcA.ID, srcB.ID} {
		updated := findMemoryUpdate(writer.updates, id)
		if updated == nil {
			t.Errorf("expected Update on superseded original %s", id)
			continue
		}
		if updated.EmbeddingDim != nil {
			t.Errorf("original %s should have EmbeddingDim cleared on supersede; got %v", id, *updated.EmbeddingDim)
		}
	}
}

// findMemoryUpdate returns the most recent Update record for the given
// memory ID, or nil if none.
func findMemoryUpdate(updates []model.Memory, id uuid.UUID) *model.Memory {
	for i := len(updates) - 1; i >= 0; i-- {
		if updates[i].ID == id {
			cp := updates[i]
			return &cp
		}
	}
	return nil
}

// scriptedEmbedder lets tests provide a per-call embedding response so the
// backfill suite can drive distinct similarity outcomes for sequential audits.
type scriptedEmbedder struct {
	calls atomic.Int32
	next  func(*provider.EmbeddingRequest) (*provider.EmbeddingResponse, error)
	dim   int
}

func (s *scriptedEmbedder) Embed(_ context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	s.calls.Add(1)
	return s.next(req)
}
func (s *scriptedEmbedder) Name() string      { return "scripted" }
func (s *scriptedEmbedder) Dimensions() []int { return []int{s.dim} }

func auditReasonFromMeta(raw json.RawMessage) string {
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	r, _ := m["novelty_audit_reason"].(string)
	return r
}

// TestAuditExistingDreams_PersistentEmbedErrorStamps proves the
// silent-re-eligibility-loop fix: when the embedder returns an error that
// will fail identically on retry (HTTP 4xx, context-overflow phrasing),
// the audit stamps the synthesis with embed_error_persistent so it exits
// eligibility instead of re-entering it every cycle. Without this, the
// project's dirty flag never clears because consolidation always reports
// has_residual=true on these memories. See phase_consolidation.go and
// scheduler.go:251.
func TestAuditExistingDreams_PersistentEmbedErrorStamps(t *testing.T) {
	src := model.Memory{ID: uuid.New(), Content: "source"}
	dream := dreamMemory("synthesis with oversized context", []uuid.UUID{src.ID})

	emb := &staticEmbedder{
		err: errors.New("openai: embedding request failed: API error (400): context length exceeded for model"),
	}
	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyBackfillPerCycle] = 50
	writer := &updatingMemoryWriter{}
	reader := &fakeMemoryReader{list: []model.Memory{src, dream}}
	phase := newAuditPhase(emb, &scriptedJudgeLLM{}, settings, writer, reader)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: dream.NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.AuditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, reader.list, settings.ints[service.SettingDreamNoveltyBackfillPerCycle]); err != nil {
		t.Fatalf("AuditExistingDreams returned error: %v", err)
	}

	// Persistent embed error stamps as audited without demoting; that's
	// a metadata-only write so it lands in metadataUpdates.
	if len(writer.updates) != 0 {
		t.Fatalf("persistent embed error without demote must not call Update, got %d", len(writer.updates))
	}
	if len(writer.metadataUpdates) != 1 {
		t.Fatalf("persistent embed error must stamp the synthesis (1 metadata update), got %d", len(writer.metadataUpdates))
	}
	stamped := writer.metadataUpdates[0]
	if stamped.ID != dream.ID {
		t.Fatalf("expected the dream to be stamped, got %s", stamped.ID)
	}
	if !hasAuditMarker(stamped.Metadata) {
		t.Errorf("dream missing novelty_audited_at marker after persistent error; got %s", string(stamped.Metadata))
	}
	if got := auditReasonFromMeta(stamped.Metadata); got != "embed_error_persistent" {
		t.Errorf("expected reason embed_error_persistent, got %q", got)
	}
	if isLowNoveltyJSON(stamped.Metadata) {
		t.Errorf("persistent embed error must NOT demote (low_novelty=false); got %s", string(stamped.Metadata))
	}
}

// TestAuditExistingDreams_TransientEmbedErrorDoesNotStamp asserts the
// inverse: 5xx and other transient errors leave the synthesis in the
// eligibility set so the next cycle retries. This is the pre-fix
// behavior preserved for transient failures.
func TestAuditExistingDreams_TransientEmbedErrorDoesNotStamp(t *testing.T) {
	src := model.Memory{ID: uuid.New(), Content: "source"}
	dream := dreamMemory("synthesis with transient blip", []uuid.UUID{src.ID})

	emb := &staticEmbedder{
		err: errors.New("openai: embedding request failed: API error (503): service unavailable"),
	}
	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyBackfillPerCycle] = 50
	writer := &updatingMemoryWriter{}
	reader := &fakeMemoryReader{list: []model.Memory{src, dream}}
	phase := newAuditPhase(emb, &scriptedJudgeLLM{}, settings, writer, reader)

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: dream.NamespaceID}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.AuditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, reader.list, settings.ints[service.SettingDreamNoveltyBackfillPerCycle]); err != nil {
		t.Fatalf("AuditExistingDreams returned error: %v", err)
	}

	if len(writer.updates) != 0 {
		t.Fatalf("transient embed error must NOT stamp; got %d updates", len(writer.updates))
	}
}

func TestIsPersistentEmbedError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"http_400", errors.New("openai: embedding request failed: API error (400): bad request"), true},
		{"http_413_payload", errors.New("API error (413): request entity too large"), true},
		{"http_499", errors.New("API error (499): client closed request"), true},
		{"http_503_transient", errors.New("openai: embedding request failed: API error (503): service unavailable"), false},
		{"http_500_transient", errors.New("API error (500): internal server error"), false},
		{"context_length_phrase", errors.New("the input exceeds context length of 2048 tokens"), true},
		{"too_long_phrase", errors.New("input is too long for the model"), true},
		{"connection_refused_transient", errors.New("dial tcp 192.168.2.35:11434: connection refused"), false},
		{"timeout_transient", errors.New("context deadline exceeded"), false},
		{"context_window_phrase", errors.New("Input length exceeds maximum context window."), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isPersistentEmbedError(c.err); got != c.want {
				t.Errorf("isPersistentEmbedError(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
