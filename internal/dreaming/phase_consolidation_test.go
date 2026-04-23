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
// inspect what the consolidation phase wrote back.
type updatingMemoryWriter struct {
	creates []*model.Memory
	updates []model.Memory
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
		&recordingTokenRecorder{},
		stubUsageCtxResolver{},
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
	passed, reason, usage, err := phase.auditNovelty(context.Background(), llm, "candidate", []model.Memory{src})
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

func TestAuditNovelty_EmbedLowSim_AutoAccept(t *testing.T) {
	emb := &staticEmbedder{vectors: [][]float32{{1, 0, 0}, {0, 1, 0}}}
	llm := &scriptedJudgeLLM{}
	phase := newAuditPhase(emb, llm, noveltySettings(true), &updatingMemoryWriter{}, &fakeMemoryReader{})

	src := model.Memory{ID: uuid.New(), Content: "source"}
	passed, reason, usage, err := phase.auditNovelty(context.Background(), llm, "candidate", []model.Memory{src})
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
	passed, reason, usage, err := phase.auditNovelty(context.Background(), llm, "candidate", []model.Memory{src})
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
	passed, reason, usage, _ := phase.auditNovelty(context.Background(), llm, "candidate", []model.Memory{src})
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
	passed, reason, _, err := phase.auditNovelty(context.Background(), llm, "candidate", []model.Memory{src})
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
	passed, reason, usage, err := phase.auditNovelty(context.Background(), llm, "candidate", []model.Memory{src})
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
	passed, reason, _, _ := phase.auditNovelty(context.Background(), &scriptedJudgeLLM{}, "candidate", nil)
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

	if err := phase.auditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, reader.list); err != nil {
		t.Fatalf("auditExistingDreams returned error: %v", err)
	}

	// Both dreams should have been processed (Update called on each).
	if len(writer.updates) != 2 {
		t.Fatalf("expected 2 Update calls (one demote + one stamp), got %d", len(writer.updates))
	}

	var demoted, stamped *model.Memory
	for i := range writer.updates {
		u := writer.updates[i]
		if u.ID == dupDream.ID {
			demoted = &u
		}
		if u.ID == novelDream.ID {
			stamped = &u
		}
	}
	if demoted == nil {
		t.Fatalf("duplicate dream was not updated")
	}
	if stamped == nil {
		t.Fatalf("novel dream was not stamped")
	}

	if demoted.Confidence != 0 {
		t.Errorf("duplicate dream confidence should be 0, got %v", demoted.Confidence)
	}
	if !isLowNoveltyJSON(demoted.Metadata) {
		t.Errorf("duplicate dream metadata.low_novelty must be true; got %s", string(demoted.Metadata))
	}
	if !hasAuditMarker(demoted.Metadata) {
		t.Errorf("duplicate dream missing novelty_audited_at marker")
	}

	if stamped.Confidence == 0 {
		t.Errorf("novel dream confidence should not be zeroed by passing audit, got 0")
	}
	if isLowNoveltyJSON(stamped.Metadata) {
		t.Errorf("novel dream must not be flagged low_novelty")
	}
	if !hasAuditMarker(stamped.Metadata) {
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

	if err := phase.auditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, memories); err != nil {
		t.Fatalf("auditExistingDreams returned error: %v", err)
	}
	if len(writer.updates) != 3 {
		t.Fatalf("backfill cap=3 should produce exactly 3 updates, got %d", len(writer.updates))
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

	_ = phase.auditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, memories)

	if len(writer.updates) != 0 {
		t.Fatalf("already-audited dream must not be updated, got %d updates", len(writer.updates))
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
	_ = phase.auditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, []model.Memory{src, dream})

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
	_ = phase.auditExistingDreams(context.Background(), cycle, budget, logger, &scriptedJudgeLLM{}, []model.Memory{orphan})

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
