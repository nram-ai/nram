package dreaming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

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
//
// createErr, when non-nil, is returned from Create instead of recording —
// used by the consolidate error-path table test to exercise the
// post-audit Create-failure branch.
type updatingMemoryWriter struct {
	creates             []*model.Memory
	updates             []model.Memory
	metadataUpdates     []metadataUpdateRecord
	embeddingDimUpdates []embeddingDimUpdateRecord
	embeddingDimClears  []idNamespaceRecord
	confidenceUpdates   []confidenceUpdateRecord
	demotes             []demoteRecord
	supersedeMarks      []supersedeMarkRecord
	createErr           error
	// seed is the initial memory state MutateInLock re-reads against
	// when no prior Update for the id exists. Tests that exercise paths
	// going through MutateInLock should populate seed with the same
	// memories given to the reader.
	seed []model.Memory
}

type embeddingDimUpdateRecord struct {
	ID  uuid.UUID
	Dim int
}

type idNamespaceRecord struct {
	ID          uuid.UUID
	NamespaceID uuid.UUID
}

type confidenceUpdateRecord struct {
	ID          uuid.UUID
	NamespaceID uuid.UUID
	Confidence  float64
}

type demoteRecord struct {
	ID          uuid.UUID
	NamespaceID uuid.UUID
	Metadata    json.RawMessage
}

type supersedeMarkRecord struct {
	OldID       uuid.UUID
	NamespaceID uuid.UUID
	NewID       uuid.UUID
}

type metadataUpdateRecord struct {
	ID          uuid.UUID
	NamespaceID uuid.UUID
	Metadata    json.RawMessage
}

func (w *updatingMemoryWriter) Create(_ context.Context, mem *model.Memory) error {
	if w.createErr != nil {
		return w.createErr
	}
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
func (w *updatingMemoryWriter) UpdateEmbeddingDim(_ context.Context, id uuid.UUID, dim int) error {
	w.embeddingDimUpdates = append(w.embeddingDimUpdates, embeddingDimUpdateRecord{ID: id, Dim: dim})
	return nil
}
func (w *updatingMemoryWriter) ClearEmbeddingDim(_ context.Context, id, namespaceID uuid.UUID) error {
	w.embeddingDimClears = append(w.embeddingDimClears, idNamespaceRecord{ID: id, NamespaceID: namespaceID})
	return nil
}
func (w *updatingMemoryWriter) UpdateConfidence(_ context.Context, id, namespaceID uuid.UUID, confidence float64) error {
	w.confidenceUpdates = append(w.confidenceUpdates, confidenceUpdateRecord{
		ID: id, NamespaceID: namespaceID, Confidence: confidence,
	})
	return nil
}
func (w *updatingMemoryWriter) Demote(_ context.Context, id, namespaceID uuid.UUID, metadata json.RawMessage) error {
	cp := append(json.RawMessage(nil), metadata...)
	w.demotes = append(w.demotes, demoteRecord{ID: id, NamespaceID: namespaceID, Metadata: cp})
	return nil
}
func (w *updatingMemoryWriter) MarkSupersededBy(_ context.Context, oldID, namespaceID, newID uuid.UUID) error {
	w.supersedeMarks = append(w.supersedeMarks, supersedeMarkRecord{
		OldID: oldID, NamespaceID: namespaceID, NewID: newID,
	})
	return nil
}
func (w *updatingMemoryWriter) MutateInLock(_ context.Context, id uuid.UUID, mutate func(*model.Memory) (bool, error)) (*model.Memory, error) {
	var current *model.Memory
	for i := len(w.updates) - 1; i >= 0; i-- {
		if w.updates[i].ID == id {
			cp := w.updates[i]
			current = &cp
			break
		}
	}
	if current == nil {
		for i := range w.seed {
			if w.seed[i].ID == id {
				cp := w.seed[i]
				current = &cp
				break
			}
		}
	}
	if current == nil {
		return nil, fmt.Errorf("updatingMemoryWriter.MutateInLock: memory %s not in updates or seed", id)
	}
	write, err := mutate(current)
	if err != nil {
		return nil, err
	}
	if write {
		w.updates = append(w.updates, *current)
	}
	return current, nil
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
	meta := map[string]any{}
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

func TestAuditNovelty_BudgetSkipsJudge(t *testing.T) {
	// Borderline cosine forces fall-through to the LLM judge so the
	// pre-flight gate is the only thing preventing the call.
	emb := &staticEmbedder{vectors: [][]float32{{1, 0}, {0.95, 0.31}}}
	llm := &scriptedJudgeLLM{
		content: `{"novel_facts": ["should not be called"]}`,
		usage:   provider.TokenUsage{PromptTokens: 50, CompletionTokens: 10, TotalTokens: 60},
	}
	settings := noveltySettings(true)
	settings.ints[service.SettingDreamNoveltyJudgeMaxTokens] = 256
	phase := newAuditPhase(emb, llm, settings, &updatingMemoryWriter{}, &fakeMemoryReader{})

	// Budget sized so the embed pre-filter spend fits but the judge
	// prompt + per-call cap does not. EstimateTokens uses len/4.
	budget := NewTokenBudget(10, 5)

	src := model.Memory{ID: uuid.New(), Content: "source"}
	passed, reason, usage, embedTokens, err := phase.auditNovelty(
		context.Background(), llm, budget, "candidate", []model.Memory{src}, 0, "")

	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("expected ErrBudgetExhausted, got %v", err)
	}
	if passed {
		t.Fatalf("expected passed=false on budget skip, got true")
	}
	if reason != "skipped_budget" {
		t.Fatalf("expected reason=skipped_budget, got %q", reason)
	}
	if usage != nil {
		t.Fatalf("expected nil judge usage when call is skipped, got %+v", usage)
	}
	if llm.calls.Load() != 0 {
		t.Fatalf("LLM judge must not run under budget pressure, got %d calls", llm.calls.Load())
	}
	if embedTokens == 0 {
		t.Fatalf("expected embed tokens to be preserved on budget-skip path")
	}
	if budget.Remaining() == 0 {
		t.Fatalf("budget should retain headroom on a pre-flight skip; remaining=%d total=%d used=%d", budget.Remaining(), budget.Total(), budget.Used())
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

	// Demote takes the partial Demote path (confidence=0 + clear dim +
	// metadata in one statement) so a concurrent supersede on the row
	// cannot be clobbered. Stamp-only audits go through UpdateMetadata
	// so updated_at stays intact and the next cycle does not re-audit.
	if len(writer.demotes) != 1 {
		t.Fatalf("expected 1 Demote call, got %d", len(writer.demotes))
	}
	if len(writer.metadataUpdates) != 1 {
		t.Fatalf("expected 1 UpdateMetadata call (stamp-only audit), got %d", len(writer.metadataUpdates))
	}

	demoted := writer.demotes[0]
	if demoted.ID != dupDream.ID {
		t.Fatalf("Demote target should be duplicate dream %s, got %s", dupDream.ID, demoted.ID)
	}
	stampRecord := writer.metadataUpdates[0]
	if stampRecord.ID != novelDream.ID {
		t.Fatalf("UpdateMetadata target should be novel dream %s, got %s", novelDream.ID, stampRecord.ID)
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
	for i := range 5 {
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
	meta := map[string]any{
		"source_memory_ids":  []any{src.ID.String()},
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

	if len(writer.demotes) != 1 {
		t.Fatalf("orphan dream must be demoted (Demote partial-update); got %d demotes", len(writer.demotes))
	}
	if !isLowNoveltyJSON(writer.demotes[0].Metadata) {
		t.Errorf("orphan dream must be marked low_novelty; got %s", string(writer.demotes[0].Metadata))
	}
}

// --- helpers used only by tests ---

func isLowNoveltyJSON(raw json.RawMessage) bool {
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	v, ok := m["low_novelty"].(bool)
	return ok && v
}

func hasAuditMarker(raw json.RawMessage) bool {
	var m map[string]any
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
	return slices.Contains(ids, target)
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
	meta, _ := json.Marshal(map[string]any{
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

	// Both originals should be marked superseded by the synthesis
	// through the partial MarkSupersededBy path (which also clears
	// embedding_dim atomically). The race-guarded WHERE clause prevents
	// a concurrent memory_update from clobbering this write.
	for _, id := range []uuid.UUID{srcA.ID, srcB.ID} {
		var found *supersedeMarkRecord
		for i := range writer.supersedeMarks {
			if writer.supersedeMarks[i].OldID == id {
				found = &writer.supersedeMarks[i]
				break
			}
		}
		if found == nil {
			t.Errorf("expected MarkSupersededBy on original %s", id)
			continue
		}
		if found.NewID != synthesis.ID {
			t.Errorf("original %s should be superseded by synthesis %s, got %s", id, synthesis.ID, found.NewID)
		}
	}
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
	var m map[string]any
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

// --- reinforce sub-phase tests ---

// reinforceSettings returns a settings stub with the alignment-prompt
// template and the confidence floats reinforce reads at startup. Callers
// can mutate the returned struct to override individual values.
func reinforceSettings() *staticDreamSettings {
	return &staticDreamSettings{
		values: map[string]string{
			service.SettingDreamAlignmentPrompt: "synthesis: %s\nevidence: %s",
		},
		floats: map[string]float64{
			service.SettingDreamInitialConfidence:     0.3,
			service.SettingDreamSupersessionThreshold: 0.85,
		},
		ints: map[string]int{},
	}
}

// reinforcePhase wires a ConsolidationPhase and an updatingMemoryWriter
// suitable for reinforce-only tests (no embedder, no audit).
func reinforcePhase(llm provider.LLMProvider, settings SettingsResolver) (*ConsolidationPhase, *updatingMemoryWriter) {
	writer := &updatingMemoryWriter{}
	phase := NewConsolidationPhase(
		&fakeMemoryReader{},
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return nil },
		settings,
	)
	return phase, writer
}

// userMemoryForReinforce builds a non-DreamSource memory suitable for use as
// reinforce evidence. UpdatedAt is set so the row is comparable to syntheses.
func userMemoryForReinforce(content string, ns uuid.UUID) model.Memory {
	src := "user"
	now := time.Now().UTC()
	return model.Memory{
		ID:          uuid.New(),
		NamespaceID: ns,
		Content:     content,
		Source:      &src,
		Confidence:  0.5,
		Metadata:    json.RawMessage("{}"),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// synthesisForReinforce builds a DreamSource memory with a known UpdatedAt
// so stamp/staleness round-trips are exact.
func synthesisForReinforce(content string, ns uuid.UUID, updatedAt time.Time) model.Memory {
	src := model.DreamSource
	return model.Memory{
		ID:          uuid.New(),
		NamespaceID: ns,
		Content:     content,
		Source:      &src,
		Confidence:  0.3,
		Metadata:    json.RawMessage("{}"),
		CreatedAt:   updatedAt,
		UpdatedAt:   updatedAt,
	}
}

// alignmentResponse builds the JSON content scoreAlignment expects.
func alignmentResponse(score float64) string {
	return `{"alignment": ` + jsonFloat(score) + `, "reasoning": ""}`
}

func jsonFloat(f float64) string {
	b, _ := json.Marshal(f)
	return string(b)
}

// TestReinforce_StampsOnNoChange asserts the bug fix: a synthesis scored by
// reinforce whose alignment leaves Confidence unchanged is stamped via
// UpdateMetadata so the next cycle filters it out. Pre-fix, no stamp meant
// the synthesis re-entered the loop every cycle and projects with more
// syntheses than the per-cycle budget could fit kept dirty_since set
// indefinitely.
func TestReinforce_StampsOnNoChange(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	synth := synthesisForReinforce("stable synthesis content", ns, now)
	user := userMemoryForReinforce("evidence", ns)

	llm := &scriptedJudgeLLM{
		content: alignmentResponse(0.0),
		usage:   provider.TokenUsage{PromptTokens: 30, CompletionTokens: 5, TotalTokens: 35},
	}
	phase, writer := reinforcePhase(llm, reinforceSettings())

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	residual, err := phase.reinforce(context.Background(), cycle, budget, logger, llm, []model.Memory{synth, user})
	if err != nil {
		t.Fatalf("reinforce returned error: %v", err)
	}
	if residual {
		t.Fatalf("residual must be false when every stale synthesis was visited")
	}

	if len(writer.updates) != 0 {
		t.Fatalf("no-change branch must not call Update (Update would bump updated_at and re-stale the row); got %d Update calls", len(writer.updates))
	}
	if len(writer.metadataUpdates) != 1 {
		t.Fatalf("no-change branch must stamp via UpdateMetadata; got %d UpdateMetadata calls", len(writer.metadataUpdates))
	}
	stamp := writer.metadataUpdates[0]
	if stamp.ID != synth.ID {
		t.Errorf("wrong row stamped: got %s, want %s", stamp.ID, synth.ID)
	}
	var meta map[string]any
	if err := json.Unmarshal(stamp.Metadata, &meta); err != nil {
		t.Fatalf("stamp metadata not valid JSON: %v", err)
	}
	stampVal, ok := meta[ReinforceCheckedStampKey].(string)
	if !ok || stampVal == "" {
		t.Fatalf("stamp must carry %s string; got metadata=%s", ReinforceCheckedStampKey, string(stamp.Metadata))
	}
	if stampVal != now.Format(time.RFC3339Nano) {
		t.Errorf("stamp value should equal mem.UpdatedAt formatted as RFC3339Nano; got %q want %q", stampVal, now.Format(time.RFC3339Nano))
	}
}

// TestReinforce_FreshStampSkipsSynthesis asserts the eligibility filter: a
// synthesis whose stamp equals UpdatedAt is fresh (predicate is strictly
// before, not before-or-equal) and must be filtered out before any LLM
// call.
func TestReinforce_FreshStampSkipsSynthesis(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	synth := synthesisForReinforce("already checked", ns, now)

	meta := map[string]any{
		ReinforceCheckedStampKey: now.Format(time.RFC3339Nano),
	}
	raw, _ := json.Marshal(meta)
	synth.Metadata = raw

	user := userMemoryForReinforce("evidence", ns)

	llm := &scriptedJudgeLLM{content: alignmentResponse(0.0)}
	phase, writer := reinforcePhase(llm, reinforceSettings())

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	residual, err := phase.reinforce(context.Background(), cycle, budget, logger, llm, []model.Memory{synth, user})
	if err != nil {
		t.Fatalf("reinforce returned error: %v", err)
	}
	if residual {
		t.Fatalf("residual must be false when there are zero stale syntheses")
	}
	if llm.calls.Load() != 0 {
		t.Fatalf("fresh-stamped synthesis must skip the LLM; got %d alignment calls", llm.calls.Load())
	}
	if len(writer.updates) != 0 || len(writer.metadataUpdates) != 0 {
		t.Errorf("fresh-stamped row must not be touched; got %d Update / %d UpdateMetadata", len(writer.updates), len(writer.metadataUpdates))
	}
}

// TestReinforce_StaleStampReEvaluated asserts that a stamp strictly before
// UpdatedAt (e.g. a confidence change happened after the last visit)
// re-stales the row so it gets re-aligned this cycle.
func TestReinforce_StaleStampReEvaluated(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	synth := synthesisForReinforce("changed since last check", ns, now)

	meta := map[string]any{
		ReinforceCheckedStampKey: now.Add(-time.Hour).Format(time.RFC3339Nano),
	}
	raw, _ := json.Marshal(meta)
	synth.Metadata = raw

	user := userMemoryForReinforce("evidence", ns)

	llm := &scriptedJudgeLLM{
		content: alignmentResponse(0.0),
		usage:   provider.TokenUsage{PromptTokens: 30, CompletionTokens: 5, TotalTokens: 35},
	}
	phase, writer := reinforcePhase(llm, reinforceSettings())

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.reinforce(context.Background(), cycle, budget, logger, llm, []model.Memory{synth, user}); err != nil {
		t.Fatalf("reinforce returned error: %v", err)
	}
	if llm.calls.Load() != 1 {
		t.Fatalf("stale-stamped synthesis must be re-aligned; got %d alignment calls", llm.calls.Load())
	}
	if len(writer.metadataUpdates) != 1 {
		t.Fatalf("stale row should be re-stamped on no-change; got %d UpdateMetadata calls", len(writer.metadataUpdates))
	}
}

// TestReinforce_ConfidenceChangeDoesNotStamp asserts that when alignment
// shifts confidence, reinforce calls Update (which bumps updated_at) but
// does NOT call UpdateMetadata. The bumped updated_at re-stales the row so
// the changed synthesis is re-evaluated next cycle.
func TestReinforce_ConfidenceChangeDoesNotStamp(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	synth := synthesisForReinforce("will move", ns, now)

	user := userMemoryForReinforce("evidence", ns)

	// alignment 0.5 against confidence 0.3 → newConfidence = 0.65, != 0.3
	llm := &scriptedJudgeLLM{
		content: alignmentResponse(0.5),
		usage:   provider.TokenUsage{PromptTokens: 30, CompletionTokens: 5, TotalTokens: 35},
	}
	phase, writer := reinforcePhase(llm, reinforceSettings())

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.reinforce(context.Background(), cycle, budget, logger, llm, []model.Memory{synth, user}); err != nil {
		t.Fatalf("reinforce returned error: %v", err)
	}

	// Confidence change goes through the partial UpdateConfidence
	// helper (which bumps updated_at) so a concurrent supersede on the
	// synthesis is not clobbered.
	if len(writer.confidenceUpdates) != 1 {
		t.Fatalf("confidence change must call UpdateConfidence exactly once; got %d", len(writer.confidenceUpdates))
	}
	if len(writer.metadataUpdates) != 0 {
		t.Errorf("confidence change must NOT stamp via UpdateMetadata (UpdateConfidence bumps updated_at; the bump is the re-stale signal); got %d UpdateMetadata calls", len(writer.metadataUpdates))
	}
}

// TestReinforce_ErrorScoringLeavesRowStampFree asserts that an LLM error on
// scoreAlignment leaves the synthesis unstamped so the next cycle retries.
// The skipped-budget paths share this property structurally (they break
// before any stamp call) and are covered by inspection rather than a
// dedicated test.
func TestReinforce_ErrorScoringLeavesRowStampFree(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	synth := synthesisForReinforce("alignment will error", ns, now)

	user := userMemoryForReinforce("evidence", ns)

	llm := &scriptedJudgeLLM{err: errors.New("provider down")}
	phase, writer := reinforcePhase(llm, reinforceSettings())

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.reinforce(context.Background(), cycle, budget, logger, llm, []model.Memory{synth, user}); err != nil {
		t.Fatalf("reinforce should swallow per-call alignment errors; got %v", err)
	}
	if len(writer.updates) != 0 || len(writer.confidenceUpdates) != 0 {
		t.Errorf("error-scoring path must not Update; got %d Update / %d UpdateConfidence", len(writer.updates), len(writer.confidenceUpdates))
	}
	if len(writer.metadataUpdates) != 0 {
		t.Errorf("error-scoring path must not stamp; got %d UpdateMetadata calls", len(writer.metadataUpdates))
	}
}

// TestReinforce_StampsPersistAcrossCycles is the load-bearing regression
// for the stamp self-invalidation class of bug. Two consecutive reinforce
// cycles, no content edits between. Cycle 1 stamps via UpdateMetadata;
// cycle 2 must classify the synthesis as fresh and skip it. Mirrors
// TestParaphraseDedupPhase_StampsPersistAcrossCycles.
func TestReinforce_StampsPersistAcrossCycles(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	synth := synthesisForReinforce("stable synthesis", ns, now)
	user := userMemoryForReinforce("evidence", ns)

	llm := &scriptedJudgeLLM{
		content: alignmentResponse(0.0),
		usage:   provider.TokenUsage{PromptTokens: 30, CompletionTokens: 5, TotalTokens: 35},
	}
	phase, writer := reinforcePhase(llm, reinforceSettings())

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	// Cycle 1: stamp written via UpdateMetadata.
	if _, err := phase.reinforce(context.Background(), cycle, budget, logger, llm, []model.Memory{synth, user}); err != nil {
		t.Fatalf("cycle 1 reinforce: %v", err)
	}
	if len(writer.metadataUpdates) != 1 {
		t.Fatalf("cycle 1 should stamp once via UpdateMetadata; got %d", len(writer.metadataUpdates))
	}
	if llm.calls.Load() != 1 {
		t.Fatalf("cycle 1 should call alignment LLM once; got %d", llm.calls.Load())
	}

	// Apply the metadata update to the in-memory copy so cycle 2 sees the
	// stamp. updated_at is intentionally NOT bumped — that's the whole
	// point of UpdateMetadata.
	synth.Metadata = writer.metadataUpdates[0].Metadata

	// Cycle 2: same memory, stamped now. Must be filtered out at
	// eligibility, no LLM call, no writes.
	if _, err := phase.reinforce(context.Background(), cycle, budget, logger, llm, []model.Memory{synth, user}); err != nil {
		t.Fatalf("cycle 2 reinforce: %v", err)
	}
	if llm.calls.Load() != 1 {
		t.Errorf("cycle 2 should not invoke the LLM (synthesis is fresh); total calls now %d, expected 1 from cycle 1", llm.calls.Load())
	}
	if len(writer.metadataUpdates) != 1 {
		t.Errorf("cycle 2 should not re-stamp; total UpdateMetadata calls now %d, expected 1 from cycle 1", len(writer.metadataUpdates))
	}
	if len(writer.updates) != 0 {
		t.Errorf("cycle 2 must not Update; got %d", len(writer.updates))
	}
}

// TestCollectReinforceStale_PredicateCases is the table-driven unit test of
// the staleness predicate. Mirrors the cases in isStale (contradictions)
// and isParaphraseStale: no stamp, malformed stamp, parsed stamp before
// UpdatedAt → stale; parsed stamp == UpdatedAt or after → fresh.
func TestCollectReinforceStale_PredicateCases(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()

	mkSynth := func(meta map[string]any) model.Memory {
		s := synthesisForReinforce("c", ns, now)
		if meta != nil {
			raw, _ := json.Marshal(meta)
			s.Metadata = raw
		}
		return s
	}

	cases := []struct {
		name      string
		mem       model.Memory
		wantStale bool
	}{
		{
			name:      "no_metadata",
			mem:       mkSynth(nil),
			wantStale: true,
		},
		{
			name:      "no_stamp_key",
			mem:       mkSynth(map[string]any{"other": "value"}),
			wantStale: true,
		},
		{
			name:      "malformed_stamp_value",
			mem:       mkSynth(map[string]any{ReinforceCheckedStampKey: "not-a-timestamp"}),
			wantStale: true,
		},
		{
			name:      "non_string_stamp_value",
			mem:       mkSynth(map[string]any{ReinforceCheckedStampKey: 12345}),
			wantStale: true,
		},
		{
			name:      "stamp_before_updated_at",
			mem:       mkSynth(map[string]any{ReinforceCheckedStampKey: now.Add(-time.Hour).Format(time.RFC3339Nano)}),
			wantStale: true,
		},
		{
			name:      "stamp_equal_updated_at",
			mem:       mkSynth(map[string]any{ReinforceCheckedStampKey: now.Format(time.RFC3339Nano)}),
			wantStale: false,
		},
		{
			name:      "stamp_after_updated_at",
			mem:       mkSynth(map[string]any{ReinforceCheckedStampKey: now.Add(time.Hour).Format(time.RFC3339Nano)}),
			wantStale: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stale := collectReinforceStale([]model.Memory{c.mem})
			gotStale := len(stale) == 1
			if gotStale != c.wantStale {
				t.Errorf("collectReinforceStale: gotStale=%v, want=%v (metadata=%s)", gotStale, c.wantStale, string(c.mem.Metadata))
			}
		})
	}

	// RFC3339 (no-nano) fallback: data written by an earlier version that
	// stamped at second precision must still classify fresh when the
	// memory's UpdatedAt is second-aligned. Tested separately because
	// arbitrary nanosecond UpdatedAt loses precision through the fallback
	// format and that precision loss is acceptable, just out of scope.
	t.Run("rfc3339_fallback_second_aligned", func(t *testing.T) {
		secondAligned := now.Truncate(time.Second)
		mem := synthesisForReinforce("c", ns, secondAligned)
		stamp := map[string]any{
			ReinforceCheckedStampKey: secondAligned.Format(time.RFC3339),
		}
		raw, _ := json.Marshal(stamp)
		mem.Metadata = raw
		if got := collectReinforceStale([]model.Memory{mem}); len(got) != 0 {
			t.Errorf("RFC3339-formatted stamp on second-aligned UpdatedAt should be fresh; got %d stale", len(got))
		}
	})
}

// TestCollectReinforceStale_PartitionsMixedSet asserts the eligibility
// filter on a heterogeneous slice: only stale syntheses survive, and the
// result count matches what reinforce will report as syntheses_stale.
func TestCollectReinforceStale_PartitionsMixedSet(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()

	freshMeta, _ := json.Marshal(map[string]any{
		ReinforceCheckedStampKey: now.Format(time.RFC3339Nano),
	})
	staleMeta, _ := json.Marshal(map[string]any{
		ReinforceCheckedStampKey: now.Add(-time.Hour).Format(time.RFC3339Nano),
	})

	fresh := synthesisForReinforce("fresh", ns, now)
	fresh.Metadata = freshMeta

	stale := synthesisForReinforce("stale", ns, now)
	stale.Metadata = staleMeta

	unstamped := synthesisForReinforce("unstamped", ns, now)

	got := collectReinforceStale([]model.Memory{fresh, stale, unstamped})
	if len(got) != 2 {
		t.Fatalf("expected 2 stale (stale + unstamped), got %d", len(got))
	}

	ids := map[uuid.UUID]bool{}
	for _, s := range got {
		ids[s.mem.ID] = true
	}
	if !ids[stale.ID] {
		t.Errorf("stale synthesis missing from result")
	}
	if !ids[unstamped.ID] {
		t.Errorf("unstamped synthesis missing from result")
	}
	if ids[fresh.ID] {
		t.Errorf("fresh synthesis must not be in stale set")
	}
}

// --- consolidate sub-phase tests ---

// consolidateSettings returns a settings stub configured for consolidate
// tests. Callers can mutate the returned struct to disable novelty (omit
// the SettingDreamNoveltyEnabled key) or override thresholds.
func consolidateSettings(noveltyEnabled bool) *staticDreamSettings {
	values := map[string]string{
		service.SettingDreamSynthesisPrompt:    "synth: %s",
		service.SettingDreamNoveltyJudgePrompt: "judge: %s vs %s",
	}
	if noveltyEnabled {
		values[service.SettingDreamNoveltyEnabled] = "true"
	}
	return &staticDreamSettings{
		values: values,
		floats: map[string]float64{
			service.SettingDreamInitialConfidence:         0.3,
			service.SettingDreamNoveltyEmbedHighThreshold: 0.97,
			service.SettingDreamNoveltyEmbedLowThreshold:  0.85,
		},
		ints: map[string]int{
			service.SettingDreamNoveltyJudgeMaxTokens: 512,
		},
	}
}

// consolidatePhase wires a ConsolidationPhase + writer for direct
// invocation of consolidate(). embedder may be nil to skip the audit's
// embed pre-filter and route directly to the LLM judge.
func consolidatePhase(llm provider.LLMProvider, embedder provider.EmbeddingProvider, settings SettingsResolver) (*ConsolidationPhase, *updatingMemoryWriter) {
	writer := &updatingMemoryWriter{}
	phase := NewConsolidationPhase(
		&fakeMemoryReader{},
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return llm },
		func() provider.EmbeddingProvider { return embedder },
		settings,
	)
	return phase, writer
}

// candidateForConsolidate builds a non-DreamSource memory eligible as a
// consolidate candidate. Content is set so the caller controls
// clusterMemories' word-overlap output.
func candidateForConsolidate(content string, ns uuid.UUID, updatedAt time.Time) model.Memory {
	src := "user"
	return model.Memory{
		ID:          uuid.New(),
		NamespaceID: ns,
		Content:     content,
		Source:      &src,
		Confidence:  0.5,
		Metadata:    json.RawMessage("{}"),
		CreatedAt:   updatedAt,
		UpdatedAt:   updatedAt,
	}
}

// triClusterCandidates returns three user memories whose word overlap
// makes clusterMemories produce a single 3-member cluster.
func triClusterCandidates(ns uuid.UUID, t time.Time) (a, b, c model.Memory) {
	a = candidateForConsolidate("shared term1 term2 term3 term4", ns, t)
	b = candidateForConsolidate("shared term1 term2 alpha beta", ns, t)
	c = candidateForConsolidate("shared term1 gamma delta epsilon", ns, t)
	return
}

// stampClusterFresh marks every member of cluster with a stamp anchored
// to its UpdatedAt and the supplied fingerprint, simulating a row that
// was visited last cycle and should be filtered out this cycle.
func stampClusterFresh(t *testing.T, cluster []*model.Memory, fingerprint string) {
	t.Helper()
	for _, m := range cluster {
		raw, err := json.Marshal(map[string]any{
			ConsolidationClusterStampKey:       m.UpdatedAt.UTC().Format(time.RFC3339Nano),
			ConsolidationClusterFingerprintKey: fingerprint,
		})
		if err != nil {
			t.Fatalf("marshal stamp: %v", err)
		}
		m.Metadata = raw
	}
}

// TestConsolidate_StampsOnAuditRejection asserts that when the novelty
// audit rejects the synthesis, every source memory in the cluster is
// stamped via UpdateMetadata (no Create, no Update) so the same cluster
// does not burn budget next cycle.
func TestConsolidate_StampsOnAuditRejection(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c := triClusterCandidates(ns, now)

	// Empty novel_facts → audit rejects.
	llm := &scriptedJudgeLLM{
		content: `{"novel_facts": []}`,
		usage:   provider.TokenUsage{PromptTokens: 20, CompletionTokens: 5, TotalTokens: 25},
	}
	phase, writer := consolidatePhase(llm, nil, consolidateSettings(true))

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	residual, err := phase.consolidate(context.Background(), cycle, budget, logger, llm, []model.Memory{a, b, c})
	if err != nil {
		t.Fatalf("consolidate returned error: %v", err)
	}
	if residual {
		t.Fatalf("residual must be false when every stale cluster was visited")
	}

	if len(writer.creates) != 0 {
		t.Fatalf("audit-reject path must not Create; got %d", len(writer.creates))
	}
	if len(writer.updates) != 0 {
		t.Fatalf("audit-reject path must not Update; got %d", len(writer.updates))
	}
	if len(writer.metadataUpdates) != 3 {
		t.Fatalf("audit-reject path must stamp every cluster member exactly once; got %d UpdateMetadata calls", len(writer.metadataUpdates))
	}

	expectedFP := clusterFingerprint([]model.Memory{a, b, c})
	stamped := map[uuid.UUID]bool{}
	for _, u := range writer.metadataUpdates {
		var meta map[string]any
		if err := json.Unmarshal(u.Metadata, &meta); err != nil {
			t.Fatalf("stamp metadata not valid JSON: %v", err)
		}
		stampVal, ok := meta[ConsolidationClusterStampKey].(string)
		if !ok || stampVal == "" {
			t.Errorf("stamp must carry %s string; got %s", ConsolidationClusterStampKey, string(u.Metadata))
		}
		fp, ok := meta[ConsolidationClusterFingerprintKey].(string)
		if !ok || fp != expectedFP {
			t.Errorf("stamp fingerprint mismatch: got %q want %q", fp, expectedFP)
		}
		stamped[u.ID] = true
	}
	for _, m := range []model.Memory{a, b, c} {
		if !stamped[m.ID] {
			t.Errorf("cluster member %s missing from metadata updates", m.ID)
		}
	}
}

// TestConsolidate_StampsOnSuccessfulCreate asserts that when the audit
// passes and the synthesis is created, every source member is stamped
// (Create runs once for the synthesis, plus N stamp writes).
func TestConsolidate_StampsOnSuccessfulCreate(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c := triClusterCandidates(ns, now)

	llm := &scriptedJudgeLLM{
		content: `{"novel_facts": ["new fact"]}`,
		usage:   provider.TokenUsage{PromptTokens: 20, CompletionTokens: 5, TotalTokens: 25},
	}
	phase, writer := consolidatePhase(llm, nil, consolidateSettings(true))

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.consolidate(context.Background(), cycle, budget, logger, llm, []model.Memory{a, b, c}); err != nil {
		t.Fatalf("consolidate returned error: %v", err)
	}

	if len(writer.creates) != 1 {
		t.Fatalf("audit-pass must Create exactly one synthesis; got %d", len(writer.creates))
	}
	if len(writer.metadataUpdates) != 3 {
		t.Fatalf("audit-pass must stamp every source member; got %d UpdateMetadata calls", len(writer.metadataUpdates))
	}

	expectedFP := clusterFingerprint([]model.Memory{a, b, c})
	for _, u := range writer.metadataUpdates {
		var meta map[string]any
		_ = json.Unmarshal(u.Metadata, &meta)
		if fp, _ := meta[ConsolidationClusterFingerprintKey].(string); fp != expectedFP {
			t.Errorf("stamp fingerprint mismatch on member %s: got %q want %q", u.ID, fp, expectedFP)
		}
	}
}

// TestConsolidate_FreshClusterSkipped asserts the eligibility filter:
// when every member of the cluster is stamp-fresh AND the stored
// fingerprint matches the current cluster's fingerprint, the cluster is
// filtered out before the synthesis call.
func TestConsolidate_FreshClusterSkipped(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c := triClusterCandidates(ns, now)

	fp := clusterFingerprint([]model.Memory{a, b, c})
	stampClusterFresh(t, []*model.Memory{&a, &b, &c}, fp)

	llm := &scriptedJudgeLLM{content: `{"novel_facts": []}`}
	phase, writer := consolidatePhase(llm, nil, consolidateSettings(true))

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	residual, err := phase.consolidate(context.Background(), cycle, budget, logger, llm, []model.Memory{a, b, c})
	if err != nil {
		t.Fatalf("consolidate returned error: %v", err)
	}
	if residual {
		t.Fatalf("residual must be false when there are zero stale clusters")
	}
	if llm.calls.Load() != 0 {
		t.Errorf("fresh-stamped cluster must skip the LLM; got %d calls", llm.calls.Load())
	}
	if len(writer.creates) != 0 || len(writer.updates) != 0 || len(writer.metadataUpdates) != 0 {
		t.Errorf("fresh-stamped cluster must not be touched; got %d Create / %d Update / %d UpdateMetadata",
			len(writer.creates), len(writer.updates), len(writer.metadataUpdates))
	}
}

// TestConsolidate_StaleStampReEvaluated asserts that when even one
// member of the cluster is stale (stamp < UpdatedAt), the entire cluster
// surfaces and is re-audited; on a stable verdict, every member is
// re-stamped with the current fingerprint.
func TestConsolidate_StaleStampReEvaluated(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c := triClusterCandidates(ns, now)

	fp := clusterFingerprint([]model.Memory{a, b, c})
	// a and b stamp-fresh; c stamped before its UpdatedAt → stale.
	stampClusterFresh(t, []*model.Memory{&a, &b}, fp)
	rawStaleC, _ := json.Marshal(map[string]any{
		ConsolidationClusterStampKey:       now.Add(-time.Hour).Format(time.RFC3339Nano),
		ConsolidationClusterFingerprintKey: fp,
	})
	c.Metadata = rawStaleC

	llm := &scriptedJudgeLLM{
		content: `{"novel_facts": []}`,
		usage:   provider.TokenUsage{PromptTokens: 20, CompletionTokens: 5, TotalTokens: 25},
	}
	phase, writer := consolidatePhase(llm, nil, consolidateSettings(true))

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.consolidate(context.Background(), cycle, budget, logger, llm, []model.Memory{a, b, c}); err != nil {
		t.Fatalf("consolidate returned error: %v", err)
	}
	if llm.calls.Load() == 0 {
		t.Errorf("a stale member must surface the cluster; expected at least one LLM call, got %d", llm.calls.Load())
	}
	if len(writer.metadataUpdates) != 3 {
		t.Errorf("stale cluster should stamp every member on re-evaluation; got %d", len(writer.metadataUpdates))
	}
}

// TestConsolidate_ClusterReshape_StalesSurvivors is the load-bearing
// regression for the cluster-fingerprint extension. Pre-stamp every
// member with stamp == UpdatedAt AND a fingerprint that does not match
// the current cluster's fingerprint (simulating a member having migrated
// out between cycles, leaving stamp-fresh survivors). The cluster must
// re-enter eligibility despite all members appearing time-fresh.
func TestConsolidate_ClusterReshape_StalesSurvivors(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c := triClusterCandidates(ns, now)

	// Compute a fingerprint as if a fourth (now-migrated-out) member
	// had been part of the prior cluster. Stamp every current member
	// with that stale fingerprint.
	migratedOut := model.Memory{ID: uuid.New()}
	staleFP := clusterFingerprint([]model.Memory{a, b, c, migratedOut})
	stampClusterFresh(t, []*model.Memory{&a, &b, &c}, staleFP)

	currentFP := clusterFingerprint([]model.Memory{a, b, c})
	if staleFP == currentFP {
		t.Fatalf("test setup invalid: stale and current fingerprints must differ")
	}

	llm := &scriptedJudgeLLM{
		content: `{"novel_facts": []}`,
		usage:   provider.TokenUsage{PromptTokens: 20, CompletionTokens: 5, TotalTokens: 25},
	}
	phase, writer := consolidatePhase(llm, nil, consolidateSettings(true))

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	if _, err := phase.consolidate(context.Background(), cycle, budget, logger, llm, []model.Memory{a, b, c}); err != nil {
		t.Fatalf("consolidate returned error: %v", err)
	}
	if llm.calls.Load() == 0 {
		t.Errorf("fingerprint mismatch must surface the cluster; expected LLM calls, got 0")
	}
	if len(writer.metadataUpdates) != 3 {
		t.Errorf("reshape-detected cluster should re-stamp every member; got %d", len(writer.metadataUpdates))
	}
	for _, u := range writer.metadataUpdates {
		var meta map[string]any
		_ = json.Unmarshal(u.Metadata, &meta)
		fp, _ := meta[ConsolidationClusterFingerprintKey].(string)
		if fp != currentFP {
			t.Errorf("re-stamp must carry the current cluster fingerprint; got %q want %q", fp, currentFP)
		}
	}
}

// TestConsolidate_StampsPersistAcrossCycles is the two-cycle regression
// for the stamp self-invalidation class of bug. Cycle 1 stamps via
// UpdateMetadata; cycle 2 must classify the cluster as fresh and skip it.
func TestConsolidate_StampsPersistAcrossCycles(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c := triClusterCandidates(ns, now)

	llm := &scriptedJudgeLLM{
		content: `{"novel_facts": []}`,
		usage:   provider.TokenUsage{PromptTokens: 20, CompletionTokens: 5, TotalTokens: 25},
	}
	phase, writer := consolidatePhase(llm, nil, consolidateSettings(true))

	cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
	logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
	budget := NewTokenBudget(10000, 2048)

	// Cycle 1: should reject + stamp.
	if _, err := phase.consolidate(context.Background(), cycle, budget, logger, llm, []model.Memory{a, b, c}); err != nil {
		t.Fatalf("cycle 1 consolidate: %v", err)
	}
	if len(writer.metadataUpdates) != 3 {
		t.Fatalf("cycle 1 should stamp 3 members; got %d", len(writer.metadataUpdates))
	}
	cycle1LLMCalls := llm.calls.Load()
	if cycle1LLMCalls == 0 {
		t.Fatalf("cycle 1 should invoke the LLM at least once")
	}

	// Apply stamps onto the in-memory copies so cycle 2 sees them.
	for _, m := range []*model.Memory{&a, &b, &c} {
		for _, u := range writer.metadataUpdates {
			if u.ID == m.ID {
				m.Metadata = u.Metadata
				break
			}
		}
	}

	// Cycle 2: cluster is fresh → no LLM calls, no writes.
	if _, err := phase.consolidate(context.Background(), cycle, budget, logger, llm, []model.Memory{a, b, c}); err != nil {
		t.Fatalf("cycle 2 consolidate: %v", err)
	}
	if llm.calls.Load() != cycle1LLMCalls {
		t.Errorf("cycle 2 should not invoke the LLM; total calls now %d, expected %d from cycle 1", llm.calls.Load(), cycle1LLMCalls)
	}
	if len(writer.metadataUpdates) != 3 {
		t.Errorf("cycle 2 should not re-stamp; total UpdateMetadata calls now %d, expected 3 from cycle 1", len(writer.metadataUpdates))
	}
	if len(writer.creates) != 0 {
		t.Errorf("cycle 2 must not Create; got %d", len(writer.creates))
	}
}

// TestConsolidate_ErrorPathsLeaveStampFree asserts that error and
// transient-output branches of the consolidate loop do not stamp the
// cluster, leaving it eligible for retry next cycle.
func TestConsolidate_ErrorPathsLeaveStampFree(t *testing.T) {
	type setup struct {
		name        string
		llm         *scriptedJudgeLLM
		embedder    provider.EmbeddingProvider
		settings    *staticDreamSettings
		writerSetup func(w *updatingMemoryWriter)
	}

	cases := []setup{
		{
			name:     "synthesis_error",
			llm:      &scriptedJudgeLLM{err: errors.New("synth boom")},
			settings: consolidateSettings(false),
		},
		{
			name: "audit_embed_error",
			llm: &scriptedJudgeLLM{
				content: `{"novel_facts": ["x"]}`,
				usage:   provider.TokenUsage{PromptTokens: 20, CompletionTokens: 5, TotalTokens: 25},
			},
			embedder: &staticEmbedder{err: errors.New("embed down")},
			settings: consolidateSettings(true),
		},
		{
			name: "empty_synthesis_content",
			llm: &scriptedJudgeLLM{
				content: "",
				usage:   provider.TokenUsage{PromptTokens: 20, CompletionTokens: 0, TotalTokens: 20},
			},
			settings: consolidateSettings(true),
		},
		{
			name: "create_error",
			llm: &scriptedJudgeLLM{
				content: "synthesized",
				usage:   provider.TokenUsage{PromptTokens: 20, CompletionTokens: 5, TotalTokens: 25},
			},
			settings: consolidateSettings(false),
			writerSetup: func(w *updatingMemoryWriter) {
				w.createErr = errors.New("create boom")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ns := uuid.New()
			now := time.Now().UTC()
			a, b, c := triClusterCandidates(ns, now)

			phase, writer := consolidatePhase(tc.llm, tc.embedder, tc.settings)
			if tc.writerSetup != nil {
				tc.writerSetup(writer)
			}

			cycle := &model.DreamCycle{ID: uuid.New(), NamespaceID: ns}
			logger := NewDreamLogWriter(nil, cycle.ID, uuid.UUID{})
			budget := NewTokenBudget(10000, 2048)

			if _, err := phase.consolidate(context.Background(), cycle, budget, logger, tc.llm, []model.Memory{a, b, c}); err != nil {
				t.Fatalf("consolidate should swallow per-call errors; got %v", err)
			}
			if len(writer.metadataUpdates) != 0 {
				t.Errorf("error path %s must leave cluster stamp-free; got %d UpdateMetadata calls", tc.name, len(writer.metadataUpdates))
			}
		})
	}
}

// TestCollectConsolidateStale_AnyMemberStaleStalesCluster asserts the
// cluster-level OR semantics: a single stale member surfaces the entire
// cluster; an all-fresh + matching-fingerprint cluster does not surface.
func TestCollectConsolidateStale_AnyMemberStaleStalesCluster(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c := triClusterCandidates(ns, now)

	fp := clusterFingerprint([]model.Memory{a, b, c})

	// All-fresh case.
	stampClusterFresh(t, []*model.Memory{&a, &b, &c}, fp)
	allFresh, eligible := collectConsolidateStale([][]model.Memory{{a, b, c}})
	if len(allFresh) != 0 {
		t.Errorf("all-fresh + matching fingerprint cluster must not surface; got %d stale", len(allFresh))
	}
	if eligible != 1 {
		t.Errorf("len-3 cluster should count as 1 eligible; got %d", eligible)
	}

	// One-stale case: rewrite c's stamp to a stale time.
	staleMeta, _ := json.Marshal(map[string]any{
		ConsolidationClusterStampKey:       now.Add(-time.Hour).Format(time.RFC3339Nano),
		ConsolidationClusterFingerprintKey: fp,
	})
	c.Metadata = staleMeta

	mixed, _ := collectConsolidateStale([][]model.Memory{{a, b, c}})
	if len(mixed) != 1 {
		t.Fatalf("one-stale member must surface the cluster; got %d", len(mixed))
	}
	if len(mixed[0].members) != 3 {
		t.Errorf("surfaced cluster should retain all members; got %d", len(mixed[0].members))
	}
	if mixed[0].fingerprint != fp {
		t.Errorf("surfaced cluster should carry current fingerprint; got %q want %q", mixed[0].fingerprint, fp)
	}
}

// TestCollectConsolidateStale_SkipsBelowFloor asserts that single-member
// clusters never surface, regardless of stamp state. Mirrors the
// eligibility floor inside consolidate (synthesis requires >= 2 sources).
func TestCollectConsolidateStale_SkipsBelowFloor(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	a := candidateForConsolidate("alpha lonely standalone unmatched orphan", ns, now)

	got, eligible := collectConsolidateStale([][]model.Memory{{a}})
	if len(got) != 0 {
		t.Errorf("single-member cluster must never surface; got %d stale", len(got))
	}
	if eligible != 0 {
		t.Errorf("single-member cluster must not count as eligible; got %d", eligible)
	}
}

// TestClusterFingerprint_Stable asserts iteration order does not affect
// the fingerprint — clusterMemories' anchor-order is unstable across
// cycles when one member is removed, so the stamp must be order-blind.
func TestClusterFingerprint_Stable(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c := triClusterCandidates(ns, now)

	abc := clusterFingerprint([]model.Memory{a, b, c})
	cba := clusterFingerprint([]model.Memory{c, b, a})
	bca := clusterFingerprint([]model.Memory{b, c, a})

	if abc != cba {
		t.Errorf("fingerprint must be order-independent; got abc=%q cba=%q", abc, cba)
	}
	if abc != bca {
		t.Errorf("fingerprint must be order-independent; got abc=%q bca=%q", abc, bca)
	}
}

// TestClusterFingerprint_DiffersOnMembershipChange asserts adding or
// removing one member changes the fingerprint, so the survivor-only
// reshape staleness check has the signal it needs.
func TestClusterFingerprint_DiffersOnMembershipChange(t *testing.T) {
	ns := uuid.New()
	now := time.Now().UTC()
	a, b, c := triClusterCandidates(ns, now)
	d := candidateForConsolidate("zeta eta theta iota kappa", ns, now)

	abc := clusterFingerprint([]model.Memory{a, b, c})
	ab := clusterFingerprint([]model.Memory{a, b})
	abcd := clusterFingerprint([]model.Memory{a, b, c, d})

	if abc == ab {
		t.Errorf("removing a member must change the fingerprint; abc=%q ab=%q", abc, ab)
	}
	if abc == abcd {
		t.Errorf("adding a member must change the fingerprint; abc=%q abcd=%q", abc, abcd)
	}
}

// TestStampConsolidateLoad_WritesStampPerMember confirms the load stamp is
// applied to every loaded memory, anchored to that memory's own UpdatedAt
// and persisted via UpdateMetadata (which preserves UpdatedAt).
func TestStampConsolidateLoad_WritesStampPerMember(t *testing.T) {
	writer := &updatingMemoryWriter{}
	phase := NewConsolidationPhase(
		&fakeMemoryReader{},
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return nil },
		func() provider.EmbeddingProvider { return nil },
		noveltySettings(false),
	)

	ns := uuid.New()
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	members := []model.Memory{
		{ID: uuid.New(), NamespaceID: ns, Content: "a", UpdatedAt: now.Add(-2 * time.Hour), Metadata: json.RawMessage(`{}`)},
		{ID: uuid.New(), NamespaceID: ns, Content: "b", UpdatedAt: now.Add(-1 * time.Hour), Metadata: json.RawMessage(`{"existing":"keep"}`)},
		{ID: uuid.New(), NamespaceID: ns, Content: "c (deleted)", UpdatedAt: now, Metadata: json.RawMessage(`{}`)},
	}
	deletedAt := now
	members[2].DeletedAt = &deletedAt

	phase.stampConsolidateLoad(context.Background(), members)

	// Soft-deleted members are skipped: 3 members → 2 stamp writes.
	if len(writer.metadataUpdates) != 2 {
		t.Fatalf("expected 2 UpdateMetadata calls (soft-deleted skipped), got %d", len(writer.metadataUpdates))
	}

	// Each stamp must:
	//   * carry ConsolidationLoadCheckedStampKey equal to the member's UpdatedAt in RFC3339Nano
	//   * preserve any pre-existing metadata fields
	for i, rec := range writer.metadataUpdates {
		idx := i // first two members; deleted member is skipped
		var meta map[string]any
		if err := json.Unmarshal(rec.Metadata, &meta); err != nil {
			t.Fatalf("write %d: unmarshal: %v", i, err)
		}
		stampVal, ok := meta[ConsolidationLoadCheckedStampKey].(string)
		if !ok {
			t.Fatalf("write %d: stamp missing or wrong type: %#v", i, meta[ConsolidationLoadCheckedStampKey])
		}
		want := members[idx].UpdatedAt.UTC().Format(time.RFC3339Nano)
		if stampVal != want {
			t.Errorf("write %d: stamp %q != updated_at %q", i, stampVal, want)
		}
		if rec.ID != members[idx].ID {
			t.Errorf("write %d: target ID %s != member ID %s", i, rec.ID, members[idx].ID)
		}
		if rec.NamespaceID != ns {
			t.Errorf("write %d: namespace %s != %s", i, rec.NamespaceID, ns)
		}
	}

	// The 'b' member's existing key must survive the stamp write.
	var bMeta map[string]any
	if err := json.Unmarshal(writer.metadataUpdates[1].Metadata, &bMeta); err != nil {
		t.Fatalf("unmarshal b: %v", err)
	}
	if bMeta["existing"] != "keep" {
		t.Errorf("existing metadata fields must survive stamp; got %#v", bMeta["existing"])
	}
}
