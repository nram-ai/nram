package provider

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// fakeRerankProvider is a RerankProvider stub for the usage-recording wrapper
// tests: it returns a fixed response/error without any network or model call.
type fakeRerankProvider struct {
	name string
	resp *RerankResponse
	err  error
}

func (f *fakeRerankProvider) Rerank(_ context.Context, _ string, _ []string) (*RerankResponse, error) {
	return f.resp, f.err
}
func (f *fakeRerankProvider) Name() string { return f.name }

// TestUsageRecordingRerank_AttributesOwnership proves the rerank recorder reads
// org/user/project/namespace off the context (as the recall and ask pipelines
// now stamp it) and writes a non-NULL-ownership token_usage row stamped
// OperationRerank, so analytics' caller-scoped query can see it.
func TestUsageRecordingRerank_AttributesOwnership(t *testing.T) {
	rec := &captureRecorder{}
	inner := &fakeRerankProvider{name: "llama-server", resp: &RerankResponse{
		Scores: []float64{0.1, 0.2},
		Model:  "bge-reranker-v2-m3",
		Usage:  TokenUsage{PromptTokens: 4319, TotalTokens: 4319},
	}}
	w := NewUsageRecordingRerank(inner, rec, nil)

	org, user, proj, ns := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	ctx := WithUsageContext(context.Background(), &model.UsageContext{OrgID: &org, UserID: &user, ProjectID: &proj})
	ctx = WithNamespaceID(ctx, ns)
	ctx = WithOperation(ctx, OperationRerank)

	if _, err := w.Rerank(ctx, "q", []string{"a", "b"}); err != nil {
		t.Fatalf("Rerank: %v", err)
	}
	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if got.Operation != string(OperationRerank) {
		t.Errorf("operation: got %q want %q", got.Operation, OperationRerank)
	}
	if got.OrgID == nil || *got.OrgID != org {
		t.Errorf("org_id: got %v want %v", got.OrgID, org)
	}
	if got.UserID == nil || *got.UserID != user {
		t.Errorf("user_id: got %v want %v", got.UserID, user)
	}
	if got.ProjectID == nil || *got.ProjectID != proj {
		t.Errorf("project_id: got %v want %v", got.ProjectID, proj)
	}
	if got.NamespaceID != ns {
		t.Errorf("namespace_id: got %v want %v", got.NamespaceID, ns)
	}
	if got.TokensInput != 4319 || got.TokensOutput != 0 {
		t.Errorf("tokens: in=%d out=%d, want in=4319 out=0", got.TokensInput, got.TokensOutput)
	}
}

// TestUsageRecordingRerank_ZeroUsageEstimates proves the per-pair tokenizer
// fallback fires when a rerank server omits the usage block: the query is
// counted once per document and completion stays 0 (reranking does no
// generation).
func TestUsageRecordingRerank_ZeroUsageEstimates(t *testing.T) {
	rec := &captureRecorder{}
	inner := &fakeRerankProvider{name: "llama-server", resp: &RerankResponse{
		Scores: []float64{0.1, 0.2},
		Model:  "bge",
		Usage:  TokenUsage{}, // server returned no usage
	}}
	w := NewUsageRecordingRerank(inner, rec, nil)

	query := "hello world relevance query"
	docs := []string{"the first candidate document", "a second, longer candidate document about things"}
	ctx := WithOperation(context.Background(), OperationRerank)
	if _, err := w.Rerank(ctx, query, docs); err != nil {
		t.Fatalf("Rerank: %v", err)
	}

	want := EstimateTokens("bge", query) * len(docs)
	for _, d := range docs {
		want += EstimateTokens("bge", d)
	}
	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if want == 0 {
		t.Fatal("test setup: expected a non-zero estimate")
	}
	if got.TokensInput != want {
		t.Errorf("estimated input: got %d want %d (query once per doc + each doc)", got.TokensInput, want)
	}
	if got.TokensOutput != 0 {
		t.Errorf("output tokens: got %d want 0", got.TokensOutput)
	}
}

// TestUsageRecordingRerank_ReportedUsageUnchanged proves the fallback does NOT
// override measured counts when the server reports usage.
func TestUsageRecordingRerank_ReportedUsageUnchanged(t *testing.T) {
	rec := &captureRecorder{}
	inner := &fakeRerankProvider{name: "openai", resp: &RerankResponse{
		Scores: []float64{0.5},
		Model:  "bge",
		Usage:  TokenUsage{PromptTokens: 4319, TotalTokens: 4319},
	}}
	w := NewUsageRecordingRerank(inner, rec, nil)

	ctx := WithOperation(context.Background(), OperationRerank)
	if _, err := w.Rerank(ctx, "q", []string{"a", "b", "c"}); err != nil {
		t.Fatalf("Rerank: %v", err)
	}
	got := rec.last()
	if got == nil {
		t.Fatal("expected a recorded row")
	}
	if got.TokensInput != 4319 {
		t.Errorf("measured input must be unchanged: got %d want 4319", got.TokensInput)
	}
	if got.TokensOutput != 0 {
		t.Errorf("output tokens: got %d want 0", got.TokensOutput)
	}
}

// TestCrossEncoderRerank_RemapsToInputOrder verifies that scores returned by a
// /v1/rerank server (sorted by score, with index back-references) are remapped
// back to the input document order, and that in-range [0,1] scores pass through
// without a sigmoid.
func TestCrossEncoderRerank_RemapsToInputOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rerank" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		// Results deliberately out of input order, descending by score.
		_ = json.NewEncoder(w).Encode(openaiRerankResponse{
			Results: []openaiRerankResult{
				{Index: 2, RelevanceScore: 0.9},
				{Index: 0, RelevanceScore: 0.5},
				{Index: 1, RelevanceScore: 0.1},
			},
			Usage: openaiUsage{PromptTokens: 42, TotalTokens: 42},
		})
	}))
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{BaseURL: srv.URL, DefaultModel: "bge"})
	resp, err := p.Rerank(context.Background(), "q", []string{"d0", "d1", "d2"})
	if err != nil {
		t.Fatalf("Rerank: %v", err)
	}
	want := []float64{0.5, 0.1, 0.9} // aligned to input order d0,d1,d2
	if len(resp.Scores) != len(want) {
		t.Fatalf("got %d scores, want %d", len(resp.Scores), len(want))
	}
	for i := range want {
		if math.Abs(resp.Scores[i]-want[i]) > 1e-9 {
			t.Errorf("score[%d] = %v, want %v", i, resp.Scores[i], want[i])
		}
	}
	if resp.Usage.PromptTokens != 42 {
		t.Errorf("usage prompt tokens = %d, want 42", resp.Usage.PromptTokens)
	}
}

// TestCrossEncoderRerank_SigmoidNormalizesLogits verifies that when any returned
// score falls outside [0,1] (bge raw logits, possibly negative), a sigmoid is
// applied to all scores, bounding them to (0,1) while preserving order.
func TestCrossEncoderRerank_SigmoidNormalizesLogits(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(openaiRerankResponse{
			Results: []openaiRerankResult{
				{Index: 0, RelevanceScore: -2.0},
				{Index: 1, RelevanceScore: 0.0},
				{Index: 2, RelevanceScore: 5.0},
			},
		})
	}))
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{BaseURL: srv.URL, DefaultModel: "bge"})
	resp, err := p.Rerank(context.Background(), "q", []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("Rerank: %v", err)
	}
	for i, s := range resp.Scores {
		if s <= 0 || s >= 1 {
			t.Errorf("score[%d] = %v not in (0,1) after sigmoid", i, s)
		}
	}
	// Sigmoid is monotonic, so the order -2 < 0 < 5 must be preserved.
	if !(resp.Scores[0] < resp.Scores[1] && resp.Scores[1] < resp.Scores[2]) {
		t.Errorf("sigmoid did not preserve order: %v", resp.Scores)
	}
	// Spot-check the midpoint: sigmoid(0) = 0.5.
	if math.Abs(resp.Scores[1]-0.5) > 1e-9 {
		t.Errorf("sigmoid(0) = %v, want 0.5", resp.Scores[1])
	}
}

// TestCrossEncoderRerank_BareArrayShape verifies the forgiving decode of the
// bare-array /v1/rerank body emitted by a stock SGLang launch server (no router):
// a top-level JSON array of {index, score, meta_info.prompt_tokens} with raw
// (possibly negative) logits and no top-level usage object. Results must remap to
// input order, the sigmoid must bound the logits to (0,1), and Usage.PromptTokens
// must sum the per-item counts.
func TestCrossEncoderRerank_BareArrayShape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rerank" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		// Bare array, descending by score, out of input order, with a negative
		// logit and per-item prompt_tokens (mirrors the live SGLang response).
		_, _ = w.Write([]byte(`[
			{"score": 8.8125, "document": "d1", "index": 1, "meta_info": {"prompt_tokens": 18}},
			{"score": -2.0, "document": "d0", "index": 0, "meta_info": {"prompt_tokens": 18}},
			{"score": -11.0, "document": "d2", "index": 2, "meta_info": {"prompt_tokens": 23}}
		]`))
	}))
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{BaseURL: srv.URL, DefaultModel: "bge"})
	resp, err := p.Rerank(context.Background(), "q", []string{"d0", "d1", "d2"})
	if err != nil {
		t.Fatalf("Rerank: %v", err)
	}
	if len(resp.Scores) != 3 {
		t.Fatalf("got %d scores, want 3", len(resp.Scores))
	}
	// Every score sigmoid-bounded to (0,1) since a logit (8.8125) is outside [0,1].
	for i, s := range resp.Scores {
		if s <= 0 || s >= 1 {
			t.Errorf("score[%d] = %v not in (0,1) after sigmoid", i, s)
		}
	}
	// Remapped to input order d0,d1,d2 from logits -2.0, 8.8125, -11.0: d1 highest, d2 lowest.
	if !(resp.Scores[1] > resp.Scores[0] && resp.Scores[0] > resp.Scores[2]) {
		t.Errorf("expected d1 > d0 > d2, got %v", resp.Scores)
	}
	if resp.Usage.PromptTokens != 59 { // 18 + 18 + 23
		t.Errorf("usage prompt tokens = %d, want 59", resp.Usage.PromptTokens)
	}
	if resp.Usage.TotalTokens != 59 {
		t.Errorf("usage total tokens = %d, want 59", resp.Usage.TotalTokens)
	}
}

func TestCrossEncoderRerank_EmptyDocs(t *testing.T) {
	p := NewOpenAIProvider(OpenAIConfig{BaseURL: "http://unused", DefaultModel: "bge"})
	resp, err := p.Rerank(context.Background(), "q", nil)
	if err != nil {
		t.Fatalf("Rerank(nil): %v", err)
	}
	if len(resp.Scores) != 0 {
		t.Errorf("expected no scores for empty docs, got %v", resp.Scores)
	}
}

// TestProbeRerankMethod dispatches cross_encoder vs judge from the probe status,
// and surfaces auth/other failures as errors rather than guessing a method.
func TestProbeRerankMethod(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		wantMethod string
		wantErr    bool
	}{
		{"200 cross-encoder", http.StatusOK, RerankMethodCrossEncoder, false},
		{"404 judge", http.StatusNotFound, RerankMethodJudge, false},
		{"400 judge", http.StatusBadRequest, RerankMethodJudge, false},
		{"501 judge", http.StatusNotImplemented, RerankMethodJudge, false},
		{"401 error", http.StatusUnauthorized, "", true},
		{"500 error", http.StatusInternalServerError, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status == http.StatusOK {
					_ = json.NewEncoder(w).Encode(openaiRerankResponse{
						Results: []openaiRerankResult{{Index: 0, RelevanceScore: 0.9}},
					})
					return
				}
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			method, err := ProbeRerankMethod(context.Background(), SlotConfig{Type: ProviderTypeLlamaServer, BaseURL: srv.URL})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for status %d, got method %q", tc.status, method)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if method != tc.wantMethod {
				t.Errorf("method = %q, want %q", method, tc.wantMethod)
			}
		})
	}
}

// TestProbeRerankMethod_HonorsTimeout pins the probe's timeout to the slot's
// configured value. The probe hardcoded 10s; a slot pointing at a slow rerank
// endpoint carries its own (larger) timeout, and an unset value keeps the 10s
// default.
func TestProbeRerankMethod_HonorsTimeout(t *testing.T) {
	t.Run("configured timeout bounds a slow probe", func(t *testing.T) {
		// The handler never responds, so the only way the probe returns is its own
		// timeout firing. It parks on an explicit release channel rather than
		// r.Context().Done(): a client-side http.Client.Timeout does not promptly
		// cancel the server request context (the parked handler never notices the
		// dropped connection), which would otherwise hang srv.Close() indefinitely.
		// Cleanup is LIFO, so registering srv.Close first means close(release) runs
		// before it and unblocks the handler.
		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			<-release
		}))
		t.Cleanup(srv.Close)
		t.Cleanup(func() { close(release) })

		start := time.Now()
		_, err := ProbeRerankMethod(context.Background(), SlotConfig{
			Type: ProviderTypeLlamaServer, BaseURL: srv.URL, Timeout: 1,
		})
		elapsed := time.Since(start)
		if err == nil {
			t.Fatal("expected a timeout error from the blocking probe, got nil")
		}
		if elapsed > 5*time.Second {
			t.Errorf("probe took %v; a 1s configured timeout must bail well before the 10s default", elapsed)
		}
	})

	t.Run("unset timeout still probes at the default", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(openaiRerankResponse{
				Results: []openaiRerankResult{{Index: 0, RelevanceScore: 0.9}},
			})
		}))
		defer srv.Close()

		method, err := ProbeRerankMethod(context.Background(), SlotConfig{
			Type: ProviderTypeLlamaServer, BaseURL: srv.URL, Timeout: 0,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if method != RerankMethodCrossEncoder {
			t.Errorf("method = %q, want cross_encoder", method)
		}
	})
}

// TestParseJudgeScore pins the parser to the judge's own instruction: output the
// number and nothing else. The cases that matter most are the ones that must NOT
// produce a score. An earlier scan-for-the-first-number form mined whichever digit
// appeared first, so a completion that merely mentions a document index, and a
// reasoning trace truncated by the token cap, both scored a confident 1.0 and
// marked every candidate maximally relevant with no error.
func TestParseJudgeScore(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want float64
		ok   bool
	}{
		{"bare decimal", "0.8", 0.8, true},
		{"bare one", "1", 1, true},
		{"bare zero", "0", 0, true},
		{"trailing period", "0.75.", 0.75, true},
		{"spaced trailing period", "0.75 .", 0.75, true},
		{"surrounding whitespace", "  0.6\n", 0.6, true},
		{"clamp above", "1.5", 1, true},
		{"clamp below", "-0.3", 0, true},
		{"closed thinking block then number", "<think>weighing it</think>0.9", 0.9, true},
		{"closed thinking block then spaced number", "<think>hmm</think>\n 0.25 ", 0.25, true},

		// Must not yield a score.
		{"prose around the number", "Score: 0.42", 0, false},
		{"document index before the score", "Document 1: 0.9", 0, false},
		{"unclosed thinking trace", "<think>I'd rate this 3 stars out of", 0, false},
		{"unclosed thinking trace, no digits", "<think>Let me consider the query", 0, false},
		{"non-numeric noise", "no number", 0, false},
		{"empty", "", 0, false},
		{"period only", ".", 0, false},
		{"not-a-number literal", "NaN", 0, false},
		{"infinity literal", "Inf", 0, false},
		{"two numbers", "0.9 0.1", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseJudgeScore(tc.in)
			if ok != tc.ok {
				t.Fatalf("parseJudgeScore(%q) ok = %v, want %v (score %v)", tc.in, ok, tc.ok, got)
			}
			if ok && math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("parseJudgeScore(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestJudgeReranker_NoParseableScore is the fail-before/pass-after proof for the
// defect: a completion carrying a stray integer used to score 1.0, silently
// ranking an unjudged document as maximally relevant. It must now abort the rerank
// with a typed error so the caller keeps its prior order.
func TestJudgeReranker_NoParseableScore(t *testing.T) {
	for _, content := range []string{"Document 1: 0.9", "<think>I'd rate this 3 stars out of"} {
		t.Run(content, func(t *testing.T) {
			j := &judgeReranker{llm: &judgeStubLLM{replies: []string{content}}}
			resp, err := j.Rerank(context.Background(), "q", []string{"a"})
			if err == nil {
				t.Fatalf("Rerank returned scores %v, want an error", resp.Scores)
			}
			noScore, ok := errors.AsType[*NoJudgeScoreError](err)
			if !ok {
				t.Fatalf("err = %v (%T), want *NoJudgeScoreError", err, err)
			}
			if noScore.Doc != 0 {
				t.Errorf("Doc = %d, want 0", noScore.Doc)
			}
			if noScore.Content != content {
				t.Errorf("Content = %q, want the raw completion %q", noScore.Content, content)
			}
		})
	}
}

// judgeStubLLM is a minimal LLMProvider returning a fixed content per call, used to
// drive the judge reranker without a network. It records the last request so a test
// can assert the prompt/params the judge passed.
type judgeStubLLM struct {
	replies []string
	calls   int
	lastReq *CompletionRequest
}

func (s *judgeStubLLM) Complete(_ context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	s.lastReq = req
	r := s.replies[s.calls%len(s.replies)]
	s.calls++
	return &CompletionResponse{Content: r, Model: "stub", Usage: TokenUsage{PromptTokens: 3, CompletionTokens: 1, TotalTokens: 4}}, nil
}
func (s *judgeStubLLM) Name() string     { return "stub" }
func (s *judgeStubLLM) Models() []string { return []string{"stub"} }

func TestJudgeReranker_ScoresEachDoc(t *testing.T) {
	j := &judgeReranker{llm: &judgeStubLLM{replies: []string{"0.2", "0.9", "0.5"}}}
	resp, err := j.Rerank(context.Background(), "q", []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("judge Rerank: %v", err)
	}
	want := []float64{0.2, 0.9, 0.5}
	for i := range want {
		if math.Abs(resp.Scores[i]-want[i]) > 1e-9 {
			t.Errorf("score[%d] = %v, want %v", i, resp.Scores[i], want[i])
		}
	}
	// Usage is summed across the three per-doc completions.
	if resp.Usage.TotalTokens != 12 {
		t.Errorf("total tokens = %d, want 12", resp.Usage.TotalTokens)
	}
}

// TestJudgeReranker_UsesContextConfig proves the judge reads the context-stamped
// RerankJudgeConfig (prompt, max tokens, temperature) the service resolves from
// settings, and falls back to defaults when nothing is stamped.
func TestJudgeReranker_UsesContextConfig(t *testing.T) {
	// Stamped config is honored.
	stub := &judgeStubLLM{replies: []string{"0.5"}}
	j := &judgeReranker{llm: stub}
	ctx := WithRerankJudgeConfig(context.Background(), RerankJudgeConfig{
		SystemPrompt: "CUSTOM JUDGE PROMPT", MaxTokens: 7, Temperature: 0.3,
	})
	if _, err := j.Rerank(ctx, "q", []string{"d"}); err != nil {
		t.Fatal(err)
	}
	if stub.lastReq.MaxTokens != 7 {
		t.Errorf("MaxTokens = %d, want 7 (from ctx)", stub.lastReq.MaxTokens)
	}
	if stub.lastReq.Temperature != 0.3 {
		t.Errorf("Temperature = %v, want 0.3 (from ctx)", stub.lastReq.Temperature)
	}
	// The system half is guarded (untrusted-data directive prepended) so an
	// injected query/document cannot pose as an instruction.
	if stub.lastReq.Messages[0].Content != GuardedSystem("CUSTOM JUDGE PROMPT") {
		t.Errorf("system prompt = %q, want the guarded ctx-stamped one", stub.lastReq.Messages[0].Content)
	}
	// The query and document are nonce-fenced, not passed as the old bare
	// "Query:"/"Document:" literals.
	if u := stub.lastReq.Messages[1].Content; !strings.Contains(u, "<query-") || !strings.Contains(u, "<document-") {
		t.Errorf("user message not fenced: %q", u)
	}

	// No stamp -> defaults applied.
	stub2 := &judgeStubLLM{replies: []string{"0.5"}}
	j2 := &judgeReranker{llm: stub2}
	if _, err := j2.Rerank(context.Background(), "q", []string{"d"}); err != nil {
		t.Fatal(err)
	}
	if stub2.lastReq.MaxTokens != defaultRerankJudgeMaxTokens {
		t.Errorf("MaxTokens = %d, want default %d", stub2.lastReq.MaxTokens, defaultRerankJudgeMaxTokens)
	}
	if stub2.lastReq.Messages[0].Content != GuardedSystem(defaultRerankJudgeSystem) {
		t.Errorf("system prompt = %q, want guarded default", stub2.lastReq.Messages[0].Content)
	}
}

// TestCreateRerankProvider verifies method dispatch and the cross-encoder wire
// requirement.
func TestCreateRerankProvider(t *testing.T) {
	// Empty method defaults to cross_encoder.
	if rp, err := createRerankProvider(SlotConfig{Type: ProviderTypeLlamaServer, BaseURL: "http://x", Model: "bge"}); err != nil || rp == nil {
		t.Fatalf("default cross_encoder: rp=%v err=%v", rp, err)
	}
	// Judge method builds a judge over any LLM type.
	if rp, err := createRerankProvider(SlotConfig{Type: ProviderTypeOpenAI, BaseURL: "http://x", Model: "gpt", RerankMethod: RerankMethodJudge}); err != nil || rp == nil {
		t.Fatalf("judge: rp=%v err=%v", rp, err)
	}
	// Cross-encoder requires an openai-compatible wire type.
	if _, err := createRerankProvider(SlotConfig{Type: ProviderTypeAnthropic, BaseURL: "http://x", RerankMethod: RerankMethodCrossEncoder}); err == nil {
		t.Fatalf("expected error for anthropic cross_encoder")
	}
}

// TestCreateRerankProvider_ThinkingPropagation pins which rerank method the
// thinking toggle reaches. The judge generates, so it must honor the slot's
// setting; the cross-encoder does not generate, so the knob must never reach it.
// This asymmetry is why the admin UI shows the toggle only for a judge slot: on a
// cross-encoder it is a dead control that stores a value nothing emits.
func TestCreateRerankProvider_ThinkingPropagation(t *testing.T) {
	thinkingOn := false

	rp, err := createRerankProvider(SlotConfig{
		Type: ProviderTypeSGLang, BaseURL: "http://x", Model: "qwen",
		RerankMethod: RerankMethodJudge, DisableThinking: &thinkingOn,
	})
	if err != nil {
		t.Fatalf("judge: %v", err)
	}
	j, ok := rp.(*judgeReranker)
	if !ok {
		t.Fatalf("judge method built %T, want *judgeReranker", rp)
	}
	op, ok := j.llm.(*OpenAIProvider)
	if !ok {
		t.Fatalf("judge wraps %T, want *OpenAIProvider", j.llm)
	}
	if op.config.DisableThinking {
		t.Error("judge DisableThinking = true, want false propagated from the slot")
	}

	// And the resolver's default (nil pointer) reaches the judge as thinking-off.
	rp, err = createRerankProvider(SlotConfig{
		Type: ProviderTypeSGLang, BaseURL: "http://x", Model: "qwen",
		RerankMethod: RerankMethodJudge,
	})
	if err != nil {
		t.Fatalf("judge default: %v", err)
	}
	if op := rp.(*judgeReranker).llm.(*OpenAIProvider); !op.config.DisableThinking {
		t.Error("judge DisableThinking = false for an unset toggle, want true (thinkingDisabled default)")
	}

	// The cross-encoder never receives the knob, whatever the slot says.
	rp, err = createRerankProvider(SlotConfig{
		Type: ProviderTypeSGLang, BaseURL: "http://x", Model: "bge",
		RerankMethod: RerankMethodCrossEncoder, DisableThinking: &thinkingOn,
	})
	if err != nil {
		t.Fatalf("cross_encoder: %v", err)
	}
	if op := rp.(*OpenAIProvider); op.config.DisableThinking {
		t.Error("cross_encoder DisableThinking = true, want the knob never set on a non-generative path")
	}
}

// calibrationStubLLM answers by which fixture document it is being asked to score,
// read off the fenced user message rather than a call counter (a rung that aborts
// mid-pair would desynchronize a counter). reply lets a test make a specific rung
// of the ladder succeed or fail, mirroring newJudgeRerankTestServer's shape in the
// admin store tests so there is one stub idiom to learn.
type calibrationStubLLM struct {
	// reply answers a rung, given its token cap and whether the fixture document
	// being scored is the relevant one.
	reply func(maxTokens int, relevant bool) string
	// attempts records the token cap of each rung that ran, in order.
	attempts []int
	// thinking records each rung's DisableThinking, parallel to attempts.
	thinking []bool
}

func (s *calibrationStubLLM) Complete(_ context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	relevant := strings.Contains(req.Messages[1].Content, judgeCalibrationRelevant)
	if relevant {
		// The relevant doc is always a rung's first call, so this records each
		// rung exactly once even when the rung aborts on an unparseable score.
		s.attempts = append(s.attempts, req.MaxTokens)
	}
	return &CompletionResponse{Content: s.reply(req.MaxTokens, relevant), Model: "stub", Usage: TokenUsage{TotalTokens: 4}}, nil
}
func (s *calibrationStubLLM) Name() string     { return "stub" }
func (s *calibrationStubLLM) Models() []string { return []string{"stub"} }

// calibrateWithStub drives the real calibrateJudge sweep, injecting a builder that
// hands back a judge over the stub and recording what each rung was built with.
func calibrateWithStub(t *testing.T, stub *calibrationStubLLM, candidates []JudgeCalibrationCandidate) *JudgeCalibrationResult {
	t.Helper()
	build := func(cfg SlotConfig) (RerankProvider, error) {
		if cfg.RerankMethod != RerankMethodJudge {
			t.Errorf("calibration built method %q, want %q", cfg.RerankMethod, RerankMethodJudge)
		}
		if cfg.DisableThinking == nil {
			t.Error("calibration left DisableThinking nil, want it stamped per candidate")
		} else {
			stub.thinking = append(stub.thinking, *cfg.DisableThinking)
		}
		return &judgeReranker{llm: stub}, nil
	}
	res, err := calibrateJudge(context.Background(), build, SlotConfig{Type: ProviderTypeSGLang, BaseURL: "http://x", Model: "m"},
		defaultRerankJudgeSystem, 0, candidates)
	if err != nil {
		t.Fatalf("calibrateJudge: %v", err)
	}
	return res
}

// TestCalibrateJudge_PicksFirstDiscriminatingRung proves the sweep stops at the
// first configuration that both parses and separates the fixture, rather than
// running the whole ladder.
func TestCalibrateJudge_PicksFirstDiscriminatingRung(t *testing.T) {
	stub := &calibrationStubLLM{reply: func(maxTokens int, relevant bool) string {
		// At the configured cap a reasoning trace truncates and no number is
		// emitted; raising the cap lets the model answer.
		if maxTokens < 32 {
			return "<think>Considering whether"
		}
		if relevant {
			return "1.0"
		}
		return "0.0"
	}}
	ladder := []JudgeCalibrationCandidate{
		{DisableThinking: true, MaxTokens: 16},
		{DisableThinking: true, MaxTokens: 32},
		{DisableThinking: false, MaxTokens: 256},
	}
	res := calibrateWithStub(t, stub, ladder)

	if !res.Calibrated {
		t.Fatalf("not calibrated: %s", res.Diagnosis)
	}
	if res.Winner.MaxTokens != 32 || !res.Winner.DisableThinking {
		t.Errorf("winner = %+v, want {DisableThinking:true MaxTokens:32}", res.Winner)
	}
	if res.RelevantScore <= res.IrrelevantScore {
		t.Errorf("scores %v/%v do not discriminate", res.RelevantScore, res.IrrelevantScore)
	}
	// The 256 rung must never have run: the ladder stops at the first winner.
	if got := stub.attempts; len(got) != 2 || got[0] != 16 || got[1] != 32 {
		t.Errorf("attempted caps = %v, want [16 32] (stop at first winner)", got)
	}
	// Both rungs that ran were built thinking-off, the preferred state.
	for i, disabled := range stub.thinking {
		if !disabled {
			t.Errorf("rung %d built with thinking on, want off before the last rung", i)
		}
	}
}

// TestCalibrateJudge_NonNumericIsCrossEncoderDiagnosis covers the note's live
// Ollama case: a cross-encoder driven down the chat path emits token noise, which
// must be reported as "not a generative judge" rather than scored.
func TestCalibrateJudge_NonNumericIsCrossEncoderDiagnosis(t *testing.T) {
	stub := &calibrationStubLLM{reply: func(_ int, relevant bool) string {
		if relevant {
			return "query passage relevance yes"
		}
		return "query passage relevance no"
	}}
	res := calibrateWithStub(t, stub, []JudgeCalibrationCandidate{
		{DisableThinking: true, MaxTokens: 16},
		{DisableThinking: true, MaxTokens: 32},
	})

	if res.Calibrated {
		t.Fatal("calibrated on non-numeric noise, want failure")
	}
	if !strings.Contains(res.Diagnosis, "cross-encoder") {
		t.Errorf("diagnosis = %q, want it to name the cross-encoder mis-detection", res.Diagnosis)
	}
	if res.LastOutput == "" {
		t.Error("LastOutput empty, want the raw completion echoed for the operator")
	}
}

// TestCalibrateJudge_FlatScoresAreNotCalibrated covers a model that emits valid
// numbers but the same one every time: parseable, useless, and previously
// indistinguishable from a working judge.
func TestCalibrateJudge_FlatScoresAreNotCalibrated(t *testing.T) {
	stub := &calibrationStubLLM{reply: func(int, bool) string { return "1.0" }}
	res := calibrateWithStub(t, stub, []JudgeCalibrationCandidate{{DisableThinking: true, MaxTokens: 16}})

	if res.Calibrated {
		t.Fatal("calibrated on flat scores, want failure")
	}
	if !strings.Contains(res.Diagnosis, "discriminate") {
		t.Errorf("diagnosis = %q, want it to name the lack of discrimination", res.Diagnosis)
	}
}

// TestCalibrateJudge_NoCandidates guards the empty ladder.
func TestCalibrateJudge_NoCandidates(t *testing.T) {
	if _, err := CalibrateJudge(context.Background(), SlotConfig{}, "p", 0, nil); err == nil {
		t.Fatal("expected an error for an empty candidate ladder")
	}
}
