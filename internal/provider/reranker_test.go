package provider

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

func TestParseJudgeScore(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"0.8", 0.8},
		{"Score: 0.42", 0.42},
		{"1", 1},
		{"0", 0},
		{"1.5", 1},       // clamp above
		{"-0.3", 0},      // clamp below
		{"no number", 0}, // unparseable -> 0
		{"", 0},
	}
	for _, tc := range cases {
		if got := parseJudgeScore(tc.in); math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("parseJudgeScore(%q) = %v, want %v", tc.in, got, tc.want)
		}
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
