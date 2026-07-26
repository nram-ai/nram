package service

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/provider"
)

// structuredFakeLLM returns distinct content for the JSON-mode structured call
// and the plain prose fallback, and counts each, so tests can prove which path
// synthesize took.
type structuredFakeLLM struct {
	jsonContent  string
	proseContent string
	jsonCalls    int
	proseCalls   int
}

func (f *structuredFakeLLM) Complete(_ context.Context, req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	if req.JSONMode {
		f.jsonCalls++
		return &provider.CompletionResponse{Content: f.jsonContent}, nil
	}
	f.proseCalls++
	return &provider.CompletionResponse{Content: f.proseContent}, nil
}
func (*structuredFakeLLM) Name() string     { return "structfake" }
func (*structuredFakeLLM) Models() []string { return []string{"structfake"} }

func structuredTestNeighborhood() []neighborMemory {
	return []neighborMemory{{shortID: "m1", memoryID: uuid.New(), content: "some memory content", projectSlug: "p"}}
}

// TestSynthesize_StructuredPath covers how synthesize routes between the
// JSON-mode structured call and the prose fallback: assembling supported and
// unsupported parts, declining with the exact sentinel when no part is
// supported, failing soft to prose on malformed JSON, and skipping the JSON
// call entirely when the feature is disabled. wantJSONCalls/wantProseCalls
// assert which path actually ran.
func TestSynthesize_StructuredPath(t *testing.T) {
	cases := []struct {
		name               string
		structuredDisabled bool
		jsonContent        string
		proseContent       string
		wantAns            string
		wantJSONCalls      int
		wantProseCalls     int
	}{
		{
			name: "assembles supported and unsupported parts",
			jsonContent: `{"parts":[` +
				`{"part":"transport","supported":true,"answer":"gRPC over TCP [m1]."},` +
				`{"part":"retry","supported":false,"answer":"The retry count is not specified in the memories."}` +
				`]}`,
			wantAns:        "gRPC over TCP [m1]. The retry count is not specified in the memories.",
			wantJSONCalls:  1,
			wantProseCalls: 0,
		},
		{
			name: "all absent returns sentinel",
			jsonContent: `{"parts":[` +
				`{"part":"a","supported":false,"answer":"a is not specified in the memories."},` +
				`{"part":"b","supported":false,"answer":"b is not specified in the memories."}` +
				`]}`,
			wantAns:        askNotInNeighborhood,
			wantJSONCalls:  1,
			wantProseCalls: 0,
		},
		{
			name:           "malformed json falls back to prose",
			jsonContent:    "not json at all",
			proseContent:   "Prose fallback answer [m1].",
			wantAns:        "Prose fallback answer [m1].",
			wantJSONCalls:  1,
			wantProseCalls: 1,
		},
		{
			name:               "structured disabled skips json",
			structuredDisabled: true,
			jsonContent:        `{"parts":[{"part":"x","supported":true,"answer":"x [m1]."}]}`,
			proseContent:       "Prose only.",
			wantAns:            "Prose only.",
			wantJSONCalls:      0,
			wantProseCalls:     1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := newMockSettingsRepo()
			if tc.structuredDisabled {
				repo.put(SettingAskSynthesisStructuredEnabled, "global", "false")
			}
			svc := &AskService{settings: NewSettingsService(repo)}
			llm := &structuredFakeLLM{jsonContent: tc.jsonContent, proseContent: tc.proseContent}
			ans, ok := svc.synthesize(context.Background(), llm, &AskRequest{Query: "q"}, structuredTestNeighborhood())
			if !ok {
				t.Fatalf("expected ok, got ans=%q", ans)
			}
			if ans != tc.wantAns {
				t.Fatalf("answer = %q, want %q", ans, tc.wantAns)
			}
			if llm.jsonCalls != tc.wantJSONCalls || llm.proseCalls != tc.wantProseCalls {
				t.Fatalf("call counts: got json=%d prose=%d, want json=%d prose=%d",
					llm.jsonCalls, llm.proseCalls, tc.wantJSONCalls, tc.wantProseCalls)
			}
		})
	}
}

// TestAsk_StructuredFullPathGroundsCitations exercises the FULL Ask pipeline with
// a successful structured parse: the assembled answer's neighborhood [shortID]
// citation must survive renumberCitations (-> [1]) so the grounding guard does not
// nuke a grounded answer to the sentinel, and sources/confidence must populate.
// Decomposition is disabled so the JSON response is only consumed by synthesis.
func TestAsk_StructuredFullPathGroundsCitations(t *testing.T) {
	rc := &askFakeRecaller{resp: &RecallResponse{Memories: []RecallResult{askCandidate("aaaaaaaa", "work", 0.8)}}}
	repo := newMockSettingsRepo()
	repo.put(SettingAskDecompositionEnabled, "global", "false")
	settings := NewSettingsService(repo) // structured enabled by default
	// aaaaaaaa is the real candidate short id; the model cites it inline.
	llm := askFakeLLM{content: `{"parts":[{"part":"transport","supported":true,"answer":"The widget uses gRPC [aaaaaaaa]."}]}`}
	svc := newAskSvc(t, rc, &askFakeMem{}, askTestProjects(), llm, settings)

	resp, err := svc.Ask(context.Background(), &AskRequest{Query: "what transport?", OwnerNamespaceID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	if resp.SynthesisMeta.SynthesisFailed {
		t.Fatal("structured synthesis should not fail")
	}
	if resp.Answer == askNotInNeighborhood {
		t.Fatalf("grounded structured answer was nuked to the sentinel: %q", resp.Answer)
	}
	if !strings.Contains(resp.Answer, "gRPC") {
		t.Errorf("answer lost the grounded fact: %q", resp.Answer)
	}
	if !strings.Contains(resp.Answer, "[1]") {
		t.Errorf("neighborhood citation should renumber to [1]: %q", resp.Answer)
	}
	if strings.Contains(resp.Answer, "aaaaaaaa") {
		t.Errorf("raw short id should not survive in the answer: %q", resp.Answer)
	}
	if len(resp.Sources) != 1 || resp.Sources[0].Citation != 1 {
		t.Errorf("expected one cited source with citation 1, got %+v", resp.Sources)
	}
	if resp.Confidence <= 0 {
		t.Errorf("expected positive confidence on a grounded structured answer, got %v", resp.Confidence)
	}
}
