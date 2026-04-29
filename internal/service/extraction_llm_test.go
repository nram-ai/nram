package service

import (
	"strings"
	"testing"
)

// Truncated payloads sourced from the production failure modes the unified
// helper was built to handle. Hand-trimmed to match the byte length the
// real LLM emitted before max_tokens cut, with one degenerate-loop sample.

// truncatedFactArray simulates a fact-extraction response cut mid-string by
// max_tokens. The first three entries are clean; the fourth is incomplete.
const truncatedFactArray = `[
  {"content":"Anthropic spent $1.2M on lobbying Q3 2025","confidence":0.92,"tags":["anthropic","lobbying"]},
  {"content":"OpenAI spent $1.6M on lobbying Q3 2025","confidence":0.91,"tags":["openai","lobbying"]},
  {"content":"NSA flagged Mythos for procurement review","confidence":0.83,"tags":["nsa","mythos"]},
  {"content":"CISA does not have access to`

// degenerateLoopFactArray simulates the small-qwen attractor pattern:
// the model emits a few legitimate facts then loops a cluster.
const degenerateLoopFactArray = `[
  {"content":"Anthropic outspent OpenAI on lobbying","confidence":0.9,"tags":["anthropic"]},
  {"content":"Topics include AI procurement","confidence":0.85,"tags":["procurement"]},
  {"content":"NSA using Mythos","confidence":0.8,"tags":["nsa"]},
  {"content":"Anthropic outspent OpenAI on lobbying","confidence":0.9,"tags":["anthropic"]},
  {"content":"Topics include AI procurement","confidence":0.85,"tags":["procurement"]},
  {"content":"Anthropic outspent OpenAI on lobbying","confidence":0.9,"tags":["anthropic"]},
  {"content":"NSA using Mythos","confidence":0.8,"tags":["nsa"]},
  {"content":"Anthropic outspent OpenAI on lobbying","confidence":0.9,"tags":["anthropic"]}
]`

// truncatedFactWrapper simulates the wrapper-shape variant of truncation.
const truncatedFactWrapper = `{
  "facts": [
    {"content":"Fact one","confidence":0.9},
    {"content":"Fact two","confidence":0.8},
    {"content":"Fact three with trunc`

// truncatedEntityResponse: entities cleanly close, relationships truncates.
const truncatedEntityResponse = `{
  "entities": [
    {"name":"Anthropic","type":"organization","properties":{}},
    {"name":"OpenAI","type":"organization","properties":{}}
  ],
  "relationships": [
    {"source":"Anthropic","target":"OpenAI","relation":"competes_with","weight":0.9},
    {"source":"Anthropic","target":"NS`

func TestParseFacts_RecoverArrayPrefix(t *testing.T) {
	facts, partial, err := parseFacts(truncatedFactArray)
	if err != nil {
		t.Fatalf("recovery should not error: %v", err)
	}
	if !partial {
		t.Errorf("expected PartialRecovery=true on truncated input")
	}
	if len(facts) != 3 {
		t.Fatalf("expected 3 recovered facts, got %d", len(facts))
	}
	wantTexts := []string{
		"Anthropic spent $1.2M on lobbying Q3 2025",
		"OpenAI spent $1.6M on lobbying Q3 2025",
		"NSA flagged Mythos for procurement review",
	}
	for i, want := range wantTexts {
		if facts[i].Content != want {
			t.Errorf("fact[%d].Content = %q, want %q", i, facts[i].Content, want)
		}
		if facts[i].Fact != want {
			t.Errorf("fact[%d].Fact = %q, want %q (normalize should set both)", i, facts[i].Fact, want)
		}
	}
}

func TestParseFacts_DegenerateLoopDeduped(t *testing.T) {
	// Clean parse, but the array contains the degenerate-loop pattern. The
	// current contract dedupes only on recovery — clean parses preserve
	// duplicates so an operator can see the pattern. Verify that contract.
	facts, partial, err := parseFacts(degenerateLoopFactArray)
	if err != nil {
		t.Fatalf("clean parse should not error: %v", err)
	}
	if partial {
		t.Errorf("clean parse should not set PartialRecovery")
	}
	if len(facts) != 8 {
		t.Errorf("expected all 8 facts preserved on clean parse (dedupe is recovery-only); got %d", len(facts))
	}
}

func TestParseFacts_RecoverWrapperPrefix(t *testing.T) {
	facts, partial, err := parseFacts(truncatedFactWrapper)
	if err != nil {
		t.Fatalf("wrapper recovery should not error: %v", err)
	}
	if !partial {
		t.Errorf("expected PartialRecovery=true")
	}
	if len(facts) != 2 {
		t.Fatalf("expected 2 recovered facts (the third is mid-string), got %d", len(facts))
	}
}

func TestParseFacts_RecoveryDedupesAcrossDuplicates(t *testing.T) {
	// Truncated body where the recovered prefix already contains the
	// degenerate-loop pattern. Recovery must dedupe, leaving the unique
	// statements.
	loopAndTruncate := `[
  {"content":"A unique fact","confidence":0.9},
  {"content":"Looping statement","confidence":0.8},
  {"content":"Looping statement","confidence":0.8},
  {"content":"Looping statement","confidence":0.8},
  {"content":"Another unique","confidence":0.85},
  {"content":"Looping state`
	facts, partial, err := parseFacts(loopAndTruncate)
	if err != nil {
		t.Fatalf("recovery should not error: %v", err)
	}
	if !partial {
		t.Errorf("expected PartialRecovery=true")
	}
	if len(facts) != 3 {
		t.Errorf("expected 3 deduped facts, got %d", len(facts))
	}
}

func TestParseEntities_RecoverPartial(t *testing.T) {
	result, partial, err := parseEntities(truncatedEntityResponse)
	if err != nil {
		t.Fatalf("entity recovery should not error: %v", err)
	}
	if !partial {
		t.Errorf("expected PartialRecovery=true")
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if len(result.Entities) != 2 {
		t.Errorf("expected 2 entities recovered, got %d", len(result.Entities))
	}
	if len(result.Relationships) != 1 {
		t.Errorf("expected 1 relationship recovered (second is mid-string), got %d", len(result.Relationships))
	}
}

func TestParseFacts_NormalizationWritesBothFields(t *testing.T) {
	// Input uses "fact" key (legacy/alternate); normalize must populate
	// both Fact and Content so callers reading either field see the value.
	input := `[{"fact":"Sample","confidence":0.9}]`
	facts, _, err := parseFacts(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact, got %d", len(facts))
	}
	if facts[0].Fact != "Sample" || facts[0].Content != "Sample" {
		t.Errorf("normalize should write to both Fact and Content; got Fact=%q Content=%q",
			facts[0].Fact, facts[0].Content)
	}
}

func TestExtractionFailure_ErrorAndAs(t *testing.T) {
	fail := &ExtractionFailure{
		Phase:        ExtractionPhaseFact,
		Reason:       ExtractionReasonParseFailed,
		Detail:       "garbled",
		FinishReason: "length",
	}
	got := fail.Error()
	if !strings.Contains(got, ExtractionPhaseFact) || !strings.Contains(got, ExtractionReasonParseFailed) {
		t.Errorf("Error() should mention phase and reason; got %q", got)
	}

	// errors.As round-trip via the helper.
	var dst *ExtractionFailure
	if asFail, ok := AsExtractionFailure(fail); !ok || asFail != fail {
		t.Errorf("AsExtractionFailure should return the *ExtractionFailure verbatim")
	}
	_ = dst
}
