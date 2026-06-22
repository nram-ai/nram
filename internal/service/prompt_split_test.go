package service

import (
	"strings"
	"testing"
)

// systemPromptKeys are the eight per-phase tunable system-prompt keys. After the
// v0.3.0 clean cut the system prompt is the only tunable LLM template; the
// dynamic data wrapper is hardcoded code, not a setting.
var systemPromptKeys = []string{
	SettingFactSystemPrompt,
	SettingEntitySystemPrompt,
	SettingIngestionDecisionSystemPrompt,
	SettingQueryAugmentSystemPrompt,
	SettingDreamContradictionSystemPrompt,
	SettingDreamSynthesisSystemPrompt,
	SettingDreamAlignmentSystemPrompt,
	SettingDreamNoveltyJudgeSystemPrompt,
}

// TestSystemPromptDefaultsAreVerbFree verifies that every phase registers a
// non-empty system-prompt default and that none contains a printf verb. The
// system prompt is pure static text sent as the system message; the dynamic data
// is injected by a per-phase code wrapper into the user message, so a stray '%'
// in the tunable prompt could only be an authoring mistake (and would corrupt any
// fmt path the value flows through).
func TestSystemPromptDefaultsAreVerbFree(t *testing.T) {
	for _, key := range systemPromptKeys {
		def, ok := GetDefault(key)
		if !ok {
			t.Errorf("%s: no registered default", key)
			continue
		}
		if strings.TrimSpace(def) == "" {
			t.Errorf("%s: default is empty", key)
		}
		if strings.Contains(def, "%") {
			t.Errorf("%s: system prompt must be verb-free static text but contains '%%'", key)
		}
	}
}

// TestSystemPromptDefaultsCarryOutputContract verifies that each phase's output
// contract lives in the system prompt (not the data wrapper, which the clean cut
// moved into code). A regression here would mean the model is no longer told the
// required output shape.
func TestSystemPromptDefaultsCarryOutputContract(t *testing.T) {
	cases := map[string]string{
		SettingFactSystemPrompt:               "Return ONLY valid JSON",
		SettingEntitySystemPrompt:             "Return ONLY valid JSON",
		SettingIngestionDecisionSystemPrompt:  `"operation":"ADD"`,
		SettingQueryAugmentSystemPrompt:       "OUTPUT FORMAT",
		SettingDreamContradictionSystemPrompt: `"contradicts"`,
		SettingDreamSynthesisSystemPrompt:     "Output ONLY the synthesized text",
		SettingDreamAlignmentSystemPrompt:     `"alignment"`,
		SettingDreamNoveltyJudgeSystemPrompt:  `"novel_facts"`,
	}
	for key, marker := range cases {
		def, _ := GetDefault(key)
		if !strings.Contains(def, marker) {
			t.Errorf("%s: system prompt is missing its output contract marker %q", key, marker)
		}
	}
}

// jsonSystemPromptKeys are the seven JSON-returning phases. The eighth phase,
// dream synthesis, emits prose and must NOT carry the minify directive.
var jsonSystemPromptKeys = []string{
	SettingFactSystemPrompt,
	SettingEntitySystemPrompt,
	SettingIngestionDecisionSystemPrompt,
	SettingQueryAugmentSystemPrompt,
	SettingDreamContradictionSystemPrompt,
	SettingDreamAlignmentSystemPrompt,
	SettingDreamNoveltyJudgeSystemPrompt,
}

// TestJSONSystemPromptsRequestMinified verifies every JSON-returning phase
// instructs the model to emit minified output (the output-token saving this
// change exists for), and that the prose synthesis phase does not.
func TestJSONSystemPromptsRequestMinified(t *testing.T) {
	const marker = "minified onto a single line"
	for _, key := range jsonSystemPromptKeys {
		def, _ := GetDefault(key)
		if !strings.Contains(def, marker) {
			t.Errorf("%s: JSON system prompt must request minified output (missing %q)", key, marker)
		}
	}
	if def, _ := GetDefault(SettingDreamSynthesisSystemPrompt); strings.Contains(def, marker) {
		t.Errorf("%s: prose synthesis prompt must NOT carry the minify directive", SettingDreamSynthesisSystemPrompt)
	}
}

// TestRenderExtractionUser verifies the fact/entity data wrapper nonce-fences
// the content as untrusted data. The nonce is random, so assert structure.
func TestRenderExtractionUser(t *testing.T) {
	got := RenderExtractionUser("hello world")
	if !strings.Contains(got, "hello world") {
		t.Errorf("content missing from extraction user message: %q", got)
	}
	if !strings.HasPrefix(got, "<text-") || !strings.Contains(got, "</text-") {
		t.Errorf("expected content fenced in <text-NONCE> tags: %q", got)
	}
}
