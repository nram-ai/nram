package api

import (
	"encoding/json"
	"testing"
)

// TestParseTestFactResponse covers the Test-surface fact parser, which decodes
// into service.ExtractedFact and normalises to the "content" key the UI renders.
// It guards the dual-key handling (the prompt asks for "content"; some models
// emit "fact") and the fact-over-content precedence that mirrors the runtime
// extractor.
func TestParseTestFactResponse(t *testing.T) {
	type outFact struct {
		Content    string   `json:"content"`
		Confidence float64  `json:"confidence"`
		Tags       []string `json:"tags"`
	}
	decode := func(t *testing.T, parsed any) []outFact {
		t.Helper()
		b, err := json.Marshal(parsed)
		if err != nil {
			t.Fatalf("re-marshal parsed: %v", err)
		}
		var out []outFact
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("decode normalised facts: %v", err)
		}
		return out
	}

	t.Run("content key populates the preview", func(t *testing.T) {
		raw := `[{"content":"John works at Acme","confidence":1.0,"tags":["employment"]}]`
		parsed, err := parseTestFactResponse(raw)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		facts := decode(t, parsed)
		if len(facts) != 1 || facts[0].Content != "John works at Acme" {
			t.Fatalf("content not carried through: %+v", facts)
		}
		if len(facts[0].Tags) != 1 || facts[0].Tags[0] != "employment" {
			t.Errorf("tags not carried through: %+v", facts[0].Tags)
		}
	})

	t.Run("fact key is accepted and wins over content", func(t *testing.T) {
		// "fact" present alongside "content": precedence must match the runtime
		// extractor (service.ExtractedFact.text prefers fact).
		raw := `[{"fact":"primary","content":"secondary","confidence":0.5}]`
		parsed, err := parseTestFactResponse(raw)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		facts := decode(t, parsed)
		if len(facts) != 1 || facts[0].Content != "primary" {
			t.Fatalf("fact should win over content, got %+v", facts)
		}
	})

	t.Run("markdown fences and surrounding prose are tolerated", func(t *testing.T) {
		raw := "Here you go:\n```json\n[{\"content\":\"a fact\",\"confidence\":0.9}]\n```"
		parsed, err := parseTestFactResponse(raw)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		facts := decode(t, parsed)
		if len(facts) != 1 || facts[0].Content != "a fact" {
			t.Fatalf("fenced/prose array not recovered: %+v", facts)
		}
	})

	t.Run("non-array output is an error", func(t *testing.T) {
		if _, err := parseTestFactResponse(`{"not":"an array"}`); err == nil {
			t.Error("expected error for non-array response")
		}
	})
}
