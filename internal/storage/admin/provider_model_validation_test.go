package admin

import (
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/api"
)

func TestValidateConfiguredModel(t *testing.T) {
	tests := []struct {
		name      string
		model     string
		served    []string
		wantWarn  bool
		wantParts []string // substrings the warning must contain when wantWarn
	}{
		{
			name:     "model served: no warning",
			model:    "qwen3:8b",
			served:   []string{"qwen3:8b", "llama3:latest"},
			wantWarn: false,
		},
		{
			name:      "model absent: warning names model and served ids",
			model:     "qwen3:4b",
			served:    []string{"qwen3:8b", "llama3:latest"},
			wantWarn:  true,
			wantParts: []string{`"qwen3:4b"`, "qwen3:8b"},
		},
		{
			name:     "ollama implicit :latest matches tagless config",
			model:    "qwen3",
			served:   []string{"qwen3:latest"},
			wantWarn: false,
		},
		{
			name:     "ollama implicit :latest matches tagless server id",
			model:    "qwen3:latest",
			served:   []string{"qwen3"},
			wantWarn: false,
		},
		{
			name:     "openai-style exact match: no warning",
			model:    "text-embedding-3-small",
			served:   []string{"text-embedding-3-small", "gpt-4o"},
			wantWarn: false,
		},
		{
			name:     "empty served list (unreachable/non-enumerable): no warning",
			model:    "qwen3:4b",
			served:   nil,
			wantWarn: false,
		},
		{
			name:     "blank model: no warning",
			model:    "",
			served:   []string{"qwen3:8b"},
			wantWarn: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateConfiguredModel(api.ProviderSlotConfig{
				Model: tt.model,
				URL:   "http://host:11434",
			}, tt.served)
			if tt.wantWarn && got == "" {
				t.Fatalf("expected a warning, got none")
			}
			if !tt.wantWarn && got != "" {
				t.Fatalf("expected no warning, got %q", got)
			}
			for _, part := range tt.wantParts {
				if !strings.Contains(got, part) {
					t.Errorf("warning %q missing expected substring %q", got, part)
				}
			}
		})
	}
}

func TestValidateConfiguredModelSamplesLongLists(t *testing.T) {
	served := []string{"a", "b", "c", "d", "e", "f", "g"}
	got := validateConfiguredModel(api.ProviderSlotConfig{Model: "zzz", URL: "http://host"}, served)
	if got == "" {
		t.Fatal("expected a warning for an absent model")
	}
	// Only the first five served ids are listed, with an ellipsis for the rest.
	if strings.Contains(got, "f") || strings.Contains(got, "g") {
		t.Errorf("warning should cap the served-id sample at five, got %q", got)
	}
	if !strings.Contains(got, "…") {
		t.Errorf("warning should mark the truncated sample with an ellipsis, got %q", got)
	}
}

func TestNormalizeModelTag(t *testing.T) {
	cases := map[string]string{
		"qwen3":        "qwen3:latest",
		"qwen3:4b":     "qwen3:4b",
		"qwen3:latest": "qwen3:latest",
		"gpt-4o":       "gpt-4o:latest",
	}
	for in, want := range cases {
		if got := normalizeModelTag(in); got != want {
			t.Errorf("normalizeModelTag(%q) = %q, want %q", in, got, want)
		}
	}
}
