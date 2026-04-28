package provider

import "testing"

func TestEstimateTokens_OpenAIModel(t *testing.T) {
	n := EstimateTokens("gpt-4", "hello world this is a test")
	if n < 4 || n > 10 {
		t.Fatalf("unexpected token count for gpt-4: %d", n)
	}
}

func TestEstimateTokens_GPT4o(t *testing.T) {
	n := EstimateTokens("gpt-4o-mini", "hello world this is a test")
	if n < 4 || n > 10 {
		t.Fatalf("unexpected token count for gpt-4o-mini: %d", n)
	}
}

func TestEstimateTokens_Empty(t *testing.T) {
	if got := EstimateTokens("gpt-4", ""); got != 0 {
		t.Fatalf("expected 0 for empty text, got %d", got)
	}
}

func TestEstimateTokens_NonOpenAIModel(t *testing.T) {
	n := EstimateTokens("claude-opus-4-7", "hello world")
	if n <= 0 {
		t.Fatalf("expected positive token count, got %d", n)
	}
}

func TestEncodingForModel(t *testing.T) {
	cases := []struct {
		model string
		want  string
	}{
		{"gpt-4o", "o200k_base"},
		{"gpt-4o-mini", "o200k_base"},
		{"o1-preview", "o200k_base"},
		{"text-embedding-3-large", "o200k_base"},
		{"gpt-4", "cl100k_base"},
		{"gpt-3.5-turbo", "cl100k_base"},
		{"text-embedding-ada-002", "cl100k_base"},
		{"claude-opus-4-7", "cl100k_base"},
		{"llama-3", "cl100k_base"},
	}
	for _, c := range cases {
		if got := encodingForModel(c.model); got != c.want {
			t.Errorf("encodingForModel(%q) = %q, want %q", c.model, got, c.want)
		}
	}
}
