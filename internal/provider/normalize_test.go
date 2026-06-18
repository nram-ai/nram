package provider

import "testing"

func TestNormalizeBaseURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"bare host unchanged", "https://api.openai.com", "https://api.openai.com"},
		{"trailing slash stripped", "https://api.openai.com/", "https://api.openai.com"},
		{"trailing v1 stripped", "https://api.openai.com/v1", "https://api.openai.com"},
		{"trailing v1 with slash stripped", "https://api.openai.com/v1/", "https://api.openai.com"},
		{"trailing v1beta stripped", "https://generativelanguage.googleapis.com/v1beta", "https://generativelanguage.googleapis.com"},
		{"version preserves non-version prefix", "https://openrouter.ai/api/v1", "https://openrouter.ai/api"},
		{"already clean non-version path", "https://openrouter.ai/api", "https://openrouter.ai/api"},
		{"version-like host untouched", "https://v1.example.com", "https://v1.example.com"},
		{"version-like host with version path", "https://v1.example.com/v1", "https://v1.example.com"},
		{"non-version trailing path kept", "https://host.example.com/openai", "https://host.example.com/openai"},
		{"mid-path version not stripped", "https://host.example.com/v1/foo", "https://host.example.com/v1/foo"},
		{"versioned alpha segment stripped", "https://host.example.com/v2alpha3", "https://host.example.com"},
		{"localhost with port unchanged", "http://localhost:11434", "http://localhost:11434"},
		{"localhost with v1 stripped", "http://localhost:11434/v1", "http://localhost:11434"},
		{"empty string", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeBaseURL(tt.in); got != tt.want {
				t.Errorf("NormalizeBaseURL(%q) = %q, want %q", tt.in, got, tt.want)
			}
			// Idempotence: normalizing the result again must not change it.
			if got := NormalizeBaseURL(NormalizeBaseURL(tt.in)); got != tt.want {
				t.Errorf("NormalizeBaseURL not idempotent for %q: %q", tt.in, got)
			}
		})
	}
}
