package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestRedactURLSecrets(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "gemini key param",
			in:   `Post "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=AIzaSECRET": dial tcp: i/o timeout`,
			want: `Post "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=REDACTED": dial tcp: i/o timeout`,
		},
		{
			name: "token param among others redacts only the secret",
			in:   `https://host/path?token=T0KEN&alt=json`,
			want: `https://host/path?token=REDACTED&alt=json`,
		},
		{
			name: "secret-free url unchanged",
			in:   `Get "https://host/v1beta/models": connection refused`,
			want: `Get "https://host/v1beta/models": connection refused`,
		},
		{
			name: "multiple secret params all redacted",
			in:   `https://host/p?key=A&x=1&access_token=B`,
			want: `https://host/p?key=REDACTED&x=1&access_token=REDACTED`,
		},
		{
			name: "case-insensitive param name",
			in:   `https://host/p?API_KEY=SECRET`,
			want: `https://host/p?API_KEY=REDACTED`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redactURLSecrets(tt.in); got != tt.want {
				t.Errorf("redactURLSecrets()\n got  %q\n want %q", got, tt.want)
			}
		})
	}
}

func TestRedactErrorPreservesUnwrap(t *testing.T) {
	if redactError(nil) != nil {
		t.Fatal("redactError(nil) must be nil")
	}

	// An error with nothing to redact is returned untouched (same instance).
	plain := errors.New("plain failure")
	if got := redactError(plain); got != plain {
		t.Errorf("redactError of a secret-free error should return the same instance")
	}

	// A wrapped ErrCircuitOpen whose message carries a secret is redacted but
	// still matches the sentinel via errors.Is (Unwrap chain preserved).
	openErr := &CircuitOpenError{
		Provider: "gemini",
		Cause:    fmt.Errorf(`Post "https://host/x?key=SECRET": timeout`),
	}
	got := redactError(openErr)
	if strings.Contains(got.Error(), "SECRET") {
		t.Errorf("redacted CircuitOpenError still leaks the secret: %q", got.Error())
	}
	if !errors.Is(got, ErrCircuitOpen) {
		t.Error("redacted error no longer matches ErrCircuitOpen sentinel")
	}
}

// TestCircuitBreakerLLMRedactsProviderError proves the redaction chokepoint at
// executeCB scrubs a URL-embedded secret from a provider error before it reaches
// any caller, for every adapter that funnels through the circuit-breaker
// decorator.
func TestCircuitBreakerLLMRedactsProviderError(t *testing.T) {
	const secret = "AIzaSECRETKEY"
	mock := &mockLLM{
		completeErr: fmt.Errorf("request failed: %w",
			fmt.Errorf(`Post "https://generativelanguage.googleapis.com/v1beta/models/x:generateContent?key=%s": dial tcp: i/o timeout`, secret)),
	}
	wrapped := NewCircuitBreakerLLM(mock, testConfig())

	_, err := wrapped.Complete(context.Background(), &CompletionRequest{
		Messages: []Message{{Role: "user", Content: "hi"}},
	})
	if err == nil {
		t.Fatal("expected an error from the wrapped provider")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("circuit-breaker error leaked the API key: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "REDACTED") {
		t.Errorf("expected the secret to be masked as REDACTED, got %q", err.Error())
	}
}
