package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestApplyCustomHeaders(t *testing.T) {
	t.Run("adds new headers and overrides non-reserved", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
		req.Header.Set("Authorization", "Bearer original")
		req.Header.Set("Content-Type", "application/json")

		applyCustomHeaders(req, map[string]string{
			"X-Proxy-Token": "secret",
			"authorization": "Bearer override", // canonicalizes to Authorization
			"Content-Type":  "text/plain",      // reserved
		}, "Content-Type")

		if got := req.Header.Get("X-Proxy-Token"); got != "secret" {
			t.Errorf("X-Proxy-Token = %q, want secret", got)
		}
		if got := req.Header.Get("Authorization"); got != "Bearer override" {
			t.Errorf("Authorization = %q, want override", got)
		}
		if got := req.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json (reserved)", got)
		}
	})

	t.Run("skips blank values and names", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
		req.Header.Set("X-Keep", "stay")
		applyCustomHeaders(req, map[string]string{"X-Keep": "", "": "x"})
		if got := req.Header.Get("X-Keep"); got != "stay" {
			t.Errorf("blank value should not clear; X-Keep = %q", got)
		}
	})

	t.Run("nil map is a no-op", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
		applyCustomHeaders(req, nil, "Content-Type")
	})
}

func TestOpenAIProviderCustomHeaders(t *testing.T) {
	p := NewOpenAIProvider(OpenAIConfig{
		APIKey: "sk-test",
		CustomHeaders: map[string]string{
			"X-Proxy-Auth":  "proxy-token",
			"Authorization": "Bearer proxy-override",
			"Content-Type":  "text/plain",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "http://example.com", nil)
	p.setHeaders(req)

	if got := req.Header.Get("X-Proxy-Auth"); got != "proxy-token" {
		t.Errorf("X-Proxy-Auth = %q", got)
	}
	// Authorization is overridable.
	if got := req.Header.Get("Authorization"); got != "Bearer proxy-override" {
		t.Errorf("Authorization = %q, want overridden", got)
	}
	// Content-Type is reserved.
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
}

func TestAnthropicProviderCustomHeaders(t *testing.T) {
	p := NewAnthropicProvider(AnthropicConfig{
		APIKey: "sk-ant",
		CustomHeaders: map[string]string{
			"X-Tenant":          "acme",
			"x-api-key":         "override-key",
			"anthropic-version": "9999-99-99",
			"Content-Type":      "text/plain",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "http://example.com", nil)
	p.setHeaders(req)

	if got := req.Header.Get("X-Tenant"); got != "acme" {
		t.Errorf("X-Tenant = %q", got)
	}
	// x-api-key is overridable.
	if got := req.Header.Get("x-api-key"); got != "override-key" {
		t.Errorf("x-api-key = %q, want overridden", got)
	}
	// anthropic-version and Content-Type are reserved.
	if got := req.Header.Get("anthropic-version"); got != "2023-06-01" {
		t.Errorf("anthropic-version = %q, want 2023-06-01 (reserved)", got)
	}
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json (reserved)", got)
	}
}

func TestGeminiProviderCustomHeaders(t *testing.T) {
	p := NewGeminiProvider(GeminiConfig{
		CustomHeaders: map[string]string{
			"X-Proxy-Auth": "proxy-token",
			"Content-Type": "text/plain",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "http://example.com", nil)
	p.setHeaders(req)

	if got := req.Header.Get("X-Proxy-Auth"); got != "proxy-token" {
		t.Errorf("X-Proxy-Auth = %q", got)
	}
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json (reserved)", got)
	}
}

func TestOllamaClientCustomHeaders(t *testing.T) {
	c := NewOllamaClient(OllamaConfig{
		CustomHeaders: map[string]string{
			"X-Proxy-Auth": "proxy-token",
			"Content-Type": "text/plain",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "http://example.com", nil)
	req.Header.Set("Content-Type", "application/json")
	c.setHeaders(req)

	if got := req.Header.Get("X-Proxy-Auth"); got != "proxy-token" {
		t.Errorf("X-Proxy-Auth = %q", got)
	}
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json (reserved)", got)
	}
}

// TestProviderCustomHeadersOnWire confirms headers reach the actual outbound
// request, not just setHeaders, by capturing them at an httptest server.
func TestProviderCustomHeadersOnWire(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Proxy-Auth")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"x","object":"model"}]}`))
	}))
	defer srv.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		BaseURL:       srv.URL,
		APIKey:        "sk-test",
		CustomHeaders: map[string]string{"X-Proxy-Auth": "wire-token"},
	})
	if err := p.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if gotHeader != "wire-token" {
		t.Errorf("server saw X-Proxy-Auth = %q, want wire-token", gotHeader)
	}
}
