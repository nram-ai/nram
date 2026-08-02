package server

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSecurityHeadersMiddleware_SetsHardeningHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	SecurityHeadersMiddleware(okHandler).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	want := map[string]string{
		"X-Content-Type-Options":  "nosniff",
		"X-Frame-Options":         "DENY",
		"Content-Security-Policy": "frame-ancestors 'none'",
	}
	for header, expected := range want {
		if got := rec.Header().Get(header); got != expected {
			t.Errorf("%s = %q, want %q", header, got, expected)
		}
	}
}

// The absence of Strict-Transport-Security is a deliberate policy choice (nram
// runs over plain HTTP locally). Guard it against reintroduction, including on a
// request that presents every "this is secure" signal a proxy could set, so no
// future secure-request branch can slip an HSTS header back in.
func TestSecurityHeadersMiddleware_NeverEmitsHSTS(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://example.com/", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	req.TLS = &tls.ConnectionState{}

	rec := httptest.NewRecorder()
	SecurityHeadersMiddleware(okHandler).ServeHTTP(rec, req)

	if got := rec.Header().Get("Strict-Transport-Security"); got != "" {
		t.Errorf("Strict-Transport-Security = %q, want it absent", got)
	}
}
