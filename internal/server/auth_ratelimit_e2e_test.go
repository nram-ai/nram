package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/auth"
)

// The pre-auth IP throttle (RouterConfig.AuthRateLimiter) fronts the
// unauthenticated login group. These tests pin the two regressions the fix
// exists to prevent: a single IP cannot brute-force /v1/auth/login without
// hitting 429, and one IP exhausting its bucket (the vector that would flood
// the WebAuthn/IdP challenge stores) does not throttle a second IP.

// postAs issues one POST to path through router, keyed to ip via
// X-Forwarded-For (the header netutil.ClientIP reads first), and returns the
// status code.
func postAs(router http.Handler, ip, path string) int {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
	req.Header.Set("X-Forwarded-For", ip)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec.Code
}

// TestAuthRateLimitThrottlesLoginBruteForce: N+1 failed logins from one IP
// within the window return 429.
func TestAuthRateLimitThrottlesLoginBruteForce(t *testing.T) {
	lim := auth.NewIPRateLimiter(1, 3, 0, 0) // 1 rps, burst of 3
	t.Cleanup(lim.Stop)

	// Stub login handler stands in for the real one: it returns 401 (a failed
	// attempt). What matters is that requests reach it until the throttle trips.
	login := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	router := NewRouter(RouterConfig{AuthRateLimiter: lim}, Handlers{AuthLogin: login})

	const attacker = "203.0.113.7"

	// The burst of 3 reaches the handler (401, not throttled).
	for i := range 3 {
		if code := postAs(router, attacker, "/v1/auth/login"); code == http.StatusTooManyRequests {
			t.Fatalf("login attempt %d unexpectedly throttled (got 429 within burst)", i)
		}
	}
	// The next attempt from the same IP is throttled.
	if code := postAs(router, attacker, "/v1/auth/login"); code != http.StatusTooManyRequests {
		t.Fatalf("N+1th login from one IP: got %d, want 429", code)
	}
}

// TestAuthRateLimitIsolatesFloodByIP: one IP filling its bucket against the
// passkey challenge endpoint does not block a second IP. This is the per-IP
// isolation that keeps a single client from exhausting the shared challenge
// store on behalf of everyone.
func TestAuthRateLimitIsolatesFloodByIP(t *testing.T) {
	lim := auth.NewIPRateLimiter(1, 3, 0, 0) // 1 rps, burst of 3
	t.Cleanup(lim.Stop)

	begin := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	router := NewRouter(RouterConfig{AuthRateLimiter: lim}, Handlers{AuthPasskeyBegin: begin})

	const flooder = "203.0.113.7"
	const victim = "198.51.100.9"

	// Flooder exhausts its bucket, then keeps going and gets throttled.
	for range 3 {
		postAs(router, flooder, "/v1/auth/passkey/begin")
	}
	if code := postAs(router, flooder, "/v1/auth/passkey/begin"); code != http.StatusTooManyRequests {
		t.Fatalf("flooder should be throttled after exhausting its burst, got %d", code)
	}

	// The victim IP is unaffected: its own bucket is full.
	if code := postAs(router, victim, "/v1/auth/passkey/begin"); code == http.StatusTooManyRequests {
		t.Fatalf("second IP was throttled by the flooder's traffic, got 429; per-IP isolation is broken")
	}
}

// TestAuthRateLimitThrottlesPublicOAuth pins that the throttle also fronts the
// fully-public OAuth /token and /register endpoints, which sit in a different
// router group (nested inside the CORS group) than the login flow. /register
// writes a DB row per call, so leaving it unthrottled is the write-amplification
// vector the note calls out. Both endpoints share the same per-IP bucket.
func TestAuthRateLimitThrottlesPublicOAuth(t *testing.T) {
	lim := auth.NewIPRateLimiter(1, 2, 0, 0) // 1 rps, burst of 2
	t.Cleanup(lim.Stop)

	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	router := NewRouter(RouterConfig{AuthRateLimiter: lim}, Handlers{OAuthToken: ok, OAuthRegister: ok})

	const client = "203.0.113.7"

	// Burst of 2 against /token reaches the handler.
	for i := range 2 {
		if code := postAs(router, client, "/token"); code == http.StatusTooManyRequests {
			t.Fatalf("/token request %d unexpectedly throttled within burst", i)
		}
	}
	// The next /token from the same IP is throttled: the nested-group mount is live.
	if code := postAs(router, client, "/token"); code != http.StatusTooManyRequests {
		t.Fatalf("/token N+1 from one IP: got %d, want 429", code)
	}
	// /register shares the same per-IP bucket, so it is already over the limit.
	if code := postAs(router, client, "/register"); code != http.StatusTooManyRequests {
		t.Fatalf("/register from the same IP shares the bucket: got %d, want 429", code)
	}
}
