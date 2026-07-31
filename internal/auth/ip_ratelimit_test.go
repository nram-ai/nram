package auth

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// makeIPRequest builds a request whose client IP resolves to ip. It sets both
// RemoteAddr and X-Forwarded-For so the key is deterministic regardless of
// which netutil.ClientIP branch runs; xffKeying below covers the case where the
// two disagree.
func makeIPRequest(ip string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/auth/login", nil)
	r.RemoteAddr = ip + ":54321"
	r.Header.Set("X-Forwarded-For", ip)
	return r
}

func TestIPRateLimitExceedingLimitReturns429(t *testing.T) {
	rl := NewIPRateLimiter(1, 2, 0, 0) // 1 rps, burst of 2
	defer rl.Stop()

	handler := rl.Handler(rateLimitOKHandler)

	// Exhaust the burst from one IP.
	for i := range 2 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, makeIPRequest("203.0.113.7"))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, rec.Code)
		}
	}

	// Third request from the same IP is throttled.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, makeIPRequest("203.0.113.7"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}

	retryAfter := rec.Header().Get("Retry-After")
	if retryAfter == "" {
		t.Fatal("missing Retry-After header on 429 response")
	}
	if v, err := strconv.Atoi(retryAfter); err != nil || v < 1 {
		t.Fatalf("Retry-After should be a positive integer, got %q", retryAfter)
	}
	if remaining := rec.Header().Get("X-RateLimit-Remaining"); remaining != "0" {
		t.Fatalf("X-RateLimit-Remaining on 429 should be 0, got %q", remaining)
	}
}

func TestIPRateLimitIndependentIPs(t *testing.T) {
	rl := NewIPRateLimiter(1, 2, 0, 0) // 1 rps, burst of 2
	defer rl.Stop()

	handler := rl.Handler(rateLimitOKHandler)

	// Exhaust IP A's burst.
	for i := range 2 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, makeIPRequest("203.0.113.7"))
		if rec.Code != http.StatusOK {
			t.Fatalf("ipA request %d: expected 200, got %d", i, rec.Code)
		}
	}

	// IP A is now throttled.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, makeIPRequest("203.0.113.7"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("ipA should be throttled, got %d", rec.Code)
	}

	// IP B is unaffected.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, makeIPRequest("198.51.100.9"))
	if rec.Code != http.StatusOK {
		t.Fatalf("ipB should not be throttled, got %d", rec.Code)
	}
}

// TestIPRateLimitKeysOnXFF pins that the throttle keys on the X-Forwarded-For
// hop, not the transport RemoteAddr: two requests arriving on different
// RemoteAddrs but carrying the same forwarded IP share one bucket. This is the
// behavior a reverse proxy relies on (every request dials from the proxy's
// RemoteAddr; the real client is in the header).
func TestIPRateLimitKeysOnXFF(t *testing.T) {
	rl := NewIPRateLimiter(1, 1, 0, 0) // burst of 1
	defer rl.Stop()

	handler := rl.Handler(rateLimitOKHandler)

	req1 := httptest.NewRequest(http.MethodPost, "/v1/auth/login", nil)
	req1.RemoteAddr = "10.0.0.1:1111" // proxy connection A
	req1.Header.Set("X-Forwarded-For", "203.0.113.7")

	req2 := httptest.NewRequest(http.MethodPost, "/v1/auth/login", nil)
	req2.RemoteAddr = "10.0.0.2:2222" // proxy connection B, same real client
	req2.Header.Set("X-Forwarded-For", "203.0.113.7")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req1)
	if rec.Code != http.StatusOK {
		t.Fatalf("first XFF request: expected 200, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req2)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request with same XFF but different RemoteAddr should be throttled, got %d", rec.Code)
	}
}

func TestIPRateLimitHeadersPresent(t *testing.T) {
	rl := NewIPRateLimiter(100, 50, 0, 0) // generous, nothing blocked
	defer rl.Stop()

	handler := rl.Handler(rateLimitOKHandler)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, makeIPRequest("203.0.113.7"))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	limit := rec.Header().Get("X-RateLimit-Limit")
	if v, err := strconv.Atoi(limit); err != nil || v != 50 {
		t.Fatalf("X-RateLimit-Limit expected 50, got %q", limit)
	}
	remaining := rec.Header().Get("X-RateLimit-Remaining")
	if v, err := strconv.Atoi(remaining); err != nil || v < 0 || v > 50 {
		t.Fatalf("X-RateLimit-Remaining out of range: %q", remaining)
	}
	reset := rec.Header().Get("X-RateLimit-Reset")
	if v, err := strconv.ParseInt(reset, 10, 64); err != nil || time.Unix(v, 0).Before(time.Now().Add(-1*time.Second)) {
		t.Fatalf("X-RateLimit-Reset not a valid future timestamp: %q", reset)
	}
}

func TestIPRateLimitCleanup(t *testing.T) {
	rl := NewIPRateLimiter(10, 5, 0, 0)
	defer rl.Stop()

	// Force staleness immediately.
	rl.staleAfter = 1 * time.Millisecond

	handler := rl.Handler(rateLimitOKHandler)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, makeIPRequest("203.0.113.7"))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	rl.mu.RLock()
	_, exists := rl.ips["203.0.113.7"]
	rl.mu.RUnlock()
	if !exists {
		t.Fatal("expected IP limiter to exist")
	}

	time.Sleep(5 * time.Millisecond)
	rl.cleanup()

	rl.mu.RLock()
	_, exists = rl.ips["203.0.113.7"]
	rl.mu.RUnlock()
	if exists {
		t.Fatal("expected stale IP limiter to be cleaned up")
	}
}

func TestIPRateLimitConcurrency(t *testing.T) {
	rl := NewIPRateLimiter(1000, 500, 0, 0) // high limits to avoid 429s
	defer rl.Stop()

	handler := rl.Handler(rateLimitOKHandler)

	done := make(chan struct{}, 100)
	for n := range 100 {
		go func(n int) {
			defer func() { done <- struct{}{} }()
			ip := "203.0.113." + strconv.Itoa(n)
			for range 10 {
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, makeIPRequest(ip))
				// Should not panic or race.
			}
		}(n)
	}

	for range 100 {
		<-done
	}
}
