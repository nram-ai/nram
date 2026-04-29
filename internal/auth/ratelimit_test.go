package auth

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
)

// rateLimitOKHandler is a simple handler that writes 200 OK.
var rateLimitOKHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
})

// makeAuthRequest creates a request with the given user's AuthContext injected.
func makeAuthRequest(t *testing.T, userID uuid.UUID) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	ac := &AuthContext{UserID: userID, Role: "user"}
	return r.WithContext(WithContext(r.Context(), ac))
}

func TestRateLimitWithinLimitSucceeds(t *testing.T) {
	rl := NewRateLimiter(10, 5, 0, 0) // 10 rps, burst of 5
	defer rl.Stop()

	handler := rl.Handler(rateLimitOKHandler)
	userID := uuid.New()

	// First 5 requests (burst) should all succeed.
	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		req := makeAuthRequest(t, userID)
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, rec.Code)
		}

		// Verify headers are present.
		limit := rec.Header().Get("X-RateLimit-Limit")
		remaining := rec.Header().Get("X-RateLimit-Remaining")
		reset := rec.Header().Get("X-RateLimit-Reset")

		if limit == "" {
			t.Fatalf("request %d: missing X-RateLimit-Limit header", i)
		}
		if remaining == "" {
			t.Fatalf("request %d: missing X-RateLimit-Remaining header", i)
		}
		if reset == "" {
			t.Fatalf("request %d: missing X-RateLimit-Reset header", i)
		}

		limitVal, err := strconv.Atoi(limit)
		if err != nil || limitVal != 5 {
			t.Fatalf("request %d: X-RateLimit-Limit expected 5, got %q", i, limit)
		}
	}
}

func TestRateLimitExceedingLimitReturns429(t *testing.T) {
	rl := NewRateLimiter(1, 2, 0, 0) // 1 rps, burst of 2
	defer rl.Stop()

	handler := rl.Handler(rateLimitOKHandler)
	userID := uuid.New()

	// Exhaust the burst.
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := makeAuthRequest(t, userID)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, rec.Code)
		}
	}

	// Third request should be rate limited.
	rec := httptest.NewRecorder()
	req := makeAuthRequest(t, userID)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}

	retryAfter := rec.Header().Get("Retry-After")
	if retryAfter == "" {
		t.Fatal("missing Retry-After header on 429 response")
	}

	retryVal, err := strconv.Atoi(retryAfter)
	if err != nil || retryVal < 1 {
		t.Fatalf("Retry-After should be a positive integer, got %q", retryAfter)
	}

	remaining := rec.Header().Get("X-RateLimit-Remaining")
	if remaining != "0" {
		t.Fatalf("X-RateLimit-Remaining on 429 should be 0, got %q", remaining)
	}
}

func TestRateLimitIndependentUsers(t *testing.T) {
	rl := NewRateLimiter(1, 2, 0, 0) // 1 rps, burst of 2
	defer rl.Stop()

	handler := rl.Handler(rateLimitOKHandler)
	user1 := uuid.New()
	user2 := uuid.New()

	// Exhaust user1's burst.
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := makeAuthRequest(t, user1)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("user1 request %d: expected 200, got %d", i, rec.Code)
		}
	}

	// User1 should be rate limited.
	rec := httptest.NewRecorder()
	req := makeAuthRequest(t, user1)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("user1 should be rate limited, got %d", rec.Code)
	}

	// User2 should still be able to make requests.
	rec = httptest.NewRecorder()
	req = makeAuthRequest(t, user2)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("user2 should not be rate limited, got %d", rec.Code)
	}
}

func TestRateLimitUnauthenticatedPassThrough(t *testing.T) {
	rl := NewRateLimiter(1, 1, 0, 0) // Very restrictive
	defer rl.Stop()

	handler := rl.Handler(rateLimitOKHandler)

	// Make requests without auth context — they should all pass through.
	for i := 0; i < 10; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("unauthenticated request %d: expected 200, got %d", i, rec.Code)
		}

		// Should not have rate limit headers.
		if h := rec.Header().Get("X-RateLimit-Limit"); h != "" {
			t.Fatalf("unauthenticated request %d: should not have X-RateLimit-Limit header", i)
		}
	}
}

func TestRateLimitHeadersPresent(t *testing.T) {
	rl := NewRateLimiter(100, 50, 0, 0) // Generous limits so nothing is blocked
	defer rl.Stop()

	handler := rl.Handler(rateLimitOKHandler)
	userID := uuid.New()

	rec := httptest.NewRecorder()
	req := makeAuthRequest(t, userID)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Verify all three headers.
	limit := rec.Header().Get("X-RateLimit-Limit")
	remaining := rec.Header().Get("X-RateLimit-Remaining")
	reset := rec.Header().Get("X-RateLimit-Reset")

	if limit == "" {
		t.Fatal("missing X-RateLimit-Limit header")
	}
	if remaining == "" {
		t.Fatal("missing X-RateLimit-Remaining header")
	}
	if reset == "" {
		t.Fatal("missing X-RateLimit-Reset header")
	}

	limitVal, err := strconv.Atoi(limit)
	if err != nil {
		t.Fatalf("X-RateLimit-Limit not a valid integer: %v", err)
	}
	if limitVal != 50 {
		t.Fatalf("X-RateLimit-Limit expected 50, got %d", limitVal)
	}

	remainingVal, err := strconv.Atoi(remaining)
	if err != nil {
		t.Fatalf("X-RateLimit-Remaining not a valid integer: %v", err)
	}
	if remainingVal < 0 || remainingVal > 50 {
		t.Fatalf("X-RateLimit-Remaining out of range: %d", remainingVal)
	}

	resetVal, err := strconv.ParseInt(reset, 10, 64)
	if err != nil {
		t.Fatalf("X-RateLimit-Reset not a valid unix timestamp: %v", err)
	}
	resetTime := time.Unix(resetVal, 0)
	if resetTime.Before(time.Now().Add(-1 * time.Second)) {
		t.Fatalf("X-RateLimit-Reset is in the past: %v", resetTime)
	}
}

func TestRateLimitCleanup(t *testing.T) {
	rl := NewRateLimiter(10, 5, 0, 0)
	defer rl.Stop()

	// Override staleAfter to a very short duration for testing.
	rl.staleAfter = 1 * time.Millisecond

	handler := rl.Handler(rateLimitOKHandler)
	userID := uuid.New()

	// Make a request to create the limiter.
	rec := httptest.NewRecorder()
	req := makeAuthRequest(t, userID)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Verify the limiter exists.
	rl.mu.RLock()
	_, exists := rl.users[userID]
	rl.mu.RUnlock()
	if !exists {
		t.Fatal("expected user limiter to exist")
	}

	// Wait for it to become stale, then run cleanup.
	time.Sleep(5 * time.Millisecond)
	rl.cleanup()

	// Verify the limiter was removed.
	rl.mu.RLock()
	_, exists = rl.users[userID]
	rl.mu.RUnlock()
	if exists {
		t.Fatal("expected stale user limiter to be cleaned up")
	}
}

func TestRateLimitConcurrency(t *testing.T) {
	rl := NewRateLimiter(1000, 500, 0, 0) // High limits to avoid 429s
	defer rl.Stop()

	handler := rl.Handler(rateLimitOKHandler)

	// Spawn multiple goroutines with different users hitting the middleware concurrently.
	done := make(chan struct{}, 100)
	for i := 0; i < 100; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			userID := uuid.New()
			for j := 0; j < 10; j++ {
				rec := httptest.NewRecorder()
				req := makeAuthRequest(t, userID)
				handler.ServeHTTP(rec, req)
				// Should not panic or race.
			}
		}()
	}

	for i := 0; i < 100; i++ {
		<-done
	}
}
