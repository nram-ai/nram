package auth

import (
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// userLimiter associates a rate.Limiter with the last time it was used,
// enabling cleanup of stale entries.
type userLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// RateLimiter provides per-user token-bucket rate limiting as HTTP middleware.
// Users without an AuthContext in the request context are not rate-limited
// (the auth middleware is expected to reject unauthenticated requests before
// this middleware runs, but if chaining order differs, unauthenticated
// requests simply pass through).
type RateLimiter struct {
	rps   float64
	burst int

	mu    sync.RWMutex
	users map[uuid.UUID]*userLimiter

	// cleanupInterval controls how often stale limiters are purged.
	cleanupInterval time.Duration
	// staleAfter is the duration after which an unused limiter is removed.
	staleAfter time.Duration

	stopCleanup chan struct{}
}

// NewRateLimiter creates a RateLimiter that allows rps requests per second
// with the given burst size per user. It starts a background goroutine that
// periodically removes limiters for users who have not made a request in the
// last 10 minutes.
func NewRateLimiter(rps float64, burst int) *RateLimiter {
	rl := &RateLimiter{
		rps:             rps,
		burst:           burst,
		users:           make(map[uuid.UUID]*userLimiter),
		cleanupInterval: 1 * time.Minute,
		staleAfter:      10 * time.Minute,
		stopCleanup:     make(chan struct{}),
	}

	go rl.cleanupLoop()

	return rl
}

// Stop terminates the background cleanup goroutine. Call this when the
// RateLimiter is no longer needed.
func (rl *RateLimiter) Stop() {
	close(rl.stopCleanup)
}

// Handler returns HTTP middleware that enforces per-user rate limits.
// It sets X-RateLimit-Limit, X-RateLimit-Remaining, and X-RateLimit-Reset
// headers on every response. If the limit is exceeded it responds with
// 429 Too Many Requests and a Retry-After header.
func (rl *RateLimiter) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ac := FromContext(r.Context())
		if ac == nil {
			// No auth context — skip rate limiting.
			next.ServeHTTP(w, r)
			return
		}

		lim := rl.getLimiter(ac.UserID)
		now := time.Now()

		// Update last-seen timestamp.
		rl.mu.Lock()
		if ul, ok := rl.users[ac.UserID]; ok {
			ul.lastSeen = now
		}
		rl.mu.Unlock()

		// Calculate reservation to determine remaining tokens and reset time.
		res := lim.ReserveN(now, 1)
		if !res.OK() {
			// Burst is zero — should not happen with valid config, but handle gracefully.
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		delay := res.DelayFrom(now)

		// Set rate limit headers on all responses.
		w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", rl.burst))

		if delay > 0 {
			// Over limit — cancel reservation and return 429.
			res.CancelAt(now)

			retryAfter := math.Ceil(delay.Seconds())
			if retryAfter < 1 {
				retryAfter = 1
			}

			resetTime := now.Add(time.Duration(retryAfter) * time.Second)

			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", resetTime.Unix()))
			w.Header().Set("Retry-After", fmt.Sprintf("%d", int(retryAfter)))
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, "rate limit exceeded")
			return
		}

		// Within limit — compute approximate remaining tokens.
		remaining := int(lim.TokensAt(now)) - 1
		if remaining < 0 {
			remaining = 0
		}

		// Reset time is when the bucket will be full again.
		tokensNeeded := float64(rl.burst) - float64(remaining)
		resetDuration := time.Duration(tokensNeeded/rl.rps*1000) * time.Millisecond
		resetTime := now.Add(resetDuration)

		w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", remaining))
		w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", resetTime.Unix()))

		next.ServeHTTP(w, r)
	})
}

// getLimiter returns the rate.Limiter for the given user, creating one if needed.
func (rl *RateLimiter) getLimiter(userID uuid.UUID) *rate.Limiter {
	rl.mu.RLock()
	ul, exists := rl.users[userID]
	rl.mu.RUnlock()

	if exists {
		return ul.limiter
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Double-check after acquiring write lock.
	if ul, exists := rl.users[userID]; exists {
		return ul.limiter
	}

	lim := rate.NewLimiter(rate.Limit(rl.rps), rl.burst)
	rl.users[userID] = &userLimiter{
		limiter:  lim,
		lastSeen: time.Now(),
	}
	return lim
}

// cleanupLoop periodically removes stale user limiters.
func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rl.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rl.stopCleanup:
			return
		case <-ticker.C:
			rl.cleanup()
		}
	}
}

// cleanup removes user limiters that have not been used recently.
func (rl *RateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	cutoff := time.Now().Add(-rl.staleAfter)
	for id, ul := range rl.users {
		if ul.lastSeen.Before(cutoff) {
			delete(rl.users, id)
		}
	}
}
