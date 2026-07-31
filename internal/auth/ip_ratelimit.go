package auth

import (
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/nram-ai/nram/internal/netutil"
	"golang.org/x/time/rate"
)

// ipLimiter associates a rate.Limiter with the last time it was used, so stale
// entries can be purged.
type ipLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// IPRateLimiter provides per-client-IP token-bucket rate limiting as HTTP
// middleware for the pre-authentication attack surface. Unlike RateLimiter,
// which keys on the authenticated user's UUID and passes anonymous traffic
// through, this limiter keys on the client IP for EVERY request and always
// applies. It fronts the semi-public login group and the public OAuth
// /token + /register endpoints so unauthenticated password brute-force,
// credential stuffing, and challenge-store exhaustion are throttled at source.
//
// The key is netutil.ClientIP (X-Forwarded-For first hop, RemoteAddr host
// fallback); requests with no determinable IP share a single "" bucket. The
// per-key limiter map, background cleanup, and X-RateLimit-* / Retry-After
// response contract mirror RateLimiter deliberately; this is a separate type
// so the working authenticated-path limiter is left untouched.
type IPRateLimiter struct {
	rps   float64
	burst int

	mu  sync.RWMutex
	ips map[string]*ipLimiter

	// cleanupInterval controls how often stale limiters are purged.
	cleanupInterval time.Duration
	// staleAfter is the duration after which an unused limiter is removed.
	staleAfter time.Duration

	stopCleanup chan struct{}
}

// NewIPRateLimiter creates an IPRateLimiter allowing rps requests per second
// with the given burst per client IP. cleanupInterval / staleAfter drive the
// background cleanup loop; zero or negative values fall back to 1m / 10m so
// misconfiguration cannot disable cleanup entirely. The signature mirrors
// NewRateLimiter so cmd/server/main.go resolves both from settings the same way.
func NewIPRateLimiter(rps float64, burst int, cleanupInterval, staleAfter time.Duration) *IPRateLimiter {
	if cleanupInterval <= 0 {
		cleanupInterval = 1 * time.Minute
	}
	if staleAfter <= 0 {
		staleAfter = 10 * time.Minute
	}
	rl := &IPRateLimiter{
		rps:             rps,
		burst:           burst,
		ips:             make(map[string]*ipLimiter),
		cleanupInterval: cleanupInterval,
		staleAfter:      staleAfter,
		stopCleanup:     make(chan struct{}),
	}

	go rl.cleanupLoop()

	return rl
}

// Stop terminates the background cleanup goroutine. Call this when the
// IPRateLimiter is no longer needed.
func (rl *IPRateLimiter) Stop() {
	close(rl.stopCleanup)
}

// Handler returns HTTP middleware that enforces per-IP rate limits. It sets
// X-RateLimit-Limit, X-RateLimit-Remaining, and X-RateLimit-Reset headers on
// every response. If the limit is exceeded it responds with 429 Too Many
// Requests and a Retry-After header.
func (rl *IPRateLimiter) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := netutil.ClientIP(r)
		now := time.Now()
		lim := rl.getLimiter(ip, now)

		// Calculate reservation to determine remaining tokens and reset time.
		res := lim.ReserveN(now, 1)
		if !res.OK() {
			// Burst is zero; should not happen with valid config, but handle gracefully.
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		delay := res.DelayFrom(now)

		// Set rate limit headers on all responses.
		w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", rl.burst))

		if delay > 0 {
			// Over limit; cancel reservation and return 429.
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
			_, _ = fmt.Fprint(w, "rate limit exceeded")
			return
		}

		// Within limit; compute approximate remaining tokens.
		remaining := max(int(lim.TokensAt(now))-1, 0)

		// Reset time is when the bucket will be full again.
		tokensNeeded := float64(rl.burst) - float64(remaining)
		resetDuration := time.Duration(tokensNeeded/rl.rps*1000) * time.Millisecond
		resetTime := now.Add(resetDuration)

		w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", remaining))
		w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", resetTime.Unix()))

		next.ServeHTTP(w, r)
	})
}

// getLimiter returns the rate.Limiter for the given IP, creating its entry if
// needed, and stamps lastSeen (as of now) for the cleanup loop. Folding the
// stamp in here keeps the hot path to a single lock acquisition per request.
func (rl *IPRateLimiter) getLimiter(ip string, now time.Time) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	il, exists := rl.ips[ip]
	if !exists {
		il = &ipLimiter{limiter: rate.NewLimiter(rate.Limit(rl.rps), rl.burst)}
		rl.ips[ip] = il
	}
	il.lastSeen = now
	return il.limiter
}

// cleanupLoop periodically removes stale IP limiters.
func (rl *IPRateLimiter) cleanupLoop() {
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

// cleanup removes IP limiters that have not been used recently.
func (rl *IPRateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	cutoff := time.Now().Add(-rl.staleAfter)
	for ip, il := range rl.ips {
		if il.lastSeen.Before(cutoff) {
			delete(rl.ips, ip)
		}
	}
}
