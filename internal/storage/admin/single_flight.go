package admin

import "sync"

// singleFlight is a one-at-a-time guard for background admin operations (the
// vector-store migration and the SQLite->Postgres migration). tryAcquire
// reports whether the caller won the slot; the winner must call release when
// the operation finishes. It exists so the two admin stores share one guard
// rather than each hand-rolling a sync.Mutex + bool.
type singleFlight struct {
	mu      sync.Mutex
	running bool
}

// tryAcquire marks the guard running and returns true, or returns false if it
// was already running.
func (s *singleFlight) tryAcquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return false
	}
	s.running = true
	return true
}

// release clears the running flag so the next operation can acquire.
func (s *singleFlight) release() {
	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
}
