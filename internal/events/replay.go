package events

import "sync"

const defaultReplayCapacity = 256

// ReplayBuffer is a thread-safe ring buffer that stores the last N events
// for SSE Last-Event-ID reconnection replay.
type ReplayBuffer struct {
	mu       sync.RWMutex
	buf      []Event
	capacity int
	start    int
	count    int
}

// NewReplayBuffer creates a replay buffer with the given capacity.
// If capacity is zero or negative, defaultReplayCapacity is used.
func NewReplayBuffer(capacity int) *ReplayBuffer {
	if capacity <= 0 {
		capacity = defaultReplayCapacity
	}
	return &ReplayBuffer{
		buf:      make([]Event, capacity),
		capacity: capacity,
	}
}

// Add appends an event to the ring buffer, overwriting the oldest if full.
func (rb *ReplayBuffer) Add(event Event) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	idx := (rb.start + rb.count) % rb.capacity
	rb.buf[idx] = event

	if rb.count < rb.capacity {
		rb.count++
	} else {
		rb.start = (rb.start + 1) % rb.capacity
	}
}

// Since returns all events stored after the event with the given ID.
// If lastEventID is empty or not found, all buffered events are returned.
func (rb *ReplayBuffer) Since(lastEventID string) []Event {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return nil
	}

	// Collect all buffered events in order.
	all := make([]Event, rb.count)
	for i := 0; i < rb.count; i++ {
		all[i] = rb.buf[(rb.start+i)%rb.capacity]
	}

	if lastEventID == "" {
		return all
	}

	// Find the event with the given ID and return everything after it.
	for i, evt := range all {
		if evt.ID == lastEventID {
			return all[i+1:]
		}
	}

	// ID not found — return all buffered events.
	return all
}
