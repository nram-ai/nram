package events

import (
	"context"
	"log"
	"strings"
	"sync"
)

const subscriberBufferSize = 64

type subscriber struct {
	ch    chan Event
	scope string
}

// MemoryBus is a thread-safe in-memory implementation of EventBus.
type MemoryBus struct {
	mu          sync.RWMutex
	subscribers map[uint64]*subscriber
	nextID      uint64
	closed      bool
	replay      *ReplayBuffer
}

// NewMemoryBus creates a new in-memory event bus.
func NewMemoryBus() *MemoryBus {
	return &MemoryBus{
		subscribers: make(map[uint64]*subscriber),
		replay:      NewReplayBuffer(defaultReplayCapacity),
	}
}

// Publish sends an event to all subscribers whose scope matches.
func (b *MemoryBus) Publish(_ context.Context, event Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return ErrBusClosed
	}

	b.replay.Add(event)

	for _, sub := range b.subscribers {
		if !matchesScope(sub.scope, event.Scope) {
			continue
		}
		select {
		case sub.ch <- event:
		default:
			log.Printf("WARNING: dropping event %s (type=%s) for subscriber with scope %q: channel full",
				event.ID, event.Type, sub.scope)
		}
	}

	return nil
}

// Subscribe creates a subscription filtered by scope prefix.
func (b *MemoryBus) Subscribe(_ context.Context, scope string) (<-chan Event, func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, nil, ErrBusClosed
	}

	id := b.nextID
	b.nextID++

	sub := &subscriber{
		ch:    make(chan Event, subscriberBufferSize),
		scope: scope,
	}
	b.subscribers[id] = sub

	cancel := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if _, ok := b.subscribers[id]; ok {
			delete(b.subscribers, id)
			close(sub.ch)
		}
	}

	return sub.ch, cancel, nil
}

// Close shuts down the bus and closes all subscriber channels.
func (b *MemoryBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	b.closed = true
	for id, sub := range b.subscribers {
		close(sub.ch)
		delete(b.subscribers, id)
	}

	return nil
}

// Replay returns buffered events after the given lastEventID.
func (b *MemoryBus) Replay(lastEventID string) []Event {
	return b.replay.Since(lastEventID)
}

// matchesScope returns true if the subscriber scope is a prefix of the event scope.
// An empty subscriber scope matches everything.
func matchesScope(subScope, eventScope string) bool {
	if subScope == "" {
		return true
	}
	return strings.HasPrefix(eventScope, subScope)
}
