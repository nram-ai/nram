package events

import (
	"context"
	"log"
	"strings"
	"sync"
)

// fallbackSubscriberBufferSize is the absolute floor used when callers pass
// 0 to NewMemoryBus. Production callers (cmd/server/main.go) resolve the
// value from SettingEventsSubscriberBufferSize. Tests pass 0 and fall here.
const fallbackSubscriberBufferSize = 64

// fallbackReplayCapacity mirrors fallbackSubscriberBufferSize for the replay
// ring buffer; resolved from SettingEventsReplayCapacity at the production
// call site.
const fallbackReplayCapacity = 256

type subscriber struct {
	ch    chan Event
	scope string
}

// MemoryBus is a thread-safe in-memory implementation of EventBus.
type MemoryBus struct {
	mu             sync.RWMutex
	subscribers    map[uint64]*subscriber
	nextID         uint64
	closed         bool
	replay         *ReplayBuffer
	subscriberBuf  int
}

// NewMemoryBus creates a new in-memory event bus. subscriberBuf and
// replayCap are resolved from settings at the production call site
// (cmd/server/main.go). Zero or negative falls back to the in-package
// floor so tests that don't care about tuning can still construct a bus.
func NewMemoryBus(subscriberBuf, replayCap int) *MemoryBus {
	if subscriberBuf < 1 {
		subscriberBuf = fallbackSubscriberBufferSize
	}
	if replayCap < 1 {
		replayCap = fallbackReplayCapacity
	}
	return &MemoryBus{
		subscribers:   make(map[uint64]*subscriber),
		replay:        NewReplayBuffer(replayCap),
		subscriberBuf: subscriberBuf,
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
		ch:    make(chan Event, b.subscriberBuf),
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
