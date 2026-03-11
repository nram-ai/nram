package events

import "context"

// EventBus defines the interface for event publishing and subscribing.
type EventBus interface {
	// Publish sends an event to all matching subscribers.
	Publish(ctx context.Context, event Event) error

	// Subscribe returns a channel that receives events whose scope starts with
	// the given prefix. An empty scope matches all events. The returned cancel
	// function removes the subscription and closes the channel.
	Subscribe(ctx context.Context, scope string) (<-chan Event, func(), error)

	// Close shuts down the bus and closes all subscriber channels.
	Close() error
}
