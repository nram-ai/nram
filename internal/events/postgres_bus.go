package events

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const notifyChannel = "nram_events"

// pgNotification is the wire format for Postgres NOTIFY payloads.
type pgNotification struct {
	InstanceID string `json:"instance_id"`
	Event      Event  `json:"event"`
}

// PostgresBus wraps a MemoryBus and adds cross-instance delivery via
// Postgres LISTEN/NOTIFY.
type PostgresBus struct {
	local      *MemoryBus
	pool       *pgxpool.Pool
	instanceID string
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewPostgresBus creates an event bus backed by Postgres LISTEN/NOTIFY for
// cross-instance fan-out, with a local MemoryBus for intra-process delivery.
// subscriberBuf and replayCap are forwarded to the local MemoryBus.
func NewPostgresBus(pool *pgxpool.Pool, subscriberBuf, replayCap int) *PostgresBus {
	ctx, cancel := context.WithCancel(context.Background())

	b := &PostgresBus{
		local:      NewMemoryBus(subscriberBuf, replayCap),
		pool:       pool,
		instanceID: uuid.New().String(),
		cancel:     cancel,
	}

	b.wg.Add(1)
	go b.listen(ctx)

	return b
}

// Publish delivers an event locally and sends a NOTIFY to Postgres so other
// instances can pick it up.
func (b *PostgresBus) Publish(ctx context.Context, event Event) error {
	if err := b.local.Publish(ctx, event); err != nil {
		return err
	}

	payload, err := json.Marshal(pgNotification{
		InstanceID: b.instanceID,
		Event:      event,
	})
	if err != nil {
		return err
	}

	_, execErr := b.pool.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, string(payload))
	return execErr
}

// Subscribe delegates to the underlying MemoryBus.
func (b *PostgresBus) Subscribe(ctx context.Context, scope string) (<-chan Event, func(), error) {
	return b.local.Subscribe(ctx, scope)
}

// Replay delegates to the underlying MemoryBus.
func (b *PostgresBus) Replay(lastEventID string) []Event {
	return b.local.Replay(lastEventID)
}

// Close stops the listener goroutine and shuts down the local bus.
func (b *PostgresBus) Close() error {
	b.cancel()
	b.wg.Wait()
	return b.local.Close()
}

// listen acquires a dedicated connection and waits for notifications, re-publishing
// remote events into the local bus.
func (b *PostgresBus) listen(ctx context.Context) {
	defer b.wg.Done()

	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		log.Printf("ERROR: events: failed to acquire connection for LISTEN: %v", err)
		return
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, "LISTEN "+notifyChannel)
	if err != nil {
		log.Printf("ERROR: events: LISTEN failed: %v", err)
		return
	}

	for {
		notification, waitErr := conn.Conn().WaitForNotification(ctx)
		if waitErr != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("ERROR: events: WaitForNotification: %v", waitErr)
			return
		}

		var n pgNotification
		if unmarshalErr := json.Unmarshal([]byte(notification.Payload), &n); unmarshalErr != nil {
			log.Printf("WARNING: events: failed to unmarshal notification payload: %v", unmarshalErr)
			continue
		}

		if n.InstanceID == b.instanceID {
			continue
		}

		if pubErr := b.local.Publish(ctx, n.Event); pubErr != nil {
			log.Printf("WARNING: events: failed to re-publish remote event: %v", pubErr)
		}
	}
}
