package logging

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// WriterOptions tunes the async writer. Zero values fall back to defaults.
type WriterOptions struct {
	BufferSize    int           // channel capacity; records dropped when full
	MaxBatch      int           // flush when this many records accumulate
	FlushInterval time.Duration // flush at least this often
	WriteTimeout  time.Duration // per-flush sink deadline
}

func (o WriterOptions) withDefaults() WriterOptions {
	if o.BufferSize <= 0 {
		o.BufferSize = 4096
	}
	if o.MaxBatch <= 0 {
		o.MaxBatch = 256
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = 2 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 5 * time.Second
	}
	return o
}

// asyncWriter buffers records on a channel and batch-writes them to a Sink from
// a single background goroutine, so logging never blocks or fails a caller. On
// buffer overflow a record is dropped and counted; the count is surfaced to
// stderr periodically (never back through slog, to avoid recursion).
type asyncWriter struct {
	sink    Sink
	opts    WriterOptions
	ch      chan Record
	done    chan struct{}
	wg      sync.WaitGroup
	dropped atomic.Int64
	started atomic.Bool
}

// NewAsyncWriter creates an async writer over the given sink. Call Start before
// enqueuing and Close on shutdown to flush buffered records.
func NewAsyncWriter(sink Sink, opts WriterOptions) *asyncWriter {
	opts = opts.withDefaults()
	return &asyncWriter{
		sink: sink,
		opts: opts,
		ch:   make(chan Record, opts.BufferSize),
		done: make(chan struct{}),
	}
}

// Start launches the background flush goroutine. Idempotent.
func (w *asyncWriter) Start() {
	if !w.started.CompareAndSwap(false, true) {
		return
	}
	w.wg.Add(1)
	go w.run()
}

// Enqueue offers a record to the buffer without blocking. If the buffer is full
// the record is dropped and the dropped counter incremented.
func (w *asyncWriter) Enqueue(r Record) {
	select {
	case w.ch <- r:
	default:
		w.dropped.Add(1)
	}
}

// Dropped returns the number of records dropped due to a full buffer.
func (w *asyncWriter) Dropped() int64 { return w.dropped.Load() }

// Close stops the writer, draining and flushing buffered records. It returns
// when the goroutine has exited or ctx is done, whichever comes first.
func (w *asyncWriter) Close(ctx context.Context) error {
	if !w.started.Load() {
		return nil
	}
	close(w.done)
	finished := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *asyncWriter) run() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.opts.FlushInterval)
	defer ticker.Stop()

	batch := make([]Record, 0, w.opts.MaxBatch)
	var lastDropReport int64

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
		if err := w.sink.Write(ctx, batch); err != nil {
			fmt.Fprintf(os.Stderr, "logging: sink write failed (%d records): %v\n", len(batch), err)
		}
		cancel()
		batch = batch[:0]
	}

	reportDrops := func() {
		if d := w.dropped.Load(); d > lastDropReport {
			fmt.Fprintf(os.Stderr, "logging: dropped %d log records (DB sink buffer full)\n", d-lastDropReport)
			lastDropReport = d
		}
	}

	for {
		select {
		case r := <-w.ch:
			batch = append(batch, r)
			if len(batch) >= w.opts.MaxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
			reportDrops()
		case <-w.done:
			// Drain whatever is buffered, then flush and exit.
			for {
				select {
				case r := <-w.ch:
					batch = append(batch, r)
					if len(batch) >= w.opts.MaxBatch {
						flush()
					}
				default:
					flush()
					reportDrops()
					return
				}
			}
		}
	}
}
