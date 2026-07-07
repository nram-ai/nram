// Package periodic provides a single background-loop primitive shared by every
// DB-maintenance sweeper: run a sweep once at startup, then again on a
// re-resolved interval, until the context is cancelled. Extracted so the
// startup-sweep and hot-reload semantics live in one tested place instead of
// being hand-rolled (subtly differently) per sweeper.
package periodic

import (
	"context"
	"time"
)

// Run invokes fn once immediately, then again after each interval() elapses,
// until ctx is cancelled. interval() is re-resolved after every run so a
// settings change takes effect without a restart; a sub-second (or unset)
// result is floored to 1s as a safety net against a hot spin (callers clamp to
// their own real defaults). fn's startup argument is true only for the
// immediate first invocation, letting a caller distinguish a boot-time failure
// from a steady-state one in its logs. The timer is Reset only after fn
// returns, when timer.C has already been drained by the receive, so the Reset
// is race-free.
func Run(ctx context.Context, interval func(context.Context) time.Duration, fn func(ctx context.Context, startup bool)) {
	fn(ctx, true)

	timer := time.NewTimer(clamp(interval(ctx)))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			fn(ctx, false)
			timer.Reset(clamp(interval(ctx)))
		}
	}
}

// Fixed adapts a constant interval to the resolver signature Run expects, for
// callers whose cadence is not settings-backed and never changes.
func Fixed(d time.Duration) func(context.Context) time.Duration {
	return func(context.Context) time.Duration { return d }
}

// clamp floors a non-positive or sub-second interval to 1s so a
// misconfigured resolver can never turn Run into a busy loop.
func clamp(d time.Duration) time.Duration {
	if d < time.Second {
		return time.Second
	}
	return d
}
