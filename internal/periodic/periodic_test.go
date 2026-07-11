package periodic

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestRun_StartupBeforeInterval proves the first sweep happens immediately, not
// after a full interval: the interval is 1h, so only the startup call can fire
// within the deadline, and it must carry startup==true.
func TestRun_StartupBeforeInterval(t *testing.T) {
	got := make(chan bool, 1)
	ctx := t.Context()

	go Run(ctx,
		func(context.Context) time.Duration { return time.Hour },
		func(_ context.Context, startup bool) {
			select {
			case got <- startup:
			default:
			}
		})

	select {
	case startup := <-got:
		if !startup {
			t.Fatal("first call must be the startup sweep (startup=true)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup sweep did not run before the interval elapsed")
	}
}

// TestRun_TicksAfterStartup proves the loop keeps firing after the startup
// sweep, and that post-startup calls carry startup==false.
func TestRun_TicksAfterStartup(t *testing.T) {
	calls := make(chan bool, 8)
	ctx := t.Context()

	// 1s is the effective floor (clamp rejects anything faster), so a
	// sub-second value here would be a lie about the cadence under test.
	go Run(ctx,
		func(context.Context) time.Duration { return time.Second },
		func(_ context.Context, startup bool) {
			select {
			case calls <- startup:
			default:
			}
		})

	// First call: the startup sweep.
	select {
	case startup := <-calls:
		if !startup {
			t.Fatal("first call must be the startup sweep (startup=true)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup sweep never ran")
	}
	// Second call: a periodic tick.
	select {
	case startup := <-calls:
		if startup {
			t.Fatal("post-startup call must be a periodic tick (startup=false)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("periodic tick never ran")
	}
}

// TestRun_ReResolvesIntervalEachTick proves the interval is resolved fresh on
// every iteration rather than cached once (the hot-reload guarantee). interval()
// counts its invocations; the resolve that arms each tick runs before that tick
// fires, so after N ticks it has been called at least N times. A cached resolver
// would call it once and never climb past 1.
func TestRun_ReResolvesIntervalEachTick(t *testing.T) {
	var mu sync.Mutex
	resolved := 0
	ticks := make(chan struct{}, 16)

	ctx := t.Context()

	go Run(ctx,
		func(context.Context) time.Duration {
			mu.Lock()
			resolved++
			mu.Unlock()
			return time.Second // clamp floors sub-second, so 1s is the real minimum
		},
		func(_ context.Context, startup bool) {
			if !startup {
				select {
				case ticks <- struct{}{}:
				default:
				}
			}
		})

	const wantTicks = 2
	for i := range wantTicks {
		select {
		case <-ticks:
		case <-time.After(3 * time.Second):
			t.Fatalf("observed only %d of %d ticks", i, wantTicks)
		}
	}

	mu.Lock()
	n := resolved
	mu.Unlock()
	// wantTicks ticks imply at least wantTicks resolves (one resolve arms each
	// tick); a cached resolver stays at 1 and still fails this check.
	if n < wantTicks {
		t.Fatalf("interval resolved %d times across %d ticks; expected re-resolution each tick", n, wantTicks)
	}
}

// TestRun_ReturnsOnContextCancel proves the loop exits promptly when its
// context is cancelled (the shutdown path every caller relies on).
func TestRun_ReturnsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		Run(ctx,
			func(context.Context) time.Duration { return time.Hour },
			func(context.Context, bool) {})
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

// TestClamp guards the busy-loop safety net: any sub-second (or non-positive)
// interval floors to 1s, while a real interval passes through untouched.
func TestClamp(t *testing.T) {
	cases := []struct {
		in, want time.Duration
	}{
		{-time.Second, time.Second},
		{0, time.Second},
		{500 * time.Millisecond, time.Second},
		{time.Second, time.Second},
		{5 * time.Minute, 5 * time.Minute},
	}
	for _, c := range cases {
		if got := clamp(c.in); got != c.want {
			t.Errorf("clamp(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}
