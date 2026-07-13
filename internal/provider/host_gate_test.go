package provider

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestHostGateBoundsConcurrency verifies the gate never admits more than its
// limit of simultaneous holders, under heavy contention.
func TestHostGateBoundsConcurrency(t *testing.T) {
	const limit = 3
	g := newHostGate(limit)

	var inFlight atomic.Int32
	var maxSeen atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.Acquire(context.Background()); err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			cur := inFlight.Add(1)
			for {
				m := maxSeen.Load()
				if cur <= m || maxSeen.CompareAndSwap(m, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			inFlight.Add(-1)
			g.Release()
		}()
	}
	wg.Wait()

	if got := maxSeen.Load(); got > limit {
		t.Fatalf("max concurrent holders = %d, want <= %d", got, limit)
	}
	if got := maxSeen.Load(); got == 0 {
		t.Fatal("no holders ran")
	}
}

// TestHostGateContextCancel verifies a blocked Acquire unblocks (and reports the
// error) when its context is cancelled, so a full gate never deadlocks a caller.
func TestHostGateContextCancel(t *testing.T) {
	g := newHostGate(1)
	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	defer g.Release()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Acquire(ctx) }()

	// The second Acquire must be blocked: nothing should arrive yet.
	select {
	case err := <-done:
		t.Fatalf("Acquire returned %v while gate full; expected to block", err)
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled Acquire returned nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled Acquire did not return")
	}
}

// TestHostGatePerHostIsolation verifies distinct gates do not share permits: a
// full gate must not block acquisition on a different one.
func TestHostGatePerHostIsolation(t *testing.T) {
	a := newHostGate(1)
	b := newHostGate(1)
	if err := a.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	defer a.Release()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Acquire(ctx); err != nil {
		t.Fatalf("acquire b (independent host) blocked: %v", err)
	}
	b.Release()
}

// TestHostGateGrowReleasesWaiter verifies raising the limit lets an otherwise
// blocked waiter proceed without anyone releasing.
func TestHostGateGrowReleasesWaiter(t *testing.T) {
	g := newHostGate(1)
	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	defer g.Release()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.Acquire(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("Acquire returned %v at limit 1; expected block", err)
	case <-time.After(20 * time.Millisecond):
	}

	g.setLimit(2) // a new permit should free the waiter
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiter errored after grow: %v", err)
		}
		g.Release()
	case <-time.After(time.Second):
		t.Fatal("waiter did not proceed after limit grew")
	}
}

// TestHostGateShrinkPreservesInFlight verifies shrinking the limit below the
// current in-flight count does not violate the new cap: a held permit plus the
// reduced limit must not let extra callers in until the debt is paid back.
func TestHostGateShrinkPreservesInFlight(t *testing.T) {
	g := newHostGate(2)
	if err := g.Acquire(context.Background()); err != nil { // 1 in flight
		t.Fatalf("acquire 1: %v", err)
	}
	if err := g.Acquire(context.Background()); err != nil { // 2 in flight
		t.Fatalf("acquire 2: %v", err)
	}

	g.setLimit(1) // below the 2 currently held: shrink is absorbed as debt

	// A third Acquire must block: 2 are held against a new limit of 1.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := g.Acquire(ctx); err == nil {
		t.Fatal("Acquire succeeded while over the shrunk limit")
	}

	// Releasing one retires the debt (no permit returned), still 1 held == limit.
	g.Release()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	if err := g.Acquire(ctx2); err == nil {
		t.Fatal("Acquire succeeded while still at the shrunk limit")
	}

	// Releasing the last held permit returns to the free pool: now acquirable.
	g.Release()
	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire after debt cleared: %v", err)
	}
	g.Release()
	if got := g.currentLimit(); got != 1 {
		t.Fatalf("currentLimit = %d, want 1", got)
	}
}

// gateStubLLM is a no-op LLMProvider for exercising the gated wrapper.
type gateStubLLM struct{}

func (gateStubLLM) Complete(context.Context, *CompletionRequest) (*CompletionResponse, error) {
	return &CompletionResponse{}, nil
}
func (gateStubLLM) Name() string     { return "stub" }
func (gateStubLLM) Models() []string { return nil }

// TestGatedLLMAppliesLiveLimit verifies the wrapper re-resolves and applies the
// limit on each call, so a settings change hot-reloads without a restart.
func TestGatedLLMAppliesLiveLimit(t *testing.T) {
	gate := newHostGate(1)
	var limit atomic.Int64
	limit.Store(5)
	p := newGatedLLM(gateStubLLM{}, gate, func(context.Context) int { return int(limit.Load()) })

	if _, err := p.Complete(context.Background(), &CompletionRequest{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if got := gate.currentLimit(); got != 5 {
		t.Fatalf("limit after first call = %d, want 5 (live raise)", got)
	}

	limit.Store(2)
	if _, err := p.Complete(context.Background(), &CompletionRequest{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if got := gate.currentLimit(); got != 2 {
		t.Fatalf("limit after lowering = %d, want 2 (live lower)", got)
	}

	// A non-positive setting means "no cap": resolves to the max permit count.
	limit.Store(0)
	if _, err := p.Complete(context.Background(), &CompletionRequest{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if got := gate.currentLimit(); got != hostGateMaxLimit {
		t.Fatalf("limit after disable = %d, want %d (uncapped)", got, hostGateMaxLimit)
	}
}

// TestBuildProvidersWiresHostGate verifies the registry actually wraps the
// providers it hands out in host gates: same-host LLM slots share one gate
// (aggregate cap, not per-slot), LLM and embed gates are separate, and each
// gate carries the configured limit. buildProviders makes no network call.
func TestBuildProvidersWiresHostGate(t *testing.T) {
	r := &Registry{}
	r.WithHostConcurrency(func(context.Context) HostConcurrency {
		return HostConcurrency{LLM: 2, Embed: 3}
	})

	cfg := RegistryConfig{Slots: map[string]SlotConfig{
		SlotEmbedding: {Type: ProviderTypeOpenAI, BaseURL: "http://embedhost:11434/v1", APIKey: "k", Model: "m"},
		SlotFact:      {Type: ProviderTypeOpenAI, BaseURL: "http://llmhost:30000/v1", APIKey: "k", Model: "m"},
		SlotEntity:    {Type: ProviderTypeOpenAI, BaseURL: "http://llmhost:30000/v1", APIKey: "k", Model: "m"},
	}}
	built, err := r.buildProviders(cfg)
	if err != nil {
		t.Fatalf("buildProviders: %v", err)
	}

	fact, ok := built.llm[SlotFact].(*gatedLLM)
	if !ok {
		t.Fatalf("fact provider not gated: %T", built.llm[SlotFact])
	}
	entity, ok := built.llm[SlotEntity].(*gatedLLM)
	if !ok {
		t.Fatalf("entity provider not gated: %T", built.llm[SlotEntity])
	}
	// Same host => one shared gate, so the cap is aggregate across both slots.
	if fact.gate != entity.gate {
		t.Fatal("fact and entity share a host but got distinct gates")
	}
	if got := fact.gate.currentLimit(); got != 2 {
		t.Fatalf("llm gate limit = %d, want 2", got)
	}

	emb, ok := built.embedding.(*gatedEmbedding)
	if !ok {
		t.Fatalf("embedding provider not gated: %T", built.embedding)
	}
	if emb.gate == fact.gate {
		t.Fatal("embed and llm must not share a gate")
	}
	if got := emb.gate.currentLimit(); got != 3 {
		t.Fatalf("embed gate limit = %d, want 3", got)
	}

	if len(built.llmGates) != 1 {
		t.Fatalf("llmGates = %d, want 1 (both LLM slots on one host)", len(built.llmGates))
	}
	if len(built.embedGates) != 1 {
		t.Fatalf("embedGates = %d, want 1", len(built.embedGates))
	}
}

// TestBuildProvidersNoGateWithoutResolver verifies the pre-gate behavior is
// preserved when no concurrency resolver is installed (e.g. in tests): the
// providers are returned without a gate wrapper.
func TestBuildProvidersNoGateWithoutResolver(t *testing.T) {
	r := &Registry{}
	cfg := RegistryConfig{Slots: map[string]SlotConfig{
		SlotFact: {Type: ProviderTypeOpenAI, BaseURL: "http://llmhost:30000/v1", APIKey: "k", Model: "m"},
	}}
	built, err := r.buildProviders(cfg)
	if err != nil {
		t.Fatalf("buildProviders: %v", err)
	}
	if _, gated := built.llm[SlotFact].(*gatedLLM); gated {
		t.Fatal("fact provider is gated despite no resolver installed")
	}
	if len(built.llmGates) != 0 {
		t.Fatalf("llmGates = %d, want 0 without a resolver", len(built.llmGates))
	}
}

// TestHostKey verifies the de-duplicated host derivation: same host:port across
// different base-URL paths shares a key; missing base URL falls back to type.
func TestHostKey(t *testing.T) {
	cases := []struct {
		name string
		slot SlotConfig
		want string
	}{
		{"base url with path", SlotConfig{Type: "openai-compatible", BaseURL: "http://192.168.2.43:30000/v1"}, "http://192.168.2.43:30000"},
		{"same host different path", SlotConfig{Type: "openai-compatible", BaseURL: "http://192.168.2.43:30000/v1/chat"}, "http://192.168.2.43:30000"},
		{"no base url", SlotConfig{Type: "Anthropic"}, "type:anthropic"},
		{"uppercase host normalized", SlotConfig{Type: "openai-compatible", BaseURL: "http://LocalHost:11434"}, "http://localhost:11434"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hostKey(tc.slot); got != tc.want {
				t.Fatalf("hostKey = %q, want %q", got, tc.want)
			}
		})
	}
}
