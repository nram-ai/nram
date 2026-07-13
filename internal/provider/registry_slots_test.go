package provider

import "testing"

// TestRegistryConfigSlotAccessors covers the two behaviors the map-backed
// RegistryConfig must hold, both on a zero-value (nil Slots map) receiver: an
// unset slot reads back as the zero (unconfigured) SlotConfig, and SetSlotConfig
// lazily allocates so a write survives the read. This is the map replacement for
// the old hand-maintained switch, whose missing-case footgun once left the ask
// slot silently unwired; a map keys every slot uniformly, so per-slot wiring can
// no longer regress and does not need iterating the canonical Slots list.
func TestRegistryConfigSlotAccessors(t *testing.T) {
	var cfg RegistryConfig // nil Slots map

	if got := cfg.slotConfig(SlotAsk); got.Type != "" {
		t.Fatalf("unset slot = %+v, want the zero (unconfigured) SlotConfig", got)
	}

	want := SlotConfig{Type: "openai", BaseURL: "http://example", Model: "m-ask"}
	cfg.SetSlotConfig(SlotAsk, want)
	if got := cfg.slotConfig(SlotAsk); got.Type != want.Type || got.BaseURL != want.BaseURL || got.Model != want.Model {
		t.Fatalf("round-trip on nil-map receiver: set %+v, got %+v", want, got)
	}
}
