package provider

import "testing"

// TestRegistryConfigRoundTripsEverySlot guards the invariant that every slot in
// the canonical Slots list is wired into RegistryConfig's SetSlotConfig /
// slotConfig switch. RegistryConfig stores slots in named struct fields (not a
// map), so a slot added to Slots without a matching case is silently dropped:
// SetSlotConfig is a no-op and buildProviders then sees an empty Type and skips
// it, so the provider never builds even though its config persisted. This test
// makes that failure loud (it caught the ask slot missing its RegistryConfig
// wiring during the ask-tool work).
func TestRegistryConfigRoundTripsEverySlot(t *testing.T) {
	for _, def := range Slots {
		t.Run(def.Name, func(t *testing.T) {
			var cfg RegistryConfig
			want := SlotConfig{Type: "openai", BaseURL: "http://example", Model: "m-" + def.Name}
			cfg.SetSlotConfig(def.Name, want)
			got := cfg.slotConfig(def.Name)
			if got.Type != want.Type || got.BaseURL != want.BaseURL || got.Model != want.Model {
				t.Fatalf("slot %q did not round-trip through RegistryConfig: set %+v, got %+v "+
					"(add a case to SetSlotConfig and slotConfig, and a field to RegistryConfig)",
					def.Name, want, got)
			}
		})
	}
}
