package admin

import (
	"testing"

	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/service"
)

// TestEverySettingHasSchemaEntry asserts the reverse direction of the
// init() check: for every key in service.settingDefaults (the runtime
// fallback registry), a matching entry exists in settingsSchemas. The
// init() check covers schemas → defaults agreement; this test covers
// defaults → schemas presence so a Setting* declared in settings.go
// without a schema row is caught at PR time.
//
// Display-only keys (memory.default_*, enrichment.batch_size, etc.) are
// already registered in settingsSchemas, so this is exhaustive.
func TestEverySettingHasSchemaEntry(t *testing.T) {
	schemaByKey := make(map[string]api.SettingSchema, len(settingsSchemas))
	for _, entry := range settingsSchemas {
		schemaByKey[entry.Key] = entry
	}

	missing := []string{}
	for _, key := range service.AllSettingKeys() {
		if _, ok := schemaByKey[key]; !ok {
			missing = append(missing, key)
		}
	}

	if len(missing) > 0 {
		t.Fatalf("settings.go declares %d key(s) with no schema entry in settings_store.go: %v",
			len(missing), missing)
	}
}

// TestSchemaDefaultsMatchRuntime is the symmetric direction: every
// schema key (other than empty-default string/secret entries) must have
// a runtime default registered. The init() check already enforces this
// for the numeric/boolean/enum/prompt subsets; this test extends to
// catch any future drift outside those types.
//
// string and secret entries with no value are skipped: the cascade
// resolver naturally returns "" when no row exists, so a missing
// settingDefaults entry is operationally equivalent to a registered
// empty string. Adding the entry would be cosmetic.
func TestSchemaDefaultsMatchRuntime(t *testing.T) {
	for _, entry := range settingsSchemas {
		// Skip prompt-typed entries: their DefaultValue is filled in at
		// init time from service.GetDefault, so by construction they
		// agree with the runtime registry.
		if entry.Type == "prompt" {
			continue
		}
		if entry.Type == "string" || entry.Type == "secret" {
			continue
		}
		if _, ok := service.GetDefault(entry.Key); !ok {
			t.Errorf("schema entry %q (type %q) has no matching runtime default in service.settingDefaults",
				entry.Key, entry.Type)
		}
	}
}

// TestNumericSchemasHaveRange asserts that EVERY numeric setting
// carries Min/Max/Step on its schema entry. The UI's useSchemaRange
// hook reads those values; a missing Min/Max/Step would silently
// re-introduce hardcoded form input increments.
//
// New numeric keys must populate Min/Max/Step at registration time —
// there is no allow-list. If a key truly has no operator-meaningful
// range, the test failure is the cue to push back on whether it should
// be operator-tunable at all (vs. a code-internal constant).
func TestNumericSchemasHaveRange(t *testing.T) {
	for _, entry := range settingsSchemas {
		if entry.Type != "number" {
			continue
		}
		if entry.Min == nil || entry.Max == nil || entry.Step == nil {
			t.Errorf("numeric schema entry %q is missing Min/Max/Step "+
				"(min=%v max=%v step=%v); every numeric setting must declare "+
				"a UX-meaningful range",
				entry.Key, entry.Min, entry.Max, entry.Step)
		}
	}
}

// TestEverySettingCategoryMapsToGroup asserts that the parent-group taxonomy
// (settingsGroups, served to the admin UI) is a total, non-overlapping cover of
// every non-prompt setting. Without this, a setting whose category is not
// referenced by any group silently never renders on the Settings page — the
// exact drift that left recall/usage/export/mcp settings unreachable while the
// grouping lived hardcoded in the frontend.
//
// Prompt-typed entries (enrichment_prompts, dreaming_prompts) are exempt: they
// are edited on the Prompt Templates page, not the Settings page, and are
// deliberately absent from settingsGroups.
func TestEverySettingCategoryMapsToGroup(t *testing.T) {
	// category -> number of sub-sections referencing it.
	referenced := make(map[string]int)
	for _, g := range settingsGroups {
		for _, sub := range g.SubSections {
			referenced[sub.Category]++
		}
	}

	// A category claimed by more than one sub-section would render the same
	// settings in two places.
	for cat, n := range referenced {
		if n > 1 {
			t.Errorf("category %q is referenced by %d sub-sections; each "+
				"category must map to exactly one", cat, n)
		}
	}

	// Count non-prompt schema entries per category.
	schemaByCategory := make(map[string]int)
	for _, entry := range settingsSchemas {
		if entry.Type == "prompt" {
			continue
		}
		schemaByCategory[entry.Category]++
	}

	// Every non-prompt setting category must be claimed by a group.
	unmapped := []string{}
	for cat := range schemaByCategory {
		if referenced[cat] == 0 {
			unmapped = append(unmapped, cat)
		}
	}
	if len(unmapped) > 0 {
		t.Errorf("%d setting categor(ies) map to no parent group in "+
			"settingsGroups: %v — add them or they never render on the "+
			"Settings page", len(unmapped), unmapped)
	}

	// Every sub-section must point at a category that actually has settings,
	// so a typo in settingsGroups surfaces as an empty tab is caught here.
	for cat := range referenced {
		if schemaByCategory[cat] == 0 {
			t.Errorf("group sub-section category %q has no non-prompt schema "+
				"entries; remove the dangling sub-section or fix the typo", cat)
		}
	}
}
