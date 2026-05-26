package admin

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

func TestGetSettingsSchemaContainsQdrantEntries(t *testing.T) {
	store := &SettingsAdminStore{settingsRepo: nil}
	schemas, err := store.GetSettingsSchema(context.Background())
	if err != nil {
		t.Fatalf("GetSettingsSchema returned error: %v", err)
	}

	// Collect qdrant entries.
	var qdrantSchemas []struct {
		key string
		typ string
	}
	for _, s := range schemas {
		if s.Category == "qdrant" {
			qdrantSchemas = append(qdrantSchemas, struct {
				key string
				typ string
			}{key: s.Key, typ: s.Type})
		}
	}

	if len(qdrantSchemas) != 6 {
		t.Fatalf("expected 6 qdrant schema entries, got %d", len(qdrantSchemas))
	}

	// Build a lookup for type assertions.
	byKey := make(map[string]string)
	for _, q := range qdrantSchemas {
		byKey[q.key] = q.typ
	}

	// Verify specific key/type pairs.
	checks := []struct {
		key      string
		wantType string
	}{
		{"qdrant.addr", "string"},
		{"qdrant.api_key", "secret"},
		{"qdrant.use_tls", "boolean"},
	}
	for _, c := range checks {
		got, ok := byKey[c.key]
		if !ok {
			t.Errorf("expected qdrant schema entry %q to exist", c.key)
			continue
		}
		if got != c.wantType {
			t.Errorf("expected %q type %q, got %q", c.key, c.wantType, got)
		}
	}
}

// uiOnlySchemaKeys are entries registered by the admin UI that intentionally
// have no service.GetDefault counterpart. The qdrant string/secret entries
// fall back to "" both at the UI and at service.Resolve (which returns "" for
// any unregistered key); the others have no runtime call site outside the
// schema registry. Adding a runtime default for any of these without removing
// the key from this allowlist will fail the consistency check below — the
// inverse drift this test also locks down.
var uiOnlySchemaKeys = map[string]struct{}{
	"qdrant.addr":    {},
	"qdrant.api_key": {},
}

// TestSettingsSchemaRequiresRestart asserts that the RequiresRestart flag is
// set on every entry that genuinely needs a server restart and cleared on
// entries that are hot-reloadable. The UI reads this flag to render the
// "Requires a server restart" badge, so drift here misleads operators.
func TestSettingsSchemaRequiresRestart(t *testing.T) {
	store := &SettingsAdminStore{settingsRepo: nil}
	schemas, err := store.GetSettingsSchema(context.Background())
	if err != nil {
		t.Fatalf("GetSettingsSchema: %v", err)
	}

	// Keys that the runtime reads only at process start. Changing them in the
	// admin UI must surface the restart badge; if an entry here ever flips to
	// hot-reloadable, remove it from this list at the same commit.
	mustRestart := map[string]struct{}{
		"qdrant.addr":                                      {},
		"qdrant.api_key":                                   {},
		"qdrant.use_tls":                                   {},
		"qdrant.pool_size":                                 {},
		"qdrant.keepalive_time":                            {},
		"qdrant.keepalive_timeout":                         {},
		service.SettingEnrichmentWorkerCountSQLite:         {},
		service.SettingEnrichmentWorkerCountPostgres:       {},
		service.SettingEnrichmentWorkerPollIntervalSeconds: {},
		service.SettingEnrichmentPoolTickIntervalSeconds:   {},
		service.SettingEnrichmentHeartbeatSeconds:          {},
		service.SettingEnrichmentStuckSweep:                {},
		service.SettingDreamSchedulerPollSeconds:           {},
		service.SettingDreamHeartbeatInterval:              {},
		service.SettingDreamStuckSweep:                     {},
		// SettingLifecycleSweepIntervalSeconds is hot-reloadable: the
		// lifecycle loop re-reads it on every iteration (see
		// internal/service/lifecycle.go resolveSweepInterval).
		service.SettingCascadeCacheTTLSeconds:     {},
		service.SettingSettingsCacheTTLSeconds:    {},
		service.SettingAPIRateLimitCleanupSeconds: {},
		service.SettingAPIRateLimitStaleSeconds:   {},
		service.SettingEventsSubscriberBufferSize: {},
		service.SettingEventsReplayCapacity:       {},
		service.SettingEventsSSEKeepaliveSeconds:  {},
		service.SettingHNSWM:                      {},
		service.SettingHNSWEfConstruction:         {},
		service.SettingHNSWEfSearch:               {},
		service.SettingHNSWMaxLoadedIndexes:       {},
	}

	seen := make(map[string]bool, len(mustRestart))
	for _, entry := range schemas {
		if _, want := mustRestart[entry.Key]; want {
			seen[entry.Key] = true
			if !entry.RequiresRestart {
				t.Errorf("key %q: expected RequiresRestart=true, got false", entry.Key)
			}
			continue
		}
		if entry.RequiresRestart {
			t.Errorf("key %q: RequiresRestart=true but not on the mustRestart list; either flag a real restart requirement or remove the field", entry.Key)
		}
	}

	for key := range mustRestart {
		if !seen[key] {
			t.Errorf("key %q is on mustRestart but no schema entry was found; was it renamed?", key)
		}
	}
}

// TestSettingsSchemaDefaultsMatchRuntime asserts that every UI schema entry's
// DefaultValue matches the runtime default returned by service.GetDefault.
// This catches the class of bug where a setting is registered in the UI and
// consumed at runtime, but settingDefaults is never updated — Resolve then
// returns "", ResolveBool returns false, and the feature silently disables
// itself on a fresh install.
func TestSettingsSchemaDefaultsMatchRuntime(t *testing.T) {
	store := &SettingsAdminStore{settingsRepo: nil}
	schemas, err := store.GetSettingsSchema(context.Background())
	if err != nil {
		t.Fatalf("GetSettingsSchema: %v", err)
	}

	for _, entry := range schemas {
		runtime, hasRuntime := service.GetDefault(entry.Key)

		if _, uiOnly := uiOnlySchemaKeys[entry.Key]; uiOnly {
			if hasRuntime {
				t.Errorf("key %q is on the UI-only allowlist but service.GetDefault now returns a value (%q); remove it from uiOnlySchemaKeys", entry.Key, runtime)
			}
			continue
		}

		if !hasRuntime {
			t.Errorf("key %q is registered in the UI schema with default %s but has no service.GetDefault entry; add it to settingDefaults or to uiOnlySchemaKeys", entry.Key, string(entry.DefaultValue))
			continue
		}

		switch entry.Type {
		case "boolean":
			var uiVal bool
			if err := json.Unmarshal(entry.DefaultValue, &uiVal); err != nil {
				t.Errorf("key %q: cannot decode UI default %s as bool: %v", entry.Key, string(entry.DefaultValue), err)
				continue
			}
			runtimeVal, err := strconv.ParseBool(runtime)
			if err != nil {
				t.Errorf("key %q: cannot parse runtime default %q as bool: %v", entry.Key, runtime, err)
				continue
			}
			if uiVal != runtimeVal {
				t.Errorf("key %q: UI default %v != runtime default %v", entry.Key, uiVal, runtimeVal)
			}
		case "number":
			var uiVal float64
			if err := json.Unmarshal(entry.DefaultValue, &uiVal); err != nil {
				t.Errorf("key %q: cannot decode UI default %s as number: %v", entry.Key, string(entry.DefaultValue), err)
				continue
			}
			runtimeVal, err := strconv.ParseFloat(runtime, 64)
			if err != nil {
				t.Errorf("key %q: cannot parse runtime default %q as float: %v", entry.Key, runtime, err)
				continue
			}
			if math.Abs(uiVal-runtimeVal) > 1e-9 {
				t.Errorf("key %q: UI default %v != runtime default %v", entry.Key, uiVal, runtimeVal)
			}
		case "string", "secret", "enum", "prompt":
			var uiVal string
			if err := json.Unmarshal(entry.DefaultValue, &uiVal); err != nil {
				t.Errorf("key %q: cannot decode UI default %s as string: %v", entry.Key, string(entry.DefaultValue), err)
				continue
			}
			if uiVal != runtime {
				t.Errorf("key %q: UI default %q != runtime default %q", entry.Key, uiVal, runtime)
			}
		case "json":
			var uiVal, runtimeVal any
			if err := json.Unmarshal(entry.DefaultValue, &uiVal); err != nil {
				t.Errorf("key %q: cannot decode UI default %s as JSON: %v", entry.Key, string(entry.DefaultValue), err)
				continue
			}
			if err := json.Unmarshal([]byte(runtime), &runtimeVal); err != nil {
				t.Errorf("key %q: cannot decode runtime default %q as JSON: %v", entry.Key, runtime, err)
				continue
			}
			if !reflect.DeepEqual(uiVal, runtimeVal) {
				t.Errorf("key %q: UI default %s != runtime default %q", entry.Key, string(entry.DefaultValue), runtime)
			}
		default:
			t.Errorf("key %q: unhandled schema type %q in defaults consistency test", entry.Key, entry.Type)
		}
	}
}

// TestResetSetting_GlobalUpsertsDefault asserts that ResetSetting at scope
// "global" writes the schema's canonical default back to (key, "global").
// updated_by is intentionally nil here so the test does not depend on a
// users row existing — production callers pass the admin UUID, and the FK
// constraint on settings.updated_by ensures only real users land in the
// column.
func TestResetSetting_GlobalUpsertsDefault(t *testing.T) {
	for _, backend := range adminTestBackends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.setup(t)
			ctx := context.Background()
			repo := storage.NewSettingsRepo(db)
			store := NewSettingsAdminStore(repo, nil)

			// Pre-set a non-default value so the reset has something to revert.
			// enrichment.enabled defaults to true.
			const key = "enrichment.enabled"
			if err := repo.Set(ctx, &model.Setting{
				Key: key, Value: json.RawMessage(`false`), Scope: "global",
			}); err != nil {
				t.Fatalf("seed Set: %v", err)
			}

			if err := store.ResetSetting(ctx, key, "global", nil); err != nil {
				t.Fatalf("ResetSetting: %v", err)
			}

			got, err := repo.Get(ctx, key, "global")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if string(got.Value) != "true" {
				t.Errorf("expected value true after reset, got %s", string(got.Value))
			}
		})
	}
}

// TestResetSetting_NonGlobalDeletes asserts that ResetSetting at a
// non-global scope deletes the override row so the cascade resolver falls
// back to global. Mirrors the behavior the API documents for project-scope
// resets even though the current UI only ever calls with scope="global".
func TestResetSetting_NonGlobalDeletes(t *testing.T) {
	for _, backend := range adminTestBackends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.setup(t)
			ctx := context.Background()
			repo := storage.NewSettingsRepo(db)
			store := NewSettingsAdminStore(repo, nil)

			const key = "enrichment.enabled"
			const projectScope = "project:abc"
			// Seed a project override and a global value.
			if err := repo.Set(ctx, &model.Setting{
				Key: key, Value: json.RawMessage(`true`), Scope: "global",
			}); err != nil {
				t.Fatalf("seed Set global: %v", err)
			}
			if err := repo.Set(ctx, &model.Setting{
				Key: key, Value: json.RawMessage(`false`), Scope: projectScope,
			}); err != nil {
				t.Fatalf("seed Set project: %v", err)
			}

			if err := store.ResetSetting(ctx, key, projectScope, nil); err != nil {
				t.Fatalf("ResetSetting: %v", err)
			}

			// Cascade-aware Get from the project scope must now return the
			// global row, not the deleted project row.
			got, err := repo.Get(ctx, key, projectScope)
			if err != nil {
				t.Fatalf("Get after reset: %v", err)
			}
			if got.Scope != "global" {
				t.Errorf("expected cascade to global, got scope %q", got.Scope)
			}
			if string(got.Value) != "true" {
				t.Errorf("expected fallback value true, got %s", string(got.Value))
			}
		})
	}
}

// TestResetAllSettings_SkipsOmitFromResetAll proves that credentials and
// connection strings tagged OmitFromResetAll survive a bulk reset; the
// operator must explicitly target the key to wipe it. Catches the footgun
// where clicking "Reset all" silently wipes Qdrant credentials.
func TestResetAllSettings_SkipsOmitFromResetAll(t *testing.T) {
	for _, backend := range adminTestBackends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.setup(t)
			ctx := context.Background()
			repo := storage.NewSettingsRepo(db)
			store := NewSettingsAdminStore(repo, nil)

			// Seed an operator-set qdrant.addr value. The schema entry for
			// qdrant.addr is OmitFromResetAll=true, so the bulk reset must
			// leave the row untouched.
			const protectedKey = "qdrant.addr"
			operatorValue := json.RawMessage(`"qdrant.internal:6334"`)
			if err := repo.Set(ctx, &model.Setting{
				Key: protectedKey, Value: operatorValue, Scope: "global",
			}); err != nil {
				t.Fatalf("seed Set: %v", err)
			}

			if _, err := store.ResetAllSettings(ctx, "global", nil); err != nil {
				t.Fatalf("ResetAllSettings: %v", err)
			}

			got, err := repo.Get(ctx, protectedKey, "global")
			if err != nil {
				t.Fatalf("Get %s post-reset: %v", protectedKey, err)
			}
			if string(got.Value) != string(operatorValue) {
				t.Errorf("OmitFromResetAll-tagged key was overwritten; want %s got %s",
					string(operatorValue), string(got.Value))
			}

			// Per-key reset on a protected key still works — the operator has
			// to explicitly target it.
			if err := store.ResetSetting(ctx, protectedKey, "global", nil); err != nil {
				t.Fatalf("explicit ResetSetting on protected key: %v", err)
			}
			got, err = repo.Get(ctx, protectedKey, "global")
			if err != nil {
				t.Fatalf("Get %s post-explicit-reset: %v", protectedKey, err)
			}
			if string(got.Value) != `""` {
				t.Errorf("explicit reset should restore default \"\"; got %s", string(got.Value))
			}
		})
	}
}

// TestResetAllSettings_GlobalWritesEveryRegisteredKey asserts that
// ResetAllSettings at "global" writes the schema default for every key in
// the registry. Catches drift: if the registry adds a key but a reset path
// silently skips it, this test surfaces the gap.
func TestResetAllSettings_GlobalWritesEveryRegisteredKey(t *testing.T) {
	for _, backend := range adminTestBackends {
		t.Run(backend.name, func(t *testing.T) {
			db := backend.setup(t)
			ctx := context.Background()
			repo := storage.NewSettingsRepo(db)
			store := NewSettingsAdminStore(repo, nil)

			n, err := store.ResetAllSettings(ctx, "global", nil)
			if err != nil {
				t.Fatalf("ResetAllSettings: %v", err)
			}
			// Count expected resets: registry size minus OmitFromResetAll entries.
			want := 0
			for i := range settingsSchemas {
				if !settingsSchemas[i].OmitFromResetAll {
					want++
				}
			}
			if n != want {
				t.Errorf("reset count %d != eligible registry size %d", n, want)
			}

			// Spot-check that a representative key is now set to its registered default.
			got, err := repo.Get(ctx, "enrichment.enabled", "global")
			if err != nil {
				t.Fatalf("Get enrichment.enabled: %v", err)
			}
			if string(got.Value) != "true" {
				t.Errorf("enrichment.enabled should be true post-reset, got %s", string(got.Value))
			}
		})
	}
}
