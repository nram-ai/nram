package admin

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"testing"

	"github.com/nram-ai/nram/internal/service"
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
	"enrichment.batch_size":     {},
	"enrichment.auto_enrich":    {},
	"memory.default_confidence": {},
	"memory.default_importance": {},
	"api.rate_limit_rps":        {},
	"api.rate_limit_burst":      {},
	"qdrant.addr":               {},
	"qdrant.api_key":            {},
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
		default:
			t.Errorf("key %q: unhandled schema type %q in defaults consistency test", entry.Key, entry.Type)
		}
	}
}
