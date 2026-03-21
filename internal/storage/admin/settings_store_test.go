package admin

import (
	"context"
	"testing"
)

func TestGetSettingsSchemaContainsQdrantEntries(t *testing.T) {
	store := &SettingsAdminStore{settingsRepo: nil}
	schemas, err := store.GetSettingsSchema(context.Background())
	if err != nil {
		t.Fatalf("GetSettingsSchema returned error: %v", err)
	}

	// Collect qdrant entries.
	var qdrantSchemas []struct {
		key  string
		typ  string
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
