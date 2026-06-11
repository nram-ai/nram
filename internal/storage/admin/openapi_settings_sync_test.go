package admin

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/nram-ai/nram/internal/repopath"
	"gopkg.in/yaml.v3"
)

// TestOpenAPISettingKeysInSync asserts that the SettingKey enum in
// docs/openapi.yaml exactly matches the canonical Go registry
// (SettingsSchemas). It is the guard for the generator
// (scripts/gen_openapi_setting_keys): a setting added without regenerating the
// spec, or a spec edited by hand, fails here with the precise drift. Fix by
// running `go generate ./...`.
func TestOpenAPISettingKeysInSync(t *testing.T) {
	path := openapiPath(t)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var doc struct {
		Components struct {
			Schemas struct {
				SettingKey struct {
					Enum []string `yaml:"enum"`
				} `yaml:"SettingKey"`
			} `yaml:"schemas"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse openapi.yaml: %v", err)
	}

	spec := toSet(doc.Components.Schemas.SettingKey.Enum)
	registry := make(map[string]struct{})
	for _, s := range SettingsSchemas() {
		registry[s.Key] = struct{}{}
	}

	missing := diff(registry, spec) // in registry, absent from spec
	extra := diff(spec, registry)   // in spec, absent from registry
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("docs/openapi.yaml SettingKey enum is out of sync with admin.SettingsSchemas().\n"+
			"  missing from spec (run `go generate ./...`): %v\n"+
			"  stale in spec: %v", missing, extra)
	}
}

func openapiPath(t *testing.T) string {
	t.Helper()
	root, err := repopath.Root()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, "docs", "openapi.yaml")
}

func toSet(keys []string) map[string]struct{} {
	s := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		s[k] = struct{}{}
	}
	return s
}

func diff(a, b map[string]struct{}) []string {
	var out []string
	for k := range a {
		if _, ok := b[k]; !ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
