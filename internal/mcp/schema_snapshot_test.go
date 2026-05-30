package mcp

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var updateSchemaSnapshots = flag.Bool("update-schema-snapshots", false,
	"rewrite the captured MCP output-schema snapshots in testdata; run after an intentional response-shape change")

// TestGraphResponseSchemaSnapshot pins the MCP output schema published for
// the graph tool. The schema is auto-derived at registration time via
// schemaFor[graphResponse]() and shipped in tools/list, where strict
// clients (Claude.ai, Cursor) may validate tool results against it. A
// silent shape change is a silent contract break.
//
// Run with -update-schema-snapshots after an intentional change to the
// graphResponse / truncationInfo / graphEntity / graphRelationship struct
// shapes; commit the regenerated testdata file alongside the code change.
func TestGraphResponseSchemaSnapshot(t *testing.T) {
	actual := schemaFor[graphResponse]()
	if actual == nil {
		t.Fatal("schemaFor[graphResponse]() returned nil; cannot snapshot")
	}

	actualCanon := canonicalizeJSON(t, actual)

	snapshotPath := filepath.Join("testdata", "graph_response_schema.json")

	if *updateSchemaSnapshots {
		if err := os.MkdirAll(filepath.Dir(snapshotPath), 0o755); err != nil {
			t.Fatalf("create testdata dir: %v", err)
		}
		if err := os.WriteFile(snapshotPath, actualCanon, 0o644); err != nil {
			t.Fatalf("write snapshot: %v", err)
		}
		t.Logf("wrote schema snapshot to %s (%d bytes)", snapshotPath, len(actualCanon))
		return
	}

	expected, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot %s: %v (run with -update-schema-snapshots to create it)", snapshotPath, err)
	}
	expectedCanon := canonicalizeJSON(t, expected)

	if !bytes.Equal(actualCanon, expectedCanon) {
		t.Errorf("graphResponse output schema drift; run -update-schema-snapshots after intentional change\n--- expected\n%s\n--- actual\n%s",
			string(expectedCanon), string(actualCanon))
	}
}

// TestRecallResponseSchemaSnapshot pins the MCP output schema published for
// the recall tool. Like the graph schema it is auto-derived at registration
// time via schemaFor[mcpRecallResponse]() and shipped in tools/list, where
// strict clients (Claude.ai, Cursor) may validate tool results against it.
// The per-memory element type is recallview.Memory (aliased as
// mcpRecallMemory), so any change to the recalled-memory wire shape —
// confidence, low_novelty, derived_from, the dropped internal fields — must be
// reflected here, or it is a silent contract change.
//
// Run with -update-schema-snapshots after an intentional change to
// mcpRecallResponse / recallview.Memory / truncationInfo / graphResponse and
// commit the regenerated testdata file alongside the code change.
func TestRecallResponseSchemaSnapshot(t *testing.T) {
	actual := schemaFor[mcpRecallResponse]()
	if actual == nil {
		t.Fatal("schemaFor[mcpRecallResponse]() returned nil; cannot snapshot")
	}

	actualCanon := canonicalizeJSON(t, actual)

	snapshotPath := filepath.Join("testdata", "recall_response_schema.json")

	if *updateSchemaSnapshots {
		if err := os.MkdirAll(filepath.Dir(snapshotPath), 0o755); err != nil {
			t.Fatalf("create testdata dir: %v", err)
		}
		if err := os.WriteFile(snapshotPath, actualCanon, 0o644); err != nil {
			t.Fatalf("write snapshot: %v", err)
		}
		t.Logf("wrote schema snapshot to %s (%d bytes)", snapshotPath, len(actualCanon))
		return
	}

	expected, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot %s: %v (run with -update-schema-snapshots to create it)", snapshotPath, err)
	}
	expectedCanon := canonicalizeJSON(t, expected)

	if !bytes.Equal(actualCanon, expectedCanon) {
		t.Errorf("mcpRecallResponse output schema drift; run -update-schema-snapshots after intentional change\n--- expected\n%s\n--- actual\n%s",
			string(expectedCanon), string(actualCanon))
	}
}

// canonicalizeJSON re-marshals JSON through Unmarshal/MarshalIndent so two
// inputs that differ only in whitespace, key ordering inside maps, or
// floating-point representation compare equal byte-for-byte.
func canonicalizeJSON(t *testing.T, in []byte) []byte {
	t.Helper()
	var v any
	if err := json.Unmarshal(in, &v); err != nil {
		t.Fatalf("canonicalize unmarshal: %v\ninput:\n%s", err, in)
	}
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("canonicalize marshal: %v", err)
	}
	return out
}
