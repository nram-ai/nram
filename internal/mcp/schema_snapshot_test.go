package mcp

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
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
	assertSchemaSnapshot(t, "graphResponse", "graph_response_schema.json", schemaFor[graphResponse]())
}

// TestRecallResponseSchemaSnapshot pins the MCP output schema published for
// the recall tool. Like the graph schema it is auto-derived at registration
// time via schemaFor[mcpRecallResponse]() and shipped in tools/list, where
// strict clients (Claude.ai, Cursor) may validate tool results against it.
// The per-memory element type is recallview.Memory (aliased as
// mcpRecallMemory), so any change to the recalled-memory wire shape
// (confidence, low_novelty, derived_from, the dropped internal fields) must be
// reflected here, or it is a silent contract change.
//
// Run with -update-schema-snapshots after an intentional change to
// mcpRecallResponse / recallview.Memory / truncationInfo / graphResponse and
// commit the regenerated testdata file alongside the code change.
func TestRecallResponseSchemaSnapshot(t *testing.T) {
	assertSchemaSnapshot(t, "mcpRecallResponse", "recall_response_schema.json", schemaFor[mcpRecallResponse]())
}

// TestAskResponseSchemaSnapshot pins the MCP output schema published for the
// ask tool. Like the graph and recall schemas it is auto-derived at
// registration time via schemaFor[mcpAskResponse]() and shipped in tools/list.
//
// The confidence field's jsonschema_description is guarded by
// TestAskSchemaDescribesConfidence, not by this snapshot, which regeneration
// would happily rewrite.
//
// Run with -update-schema-snapshots after an intentional change to
// mcpAskResponse / mcpAskSource / mcpAskSynthesisMeta and commit the
// regenerated testdata file alongside the code change.
func TestAskResponseSchemaSnapshot(t *testing.T) {
	assertSchemaSnapshot(t, "mcpAskResponse", "ask_response_schema.json", schemaFor[mcpAskResponse]())
}

// assertSchemaSnapshot compares a tool's derived output schema against its
// committed snapshot, or rewrites that snapshot under -update-schema-snapshots.
// typeName names the reflected struct in failure messages; fileBase is the
// snapshot's name under testdata/.
//
// Note what a snapshot does and does not buy. It catches unintended shape
// drift, which is the common case. It does not pin any individual field,
// because the documented repair for a failure is regeneration: delete a field's
// description, regenerate, and the snapshot agrees with the deletion. A fact
// that must survive regeneration needs its own assertion (see
// TestAskSchemaDescribesConfidence).
func assertSchemaSnapshot(t *testing.T, typeName, fileBase string, actual json.RawMessage) {
	t.Helper()
	if actual == nil {
		t.Fatalf("schemaFor[%s]() returned nil; cannot snapshot", typeName)
	}

	actualCanon := canonicalizeJSON(t, actual)
	snapshotPath := filepath.Join("testdata", fileBase)

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

	if expectedCanon := canonicalizeJSON(t, expected); !bytes.Equal(actualCanon, expectedCanon) {
		t.Errorf("%s output schema drift; run -update-schema-snapshots after intentional change\n--- expected\n%s\n--- actual\n%s",
			typeName, string(expectedCanon), string(actualCanon))
	}
}

// TestAskSchemaDescribesConfidence pins the meaning published alongside the ask
// tool's confidence score: it is grounding strength, not correctness. An agent
// reading the output schema is deciding how far to trust the answer, and a bare
// number named "confidence" reads as a correctness probability, which it is not.
//
// Deliberately not left to TestAskResponseSchemaSnapshot. That test regenerates
// on demand, so dropping the jsonschema_description tag and running the
// documented repair would pass green. This assertion reads the field out of the
// derived schema, so it fails on the deletion however the snapshot is refreshed.
func TestAskSchemaDescribesConfidence(t *testing.T) {
	var schema struct {
		Properties map[string]struct {
			Description string `json:"description"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(schemaFor[mcpAskResponse](), &schema); err != nil {
		t.Fatalf("unmarshal ask output schema: %v", err)
	}

	desc := schema.Properties["confidence"].Description
	if desc == "" {
		t.Fatal("ask output schema publishes confidence with no description; an agent cannot tell grounding from correctness")
	}
	for _, want := range []string{"grounding", "correctness"} {
		if !strings.Contains(desc, want) {
			t.Errorf("confidence description must distinguish grounding from correctness; missing %q\ngot: %s", want, desc)
		}
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
