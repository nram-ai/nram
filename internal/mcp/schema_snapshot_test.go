package mcp

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nram-ai/nram/internal/service"
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
	desc, _ := schemaNodeAt(t, schemaFor[mcpAskResponse](), "confidence")["description"].(string)
	if desc == "" {
		t.Fatal("ask output schema publishes confidence with no description; an agent cannot tell grounding from correctness")
	}
	for _, want := range []string{"grounding", "correctness"} {
		if !strings.Contains(desc, want) {
			t.Errorf("confidence description must distinguish grounding from correctness; missing %q\ngot: %s", want, desc)
		}
	}
}

// TestSchemasDescribeFieldContracts pins the meanings published alongside the
// fields whose NAME understates, or actively misstates, their contract. Each
// one is a field where an omitted or defaulted value is the load-bearing
// signal, so an agent reading only names and types draws a specific wrong
// inference: a missing ask score read as a weak match rather than a source no
// cosine could be computed for, enriched:false read as a partial failure worth
// retrying rather than the invariant every fresh insert reports, a recall score
// read as a cosine and thresholded across calls rather than an unbounded
// composite comparable only within one response.
//
// Same reasoning as TestAskSchemaDescribesConfidence, which pins the ask
// confidence field, and deliberately not left to the snapshots for the same
// reason: regeneration would agree with a deletion. These read the description
// out of the derived schema instead.
func TestSchemasDescribeFieldContracts(t *testing.T) {
	// Derived once per type, not once per row: reflecting mcpRecallResponse
	// walks every nested graph and memory struct.
	schemas := map[string]json.RawMessage{
		"ask":              schemaFor[mcpAskResponse](),
		"store":            schemaFor[mcpStoreResponse](),
		"update":           schemaFor[mcpUpdateResponse](),
		"recall":           schemaFor[mcpRecallResponse](),
		"procedural_fetch": schemaFor[mcpProceduralFetchResponse](),
		"graph":            schemaFor[graphResponse](),
	}

	cases := []struct {
		tool string
		path []string
		want string
	}{
		{"ask", []string{"sources", "score"}, "Absent"},
		{"ask", []string{"sources", "citation"}, "cited nothing"},
		{"ask", []string{"synthesis_meta", "synthesis_failed"}, "retry"},
		{"store", []string{"enriched"}, "ALWAYS false"},
		{"store", []string{"enrichment_queued"}, "dedup"},
		{"update", []string{"id"}, "NEW row"},
		{"update", []string{"superseded"}, "copy-on-write"},
		{"recall", []string{"memories", "score"}, "not a cosine"},
		{"recall", []string{"memories", "confidence"}, "INPUT to score"},
		{"recall", []string{"memories", "low_novelty"}, "redundancy"},
		{"recall", []string{"coverage_gaps", "cause"}, "filtered or capped"},
		{"procedural_fetch", []string{"count"}, "not the total"},
		{"graph", []string{"relationships", "valid_until"}, "still holds"},
	}

	for _, tc := range cases {
		name := tc.tool + "/" + strings.Join(tc.path, ".")
		t.Run(name, func(t *testing.T) {
			node := schemaNodeAt(t, schemas[tc.tool], tc.path...)
			desc, _ := node["description"].(string)
			if desc == "" {
				t.Fatalf("%s output schema publishes %s with no description; an agent cannot read the contract off the name alone",
					tc.tool, strings.Join(tc.path, "."))
			}
			if !strings.Contains(desc, tc.want) {
				t.Errorf("%s description must carry %q\ngot: %s", name, tc.want, desc)
			}
		})
	}
}

// TestRecallSchemaPublishesCoverageGapCauses pins the coverage-gap cause codes
// into the schema itself. Cause is a closed set (CoverageCause* in
// internal/service/recall.go); published as a bare string, a caller has to guess
// the vocabulary, and the distinction the codes carry (the candidates existed
// but were filtered or capped) never reaches them at all.
func TestRecallSchemaPublishesCoverageGapCauses(t *testing.T) {
	node := schemaNodeAt(t, schemaFor[mcpRecallResponse](), "coverage_gaps", "cause")

	raw, ok := node["enum"].([]any)
	if !ok {
		t.Fatalf("recall output schema publishes coverage_gaps.cause with no enum; got %v", node)
	}
	got := make([]string, 0, len(raw))
	for _, v := range raw {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("coverage_gaps.cause enum holds non-string %v (%T)", v, v)
		}
		got = append(got, s)
	}

	want := []string{
		service.CoverageCauseTagFilter,
		service.CoverageCauseThreshold,
		service.CoverageCauseLimit,
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("coverage_gaps.cause enum = %v; want %v", got, want)
	}
}

// schemaNodeAt walks a derived output schema to the node at path, where each
// element is a property name. Array hops are implicit: a node carrying "items"
// is descended into before the next property lookup, so the caller writes
// "sources", "score" rather than threading "items" through every path. Relies
// on schemaFor's DoNotReference, which inlines every nested struct, so there are
// no $refs to resolve.
func schemaNodeAt(t *testing.T, raw json.RawMessage, path ...string) map[string]any {
	t.Helper()
	if raw == nil {
		t.Fatal("schemaFor returned nil; cannot walk the schema")
	}
	var node map[string]any
	if err := json.Unmarshal(raw, &node); err != nil {
		t.Fatalf("unmarshal output schema: %v", err)
	}

	for i, name := range path {
		if items, ok := node["items"].(map[string]any); ok {
			node = items
		}
		props, ok := node["properties"].(map[string]any)
		if !ok {
			t.Fatalf("schema node at %v has no properties; cannot reach %q", path[:i], name)
		}
		next, ok := props[name].(map[string]any)
		if !ok {
			t.Fatalf("schema has no property %q under %v", name, path[:i])
		}
		node = next
	}
	return node
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
