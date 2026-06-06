package dreaming

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// Tests covering the metadata-preservation contract every dream-phase stamp
// writer must honor: a row that carries `source_memory_ids` (or any other
// non-stamp field) on disk MUST still carry it after the stamp write.
//
// The bug these tests guard: four "stale collector" optimizers passed an
// empty meta map downstream when a row's stamp marker was absent in the
// raw bytes. Five matching stamp writers marshalled `empty + new_stamp`
// and persisted via UpdateMetadata, which is full-column overwrite, not
// JSONB merge. Result: the first stamp write on a freshly-created
// synthesis wiped source_memory_ids and dream_cycle_id, and the next
// novelty audit demoted the row as `orphan_no_sources`. Cluster
// fingerprints rotated, paraphrase chains formed, consolidation residual
// stayed true forever.
//
// The fix combines (a) full-decode in every collectStale and
// (b) defensive merge inside every stamp writer via encodeStampWrite. The
// tests below pin both halves: caller passes empty meta on purpose, and
// the on-disk fields must still survive.

// --- helpers ---

// freshSynthesis builds a model.Memory whose metadata mirrors what
// phase_consolidation.go's consolidate sub-phase writes at synthesis
// creation time: a dream-source row carrying source_memory_ids and
// dream_cycle_id, no stamp markers yet.
func freshSynthesis(t *testing.T, sourceIDs []uuid.UUID) model.Memory {
	t.Helper()
	src := model.DreamSource
	cycleID := uuid.New()
	ids := make([]string, len(sourceIDs))
	for i, id := range sourceIDs {
		ids[i] = id.String()
	}
	raw, err := json.Marshal(map[string]any{
		model.DreamMetaCycleID:         cycleID.String(),
		model.DreamMetaSourceMemoryIDs: ids,
	})
	if err != nil {
		t.Fatalf("marshal seed metadata: %v", err)
	}
	now := time.Now().UTC()
	return model.Memory{
		ID:          uuid.New(),
		NamespaceID: uuid.New(),
		Source:      &src,
		Content:     "synthesis content",
		Confidence:  0.3,
		Metadata:    raw,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// configurableLineageWriter lets a test feed canned parent IDs to the
// audit's lineage fallback path. Counts lookups so tests can assert the
// fallback was actually exercised.
type configurableLineageWriter struct {
	parentsByMemoryID map[uuid.UUID][]uuid.UUID
	parentLookups     int
}

func (c *configurableLineageWriter) Create(_ context.Context, _ *model.MemoryLineage) error {
	return nil
}
func (c *configurableLineageWriter) CountConflictsBetween(_ context.Context, _, _, _ uuid.UUID) (int, error) {
	return 0, nil
}
func (c *configurableLineageWriter) FindParentIDsByRelation(_ context.Context, _, memoryID uuid.UUID, relation string) ([]uuid.UUID, error) {
	c.parentLookups++
	if relation != model.LineageSynthesizedFrom {
		return nil, nil
	}
	return c.parentsByMemoryID[memoryID], nil
}

// assertHasSourceMemoryIDs decodes the JSON bytes the writer persisted
// and fails the test if source_memory_ids is missing or differs from
// expected.
func assertHasSourceMemoryIDs(t *testing.T, label string, raw json.RawMessage, expected []uuid.UUID) {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("%s: unmarshal persisted metadata: %v\nraw=%s", label, err, string(raw))
	}
	arrRaw, ok := got["source_memory_ids"]
	if !ok {
		t.Fatalf("%s: persisted metadata is missing source_memory_ids; raw=%s", label, string(raw))
	}
	arr, ok := arrRaw.([]any)
	if !ok {
		t.Fatalf("%s: source_memory_ids is not a JSON array; raw=%s", label, string(raw))
	}
	if len(arr) != len(expected) {
		t.Fatalf("%s: source_memory_ids length=%d, want %d; raw=%s", label, len(arr), len(expected), string(raw))
	}
	for i, want := range expected {
		gotStr, _ := arr[i].(string)
		if gotStr != want.String() {
			t.Fatalf("%s: source_memory_ids[%d]=%q, want %q", label, i, gotStr, want.String())
		}
	}
}

// --- low-level helper test ---

// TestEncodeStampWrite_PreservesOnDiskFieldsWhenCallerPassesEmpty pins the
// defensive contract: even when the caller's updates map is empty,
// encodeStampWrite must produce JSON that still contains every key that
// was on disk. This is the bug's load-bearing failure mode.
func TestEncodeStampWrite_PreservesOnDiskFieldsWhenCallerPassesEmpty(t *testing.T) {
	onDisk := json.RawMessage(`{
		"source_memory_ids": ["a", "b", "c"],
		"dream_cycle_id":    "ddd",
		"some_other_field":  42
	}`)
	encoded, err := encodeStampWrite(onDisk, map[string]any{})
	if err != nil {
		t.Fatalf("encodeStampWrite: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	for _, key := range []string{"source_memory_ids", "dream_cycle_id", "some_other_field"} {
		if _, ok := got[key]; !ok {
			t.Errorf("encoded result missing on-disk key %q; got=%s", key, string(encoded))
		}
	}
}

// TestEncodeStampWrite_CallerOverridesOnDiskKeys pins the precedence rule:
// when caller's updates map carries a key already on disk, the caller wins.
// This matches the pattern every stamp writer uses: the writer adds its
// stamp key and trusts the merge to layer it over whatever was on disk.
func TestEncodeStampWrite_CallerOverridesOnDiskKeys(t *testing.T) {
	onDisk := json.RawMessage(`{"reinforce_checked_at": "2026-01-01T00:00:00Z", "source_memory_ids": ["a"]}`)
	encoded, err := encodeStampWrite(onDisk, map[string]any{
		"reinforce_checked_at": "2026-05-03T20:00:00Z",
	})
	if err != nil {
		t.Fatalf("encodeStampWrite: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["reinforce_checked_at"] != "2026-05-03T20:00:00Z" {
		t.Errorf("caller value should win; got reinforce_checked_at=%v", got["reinforce_checked_at"])
	}
	if _, ok := got["source_memory_ids"]; !ok {
		t.Errorf("untouched on-disk key must survive; got=%s", string(encoded))
	}
}

// --- collectStale + stamp writer integration tests ---

// TestCollectReinforceStale_DecodesFreshSynthesisMetadata pins the upstream
// fix in collectReinforceStale: even when no stamp marker is present, the
// returned staleSynthesis must carry the row's actual on-disk metadata,
// not an empty map. Without this, the first reinforce stamp write wipes
// source_memory_ids.
func TestCollectReinforceStale_DecodesFreshSynthesisMetadata(t *testing.T) {
	sourceIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	mem := freshSynthesis(t, sourceIDs)

	stale := collectReinforceStale([]model.Memory{mem})
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale synthesis, got %d", len(stale))
	}
	gotIDs, ok := stale[0].meta["source_memory_ids"]
	if !ok {
		t.Fatalf("stale.meta is missing source_memory_ids: collectReinforceStale dropped on-disk fields by passing an empty map")
	}
	arr, _ := gotIDs.([]any)
	if len(arr) != len(sourceIDs) {
		t.Fatalf("stale.meta source_memory_ids length=%d, want %d", len(arr), len(sourceIDs))
	}
}

// TestStampReinforce_PreservesSourceMemoryIDs is the canonical regression
// test for the metadata-clobber bug. It exercises the end-to-end path
// (collectReinforceStale → stampReinforce → UpdateMetadata) on a freshly-
// created synthesis and asserts source_memory_ids survives the round trip.
func TestStampReinforce_PreservesSourceMemoryIDs(t *testing.T) {
	sourceIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	mem := freshSynthesis(t, sourceIDs)
	writer := &updatingMemoryWriter{}
	phase := NewConsolidationPhase(
		&fakeMemoryReader{},
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return nil },
		func() provider.EmbeddingProvider { return nil },
		noveltySettings(false),
		nil,
	)

	stale := collectReinforceStale([]model.Memory{mem})
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale synthesis, got %d", len(stale))
	}
	phase.stampReinforce(context.Background(), &stale[0].mem, stale[0].meta)

	if len(writer.metadataUpdates) != 1 {
		t.Fatalf("expected 1 UpdateMetadata call, got %d", len(writer.metadataUpdates))
	}
	assertHasSourceMemoryIDs(t, "stampReinforce", writer.metadataUpdates[0].Metadata, sourceIDs)
}

// TestStampContradictionsChecked_PreservesSourceMemoryIDs exercises the
// contradiction phase's stale-collect → stamp path. Same invariant as
// the reinforce test: source_memory_ids on disk must survive the stamp.
func TestStampContradictionsChecked_PreservesSourceMemoryIDs(t *testing.T) {
	sourceIDs := []uuid.UUID{uuid.New(), uuid.New()}
	mem := freshSynthesis(t, sourceIDs)
	writer := &updatingMemoryWriter{}
	phase := NewContradictionPhase(
		&fakeMemoryReader{},
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return nil },
		func() provider.EmbeddingProvider { return nil },
		stubSettings{},
	)

	stale := phase.collectStale([]model.Memory{mem})
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale memory, got %d", len(stale))
	}
	if err := phase.stampContradictionsChecked(context.Background(), &stale[0].Mem, stale[0].Meta); err != nil {
		t.Fatalf("stampContradictionsChecked: %v", err)
	}

	if len(writer.metadataUpdates) != 1 {
		t.Fatalf("expected 1 UpdateMetadata call, got %d", len(writer.metadataUpdates))
	}
	assertHasSourceMemoryIDs(t, "stampContradictionsChecked", writer.metadataUpdates[0].Metadata, sourceIDs)
}

// TestStampReinforce_RecoversFromCallerPassingEmptyMeta locks the
// defense-in-depth backstop. Even if a future caller (or a bug) passes
// an empty meta map directly to stampReinforce, the writer must still
// preserve on-disk fields by merging through encodeStampWrite. Without
// the backstop this regresses the original bug.
func TestStampReinforce_RecoversFromCallerPassingEmptyMeta(t *testing.T) {
	sourceIDs := []uuid.UUID{uuid.New(), uuid.New()}
	mem := freshSynthesis(t, sourceIDs)
	writer := &updatingMemoryWriter{}
	phase := NewConsolidationPhase(
		&fakeMemoryReader{},
		writer,
		stubLineageWriter{},
		func() provider.LLMProvider { return nil },
		func() provider.EmbeddingProvider { return nil },
		noveltySettings(false),
		nil,
	)

	// Caller passes a deliberately-empty meta map.
	phase.stampReinforce(context.Background(), &mem, map[string]any{})

	if len(writer.metadataUpdates) != 1 {
		t.Fatalf("expected 1 UpdateMetadata call, got %d", len(writer.metadataUpdates))
	}
	assertHasSourceMemoryIDs(t, "stampReinforce-empty-meta", writer.metadataUpdates[0].Metadata, sourceIDs)
}

// --- audit lineage fallback tests ---

// TestResolveSourceMemoryIDs_FallsBackToLineage covers the runtime self-heal
// for any synthesis whose metadata.source_memory_ids was lost (historical
// damage from the metadata-clobber bug, or a future regression). With at
// least one parent in the lineage table, resolveSourceMemoryIDs must:
//  1. return the lineage parents as the source IDs, and
//  2. write them back into the row's metadata so next cycle takes the
//     fast path without re-running the lineage query.
func TestResolveSourceMemoryIDs_FallsBackToLineage(t *testing.T) {
	mem := freshSynthesis(t, nil) // empty source_memory_ids
	parentIDs := []uuid.UUID{uuid.New(), uuid.New()}

	writer := &updatingMemoryWriter{}
	lineage := &configurableLineageWriter{
		parentsByMemoryID: map[uuid.UUID][]uuid.UUID{mem.ID: parentIDs},
	}
	phase := NewConsolidationPhase(
		&fakeMemoryReader{},
		writer,
		lineage,
		func() provider.LLMProvider { return nil },
		func() provider.EmbeddingProvider { return nil },
		noveltySettings(false),
		nil,
	)

	meta := decodeMetadata(mem.Metadata)
	got := phase.resolveSourceMemoryIDs(context.Background(), &mem, meta)
	if len(got) != len(parentIDs) {
		t.Fatalf("expected %d source IDs from lineage fallback, got %d", len(parentIDs), len(got))
	}
	if lineage.parentLookups == 0 {
		t.Fatalf("lineage fallback should have queried memory_lineage, but FindParentIDsByRelation was never called")
	}
	if len(writer.metadataUpdates) != 1 {
		t.Fatalf("self-heal must write source_memory_ids back to metadata; got %d UpdateMetadata calls", len(writer.metadataUpdates))
	}
	assertHasSourceMemoryIDs(t, "resolveSourceMemoryIDs-self-heal",
		writer.metadataUpdates[0].Metadata, parentIDs)
}

// TestResolveSourceMemoryIDs_NoFallbackWhenMetadataAlreadyHasIDs proves the
// fallback is skipped on the fast path: when metadata.source_memory_ids
// is already populated, the lineage table is not queried (avoids per-row
// DB load on every audit cycle).
func TestResolveSourceMemoryIDs_NoFallbackWhenMetadataAlreadyHasIDs(t *testing.T) {
	sourceIDs := []uuid.UUID{uuid.New(), uuid.New()}
	mem := freshSynthesis(t, sourceIDs)

	writer := &updatingMemoryWriter{}
	lineage := &configurableLineageWriter{
		parentsByMemoryID: map[uuid.UUID][]uuid.UUID{mem.ID: {uuid.New()}}, // would be wrong if used
	}
	phase := NewConsolidationPhase(
		&fakeMemoryReader{},
		writer,
		lineage,
		func() provider.LLMProvider { return nil },
		func() provider.EmbeddingProvider { return nil },
		noveltySettings(false),
		nil,
	)

	meta := decodeMetadata(mem.Metadata)
	got := phase.resolveSourceMemoryIDs(context.Background(), &mem, meta)
	if len(got) != len(sourceIDs) {
		t.Fatalf("expected %d source IDs from metadata, got %d", len(sourceIDs), len(got))
	}
	if lineage.parentLookups != 0 {
		t.Errorf("metadata fast path must not query lineage; got %d lookups", lineage.parentLookups)
	}
	if len(writer.metadataUpdates) != 0 {
		t.Errorf("metadata fast path must not write; got %d UpdateMetadata calls", len(writer.metadataUpdates))
	}
}

// TestResolveSourceMemoryIDs_TrueOrphanReturnsNil pins the negative case:
// when both metadata and lineage are empty, the synthesis really is an
// orphan and the audit should fall through to demoteDream("orphan_no_sources").
func TestResolveSourceMemoryIDs_TrueOrphanReturnsNil(t *testing.T) {
	mem := freshSynthesis(t, nil)
	writer := &updatingMemoryWriter{}
	lineage := &configurableLineageWriter{} // no parents configured
	phase := NewConsolidationPhase(
		&fakeMemoryReader{},
		writer,
		lineage,
		func() provider.LLMProvider { return nil },
		func() provider.EmbeddingProvider { return nil },
		noveltySettings(false),
		nil,
	)

	got := phase.resolveSourceMemoryIDs(context.Background(), &mem, decodeMetadata(mem.Metadata))
	if got != nil {
		t.Fatalf("true orphan must return nil source IDs, got %d", len(got))
	}
	if lineage.parentLookups != 1 {
		t.Errorf("true orphan must consult lineage exactly once; got %d", lineage.parentLookups)
	}
	if len(writer.metadataUpdates) != 0 {
		t.Errorf("true orphan must not self-heal; got %d UpdateMetadata calls", len(writer.metadataUpdates))
	}
}
