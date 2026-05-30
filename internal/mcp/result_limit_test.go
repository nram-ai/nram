package mcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// TestMCPBudgetBytesFallsThroughToRegisteredDefault pins that mcpBudgetBytes
// with a nil SettingsService falls through to the registered default in
// service.settingDefaults rather than panicking or returning zero. Tests that
// construct a stub MCP server without wiring a SettingsService rely on this
// behavior.
func TestMCPBudgetBytesFallsThroughToRegisteredDefault(t *testing.T) {
	got := mcpBudgetBytes(t.Context(), nil)
	want := service.GetDefaultInt(service.SettingMCPMaxResultTokens) * charsPerTokenEstimate
	if got != want {
		t.Fatalf("mcpBudgetBytes(ctx, nil) = %d, want %d (default %d tokens × %d chars/token)",
			got, want, service.GetDefaultInt(service.SettingMCPMaxResultTokens), charsPerTokenEstimate)
	}
}

// TestMCPBudgetBytesUsesResolvedSetting pins the other half of the contract:
// with a real SettingsService the cascade resolves the operator-set value.
func TestMCPBudgetBytesUsesResolvedSetting(t *testing.T) {
	const want = 5000
	svc := newSettingsServiceWithMCPBudget(want)
	got := mcpBudgetBytes(context.Background(), svc)
	if got != want*charsPerTokenEstimate {
		t.Errorf("mcpBudgetBytes returned %d, want %d (%d tokens × %d chars/token)",
			got, want*charsPerTokenEstimate, want, charsPerTokenEstimate)
	}
}

func TestWrapToolResultUnderBudget(t *testing.T) {
	budget := 1000 * charsPerTokenEstimate
	payload := map[string]string{"hello": "world"}
	res, err := wrapToolResult(&stubMetrics{}, "test", budget, payload, nil)
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	if !strings.Contains(text, `"hello":"world"`) {
		t.Fatalf("expected payload verbatim, got %q", text)
	}
	if strings.Contains(text, "TRUNCATED") {
		t.Fatalf("did not expect truncation marker, got %q", text)
	}
}

func TestWrapToolResultHardTruncationWhenNoReducer(t *testing.T) {
	// Smallest budget the settings validator allows (Min=100 tokens). At this
	// boundary the truncation sentinel still fits and the hard-truncate
	// branch produces a sentinel-terminated cut, not a silent byte-cut.
	budget := 100 * charsPerTokenEstimate
	big := strings.Repeat("x", 5000)
	res, err := wrapToolResult(&stubMetrics{}, "test", budget, map[string]string{"data": big}, nil)
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	// IsError stays false on the hard-truncate branch so clients that
	// auto-retry on isError do not loop on a deterministically-oversized
	// response. The agent still sees the partial body in the text content.
	if res.IsError {
		t.Errorf("expected IsError=false on hard-truncate branch; got true (would trigger client retry loops)")
	}
	if res.StructuredContent != nil {
		t.Errorf("expected StructuredContent=nil on hard-truncate branch; the hard-cut JSON byte slice is not schema-conforming")
	}
	text := extractText(res)
	if !strings.HasSuffix(text, truncationSuffix) {
		tail := min(len(text), 120)
		t.Fatalf("expected truncation suffix, got tail %q", text[len(text)-tail:])
	}
	if len(text) > budget {
		t.Fatalf("hard-truncated result %d bytes exceeds budget %d", len(text), budget)
	}
}

// TestHardTruncateAtValidatorMinimum pins that hardTruncate at the minimum
// budget the SettingsService admin schema allows (100 tokens = 200 bytes)
// still emits the sentinel suffix. Below 100 tokens is rejected by the
// validator at PUT time, so the sentinel-less branch that used to exist in
// hardTruncate is now unreachable in production.
func TestHardTruncateAtValidatorMinimum(t *testing.T) {
	minBudget := 100 * charsPerTokenEstimate // matches admin schema Min
	out := []byte(strings.Repeat("x", 5000))
	text := hardTruncate(out, minBudget)
	if len(text) > minBudget {
		t.Errorf("hardTruncate exceeded budget: got %d bytes, budget %d", len(text), minBudget)
	}
	if !strings.HasSuffix(text, truncationSuffix) {
		t.Errorf("hardTruncate at validator minimum must still emit sentinel suffix; got tail %q", text[max(0, len(text)-30):])
	}
}

func TestWrapToolResultUsesReducer(t *testing.T) {
	budget := 800 * charsPerTokenEstimate // 1600B; ~800B structured
	// Build a recall response that will overflow.
	mems := make([]mcpRecallMemory, 50)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   strings.Repeat("lorem ipsum ", 80),
			Tags:      []string{"a", "b"},
			Score:     float64(50 - i),
			CreatedAt: time.Now(),
		}
	}
	resp := &mcpRecallResponse{
		Memories: mems,
		Graph: graphResponse{
			Entities:      []graphEntity{{ID: uuid.New(), Name: "x", Type: "concept"}},
			Relationships: []graphRelationship{},
		},
		LatencyMs: 12,
	}
	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp, false))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	if len(text) > budget {
		t.Fatalf("reduced result %d bytes still exceeds budget %d", len(text), budget)
	}
	// Should still be valid JSON and include _truncated.
	var decoded map[string]any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("reduced result is not valid JSON: %v\nbody: %s", err, text)
	}
	if _, ok := decoded["_truncated"]; !ok {
		t.Fatalf("expected _truncated field in reduced response, got: %v", decoded)
	}
}

// TestRecallReducerGraphSurvivesAfterAuditStrip is the observable guard for two
// graph-survival properties on real serialized output.
//
// It models the SAME dense response before and after the audit strip. "leaky"
// carries the per-row audit blob the recall projection used to leak
// (consolidation/reinforce/ingestion/migration keys); "slim" is what
// recallview.Project now emits (audit stripped, residual nil). At a budget sized
// to the slim payload's Tier-1 structured boundary (budget/2):
//   - slim ships Tier 1 with its graph intact and no _truncated marker
//     (reclaimed budget -> fits naturally);
//   - leaky overflows, so the reducer fires — but with the recall-graph
//     rebalance the graph is no longer dropped first. The reducer trims
//     memories and the graph survives, so an over-budget recall still surfaces
//     graph context.
func TestRecallReducerGraphSurvivesAfterAuditStrip(t *testing.T) {
	const n = 10
	// A realistic audit blob: the keys the recall path now strips, with
	// RFC3339Nano timestamps and UUID-shaped values like the real writers emit.
	auditBlob := json.RawMessage(`{` +
		`"consolidation_load_checked_at":"2026-04-26T09:43:17.123456789Z",` +
		`"reinforce_checked_at":"2026-04-26T09:43:17.123456789Z",` +
		`"consolidation_cluster_checked_at":"2026-04-26T09:43:17.123456789Z",` +
		`"consolidation_cluster_fingerprint":"f0e1d2c3b4a5968778695a4b3c2d1e0f",` +
		`"ingestion_decision":"ADD","ingestion_decision_at":"2026-04-26T09:43:17.123456789Z",` +
		`"ingestion_match_count":0,"ingestion_top_score":0,"ingestion_shadow_op":"none",` +
		`"migrated_from_global":true,"migration_date":"2026-05-24",` +
		`"original_global_id":"0a813a7e-47ce-4f35-ae15-71439239ee0f"}`)

	graph := graphResponse{
		Entities: []graphEntity{
			{ID: uuid.New(), Name: "Anchor", Type: "concept"},
			{ID: uuid.New(), Name: "Topic", Type: "concept"},
			{ID: uuid.New(), Name: "Person", Type: "person"},
		},
		Relationships: []graphRelationship{
			{SourceID: uuid.New(), TargetID: uuid.New(), Relation: "relates_to", Weight: 0.9},
			{SourceID: uuid.New(), TargetID: uuid.New(), Relation: "mentions", Weight: 0.7},
		},
	}

	fixed := time.Unix(1700000000, 0).UTC()
	mkMems := func(withAudit bool) []mcpRecallMemory {
		mems := make([]mcpRecallMemory, n)
		for i := range mems {
			m := mcpRecallMemory{
				ID:          uuid.New(),
				ProjectSlug: "dense",
				Content:     "memory content number " + fmt.Sprint(i),
				Tags:        []string{"alpha", "beta"},
				Origin:      model.OriginDream,
				Score:       float64(n - i),
				Confidence:  0.5,
				CreatedAt:   fixed,
				UpdatedAt:   fixed,
			}
			if withAudit {
				m.Metadata = auditBlob
			}
			mems[i] = m
		}
		return mems
	}

	slim := &mcpRecallResponse{Memories: mkMems(false), Graph: graph, LatencyMs: 5}
	leaky := &mcpRecallResponse{Memories: mkMems(true), Graph: graph, LatencyMs: 5}

	slimRaw, err := json.Marshal(slim)
	if err != nil {
		t.Fatalf("marshal slim: %v", err)
	}
	leakyRaw, err := json.Marshal(leaky)
	if err != nil {
		t.Fatalf("marshal leaky: %v", err)
	}

	// Tier 1 (graph-preserving) requires the payload to fit budget/2. Size the
	// budget to the slim payload's Tier-1 boundary.
	budget := 2 * len(slimRaw)
	if len(leakyRaw) <= budget {
		t.Fatalf("fixture invalid: leaky(%d) must exceed budget(%d=2*slim(%d)) so it cannot ship intact",
			len(leakyRaw), budget, len(slimRaw))
	}

	// Slim: ships Tier 1 with the graph intact, no _truncated.
	slimRes, err := wrapToolResult(&stubMetrics{}, "recall", budget, slim, newRecallReducer(slim, false))
	if err != nil {
		t.Fatalf("wrapToolResult slim err = %v", err)
	}
	slimText := extractText(slimRes)
	var slimDecoded map[string]any
	if err := json.Unmarshal([]byte(slimText), &slimDecoded); err != nil {
		t.Fatalf("slim result not valid JSON: %v\nbody: %s", err, slimText)
	}
	if _, truncated := slimDecoded["_truncated"]; truncated {
		t.Errorf("slim response should NOT be truncated; got _truncated=%v", slimDecoded["_truncated"])
	}
	slimGraph, _ := slimDecoded["graph"].(map[string]any)
	ents, _ := slimGraph["entities"].([]any)
	if len(ents) != 3 {
		t.Errorf("expected slim graph to round-trip 3 entities, got %d (graph=%v)", len(ents), slimGraph)
	}

	// Leaky: the audit bloat pushes it past the Tier-1 boundary so the reducer
	// fires — but the graph is NO LONGER the first casualty. The reducer trims
	// memories first (here: halves the list, since the bloat is in metadata, not
	// content) and the graph rides along untouched. This is the headline
	// invariant of the recall-graph rebalance: an over-budget recall still
	// surfaces graph context instead of blinding the caller.
	leakyRes, err := wrapToolResult(&stubMetrics{}, "recall", budget, leaky, newRecallReducer(leaky, false))
	if err != nil {
		t.Fatalf("wrapToolResult leaky err = %v", err)
	}
	leakyText := extractText(leakyRes)
	var leakyDecoded map[string]any
	if err := json.Unmarshal([]byte(leakyText), &leakyDecoded); err != nil {
		t.Fatalf("leaky result not valid JSON: %v\nbody: %s", err, leakyText)
	}
	trunc, ok := leakyDecoded["_truncated"].(map[string]any)
	if !ok {
		t.Fatalf("expected leaky response to be _truncated; got %v", leakyDecoded)
	}
	// The graph must survive: entities still present on the wire.
	leakyGraph, _ := leakyDecoded["graph"].(map[string]any)
	leakyEnts, _ := leakyGraph["entities"].([]any)
	if len(leakyEnts) == 0 {
		t.Errorf("expected leaky response to retain its graph; got empty entities (graph=%v)", leakyGraph)
	}
	// ...and the reducer must NOT claim a wholesale graph drop.
	dropped, _ := trunc["dropped"].([]any)
	for _, d := range dropped {
		if s, _ := d.(string); s == "graph.entities" || s == "graph.relationships" {
			t.Errorf("graph must not be dropped wholesale under memory-trimmable budget; dropped=%v", dropped)
		}
	}
}

// TestRecallReducerPreservesCoverageGapsThroughStages1To3 pins that
// coverage_gaps is NOT trimmed during the content-truncation stages (1-2) —
// only the later halving stages shed it in lockstep with memories. Callers
// relying on the diversify diagnostic should always see it when the reducer was
// able to fit by trimming content rather than the diagnostic.
func TestRecallReducerPreservesCoverageGapsThroughStages1To3(t *testing.T) {
	budget := 100000 * charsPerTokenEstimate // generous: stages 1-3 are sufficient

	// Build a payload whose ONLY overflow source is content length; stages
	// 2-3 (content truncation) should shrink it without touching gaps.
	mems := make([]mcpRecallMemory, 10)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   strings.Repeat("x", 50000), // huge: needs stage-2/3 trim
			Tags:      []string{"category-a"},
			Score:     float64(10 - i),
			CreatedAt: time.Now(),
		}
	}
	gaps := []service.CoverageGap{
		{GroupKey: "category-b", Cause: "limit"},
		{GroupKey: "category-c", Cause: "threshold"},
		{GroupKey: "category-d", Cause: "tag_filter"},
	}
	resp := &mcpRecallResponse{
		Memories:     mems,
		CoverageGaps: gaps,
		LatencyMs:    7,
	}

	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp, false))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)

	var decoded map[string]any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("reduced result is not valid JSON: %v\nbody: %s", err, text)
	}
	raw, ok := decoded["coverage_gaps"]
	if !ok {
		t.Fatalf("coverage_gaps dropped by reducer; decoded=%v", decoded)
	}
	arr, ok := raw.([]any)
	if !ok {
		t.Fatalf("coverage_gaps wrong shape: %T", raw)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 coverage_gaps preserved (stages 1-3 don't shed them), got %d", len(arr))
	}
}

// TestRecallReducerOmitsCoverageGapsWhenEmpty confirms that responses not
// using diversification do not gain a spurious coverage_gaps field.
func TestRecallReducerOmitsCoverageGapsWhenEmpty(t *testing.T) {
	budget := 800 * charsPerTokenEstimate

	mems := make([]mcpRecallMemory, 50)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   strings.Repeat("x", 200),
			Tags:      []string{"a"},
			Score:     float64(50 - i),
			CreatedAt: time.Now(),
		}
	}
	resp := &mcpRecallResponse{Memories: mems, LatencyMs: 1}

	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp, false))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	if strings.Contains(text, "coverage_gaps") {
		t.Errorf("coverage_gaps should be absent when unset, got %s", text)
	}
}

func TestNewListReducerProducesValidPagination(t *testing.T) {
	// Each listMemoryItem in the fixture marshals to ~370B (UUID + 200B
	// content + two timestamps). The reducer halves down to a single item;
	// allow ~950B structured budget so one item plus pagination envelope
	// fits without falling through to hard-truncate.
	budget := 1200 * charsPerTokenEstimate
	items := make([]listMemoryItem, 100)
	for i := range items {
		items[i] = listMemoryItem{
			ID:        uuid.New(),
			Content:   strings.Repeat("a", 200),
			Tags:      []string{},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}
	resp := &listMemoryResponse{
		Data: items,
		Pagination: model.Pagination{
			Total:  500,
			Limit:  100,
			Offset: 0,
		},
	}
	res, err := wrapToolResult(&stubMetrics{}, "list", budget, resp, newListReducer(resp))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	if len(text) > budget {
		t.Fatalf("reduced list result %d bytes exceeds budget %d", len(text), budget)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("reduced list is not valid JSON: %v", err)
	}
	if _, ok := decoded["_truncated"]; !ok {
		t.Fatalf("expected _truncated field in reduced list response")
	}
	if _, ok := decoded["pagination"]; !ok {
		t.Fatalf("reduced list response is missing pagination")
	}
}

// TestGraphReducerPreservesEdgeCapReason pins that when a graphResponse
// arrives already carrying an edge_cap truncation envelope (the traverser
// short-circuited at graph.max_edges), the byte-budget reducer's emitted
// _truncated envelope preserves the root cause in Reason instead of
// silently overwriting it with response_too_large. Otherwise clients lose
// the actionable remediation (raise graph.max_edges) and are misdirected
// to query-shape tuning that does not fix the underlying issue.
func TestGraphReducerPreservesEdgeCapReason(t *testing.T) {
	budget := 800 * charsPerTokenEstimate // 1600B; ~800B structured, forces reduction
	// Stage enough relationships to overshoot the budget so the reducer fires.
	rels := make([]graphRelationship, 200)
	for i := range rels {
		rels[i] = graphRelationship{
			SourceID: uuid.New(),
			TargetID: uuid.New(),
			Relation: "knows",
			Weight:   1.0,
		}
	}
	orig := &graphResponse{
		Entities:      []graphEntity{{ID: uuid.New(), Name: "alice", Type: "person"}},
		Relationships: rels,
		Truncated: &truncationInfo{
			Reason: "edge_cap",
			Hint:   "traversal stopped at graph.max_edges=2000; raise the setting or narrow the entity query/depth",
		},
	}
	res, err := wrapToolResult(&stubMetrics{}, "graph", budget, orig, newGraphReducer(orig))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	if len(text) > budget {
		t.Fatalf("reduced result %d bytes still exceeds budget %d", len(text), budget)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("reduced result is not valid JSON: %v\nbody: %s", err, text)
	}
	truncated, ok := decoded["_truncated"].(map[string]any)
	if !ok {
		t.Fatalf("expected _truncated map in reduced response, got: %v", decoded)
	}
	reason, _ := truncated["reason"].(string)
	if reason != "edge_cap+response_too_large" {
		t.Errorf("expected reason=edge_cap+response_too_large preserving the root cause, got %q", reason)
	}
	hint, _ := truncated["hint"].(string)
	if !strings.Contains(hint, "graph.max_edges") {
		t.Errorf("expected reducer hint to retain the edge_cap remediation (raise graph.max_edges), got %q", hint)
	}
	if !strings.Contains(hint, "response further halved") {
		t.Errorf("expected reducer hint to mention the byte-budget reduction, got %q", hint)
	}
}

// TestGraphReducerWithoutEdgeCapUsesDefaultReason confirms the
// preservation logic is gated on orig.Truncated being set — when the
// traversal did not short-circuit, the reducer emits its standard
// response_too_large envelope as before.
func TestGraphReducerWithoutEdgeCapUsesDefaultReason(t *testing.T) {
	budget := 800 * charsPerTokenEstimate
	rels := make([]graphRelationship, 200)
	for i := range rels {
		rels[i] = graphRelationship{
			SourceID: uuid.New(),
			TargetID: uuid.New(),
			Relation: "knows",
			Weight:   1.0,
		}
	}
	orig := &graphResponse{
		Entities:      []graphEntity{{ID: uuid.New(), Name: "alice", Type: "person"}},
		Relationships: rels,
	}
	res, err := wrapToolResult(&stubMetrics{}, "graph", budget, orig, newGraphReducer(orig))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(extractText(res)), &decoded); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	truncated, ok := decoded["_truncated"].(map[string]any)
	if !ok {
		t.Fatalf("expected _truncated map")
	}
	if reason, _ := truncated["reason"].(string); reason != "response_too_large" {
		t.Errorf("expected reason=response_too_large for uncapped traversal, got %q", reason)
	}
}

func TestWrapToolResultTextRespectsBudget(t *testing.T) {
	budget := 100 * charsPerTokenEstimate // validator minimum
	big := strings.Repeat("y", 5000)
	res, err := wrapToolResultText(&stubMetrics{}, "test", budget, big)
	if err != nil {
		t.Fatalf("wrapToolResultText err = %v", err)
	}
	text := extractText(res)
	if len(text) > budget {
		t.Fatalf("text result %d bytes exceeds budget %d", len(text), budget)
	}
	if !strings.HasSuffix(text, truncationSuffix) {
		t.Fatalf("expected truncation suffix on text result")
	}
}

// TestWrapToolResultPopulatesStructuredContent pins that wrapToolResult ships
// both text content (JSON-marshaled fallback) and structuredContent (a
// detached json.RawMessage copy) on the happy path and on the reduced path.
// Clients that declare an outputSchema rely on structuredContent being
// present AND the structured value being immune to post-call mutation.
func TestWrapToolResultPopulatesStructuredContent(t *testing.T) {
	budget := 1000 * charsPerTokenEstimate
	payload := &mcpStoreResponse{
		ID:          uuid.New(),
		ProjectSlug: "global",
		Enriched:    true,
	}
	res, err := wrapToolResult(&stubMetrics{}, "test", budget, payload, nil)
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	if res.StructuredContent == nil {
		t.Fatal("expected StructuredContent on happy path, got nil")
	}
	raw, ok := res.StructuredContent.(json.RawMessage)
	if !ok {
		t.Fatalf("StructuredContent type = %T, want json.RawMessage (detached copy)", res.StructuredContent)
	}
	var got mcpStoreResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode StructuredContent: %v", err)
	}
	if got.ProjectSlug != "global" {
		t.Errorf("StructuredContent.ProjectSlug = %q, want %q", got.ProjectSlug, "global")
	}
	if extractText(res) == "" {
		t.Error("expected text-content fallback alongside structured content")
	}

	// Mutation-immunity: changing the caller's payload after wrapToolResult
	// returns must not affect StructuredContent. The detached copy is the
	// guarantee that text and structured stay byte-equivalent on the wire.
	payload.ProjectSlug = "MUTATED"
	var afterMutation mcpStoreResponse
	if err := json.Unmarshal(res.StructuredContent.(json.RawMessage), &afterMutation); err != nil {
		t.Fatalf("decode StructuredContent post-mutation: %v", err)
	}
	if afterMutation.ProjectSlug != "global" {
		t.Errorf("StructuredContent leaked caller mutation: ProjectSlug = %q, want frozen %q", afterMutation.ProjectSlug, "global")
	}
}

// TestWrapToolResultReducedPathPopulatesStructuredContent pins that even when
// the reducer fires, structuredContent is still populated with a json.RawMessage
// snapshot of the reduced response.
func TestWrapToolResultReducedPathPopulatesStructuredContent(t *testing.T) {
	budget := 800 * charsPerTokenEstimate
	mems := make([]mcpRecallMemory, 50)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   strings.Repeat("lorem ipsum ", 80),
			Tags:      []string{"a"},
			Score:     float64(50 - i),
			CreatedAt: time.Now(),
		}
	}
	resp := &mcpRecallResponse{Memories: mems, LatencyMs: 1}
	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp, false))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	if res.StructuredContent == nil {
		t.Fatal("expected StructuredContent on reduced path, got nil")
	}
	raw, ok := res.StructuredContent.(json.RawMessage)
	if !ok {
		t.Fatalf("StructuredContent type = %T, want json.RawMessage (detached copy)", res.StructuredContent)
	}
	var reduced mcpRecallResponse
	if err := json.Unmarshal(raw, &reduced); err != nil {
		t.Fatalf("decode reduced StructuredContent: %v", err)
	}
	if reduced.Truncated == nil {
		t.Error("expected reduced response to carry Truncated, got nil")
	}
}

// TestMCPExportResponseTopLevelKeys removed 2026-05-27 along with the
// mcpExportResponse wrapper. The MCP export tool no longer exists; see
// internal/api/handler_me_exports.go for the replacement REST surface.

// stubMetrics records the (tool, tier) increments wrapToolResult emits so
// tests can assert telemetry behavior without standing up a real Prometheus
// registry.
type stubMetrics struct {
	calls []struct{ tool, tier string }
}

func (s *stubMetrics) RecordMCPToolResultTier(tool, tier string) {
	s.calls = append(s.calls, struct{ tool, tier string }{tool, tier})
}

// mapSettingsRepo is a test-only in-memory SettingsRepository that returns
// fixed values for specific keys and falls through (sql.ErrNoRows) for the
// rest. Used to inject the MCP budget into handler-level tests that need a
// specific value, without standing up a real DB.
type mapSettingsRepo struct {
	values map[string]string
}

func (m *mapSettingsRepo) Get(_ context.Context, key, _ string) (*model.Setting, error) {
	if v, ok := m.values[key]; ok {
		return &model.Setting{Key: key, Value: json.RawMessage(v)}, nil
	}
	return nil, sql.ErrNoRows
}
func (m *mapSettingsRepo) Set(context.Context, *model.Setting) error    { return nil }
func (m *mapSettingsRepo) Delete(context.Context, string, string) error { return nil }
func (m *mapSettingsRepo) ListByScope(context.Context, string) ([]model.Setting, error) {
	return nil, nil
}

// newSettingsServiceWithMCPBudget builds a *service.SettingsService whose
// mcp.max_result_tokens resolves to the supplied token count. Every other
// key falls through to settingDefaults via the underlying noop behaviour.
func newSettingsServiceWithMCPBudget(tokens int) *service.SettingsService {
	return service.NewSettingsService(&mapSettingsRepo{values: map[string]string{
		service.SettingMCPMaxResultTokens: strconv.Itoa(tokens),
	}})
}

// newTestServer wraps mcp.NewServer with a stub MetricsRecorder so tests
// don't have to wire one explicitly. Production code constructs the server
// via NewServer directly; that path still panics on nil Metrics — the panic
// catches wiring drift in main.go and any other binary, exactly as designed.
func newTestServer(deps Dependencies) *Server {
	if deps.Metrics == nil {
		deps.Metrics = &stubMetrics{}
	}
	return NewServer(deps)
}

// TestWrapToolResultTier2GracefulDegradation pins the missing-middle tier:
// a payload that exceeds the halved structured budget but fits the full
// text budget must ship as text-only with NO sentinel, NO _truncated, and
// valid JSON in the text content. The hard-truncate corruption that the
// pre-fix code produced on this input is exactly the bug Tier 2 fixes.
func TestWrapToolResultTier2GracefulDegradation(t *testing.T) {
	// Budget math: 600 tokens → 1200B; structuredBudget=600. We want a
	// payload in (600, 1200].
	budget := 600 * charsPerTokenEstimate
	payload := struct {
		Blob string `json:"blob"`
	}{
		Blob: strings.Repeat("a", 900), // ~915 marshaled bytes
	}
	rec := &stubMetrics{}
	res, err := wrapToolResult(rec, "tier2_test", budget, payload, nil)
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	if res.StructuredContent != nil {
		t.Errorf("Tier 2 must NOT ship StructuredContent; got %T", res.StructuredContent)
	}
	text := extractText(res)
	if strings.HasSuffix(text, truncationSuffix) {
		t.Errorf("Tier 2 must NOT have truncation suffix; text=%q", text)
	}
	if res.IsError {
		t.Errorf("Tier 2 must NOT have IsError=true")
	}
	// The text MUST be valid JSON (the whole point of Tier 2: complete data).
	var decoded map[string]any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Errorf("Tier 2 text must be valid JSON; err=%v body=%s", err, text)
	}
	// Telemetry: tier="text_only" recorded once.
	if len(rec.calls) != 1 || rec.calls[0].tool != "tier2_test" || rec.calls[0].tier != tierTextOnly {
		t.Errorf("expected 1 text_only counter increment; got %+v", rec.calls)
	}
}

// TestRecallReducerHintsComposed pins that a multi-stage reduction emits a hint
// that mentions BOTH reductions, not just the last one. Here the graph was
// balance-trimmed by the handler's pre-cap (graphPreTrimmed=true, with the
// kept/total sentinels stamped on orig.Truncated as the handler does) and the
// memory reducer additionally trims content. The composed hint must surface the
// graph-trim clause AND the content-truncation clause.
func TestRecallReducerHintsComposed(t *testing.T) {
	budget := 1500 * charsPerTokenEstimate
	mems := make([]mcpRecallMemory, 5)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   strings.Repeat("x", 2000), // forces the content-trim stages
			Tags:      []string{"a"},
			Score:     float64(5 - i),
			CreatedAt: time.Now(),
		}
	}
	resp := &mcpRecallResponse{
		Memories: mems,
		Graph: graphResponse{
			Entities:      []graphEntity{{ID: uuid.New(), Name: "n", Type: "concept"}},
			Relationships: []graphRelationship{},
		},
		// Simulate the handler's pre-cap having balance-trimmed the graph.
		Truncated: &truncationInfo{
			Reason:  "response_too_large",
			Dropped: []string{"entities_kept:1/4", "relationships_kept:0/0"},
			Hint:    recallGraphTrimHint,
		},
		LatencyMs: 1,
	}
	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp, true))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	var decoded mcpRecallResponse
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("reduced result is not valid JSON: %v", err)
	}
	if decoded.Truncated == nil {
		t.Fatal("expected Truncated set on reduced response")
	}
	hint := decoded.Truncated.Hint
	if !strings.Contains(hint, "graph") {
		t.Errorf("composed hint must mention the graph trim; got %q", hint)
	}
	if !strings.Contains(hint, "content") {
		t.Errorf("composed hint must mention content truncation; got %q", hint)
	}
	// The pre-cap kept/total sentinels must survive into the reduced envelope.
	foundGraphSentinel := false
	for _, d := range decoded.Truncated.Dropped {
		if strings.HasPrefix(d, "entities_kept:") {
			foundGraphSentinel = true
		}
	}
	if !foundGraphSentinel {
		t.Errorf("expected pre-cap entities_kept:N/M sentinel to survive; got %v", decoded.Truncated.Dropped)
	}
}

// TestRecallReducerNoGraphNoDropClaim pins that stage 1 only claims a
// graph drop when the original response HAD a graph. With an empty graph,
// Truncated.Dropped must NOT include graph fields and Hint must NOT
// mention graph_depth.
func TestRecallReducerNoGraphNoDropClaim(t *testing.T) {
	budget := 1500 * charsPerTokenEstimate
	mems := make([]mcpRecallMemory, 5)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   strings.Repeat("x", 2000),
			Tags:      []string{"a"},
			Score:     float64(5 - i),
			CreatedAt: time.Now(),
		}
	}
	// Empty graph (entities and relationships both zero-length).
	resp := &mcpRecallResponse{
		Memories:  mems,
		Graph:     graphResponse{Entities: []graphEntity{}, Relationships: []graphRelationship{}},
		LatencyMs: 1,
	}
	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp, false))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	var decoded mcpRecallResponse
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("reduced result is not valid JSON: %v", err)
	}
	if decoded.Truncated == nil {
		t.Fatal("expected Truncated set on reduced response")
	}
	for _, d := range decoded.Truncated.Dropped {
		if strings.Contains(d, "graph") {
			t.Errorf("Dropped must not claim graph drop when original graph was empty; got %v", decoded.Truncated.Dropped)
		}
	}
	if strings.Contains(decoded.Truncated.Hint, "graph_depth") {
		t.Errorf("Hint must not mention graph_depth when original graph was empty; got %q", decoded.Truncated.Hint)
	}
}

// TestListReducerPreservesRequestLimit pins that newListReducer leaves
// Pagination.Limit at the caller's requested value. The reduced count goes
// in Truncated.ReturnedCount only.
func TestListReducerPreservesRequestLimit(t *testing.T) {
	budget := 1500 * charsPerTokenEstimate
	items := make([]listMemoryItem, 100)
	for i := range items {
		items[i] = listMemoryItem{
			ID:        uuid.New(),
			Content:   strings.Repeat("a", 200),
			Tags:      []string{},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}
	resp := &listMemoryResponse{
		Data: items,
		Pagination: model.Pagination{
			Total:  500,
			Limit:  50, // request value
			Offset: 0,
		},
	}
	res, err := wrapToolResult(&stubMetrics{}, "list", budget, resp, newListReducer(resp))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	var decoded listMemoryResponse
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("reduced list is not valid JSON: %v", err)
	}
	if decoded.Pagination.Limit != 50 {
		t.Errorf("Pagination.Limit was overwritten: got %d, want 50 (request value)", decoded.Pagination.Limit)
	}
	if decoded.Truncated == nil {
		t.Fatal("expected Truncated set on reduced list")
	}
	if decoded.Truncated.ReturnedCount != len(decoded.Data) {
		t.Errorf("Truncated.ReturnedCount=%d should equal len(Data)=%d", decoded.Truncated.ReturnedCount, len(decoded.Data))
	}
}

// TestRecallReducerCoverageGapsTrimmed pins that the halving stages (3+) shed
// coverage_gaps in lockstep with memories and record the trim ratio in Dropped.
func TestRecallReducerCoverageGapsTrimmed(t *testing.T) {
	budget := 800 * charsPerTokenEstimate
	mems := make([]mcpRecallMemory, 4)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   strings.Repeat("x", 200),
			Tags:      []string{"a"},
			Score:     float64(4 - i),
			CreatedAt: time.Now(),
		}
	}
	gaps := make([]service.CoverageGap, 20)
	for i := range gaps {
		gaps[i] = service.CoverageGap{
			GroupKey: fmt.Sprintf("group-%d", i),
			Cause:    "limit",
		}
	}
	resp := &mcpRecallResponse{
		Memories:     mems,
		CoverageGaps: gaps,
		LatencyMs:    1,
	}
	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp, false))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	var decoded mcpRecallResponse
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("reduced result is not valid JSON: %v\nbody: %s", err, text)
	}
	if decoded.Truncated == nil {
		t.Fatal("expected Truncated set on reduced response")
	}
	// At least one Dropped entry must reference the frame-independent
	// coverage_gaps_kept:N/M marker.
	foundGapDrop := false
	for _, d := range decoded.Truncated.Dropped {
		if strings.HasPrefix(d, "coverage_gaps_kept:") {
			foundGapDrop = true
		}
	}
	if !foundGapDrop {
		t.Errorf("expected Dropped to include 'coverage_gaps_kept:N/M' marker; got %v", decoded.Truncated.Dropped)
	}
	if len(decoded.CoverageGaps) >= 20 {
		t.Errorf("coverage_gaps should have been trimmed; got %d (original 20)", len(decoded.CoverageGaps))
	}
}

// TestListProjectsReducerHalves pins that the projects reducer trims
// Projects without touching Pagination.Limit.
func TestListProjectsReducerHalves(t *testing.T) {
	budget := 1500 * charsPerTokenEstimate
	projects := make([]projectItem, 100)
	for i := range projects {
		projects[i] = projectItem{
			ID:          uuid.New(),
			Name:        fmt.Sprintf("project-%d", i),
			Slug:        fmt.Sprintf("slug-%d", i),
			Description: strings.Repeat("d", 100),
		}
	}
	resp := &listProjectsResponse{
		Projects:   projects,
		Pagination: model.Pagination{Total: 200, Limit: 100, Offset: 0},
	}
	res, err := wrapToolResult(&stubMetrics{}, "list_projects", budget, resp, newListProjectsReducer(resp))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	var decoded listProjectsResponse
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("reduced list_projects is not valid JSON: %v", err)
	}
	if decoded.Pagination.Limit != 100 {
		t.Errorf("Pagination.Limit was overwritten: got %d, want 100", decoded.Pagination.Limit)
	}
	if decoded.Truncated == nil {
		t.Fatal("expected Truncated set on reduced list_projects")
	}
	if len(decoded.Projects) >= 100 {
		t.Errorf("expected halved projects; got %d", len(decoded.Projects))
	}
}

// TestBatchGetReducerHalves pins that the get reducer halves Found, leaves
// NotFound intact, and reports ReturnedCount correctly.
func TestBatchGetReducerHalves(t *testing.T) {
	budget := 1500 * charsPerTokenEstimate
	found := make([]mcpMemoryDetail, 40)
	for i := range found {
		found[i] = mcpMemoryDetail{
			ID:          uuid.New(),
			ProjectSlug: "p",
			Content:     strings.Repeat("c", 200),
			Tags:        []string{},
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}
	}
	notFound := []uuid.UUID{uuid.New(), uuid.New()}
	resp := &mcpBatchGetResponse{Found: found, NotFound: notFound}
	res, err := wrapToolResult(&stubMetrics{}, "get", budget, resp, newBatchGetReducer(resp))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	var decoded mcpBatchGetResponse
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		t.Fatalf("reduced get is not valid JSON: %v", err)
	}
	if decoded.Truncated == nil {
		t.Fatal("expected Truncated set on reduced get")
	}
	if len(decoded.NotFound) != 2 {
		t.Errorf("NotFound must be preserved verbatim; got %d", len(decoded.NotFound))
	}
	if len(decoded.Found) >= 40 {
		t.Errorf("Found should have been halved; got %d", len(decoded.Found))
	}
	if decoded.Truncated.ReturnedCount != len(decoded.Found)+len(decoded.NotFound) {
		t.Errorf("ReturnedCount=%d should equal Found+NotFound=%d", decoded.Truncated.ReturnedCount, len(decoded.Found)+len(decoded.NotFound))
	}
}

// fuzzPayload is a deterministic fixture for the property test: a struct
// with a single string field whose marshaled JSON size is roughly the
// requested target.
type fuzzPayload struct {
	Blob string `json:"blob"`
}

func newFuzzPayload(targetBytes int) *fuzzPayload {
	// JSON envelope is ~12 bytes ({"blob":""}); pad the rest with data.
	const envelope = 12
	n := max(targetBytes-envelope, 0)
	return &fuzzPayload{Blob: strings.Repeat("x", n)}
}

// halvingReducer returns a stateful reducer that emits payloads of
// strictly-decreasing size until reaching len <= 1.
func halvingReducer(initial int) reducerFunc {
	size := initial
	return func() (any, bool) {
		if size <= 1 {
			return nil, false
		}
		size /= 2
		return newFuzzPayload(size), size > 1
	}
}

// stubbornReducer returns the same-size payload once with more=false. It
// stresses the "reducer signals can't shrink further" branch.
func stubbornReducer(size int) reducerFunc {
	called := false
	return func() (any, bool) {
		if called {
			return nil, false
		}
		called = true
		return newFuzzPayload(size), false
	}
}

// TestWrapToolResultThreeTierContractProperty fuzzes wrapToolResult across
// the input space and asserts the three-tier contract holds on every
// iteration. The seed is fixed for reproducibility; a failure dumps the
// inputs so the case is constructible.
//
// Invariants (every iteration):
//
//   - inv_no_error: err == nil and res != nil
//   - inv_tier_exclusive: (StructuredContent != nil) XOR sentinel suffix
//   - inv_tier1_consistency: structured bytes equal text bytes
//   - inv_tier1_or_2_valid_json: non-sentinel text must be valid JSON
//   - inv_tier3_has_sentinel: invalid JSON text MUST end with the sentinel
//   - inv_telemetry_consistent: counter increments exactly once per
//     Tier-2/Tier-3 emission and zero times per Tier-1
func TestWrapToolResultThreeTierContractProperty(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	// All budgets sit at or above the SettingsService Min=100 (200 bytes),
	// so the property test does not exercise the unreachable-in-prod
	// sentinel-less branch in hardTruncate (deleted as part of pass 4).
	budgets := []int{200, 500, 1000, 4000, 22000, 100000}

	type reducerCase struct {
		name string
		make func(initialSize int) reducerFunc
	}
	reducerCases := []reducerCase{
		{"nil_reducer", func(int) reducerFunc { return nil }},
		{"halving", halvingReducer},
		{"stubborn", stubbornReducer},
	}

	const iterations = 200
	for i := range iterations {
		budgetTokens := budgets[r.Intn(len(budgets))]
		budget := budgetTokens * charsPerTokenEstimate

		// Payload sizes spanning under/near-cliff/over budget.
		mults := []float64{0, 0.25, 0.5, 0.75, 1.0, 2.0, 10.0}
		mult := mults[r.Intn(len(mults))]
		payloadSize := int(float64(budget) * mult)
		payload := newFuzzPayload(payloadSize)

		rc := reducerCases[r.Intn(len(reducerCases))]
		reducer := rc.make(payloadSize)
		rec := &stubMetrics{}

		res, err := wrapToolResult(rec, "fuzz", budget, payload, reducer)

		dump := func(msg string, extra ...any) {
			args := append([]any{msg, i, budgetTokens, budget, payloadSize, rc.name}, extra...)
			t.Errorf("[seed=1 iter=%d] budget_tokens=%d budget_bytes=%d payload=%d reducer=%s -- "+args[0].(string), args[1:]...)
		}

		// inv_no_error
		if err != nil || res == nil {
			dump("wrapToolResult returned err=%v res=%v", err, res)
			continue
		}

		tier := classifyTier(res)
		text := callToolResultText(res)

		hasStructured := res.StructuredContent != nil
		hasSentinel := strings.HasSuffix(text, truncationSuffix)

		// inv_tier_exclusive
		if hasStructured && hasSentinel {
			dump("inv_tier_exclusive: both structured and sentinel present (tier=%s)", tier)
		}

		// inv_tier1_consistency
		if hasStructured {
			raw, ok := res.StructuredContent.(json.RawMessage)
			if !ok {
				dump("inv_tier1_consistency: structured is %T, want json.RawMessage", res.StructuredContent)
			} else if string(raw) != text {
				dump("inv_tier1_consistency: structured bytes (%d) != text bytes (%d)", len(raw), len(text))
			}
		}

		// inv_tier1_or_2_valid_json
		if !hasSentinel {
			var v any
			if err := json.Unmarshal([]byte(text), &v); err != nil {
				dump("inv_tier1_or_2_valid_json: text is not valid JSON: %v\nbody: %s", err, text)
			}
		}

		// inv_tier3_has_sentinel: any non-JSON text MUST be a sentinel result.
		var probe any
		if err := json.Unmarshal([]byte(text), &probe); err != nil {
			if !hasSentinel {
				dump("inv_tier3_has_sentinel: text is not valid JSON and lacks sentinel suffix")
			}
		}

		// inv_telemetry_consistent: tier1 (natural) is silent, all others
		// record exactly once with the matching label. tier1_reduced is
		// covered by TestTier1AfterReducerRecordsTelemetry below; here a
		// classifyTier=="tier1" outcome can also arise from a successful
		// in-loop short-circuit AFTER reducer iterations, which the
		// property test distinguishes by counter state.
		switch tier {
		case "tier1":
			if len(rec.calls) == 0 {
				// Natural Tier 1: never invoked the reducer.
				break
			}
			// Tier 1 after reducer iteration: must be exactly one
			// tier1_reduced increment.
			if len(rec.calls) != 1 || rec.calls[0].tier != tierTier1Reduced {
				dump("inv_telemetry_consistent: tier1 with calls must be one tier1_reduced; got %+v", rec.calls)
			}
		case "tier2":
			if len(rec.calls) != 1 || rec.calls[0].tier != tierTextOnly {
				dump("inv_telemetry_consistent: tier2 must increment text_only exactly once; got %+v", rec.calls)
			}
		case "tier3":
			if len(rec.calls) != 1 || rec.calls[0].tier != tierHardTruncate {
				dump("inv_telemetry_consistent: tier3 must increment hard_truncate exactly once; got %+v", rec.calls)
			}
		default:
			dump("inv_telemetry_consistent: unknown tier %q", tier)
		}
	}
}

// TestWrapToolResultTextContractProperty fuzzes wrapToolResultText. The
// helper has a two-tier contract: fit verbatim, or hard-truncate with
// sentinel. Never StructuredContent. The text is preserved byte-for-byte
// when it fits.
func TestWrapToolResultTextContractProperty(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	budgets := []int{200, 500, 1000, 4000, 22000}

	const iterations = 120
	for i := range iterations {
		budget := budgets[r.Intn(len(budgets))]
		mult := []float64{0, 0.5, 1.0, 2.0, 10.0}[r.Intn(5)]
		text := strings.Repeat("x", int(float64(budget)*mult))
		rec := &stubMetrics{}
		res, err := wrapToolResultText(rec, "fuzz_text", budget, text)

		dump := func(msg string, extra ...any) {
			args := append([]any{msg, i, budget, len(text)}, extra...)
			t.Errorf("[seed=2 iter=%d budget=%d text_len=%d] "+args[0].(string), args[1:]...)
		}

		if err != nil || res == nil {
			dump("err=%v res=%v", err, res)
			continue
		}
		if res.StructuredContent != nil {
			dump("wrapToolResultText must never set StructuredContent; got %T", res.StructuredContent)
		}
		got := callToolResultText(res)
		fits := len(text) <= budget
		hasSentinel := strings.HasSuffix(got, truncationSuffix)

		if fits {
			if got != text {
				dump("fit case must preserve text verbatim; in_len=%d out_len=%d", len(text), len(got))
			}
			if len(rec.calls) != 0 {
				dump("fit case must not record telemetry; got %+v", rec.calls)
			}
			if hasSentinel {
				dump("fit case must not emit sentinel")
			}
		} else {
			if !hasSentinel {
				dump("over-budget must emit sentinel; tail %q", got[max(0, len(got)-30):])
			}
			if len(got) > budget {
				dump("over-budget result %d bytes exceeds budget %d", len(got), budget)
			}
			if len(rec.calls) != 1 || rec.calls[0].tier != tierHardTruncate {
				dump("over-budget must increment hard_truncate once; got %+v", rec.calls)
			}
		}
	}
}

// TestWrapToolResultJSONTextContractProperty fuzzes wrapToolResultJSONText
// across nil/halving/stubborn reducers. The helper marshals + reduces but
// never emits StructuredContent. Telemetry labels are text_only when the
// reducer succeeds and hard_truncate otherwise.
func TestWrapToolResultJSONTextContractProperty(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	budgets := []int{200, 500, 1000, 4000, 22000}
	reducerCases := []struct {
		name string
		make func(initialSize int) reducerFunc
	}{
		{"nil", func(int) reducerFunc { return nil }},
		{"halving", halvingReducer},
		{"stubborn", stubbornReducer},
	}

	const iterations = 120
	for i := range iterations {
		budget := budgets[r.Intn(len(budgets))]
		mult := []float64{0, 0.5, 1.0, 2.0, 10.0}[r.Intn(5)]
		payloadSize := int(float64(budget) * mult)
		payload := newFuzzPayload(payloadSize)
		rc := reducerCases[r.Intn(len(reducerCases))]
		reducer := rc.make(payloadSize)
		rec := &stubMetrics{}
		res, err := wrapToolResultJSONText(rec, "fuzz_json_text", budget, payload, reducer)

		dump := func(msg string, extra ...any) {
			args := append([]any{msg, i, budget, payloadSize, rc.name}, extra...)
			t.Errorf("[seed=3 iter=%d budget=%d payload=%d reducer=%s] "+args[0].(string), args[1:]...)
		}

		if err != nil || res == nil {
			dump("err=%v res=%v", err, res)
			continue
		}
		if res.StructuredContent != nil {
			dump("wrapToolResultJSONText must never set StructuredContent; got %T", res.StructuredContent)
		}
		text := callToolResultText(res)
		hasSentinel := strings.HasSuffix(text, truncationSuffix)

		if hasSentinel {
			if len(rec.calls) != 1 || rec.calls[0].tier != tierHardTruncate {
				dump("sentinel case must increment hard_truncate once; got %+v", rec.calls)
			}
		} else {
			// Non-sentinel must be valid JSON.
			var v any
			if err := json.Unmarshal([]byte(text), &v); err != nil {
				dump("non-sentinel text must be valid JSON: %v\nbody: %s", err, text)
			}
			// Telemetry: 0 increments if it fit at first marshal; 1
			// text_only if the reducer produced a fit.
			if len(rec.calls) > 1 {
				dump("non-sentinel must record at most once; got %+v", rec.calls)
			}
			if len(rec.calls) == 1 && rec.calls[0].tier != tierTextOnly {
				dump("non-sentinel reducer-fit must increment text_only; got %+v", rec.calls)
			}
		}
	}
}

// TestRecallReducerFlagToHintFaithfulness fuzzes the recall reducer across
// random fixtures and asserts the composed Hint reflects exactly the
// reductions that actually happened — no claim without action, no silent
// action without claim. Compares final reduced state against the original.
func TestRecallReducerFlagToHintFaithfulness(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	const iterations = 60

	for i := range iterations {
		// Build a random fixture.
		memCount := r.Intn(20)
		mems := make([]mcpRecallMemory, memCount)
		anyLongContent := false
		for j := range mems {
			contentLen := r.Intn(1500) // up to ~1500 chars, may or may not trigger stage 2/3
			if contentLen > 200 {
				anyLongContent = true
			}
			mems[j] = mcpRecallMemory{
				ID:        uuid.New(),
				Content:   strings.Repeat("x", contentLen),
				Tags:      []string{"t"},
				Score:     float64(memCount - j),
				CreatedAt: time.Now(),
			}
		}
		hasGraph := r.Intn(2) == 0
		var graph graphResponse
		if hasGraph {
			graph = graphResponse{
				Entities:      []graphEntity{{ID: uuid.New(), Name: "n", Type: "t"}},
				Relationships: []graphRelationship{},
			}
		} else {
			graph = graphResponse{Entities: []graphEntity{}, Relationships: []graphRelationship{}}
		}
		gapCount := r.Intn(15)
		gaps := make([]service.CoverageGap, gapCount)
		for j := range gaps {
			gaps[j] = service.CoverageGap{GroupKey: fmt.Sprintf("g-%d", j), Cause: "limit"}
		}

		orig := &mcpRecallResponse{
			Memories:     mems,
			Graph:        graph,
			CoverageGaps: gaps,
			LatencyMs:    1,
		}

		// Drive the reducer to exhaustion.
		reducer := newRecallReducer(orig, false)
		var last *mcpRecallResponse
		for range maxReducerIterations {
			smaller, more := reducer()
			if smaller == nil {
				break
			}
			last = smaller.(*mcpRecallResponse)
			if !more {
				break
			}
		}

		if last == nil {
			// Reducer never produced output (degenerate empty case). Skip.
			continue
		}

		// Detect the structural changes vs orig.
		graphActuallyDropped := hasGraph && len(last.Graph.Entities) == 0 && len(last.Graph.Relationships) == 0
		memoriesActuallyHalved := len(last.Memories) < memCount
		coverageGapsActuallyTrimmed := len(last.CoverageGaps) < gapCount
		// Content-trim detection: the reducer always runs the content stages
		// (1-2) before halving (the reducer is driven to exhaustion here). If any
		// memory originally had content > 200 chars, stage 2 trimmed it — even if
		// that memory was later halved away by the halving stages, the flag was
		// set legitimately at the time of the trim.
		contentActuallyTrimmed := anyLongContent

		// If the fixture admitted no actual reduction (graph empty, all
		// content short, ≤1 memory, ≤1 coverage gap), the reducer correctly
		// returns the original payload unmodified with Truncated nil. There
		// is nothing further to assert.
		if !graphActuallyDropped && !memoriesActuallyHalved && !coverageGapsActuallyTrimmed && !contentActuallyTrimmed {
			if last.Truncated != nil {
				t.Errorf("[seed=42 iter=%d] no reduction happened but Truncated was set: %+v", i, last.Truncated)
			}
			continue
		}

		if last.Truncated == nil {
			t.Errorf("[seed=42 iter=%d] expected Truncated set on final reduced response", i)
			continue
		}
		hint := last.Truncated.Hint
		dropped := last.Truncated.Dropped

		dump := func(msg string, extra ...any) {
			args := append([]any{msg, i, hint, dropped}, extra...)
			t.Errorf("[seed=42 iter=%d hint=%q dropped=%v] "+args[0].(string), args[1:]...)
		}

		// Hint must mention graph if and only if it was dropped.
		hintMentionsGraph := strings.Contains(hint, "graph")
		if graphActuallyDropped && !hintMentionsGraph {
			dump("graph was dropped but hint does not mention it")
		}
		if !graphActuallyDropped && hintMentionsGraph {
			dump("hint mentions graph but graph was not dropped")
		}

		// Dropped must claim graph fields iff graph dropped.
		droppedClaimsGraph := false
		for _, d := range dropped {
			if strings.Contains(d, "graph.") {
				droppedClaimsGraph = true
			}
		}
		if graphActuallyDropped && !droppedClaimsGraph {
			dump("graph was dropped but Dropped does not list graph.* fields")
		}
		if !graphActuallyDropped && droppedClaimsGraph {
			dump("Dropped claims graph.* fields but graph was not dropped")
		}

		// Hint must mention memory list halving iff it happened.
		hintMentionsHalve := strings.Contains(hint, "memory list halved")
		if memoriesActuallyHalved && !hintMentionsHalve {
			dump("memories were halved but hint does not mention it")
		}
		if !memoriesActuallyHalved && hintMentionsHalve {
			dump("hint mentions memory list halving but memories not halved")
		}

		// Hint must mention coverage_gaps iff it was trimmed.
		hintMentionsGaps := strings.Contains(hint, "coverage_gaps")
		if coverageGapsActuallyTrimmed && !hintMentionsGaps {
			dump("coverage_gaps trimmed but hint does not mention it")
		}
		if !coverageGapsActuallyTrimmed && hintMentionsGaps {
			dump("hint mentions coverage_gaps but it was not trimmed")
		}

		// Hint must mention content trim iff it happened.
		hintMentionsContent := strings.Contains(hint, "content")
		if contentActuallyTrimmed && !hintMentionsContent {
			dump("memory content was trimmed but hint does not mention it")
		}
		if !contentActuallyTrimmed && hintMentionsContent {
			dump("hint mentions content trim but no content was actually shortened")
		}
	}
}

// TestTruncationCounterIncrements pins that wrapToolResult invokes the
// MetricsRecorder with the correct (tool, tier) labels for Tier 2 and
// Tier 3 outcomes (and never for Tier 1).
func TestTruncationCounterIncrements(t *testing.T) {
	t.Run("tier1_no_increment", func(t *testing.T) {
		budget := 10000 * charsPerTokenEstimate
		rec := &stubMetrics{}
		_, _ = wrapToolResult(rec, "tier1", budget, map[string]string{"k": "v"}, nil)
		if len(rec.calls) != 0 {
			t.Errorf("Tier 1 must not increment counter; got %+v", rec.calls)
		}
	})
	t.Run("tier2_text_only", func(t *testing.T) {
		budget := 600 * charsPerTokenEstimate
		rec := &stubMetrics{}
		big := strings.Repeat("a", 900)
		_, _ = wrapToolResult(rec, "tier2", budget, struct {
			Blob string `json:"blob"`
		}{Blob: big}, nil)
		if len(rec.calls) != 1 || rec.calls[0].tier != tierTextOnly {
			t.Errorf("Tier 2 must increment text_only once; got %+v", rec.calls)
		}
		if rec.calls[0].tool != "tier2" {
			t.Errorf("Tier 2 counter tool label = %q, want %q", rec.calls[0].tool, "tier2")
		}
	})
	t.Run("tier3_hard_truncate", func(t *testing.T) {
		budget := 100 * charsPerTokenEstimate // validator minimum
		rec := &stubMetrics{}
		big := strings.Repeat("a", 50000)
		_, _ = wrapToolResult(rec, "tier3", budget, struct {
			Blob string `json:"blob"`
		}{Blob: big}, nil)
		if len(rec.calls) != 1 || rec.calls[0].tier != tierHardTruncate {
			t.Errorf("Tier 3 must increment hard_truncate once; got %+v", rec.calls)
		}
	})
}

// TestTier1AfterReducerRecordsTelemetry pins that when wrapToolResult reaches
// Tier 1 via the in-loop short-circuit (the reducer ran at least once and
// happened to land at structured-fit), the counter increments with the
// tier1_reduced label — distinguishing "fit naturally" from "fit only
// because the reducer discarded data".
func TestTier1AfterReducerRecordsTelemetry(t *testing.T) {
	// Budget chosen so the original recall response overflows the
	// structured budget but the reducer's stage-2/3 content trim brings
	// it back inside.
	budget := 2000 * charsPerTokenEstimate // 4000B; structured budget 2000B
	mems := make([]mcpRecallMemory, 5)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   strings.Repeat("y", 1500), // exceeds 800 trigger; stage 2 trims
			Tags:      []string{"a"},
			Score:     float64(5 - i),
			CreatedAt: time.Now(),
		}
	}
	resp := &mcpRecallResponse{Memories: mems, LatencyMs: 1}
	rec := &stubMetrics{}
	res, err := wrapToolResult(rec, "recall", budget, resp, newRecallReducer(resp, false))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	if classifyTier(res) != "tier1" {
		t.Fatalf("expected tier1 after reducer; got %s", classifyTier(res))
	}
	if len(rec.calls) != 1 || rec.calls[0].tier != tierTier1Reduced {
		t.Errorf("expected one tier1_reduced increment; got %+v", rec.calls)
	}
}

// TestRecallReducerNoopStageReturnsOriginal pins that when the content-trim
// stages (1-2) are no-ops (short content), the reducer returns the original
// payload byte-for-byte WITHOUT stamping a Truncated envelope. A no-op stage
// must not pretend a reduction happened. (Stage 3+ halves the >1-element memory
// list, which IS a reduction, so it is intentionally outside this loop.)
func TestRecallReducerNoopStageReturnsOriginal(t *testing.T) {
	mems := make([]mcpRecallMemory, 3)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   "short", // well under 200 chars — stages 1-2 are no-ops
			Tags:      []string{"a"},
			Score:     float64(3 - i),
			CreatedAt: time.Now(),
		}
	}
	// Empty graph means there is no graph reduction to claim either.
	resp := &mcpRecallResponse{
		Memories: mems,
		Graph:    graphResponse{Entities: []graphEntity{}, Relationships: []graphRelationship{}},
	}
	reducer := newRecallReducer(resp, false)
	// Drive the content stages (1, 2) — both no-ops. Each must return the
	// original pointer with Truncated == nil.
	for stage := 1; stage <= 2; stage++ {
		out, more := reducer()
		if out == nil {
			t.Fatalf("stage %d returned nil unexpectedly", stage)
		}
		got, ok := out.(*mcpRecallResponse)
		if !ok {
			t.Fatalf("stage %d returned %T, want *mcpRecallResponse", stage, out)
		}
		if got.Truncated != nil {
			t.Errorf("stage %d set Truncated despite being a no-op: %+v", stage, got.Truncated)
		}
		if got != resp {
			t.Errorf("stage %d returned a copy instead of the original pointer", stage)
		}
		if !more {
			t.Errorf("stage %d signaled more=false early", stage)
		}
	}
}

// TestBatchGetReducerSurfacesDroppedIDs pins that newBatchGetReducer
// accumulates the stringified UUIDs of the trimmed Found-tail items into
// Truncated.DroppedIDs. NotFound is preserved verbatim.
func TestBatchGetReducerSurfacesDroppedIDs(t *testing.T) {
	found := make([]mcpMemoryDetail, 8)
	wantIDs := make([]string, 0, 8)
	for i := range found {
		id := uuid.New()
		found[i] = mcpMemoryDetail{
			ID:        id,
			Content:   strings.Repeat("c", 200),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		wantIDs = append(wantIDs, id.String())
	}
	notFound := []uuid.UUID{uuid.New(), uuid.New()}
	resp := &mcpBatchGetResponse{Found: found, NotFound: notFound}

	reducer := newBatchGetReducer(resp)
	// First halving: 8 -> 4. DroppedIDs should contain wantIDs[4..7].
	out, _ := reducer()
	got1, ok := out.(*mcpBatchGetResponse)
	if !ok {
		t.Fatalf("reducer returned %T", out)
	}
	if len(got1.Found) != 4 {
		t.Errorf("expected Found halved to 4; got %d", len(got1.Found))
	}
	if got1.Truncated == nil || len(got1.Truncated.DroppedIDs) != 4 {
		t.Fatalf("expected 4 DroppedIDs on first halving; got %+v", got1.Truncated)
	}
	for i, want := range wantIDs[4:8] {
		if got1.Truncated.DroppedIDs[i] != want {
			t.Errorf("DroppedIDs[%d]=%q want %q", i, got1.Truncated.DroppedIDs[i], want)
		}
	}
	if len(got1.NotFound) != 2 {
		t.Errorf("NotFound must be preserved verbatim; got %d", len(got1.NotFound))
	}

	// Second halving: 4 -> 2. DroppedIDs should accumulate to 6 entries
	// (wantIDs[4..7] plus wantIDs[2..3]).
	out, _ = reducer()
	got2, _ := out.(*mcpBatchGetResponse)
	if len(got2.Truncated.DroppedIDs) != 6 {
		t.Errorf("expected accumulated 6 DroppedIDs after second halving; got %d (%v)", len(got2.Truncated.DroppedIDs), got2.Truncated.DroppedIDs)
	}
}

// (the real regression test for handleProjectsResource Pagination lives in
// resources_test.go where the other handler-level tests are colocated.)

// TestBatchGetReducerDrainsToEmptyOnFinalIteration pins that the batch_get
// reducer drains Found all the way to zero across iterations, accumulating
// every dropped ID into DroppedIDs. The old behavior floored at len==1 and
// returned (nil,false), which forced tier-3 to byte-cut a single oversized
// memory into invalid mid-content JSON. The new behavior produces a small,
// parseable Found=[] + DroppedIDs=[every id] envelope that the caller can
// act on by re-issuing per-id get calls.
func TestBatchGetReducerDrainsToEmptyOnFinalIteration(t *testing.T) {
	found := make([]mcpMemoryDetail, 3)
	ids := make([]string, 3)
	for i := range found {
		id := uuid.New()
		ids[i] = id.String()
		found[i] = mcpMemoryDetail{ID: id, Content: "c"}
	}
	resp := &mcpBatchGetResponse{Found: found}
	reducer := newBatchGetReducer(resp)

	// 3 -> 1 (keep=1, drops ids[1..2])
	out, more := reducer()
	got, _ := out.(*mcpBatchGetResponse)
	if len(got.Found) != 1 || len(got.Truncated.DroppedIDs) != 2 {
		t.Fatalf("iter1: Found=%d DroppedIDs=%d; want 1,2", len(got.Found), len(got.Truncated.DroppedIDs))
	}
	if !more {
		t.Fatalf("iter1: expected more=true while Found still has entries")
	}

	// 1 -> 0 (final drain; drops ids[0])
	out, more = reducer()
	got, _ = out.(*mcpBatchGetResponse)
	if len(got.Found) != 0 {
		t.Fatalf("iter2: Found=%d; want 0 after final drain", len(got.Found))
	}
	if len(got.Truncated.DroppedIDs) != 3 {
		t.Fatalf("iter2: DroppedIDs=%d; want 3 (all original ids)", len(got.Truncated.DroppedIDs))
	}
	if more {
		t.Fatalf("iter2: expected more=false after Found drained to empty")
	}

	// Third call must yield (nil, false): nothing left to shrink.
	out, more = reducer()
	if out != nil || more {
		t.Fatalf("iter3: expected (nil,false) after drain; got (%v,%v)", out, more)
	}
}

// TestRecallReducerPreservesSingleMemoryAtStage4 pins that stage 4+ does
// NOT halve a single-element memories slice to empty. The old behavior set
// memoriesHalved=true and emitted ReturnedCount=0, telling the caller their
// recall query returned nothing when in fact one hit existed and tier-3
// byte-cutting would have surfaced it. The new behavior leaves the single
// memory in place, lets the loop exhaust on the default branch when nothing
// further can shrink, and lets the wrapper fall to tier-3 with the
// single-memory payload intact.
func TestRecallReducerPreservesSingleMemoryAtStage4(t *testing.T) {
	resp := &mcpRecallResponse{
		Memories: []mcpRecallMemory{{
			ID:        uuid.New(),
			Content:   "short", // < 200 chars; stages 2/3 are no-ops
			Tags:      []string{"t"},
			Score:     1,
			CreatedAt: time.Now(),
		}},
		Graph:        graphResponse{Entities: []graphEntity{}, Relationships: []graphRelationship{}},
		CoverageGaps: nil,
		LatencyMs:    1,
	}
	reducer := newRecallReducer(resp, false)

	// Drive the reducer to exhaustion. The final non-nil output, if any,
	// must NOT have memoriesHalved set; ReturnedCount must equal 1 if a
	// Truncated envelope is emitted at all.
	var last *mcpRecallResponse
	for range maxReducerIterations {
		smaller, more := reducer()
		if smaller == nil {
			break
		}
		last = smaller.(*mcpRecallResponse)
		if !more {
			break
		}
	}
	if last == nil {
		t.Fatal("reducer produced no output at all")
	}
	if len(last.Memories) == 0 {
		t.Fatalf("reducer zeroed the single memory; got Memories=%d, want 1", len(last.Memories))
	}
	if last.Truncated != nil && last.Truncated.ReturnedCount == 0 {
		t.Fatalf("reducer emitted Truncated{ReturnedCount:0} for a 1-memory input; want either nil Truncated or ReturnedCount==1")
	}
}

// TestMCPBudgetBytesFloorsAtSentinelSize pins that mcpBudgetBytes clamps
// upward to at least len(truncationSuffix) bytes. The admin schema's Min=100
// is enforced only at the HTTP write path; service.SettingsService.Set
// skips schema validation, so a caller (test, migration, future internal
// setter) can persist a tiny value. Without this floor, hardTruncate would
// compute keep = budget - len(truncationSuffix) < 0, clamp keep to 0, and
// emit the full 108-byte sentinel — violating its own budget contract.
func TestMCPBudgetBytesFloorsAtSentinelSize(t *testing.T) {
	// Stub a SettingsService that resolves a value far below the schema
	// minimum, simulating a Set() that bypassed validation.
	svc := newSettingsServiceWithMCPBudget(10) // 10 tokens × 2 = 20 bytes raw
	got := mcpBudgetBytes(context.Background(), svc)
	if got < len(truncationSuffix) {
		t.Fatalf("mcpBudgetBytes returned %d, want >= %d (len(truncationSuffix))", got, len(truncationSuffix))
	}
	// hardTruncate at this floor must honor the budget — sentinel fits exactly.
	out := []byte(strings.Repeat("x", 5000))
	text := hardTruncate(out, got)
	if len(text) > got {
		t.Fatalf("hardTruncate exceeded floored budget: got %d bytes, budget %d", len(text), got)
	}
	if !strings.HasSuffix(text, truncationSuffix) {
		t.Fatalf("hardTruncate at floor must still emit sentinel; tail=%q", text[max(0, len(text)-30):])
	}
}

// TestGraphReducerShrinksBothAxes pins the central invariant of the
// rebalanced graph reducer: when oversized, BOTH entities and relationships
// shrink on each iteration (parallel halving) instead of one axis being
// drained to zero before the other is touched. The prior algorithm halved
// relationships first via a switch — for a typical dense graph
// (graph.max_edges=2000), all 2000 edges drained to 0 in 11 iterations
// before a single entity was trimmed, producing a node-list-with-no-edges
// payload that defeated the graph tool's purpose.
//
// Assertion: with a payload large enough to require multiple halvings,
// after one reducer iteration both entities count AND relationships count
// have decreased (strictly less than the originals).
func TestGraphReducerShrinksBothAxes(t *testing.T) {
	const (
		nEntities = 64
		nRels     = 128
	)
	entities := make([]graphEntity, nEntities)
	for i := range entities {
		entities[i] = graphEntity{ID: uuid.New(), Name: fmt.Sprintf("e-%d", i), Type: "person", MentionCount: nEntities - i}
	}
	rels := make([]graphRelationship, nRels)
	for i := range rels {
		rels[i] = graphRelationship{
			SourceID: uuid.New(),
			TargetID: uuid.New(),
			Relation: "knows",
			Weight:   1.0,
		}
	}
	orig := &graphResponse{Entities: entities, Relationships: rels}

	reducer := newGraphReducer(orig)
	smaller, more := reducer()
	if smaller == nil {
		t.Fatalf("reducer returned nil on first call with oversized input; expected a smaller payload")
	}
	if !more {
		t.Errorf("reducer signaled more=false on first call when both axes have >1 items; expected more=true")
	}
	reduced, ok := smaller.(*graphResponse)
	if !ok {
		t.Fatalf("reducer returned %T; want *graphResponse", smaller)
	}
	if len(reduced.Entities) >= nEntities {
		t.Errorf("entities did not shrink: got %d, started %d (the bug this test guards: one axis stays full while the other drains)", len(reduced.Entities), nEntities)
	}
	if len(reduced.Relationships) >= nRels {
		t.Errorf("relationships did not shrink: got %d, started %d (the bug this test guards: one axis stays full while the other drains)", len(reduced.Relationships), nRels)
	}
}

// TestGraphReducerNeitherAxisReachesZeroFromNonZero pins the structural
// floor: as long as either axis started with at least one item, the
// reducer must never trim that axis to zero. The prior algorithm zeroed
// relationships under any pressure; the rebalanced algorithm preserves at
// least one item per non-empty axis so a graph response always carries
// some signal until the wrapper falls through to tier-3 hardTruncate.
//
// Assertion: drive the reducer to exhaustion. After every call, both
// counts are either zero (if started at zero) or >= 1.
func TestGraphReducerNeitherAxisReachesZeroFromNonZero(t *testing.T) {
	entities := []graphEntity{
		{ID: uuid.New(), Name: "alice", Type: "person", MentionCount: 5},
		{ID: uuid.New(), Name: "bob", Type: "person", MentionCount: 3},
		{ID: uuid.New(), Name: "carol", Type: "person", MentionCount: 1},
	}
	rels := []graphRelationship{
		{SourceID: uuid.New(), TargetID: uuid.New(), Relation: "knows", Weight: 0.9},
		{SourceID: uuid.New(), TargetID: uuid.New(), Relation: "knows", Weight: 0.5},
	}
	orig := &graphResponse{Entities: entities, Relationships: rels}

	reducer := newGraphReducer(orig)
	for i := range maxReducerIterations {
		smaller, more := reducer()
		if smaller == nil {
			// Reducer signaled exhaustion. Floor invariant already
			// enforced on the prior iteration's payload.
			break
		}
		reduced := smaller.(*graphResponse)
		if len(reduced.Entities) == 0 {
			t.Fatalf("iteration %d: entities reached 0 from non-zero start; the floor failed", i)
		}
		if len(reduced.Relationships) == 0 {
			t.Fatalf("iteration %d: relationships reached 0 from non-zero start; the floor failed", i)
		}
		if !more {
			break
		}
	}
}

// TestGraphReducerStallsWhenBothAtFloor pins the termination condition.
// Once both axes are at floor (len == 1) and the payload still exceeds
// budget, the reducer must signal (nil, false) so the wrapper falls
// through to hardTruncate rather than spinning maxReducerIterations times
// re-marshaling the same payload.
//
// Construction: a graphResponse with exactly one entity and one
// relationship. First reducer call: nothing to shrink, both already at
// floor → expect (nil, false).
func TestGraphReducerStallsWhenBothAtFloor(t *testing.T) {
	orig := &graphResponse{
		Entities:      []graphEntity{{ID: uuid.New(), Name: "alice", Type: "person", MentionCount: 1}},
		Relationships: []graphRelationship{{SourceID: uuid.New(), TargetID: uuid.New(), Relation: "knows", Weight: 1.0}},
	}
	reducer := newGraphReducer(orig)
	smaller, more := reducer()
	if smaller != nil {
		t.Errorf("expected nil smaller payload when both axes already at floor; got %T", smaller)
	}
	if more {
		t.Errorf("expected more=false at stall; got true (would loop indefinitely)")
	}
}

// TestGraphReducerDroppedReportsBothAxes pins that the truncation
// envelope reports kept/original counts SYMMETRICALLY for both entities
// and relationships. The prior reducer reported only the verbose tail
// being trimmed (no per-axis markers); the rebalanced reducer surfaces
// both axes so callers can see exactly what fraction of each survived.
func TestGraphReducerDroppedReportsBothAxes(t *testing.T) {
	const nE, nR = 50, 100
	entities := make([]graphEntity, nE)
	for i := range entities {
		entities[i] = graphEntity{ID: uuid.New(), Name: fmt.Sprintf("e-%d", i), Type: "person", MentionCount: nE - i}
	}
	rels := make([]graphRelationship, nR)
	for i := range rels {
		rels[i] = graphRelationship{
			SourceID: uuid.New(),
			TargetID: uuid.New(),
			Relation: "knows",
			Weight:   1.0,
		}
	}
	orig := &graphResponse{Entities: entities, Relationships: rels}

	reducer := newGraphReducer(orig)
	smaller, _ := reducer()
	reduced := smaller.(*graphResponse)
	if reduced.Truncated == nil {
		t.Fatalf("expected _truncated envelope; got nil")
	}
	dropped := reduced.Truncated.Dropped
	var sawEntities, sawRels bool
	for _, d := range dropped {
		if strings.HasPrefix(d, "entities_kept:") {
			sawEntities = true
			expected := fmt.Sprintf("entities_kept:%d/%d", len(reduced.Entities), nE)
			if d != expected {
				t.Errorf("entities_kept marker = %q, want %q", d, expected)
			}
		}
		if strings.HasPrefix(d, "relationships_kept:") {
			sawRels = true
			expected := fmt.Sprintf("relationships_kept:%d/%d", len(reduced.Relationships), nR)
			if d != expected {
				t.Errorf("relationships_kept marker = %q, want %q", d, expected)
			}
		}
	}
	if !sawEntities {
		t.Errorf("expected entities_kept marker in Dropped %v", dropped)
	}
	if !sawRels {
		t.Errorf("expected relationships_kept marker in Dropped %v", dropped)
	}
}

// TestHalveWithFloor pins the per-axis shrinkage rule the graph reducer
// composes on top of. n==0 stays at 0 (don't conjure data); n==1 stays at
// 1 (floor); n>=2 returns n/2. This is the building block that prevents
// the reducer from zeroing a non-empty axis.
func TestHalveWithFloor(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{0, 0},
		{1, 1},
		{2, 1},
		{3, 1},
		{4, 2},
		{100, 50},
		{2000, 1000},
	}
	for _, tc := range cases {
		if got := halveWithFloor(tc.in); got != tc.want {
			t.Errorf("halveWithFloor(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestSortGraphBySignalSharedWithGraphTool pins the ordering contract that both
// the graph tool (handleMemoryGraph) and the recall pre-cap now route through:
// relationships by Weight DESC, entities by MentionCount DESC, with deterministic
// tiebreaks so prefix truncation is stable across calls.
func TestSortGraphBySignalSharedWithGraphTool(t *testing.T) {
	ents := []graphEntity{
		{ID: uuid.New(), Name: "low", MentionCount: 1},
		{ID: uuid.New(), Name: "high", MentionCount: 9},
		{ID: uuid.New(), Name: "mid", MentionCount: 5},
	}
	rels := []graphRelationship{
		{SourceID: uuid.New(), TargetID: uuid.New(), Relation: "a", Weight: 0.2},
		{SourceID: uuid.New(), TargetID: uuid.New(), Relation: "b", Weight: 0.8},
	}
	sortGraphBySignal(ents, rels)
	if ents[0].MentionCount != 9 || ents[1].MentionCount != 5 || ents[2].MentionCount != 1 {
		t.Errorf("entities not sorted by MentionCount DESC: %+v", ents)
	}
	if rels[0].Weight != 0.8 || rels[1].Weight != 0.2 {
		t.Errorf("relationships not sorted by Weight DESC: %+v", rels)
	}

	// Equal-signal items get a total order via the ID tiebreak.
	a, b := uuid.New(), uuid.New()
	if a.String() > b.String() {
		a, b = b, a // ensure a < b lexically
	}
	tie := []graphEntity{
		{ID: b, Name: "same", MentionCount: 3},
		{ID: a, Name: "same", MentionCount: 3},
	}
	sortGraphBySignal(tie, nil)
	if tie[0].ID != a {
		t.Errorf("equal-mention tiebreak must order by ID asc; got %v before %v", tie[0].ID, tie[1].ID)
	}
}

// TestRecallGraphPreCapKeepsBalancedSubset is the direct unit test for the
// recall pre-cap: under a tiny byte reserve it trims BOTH axes (never zeroing
// one while the other survives) and emits the kept/total sentinels.
func TestRecallGraphPreCapKeepsBalancedSubset(t *testing.T) {
	// Asymmetric input (2 entities, 40 relationships) proves the small axis is
	// floor-protected while the large axis keeps halving.
	ents := make([]graphEntity, 2)
	for i := range ents {
		ents[i] = graphEntity{ID: uuid.New(), Name: fmt.Sprintf("e%d", i), Type: "concept", MentionCount: 2 - i}
	}
	rels := make([]graphRelationship, 40)
	for i := range rels {
		rels[i] = graphRelationship{SourceID: uuid.New(), TargetID: uuid.New(), Relation: "rel", Weight: float64(40 - i)}
	}
	sortGraphBySignal(ents, rels)

	const reserve = 500
	keptE, keptR, sentinels := packGraphToByteBudget(ents, rels, reserve)

	if len(keptE) == 0 || len(keptR) == 0 {
		t.Fatalf("balanced pack must keep >=1 of each axis; got entities=%d relationships=%d", len(keptE), len(keptR))
	}
	if len(keptR) >= 40 {
		t.Fatalf("expected the relationships axis to be trimmed under a tiny reserve; got %d", len(keptR))
	}
	out, _ := json.Marshal(graphResponse{Entities: keptE, Relationships: keptR})
	atFloor := len(keptE) == 1 && len(keptR) == 1
	if len(out) > reserve && !atFloor {
		t.Errorf("packed graph %d bytes exceeds reserve %d without being at the 1/1 floor", len(out), reserve)
	}
	if len(sentinels) != 2 {
		t.Fatalf("expected entities_kept + relationships_kept sentinels, got %v", sentinels)
	}
	if !strings.HasPrefix(sentinels[0], "entities_kept:") || !strings.HasPrefix(sentinels[1], "relationships_kept:") {
		t.Errorf("sentinel shape drift: %v", sentinels)
	}
}

// TestRecallGraphPreCapNoopWhenFits confirms the pre-cap leaves a graph that
// already fits the reserve untouched and emits no sentinels (so the handler
// stamps no truncation envelope).
func TestRecallGraphPreCapNoopWhenFits(t *testing.T) {
	ents := []graphEntity{{ID: uuid.New(), Name: "a", Type: "c", MentionCount: 1}}
	rels := []graphRelationship{{SourceID: uuid.New(), TargetID: uuid.New(), Relation: "r", Weight: 1}}
	keptE, keptR, sentinels := packGraphToByteBudget(ents, rels, 1_000_000)
	if len(keptE) != 1 || len(keptR) != 1 {
		t.Errorf("expected the graph to pass through untrimmed; got %d/%d", len(keptE), len(keptR))
	}
	if sentinels != nil {
		t.Errorf("expected no sentinels when the graph fits; got %v", sentinels)
	}

	// Empty graph is also a no-op.
	e2, r2, s2 := packGraphToByteBudget(nil, nil, 10)
	if len(e2) != 0 || len(r2) != 0 || s2 != nil {
		t.Errorf("empty graph must pack to empty with no sentinels; got %d/%d %v", len(e2), len(r2), s2)
	}
}

// TestRecallGraphPreCapDeterministic pins that the pre-cap keeps an identical
// prefix across two runs on identical input (relies on sortGraphBySignal's
// total order plus deterministic prefix slicing).
func TestRecallGraphPreCapDeterministic(t *testing.T) {
	mkGraph := func() ([]graphEntity, []graphRelationship) {
		ents := make([]graphEntity, 12)
		for i := range ents {
			ents[i] = graphEntity{
				ID:           uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
				Name:         fmt.Sprintf("e%d", i),
				Type:         "concept",
				MentionCount: i % 4, // deliberate ties to exercise the tiebreak
			}
		}
		rels := make([]graphRelationship, 18)
		for i := range rels {
			rels[i] = graphRelationship{
				SourceID: ents[i%12].ID,
				TargetID: ents[(i+1)%12].ID,
				Relation: "r",
				Weight:   float64(i % 5), // deliberate ties
			}
		}
		return ents, rels
	}

	run := func() ([]graphEntity, []graphRelationship, []string) {
		e, r := mkGraph()
		sortGraphBySignal(e, r)
		return packGraphToByteBudget(e, r, 400)
	}
	e1, r1, s1 := run()
	e2, r2, s2 := run()

	if len(e1) != len(e2) || len(r1) != len(r2) {
		t.Fatalf("nondeterministic kept counts: run1 %d/%d run2 %d/%d", len(e1), len(r1), len(e2), len(r2))
	}
	for i := range e1 {
		if e1[i].ID != e2[i].ID {
			t.Errorf("entity prefix diverged at %d: %v vs %v", i, e1[i].ID, e2[i].ID)
		}
	}
	for i := range r1 {
		if r1[i] != r2[i] {
			t.Errorf("relationship prefix diverged at %d", i)
		}
	}
	if strings.Join(s1, "|") != strings.Join(s2, "|") {
		t.Errorf("sentinels diverged: %v vs %v", s1, s2)
	}
}

// TestRecallReducerNeverDropsGraphWholesaleUnderTypicalBudget is the headline
// behavioral guard: an over-budget recall whose overflow is absorbable by
// trimming memories must keep BOTH graph axes, never falling back to the
// last-resort wholesale drop.
func TestRecallReducerNeverDropsGraphWholesaleUnderTypicalBudget(t *testing.T) {
	budget := 2000 * charsPerTokenEstimate // 4000B; structured 2000B
	mems := make([]mcpRecallMemory, 8)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   strings.Repeat("z", 1200),
			Tags:      []string{"a"},
			Score:     float64(8 - i),
			CreatedAt: time.Now(),
		}
	}
	graph := graphResponse{
		Entities: []graphEntity{
			{ID: uuid.New(), Name: "A", Type: "concept", MentionCount: 5},
			{ID: uuid.New(), Name: "B", Type: "concept", MentionCount: 3},
		},
		Relationships: []graphRelationship{
			{SourceID: uuid.New(), TargetID: uuid.New(), Relation: "knows", Weight: 0.9},
		},
	}
	resp := &mcpRecallResponse{Memories: mems, Graph: graph, LatencyMs: 1}

	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp, false))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	var decoded mcpRecallResponse
	if err := json.Unmarshal([]byte(extractText(res)), &decoded); err != nil {
		t.Fatalf("reduced result is not valid JSON: %v", err)
	}
	if len(decoded.Graph.Entities) == 0 {
		t.Errorf("graph entities must survive a memory-trimmable reduction; got 0")
	}
	if len(decoded.Graph.Relationships) == 0 {
		t.Errorf("graph relationships must survive a memory-trimmable reduction; got 0")
	}
	if decoded.Truncated != nil {
		for _, d := range decoded.Truncated.Dropped {
			if d == "graph.entities" || d == "graph.relationships" {
				t.Errorf("graph must not be dropped wholesale here; dropped=%v", decoded.Truncated.Dropped)
			}
		}
	}
}

// TestRecallReducerDropsGraphOnlyAsLastResort pins the inverse: when memories
// are at floor and the payload still overflows, the graph is dropped — but only
// AFTER it survived every earlier stage. Drives the reducer directly so the
// last-resort stage is observed without depending on tier classification.
func TestRecallReducerDropsGraphOnlyAsLastResort(t *testing.T) {
	resp := &mcpRecallResponse{
		Memories: []mcpRecallMemory{{
			ID:        uuid.New(),
			Content:   "short", // content trim cannot help
			Tags:      []string{"a"},
			CreatedAt: time.Now(),
		}},
		Graph: graphResponse{
			Entities:      []graphEntity{{ID: uuid.New(), Name: "A", Type: "concept"}},
			Relationships: []graphRelationship{{SourceID: uuid.New(), TargetID: uuid.New(), Relation: "r", Weight: 1}},
		},
		LatencyMs: 1,
	}
	reducer := newRecallReducer(resp, false)

	var last *mcpRecallResponse
	graphIntactBeforeDrop := false
	for range maxReducerIterations {
		smaller, more := reducer()
		if smaller == nil {
			break
		}
		r := smaller.(*mcpRecallResponse)
		if len(r.Graph.Entities) > 0 || len(r.Graph.Relationships) > 0 {
			graphIntactBeforeDrop = true
		}
		last = r
		if !more {
			break
		}
	}
	if last == nil {
		t.Fatal("reducer produced no output")
	}
	if !graphIntactBeforeDrop {
		t.Error("graph should have survived the earlier stages before the last-resort drop")
	}
	if len(last.Graph.Entities) != 0 || len(last.Graph.Relationships) != 0 {
		t.Errorf("last-resort stage must empty the graph; got entities=%d rels=%d", len(last.Graph.Entities), len(last.Graph.Relationships))
	}
	if last.Truncated == nil {
		t.Fatal("expected Truncated set on the last-resort output")
	}
	hasGraphDrop := false
	for _, d := range last.Truncated.Dropped {
		if d == "graph.entities" || d == "graph.relationships" {
			hasGraphDrop = true
		}
	}
	if !hasGraphDrop {
		t.Errorf("expected graph.entities/graph.relationships drop sentinels at exhaustion; got %v", last.Truncated.Dropped)
	}
}
