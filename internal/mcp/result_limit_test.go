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
	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp))
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

// TestRecallReducerPreservesCoverageGapsThroughStages1To3 pins that
// coverage_gaps is NOT trimmed during stages 1-3 (graph drop, content
// truncation) — only at stage 4+ does it halve in lockstep with memories.
// Callers relying on the diversify diagnostic should always see it when the
// reducer was able to fit by trimming content rather than the diagnostic.
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

	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp))
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

	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp))
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
	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp))
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

// TestMCPExportResponseTopLevelKeys pins the embedded-field JSON promotion of
// mcpExportResponse. service.ExportData is embedded by value; if a future
// change adds a custom MarshalJSON on ExportData (or a field whose json tag
// collides with `_truncated`), encoding/json's anonymous-field promotion
// silently breaks and the wire shape changes from
// {version, exported_at, project, memories, entities, relationships, stats, _truncated}
// to {ExportData: {...}, _truncated: ...} (or drops _truncated). This test
// catches the regression at compile-or-test time.
func TestMCPExportResponseTopLevelKeys(t *testing.T) {
	resp := &mcpExportResponse{
		ExportData: service.ExportData{
			Version: "1",
		},
		Truncated: &truncationInfo{Reason: "test"},
	}
	out, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal mcpExportResponse: %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(out, &top); err != nil {
		t.Fatalf("unmarshal top-level: %v", err)
	}
	want := []string{
		"version", "exported_at", "project",
		"memories", "entities", "relationships", "stats",
		"_truncated",
	}
	for _, k := range want {
		if _, ok := top[k]; !ok {
			t.Errorf("expected top-level key %q on mcpExportResponse wire shape; got keys %v", k, mapKeys(top))
		}
	}
	if _, ok := top["ExportData"]; ok {
		t.Errorf("unexpected unflattened 'ExportData' key — embedded promotion broke; got keys %v", mapKeys(top))
	}
}

func mapKeys(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

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

// TestRecallReducerHintsComposed pins that a multi-stage reduction
// (graph drop + content trim) emits a hint that mentions BOTH reductions,
// not just the last one. The recallReductions flag struct composes hints
// from every flag set so far.
func TestRecallReducerHintsComposed(t *testing.T) {
	budget := 1500 * charsPerTokenEstimate
	mems := make([]mcpRecallMemory, 5)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   strings.Repeat("x", 2000), // forces stages 2/3
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
		LatencyMs: 1,
	}
	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp))
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
		t.Errorf("composed hint must mention graph drop; got %q", hint)
	}
	if !strings.Contains(hint, "content") {
		t.Errorf("composed hint must mention content truncation; got %q", hint)
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
	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp))
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

// TestRecallReducerCoverageGapsTrimmed pins that stage 4+ halves
// coverage_gaps in lockstep with memories and records the trim index in
// Dropped.
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
	res, err := wrapToolResult(&stubMetrics{}, "recall", budget, resp, newRecallReducer(resp))
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
		reducer := newRecallReducer(orig)
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
		// Content-trim detection: the reducer always runs stages 1-3 before
		// halving (the reducer is driven to exhaustion here). If any memory
		// originally had content > 200 chars, stage 3 trimmed it — even if
		// that memory was later halved away by stage 4+, the flag was set
		// legitimately at the time of the trim.
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
	res, err := wrapToolResult(rec, "recall", budget, resp, newRecallReducer(resp))
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

// TestRecallReducerNoopStageReturnsOriginal pins that when stages 1-3 are
// no-ops (empty graph, short content), the reducer returns the original
// payload byte-for-byte WITHOUT stamping a Truncated envelope. A no-op stage
// must not pretend a reduction happened.
func TestRecallReducerNoopStageReturnsOriginal(t *testing.T) {
	mems := make([]mcpRecallMemory, 3)
	for i := range mems {
		mems[i] = mcpRecallMemory{
			ID:        uuid.New(),
			Content:   "short", // well under 200 chars — stages 2/3 are no-ops
			Tags:      []string{"a"},
			Score:     float64(3 - i),
			CreatedAt: time.Now(),
		}
	}
	// Empty graph means stage 1 is a no-op too.
	resp := &mcpRecallResponse{
		Memories: mems,
		Graph:    graphResponse{Entities: []graphEntity{}, Relationships: []graphRelationship{}},
	}
	reducer := newRecallReducer(resp)
	// Drive stages 1, 2, 3 — all no-ops. Each must return the original
	// pointer with Truncated == nil.
	for stage := 1; stage <= 3; stage++ {
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
		if !more && stage < 3 {
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
	reducer := newRecallReducer(resp)

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
