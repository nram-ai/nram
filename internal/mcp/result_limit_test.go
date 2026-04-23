package mcp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

func TestMaxResultBytesHonorsEnv(t *testing.T) {
	t.Setenv("NRAM_MCP_MAX_RESULT_TOKENS", "1000")
	got := maxResultBytes()
	want := 1000 * charsPerTokenEstimate
	if got != want {
		t.Fatalf("maxResultBytes() = %d, want %d", got, want)
	}
}

func TestMaxResultBytesDefault(t *testing.T) {
	t.Setenv("NRAM_MCP_MAX_RESULT_TOKENS", "")
	got := maxResultBytes()
	want := defaultMaxResultTokens * charsPerTokenEstimate
	if got != want {
		t.Fatalf("maxResultBytes() = %d, want %d", got, want)
	}
}

func TestWrapToolResultUnderBudget(t *testing.T) {
	t.Setenv("NRAM_MCP_MAX_RESULT_TOKENS", "1000")
	payload := map[string]string{"hello": "world"}
	res, err := wrapToolResult(payload, nil)
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
	t.Setenv("NRAM_MCP_MAX_RESULT_TOKENS", "100") // 200 byte budget
	big := strings.Repeat("x", 5000)
	res, err := wrapToolResult(map[string]string{"data": big}, nil)
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	if !strings.HasSuffix(text, truncationSuffix) {
		tail := len(text)
		if tail > 120 {
			tail = 120
		}
		t.Fatalf("expected truncation suffix, got tail %q", text[len(text)-tail:])
	}
	if len(text) > maxResultBytes() {
		t.Fatalf("hard-truncated result %d bytes exceeds budget %d", len(text), maxResultBytes())
	}
}

func TestWrapToolResultUsesReducer(t *testing.T) {
	t.Setenv("NRAM_MCP_MAX_RESULT_TOKENS", "400") // 800 byte budget
	// Build a recall response that will overflow.
	mems := make([]service.RecallResult, 50)
	for i := range mems {
		mems[i] = service.RecallResult{
			ID:        uuid.New(),
			Content:   strings.Repeat("lorem ipsum ", 80),
			Tags:      []string{"a", "b"},
			Score:     float64(50 - i),
			CreatedAt: time.Now(),
		}
	}
	resp := &service.RecallResponse{
		Memories: mems,
		Graph: service.RecallGraph{
			Entities:      []service.RecallEntity{{ID: uuid.New(), Name: "x", EntityType: "concept"}},
			Relationships: []service.RecallRelationship{},
		},
		TotalSearched: 999,
		LatencyMs:     12,
	}
	res, err := wrapToolResult(resp, newRecallReducer(resp))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	if len(text) > maxResultBytes() {
		t.Fatalf("reduced result %d bytes still exceeds budget %d", len(text), maxResultBytes())
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

// TestRecallReducerPreservesCoverageGaps asserts that coverage_gaps, which is
// load-bearing diagnostic data for callers using diversify_by_tag_prefix,
// passes through the reducer verbatim even when the rest of the payload is
// aggressively shrunk.
func TestRecallReducerPreservesCoverageGaps(t *testing.T) {
	t.Setenv("NRAM_MCP_MAX_RESULT_TOKENS", "400") // 800 byte budget, forces reduction

	mems := make([]service.RecallResult, 50)
	for i := range mems {
		mems[i] = service.RecallResult{
			ID:        uuid.New(),
			Content:   strings.Repeat("lorem ipsum ", 80),
			Tags:      []string{"category-a"},
			Score:     float64(50 - i),
			CreatedAt: time.Now(),
		}
	}
	gaps := []service.CoverageGap{
		{GroupKey: "category-b", Cause: "limit"},
		{GroupKey: "category-c", Cause: "threshold"},
		{GroupKey: "category-d", Cause: "tag_filter"},
	}
	resp := &service.RecallResponse{
		Memories:     mems,
		CoverageGaps: gaps,
		LatencyMs:    7,
	}

	res, err := wrapToolResult(resp, newRecallReducer(resp))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	if len(text) > maxResultBytes() {
		t.Fatalf("reduced result %d bytes still exceeds budget %d", len(text), maxResultBytes())
	}

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
		t.Errorf("expected 3 coverage_gaps, got %d", len(arr))
	}
}

// TestRecallReducerOmitsCoverageGapsWhenEmpty confirms that responses not
// using diversification do not gain a spurious coverage_gaps field.
func TestRecallReducerOmitsCoverageGapsWhenEmpty(t *testing.T) {
	t.Setenv("NRAM_MCP_MAX_RESULT_TOKENS", "400")

	mems := make([]service.RecallResult, 50)
	for i := range mems {
		mems[i] = service.RecallResult{
			ID:        uuid.New(),
			Content:   strings.Repeat("x", 200),
			Tags:      []string{"a"},
			Score:     float64(50 - i),
			CreatedAt: time.Now(),
		}
	}
	resp := &service.RecallResponse{Memories: mems, LatencyMs: 1}

	res, err := wrapToolResult(resp, newRecallReducer(resp))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	if strings.Contains(text, "coverage_gaps") {
		t.Errorf("coverage_gaps should be absent when unset, got %s", text)
	}
}

func TestNewListReducerProducesValidPagination(t *testing.T) {
	t.Setenv("NRAM_MCP_MAX_RESULT_TOKENS", "400") // 800 byte budget
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
	resp := listMemoryResponse{
		Data: items,
		Pagination: model.Pagination{
			Total:  500,
			Limit:  100,
			Offset: 0,
		},
	}
	res, err := wrapToolResult(resp, newListReducer(resp))
	if err != nil {
		t.Fatalf("wrapToolResult err = %v", err)
	}
	text := extractText(res)
	if len(text) > maxResultBytes() {
		t.Fatalf("reduced list result %d bytes exceeds budget %d", len(text), maxResultBytes())
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

func TestWrapToolResultTextRespectsBudget(t *testing.T) {
	t.Setenv("NRAM_MCP_MAX_RESULT_TOKENS", "100")
	big := strings.Repeat("y", 5000)
	res, err := wrapToolResultText(big)
	if err != nil {
		t.Fatalf("wrapToolResultText err = %v", err)
	}
	text := extractText(res)
	if len(text) > maxResultBytes() {
		t.Fatalf("text result %d bytes exceeds budget %d", len(text), maxResultBytes())
	}
	if !strings.HasSuffix(text, truncationSuffix) {
		t.Fatalf("expected truncation suffix on text result")
	}
}

