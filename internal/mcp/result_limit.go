package mcp

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/service"
)

// MCP clients enforce a hard ceiling on tool result size. Claude Code and
// Cursor both reject results above 25,000 tokens by default. We adopt the
// minimum of those ceilings minus a safety margin so that JSON-RPC envelope,
// content-block framing, and tokenizer drift cannot push us over the line.
//
// 22,000 tokens × 2 chars/token ≈ 44 KB. Structured JSON (short keys, UUIDs,
// timestamps) tokenizes at ~2–3 chars/token, well below the 4 chars/token of
// English prose. The conservative estimate ensures the reducer fires before
// MCP clients reject the result. Operators running clients with a raised
// MAX_MCP_OUTPUT_TOKENS can override via NRAM_MCP_MAX_RESULT_TOKENS.
const (
	defaultMaxResultTokens = 22000
	charsPerTokenEstimate  = 2
	maxReducerIterations   = 32
	truncationSuffix       = "... [TRUNCATED: response exceeded MCP token budget; use pagination, narrower query, or REST API for full data]"
)

// maxResultBytes returns the byte budget for a tool result, honoring the
// NRAM_MCP_MAX_RESULT_TOKENS override.
func maxResultBytes() int {
	tokens := defaultMaxResultTokens
	if v := os.Getenv("NRAM_MCP_MAX_RESULT_TOKENS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			tokens = n
		}
	}
	return tokens * charsPerTokenEstimate
}

// truncationInfo is the canonical envelope describing what a reducer dropped.
// It is attached to reduced payloads under the `_truncated` key so agents can
// detect partial results uniformly across every tool.
type truncationInfo struct {
	Reason        string   `json:"reason"`
	OriginalCount int      `json:"original_count,omitempty"`
	ReturnedCount int      `json:"returned_count,omitempty"`
	Dropped       []string `json:"dropped,omitempty"`
	Hint          string   `json:"hint,omitempty"`
}

// reducerFunc returns the next-smaller version of the payload, plus a flag
// indicating whether further reduction is still possible. Reducers are
// stateful closures that capture the original payload and shrink it
// incrementally on each call. Returning (nil, false) signals that no more
// reduction is possible.
type reducerFunc func() (smaller any, canShrinkMore bool)

// wrapToolResultText enforces the size budget on a pre-formatted text result
// (for example NDJSON exports). The text is emitted verbatim if it fits;
// otherwise it is hard-truncated and the sentinel suffix is appended.
func wrapToolResultText(text string) (*mcp.CallToolResult, error) {
	budget := maxResultBytes()
	if len(text) <= budget {
		return mcp.NewToolResultText(text), nil
	}
	keep := budget - len(truncationSuffix)
	if keep < 0 {
		keep = 0
	}
	if keep > len(text) {
		keep = len(text)
	}
	return mcp.NewToolResultText(text[:keep] + truncationSuffix), nil
}

// wrapToolResult marshals a payload, enforces the size budget, and returns
// an MCP CallToolResult. If the marshaled payload exceeds the budget and a
// reducer is supplied, it shrinks the payload iteratively until it fits.
// If reduction is exhausted or no reducer is given, the JSON is hard-cut
// and a sentinel suffix is appended so the agent still sees a clear
// truncation marker rather than receiving an upstream-rejection error.
func wrapToolResult(payload any, reducer reducerFunc) (*mcp.CallToolResult, error) {
	budget := maxResultBytes()

	out, err := json.Marshal(payload)
	if err != nil {
		return mcp.NewToolResultError("failed to marshal response: " + err.Error()), nil
	}
	if len(out) <= budget {
		return mcp.NewToolResultText(string(out)), nil
	}

	if reducer != nil {
		for i := 0; i < maxReducerIterations; i++ {
			smaller, more := reducer()
			if smaller == nil {
				break
			}
			out, err = json.Marshal(smaller)
			if err != nil {
				return mcp.NewToolResultError("failed to marshal reduced response: " + err.Error()), nil
			}
			if len(out) <= budget {
				return mcp.NewToolResultText(string(out)), nil
			}
			if !more {
				break
			}
		}
	}

	// Last-resort: hard-truncate the JSON byte slice and append a sentinel.
	// The result is no longer valid JSON, but agents will still see the bulk
	// of the payload plus a clear truncation marker — strictly better than
	// the upstream client rejecting the entire response.
	keep := budget - len(truncationSuffix)
	if keep < 0 {
		keep = 0
	}
	if keep > len(out) {
		keep = len(out)
	}
	return mcp.NewToolResultText(string(out[:keep]) + truncationSuffix), nil
}

// newRecallReducer builds a stateful reducer for memory_recall responses.
// Stages, in order:
//  1. Drop the entire graph (entities + relationships).
//  2. Truncate every memory.content to 800 chars.
//  3. Truncate every memory.content to 200 chars.
//  4+. Halve the memories slice (lowest-scored first; recall already returns
//      results sorted by score descending, so the tail is the weakest hits).
//      Halving — rather than dropping one at a time — keeps the reducer
//      bounded by O(log N) iterations regardless of how large the original
//      result set was.
func newRecallReducer(orig *service.RecallResponse) reducerFunc {
	memories := append([]service.RecallResult(nil), orig.Memories...)
	originalMemories := len(memories)
	dropGraph := false
	stage := 0
	return func() (any, bool) {
		stage++
		switch stage {
		case 1:
			dropGraph = true
		case 2:
			for i := range memories {
				if len(memories[i].Content) > 800 {
					memories[i].Content = memories[i].Content[:800] + "..."
				}
			}
		case 3:
			for i := range memories {
				if len(memories[i].Content) > 200 {
					memories[i].Content = memories[i].Content[:200] + "..."
				}
			}
		default:
			if len(memories) == 0 {
				return nil, false
			}
			next := len(memories) / 2
			memories = memories[:next]
		}
		more := stage < 4 || len(memories) > 0
		return buildRecallPayload(orig, memories, dropGraph, originalMemories), more
	}
}

func buildRecallPayload(orig *service.RecallResponse, memories []service.RecallResult, dropGraph bool, originalMemories int) map[string]any {
	info := truncationInfo{
		Reason:        "response_too_large",
		OriginalCount: originalMemories,
		ReturnedCount: len(memories),
		Hint:          "narrow your query, lower the limit, or use the REST API at POST /v1/projects/{id}/memories/recall for full results",
	}
	graph := orig.Graph
	if dropGraph {
		info.Dropped = []string{"graph.entities", "graph.relationships"}
		graph = service.RecallGraph{
			Entities:      []service.RecallEntity{},
			Relationships: []service.RecallRelationship{},
		}
	}
	payload := map[string]any{
		"memories":       memories,
		"graph":          graph,
		"total_searched": orig.TotalSearched,
		"latency_ms":     orig.LatencyMs,
		"_truncated":     info,
	}
	// coverage_gaps is bounded by the number of distinct prefix-groups in the
	// candidate pool (typically tens of entries) and is load-bearing metadata
	// for callers using diversify_by_tag_prefix — pass it through verbatim,
	// never shed under token pressure.
	if len(orig.CoverageGaps) > 0 {
		payload["coverage_gaps"] = orig.CoverageGaps
	}
	return payload
}

// newListReducer builds a stateful reducer for memory_list responses.
// Each step halves the returned items so the agent can resume from the
// truncated offset on its next call.
func newListReducer(orig listMemoryResponse) reducerFunc {
	items := append([]listMemoryItem(nil), orig.Data...)
	originalItems := len(items)
	return func() (any, bool) {
		if len(items) <= 1 {
			return nil, false
		}
		items = items[:len(items)/2]
		nextOffset := orig.Pagination.Offset + len(items)
		return map[string]any{
			"data": items,
			"pagination": map[string]any{
				"total":  orig.Pagination.Total,
				"limit":  len(items),
				"offset": orig.Pagination.Offset,
			},
			"_truncated": truncationInfo{
				Reason:        "response_too_large",
				OriginalCount: originalItems,
				ReturnedCount: len(items),
				Hint:          fmt.Sprintf("call memory_list again with offset=%d to fetch the rest", nextOffset),
			},
		}, len(items) > 1
	}
}

// newGraphReducer builds a stateful reducer for memory_graph responses.
// Each step halves the relationships first (the verbose tail), then
// halves the entities.
func newGraphReducer(orig graphResponse) reducerFunc {
	entities := append([]graphEntity(nil), orig.Entities...)
	rels := append([]graphRelationship(nil), orig.Relationships...)
	origE, origR := len(entities), len(rels)
	return func() (any, bool) {
		switch {
		case len(rels) > 0:
			rels = rels[:len(rels)/2]
		case len(entities) > 1:
			entities = entities[:len(entities)/2]
		default:
			return nil, false
		}
		more := len(rels) > 0 || len(entities) > 1
		return map[string]any{
			"entities":        entities,
			"relationships":   rels,
			"query":           orig.Query,
			"depth":           orig.Depth,
			"include_history": orig.IncludeHistory,
			"_truncated": truncationInfo{
				Reason:        "response_too_large",
				OriginalCount: origE + origR,
				ReturnedCount: len(entities) + len(rels),
				Hint:          "narrow the entity query, lower depth, or raise min_weight",
			},
		}, more
	}
}

// newExportReducer builds a stateful reducer for memory_export responses.
// Halves memories, entities, and relationships in lockstep on each call.
func newExportReducer(orig *service.ExportData) reducerFunc {
	memories := append([]service.ExportMemory(nil), orig.Memories...)
	entities := append([]service.ExportEntity(nil), orig.Entities...)
	rels := append([]service.ExportRelationship(nil), orig.Relationships...)
	origM := len(memories)
	return func() (any, bool) {
		if len(memories) <= 1 && len(entities) <= 1 && len(rels) <= 1 {
			return nil, false
		}
		if len(memories) > 1 {
			memories = memories[:len(memories)/2]
		}
		if len(entities) > 1 {
			entities = entities[:len(entities)/2]
		}
		if len(rels) > 1 {
			rels = rels[:len(rels)/2]
		}
		more := len(memories) > 1 || len(entities) > 1 || len(rels) > 1
		return map[string]any{
			"version":       orig.Version,
			"exported_at":   orig.ExportedAt,
			"project":       orig.Project,
			"memories":      memories,
			"entities":      entities,
			"relationships": rels,
			"stats":         orig.Stats,
			"_truncated": truncationInfo{
				Reason:        "response_too_large",
				OriginalCount: origM,
				ReturnedCount: len(memories),
				Hint:          "export is too large for an MCP response; use the REST API at GET /v1/projects/{id}/export for the full archive",
			},
		}, more
	}
}
