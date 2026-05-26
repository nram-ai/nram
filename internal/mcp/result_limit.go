package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/service"
)

// MCP clients enforce a hard ceiling on tool result size. Claude Code and
// Cursor both reject results above 25,000 tokens by default. We adopt the
// minimum of those ceilings minus a safety margin so JSON-RPC envelope,
// content-block framing, and tokenizer drift cannot push us over the line.
//
// 22,000 tokens × 2 chars/token ≈ 44 KB. Structured JSON (short keys, UUIDs,
// timestamps) tokenizes at ~2–3 chars/token, well below the 4 chars/token of
// English prose. The conservative estimate ensures the reducer fires before
// MCP clients reject the result. Operators tune via /admin/settings:
// mcp.max_result_tokens (service.SettingMCPMaxResultTokens). The admin schema
// enforces Min=100; mcpBudgetBytes additionally floors at
// len(truncationSuffix) so the sentinel path is well-formed even when a
// caller persists a value via service.SettingsService.Set (which skips
// schema validation).
const (
	charsPerTokenEstimate = 2
	maxReducerIterations  = 32
	truncationSuffix      = "... [TRUNCATED: response exceeded MCP token budget; narrow the query, lower the limit, or paginate via list]"
)

// mcpBudgetBytes resolves the per-tool MCP result budget through the
// SettingsService cascade and returns the byte budget (tokens *
// charsPerTokenEstimate). ResolveIntWithDefault panics if the setting is not
// registered in settingDefaults — that is the codebase's eager-failure
// convention for missing-key programmer errors. A nil SettingsService falls
// through to the registered default so tests that construct a stub MCP
// server without wiring SettingsService still get a sensible budget.
//
// The admin schema enforces Min=100 (200 bytes at charsPerTokenEstimate=2)
// at the HTTP write path, but service.SettingsService.Set bypasses schema
// validation — direct callers (tests, migrations, future internal setters)
// can persist a smaller value. We floor at len(truncationSuffix) here so
// hardTruncate's contract ("returns at most budget bytes") holds for every
// caller, regardless of how the persisted value got there.
func mcpBudgetBytes(ctx context.Context, s *service.SettingsService) int {
	var tokens int
	if s == nil {
		tokens = service.GetDefaultInt(service.SettingMCPMaxResultTokens)
	} else {
		tokens = s.ResolveIntWithDefault(ctx, service.SettingMCPMaxResultTokens, "global")
	}
	bytes := tokens * charsPerTokenEstimate
	if bytes < len(truncationSuffix) {
		bytes = len(truncationSuffix)
	}
	return bytes
}

// truncationInfo is the canonical envelope describing what a reducer dropped.
// It is attached to reduced payloads under the `_truncated` key so agents can
// detect partial results uniformly across every tool.
//
// DroppedIDs surfaces the actual UUIDs the reducer trimmed from typed-id
// responses (e.g. batch_get's Found tail). Callers parse it to retry with a
// known subset rather than guessing which ids "don't exist" versus "were
// budget-trimmed". Dropped (field paths like "graph.entities" or
// "coverage_gaps_kept:N/M") describes shape-level trims; DroppedIDs describes
// item-identity-level trims. Both omitempty.
type truncationInfo struct {
	Reason        string   `json:"reason"`
	OriginalCount int      `json:"original_count,omitempty"`
	ReturnedCount int      `json:"returned_count,omitempty"`
	Dropped       []string `json:"dropped,omitempty"`
	DroppedIDs    []string `json:"dropped_ids,omitempty"`
	Hint          string   `json:"hint,omitempty"`
}

// reducerFunc returns the next-smaller version of the payload, plus a flag
// indicating whether further reduction is still possible. Reducers are
// stateful closures that capture the original payload and shrink it
// incrementally on each call. Returning (nil, false) signals that no more
// reduction is possible.
//
// Reducers MUST return the same typed response struct the tool declares in
// its outputSchema (with the `Truncated` field populated). Returning an
// untyped map would break clients that key off structuredContent.
type reducerFunc func() (smaller any, canShrinkMore bool)

// Tier labels for the truncation counter. Natural Tier 1 (no truncation,
// no reducer iteration) is the happy path and does not increment the counter.
// tier1Reduced is the in-loop Tier-1 short-circuit reached AFTER the reducer
// has run at least one iteration: structurally Tier 1, but the payload has
// been shrunk to fit. Distinct label so operators can tell "this fit
// naturally" from "this fit only because the reducer threw data away".
const (
	tierTier1Reduced = "tier1_reduced"
	tierTextOnly     = "text_only"
	tierHardTruncate = "hard_truncate"
)

// wrapToolResultText enforces the size budget on a pre-formatted text result
// (for example NDJSON exports). The text is emitted verbatim if it fits;
// otherwise it is hard-truncated and the sentinel suffix is appended. No
// structuredContent is attached — callers using this helper opted out of the
// structured surface. Records to the truncation counter if the body is cut.
// budget is the byte budget resolved by the caller via mcpBudgetBytes.
func wrapToolResultText(rec MetricsRecorder, toolName string, budget int, text string) (*mcp.CallToolResult, error) {
	if len(text) <= budget {
		return mcp.NewToolResultText(text), nil
	}
	recordTier(rec, toolName, tierHardTruncate)
	return mcp.NewToolResultText(hardTruncate([]byte(text), budget)), nil
}

// wrapToolResultJSONText marshals a payload, runs the reducer to keep the
// result under maxResultBytes(), and emits TEXT ONLY (no structuredContent).
// Use this for tools whose outputSchema is not declared (e.g. the export tool
// has two output shapes) — without an advertised schema, shipping
// structuredContent is half-honored and wastes wire bytes. Full budget is
// available because the wire only carries the payload once. Records to the
// truncation counter when the reducer fires (any text-only emission below
// the no-truncation happy path).
func wrapToolResultJSONText(rec MetricsRecorder, toolName string, budget int, payload any, reducer reducerFunc) (*mcp.CallToolResult, error) {
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
				// Reducer-shrunk text fits: emit as text-only. Counter
				// labeled text_only signals "this tool's reducer fired"
				// for observability even though the contract here has no
				// structured tier to fall back from.
				recordTier(rec, toolName, tierTextOnly)
				return mcp.NewToolResultText(string(out)), nil
			}
			if !more {
				break
			}
		}
	}

	recordTier(rec, toolName, tierHardTruncate)
	return mcp.NewToolResultText(hardTruncate(out, budget)), nil
}

// wrapToolResult implements the three-tier contract for outputSchema-bearing
// tools. Given a typed payload and an optional reducer, it returns a
// CallToolResult occupying exactly ONE of three tiers:
//
//	Tier 1 (best, schema-conforming): the marshaled payload fits the halved
//	    structured budget. Ships StructuredContent (detached as json.RawMessage)
//	    + text fallback. The wire carries the payload twice (text-escaped +
//	    structured-raw); halving keeps the total under the upstream-client
//	    ceiling. No counter increment.
//
//	Tier 2 (text-only graceful, complete data): the payload fits the full
//	    text budget but not the halved structured budget. Ships text-only,
//	    NO StructuredContent, NO _truncated, NO sentinel. The data is
//	    complete and parseable; the only thing missing is the second
//	    (structured) wire copy. counter: text_only.
//
//	Tier 3 (last-resort, partial data): even the reduced payload exceeds the
//	    text budget. Ships text-only with hardTruncate (byte-cut + sentinel
//	    suffix). The body is no longer valid JSON; the agent still sees the
//	    bulk of the payload. IsError stays false so clients do not auto-retry
//	    a deterministically-oversized response. counter: hard_truncate.
//
// Inside the reducer loop we track the SMALLEST tier-2 candidate seen so
// far. If the loop ends without ever fitting tier 1, we ship the tier-2
// candidate; only when no marshaled output ever fit the text budget do we
// fall to tier 3. This guarantees: tier 3 only fires when even the smallest
// reduced output exceeds the full text budget — a rare, true-overflow case.
//
// Mutation: tier-1 structuredContent is detached from the caller's pointer
// by copying the marshaled bytes into a fresh json.RawMessage. mcp-go would
// otherwise re-marshal the caller's live payload at JSON-RPC framing time,
// and any post-call mutation would silently diverge the structured response
// from the text response.
func wrapToolResult(rec MetricsRecorder, toolName string, budget int, payload any, reducer reducerFunc) (*mcp.CallToolResult, error) {
	structuredBudget := budget / 2
	if structuredBudget < 1 {
		structuredBudget = 1
	}

	out, err := json.Marshal(payload)
	if err != nil {
		return mcp.NewToolResultError("failed to marshal response: " + err.Error()), nil
	}

	// Natural Tier 1: structured fits before any reducer call. Counter is
	// not incremented — this is the no-truncation baseline.
	if len(out) <= structuredBudget {
		return tier1Result(out), nil
	}

	// Track the FIRST tier-2 candidate (most data preserved). The reducer
	// emits monotonically-smaller payloads; the first one to fit the text
	// budget retains the most data — subsequent shrinks lose data
	// unnecessarily for the tier-2 outcome.
	var tier2Out []byte
	if len(out) <= budget {
		tier2Out = out
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
			// Tier 1 reached AFTER reduction. Record under the
			// tier1_reduced label so observers can distinguish "fit
			// naturally" from "fit only because the reducer discarded
			// data".
			if len(out) <= structuredBudget {
				recordTier(rec, toolName, tierTier1Reduced)
				return tier1Result(out), nil
			}
			// Capture tier-2 candidate on first fit.
			if tier2Out == nil && len(out) <= budget {
				tier2Out = out
			}
			if !more {
				break
			}
		}
	}

	// Tier 2: ship text-only with full data, no truncation marker.
	if tier2Out != nil {
		recordTier(rec, toolName, tierTextOnly)
		return mcp.NewToolResultText(string(tier2Out)), nil
	}

	// Tier 3: even reduced data exceeds the text budget. Hard-cut + sentinel.
	recordTier(rec, toolName, tierHardTruncate)
	return mcp.NewToolResultText(hardTruncate(out, budget)), nil
}

// tier1Result wraps already-marshaled JSON bytes as a CallToolResult with
// both text and structuredContent populated. structuredContent is a detached
// json.RawMessage copy of the marshaled bytes, immune to post-call mutation
// by the caller. The text fallback is the same bytes as a string.
func tier1Result(out []byte) *mcp.CallToolResult {
	detached := json.RawMessage(append([]byte(nil), out...))
	return mcp.NewToolResultStructured(detached, string(out))
}

// recordTier increments the truncation counter. The recorder is invariantly
// non-nil at runtime because mcp.NewServer panics on a nil Metrics; tests
// must pass a non-nil stub (see stubMetrics in tool_null_safety_test.go).
func recordTier(rec MetricsRecorder, tool, tier string) {
	rec.RecordMCPToolResultTier(tool, tier)
}

// hardTruncate returns at most budget bytes formed from a prefix of out
// followed by the truncation sentinel.
//
// Callers via mcpBudgetBytes are guaranteed budget >= len(truncationSuffix)
// by the floor in that function — the admin schema's Min=100 alone is
// insufficient because SettingsService.Set bypasses schema validation. The
// floor in mcpBudgetBytes is the actual invariant that keeps the sentinel
// path well-formed; this function trusts that. Direct callers passing
// budget < len(truncationSuffix) get a zero-length prefix + full sentinel
// (which exceeds budget) — that path is owned by the caller.
func hardTruncate(out []byte, budget int) string {
	if budget <= 0 {
		return ""
	}
	keep := budget - len(truncationSuffix)
	if keep < 0 {
		keep = 0
	}
	if keep > len(out) {
		keep = len(out)
	}
	return string(out[:keep]) + truncationSuffix
}

// recallReductions tracks which actual reductions the recall reducer has
// applied across its iterations. Each flag is set ONLY when the
// corresponding reduction modified the response — e.g. graphDropped is set
// only when the original graph was non-empty, contentTrimmed is set only
// when at least one memory's content was actually shortened. This means the
// composed hint and Dropped list never claim a reduction that did not
// happen.
type recallReductions struct {
	graphDropped        bool
	contentTrimmedTo800 bool
	contentTrimmedTo200 bool
	memoriesHalved      bool
	coverageGapsTrimmed bool
}

// any reports whether any reduction has actually been applied. Used as the
// gate for emitting a Truncated envelope: a no-op stage (e.g. stage 1 on an
// already-empty graph) must not stamp Truncated, since the data on the wire
// is byte-for-byte identical to the input.
func (r recallReductions) any() bool {
	return r.graphDropped || r.contentTrimmedTo800 || r.contentTrimmedTo200 || r.memoriesHalved || r.coverageGapsTrimmed
}

// hint composes a remediation hint string from the flags set so far. Each
// active flag contributes its own clause; clauses join with "; ". When no
// reduction has yet happened (e.g. stage 1 on an already-empty graph), the
// hint is empty rather than a misleading "narrow your query".
func (r recallReductions) hint() string {
	var parts []string
	if r.graphDropped {
		parts = append(parts, "graph dropped; lower graph_depth or omit the graph")
	}
	if r.contentTrimmedTo800 || r.contentTrimmedTo200 {
		parts = append(parts, "memory content truncated; lower the limit, narrow the query, or filter by tags")
	}
	if r.memoriesHalved {
		parts = append(parts, "memory list halved; lower the limit or narrow the query")
	}
	if r.coverageGapsTrimmed {
		parts = append(parts, "coverage_gaps diagnostic truncated; turn off diversify_by_tag_prefix to keep all gaps")
	}
	return strings.Join(parts, "; ")
}

// newRecallReducer builds a stateful reducer for recall responses. Stages:
//
//	1. Drop the entire graph (only if non-empty).
//	2. Truncate every memory.content > 800 chars to 800.
//	3. Truncate every memory.content > 200 chars to 200.
//	4+. Halve the memories slice AND coverage_gaps in lockstep. Halving
//	    bounds the reducer at O(log N) iterations; coverage_gaps trim is
//	    in lockstep because gaps can dominate the budget on diversified
//	    queries (see the comment on mcpRecallResponse.CoverageGaps).
//
// The reducer returns *mcpRecallResponse with Truncated populated by the
// recallReductions flags (composed hint, Dropped list, counts).
func newRecallReducer(orig *mcpRecallResponse) reducerFunc {
	memories := append([]mcpRecallMemory(nil), orig.Memories...)
	coverageGaps := append([]service.CoverageGap(nil), orig.CoverageGaps...)
	originalMemories := len(memories)
	origGraphEmpty := len(orig.Graph.Entities) == 0 && len(orig.Graph.Relationships) == 0
	var flags recallReductions
	stage := 0

	origCoverageGaps := len(coverageGaps)
	return func() (any, bool) {
		stage++
		switch stage {
		case 1:
			if !origGraphEmpty {
				flags.graphDropped = true
			}
		case 2:
			for i := range memories {
				if len(memories[i].Content) > 800 {
					memories[i].Content = memories[i].Content[:800] + "..."
					flags.contentTrimmedTo800 = true
				}
			}
		case 3:
			for i := range memories {
				if len(memories[i].Content) > 200 {
					memories[i].Content = memories[i].Content[:200] + "..."
					flags.contentTrimmedTo200 = true
				}
			}
		default:
			// Floor at 1, not 0: halving len==1 to len==0 would zero out the
			// only real hit while setting flags.memoriesHalved=true, producing
			// a Truncated envelope with ReturnedCount=0 that is strictly worse
			// than letting tier-3 byte-cut the single-memory response (which
			// at least carries the memory id and partial fields). When both
			// memories and coverage_gaps reach 1, the reducer is exhausted
			// and signals nil/false so the wrapper falls to tier-3.
			halved := false
			if len(memories) > 1 {
				memories = memories[:len(memories)/2]
				flags.memoriesHalved = true
				halved = true
			}
			if len(coverageGaps) > 1 {
				coverageGaps = coverageGaps[:len(coverageGaps)/2]
				flags.coverageGapsTrimmed = true
				halved = true
			}
			if !halved {
				return nil, false
			}
		}
		more := stage < 4 || len(memories) > 1 || len(coverageGaps) > 1
		// No-op stage: return the original payload unmodified (no
		// Truncated envelope). Signal more=true so the loop advances
		// to the next stage rather than terminating on a false negative.
		if !flags.any() {
			return orig, more
		}
		return buildReducedRecallResponse(orig, memories, coverageGaps, flags, originalMemories, origCoverageGaps), more
	}
}

func buildReducedRecallResponse(
	orig *mcpRecallResponse,
	memories []mcpRecallMemory,
	coverageGaps []service.CoverageGap,
	flags recallReductions,
	originalMemories int,
	origCoverageGaps int,
) *mcpRecallResponse {
	info := &truncationInfo{
		Reason:        "response_too_large",
		OriginalCount: originalMemories,
		ReturnedCount: len(memories),
		Hint:          flags.hint(),
	}
	if flags.graphDropped {
		info.Dropped = append(info.Dropped, "graph.entities", "graph.relationships")
	}
	if flags.coverageGapsTrimmed {
		// Frame-independent marker: how many of the original gaps remain
		// in the returned response. Avoids the ambiguity of an N: slice
		// expression that read as either post-trim index (returned-frame)
		// or original-frame, depending on the reader.
		info.Dropped = append(info.Dropped, fmt.Sprintf("coverage_gaps_kept:%d/%d", len(coverageGaps), origCoverageGaps))
	}
	graph := orig.Graph
	if flags.graphDropped {
		graph = graphResponse{
			Entities:      []graphEntity{},
			Relationships: []graphRelationship{},
		}
	}
	return &mcpRecallResponse{
		Memories:     memories,
		Graph:        graph,
		LatencyMs:    orig.LatencyMs,
		CoverageGaps: coverageGaps,
		Truncated:    info,
	}
}

// newListReducer builds a stateful reducer for list responses. Halves the
// returned items each step. Pagination.Limit is left at the caller's
// requested value (it's the page-size parameter, not the returned count);
// the actually-returned count lives in Truncated.ReturnedCount.
func newListReducer(orig *listMemoryResponse) reducerFunc {
	items := append([]listMemoryItem(nil), orig.Data...)
	originalItems := len(items)
	return func() (any, bool) {
		if len(items) <= 1 {
			return nil, false
		}
		items = items[:len(items)/2]
		nextOffset := orig.Pagination.Offset + len(items)
		return &listMemoryResponse{
			Data:       items,
			Pagination: orig.Pagination, // Limit stays at the request value
			Truncated: &truncationInfo{
				Reason:        "response_too_large",
				OriginalCount: originalItems,
				ReturnedCount: len(items),
				Hint:          fmt.Sprintf("call list again with offset=%d to fetch the rest", nextOffset),
			},
		}, len(items) > 1
	}
}

// newListProjectsReducer halves the Projects slice. Pagination.Limit stays
// at the request value; the returned count lives in Truncated.
func newListProjectsReducer(orig *listProjectsResponse) reducerFunc {
	items := append([]projectItem(nil), orig.Projects...)
	originalItems := len(items)
	return func() (any, bool) {
		if len(items) <= 1 {
			return nil, false
		}
		items = items[:len(items)/2]
		nextOffset := orig.Pagination.Offset + len(items)
		return &listProjectsResponse{
			Projects:   items,
			Pagination: orig.Pagination,
			Truncated: &truncationInfo{
				Reason:        "response_too_large",
				OriginalCount: originalItems,
				ReturnedCount: len(items),
				Hint:          fmt.Sprintf("call list_projects again with offset=%d to fetch the rest", nextOffset),
			},
		}, len(items) > 1
	}
}

// newBatchGetReducer halves the Found slice from the tail; NotFound is
// preserved verbatim (small, diagnostic). DroppedIDs accumulates the
// stringified UUIDs of the trimmed Found tail across iterations so the
// caller can distinguish "this id doesn't exist" (NotFound) from "this id
// was too big to ship in this batch" (DroppedIDs) and retry precisely.
//
// Unlike list/list_projects/recall reducers, this one drains Found all the
// way to empty on its final iteration. The DroppedIDs envelope IS the
// useful payload — a caller that retains an empty Found plus
// DroppedIDs=[...] can re-issue get(id) per id and recover precisely. The
// alternative (refusing to shrink at len==1 when a single oversized memory
// still busts budget) would force tier-3 to byte-cut mid-content,
// producing invalid JSON that the caller cannot recover from.
func newBatchGetReducer(orig *mcpBatchGetResponse) reducerFunc {
	found := append([]mcpMemoryDetail(nil), orig.Found...)
	originalFound := len(found)
	originalTotal := len(found) + len(orig.NotFound)
	var droppedIDs []string
	return func() (any, bool) {
		if len(found) == 0 {
			return nil, false
		}
		keep := len(found) / 2
		// Trimmed tail = found[keep:]. Capture their IDs before the
		// slice header moves so the wire envelope can name them.
		for _, m := range found[keep:] {
			droppedIDs = append(droppedIDs, m.ID.String())
		}
		found = found[:keep]
		// DroppedIDs is monotonically growing across iterations; copy
		// the slice into each emitted envelope so a caller that retains
		// an earlier *mcpBatchGetResponse pointer doesn't observe a
		// later iteration's growth through aliasing.
		dropped := append([]string(nil), droppedIDs...)
		return &mcpBatchGetResponse{
			Found:    found,
			NotFound: orig.NotFound,
			Truncated: &truncationInfo{
				Reason:        "response_too_large",
				OriginalCount: originalTotal,
				ReturnedCount: len(found) + len(orig.NotFound),
				DroppedIDs:    dropped,
				Hint:          fmt.Sprintf("retry with the ids in dropped_ids; halved from %d to %d", originalFound, len(found)),
			},
		}, len(found) > 0
	}
}

// newGraphReducer halves relationships first (the verbose tail), then
// halves entities. When the upstream traversal already short-circuited at
// graph.max_edges (orig.Truncated has Reason "edge_cap"), the reducer
// preserves that root-cause Reason and merges its byte-budget hint into the
// emitted envelope so the client sees the actual remediation (raise
// graph.max_edges) rather than being misdirected to query-shape tuning.
func newGraphReducer(orig *graphResponse) reducerFunc {
	entities := append([]graphEntity(nil), orig.Entities...)
	rels := append([]graphRelationship(nil), orig.Relationships...)
	origE, origR := len(entities), len(rels)
	origTrunc := orig.Truncated
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
		info := &truncationInfo{
			Reason:        "response_too_large",
			OriginalCount: origE + origR,
			ReturnedCount: len(entities) + len(rels),
			Hint:          "narrow the entity query, lower depth, or raise min_weight",
		}
		if origTrunc != nil && origTrunc.Reason != "" {
			info.Reason = origTrunc.Reason + "+response_too_large"
			if origTrunc.Hint != "" {
				info.Hint = origTrunc.Hint + "; response further halved to fit MCP token budget"
			}
		}
		return &graphResponse{
			Entities:      entities,
			Relationships: rels,
			Truncated:     info,
		}, more
	}
}

// newExportReducer halves memories, entities, and relationships in
// lockstep. Truncated counts aggregate across all three categories so a
// caller computing "fraction returned" gets a true completeness signal.
func newExportReducer(orig *mcpExportResponse) reducerFunc {
	memories := append([]service.ExportMemory(nil), orig.Memories...)
	entities := append([]service.ExportEntity(nil), orig.Entities...)
	rels := append([]service.ExportRelationship(nil), orig.Relationships...)
	origTotal := len(memories) + len(entities) + len(rels)
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
		return &mcpExportResponse{
			ExportData: service.ExportData{
				Version:       orig.Version,
				ExportedAt:    orig.ExportedAt,
				Project:       orig.Project,
				Memories:      memories,
				Entities:      entities,
				Relationships: rels,
				Stats:         orig.Stats,
			},
			Truncated: &truncationInfo{
				Reason:        "response_too_large",
				OriginalCount: origTotal,
				ReturnedCount: len(memories) + len(entities) + len(rels),
				Hint:          "export exceeded MCP token budget; result is partial — narrow the project scope or paginate memories via list",
			},
		}, more
	}
}

// classifyTier returns the tier label that wrapToolResult assigned to res.
// Exposed for tests to assert which path was taken. The contract:
//   - tier1: StructuredContent != nil
//   - tier3: text content ends with truncationSuffix
//   - tier2: otherwise (text-only, valid JSON, no marker)
func classifyTier(res *mcp.CallToolResult) string {
	if res == nil {
		return ""
	}
	if res.StructuredContent != nil {
		return "tier1"
	}
	text := callToolResultText(res)
	if strings.HasSuffix(text, truncationSuffix) {
		return "tier3"
	}
	return "tier2"
}

// callToolResultText extracts the first TextContent block from a
// CallToolResult, or "" if absent. Test helper.
func callToolResultText(res *mcp.CallToolResult) string {
	if res == nil {
		return ""
	}
	for _, c := range res.Content {
		if tc, ok := c.(mcp.TextContent); ok {
			return tc.Text
		}
	}
	return ""
}
