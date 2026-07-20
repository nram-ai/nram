package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/instructions"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// instructionVariant pairs a buildInstructions provider combination with the
// text it produces, so tests that must hold for every variant can range over
// them instead of restating the four calls.
type instructionVariant struct {
	name string
	text string
}

func instructionVariants() []instructionVariant {
	return []instructionVariant{
		{"embedding+enrichment+ask", buildInstructions(true, true, true)},
		{"embedding+enrichment", buildInstructions(true, true, false)},
		{"enrichment only", buildInstructions(false, true, false)},
		{"embedding only", buildInstructions(true, false, false)},
		{"no providers", buildInstructions(false, false, false)},
	}
}

// allTiers returns every surface that carries nram's agent guidance: the two
// document tiers served over HTTP plus each handshake variant. Tests that must
// hold for all of them share this list so none can silently cover fewer.
func allTiers(t *testing.T) []instructionVariant {
	t.Helper()

	tiers := make([]instructionVariant, 0, 2+5)
	for _, tc := range []struct{ name, format string }{
		{"full", "claude"},
		{"condensed", "condensed"},
	} {
		body, ok := instructions.Lookup(tc.format)
		if !ok {
			t.Fatalf("%s tier must resolve", tc.name)
		}
		tiers = append(tiers, instructionVariant{tc.name, body})
	}
	return append(tiers, instructionVariants()...)
}

func TestNewServer_NonNil(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
}

// TestNewServerPanicsOnNilMetrics pins the construction-time enforcement.
// Production wiring drift (e.g. cmd/server/main.go forgetting to populate
// Dependencies.Metrics) must fail at startup, not silently in production.
// The MCP wrappers depend on a non-nil recorder; recordTier has no nil-guard
// since the panic invariant makes it unreachable.
func TestNewServerPanicsOnNilMetrics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil Metrics; got none")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value is %T, want string", r)
		}
		if !strings.Contains(msg, "Dependencies.Metrics is required") {
			t.Errorf("panic message %q must name the required field", msg)
		}
	}()
	_ = NewServer(Dependencies{Metrics: nil})
}

func TestHandler_NonNil(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)
	h := srv.Handler()
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestBackend_SQLite(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)
	if got := srv.Backend(); got != storage.BackendSQLite {
		t.Fatalf("expected backend %q, got %q", storage.BackendSQLite, got)
	}
}

func TestBackend_Postgres(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := newTestServer(deps)
	if got := srv.Backend(); got != storage.BackendPostgres {
		t.Fatalf("expected backend %q, got %q", storage.BackendPostgres, got)
	}
}

func TestMCPServer_NonNil(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendSQLite}
	srv := newTestServer(deps)
	if srv.MCPServer() == nil {
		t.Fatal("expected non-nil MCPServer")
	}
}

func TestDeps_ReturnsSameBackend(t *testing.T) {
	deps := Dependencies{Backend: storage.BackendPostgres}
	srv := newTestServer(deps)
	if got := srv.Deps().Backend; got != storage.BackendPostgres {
		t.Fatalf("expected deps backend %q, got %q", storage.BackendPostgres, got)
	}
}

func TestHTTPRequestFromContext_Present(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	ctx := context.WithValue(context.Background(), httpRequestKey, req)
	got := HTTPRequestFromContext(ctx)
	if got != req {
		t.Fatal("expected to retrieve the stored request")
	}
}

func TestHTTPRequestFromContext_Absent(t *testing.T) {
	got := HTTPRequestFromContext(context.Background())
	if got != nil {
		t.Fatal("expected nil when no request in context")
	}
}

// --- Origin validation tests (MCP spec security requirement) ---

func TestOriginValidation_NoOrigin_Allowed(t *testing.T) {
	srv := newTestServer(Dependencies{Backend: storage.BackendSQLite})
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	// No Origin header; should pass through to the MCP handler.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// The MCP SDK may return various codes for an uninitialized POST, but
	// it MUST NOT be 403 (that's the Origin rejection code).
	if rec.Code == http.StatusForbidden {
		t.Fatal("request without Origin should not be rejected with 403")
	}
}

func TestOriginValidation_MatchingOrigin_Allowed(t *testing.T) {
	srv := newTestServer(Dependencies{Backend: storage.BackendSQLite})
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Host = "localhost:8674"
	req.Header.Set("Origin", "http://localhost:8674")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code == http.StatusForbidden {
		t.Fatalf("matching Origin should not be rejected; got 403: %s", rec.Body.String())
	}
}

func TestOriginValidation_MismatchedOrigin_Rejected(t *testing.T) {
	srv := newTestServer(Dependencies{Backend: storage.BackendSQLite})
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Host = "localhost:8674"
	req.Header.Set("Origin", "http://evil.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("mismatched Origin should be rejected with 403; got %d", rec.Code)
	}
}

func TestOriginValidation_HTTPSOrigin_Matches(t *testing.T) {
	srv := newTestServer(Dependencies{Backend: storage.BackendSQLite})
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Host = "nram.example.com"
	req.Header.Set("Origin", "https://nram.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code == http.StatusForbidden {
		t.Fatalf("https Origin matching Host should not be rejected; got 403")
	}
}

func TestIsAllowedOrigin(t *testing.T) {
	tests := []struct {
		origin string
		host   string
		want   bool
	}{
		{"http://localhost:8674", "localhost:8674", true},
		{"https://nram.example.com", "nram.example.com", true},
		{"http://evil.com", "localhost:8674", false},
		{"https://evil.com:8674", "localhost:8674", false},
		{"http://localhost:9999", "localhost:8674", false},
		{"http://localhost:8674", "localhost:8674", true},
	}
	for _, tt := range tests {
		got := isAllowedOrigin(tt.origin, tt.host)
		if got != tt.want {
			t.Errorf("isAllowedOrigin(%q, %q) = %v, want %v", tt.origin, tt.host, got, tt.want)
		}
	}
}

func TestBuildInstructions_UnderSizeLimit(t *testing.T) {
	// Claude Code truncates server instructions at 2048 characters.
	// The longest variant (both providers + ask enabled) must fit.
	const maxChars = 2048

	full := buildInstructions(true, true, true)
	if len(full) > maxChars {
		t.Errorf("full instructions are %d chars, must be under %d (over by %d)",
			len(full), maxChars, len(full)-maxChars)
	}
}

// TestInitializeInstructions_MatchBuildInstructions closes the gap that let the
// size cap go unenforced: the initialize hook used to append an instance-ID
// sentence to result.Instructions after buildInstructions returned, so the
// payload a client actually received ran 96 chars past what
// TestBuildInstructions_UnderSizeLimit measured. The guard passed while the
// wire was over the limit.
//
// The instance ID now rides in _meta instead, and this test pins the invariant
// that made the old bug possible: whatever ships in Instructions must be
// exactly what buildInstructions produced, so measuring the latter is a real
// measurement of the former.
func TestInitializeInstructions_MatchBuildInstructions(t *testing.T) {
	result := initializeResultForTest(t, Dependencies{
		Backend:    storage.BackendSQLite,
		InstanceID: "11111111-2222-3333-4444-555555555555",
	})

	if result.Instructions == "" {
		t.Fatal("initialize returned empty instructions")
	}
	if strings.Contains(result.Instructions, "Instance:") {
		t.Error("instance identity must not be spliced into Instructions; it belongs in _meta")
	}
	if len(result.Instructions) > 2048 {
		t.Errorf("shipped instructions are %d chars, over the 2048 cap by %d",
			len(result.Instructions), len(result.Instructions)-2048)
	}

	// Instructions must be byte-identical to one of the buildInstructions
	// variants: nothing may decorate it on the way out.
	matched := false
	for _, v := range instructionVariants() {
		if result.Instructions == v.text {
			matched = true
			break
		}
	}
	if !matched {
		t.Errorf("shipped instructions do not match any buildInstructions variant; "+
			"something is decorating the payload and the size cap no longer measures what ships:\n%s",
			result.Instructions)
	}
}

// TestInitializeMeta_CarriesInstanceIdentity pins the replacement surface: a
// client (or a future central router) can still identify the instance it is
// connected to, without spending the model's instruction budget on it.
func TestInitializeMeta_CarriesInstanceIdentity(t *testing.T) {
	const id = "11111111-2222-3333-4444-555555555555"
	result := initializeResultForTest(t, Dependencies{
		Backend:    storage.BackendSQLite,
		InstanceID: id,
	})

	if result.Meta == nil || result.Meta.AdditionalFields == nil {
		t.Fatal("initialize result must carry _meta when an instance ID is configured")
	}
	if got := result.Meta.AdditionalFields[MetaInstanceID]; got != id {
		t.Errorf("_meta[%s] = %v, want %q", MetaInstanceID, got, id)
	}
	if got := result.Meta.AdditionalFields[MetaJWKSURI]; got != JWKSPath {
		t.Errorf("_meta[%s] = %v, want %q", MetaJWKSURI, got, JWKSPath)
	}

	// The key names are wire contract, so pin their spelling rather than only
	// reading them back through the same constants. MCP (2025-06-18,
	// basic/index) requires a prefix to be dot-separated labels followed by a
	// slash; a bare dotted name like "ai.nram.instance_id" is unprefixed and
	// claims no namespace at all.
	for _, key := range []string{MetaInstanceID, MetaJWKSURI} {
		prefix, name, ok := strings.Cut(key, "/")
		if !ok || prefix == "" || name == "" {
			t.Errorf("_meta key %q must be a prefix and a name separated by a slash", key)
		}
	}

	// An unconfigured instance advertises nothing rather than an empty string.
	bare := initializeResultForTest(t, Dependencies{Backend: storage.BackendSQLite})
	if bare.Meta != nil && bare.Meta.AdditionalFields[MetaInstanceID] != nil {
		t.Error("no instance ID configured; _meta must not advertise one")
	}
}

func TestToolDescriptions_UnderSizeLimit(t *testing.T) {
	// Claude Desktop truncates individual tool descriptions at 2048 bytes.
	// Test both backends since Postgres registers additional tools.
	const maxDescBytes = 2048

	for _, backend := range []string{storage.BackendSQLite, storage.BackendPostgres} {
		deps := Dependencies{Backend: backend}
		srv := newTestServer(deps)
		tools := srv.MCPServer().ListTools()

		for name, st := range tools {
			desc := st.Tool.Description
			if len(desc) > maxDescBytes {
				t.Errorf("[%s] tool %q description is %d bytes, must be under %d",
					backend, name, len(desc), maxDescBytes)
			}
		}
	}
}

// TestEveryToolHasOutputSchema pins the wire contract that every MCP tool
// nram exposes advertises an outputSchema with type=object. Clients (Claude,
// Cursor, MCP Inspector) key off outputSchema to reason about tool results
// without parsing opaque JSON text. Per the MCP spec
// (modelcontextprotocol.io/specification/2025-06-18/server/tools), the
// top-level type MUST be "object".
//
// `export` is exempt because it has two output shapes (json structured,
// ndjson text) that a single schema cannot honestly describe; the tool
// advertises no outputSchema and consumers branch on the requested format.
//
// The expected tool-name set is pinned explicitly so a future registration
// regression (e.g. a Register* call removed, or a tool gated on a non-nil
// dep that this test does not provide) fails loudly instead of silently
// shrinking coverage.
func TestEveryToolHasOutputSchema(t *testing.T) {
	expected := []string{
		"list", "store", "store_batch", "recall", "ask", "forget",
		"update", "get", "graph", "list_projects",
		"delete_project", "update_project",
		"procedural_fetch", "procedural_store", "procedural_update", "procedural_forget",
		"about_me",
	}
	exempt := map[string]bool{} // export was the sole exemption; tool withdrawn 2026-05-27

	for _, backend := range []string{storage.BackendSQLite, storage.BackendPostgres} {
		// delete_project is gated on a non-nil ProjectDelete service
		// (RegisterProjectDeleteTool early-returns otherwise). Inject a
		// non-nil sentinel so the test exercises the fully-configured
		// surface; the handler is never invoked in this test.
		deps := Dependencies{
			Backend:       backend,
			ProjectDelete: &service.ProjectDeleteService{},
			// ask registers only when Ask is wired; inject a sentinel so the
			// output-schema contract covers it. ListTools() returns the raw
			// registered set (the ask.enabled visibility filter runs only on
			// tools/list requests), so ask appears here regardless of the flag.
			Ask: &service.AskService{},
		}
		srv := newTestServer(deps)
		tools := srv.MCPServer().ListTools()

		// Pin the exact registered set so a silent drop or rename surfaces.
		for _, name := range expected {
			if _, ok := tools[name]; !ok {
				t.Errorf("[%s] expected tool %q to be registered", backend, name)
			}
		}
		if len(tools) != len(expected) {
			got := make([]string, 0, len(tools))
			for n := range tools {
				got = append(got, n)
			}
			t.Errorf("[%s] tool-name set drifted: want %v, got %v", backend, expected, got)
		}

		for name, st := range tools {
			if exempt[name] {
				continue
			}
			schema := st.Tool.OutputSchema
			rawSchemaPresent := len(st.Tool.RawOutputSchema) > 0
			if !rawSchemaPresent && schema.Type == "" && schema.Properties == nil {
				t.Errorf("[%s] tool %q has no outputSchema", backend, name)
				continue
			}
			if rawSchemaPresent {
				var parsed struct {
					Type string `json:"type"`
				}
				if err := json.Unmarshal(st.Tool.RawOutputSchema, &parsed); err != nil {
					t.Errorf("[%s] tool %q outputSchema is not valid JSON: %v", backend, name, err)
					continue
				}
				if parsed.Type != "object" {
					t.Errorf("[%s] tool %q outputSchema.type = %q, want \"object\"",
						backend, name, parsed.Type)
				}
				continue
			}
			if schema.Type != "object" {
				t.Errorf("[%s] tool %q outputSchema.type = %q, want \"object\"",
					backend, name, schema.Type)
			}
		}
	}
}

func TestBuildInstructions_AllVariants(t *testing.T) {
	tests := []struct {
		name           string
		hasEmbedding   bool
		hasEnrichment  bool
		mustContain    []string
		mustNotContain []string
	}{
		{
			name:          "both providers",
			hasEmbedding:  true,
			hasEnrichment: true,
			mustContain: []string{
				"STORAGE",
				"RETRIEVAL",
				"1. recall",
				"2. graph",
				"list",
				"Enrichment is server-managed",
			},
			mustNotContain: []string{
				"No embedding provider",
				"no embedding provider",
				"specific tags",
				"enrich: true",
			},
		},
		{
			name:          "embedding only",
			hasEmbedding:  true,
			hasEnrichment: false,
			mustContain: []string{
				"STORAGE",
				"RETRIEVAL",
				"recall",
				"list",
				"semantic search",
			},
			mustNotContain: []string{
				"graph",
				"enrich: true",
			},
		},
		{
			name:          "enrichment only",
			hasEmbedding:  false,
			hasEnrichment: true,
			mustContain: []string{
				"STORAGE",
				"RETRIEVAL",
				"1. recall",
				"2. graph",
				"Enrichment is server-managed",
				"specific tags",
			},
			mustNotContain: []string{
				"semantic search",
			},
		},
		{
			name:          "neither provider",
			hasEmbedding:  false,
			hasEnrichment: false,
			mustContain: []string{
				"STORAGE",
				"RETRIEVAL",
				"recall",
				"list",
				"specific tags",
			},
			mustNotContain: []string{
				"graph",
				"enrich: true",
				"semantic search",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildInstructions(tt.hasEmbedding, tt.hasEnrichment, false)
			for _, s := range tt.mustContain {
				if !strings.Contains(result, s) {
					t.Errorf("expected instructions to contain %q", s)
				}
			}
			for _, s := range tt.mustNotContain {
				if strings.Contains(result, s) {
					t.Errorf("expected instructions NOT to contain %q", s)
				}
			}
			// The procedural session-start guidance is provider-independent and
			// must appear in every variant.
			if !strings.Contains(result, "procedural_fetch") {
				t.Errorf("expected instructions to contain procedural_fetch session-start guidance")
			}
			if !strings.Contains(result, "SESSION START") {
				t.Errorf("expected instructions to contain SESSION START guidance")
			}
			if !strings.Contains(result, "your first action this session MUST be procedural_fetch") {
				t.Errorf("expected instructions to contain the blocking session-start directive")
			}
			if !strings.Contains(result, "Reasoning or justifying a skip is itself a violation") {
				t.Errorf("expected instructions to contain the anti-rationalization clause")
			}
		})
	}
}

// TestBuildInstructions_AskConfidenceIsGrounding pins the confidence caveat in
// the handshake instructions, which sit in the calling agent's system prompt for
// the whole session. This variant runs 20 chars under the 2048 cap that
// TestBuildInstructions_UnderSizeLimit enforces, so a later edit needing room
// must not buy it back by deleting this clause.
func TestBuildInstructions_AskConfidenceIsGrounding(t *testing.T) {
	withAsk := buildInstructions(true, true, true)
	for _, want := range []string{"grounding", "correctness"} {
		if !strings.Contains(withAsk, want) {
			t.Errorf("ask-enabled instructions must distinguish grounding from correctness; missing %q", want)
		}
	}

	// The caveat rides on the ask line, so it must not leak in when ask is off.
	if noAsk := buildInstructions(true, true, false); strings.Contains(noAsk, "grounding") {
		t.Error("confidence caveat must not appear when the ask tool is disabled")
	}
}

// TestBuildInstructions_RetrievalPrecedence pins recall-first retrieval. The
// recall tool sets IncludeGraph unconditionally (tool_recall.go), so a recall
// already returns the entities and relationships a preceding graph call would
// have fetched; instructing agents to query graph first bought a redundant
// round trip. Graph is the follow-up for a recall that comes back noisy or
// short, which is what agent-instructions.md, condensed.md, and the recall and
// graph tool descriptions all say. This test exists to keep those five surfaces
// from diverging again.
func TestBuildInstructions_RetrievalPrecedence(t *testing.T) {
	// With enrichment: recall must come before graph, graph before list.
	full := buildInstructions(true, true, false)
	recallIdx := strings.Index(full, "1. recall")
	graphIdx := strings.Index(full, "2. graph")
	listIdx := strings.Index(full, "3. list")

	if recallIdx < 0 || graphIdx < 0 || listIdx < 0 {
		t.Fatalf("expected all three retrieval tools to be numbered in order; got:\n%s", full)
	}
	if recallIdx >= graphIdx {
		t.Error("recall must appear before graph in retrieval order")
	}
	if graphIdx >= listIdx {
		t.Error("graph must appear before list in retrieval order")
	}

	// The graph step being framed as a fallback rather than a mandatory first
	// hop is asserted across every tier by TestTiersAgree_RecallLeadsEverywhere.

	// Without enrichment the graph tool returns nothing, so it must not be
	// offered at all; recall still leads.
	noGraph := buildInstructions(true, false, false)
	if !strings.Contains(noGraph, "1. recall") {
		t.Error("recall must lead retrieval when enrichment is unconfigured")
	}
	if strings.Contains(noGraph, "graph") {
		t.Error("graph must not be offered when enrichment is unconfigured")
	}
}

// TestBuildInstructions_ComposesEmbeddedBody pins that buildInstructions splices
// the provider-conditional blocks into data/mcp-handshake.md rather than
// carrying its own copy. A truncated file would otherwise silently ship a
// handshake missing its trailing half, and an unsubstituted marker would ship
// literal template syntax to the model.
//
// Only the two properties no other test covers live here: the tail of the file
// surviving the splice, and the markers being consumed. The head and middle
// segments are already asserted per-variant by TestBuildInstructions_AllVariants
// and TestTiersAgree.
func TestBuildInstructions_ComposesEmbeddedBody(t *testing.T) {
	for _, variant := range instructionVariants() {
		if !strings.Contains(variant.text, "KEY RULES:") {
			t.Errorf("[%s] composed instructions dropped the segment after the last marker", variant.name)
		}
		if strings.Contains(variant.text, "{{") {
			t.Errorf("[%s] composed instructions still contain an unsubstituted marker", variant.name)
		}
	}
}

func TestBuildInstructions_BehavioralTriggers(t *testing.T) {
	full := buildInstructions(true, true, false)
	triggers := []string{
		"preference",
		"decision",
		"each task start",
		"before storing",
		"avoid duplicates",
		"store immediately",
	}
	for _, trigger := range triggers {
		if !strings.Contains(full, trigger) {
			t.Errorf("expected behavioral trigger %q in instructions", trigger)
		}
	}
}

// TestBuildInstructions_FocusedRecallGuidance: whenever semantic search is
// active, the recall step must steer agents away from keyword-stuffed grab-bag
// queries toward focused, single-intent recalls (the multi-facet facet-drop
// guidance), regardless of whether enrichment is also configured.
func TestBuildInstructions_FocusedRecallGuidance(t *testing.T) {
	for _, hasEnrichment := range []bool{true, false} {
		full := buildInstructions(true, hasEnrichment, false)
		for _, want := range []string{"focused", "single-intent", "keyword grab-bag"} {
			if !strings.Contains(full, want) {
				t.Errorf("hasEnrichment=%v: expected focused-recall guidance %q in instructions", hasEnrichment, want)
			}
		}
	}
}

// TestTiersAgree is the drift guard for nram's three agent-instruction tiers:
// the full markdown body served at GET /instructions and pasted into CLAUDE.md,
// the length-capped condensed body, and the MCP handshake in every provider
// variant. The tiers are length-ordered and two are capped, so they carry
// different amounts of detail by design; what must never vary is the core
// guidance below.
//
// This test exists because the tiers had drifted into contradiction: the
// handshake told agents to query graph before recall while both markdown tiers
// told them the reverse, and the tag vocabulary had lost a value in the
// handshake. Nothing tied the three together, because the handshake was built
// in this package while the other two were embedded files in another. They now
// share a directory; this pins the content.
func TestTiersAgree(t *testing.T) {
	tiers := allTiers(t)

	// Each fact is a set of substrings; a tier satisfies the fact if it contains
	// any one of them. The alternatives exist because the tiers phrase the same
	// rule at different lengths, not because the rule is optional.
	facts := []struct {
		name  string
		anyOf []string
	}{
		{"blocking procedural_fetch session start", []string{"first action this session MUST be procedural_fetch", "your first action MUST be to call procedural_fetch"}},
		{"nram is the only memory system", []string{"ONLY memory system"}},
		{"never write local memory files", []string{"MEMORY.md"}},
		{"recall before storing, to dedupe", []string{"avoid duplicates", "check for duplicates", "check duplicates"}},
		{"list_projects before creating a project", []string{"list_projects first"}},
		{"reserved about_me tier", []string{"about_me"}},
		{"shared tag vocabulary", []string{"decision, preference, architecture, config, bug, workaround, convention"}},
	}

	for _, tier := range tiers {
		for _, fact := range facts {
			if !containsAny(tier.text, fact.anyOf) {
				t.Errorf("tier %q is missing the %s guidance (any of %q)", tier.name, fact.name, fact.anyOf)
			}
		}
	}
}

// TestTiersAgree_RecallLeadsEverywhere pins the retrieval order across tiers.
// Whichever tier a given agent receives, and many receive two at once (an MCP
// client whose CLAUDE.md carries the full body), it must not be told to query
// the graph before recalling.
//
// Two complementary checks, because neither alone is sufficient. The structural
// one catches a graph-first rewrite phrased in new words; the banned-phrase one
// catches a literal revert of the wording this change deleted. Note the
// structural check anchors on each markdown tier's section heading rather than
// the first bare occurrence of "recall"/"graph": in agent-instructions.md those
// first occurrences are both inside the incidental phrase "no recall/graph/list"
// in the session-start bullet, so a naive index comparison would pass on a
// coincidence and break on an unrelated edit.
func TestTiersAgree_RecallLeadsEverywhere(t *testing.T) {
	for _, tier := range allTiers(t) {
		for _, banned := range []string{"graph: ALWAYS", "ALWAYS query first", "graph first"} {
			if strings.Contains(tier.text, banned) {
				t.Errorf("tier %q instructs graph-before-recall (%q); recall leads in every tier", tier.name, banned)
			}
		}
	}

	// Section ordering, per tier, using that tier's own headings. The handshake
	// variants are covered structurally by TestBuildInstructions_RetrievalPrecedence,
	// which reads their numbered steps.
	for _, tc := range []struct{ tier, format, recall, graph string }{
		{"full", "claude", "**WHEN TO RECALL**", "**WHEN TO EXPLORE**"},
		{"condensed", "condensed", "RECALL (recall)", "EXPLORE (graph)"},
	} {
		body, ok := instructions.Lookup(tc.format)
		if !ok {
			t.Fatalf("%s tier must resolve", tc.tier)
		}
		recallIdx, graphIdx := strings.Index(body, tc.recall), strings.Index(body, tc.graph)
		if recallIdx < 0 || graphIdx < 0 {
			t.Errorf("tier %q is missing a retrieval section heading (%q@%d, %q@%d)",
				tc.tier, tc.recall, recallIdx, tc.graph, graphIdx)
			continue
		}
		if recallIdx >= graphIdx {
			t.Errorf("tier %q puts its graph section before its recall section", tc.tier)
		}
	}
}

// TestHandshakeIsNotWebServed pins that the MCP handshake body stays off
// GET /instructions. It is a wire payload for MCP clients, not a document for
// humans to paste, and exposing it would hand operators a fourth flavor to keep
// in step with the other three.
func TestHandshakeIsNotWebServed(t *testing.T) {
	for _, format := range []string{"handshake", "mcp-handshake", "mcp"} {
		if body, ok := instructions.Lookup(format); ok {
			t.Errorf("Lookup(%q) resolved to a %d-char body; the handshake must not be web-served", format, len(body))
		}
	}
}

func containsAny(haystack string, needles []string) bool {
	for _, n := range needles {
		if strings.Contains(haystack, n) {
			return true
		}
	}
	return false
}

// initializeResultForTest drives a real MCP initialize through the server's
// HTTP handler and decodes the result. It deliberately goes over the wire
// rather than calling the hook directly: the bug these tests guard against was
// a decoration applied after buildInstructions returned, which only a
// round-trip observes.
func initializeResultForTest(t *testing.T, deps Dependencies) mcp.InitializeResult {
	t.Helper()

	srv := newTestServer(deps)
	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{` +
		`"protocolVersion":"2025-06-18","capabilities":{},` +
		`"clientInfo":{"name":"test","version":"1"}}}`
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("initialize returned %d: %s", rec.Code, rec.Body.String())
	}

	// parseJSONRPCResponse (http_stack_test.go) already handles both shapes the
	// Streamable HTTP transport can answer with, plain JSON and a single SSE
	// event, and validates the envelope.
	rpc := parseJSONRPCResponse(t, rec.Result())
	if rpc.Error != nil {
		t.Fatalf("initialize returned an error: %d %s", rpc.Error.Code, rpc.Error.Message)
	}

	var result mcp.InitializeResult
	if err := json.Unmarshal(rpc.Result, &result); err != nil {
		t.Fatalf("decode initialize result: %v (%s)", err, rpc.Result)
	}
	return result
}
