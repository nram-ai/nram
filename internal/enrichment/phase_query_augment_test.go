package enrichment

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

func TestRenderQueryAugmentPrompt_SubstitutesPlaceholders(t *testing.T) {
	tpl := "Generate {N} queries for: {content}. End."
	out := RenderQueryAugmentPrompt(tpl, "a memory", 7)
	if !strings.Contains(out, "Generate 7 queries") {
		t.Fatalf("N placeholder not substituted: %q", out)
	}
	if !strings.Contains(out, "for: a memory.") {
		t.Fatalf("content placeholder not substituted: %q", out)
	}
}

func TestRenderQueryAugmentPrompt_NoPanicOnLiteralPercent(t *testing.T) {
	// strings.Replace not fmt.Sprintf, so a literal % anywhere must round-trip.
	tpl := "Coverage target: 90% recall. Memory: {content}. N={N}."
	out := RenderQueryAugmentPrompt(tpl, "x", 3)
	if !strings.Contains(out, "90% recall") {
		t.Fatalf("literal %% mangled: %q", out)
	}
}

func TestParseQueryAugmentResponse_PlainArray(t *testing.T) {
	queries, err := ParseQueryAugmentResponse(`["q one","q two","q three"]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(queries) != 3 || queries[0] != "q one" {
		t.Fatalf("queries = %v", queries)
	}
}

func TestParseQueryAugmentResponse_FencedJSON(t *testing.T) {
	body := "Sure, here you go:\n```json\n[\"alpha\", \"beta\"]\n```\nDone."
	queries, err := ParseQueryAugmentResponse(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(queries) != 2 || queries[1] != "beta" {
		t.Fatalf("queries = %v", queries)
	}
}

func TestParseQueryAugmentResponse_DropsEmpties(t *testing.T) {
	queries, err := ParseQueryAugmentResponse(`["one","","   ","two"]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(queries) != 2 || queries[0] != "one" || queries[1] != "two" {
		t.Fatalf("expected only non-empty queries; got %v", queries)
	}
}

func TestParseQueryAugmentResponse_MalformedReturnsError(t *testing.T) {
	if _, err := ParseQueryAugmentResponse(`{"not": "an array"}`); err == nil {
		t.Fatalf("expected error on object payload")
	}
	if _, err := ParseQueryAugmentResponse(`not json at all`); err == nil {
		t.Fatalf("expected error on garbage")
	}
	if _, err := ParseQueryAugmentResponse(`[]`); err == nil {
		t.Fatalf("expected error on empty array (no useful augmentation)")
	}
}

// Small models (qwen3:8b-extract in particular) periodically wrap the array
// in a single-key object even when prompted otherwise. The parser must accept
// any envelope key so we don't ship spurious failures to operators.
func TestParseQueryAugmentResponse_ObjectEnvelope(t *testing.T) {
	cases := map[string]string{
		"queries key":   `{"queries": ["one", "two", "three"]}`,
		"questions key": `{"questions": ["alpha", "beta"]}`,
		"arbitrary key": `{"output": ["only", "this"]}`,
		"with prose":    "Here you go:\n```json\n{\"queries\": [\"x\", \"y\"]}\n```",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			queries, err := ParseQueryAugmentResponse(body)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(queries) == 0 {
				t.Fatalf("expected queries; got empty")
			}
		})
	}
}

// Bare array with mixed element types should stringify each rather than
// blowing up the entire parse. The model occasionally interpolates a number
// when the prompt asks for a count-related phrasing.
func TestParseQueryAugmentResponse_MixedElementTypes(t *testing.T) {
	queries, err := ParseQueryAugmentResponse(`["how many", 42, "or about so", true]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(queries) != 4 {
		t.Fatalf("expected 4 stringified queries; got %v", queries)
	}
	if queries[1] != "42" {
		t.Fatalf("expected numeric element coerced to \"42\"; got %q", queries[1])
	}
	if queries[3] != "true" {
		t.Fatalf("expected bool element coerced to \"true\"; got %q", queries[3])
	}
}

// Object containing a nested object (not an array) under its single key is
// irrecoverable; verify it still surfaces an error rather than silently
// collapsing into garbage strings.
func TestParseQueryAugmentResponse_NestedObjectStillFails(t *testing.T) {
	if _, err := ParseQueryAugmentResponse(`{"queries": {"a": "b"}}`); err == nil {
		t.Fatalf("expected error on object-of-object payload")
	}
}

// Lenient pass: small models (qwen3:8b observed) periodically emit the array
// brackets but drop the per-element double quotes, even when the prompt
// explicitly says QUOTED. The parser must recover rather than fail-soft to
// zero queries — otherwise the operator's enable-flag silently no-ops.
func TestParseQueryAugmentResponse_LenientUnquotedElements(t *testing.T) {
	cases := map[string]struct {
		body string
		want []string
	}{
		"bare unquoted comma list": {
			body: `[who is Emma's husband, Brandon's spouse name, Emma's marital status, wife of Brandon]`,
			want: []string{"who is Emma's husband", "Brandon's spouse name", "Emma's marital status", "wife of Brandon"},
		},
		"single-quoted elements": {
			body: `['who is Emma married to', 'Brandon spouse', 'Emma husband']`,
			want: []string{"who is Emma married to", "Brandon spouse", "Emma husband"},
		},
		"mixed quoting": {
			body: `["who is Emma married to", Brandon spouse, 'Emma husband']`,
			want: []string{"who is Emma married to", "Brandon spouse", "Emma husband"},
		},
		"backtick wrap": {
			body: "[`alpha`, `beta`, `gamma`]",
			want: []string{"alpha", "beta", "gamma"},
		},
		"newline delimited inside brackets": {
			body: "[\n  alpha\n  beta\n  gamma\n]",
			want: []string{"alpha", "beta", "gamma"},
		},
		"unquoted with prose envelope": {
			body: "Here you go: [alpha query, beta query, gamma query]. Done.",
			want: []string{"alpha query", "beta query", "gamma query"},
		},
		"unquoted inside object envelope": {
			body: `{"queries": [alpha, beta, gamma]}`,
			want: []string{"alpha", "beta", "gamma"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseQueryAugmentResponse(tc.body)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

// Lenient pass must not paper over genuinely empty or non-array payloads.
// These three cases existed before the lenient pass landed and continue to
// produce parse errors because either the brackets are absent or the bracket
// interior holds no extractable tokens.
func TestParseQueryAugmentResponse_LenientStillRejectsTrueGarbage(t *testing.T) {
	for _, body := range []string{
		`not json at all`,
		`{"not": "an array"}`,
		`[]`,
		`[ , , ]`,
	} {
		if _, err := ParseQueryAugmentResponse(body); err == nil {
			t.Fatalf("expected error on %q; lenient pass overshot", body)
		}
	}
}

// Truncation-prefix recovery: the dominant qwen3:8b-extract failure mode in
// production. The model writes the opening '[' plus N well-formed string
// elements then runs out of tokens (or stops emitting) before the closing
// ']'. Strict JSON returns "unexpected end of JSON input"; the lenient
// pre-recovery pass bails because no ']' is present. The fifth pass walks
// the body with json.Decoder and salvages every cleanly-decoded element.
func TestParseQueryAugmentResponse_TruncatedArrayRecovery(t *testing.T) {
	cases := map[string]struct {
		body string
		want []string
	}{
		"trailing comma, no closing bracket": {
			body: `["who is Brandon", "Brandon's company", "Brandon role at Velocity",`,
			want: []string{"who is Brandon", "Brandon's company", "Brandon role at Velocity"},
		},
		"three full elements then truncated mid-string": {
			body: `["alpha", "beta", "gamma", "del`,
			want: []string{"alpha", "beta", "gamma"},
		},
		"two elements then truncated immediately after comma": {
			body: `["one", "two",`,
			want: []string{"one", "two"},
		},
		"object envelope truncated mid-array": {
			body: `{"queries": ["first", "second", "third",`,
			want: []string{"first", "second", "third"},
		},
		"object envelope truncated mid-string": {
			body: `{"queries": ["aaa", "bbb", "cc`,
			want: []string{"aaa", "bbb"},
		},
		"prose preamble plus truncated array": {
			body: "Here are the queries:\n[\"q1\", \"q2\", \"q3\",",
			want: []string{"q1", "q2", "q3"},
		},
		"fenced markdown with truncated array body": {
			body: "```json\n[\"alpha query\", \"beta query\", \"gam",
			want: []string{"alpha query", "beta query"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseQueryAugmentResponse(tc.body)
			if err != nil {
				t.Fatalf("unexpected error on truncated payload: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

// Recovery must NOT paper over payloads with zero recoverable string
// elements. These cases reach pass 5 (no closing ']' so the lenient pass
// rejects them) but yield no clean string tokens, so the parser must
// continue to surface a parse error rather than fabricate empty queries.
func TestParseQueryAugmentResponse_TruncatedArrayRecoveryStillFailsOnZeroElements(t *testing.T) {
	for _, body := range []string{
		`[`,
		`[ ,`,
		`[123, 456,`,            // non-string elements only; pass 3 also fails because of truncation
		`{"queries": [1, 2, 3,`, // truncated object envelope, non-string elements
	} {
		if _, err := ParseQueryAugmentResponse(body); err == nil {
			t.Fatalf("expected error on %q; recovery overshot to fabricate queries", body)
		}
	}
}

func TestBuildAugmentedInput_NoCap(t *testing.T) {
	got, trimmed := BuildAugmentedInput([]string{"q1", "q2"}, "the memory content", 0)
	want := "q1\nq2" + QueryAugmentSeparator + "the memory content"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if trimmed != 0 {
		t.Fatalf("expected trimmed=0, got %d", trimmed)
	}
}

func TestBuildAugmentedInput_CapTruncatesContentTail(t *testing.T) {
	queries := []string{"first query", "second query"}
	content := strings.Repeat("X", 200)
	cap := 80
	got, trimmed := BuildAugmentedInput(queries, content, cap)
	if len(got) > cap {
		t.Fatalf("output length %d exceeds cap %d", len(got), cap)
	}
	// Queries + separator must always be present in full.
	prefix := strings.Join(queries, "\n") + QueryAugmentSeparator
	if !strings.HasPrefix(got, prefix) {
		t.Fatalf("expected output to start with queries+separator; got %q", got)
	}
	if trimmed != len(content)-(cap-len(prefix)) {
		t.Fatalf("trimmed bytes mismatch: got %d, content=%d cap=%d prefix=%d", trimmed, len(content), cap, len(prefix))
	}
}

func TestBuildAugmentedInput_CapTooSmallForQueriesKeepsQueriesOnly(t *testing.T) {
	queries := []string{"qqqq"}
	content := "memory content body"
	// Cap that doesn't even fit the prefix.
	got, trimmed := BuildAugmentedInput(queries, content, 4)
	prefix := strings.Join(queries, "\n") + QueryAugmentSeparator
	if got != prefix {
		t.Fatalf("expected prefix-only fallback %q, got %q", prefix, got)
	}
	if trimmed != len(content) {
		t.Fatalf("expected entire content trimmed; got %d (content was %d)", trimmed, len(content))
	}
}

// runQueryAugment fail-soft contract: any failure path returns nil so
// ingestion never gets blocked. Pure unit tests on the helpers cover the
// success path's data shape; this test exercises the orchestration.
func TestRunQueryAugment_DisabledIsNoOp(t *testing.T) {
	h := newIngestionHarness(
		map[string]string{service.SettingQueryAugmentEnabled: "false"},
		nil,
		minimalFactLLM(),
		minimalEntityLLM(),
		constStringLLM("ingestion", `{"operation":"ADD","target_id":null,"rationale":"new"}`),
		constEmbedder(),
	)
	res, skip := h.pool.runQueryAugment(context.Background(),
		&model.EnrichmentJob{ID: uuid.New()},
		&model.Memory{ID: uuid.New(), Content: "hello"})
	if res != nil {
		t.Fatalf("expected nil result when disabled; got %#v", res)
	}
	if skip != model.QueryAugmentSkipDisabled {
		t.Fatalf("expected disabled skip reason, got %q", skip)
	}
}

func TestRunQueryAugment_EmptyContentIsNoOp(t *testing.T) {
	h := newIngestionHarness(
		map[string]string{service.SettingQueryAugmentEnabled: "true"},
		nil,
		minimalFactLLM(),
		minimalEntityLLM(),
		constStringLLM("ingestion", `{"operation":"ADD","target_id":null,"rationale":"new"}`),
		constEmbedder(),
	)
	res, skip := h.pool.runQueryAugment(context.Background(),
		&model.EnrichmentJob{ID: uuid.New()},
		&model.Memory{ID: uuid.New(), Content: "   \n  "})
	if res != nil {
		t.Fatalf("expected nil result for whitespace-only content; got %#v", res)
	}
	if skip != model.QueryAugmentSkipContentEmpty {
		t.Fatalf("expected content_empty skip reason, got %q", skip)
	}
}

func TestRunQueryAugment_HappyPath(t *testing.T) {
	// The augmentation phase uses the fact provider by default, so swap that
	// stub for one that returns a JSON array.
	factAugmentLLM := &mockLLMProvider{
		name: "augment-fact",
		respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
			return &provider.CompletionResponse{
				Content: `["alt 1","alt 2","alt 3"]`,
				Model:   "augment-model",
				Usage:   provider.TokenUsage{PromptTokens: 12, CompletionTokens: 6, TotalTokens: 18},
			}, nil
		},
	}
	h := newIngestionHarness(
		map[string]string{service.SettingQueryAugmentEnabled: "true"},
		nil,
		factAugmentLLM,
		minimalEntityLLM(),
		constStringLLM("ingestion", `{"operation":"ADD","target_id":null,"rationale":"new"}`),
		constEmbedder(),
	)
	res, skip := h.pool.runQueryAugment(context.Background(),
		&model.EnrichmentJob{ID: uuid.New()},
		&model.Memory{ID: uuid.New(), Content: "the memory text"})
	if res == nil {
		t.Fatalf("expected non-nil result")
	}
	if skip != "" {
		t.Fatalf("expected empty skip reason on success, got %q", skip)
	}
	if len(res.queries) != 3 {
		t.Fatalf("queries = %v", res.queries)
	}
	if !strings.HasPrefix(res.augmentedContent, "alt 1\nalt 2\nalt 3"+QueryAugmentSeparator) {
		t.Fatalf("augmented content missing prefix: %q", res.augmentedContent)
	}
	if !strings.HasSuffix(res.augmentedContent, "the memory text") {
		t.Fatalf("augmented content missing original tail: %q", res.augmentedContent)
	}
}

// Precedence: when query-augmentation produced content AND the
// ingestion-decision phase also pre-computed parentEmbedding for the raw
// content, augmentation wins. runEmbedBatch discards the pre-embed, re-embeds
// against the augmented blob, and finalizeJob records the augmented marker on
// the memory row. Without this precedence the operator's enable-flag would
// silently no-op whenever ingestion-decision is also on.
func TestQueryAugment_PrecedenceOverridesIngestionDecisionPreEmbed(t *testing.T) {
	// LLM that routes by prompt content: augment requests get a JSON array,
	// ingestion-decision requests get an ADD decision, anything else gets the
	// minimal fact-extraction payload (so runPreEmbed completes cleanly).
	routedLLM := &mockLLMProvider{
		name: "routed",
		respond: func(req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
			prompt := req.Messages[0].Content
			switch {
			case strings.Contains(prompt, "query augmentation"):
				return &provider.CompletionResponse{
					Content: `["q1","q2","q3"]`,
					Model:   "augment-model",
					Usage:   provider.TokenUsage{PromptTokens: 5, CompletionTokens: 5, TotalTokens: 10},
				}, nil
			case strings.Contains(prompt, "ingestion decision"):
				return &provider.CompletionResponse{
					Content: `{"operation":"ADD","target_id":null,"rationale":""}`,
					Model:   "ingest-model",
				}, nil
			default:
				return &provider.CompletionResponse{Content: `[]`, Model: "fact-model"}, nil
			}
		},
	}

	// Near-match seeds the dedup vector store so the ingestion-decision phase
	// runs an embed call against raw content and stamps parentEmbedding onto
	// the pendingJob — the exact precondition that makes runEmbedBatch skip
	// the parent slot below.
	target := testMemory()
	target.Content = "existing fact"
	d := 384
	target.EmbeddingDim = &d
	dedupResults := []storage.VectorSearchResult{
		{ID: target.ID, Score: 0.96, NamespaceID: target.NamespaceID},
	}

	h := newIngestionHarness(
		map[string]string{
			service.SettingIngestionDecisionEnabled: "true",
			service.SettingIngestionDecisionShadow:  "false",
			service.SettingQueryAugmentEnabled:      "true",
		},
		dedupResults,
		routedLLM,
		minimalEntityLLM(),
		routedLLM,
		constEmbedder(),
	)

	newMem := testMemory()
	newMem.NamespaceID = target.NamespaceID
	h.reader.byID[newMem.ID] = newMem
	h.reader.byID[target.ID] = target
	job := testJob(newMem.ID, newMem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	if len(h.updater.enrichedMarks) != 1 {
		t.Fatalf("expected exactly one MarkEnriched call; got %d", len(h.updater.enrichedMarks))
	}
	mark := h.updater.enrichedMarks[0]
	if mark.augmentedQueries == nil || mark.augmentedEmbeddingAt == nil {
		t.Fatalf("augmented marker MUST be written when augmentation produced queries — ingestion-decision pre-embed must not suppress it; got queries=%v at=%v",
			mark.augmentedQueries, mark.augmentedEmbeddingAt)
	}
	if want := []string{"q1", "q2", "q3"}; !reflect.DeepEqual(mark.augmentedQueries, want) {
		t.Fatalf("augmented queries mismatch: got %v want %v", mark.augmentedQueries, want)
	}
	if got := h.queue.queryAugmentSkips[job.ID]; got != "" {
		t.Fatalf("query_augment_skip_reason MUST be empty when augmentation succeeded; got %q", got)
	}
}

// Complement to the regression above: with ingestion-decision OFF and only
// query-augment ON, the marker MUST be written — confirms the gate is not
// over-restrictive and that the happy path still records augmentation.
func TestQueryAugment_MarkerWrittenWhenAugmentedEmbedActuallyLanded(t *testing.T) {
	augLLM := &mockLLMProvider{
		name: "augment",
		respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
			return &provider.CompletionResponse{
				Content: `["q1","q2"]`,
				Model:   "augment-model",
				Usage:   provider.TokenUsage{PromptTokens: 5, CompletionTokens: 5, TotalTokens: 10},
			}, nil
		},
	}
	h := newIngestionHarness(
		map[string]string{service.SettingQueryAugmentEnabled: "true"},
		nil,
		augLLM,
		minimalEntityLLM(),
		constStringLLM("ingest", `{"operation":"ADD","target_id":null,"rationale":""}`),
		constEmbedder(),
	)

	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}

	if len(h.updater.enrichedMarks) != 1 {
		t.Fatalf("expected exactly one MarkEnriched call; got %d", len(h.updater.enrichedMarks))
	}
	mark := h.updater.enrichedMarks[0]
	if mark.augmentedEmbeddingAt == nil {
		t.Fatalf("expected augmented_embedding_at to be set on happy path; got nil")
	}
	if got := mark.augmentedQueries; len(got) != 2 || got[0] != "q1" || got[1] != "q2" {
		t.Fatalf("augmented marker queries = %v, want [q1 q2]", got)
	}
	if mark.id != mem.ID {
		t.Fatalf("augmented marker target = %v, want %v", mark.id, mem.ID)
	}
	if got := h.queue.queryAugmentSkips[job.ID]; got != "" {
		t.Fatalf("query_augment_skip_reason MUST be empty when augmentation succeeded; got %q", got)
	}
}

// Skip-reason persistence: with the augmentation flag off, finalizeJob must
// stamp QueryAugmentSkipDisabled on the queue row so EnrichmentMonitor renders
// "skipped (feature disabled)" instead of bare "skipped".
func TestQueryAugment_SkipReasonPersistedWhenDisabled(t *testing.T) {
	h := newIngestionHarness(
		map[string]string{service.SettingQueryAugmentEnabled: "false"},
		nil,
		minimalFactLLM(),
		minimalEntityLLM(),
		constStringLLM("ingest", `{"operation":"ADD","target_id":null,"rationale":""}`),
		constEmbedder(),
	)
	mem := testMemory()
	h.reader.byID[mem.ID] = mem
	job := testJob(mem.ID, mem.NamespaceID)

	if err := h.pool.processJob(context.Background(), "w-0", job); err != nil {
		t.Fatalf("processJob: %v", err)
	}
	if got := h.queue.queryAugmentSkips[job.ID]; got != model.QueryAugmentSkipDisabled {
		t.Fatalf("query_augment_skip_reason = %q, want %q", got, model.QueryAugmentSkipDisabled)
	}
}

func TestRunQueryAugment_MalformedJSONFailSoft(t *testing.T) {
	badLLM := &mockLLMProvider{
		name: "augment-bad",
		respond: func(_ *provider.CompletionRequest) (*provider.CompletionResponse, error) {
			return &provider.CompletionResponse{
				Content: "I am not JSON, I am free.",
				Model:   "augment-model",
			}, nil
		},
	}
	h := newIngestionHarness(
		map[string]string{service.SettingQueryAugmentEnabled: "true"},
		nil,
		badLLM,
		minimalEntityLLM(),
		constStringLLM("ingestion", `{"operation":"ADD","target_id":null,"rationale":"new"}`),
		constEmbedder(),
	)
	res, skip := h.pool.runQueryAugment(context.Background(),
		&model.EnrichmentJob{ID: uuid.New()},
		&model.Memory{ID: uuid.New(), Content: "anything"})
	if res != nil {
		t.Fatalf("expected fail-soft nil on malformed JSON; got %#v", res)
	}
	if skip != model.QueryAugmentSkipParseError {
		t.Fatalf("expected parse_error skip reason, got %q", skip)
	}
}
