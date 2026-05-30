package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/enrichment"
	"github.com/nram-ai/nram/internal/provider"
)

// EnrichmentAdminStore abstracts storage and worker management operations
// for the enrichment admin API.
type EnrichmentAdminStore interface {
	// QueueStatus returns counts by status and a page of queue items.
	QueueStatus(ctx context.Context, params QueueListParams) (*EnrichmentQueueStatus, error)
	// RetryFailed retries failed enrichment jobs. If ids is nil/empty, retries all failed.
	RetryFailed(ctx context.Context, ids []uuid.UUID) (int, error)
	// SetPaused pauses or resumes enrichment workers.
	SetPaused(ctx context.Context, paused bool) error
	// IsPaused returns whether enrichment workers are paused.
	IsPaused(ctx context.Context) (bool, error)
}

// EnrichmentAdminConfig holds the dependencies for the enrichment admin handler.
type EnrichmentAdminConfig struct {
	Store                 EnrichmentAdminStore
	FactProvider          func() provider.LLMProvider
	EntityProvider        func() provider.LLMProvider
	FactPromptDefault     func(ctx context.Context) string
	EntityPromptDefault   func(ctx context.Context) string
	QueryAugmentPromptDef func(ctx context.Context) string

	// BackfillAugmentation runs the query-augmentation backfill against memories
	// whose vector pre-dates the feature flip. Nil disables the backfill
	// endpoint with a 503 response so the UI button can render "not available"
	// rather than 404ing in deployments without the service wired.
	BackfillAugmentation func(ctx context.Context, projectID uuid.UUID, dryRun bool, limit int) (candidateCount, enqueued int, err error)

	// BackfillExtractedFactParaphrase enqueues paraphrase-guard sweep jobs for
	// enriched parents that have extracted-fact children. Nil disables the
	// endpoint with a 503 response in deployments where the service is not
	// wired.
	BackfillExtractedFactParaphrase func(ctx context.Context, projectID uuid.UUID, dryRun bool, limit int) (candidateCount, enqueued int, err error)
}

// EnrichmentQueueStatus is the response for GET /enrichment/queue.
type EnrichmentQueueStatus struct {
	Counts EnrichmentQueueCounts `json:"counts"`
	Items  []EnrichmentQueueItem `json:"items"`
	Paused bool                  `json:"paused"`
}

// Queue list pagination bounds. The default page size matches the historical
// hardcoded LIMIT; the max caps how much a single request can pull.
const (
	QueueListDefaultLimit = 50
	QueueListMaxLimit     = 200
)

// QueueListParams controls pagination, ordering, and status filtering for the
// enrichment queue list endpoints (/v1/me, /v1/orgs/{id}, /v1/admin). The zero
// value is valid: Normalize resolves it to the first page (limit 50, offset 0)
// ordered newest-first across all statuses, so internal callers and tests may
// pass QueueListParams{} unchanged.
type QueueListParams struct {
	Limit  int
	Offset int
	// Sort is one of "created_at", "status", "attempts". Anything else
	// normalizes to "created_at".
	Sort string
	// Dir is "asc" or "desc" (default "desc").
	Dir string
	// Status filters to a single queue state ("pending", "processing",
	// "completed", "failed"). Empty means no filter (all states).
	Status string
}

// Normalize returns a copy with out-of-range or unrecognized values replaced
// by safe defaults. Storage implementations call this so a zero-value params
// still yields the default first page, and the Sort/Dir/Status whitelists are
// enforced before any value reaches a SQL ORDER BY / WHERE clause.
func (p QueueListParams) Normalize() QueueListParams {
	out := p
	if out.Limit <= 0 {
		out.Limit = QueueListDefaultLimit
	}
	if out.Limit > QueueListMaxLimit {
		out.Limit = QueueListMaxLimit
	}
	if out.Offset < 0 {
		out.Offset = 0
	}
	switch out.Sort {
	case "status", "attempts", "created_at":
		// recognized
	default:
		out.Sort = "created_at"
	}
	if strings.EqualFold(out.Dir, "asc") {
		out.Dir = "asc"
	} else {
		out.Dir = "desc"
	}
	switch out.Status {
	case "pending", "processing", "completed", "failed":
		// recognized
	default:
		out.Status = ""
	}
	return out
}

// parseQueueListParams reads pagination/sort/filter query parameters from an
// enrichment queue list request and returns a normalized QueueListParams.
func parseQueueListParams(r *http.Request) QueueListParams {
	q := r.URL.Query()
	p := QueueListParams{
		Sort:   q.Get("sort"),
		Dir:    q.Get("dir"),
		Status: q.Get("status"),
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			p.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			p.Offset = n
		}
	}
	return p.Normalize()
}

// EnrichmentQueueCounts contains the count of items in each queue state.
type EnrichmentQueueCounts struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Completed  int `json:"completed"`
	Failed     int `json:"failed"`
}

// EnrichmentQueueItem describes a single item in the enrichment queue.
// is_stale_diagnostic is true when claimed_at_age_ms exceeds half of
// enrichment.stuck_threshold_seconds. claimed_at_age_ms and claimed_at are
// populated only when status='processing'. claimed_at resets on every
// retry/requeue (Retry + RequeueStale null it out and the next worker
// re-claims), so the UI can use it as the anchor for "time on this attempt"
// without paying for the cumulative wait since job creation.
//
// ProjectID is populated whenever the memory's project is resolvable.
// ProjectName is populated only on self-tier responses; org and system
// tiers intentionally leave it empty so an org_owner or admin sees UUIDs
// only for projects owned by other users and the UI falls through.
type EnrichmentQueueItem struct {
	ID                uuid.UUID  `json:"id"`
	MemoryID          uuid.UUID  `json:"memory_id"`
	ProjectID         *uuid.UUID `json:"project_id,omitempty"`
	ProjectName       string     `json:"project_name,omitempty"`
	Status            string     `json:"status"`
	Attempts          int        `json:"attempts"`
	MaxAttempts       int        `json:"max_attempts,omitempty"`
	LastError         string     `json:"last_error,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	ClaimedBy         *string    `json:"claimed_by,omitempty"`
	ClaimedAt         *time.Time `json:"claimed_at,omitempty"`
	ClaimedAtAgeMs    *int64     `json:"claimed_at_age_ms,omitempty"`
	IsStaleDiagnostic bool       `json:"is_stale_diagnostic"`
	LastRequeueReason *string    `json:"last_requeue_reason,omitempty"`
	// StepsCompleted lists the enrichment phases that finished for this job
	// (subset of model.Step* constants). Always emitted as an array (never
	// null) so UI consumers never need an undefined check.
	StepsCompleted []string `json:"steps_completed"`
	// QueryAugmentSkipReason carries the structured cause when the
	// query_augmentation step is absent from StepsCompleted on a completed
	// job. Values are drawn from model.QueryAugmentSkip* constants. nil =
	// step ran successfully (look in StepsCompleted) or the row predates the
	// column.
	QueryAugmentSkipReason *string `json:"query_augment_skip_reason,omitempty"`
	// AugmentedQueries / AugmentedEmbeddingAt mirror the same-named columns
	// on the joined memory row so the enrichment-monitor "Augmentation"
	// accordion can render the persisted badge ("✓ Augmented · N queries")
	// without a second roundtrip per expanded row. Empty/nil means the
	// memory's vector was built from raw content (the badge falls back to
	// "Raw embed · not augmented"). Joined via LEFT JOIN so a row whose
	// memory has been soft-deleted still hydrates with both fields nil.
	AugmentedQueries     []string   `json:"augmented_queries,omitempty"`
	AugmentedEmbeddingAt *time.Time `json:"augmented_embedding_at,omitempty"`
}

// enrichmentRetryRequest is the request body for POST /enrichment/retry.
type enrichmentRetryRequest struct {
	IDs []uuid.UUID `json:"ids"`
}

// enrichmentPauseRequest is the request body for POST /enrichment/pause.
type enrichmentPauseRequest struct {
	Paused bool `json:"paused"`
}

// NewAdminEnrichmentHandler returns an http.HandlerFunc that dispatches enrichment
// admin requests based on method and sub-path under /enrichment.
//
// Routes:
//   - GET  /enrichment         — queue status (convenience alias)
//   - GET  /enrichment/queue   — queue status with counts and recent items
//   - POST /enrichment/retry   — retry failed jobs (all or specific IDs)
//   - POST /enrichment/pause   — pause or resume enrichment workers
func NewAdminEnrichmentHandler(cfg EnrichmentAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sub := extractEnrichmentSubPath(r.URL.Path)

		// Write operations require administrator role.
		if sub == "retry" || sub == "pause" || sub == "test-prompt" || sub == "backfill-augmentation" || sub == "backfill-extracted-fact-paraphrase" {
			ac := auth.FromContext(r.Context())
			if ac == nil || ac.Role != auth.RoleAdministrator {
				http.Error(w, "forbidden: administrator required", http.StatusForbidden)
				return
			}
		}

		switch sub {
		case "", "queue":
			handleEnrichmentQueue(w, r, cfg)
		case "retry":
			handleEnrichmentRetry(w, r, cfg)
		case "pause":
			handleEnrichmentPause(w, r, cfg)
		case "test-prompt":
			handleEnrichmentTestPrompt(w, r, cfg)
		case "backfill-augmentation":
			handleEnrichmentBackfillAugmentation(w, r, cfg)
		case "backfill-extracted-fact-paraphrase":
			handleEnrichmentBackfillExtractedFactParaphrase(w, r, cfg)
		default:
			WriteError(w, ErrBadRequest("unknown enrichment sub-path"))
		}
	}
}

// extractEnrichmentSubPath returns the portion of the URL path after "/enrichment".
// For example, "/v1/admin/enrichment/queue" returns "queue".
func extractEnrichmentSubPath(path string) string {
	const marker = "/enrichment"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return ""
	}
	rest := path[idx+len(marker):]
	rest = strings.TrimPrefix(rest, "/")
	return rest
}

// handleEnrichmentQueue handles GET /enrichment and GET /enrichment/queue.
func handleEnrichmentQueue(w http.ResponseWriter, r *http.Request, cfg EnrichmentAdminConfig) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	status, err := cfg.Store.QueueStatus(r.Context(), parseQueueListParams(r))
	if err != nil {
		WriteError(w, ErrInternal("failed to get enrichment queue status"))
		return
	}

	if status.Items == nil {
		status.Items = []EnrichmentQueueItem{}
	}

	writeJSON(w, http.StatusOK, status)
}

// handleEnrichmentRetry handles POST /enrichment/retry.
func handleEnrichmentRetry(w http.ResponseWriter, r *http.Request, cfg EnrichmentAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var body enrichmentRetryRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	retried, err := cfg.Store.RetryFailed(r.Context(), body.IDs)
	if err != nil {
		WriteError(w, ErrInternal("failed to retry enrichment jobs"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]int{"retried": retried})
}

// handleEnrichmentPause handles POST /enrichment/pause.
func handleEnrichmentPause(w http.ResponseWriter, r *http.Request, cfg EnrichmentAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var body enrichmentPauseRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	if err := cfg.Store.SetPaused(r.Context(), body.Paused); err != nil {
		WriteError(w, ErrInternal("failed to set enrichment pause state"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"paused": body.Paused})
}

// enrichmentTestPromptRequest is the request body for POST /enrichment/test-prompt.
type enrichmentTestPromptRequest struct {
	Type        string `json:"type"`            // "fact", "entity", or "augment"
	Prompt      string `json:"prompt"`          // custom prompt text (optional; uses default if empty)
	SampleInput string `json:"sample_input"`    // memory content to test against
	Count       int    `json:"count,omitempty"` // only used when type=="augment"; defaults to 4
}

// enrichmentTestPromptResponse is the response for POST /enrichment/test-prompt.
type enrichmentTestPromptResponse struct {
	Output    string `json:"output"` // raw LLM output
	Parsed    any    `json:"parsed"` // parsed structured data (facts or entities)
	Error     string `json:"error,omitempty"`
	LatencyMs int64  `json:"latency_ms"`
}

// handleEnrichmentTestPrompt handles POST /enrichment/test-prompt.
// It sends the sample input through the configured LLM provider using the given prompt
// and returns both the raw output and parsed structured data.
func handleEnrichmentTestPrompt(w http.ResponseWriter, r *http.Request, cfg EnrichmentAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var body enrichmentTestPromptRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	if body.Type != "fact" && body.Type != "entity" && body.Type != "augment" {
		WriteError(w, ErrBadRequest("type must be 'fact', 'entity', or 'augment'"))
		return
	}

	if strings.TrimSpace(body.SampleInput) == "" {
		WriteError(w, ErrBadRequest("sample_input is required"))
		return
	}

	var llmProvider provider.LLMProvider
	var promptTemplate string

	switch body.Type {
	case "fact":
		if cfg.FactProvider == nil {
			WriteError(w, ErrBadRequest("no fact extraction provider configured"))
			return
		}
		llmProvider = cfg.FactProvider()
		if llmProvider == nil {
			WriteError(w, ErrBadRequest("fact extraction provider is not available"))
			return
		}
		if strings.TrimSpace(body.Prompt) != "" {
			promptTemplate = body.Prompt
		} else if cfg.FactPromptDefault != nil {
			promptTemplate = cfg.FactPromptDefault(r.Context())
		}
	case "entity":
		if cfg.EntityProvider == nil {
			WriteError(w, ErrBadRequest("no entity extraction provider configured"))
			return
		}
		llmProvider = cfg.EntityProvider()
		if llmProvider == nil {
			WriteError(w, ErrBadRequest("entity extraction provider is not available"))
			return
		}
		if strings.TrimSpace(body.Prompt) != "" {
			promptTemplate = body.Prompt
		} else if cfg.EntityPromptDefault != nil {
			promptTemplate = cfg.EntityPromptDefault(r.Context())
		}
	case "augment":
		// Augmentation reuses the fact-extraction provider by default; the
		// runtime phase does the same. A future operator setting may override
		// the model, but the provider plumbing is shared.
		if cfg.FactProvider == nil {
			WriteError(w, ErrBadRequest("no fact extraction provider configured"))
			return
		}
		llmProvider = cfg.FactProvider()
		if llmProvider == nil {
			WriteError(w, ErrBadRequest("fact extraction provider is not available"))
			return
		}
		if strings.TrimSpace(body.Prompt) != "" {
			promptTemplate = body.Prompt
		} else if cfg.QueryAugmentPromptDef != nil {
			promptTemplate = cfg.QueryAugmentPromptDef(r.Context())
		}
	}

	if promptTemplate == "" {
		WriteError(w, ErrBadRequest("no prompt template available"))
		return
	}

	count := body.Count
	if count <= 0 {
		count = 4
	}

	start := time.Now()

	rendered := fmt.Sprintf(promptTemplate, body.SampleInput)
	if body.Type == "augment" {
		rendered = enrichment.RenderQueryAugmentPrompt(promptTemplate, body.SampleInput, count)
	}

	completionReq := &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "user", Content: rendered},
		},
		MaxTokens:   2048,
		Temperature: 0.1,
		// Augmentation expects a JSON-array response. The runtime phase and
		// the per-memory preview endpoint both set JSONMode; without it
		// here, the Test button can fail to parse on providers that emit
		// prose without an explicit JSON-mode signal even when the prompt
		// requests JSON, masking a working production prompt as broken.
		JSONMode: body.Type == "augment",
	}

	resp, err := llmProvider.Complete(r.Context(), completionReq)
	if err != nil {
		writeJSON(w, http.StatusOK, enrichmentTestPromptResponse{
			Error:     fmt.Sprintf("LLM call failed: %v", err),
			LatencyMs: time.Since(start).Milliseconds(),
		})
		return
	}

	latency := time.Since(start).Milliseconds()
	rawOutput := resp.Content

	// Attempt to parse the output into structured data.
	var parsed any
	var parseErr string

	switch body.Type {
	case "fact":
		facts, err := parseTestFactResponse(rawOutput)
		if err != nil {
			parseErr = err.Error()
		} else {
			parsed = facts
		}
	case "entity":
		result, err := parseTestEntityResponse(rawOutput)
		if err != nil {
			parseErr = err.Error()
		} else {
			parsed = result
		}
	case "augment":
		queries, err := enrichment.ParseQueryAugmentResponse(rawOutput)
		if err != nil {
			parseErr = err.Error()
		} else {
			parsed = queries
		}
	}

	response := enrichmentTestPromptResponse{
		Output:    rawOutput,
		Parsed:    parsed,
		LatencyMs: latency,
	}
	if parseErr != "" {
		response.Error = fmt.Sprintf("parse warning: %s", parseErr)
	}

	writeJSON(w, http.StatusOK, response)
}

// parseTestFactResponse parses an LLM fact extraction response.
func parseTestFactResponse(raw string) (any, error) {
	raw = strings.TrimSpace(raw)

	type extractedFact struct {
		Fact       string  `json:"fact"`
		Confidence float64 `json:"confidence"`
	}

	var facts []extractedFact
	if err := json.Unmarshal([]byte(raw), &facts); err == nil {
		return facts, nil
	}

	stripped := stripTestMarkdownFences(raw)
	if err := json.Unmarshal([]byte(stripped), &facts); err == nil {
		return facts, nil
	}

	re := regexp.MustCompile(`\[[\s\S]*\]`)
	match := re.FindString(raw)
	if match != "" {
		if err := json.Unmarshal([]byte(match), &facts); err == nil {
			return facts, nil
		}
	}

	return nil, fmt.Errorf("could not parse response as JSON fact array")
}

// parseTestEntityResponse parses an LLM entity extraction response.
func parseTestEntityResponse(raw string) (any, error) {
	raw = strings.TrimSpace(raw)

	var result map[string]any
	if err := json.Unmarshal([]byte(raw), &result); err == nil {
		return result, nil
	}

	stripped := stripTestMarkdownFences(raw)
	if err := json.Unmarshal([]byte(stripped), &result); err == nil {
		return result, nil
	}

	re := regexp.MustCompile(`\{[\s\S]*\}`)
	match := re.FindString(raw)
	if match != "" {
		if err := json.Unmarshal([]byte(match), &result); err == nil {
			return result, nil
		}
	}

	return nil, fmt.Errorf("could not parse response as JSON entity object")
}

// enrichmentBackfillAugmentRequest is the body for
// POST /enrichment/backfill-augmentation. ProjectID is optional; omit to scan
// the entire deployment. DryRun returns the candidate count without
// enqueueing. Limit caps how many candidates land in the queue this call.
type enrichmentBackfillAugmentRequest struct {
	ProjectID *uuid.UUID `json:"project_id,omitempty"`
	DryRun    bool       `json:"dry_run,omitempty"`
	Limit     int        `json:"limit,omitempty"`
}

type enrichmentBackfillAugmentResponse struct {
	CandidateCount int  `json:"candidate_count"`
	Enqueued       int  `json:"enqueued"`
	DryRun         bool `json:"dry_run"`
}

// handleEnrichmentBackfillAugmentation handles POST /enrichment/backfill-augmentation.
func handleEnrichmentBackfillAugmentation(w http.ResponseWriter, r *http.Request, cfg EnrichmentAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	if cfg.BackfillAugmentation == nil {
		http.Error(w, "backfill-augmentation not available in this deployment", http.StatusServiceUnavailable)
		return
	}
	var body enrichmentBackfillAugmentRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}
	var projectID uuid.UUID
	if body.ProjectID != nil {
		projectID = *body.ProjectID
	}
	count, enq, err := cfg.BackfillAugmentation(r.Context(), projectID, body.DryRun, body.Limit)
	if err != nil {
		WriteError(w, ErrInternal("backfill augmentation: "+err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, enrichmentBackfillAugmentResponse{
		CandidateCount: count,
		Enqueued:       enq,
		DryRun:         body.DryRun,
	})
}

// handleEnrichmentBackfillExtractedFactParaphrase handles
// POST /enrichment/backfill-extracted-fact-paraphrase. ProjectID is optional;
// omit to scan the whole deployment. DryRun returns the candidate count
// without enqueueing. Limit caps how many parents this call enqueues.
func handleEnrichmentBackfillExtractedFactParaphrase(w http.ResponseWriter, r *http.Request, cfg EnrichmentAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}
	if cfg.BackfillExtractedFactParaphrase == nil {
		http.Error(w, "backfill-extracted-fact-paraphrase not available in this deployment", http.StatusServiceUnavailable)
		return
	}
	var body enrichmentBackfillAugmentRequest // identical wire shape
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}
	var projectID uuid.UUID
	if body.ProjectID != nil {
		projectID = *body.ProjectID
	}
	count, enq, err := cfg.BackfillExtractedFactParaphrase(r.Context(), projectID, body.DryRun, body.Limit)
	if err != nil {
		WriteError(w, ErrInternal("backfill extracted-fact paraphrase: "+err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, enrichmentBackfillAugmentResponse{
		CandidateCount: count,
		Enqueued:       enq,
		DryRun:         body.DryRun,
	})
}

// stripTestMarkdownFences removes markdown code fence wrappers.
func stripTestMarkdownFences(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "```") {
		idx := strings.Index(s, "\n")
		if idx < 0 {
			return s
		}
		s = s[idx+1:]
		if lastIdx := strings.LastIndex(s, "```"); lastIdx >= 0 {
			s = s[:lastIdx]
		}
		s = strings.TrimSpace(s)
	}
	return s
}
