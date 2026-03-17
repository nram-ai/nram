package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/provider"
)

// EnrichmentAdminStore abstracts storage and worker management operations
// for the enrichment admin API.
type EnrichmentAdminStore interface {
	// QueueStatus returns counts by status and recent queue items.
	QueueStatus(ctx context.Context) (*EnrichmentQueueStatus, error)
	// RetryFailed retries failed enrichment jobs. If ids is nil/empty, retries all failed.
	RetryFailed(ctx context.Context, ids []uuid.UUID) (int, error)
	// SetPaused pauses or resumes enrichment workers.
	SetPaused(ctx context.Context, paused bool) error
	// IsPaused returns whether enrichment workers are paused.
	IsPaused(ctx context.Context) (bool, error)
}

// EnrichmentAdminConfig holds the dependencies for the enrichment admin handler.
type EnrichmentAdminConfig struct {
	Store          EnrichmentAdminStore
	FactProvider   func() provider.LLMProvider
	EntityProvider func() provider.LLMProvider
}

// EnrichmentQueueStatus is the response for GET /enrichment/queue.
type EnrichmentQueueStatus struct {
	Counts EnrichmentQueueCounts `json:"counts"`
	Items  []EnrichmentQueueItem `json:"items"`
	Paused bool                  `json:"paused"`
}

// EnrichmentQueueCounts contains the count of items in each queue state.
type EnrichmentQueueCounts struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Completed  int `json:"completed"`
	Failed     int `json:"failed"`
}

// EnrichmentQueueItem describes a single item in the enrichment queue.
type EnrichmentQueueItem struct {
	ID        uuid.UUID `json:"id"`
	MemoryID  uuid.UUID `json:"memory_id"`
	Status    string    `json:"status"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
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
		if sub == "retry" || sub == "pause" || sub == "test-prompt" {
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

	status, err := cfg.Store.QueueStatus(r.Context())
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
	Type        string `json:"type"`         // "fact" or "entity"
	Prompt      string `json:"prompt"`       // custom prompt text (optional; uses default if empty)
	SampleInput string `json:"sample_input"` // memory content to test against
}

// enrichmentTestPromptResponse is the response for POST /enrichment/test-prompt.
type enrichmentTestPromptResponse struct {
	Output   string `json:"output"`    // raw LLM output
	Parsed   any    `json:"parsed"`    // parsed structured data (facts or entities)
	Error    string `json:"error,omitempty"`
	LatencyMs int64 `json:"latency_ms"`
}

// Default prompts (must match service/extract.go).
const defaultFactPrompt = `You are a memory extraction system. Given the following text, extract discrete, standalone facts that would be useful to remember about the user or context in future conversations.

Rules:
- Each fact must be self-contained (understandable without the original text)
- Prefer specific over vague ("lives in Denver" not "lives somewhere in Colorado")
- Include temporal context when relevant ("as of March 2026")
- Assign confidence 0.0-1.0 based on how explicitly the fact was stated vs inferred
- Skip pleasantries, filler, and procedural content

Respond ONLY as a JSON array, no markdown fences, no preamble:
[{"fact": "...", "confidence": 0.95}, ...]`

const defaultEntityPrompt = `You are an entity and relationship extraction system. Given the following text, extract entities (people, organizations, technologies, places, concepts) and the relationships between them.

Rules:
- Each entity needs a name, a type, and optionally key properties
- Each relationship needs a source entity, target entity, relationship label, and temporal qualifier
- Temporal qualifiers: "current" (default), "as of <date>", "previously", "no longer"
- Normalize entity names
- Include relationship directionality

Respond ONLY as JSON, no markdown fences, no preamble:
{
  "entities": [{"name": "...", "type": "person|org|tech|place|concept", "properties": {}}],
  "relationships": [{"source": "...", "target": "...", "relation": "...", "temporal": "current"}]
}`

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

	if body.Type != "fact" && body.Type != "entity" {
		WriteError(w, ErrBadRequest("type must be 'fact' or 'entity'"))
		return
	}

	if strings.TrimSpace(body.SampleInput) == "" {
		WriteError(w, ErrBadRequest("sample_input is required"))
		return
	}

	// Select the provider and prompt based on type.
	var llmProvider provider.LLMProvider
	var systemPrompt string

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
		systemPrompt = defaultFactPrompt
		if strings.TrimSpace(body.Prompt) != "" {
			systemPrompt = body.Prompt
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
		systemPrompt = defaultEntityPrompt
		if strings.TrimSpace(body.Prompt) != "" {
			systemPrompt = body.Prompt
		}
	}

	start := time.Now()

	completionReq := &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: body.SampleInput},
		},
		MaxTokens:   2048,
		Temperature: 0.1,
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
