package api

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/enrichment"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
)

// previewAugmentParseErrorMessage is the user-facing string surfaced when
// ParseQueryAugmentResponse refuses the model output. Kept fixed so internal
// Go type names (e.g. "[]string") never reach the operator. The underlying
// error is logged server-side for diagnostics.
const previewAugmentParseErrorMessage = "The model returned malformed output that could not be parsed as a query list. Re-run the preview, or adjust the prompt template if the failure is persistent."

// previewAugmentLLMErrorMessage mirrors the parse-error message for the
// upstream LLM-call path. The raw error often contains transport details
// (endpoint URLs, timeouts) that don't belong in the preview surface.
const previewAugmentLLMErrorMessage = "The model did not respond. Check the configured provider and try again."

// QueryAugmentPromptResolver returns the operator-edited prompt template (or
// the registered default if no override is set). Factored out so the handler
// does not depend directly on the SettingsService concrete type.
type QueryAugmentPromptResolver interface {
	Resolve(ctx context.Context, key, scope string) (string, error)
}


// MemoryPreviewAugmentConfig wires the per-memory preview endpoint. All
// dependencies are required; nil checks in the handler surface a 503 rather
// than a panic so deployments without augmentation wired report cleanly.
type MemoryPreviewAugmentConfig struct {
	Memories     MemoryLister
	Projects     ProjectGetter
	FactProvider func() provider.LLMProvider
	Settings     QueryAugmentPromptResolver
}

// MemoryPreviewAugmentResponse is the JSON envelope for the preview endpoint.
// Mirrors the Prompt-Templates page test response so the UI components can be
// shared between the two surfaces.
type MemoryPreviewAugmentResponse struct {
	Queries          []string `json:"queries"`
	AugmentedContent string   `json:"augmented_content"`
	RenderedPrompt   string   `json:"rendered_prompt"`
	Model            string   `json:"model"`
	LatencyMs        int64    `json:"latency_ms"`
	TruncatedBytes   int      `json:"truncated_bytes"`
	Error            string   `json:"error,omitempty"`
}

// NewMemoryPreviewAugmentHandler serves
// POST /v1/projects/{project_id}/memories/{id}/preview-augmentation
// running the augmentation phase against the memory's actual content without
// persisting. Project-scoped: caller must already have read access to the
// project (the chi route is mounted under the per-project tenancy middleware).
func NewMemoryPreviewAugmentHandler(cfg MemoryPreviewAugmentConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			WriteError(w, ErrBadRequest("method not allowed"))
			return
		}
		if cfg.Memories == nil || cfg.Projects == nil || cfg.FactProvider == nil || cfg.Settings == nil {
			http.Error(w, "preview-augmentation not available in this deployment", http.StatusServiceUnavailable)
			return
		}

		projectIDStr := chi.URLParam(r, "project_id")
		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid project_id: must be a valid UUID"))
			return
		}
		memoryIDStr := chi.URLParam(r, "id")
		memoryID, err := uuid.Parse(memoryIDStr)
		if err != nil {
			WriteError(w, ErrBadRequest("invalid id: must be a valid UUID"))
			return
		}

		project, err := cfg.Projects.GetByID(r.Context(), projectID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				WriteError(w, ErrNotFound("project not found"))
				return
			}
			WriteError(w, ErrInternal("failed to look up project"))
			return
		}
		mem, err := cfg.Memories.GetByID(r.Context(), memoryID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				WriteError(w, ErrNotFound("memory not found"))
				return
			}
			WriteError(w, ErrInternal("failed to look up memory"))
			return
		}
		if mem.NamespaceID != project.NamespaceID {
			WriteError(w, ErrNotFound("memory not found"))
			return
		}

		// Resolve prompt, count, and model from settings; fall back to
		// registered defaults so an operator who hasn't edited any of them
		// still gets the same shape the runtime phase produces.
		prompt, _ := cfg.Settings.Resolve(r.Context(), service.SettingQueryAugmentPrompt, "global")
		if prompt == "" {
			prompt, _ = service.GetDefault(service.SettingQueryAugmentPrompt)
		}
		count := service.GetDefaultInt(service.SettingQueryAugmentCount)
		if v, err := cfg.Settings.Resolve(r.Context(), service.SettingQueryAugmentCount, "global"); err == nil && v != "" {
			// Soft parse: fall back to default on garbage so the preview still
			// runs against a sensible N.
			if parsed, perr := strconv.Atoi(v); perr == nil && parsed > 0 {
				count = parsed
			}
		}
		// Clamp count to the schema-declared range so a stored setting that
		// bypassed the write-path validation (direct DB edit, future schema
		// drift) cannot blow up the prompt size at request time.
		if count > enrichment.QueryAugmentMaxCount {
			count = enrichment.QueryAugmentMaxCount
		}
		// Resolve the optional per-feature model override so preview matches
		// what the runtime phase would emit. Empty falls back to whatever
		// model the fact provider was registered with.
		modelOverride, _ := cfg.Settings.Resolve(r.Context(), service.SettingQueryAugmentModel, "global")

		// Resolve the completion-token cap from the same settings key the
		// worker phase reads, so the preview matches what the live ingestion
		// path would observe. Soft-parse: garbage in the stored value falls
		// back to the registered default rather than 503-ing the request.
		maxTokens := enrichment.QueryAugmentDefaultMaxTokens
		if v, err := cfg.Settings.Resolve(r.Context(), service.SettingQueryAugmentMaxTokens, "global"); err == nil && v != "" {
			if parsed, perr := strconv.Atoi(v); perr == nil && parsed > 0 {
				maxTokens = parsed
			}
		}

		llm := cfg.FactProvider()
		if llm == nil {
			http.Error(w, "fact provider not available", http.StatusServiceUnavailable)
			return
		}

		rendered := enrichment.RenderQueryAugmentPrompt(prompt, mem.Content, count)
		start := time.Now()
		resp, err := llm.Complete(provider.WithOperation(r.Context(), provider.OperationQueryAugment), &provider.CompletionRequest{
			Messages:  []provider.Message{{Role: "user", Content: rendered}},
			Model:     modelOverride,
			MaxTokens: maxTokens,
			JSONMode:  true,
		})
		latency := time.Since(start).Milliseconds()
		if err != nil {
			slog.Warn("preview_augment: llm call failed",
				"project", projectID,
				"memory", memoryID,
				"err", err,
				"llm_latency_ms", latency)
			writeJSON(w, http.StatusOK, MemoryPreviewAugmentResponse{
				RenderedPrompt: rendered,
				LatencyMs:      latency,
				Error:          previewAugmentLLMErrorMessage,
			})
			return
		}
		queries, perr := enrichment.ParseQueryAugmentResponse(resp.Content)
		if perr != nil {
			// Surface the raw LLM body and finish_reason so an operator can
			// diagnose without re-running the preview. finish_reason="length"
			// alongside "unexpected end of JSON input" is the canonical
			// truncation signal and points at enrichment.query_augment.max_tokens.
			slog.Warn("preview_augment: parse failed",
				"project", projectID,
				"memory", memoryID,
				"err", perr,
				"raw_len", len(resp.Content),
				"finish_reason", resp.FinishReason,
				"model", resp.Model,
				"raw", resp.Content,
				"llm_latency_ms", latency)
			writeJSON(w, http.StatusOK, MemoryPreviewAugmentResponse{
				RenderedPrompt: rendered,
				Model:          resp.Model,
				LatencyMs:      latency,
				Error:          previewAugmentParseErrorMessage,
			})
			return
		}
		augmented, trimmed := enrichment.BuildAugmentedInput(queries, mem.Content, 0)
		writeJSON(w, http.StatusOK, MemoryPreviewAugmentResponse{
			Queries:          queries,
			AugmentedContent: augmented,
			RenderedPrompt:   rendered,
			Model:            resp.Model,
			LatencyMs:        latency,
			TruncatedBytes:   trimmed,
		})
	}
}

