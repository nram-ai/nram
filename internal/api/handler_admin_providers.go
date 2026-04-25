package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// ProviderAdminStore abstracts storage and provider management operations
// for the provider admin API.
type ProviderAdminStore interface {
	GetProviderConfig(ctx context.Context) (*ProviderConfigResponse, error)
	TestProvider(ctx context.Context, req ProviderTestRequest) (*ProviderTestResult, error)
	UpdateProviderSlot(ctx context.Context, slot string, cfg ProviderSlotConfig, opts UpdateProviderSlotOpts) (*UpdateProviderSlotResult, error)
	ListOllamaModels(ctx context.Context, ollamaURL string) ([]OllamaModel, error)
	PullOllamaModel(ctx context.Context, model string, ollamaURL string) error
}

// UpdateProviderSlotOpts carries request-only options for an update that
// must not be persisted into the settings JSON. ConfirmInvalidate gates
// the destructive embedding-model switch cascade — without it the store
// returns a "needs confirmation" response and persists nothing.
type UpdateProviderSlotOpts struct {
	ConfirmInvalidate bool
}

// UpdateProviderSlotResult describes the outcome of an update that may
// have triggered the embedding-model switch cascade. NeedsConfirmation
// is true when the user attempted to change the embedding model without
// providing confirm_invalidate=true; the response carries row counts
// the UI can show in its confirmation modal.
type UpdateProviderSlotResult struct {
	NeedsConfirmation     bool   `json:"needs_confirmation,omitempty"`
	OldModel              string `json:"old_model,omitempty"`
	NewModel              string `json:"new_model,omitempty"`
	MemoriesAffected      int64  `json:"memories_affected,omitempty"`
	EntitiesAffected      int64  `json:"entities_affected,omitempty"`
	MemoryJobsEnqueued    int64  `json:"memory_jobs_enqueued,omitempty"`
	EntityReembedQueued   bool   `json:"entity_reembed_queued,omitempty"`
}

// ProviderAdminConfig holds the dependencies for the provider admin handler.
type ProviderAdminConfig struct {
	Store ProviderAdminStore
}

// ProviderConfigResponse describes the current configuration of all three
// provider slots (embedding, fact extraction, entity extraction).
type ProviderConfigResponse struct {
	Embedding ProviderSlotStatus `json:"embedding"`
	Fact      ProviderSlotStatus `json:"fact"`
	Entity    ProviderSlotStatus `json:"entity"`
}

// ProviderSlotStatus describes the current state of a single provider
// slot. Dimensions is the embedder's probed output dim (Registry.EmbeddingDim);
// nil for non-embedding slots and on probe failure.
type ProviderSlotStatus struct {
	Configured bool   `json:"configured"`
	Type       string `json:"type,omitempty"`
	URL        string `json:"url,omitempty"`
	Model      string `json:"model,omitempty"`
	Dimensions *int   `json:"dimensions,omitempty"`
	Timeout    *int   `json:"timeout,omitempty"`
	Status     string `json:"status"`
	LatencyMs  *int64 `json:"latency_ms,omitempty"`
}

// ProviderTestRequest is the request body for POST /providers/test.
type ProviderTestRequest struct {
	Slot   string             `json:"slot"`
	Config ProviderSlotConfig `json:"config"`
}

// ProviderSlotConfig is the desired configuration for a provider slot.
// Dimensions is intentionally absent — it's discovered by Registry.EmbeddingDim,
// not user-configurable (a mismatched dim sends vectors to the wrong
// per-dim table and recall silently breaks).
type ProviderSlotConfig struct {
	Type    string `json:"type"`
	URL     string `json:"url"`
	APIKey  string `json:"api_key,omitempty"`
	Model   string `json:"model"`
	Timeout *int   `json:"timeout,omitempty"` // seconds, 0 = default (120s)
}

// ProviderTestResult is the response body for POST /providers/test.
type ProviderTestResult struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	LatencyMs int64  `json:"latency_ms"`
}

// OllamaModel describes an Ollama model available on the instance.
type OllamaModel struct {
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	ModifiedAt string `json:"modified_at"`
}

// NewAdminProvidersHandler returns an http.HandlerFunc that dispatches provider
// admin requests based on method and sub-path under /providers.
func NewAdminProvidersHandler(cfg ProviderAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract sub-path after "/providers".
		sub := extractProviderSubPath(r.URL.Path)

		switch sub {
		case "":
			handleProviderConfig(w, r, cfg)
		case "test":
			handleProviderTest(w, r, cfg)
		case "embedding":
			handleProviderSlotUpdate(w, r, cfg, "embedding")
		case "fact":
			handleProviderSlotUpdate(w, r, cfg, "fact")
		case "entity":
			handleProviderSlotUpdate(w, r, cfg, "entity")
		case "ollama/models":
			handleOllamaModels(w, r, cfg)
		case "ollama/pull":
			handleOllamaPull(w, r, cfg)
		default:
			WriteError(w, ErrBadRequest("unknown provider sub-path"))
		}
	}
}

// extractProviderSubPath returns the portion of the URL path after "/providers".
// For example, "/v1/admin/providers/test" returns "test".
func extractProviderSubPath(path string) string {
	const marker = "/providers"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return ""
	}
	rest := path[idx+len(marker):]
	rest = strings.TrimPrefix(rest, "/")
	return rest
}

// handleProviderConfig handles GET /providers — returns current provider config.
func handleProviderConfig(w http.ResponseWriter, r *http.Request, cfg ProviderAdminConfig) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	resp, err := cfg.Store.GetProviderConfig(r.Context())
	if err != nil {
		WriteError(w, ErrInternal("failed to get provider config"))
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleProviderTest handles POST /providers/test — tests a provider connection.
func handleProviderTest(w http.ResponseWriter, r *http.Request, cfg ProviderAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var req ProviderTestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	if req.Slot != "embedding" && req.Slot != "fact" && req.Slot != "entity" {
		WriteError(w, ErrBadRequest("slot must be one of: embedding, fact, entity"))
		return
	}

	result, err := cfg.Store.TestProvider(r.Context(), req)
	if err != nil {
		WriteError(w, ErrInternal("failed to test provider"))
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// handleProviderSlotUpdate handles PUT /providers/{slot}. An embedding-model
// change is gated on confirm_invalidate=true; without it the handler
// returns 409 Conflict with the row counts the UI shows in its modal.
func handleProviderSlotUpdate(w http.ResponseWriter, r *http.Request, cfg ProviderAdminConfig, slot string) {
	if r.Method != http.MethodPut {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	// Decode into a wrapper so confirm_invalidate is captured but does not
	// pollute the persisted ProviderSlotConfig JSON.
	var body struct {
		ProviderSlotConfig
		ConfirmInvalidate bool `json:"confirm_invalidate,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	if body.Type == "" {
		WriteError(w, ErrBadRequest("type is required"))
		return
	}

	result, err := cfg.Store.UpdateProviderSlot(r.Context(), slot, body.ProviderSlotConfig, UpdateProviderSlotOpts{
		ConfirmInvalidate: body.ConfirmInvalidate,
	})
	if err != nil {
		WriteError(w, ErrInternal("failed to update provider slot: "+err.Error()))
		return
	}

	if result != nil && result.NeedsConfirmation {
		// 409 Conflict signals the UI to show the destructive-action modal
		// and re-submit with confirm_invalidate=true.
		writeJSON(w, http.StatusConflict, result)
		return
	}

	if result == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleOllamaModels handles GET /providers/ollama/models — lists Ollama models.
func handleOllamaModels(w http.ResponseWriter, r *http.Request, cfg ProviderAdminConfig) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	ollamaURL := r.URL.Query().Get("url")

	models, err := cfg.Store.ListOllamaModels(r.Context(), ollamaURL)
	if err != nil {
		WriteError(w, ErrInternal("failed to list ollama models: "+err.Error()))
		return
	}

	if models == nil {
		writeJSON(w, http.StatusOK, []struct{}{})
		return
	}

	writeJSON(w, http.StatusOK, models)
}

// handleOllamaPull handles POST /providers/ollama/pull — pulls a model on Ollama.
func handleOllamaPull(w http.ResponseWriter, r *http.Request, cfg ProviderAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var body struct {
		Model string `json:"model"`
		URL   string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	if body.Model == "" {
		WriteError(w, ErrBadRequest("model is required"))
		return
	}

	if err := cfg.Store.PullOllamaModel(r.Context(), body.Model, body.URL); err != nil {
		WriteError(w, ErrInternal("failed to pull ollama model: "+err.Error()))
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"status": "accepted",
		"model":  body.Model,
	})
}
