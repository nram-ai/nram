package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/nram-ai/nram/internal/provider"
)

// ProviderAdminStore abstracts storage and provider management operations
// for the provider admin API.
type ProviderAdminStore interface {
	GetProviderConfig(ctx context.Context) (ProviderConfigResponse, error)
	TestProvider(ctx context.Context, req ProviderTestRequest) (*ProviderTestResult, error)
	UpdateProviderSlot(ctx context.Context, slot string, cfg ProviderSlotConfig, opts UpdateProviderSlotOpts) (*UpdateProviderSlotResult, error)
	ListOllamaModels(ctx context.Context, ollamaURL string, headers map[string]string) ([]OllamaModel, error)
	PullOllamaModel(ctx context.Context, model string, ollamaURL string, headers map[string]string) error
	ListProviderModels(ctx context.Context, url string, headers map[string]string) ([]string, error)
}

// UpdateProviderSlotOpts carries request-only options for an update that
// must not be persisted into the settings JSON. ConfirmInvalidate gates
// the destructive embedding-model switch cascade; without it the store
// returns a "needs confirmation" response and persists nothing.
type UpdateProviderSlotOpts struct {
	ConfirmInvalidate bool
	// ClearAPIKey forces the stored api_key to empty. Without it, a blank
	// incoming api_key preserves the previously stored key (the "leave blank
	// to keep" behavior); this flag is the explicit way to drop a key, e.g.
	// when switching a slot to header-only auth.
	ClearAPIKey bool
}

// UpdateProviderSlotResult describes the outcome of an update that may
// have triggered the embedding-model switch cascade. NeedsConfirmation
// is true when the user attempted to change the embedding model without
// providing confirm_invalidate=true; the response carries row counts
// the UI can show in its confirmation modal.
type UpdateProviderSlotResult struct {
	NeedsConfirmation   bool   `json:"needs_confirmation,omitempty"`
	OldModel            string `json:"old_model,omitempty"`
	NewModel            string `json:"new_model,omitempty"`
	MemoriesAffected    int64  `json:"memories_affected,omitempty"`
	EntitiesAffected    int64  `json:"entities_affected,omitempty"`
	MemoryJobsEnqueued  int64  `json:"memory_jobs_enqueued,omitempty"`
	EntityReembedQueued bool   `json:"entity_reembed_queued,omitempty"`
	// Warning is a non-fatal advisory the save still succeeded despite, e.g. the
	// configured model id is not served by an otherwise-reachable host. Empty when
	// there is nothing to warn about; rendered by the console after a successful save.
	Warning string `json:"warning,omitempty"`
}

// ProviderAdminConfig holds the dependencies for the provider admin handler.
type ProviderAdminConfig struct {
	Store ProviderAdminStore
}

// ProviderConfigResponse is the ordered list of every provider slot's status,
// one entry per provider.Slots in canonical order. Each entry carries its
// identity and metadata (slot/label/description/required) so the UI renders
// purely from this response without re-listing the slot set.
type ProviderConfigResponse []ProviderSlotStatus

// ProviderSlotStatus describes the current state of a single provider
// slot. Dimensions is the embedder's probed output dim
// (Registry.EmbeddingDim); nil for non-embedding slots and on probe
// failure.
//
// ContextWindow is the *effective* input length in tokens: for Ollama
// slots that means min(model GGUF max, runtime num_ctx); for OpenRouter
// it equals the model's reported context_length. Populated only for
// providers that expose it via API (Ollama via /api/show + /api/ps,
// OpenRouter via /models). Other providers leave it nil and the UI
// shows a "see provider docs" placeholder.
//
// ContextWindowMax is the model's GGUF-declared maximum, set only when
// it is strictly greater than ContextWindow (i.e., an Ollama-side
// num_ctx is the binding constraint). When present, the UI surfaces it
// as a muted "(model max N)" suffix so users see the headroom story.
type ProviderSlotStatus struct {
	// Identity + metadata, stamped from the canonical provider.SlotDef so the
	// UI renders order, labels, and help text without re-listing the slots.
	Slot        string `json:"slot"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Required    bool   `json:"required"`

	Configured bool   `json:"configured"`
	Type       string `json:"type,omitempty"`
	URL        string `json:"url,omitempty"`
	Model      string `json:"model,omitempty"`
	Dimensions *int   `json:"dimensions,omitempty"`
	// Dimension echoes the slot's opt-in configured output dimension (nil/0 =
	// native), so the editor can prefill the field. Distinct from Dimensions,
	// which is the probed effective dimension.
	Dimension        *int   `json:"dimension,omitempty"`
	ContextWindow    *int   `json:"context_window,omitempty"`
	ContextWindowMax *int   `json:"context_window_max,omitempty"`
	Timeout          *int   `json:"timeout,omitempty"`
	Status           string `json:"status"`
	LatencyMs        *int64 `json:"latency_ms,omitempty"`

	// APIKeySet reports whether a non-empty api_key is stored for this slot.
	// The key value itself is never returned; this lets the UI show a "key
	// configured" state and an honest "leave blank to keep" affordance.
	APIKeySet bool `json:"api_key_set"`
	// CustomHeaderKeys lists the names (sorted) of custom headers configured
	// for this slot. Values are never returned because they may carry secrets
	// (e.g. a fronting proxy's auth token); the UI shows the names so the user
	// can see which headers exist and re-edit them.
	CustomHeaderKeys []string `json:"custom_header_keys,omitempty"`
	// ExtraBody echoes the slot's configured extra_body verbatim. Unlike custom
	// headers, these values are not secret (e.g. chat_template_kwargs), so the
	// full map is returned so the UI can render and re-edit it.
	ExtraBody map[string]any `json:"extra_body,omitempty"`
	// DisableThinking echoes the slot's stored toggle so the UI initializes the
	// checkbox. Nil means unset (resolved to disabled); the UI defaults the
	// checkbox checked when this is nil or true.
	DisableThinking *bool `json:"disable_thinking,omitempty"`
	// RerankMethod echoes the reranker slot's auto-detected implementation
	// ("cross_encoder" or "judge") so the UI can show the detected mode. Empty
	// for non-reranker slots and for a reranker slot whose method was never probed.
	RerankMethod string `json:"rerank_method,omitempty"`
}

// ProviderTestRequest is the request body for POST /providers/test.
type ProviderTestRequest struct {
	Slot   string             `json:"slot"`
	Config ProviderSlotConfig `json:"config"`
}

// ProviderSlotConfig is the desired configuration for a provider slot.
// Dimension defaults to 0 = "use the model's native size" (discovered by
// Registry.EmbeddingDim); a positive value is an opt-in output dimension for a
// Matryoshka-capable embedding model. It is applied uniformly to the probe and to
// production embeds, so the measured dimension always matches what production
// writes and vectors never land in the wrong per-dim table.
type ProviderSlotConfig struct {
	Type    string `json:"type"`
	URL     string `json:"url"`
	APIKey  string `json:"api_key,omitempty"`
	Model   string `json:"model"`
	Timeout *int   `json:"timeout,omitempty"` // seconds, 0 = default (300s)
	// Dimension is the embedding slot's opt-in output dimension. 0/omitted means
	// the model's native size (the OpenAI "dimensions" request field is not sent).
	// A positive value requires a Matryoshka-capable model; sending it to a
	// fixed-dimension server (e.g. SGLang, vLLM) 400s the request. Embedding slot
	// only; inert elsewhere.
	Dimension *int `json:"dimension,omitempty"`
	// CustomHeaders are arbitrary HTTP headers attached to every outbound
	// request to this slot's provider host (inference, embeddings, health
	// pings, and the Ollama/OpenRouter auxiliary calls). Intended for proxies
	// or gateways between nram and the provider. On update, the map is the new
	// full set (omitted names are removed); a header whose value is blank keeps
	// its previously stored value (so masked headers survive a re-save).
	CustomHeaders map[string]string `json:"custom_headers,omitempty"`
	// ExtraBody is merged onto the top level of every OpenAI-compatible request
	// body (chat completions and embeddings), mirroring the OpenAI SDK's
	// extra_body. The primary use is chat_template_kwargs:{enable_thinking:false}
	// on a self-hosted vLLM/SGLang slot, but any key is allowed. On update the
	// map is the new full set (omitted keys are removed); values are not secret
	// and are returned verbatim on read.
	ExtraBody map[string]any `json:"extra_body,omitempty"`
	// DisableThinking toggles whether nram sends the provider-appropriate
	// "thinking off" knob on completions. A nil pointer means unset and resolves
	// to true (disabled), so existing slots and slots that never touch the toggle
	// keep skipping the reasoning pass. Honored for the Ollama/OpenRouter/vLLM/
	// SGLang/llama-server/Gemini types; inert for openai/anthropic/openai-compatible,
	// where an explicit disable would 400 on current models.
	DisableThinking *bool `json:"disable_thinking,omitempty"`
	// RerankMethod selects the reranker-slot implementation ("cross_encoder" or
	// "judge"). Reranker slot only. Auto-detected by the save/test path
	// (ProbeRerankMethod) and returned on read so the UI can show the detected
	// mode; an operator may set it explicitly to override the probe. Inert for
	// every other slot.
	RerankMethod string `json:"rerank_method,omitempty"`
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
		case "ollama/models":
			handleOllamaModels(w, r, cfg)
		case "ollama/pull":
			handleOllamaPull(w, r, cfg)
		case "models":
			handleProviderModels(w, r, cfg)
		default:
			// Any canonical slot name is a slot-update path.
			if provider.IsValidSlot(sub) {
				handleProviderSlotUpdate(w, r, cfg, sub)
			} else {
				WriteError(w, ErrBadRequest("unknown provider sub-path"))
			}
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

// forwardedHeaderPrefix is the request-header prefix the UI uses to forward
// in-progress custom provider headers on GET endpoints (where a JSON body is not
// available). Carrying them as request headers, rather than query parameters,
// keeps secret values out of URLs and access logs. The prefix is stripped and
// the remainder is the target header name.
const forwardedHeaderPrefix = "X-Nram-Provider-Header-"

// extractForwardedHeaders pulls the X-Nram-Provider-Header-* headers off the
// request and returns them as a target-header map (prefix stripped). Returns nil
// when none are present.
func extractForwardedHeaders(r *http.Request) map[string]string {
	var out map[string]string
	for name, values := range r.Header {
		canonical := http.CanonicalHeaderKey(name)
		if !strings.HasPrefix(canonical, forwardedHeaderPrefix) || len(values) == 0 {
			continue
		}
		target := strings.TrimPrefix(canonical, forwardedHeaderPrefix)
		if target == "" {
			continue
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[target] = values[0]
	}
	return out
}

// handleProviderConfig handles GET /providers: returns current provider config.
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

// handleProviderTest handles POST /providers/test: tests a provider connection.
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

	if !provider.IsValidSlot(req.Slot) {
		WriteError(w, ErrBadRequest("unknown slot: "+req.Slot))
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

	// Decode into a wrapper so confirm_invalidate / clear_api_key are captured
	// but do not pollute the persisted ProviderSlotConfig JSON.
	var body struct {
		ProviderSlotConfig
		ConfirmInvalidate bool `json:"confirm_invalidate,omitempty"`
		ClearAPIKey       bool `json:"clear_api_key,omitempty"`
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
		ClearAPIKey:       body.ClearAPIKey,
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

// handleOllamaModels handles GET /providers/ollama/models: lists Ollama models.
func handleOllamaModels(w http.ResponseWriter, r *http.Request, cfg ProviderAdminConfig) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	ollamaURL := r.URL.Query().Get("url")

	models, err := cfg.Store.ListOllamaModels(r.Context(), ollamaURL, extractForwardedHeaders(r))
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

// handleProviderModels handles GET /providers/models: lists the models any
// OpenAI-compatible server (e.g. vLLM, SGLang) reports at /v1/models, so the UI
// can detect the served model id instead of requiring the operator to type it.
// The target URL comes from the url query param; in-progress custom headers are
// forwarded as request headers (see forwardedHeaderPrefix).
func handleProviderModels(w http.ResponseWriter, r *http.Request, cfg ProviderAdminConfig) {
	if r.Method != http.MethodGet {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	url := r.URL.Query().Get("url")
	if url == "" {
		WriteError(w, ErrBadRequest("url is required"))
		return
	}

	models, err := cfg.Store.ListProviderModels(r.Context(), url, extractForwardedHeaders(r))
	if err != nil {
		WriteError(w, ErrInternal("failed to list provider models: "+err.Error()))
		return
	}

	if models == nil {
		writeJSON(w, http.StatusOK, []string{})
		return
	}

	writeJSON(w, http.StatusOK, models)
}

// handleOllamaPull handles POST /providers/ollama/pull: pulls a model on Ollama.
func handleOllamaPull(w http.ResponseWriter, r *http.Request, cfg ProviderAdminConfig) {
	if r.Method != http.MethodPost {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var body struct {
		Model         string            `json:"model"`
		URL           string            `json:"url"`
		CustomHeaders map[string]string `json:"custom_headers,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	if body.Model == "" {
		WriteError(w, ErrBadRequest("model is required"))
		return
	}

	if err := cfg.Store.PullOllamaModel(r.Context(), body.Model, body.URL, body.CustomHeaders); err != nil {
		WriteError(w, ErrInternal("failed to pull ollama model: "+err.Error()))
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"status": "accepted",
		"model":  body.Model,
	})
}
