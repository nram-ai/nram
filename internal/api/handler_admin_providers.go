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
	UpdateProviderSlot(ctx context.Context, slot string, cfg ProviderSlotConfig) error
	ListOllamaModels(ctx context.Context, ollamaURL string) ([]OllamaModel, error)
	PullOllamaModel(ctx context.Context, model string, ollamaURL string) error
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

// ProviderSlotStatus describes the current state of a single provider slot.
type ProviderSlotStatus struct {
	Configured bool   `json:"configured"`
	Type       string `json:"type,omitempty"`
	URL        string `json:"url,omitempty"`
	Model      string `json:"model,omitempty"`
	Dimensions *int   `json:"dimensions,omitempty"`
	Status     string `json:"status"`
	LatencyMs  *int64 `json:"latency_ms,omitempty"`
}

// ProviderTestRequest is the request body for POST /providers/test.
type ProviderTestRequest struct {
	Slot   string             `json:"slot"`
	Config ProviderSlotConfig `json:"config"`
}

// ProviderSlotConfig describes the desired configuration for a provider slot.
type ProviderSlotConfig struct {
	Type       string `json:"type"`
	URL        string `json:"url"`
	APIKey     string `json:"api_key,omitempty"`
	Model      string `json:"model"`
	Dimensions *int   `json:"dimensions,omitempty"`
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

// handleProviderSlotUpdate handles PUT /providers/{slot} — updates a provider slot.
func handleProviderSlotUpdate(w http.ResponseWriter, r *http.Request, cfg ProviderAdminConfig, slot string) {
	if r.Method != http.MethodPut {
		WriteError(w, ErrBadRequest("method not allowed"))
		return
	}

	var slotCfg ProviderSlotConfig
	if err := json.NewDecoder(r.Body).Decode(&slotCfg); err != nil {
		WriteError(w, ErrBadRequest("invalid JSON body"))
		return
	}

	if slotCfg.Type == "" {
		WriteError(w, ErrBadRequest("type is required"))
		return
	}

	if err := cfg.Store.UpdateProviderSlot(r.Context(), slot, slotCfg); err != nil {
		WriteError(w, ErrInternal("failed to update provider slot"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
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
