package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// emptyPromptResolver returns "" for every key so the handler falls back to the
// registered defaults, matching a fresh deployment.
type emptyPromptResolver struct{}

func (emptyPromptResolver) Resolve(_ context.Context, _ string, _ string) (string, error) {
	return "", nil
}

// TestMemoryPreviewAugment_GuardsSystemPrompt proves the per-memory preview
// endpoint now sends a GuardedSystem-wrapped system prompt (the untrusted-data
// directive) rather than the bare template, and that the operator-facing
// RenderedPrompt reflects exactly what the model saw. Before the fix the user
// was fenced but the system was passed bare (the half-application bug).
func TestMemoryPreviewAugment_GuardsSystemPrompt(t *testing.T) {
	nsID := uuid.New()
	projID := uuid.New()
	memID := uuid.New()
	mem := &model.Memory{ID: memID, NamespaceID: nsID, Content: "some memory body"}

	capLLM := &capturingLLMProvider{content: `["alpha","beta"]`}
	cfg := MemoryPreviewAugmentConfig{
		Memories: &mockMemoryLister{
			getFn: func(_ context.Context, _ uuid.UUID) (*model.Memory, error) { return mem, nil },
		},
		Projects:             &mockProjectGetter{project: &model.Project{ID: projID, Slug: "p", NamespaceID: nsID}},
		QueryAugmentProvider: func() provider.LLMProvider { return capLLM },
		Settings:             emptyPromptResolver{},
	}

	r := chi.NewRouter()
	r.Post("/v1/projects/{project_id}/memories/{id}/preview-augmentation", NewMemoryPreviewAugmentHandler(cfg))

	path := "/v1/projects/" + projID.String() + "/memories/" + memID.String() + "/preview-augmentation"
	req := httptest.NewRequest(http.MethodPost, path, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}

	// The system message the model actually saw must carry the directive.
	if len(capLLM.gotMessages) < 2 {
		t.Fatalf("expected a two-message request, got %+v", capLLM.gotMessages)
	}
	if !strings.HasPrefix(capLLM.gotMessages[0].Content, provider.UntrustedDataDirective) {
		t.Errorf("system message not guarded: %q", capLLM.gotMessages[0].Content)
	}

	// The operator-facing RenderedPrompt must reflect the guarded system.
	var resp MemoryPreviewAugmentResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.HasPrefix(resp.RenderedPrompt, provider.UntrustedDataDirective) {
		t.Errorf("RenderedPrompt not guarded: %q", resp.RenderedPrompt)
	}
}
