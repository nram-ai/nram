package mcp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// mcpAskSource is one cited/seed memory in the ask response (lean provenance).
type mcpAskSource struct {
	MemoryID    string `json:"memory_id"`
	ProjectSlug string `json:"project_slug"`
	// Score is the source's absolute vector cosine to the query. Omitted for
	// sources that entered via graph/sibling expansion (no direct query match),
	// rather than the misleading 0 the old fixed-float field reported.
	Score *float64 `json:"score,omitempty"`
	// Citation is the footnote number ([1], [2], …) this source carries inline
	// in the answer. Omitted (0) on the uncited fallback.
	Citation int `json:"citation,omitempty"`
}

// mcpAskSynthesisMeta is the minimal synthesis metadata returned with an answer.
type mcpAskSynthesisMeta struct {
	LatencyMs        int64 `json:"latency_ms"`
	NeighborhoodSize int   `json:"neighborhood_size"`
	SynthesisFailed  bool  `json:"synthesis_failed,omitempty"`
}

// mcpAskResponse is the wire shape of the ask tool result. Kept in sync with the
// registered output schema (schemaFor[mcpAskResponse]).
type mcpAskResponse struct {
	Answer  string         `json:"answer"`
	Sources []mcpAskSource `json:"sources"`
	// The tag is jsonschema_description rather than a description= key inside
	// the comma-split jsonschema tag, so the text may contain commas.
	Confidence    float64             `json:"confidence" jsonschema_description:"A grounding / evidence-strength signal in [0,1], not a correctness or faithfulness score. It reports how strongly the sources the answer cited match the query, derived from their calibrated vector cosines. It does not report whether the answer is right: an answer that draws a wrong conclusion from a strong-matching source still scores high, so read the cited sources before acting on a high value. Zero when the answer cited no source: it was ungrounded, said \"Not in neighborhood.\", or synthesis failed."`
	SynthesisMeta mcpAskSynthesisMeta `json:"synthesis_meta"`
}

// RegisterAskTool registers the ask synthesis MCP tool. The tool is always
// registered (gated only on the Ask service being wired) so the tool-list
// filter in NewServer can reveal or hide it live based on the ask.enabled
// feature flag without a server restart. The handler also guards, so an
// out-of-band call while the flag is off is rejected.
func RegisterAskTool(s *Server) {
	if s.Deps().Ask == nil {
		return
	}
	tool := mcp.NewTool("ask",
		mcp.WithTitleAnnotation("Ask"),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		mcp.WithRawOutputSchema(schemaFor[mcpAskResponse]()),
		mcp.WithDescription("Ask a question and get one synthesized answer over your stored memories, with the source memories it drew on. Unlike recall (which returns a ranked list), ask runs the retrieval for you and writes a single grounded answer with footnote citations ([1], [2], …) that map to the returned sources (each source carries its citation number). Omit project for a wide synthesis across all of your projects (plus global and about_me); pass a project slug to scope to that project plus global and about_me. Single-shot: each call is one question, not a conversation. Costs a model call, so prefer recall for simple lookups and use ask when you want the answer composed for you. The confidence returned with the answer is a grounding signal, not a correctness score: it reports how strongly the cited sources match the query, so a high value does not mean the answer is right."),
		mcp.WithString("query", mcp.Required(), mcp.Description("The question to answer from your memories.")),
		mcp.WithString("project", mcp.Description("Optional project slug. Omit for a wide cross-project synthesis over all your projects; supply a slug to scope to that project + global + about_me.")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleAsk(ctx, s, request)
	})
}

func handleAsk(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return mcp.NewToolResultError("no HTTP request in context"), nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return mcp.NewToolResultError("authentication required"), nil
	}

	deps := s.Deps()

	// Handler-side feature-flag guard. The tool-list filter hides ask when the
	// flag is off; this rejects any out-of-band invocation while off.
	if deps.Ask == nil || !deps.Settings.ResolveBoolWithDefault(ctx, service.SettingAskEnabled, "global") {
		return mcp.NewToolResultError("the ask tool is disabled; enable it in settings (ask.enabled)"), nil
	}

	args := request.GetArguments()
	query, ok := args["query"].(string)
	if !ok || strings.TrimSpace(query) == "" {
		return mcp.NewToolResultError("query is required"), nil
	}
	projectSlug, _ := args["project"].(string)
	projectSlug = strings.TrimSpace(projectSlug)

	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	// Share-bearer callers are scoped strictly to the projects their share
	// grants read access on. The aperture never includes the owner's global or
	// about_me tiers or any ungranted project (enforced in AskService).
	var shareScoped bool
	var shareProjectIDs []uuid.UUID
	if ac.ShareTokenID != nil {
		shareScoped = true
		for _, g := range ac.ShareGrants {
			if g.Permission.Allows(model.SharePermissionRead) {
				shareProjectIDs = append(shareProjectIDs, g.ProjectID)
			}
		}
		if len(shareProjectIDs) == 0 {
			return mcp.NewToolResultError("this share does not grant read access to any project"), nil
		}
	}

	uid := ac.UserID
	resp, err := deps.Ask.Ask(ctx, &service.AskRequest{
		Query:            query,
		ProjectSlug:      projectSlug,
		OwnerNamespaceID: user.NamespaceID,
		OrgID:            ac.OrgID,
		UserID:           &uid,
		APIKeyID:         ac.APIKeyID,
		ShareScoped:      shareScoped,
		ShareProjectIDs:  shareProjectIDs,
	})
	if err != nil {
		if errors.Is(err, service.ErrAskProviderUnconfigured) {
			return mcp.NewToolResultError("the ask synthesis provider is not configured; set up the Ask Synthesis provider slot"), nil
		}
		return mcp.NewToolResultError(fmt.Sprintf("ask failed: %v", err)), nil
	}

	out := mcpAskResponse{
		Answer:     resp.Answer,
		Confidence: resp.Confidence,
		SynthesisMeta: mcpAskSynthesisMeta{
			LatencyMs:        resp.SynthesisMeta.LatencyMs,
			NeighborhoodSize: resp.SynthesisMeta.NeighborhoodSize,
			SynthesisFailed:  resp.SynthesisMeta.SynthesisFailed,
		},
	}
	out.Sources = make([]mcpAskSource, 0, len(resp.Sources))
	for _, src := range resp.Sources {
		out.Sources = append(out.Sources, mcpAskSource{
			MemoryID:    src.MemoryID.String(),
			ProjectSlug: src.ProjectSlug,
			Score:       src.Score,
			Citation:    src.Citation,
		})
	}

	budget := mcpBudgetBytes(ctx, s.deps.Settings)
	return wrapToolResult(s.deps.Metrics, "ask", budget, out, nil)
}
