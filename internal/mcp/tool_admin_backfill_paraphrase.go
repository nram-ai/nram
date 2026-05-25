package mcp

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/service"
)

// RegisterAdminBackfillParaphraseTool registers the admin-only
// memory_backfill_extracted_fact_paraphrase MCP tool. The tool enqueues
// paraphrase-guard sweep jobs onto the enrichment queue, one per parent
// memory that has extracted-fact children. Operators use it to clean up
// near-duplicate child memories produced by enrichment before the
// pre-insert paraphrase guard was added.
func RegisterAdminBackfillParaphraseTool(s *Server) {
	tool := mcp.NewTool("memory_backfill_extracted_fact_paraphrase",
		mcp.WithDescription("Admin only. Enqueue paraphrase-guard sweep jobs that merge near-duplicate extracted-fact children into their parents and supersede the children. Omit project to scan the whole deployment; pass dry_run: true to count candidates without enqueueing. Limit caps the number of parents enqueued in one call."),
		mcp.WithString("project", mcp.Description("Project slug (omit to scan all projects in the deployment)")),
		mcp.WithBoolean("dry_run", mcp.Description("When true, return candidate_count without enqueueing any jobs")),
		mcp.WithNumber("limit", mcp.Description("Cap on candidate parents enqueued in this call. 0 = no cap. Default 0.")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleAdminBackfillParaphrase(ctx, s, request)
	})
}

func handleAdminBackfillParaphrase(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return mcp.NewToolResultError("no HTTP request in context"), nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return mcp.NewToolResultError("authentication required"), nil
	}
	if ac.Role != auth.RoleAdministrator {
		return mcp.NewToolResultError("forbidden: administrator required"), nil
	}
	deps := s.Deps()
	if deps.Enrich == nil {
		return mcp.NewToolResultError("backfill service not configured"), nil
	}

	args := request.GetArguments()
	projectSlug, _ := args["project"].(string)
	dryRun, _ := args["dry_run"].(bool)
	limit := 0
	if v, ok := args["limit"].(float64); ok {
		limit = int(v)
	}

	var projectID uuid.UUID
	if projectSlug != "" {
		if deps.ProjectRepo == nil || deps.UserRepo == nil {
			return mcp.NewToolResultError("project lookup not configured"), nil
		}
		user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
		}
		p, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
		if err != nil || p == nil {
			return mcp.NewToolResultError(fmt.Sprintf("project %q not found", projectSlug)), nil
		}
		projectID = p.ID
	}

	resp, err := deps.Enrich.BackfillExtractedFactParaphrase(ctx, &service.BackfillExtractedFactParaphraseRequest{
		ProjectID: projectID,
		DryRun:    dryRun,
		Limit:     limit,
	})
	if err != nil {
		return mcp.NewToolResultError("backfill failed: " + err.Error()), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf(
		"candidate_count=%d enqueued=%d dry_run=%v latency_ms=%d",
		resp.CandidateCount, resp.Enqueued, resp.DryRun, resp.LatencyMs,
	)), nil
}
