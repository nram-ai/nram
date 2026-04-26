package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/storage"
)

const (
	listDefaultLimit = 50
	listMaxLimit     = 200
)

// listMemoryItem applies the same dream-lineage hoisting as mcpRecallMemory
// (source_memory_ids → derived_from) so the two outputs stay consistent.
// project_slug disambiguates per-item because list crosses the requested
// project's namespace and the global namespace.
type listMemoryItem struct {
	ID          uuid.UUID       `json:"id"`
	ProjectSlug string          `json:"project_slug"`
	Content     string          `json:"content"`
	Source      *string         `json:"source,omitempty"`
	Tags        []string        `json:"tags"`
	DerivedFrom []uuid.UUID     `json:"derived_from,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// listMemoryResponse is the paginated response envelope for memory_list.
type listMemoryResponse struct {
	Data       []listMemoryItem `json:"data"`
	Pagination model.Pagination `json:"pagination"`
}

// RegisterListTool registers the memory_list MCP tool on the given server.
func RegisterListTool(s *Server) {
	tool := mcp.NewTool("memory_list",
		mcp.WithDescription("List memories in a project with pagination. Use to browse stored memories when you need an overview rather than a semantic search. Returns memories ordered by most recently created."),
		mcp.WithString("project", mcp.Description("Project slug. Lists this project + global. Omit to list only the global project")),
		mcp.WithNumber("limit", mcp.Description("Maximum number of memories to return (default 50, max 200)")),
		mcp.WithNumber("offset", mcp.Description("Number of memories to skip for pagination (default 0)")),
		mcp.WithBoolean(includeSupersededArg, mcp.Description(includeSupersededDesc)),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleMemoryList(ctx, s, request)
	})
}

func handleMemoryList(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return mcp.NewToolResultError("no HTTP request in context"), nil
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return mcp.NewToolResultError("authentication required"), nil
	}

	args := request.GetArguments()

	projectSlug, _ := args["project"].(string)
	projectSlug = strings.TrimSpace(projectSlug)
	if projectSlug == "" {
		projectSlug = "global"
	}

	limit := listDefaultLimit
	if v, ok := args["limit"].(float64); ok && v > 0 {
		limit = int(v)
	}
	if limit > listMaxLimit {
		limit = listMaxLimit
	}

	offset := 0
	if v, ok := args["offset"].(float64); ok && v >= 0 {
		offset = int(v)
	}

	filters := storage.MemoryListFilters{HideSuperseded: !argBool(args, includeSupersededArg, false)}

	deps := s.Deps()

	user, err := deps.UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err)), nil
	}

	project, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, projectSlug)
	if err != nil {
		return mcp.NewToolResultError("project not found"), nil
	}

	// Collect namespaces to query: always the specified project, plus global
	// when a non-global project is specified (consistent with memory_recall).
	namespaces := []uuid.UUID{project.NamespaceID}
	nsIDToSlug := map[uuid.UUID]string{project.NamespaceID: projectSlug}
	if projectSlug != "global" {
		if gp, err := deps.ProjectRepo.GetBySlug(ctx, user.NamespaceID, "global"); err == nil && gp != nil {
			namespaces = append(namespaces, gp.NamespaceID)
			nsIDToSlug[gp.NamespaceID] = "global"
		}
	}

	// Aggregate counts and memories across all namespaces.
	total := 0
	for _, nsID := range namespaces {
		c, err := deps.MemoryLister.CountByNamespaceFiltered(ctx, nsID, filters)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("failed to count memories: %v", err)), nil
		}
		total += c
	}

	var memories []model.Memory
	remaining := limit
	currentOffset := offset
	for _, nsID := range namespaces {
		if remaining <= 0 {
			break
		}
		nsCount, err := deps.MemoryLister.CountByNamespaceFiltered(ctx, nsID, filters)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("failed to count memories: %v", err)), nil
		}
		if currentOffset >= nsCount {
			currentOffset -= nsCount
			continue
		}
		batch, err := deps.MemoryLister.ListByNamespaceFiltered(ctx, nsID, filters, remaining, currentOffset)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("failed to list memories: %v", err)), nil
		}
		memories = append(memories, batch...)
		remaining -= len(batch)
		currentOffset = 0
	}

	items := make([]listMemoryItem, 0, len(memories))
	for _, m := range memories {
		derived, meta := extractDerivedFrom(m.Metadata, projectionOpts{})
		items = append(items, listMemoryItem{
			ID:          m.ID,
			ProjectSlug: nsIDToSlug[m.NamespaceID],
			Content:     m.Content,
			Source:      m.Source,
			Tags:        m.Tags,
			DerivedFrom: derived,
			Metadata:    meta,
			CreatedAt:   m.CreatedAt,
			UpdatedAt:   m.UpdatedAt,
		})
	}

	resp := listMemoryResponse{
		Data: items,
		Pagination: model.Pagination{
			Total:  total,
			Limit:  limit,
			Offset: offset,
		},
	}

	return wrapToolResult(resp, newListReducer(resp))
}
