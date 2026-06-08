package mcp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// mcpProceduralEntry is the wire projection for a procedural tier entry.
type mcpProceduralEntry struct {
	ID        uuid.UUID `json:"id"`
	Content   string    `json:"content"`
	Title     string    `json:"title,omitempty"`
	Category  string    `json:"category,omitempty"`
	Tags      []string  `json:"tags,omitempty"`
	Priority  int       `json:"priority"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func buildMCPProceduralEntry(e *model.ProceduralEntry) mcpProceduralEntry {
	return mcpProceduralEntry{
		ID:        e.ID,
		Content:   e.Content,
		Title:     e.Title,
		Category:  e.Category,
		Tags:      e.Tags,
		Priority:  e.Priority,
		Enabled:   e.Enabled,
		CreatedAt: e.CreatedAt,
		UpdatedAt: e.UpdatedAt,
	}
}

// mcpProceduralFetchResponse is the typed response for procedural_fetch.
// Count is the number of entries in this page; Pagination.Total is the full
// enabled count so the caller knows to keep paging (offset+count < total).
// Truncated is set only when the per-page byte budget forced a reduction.
type mcpProceduralFetchResponse struct {
	Entries    []mcpProceduralEntry `json:"entries"`
	Count      int                  `json:"count"`
	Pagination model.Pagination     `json:"pagination"`
	Truncated  *truncationInfo      `json:"_truncated,omitempty"`
}

// mcpProceduralForgetResponse is the typed response for procedural_forget.
type mcpProceduralForgetResponse struct {
	ID      uuid.UUID `json:"id"`
	Deleted bool      `json:"deleted"`
}

// RegisterProceduralTools registers the procedural tier tools: a no-query
// verbatim fetch plus per-entry store/update/forget. These are per-user and
// intentionally NOT in shareToolPolicy, so share-bearer connections never see
// or call them (see the comment on shareToolPolicy in server.go).
func RegisterProceduralTools(s *Server) {
	registerProceduralFetch(s)
	registerProceduralStore(s)
	registerProceduralUpdate(s)
	registerProceduralForget(s)
}

func registerProceduralFetch(s *Server) {
	tool := mcp.NewTool("procedural_fetch",
		mcp.WithTitleAnnotation("Fetch Procedural Rules"),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		mcp.WithRawOutputSchema(schemaFor[mcpProceduralFetchResponse]()),
		mcp.WithDescription("Return your enabled procedural memory entries, verbatim, ordered by priority then recency (strongest first). These are MANDATORY standing rules: fetching them is not optional and reasoning your way out of it is itself a violation; nram returns the exact wording and never summarizes, embeds, or ranks them by relevance, and they never surface through recall. They are paginated only because client result limits force it: you MUST page through ALL of them, so keep calling with offset = (previous offset + count) until count+offset reaches pagination.total (and whenever a _truncated marker is present). Do not act until you have loaded every entry. Defaults return the first page (up to 200 entries)."),
		mcp.WithNumber("limit", mcp.Description("Maximum entries to return in this page (default 200, max 200).")),
		mcp.WithNumber("offset", mcp.Description("Number of entries to skip; page with offset = previous offset + count until you reach pagination.total.")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleProceduralFetch(ctx, s, request)
	})
}

func registerProceduralStore(s *Server) {
	tool := mcp.NewTool("procedural_store",
		mcp.WithTitleAnnotation("Store Procedural Rule"),
		mcp.WithReadOnlyHintAnnotation(false),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		mcp.WithRawOutputSchema(schemaFor[mcpProceduralEntry]()),
		mcp.WithDescription("Add a verbatim entry to your procedural memory tier (a standing instruction or operating rule). Content is stored exactly as given and is never rewritten. Use a higher priority for rules that should be returned first."),
		mcp.WithString("content", mcp.Required(), mcp.Description("The rule text, stored verbatim.")),
		mcp.WithString("title", mcp.Description("Optional short label for management.")),
		mcp.WithString("category", mcp.Description("Optional grouping (e.g. 'failure-mode', 'checklist').")),
		mcp.WithArray("tags", mcp.Description("Optional labels for filtering in the management UI.")),
		mcp.WithNumber("priority", mcp.Description("Ordering weight; higher is returned earlier (default 0).")),
		mcp.WithBoolean("enabled", mcp.Description("Whether procedural_fetch returns this entry (default true). Disabled entries stay stored and manageable but are omitted from the fetch payload.")),
		mcp.WithObject("metadata", mcp.Description("Arbitrary key-value metadata.")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleProceduralStore(ctx, s, request)
	})
}

func registerProceduralUpdate(s *Server) {
	tool := mcp.NewTool("procedural_update",
		mcp.WithTitleAnnotation("Update Procedural Rule"),
		mcp.WithReadOnlyHintAnnotation(false),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		mcp.WithRawOutputSchema(schemaFor[mcpProceduralEntry]()),
		mcp.WithDescription("Update an existing procedural entry by id. Only the fields you pass are changed; content is stored verbatim. Use this to change priority (reorder) or toggle enabled."),
		mcp.WithString("id", mcp.Required(), mcp.Description("The entry id to update.")),
		mcp.WithString("content", mcp.Description("Replacement rule text, stored verbatim.")),
		mcp.WithString("title", mcp.Description("Replacement label.")),
		mcp.WithString("category", mcp.Description("Replacement grouping.")),
		mcp.WithArray("tags", mcp.Description("Replacement labels.")),
		mcp.WithNumber("priority", mcp.Description("Replacement ordering weight; higher is returned earlier.")),
		mcp.WithBoolean("enabled", mcp.Description("Whether procedural_fetch returns this entry.")),
		mcp.WithObject("metadata", mcp.Description("Replacement metadata.")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleProceduralUpdate(ctx, s, request)
	})
}

func registerProceduralForget(s *Server) {
	tool := mcp.NewTool("procedural_forget",
		mcp.WithTitleAnnotation("Delete Procedural Rule"),
		mcp.WithReadOnlyHintAnnotation(false),
		mcp.WithDestructiveHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(false),
		mcp.WithOpenWorldHintAnnotation(false),
		mcp.WithToolIcons(iconAnnotation()),
		mcp.WithRawOutputSchema(schemaFor[mcpProceduralForgetResponse]()),
		mcp.WithDescription("Delete a procedural entry by id. Removes the individual rule; the procedural tier itself cannot be deleted."),
		mcp.WithString("id", mcp.Required(), mcp.Description("The entry id to delete.")),
	)

	s.MCPServer().AddTool(tool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return handleProceduralForget(ctx, s, request)
	})
}

// proceduralNamespace resolves the caller's root namespace (the procedural
// scope) and rejects share-bearer callers, which have no procedural access.
func proceduralNamespace(ctx context.Context, s *Server) (uuid.UUID, *mcp.CallToolResult) {
	r := HTTPRequestFromContext(ctx)
	if r == nil {
		return uuid.Nil, mcp.NewToolResultError("no HTTP request in context")
	}
	ac := auth.FromContext(r.Context())
	if ac == nil {
		return uuid.Nil, mcp.NewToolResultError("authentication required")
	}
	if ac.ShareTokenID != nil {
		return uuid.Nil, mcp.NewToolResultError("forbidden: procedural memory is not accessible to share-token callers")
	}
	user, err := s.Deps().UserRepo.GetByID(ctx, ac.UserID)
	if err != nil {
		return uuid.Nil, mcp.NewToolResultError(fmt.Sprintf("user not found: %v", err))
	}
	return user.NamespaceID, nil
}

func handleProceduralFetch(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ns, denied := proceduralNamespace(ctx, s)
	if denied != nil {
		return denied, nil
	}
	args := request.GetArguments()
	limit := parseIntArg(args, "limit", listMaxLimit, 1, listMaxLimit)
	offset := parseIntArg(args, "offset", 0, 0, math.MaxInt32)

	// FetchActive returns ALL enabled entries already ordered priority DESC,
	// created_at DESC. Page in memory (the tier is dozens of entries, not
	// millions); total lets the caller know to keep paging.
	entries, err := s.Deps().Procedural.FetchActive(ctx, ns)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("procedural fetch failed: %v", err)), nil
	}
	total := len(entries)
	lo := min(offset, total)
	hi := min(offset+limit, total)
	page := entries[lo:hi]

	out := make([]mcpProceduralEntry, 0, len(page))
	for i := range page {
		out = append(out, buildMCPProceduralEntry(&page[i]))
	}
	resp := &mcpProceduralFetchResponse{
		Entries:    out,
		Count:      len(out),
		Pagination: model.Pagination{Total: total, Limit: limit, Offset: offset},
	}
	return wrapToolResult(s.deps.Metrics, "procedural_fetch", mcpBudgetBytes(ctx, s.deps.Settings), resp, newProceduralReducer(resp))
}

func handleProceduralStore(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if err := checkWriteAccess(ctx); err != nil {
		return err, nil
	}
	ns, denied := proceduralNamespace(ctx, s)
	if denied != nil {
		return denied, nil
	}
	args := request.GetArguments()

	content, _ := args["content"].(string)
	if strings.TrimSpace(content) == "" {
		return mcp.NewToolResultError("content is required"), nil
	}
	title, _ := args["title"].(string)
	category, _ := args["category"].(string)
	enabled := true
	if v, ok := args["enabled"].(bool); ok {
		enabled = v
	}
	priority := 0
	if v, ok := args["priority"].(float64); ok {
		priority = int(v)
	}

	entry := &model.ProceduralEntry{
		NamespaceID: ns,
		Content:     content,
		Title:       title,
		Category:    category,
		Tags:        extractStringSlice(args["tags"]),
		Priority:    priority,
		Enabled:     enabled,
		Origin:      string(model.OriginUser),
		Metadata:    extractRawJSON(args["metadata"]),
	}

	saved, err := s.Deps().Procedural.Create(ctx, entry)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("procedural store failed: %v", err)), nil
	}
	return wrapToolResult(s.deps.Metrics, "procedural_store", mcpBudgetBytes(ctx, s.deps.Settings), buildMCPProceduralEntry(saved), nil)
}

func handleProceduralUpdate(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if err := checkWriteAccess(ctx); err != nil {
		return err, nil
	}
	ns, denied := proceduralNamespace(ctx, s)
	if denied != nil {
		return denied, nil
	}
	args := request.GetArguments()

	idStr, _ := args["id"].(string)
	id, err := uuid.Parse(strings.TrimSpace(idStr))
	if err != nil {
		return mcp.NewToolResultError("a valid id is required"), nil
	}

	existing, err := s.Deps().Procedural.Get(ctx, id, ns)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return mcp.NewToolResultError("procedural entry not found"), nil
		}
		return mcp.NewToolResultError(fmt.Sprintf("procedural lookup failed: %v", err)), nil
	}

	// Apply only the fields explicitly provided (partial update).
	if v, ok := args["content"].(string); ok {
		existing.Content = v
	}
	if v, ok := args["title"].(string); ok {
		existing.Title = v
	}
	if v, ok := args["category"].(string); ok {
		existing.Category = v
	}
	if _, ok := args["tags"]; ok {
		existing.Tags = extractStringSlice(args["tags"])
	}
	if v, ok := args["priority"].(float64); ok {
		existing.Priority = int(v)
	}
	if v, ok := args["enabled"].(bool); ok {
		existing.Enabled = v
	}
	if _, ok := args["metadata"]; ok {
		existing.Metadata = extractRawJSON(args["metadata"])
	}

	saved, err := s.Deps().Procedural.Update(ctx, existing)
	if err != nil {
		if errors.Is(err, service.ErrEmptyContent) {
			return mcp.NewToolResultError("content cannot be empty"), nil
		}
		return mcp.NewToolResultError(fmt.Sprintf("procedural update failed: %v", err)), nil
	}
	return wrapToolResult(s.deps.Metrics, "procedural_update", mcpBudgetBytes(ctx, s.deps.Settings), buildMCPProceduralEntry(saved), nil)
}

func handleProceduralForget(ctx context.Context, s *Server, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if err := checkWriteAccess(ctx); err != nil {
		return err, nil
	}
	ns, denied := proceduralNamespace(ctx, s)
	if denied != nil {
		return denied, nil
	}
	args := request.GetArguments()

	idStr, _ := args["id"].(string)
	id, err := uuid.Parse(strings.TrimSpace(idStr))
	if err != nil {
		return mcp.NewToolResultError("a valid id is required"), nil
	}

	if err := s.Deps().Procedural.Delete(ctx, id, ns); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return mcp.NewToolResultError("procedural entry not found"), nil
		}
		return mcp.NewToolResultError(fmt.Sprintf("procedural forget failed: %v", err)), nil
	}
	return wrapToolResult(s.deps.Metrics, "procedural_forget", mcpBudgetBytes(ctx, s.deps.Settings), &mcpProceduralForgetResponse{ID: id, Deleted: true}, nil)
}
