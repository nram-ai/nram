package api

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
)

// DashboardStore abstracts storage operations for dashboard/activity.
// When orgID is non-nil, results are scoped to that organization. When
// userID is also non-nil, the per-project breakdown is further scoped to
// projects owned by that user (so an org_owner does not learn the names
// of other users' projects in their org) and the activity feed attaches a
// short content preview so the caller's own dashboard can render its own
// memories. Org/system tiers (userID == nil) remain content-free and
// project-name-free.
type DashboardStore interface {
	// DashboardStats returns aggregate stats for the dashboard. The
	// per-project breakdown (DashboardStatsData.MemoriesByProject) is
	// scoped to projects owned by userID when non-nil; with userID == nil
	// the breakdown falls back to org-wide aggregate (project names are
	// then omitted by the store to avoid the cross-tenant name leak that
	// motivated the 2026-05-25 self-tier scoping fix).
	DashboardStats(ctx context.Context, orgID, userID *uuid.UUID) (*DashboardStatsData, error)
	// RecentActivity returns the most recent activity events, up to limit.
	RecentActivity(ctx context.Context, limit int, orgID, userID *uuid.UUID) ([]ActivityEvent, error)
}

// DashboardStatsData holds aggregate statistics for the admin dashboard.
type DashboardStatsData struct {
	TotalMemories     int                  `json:"total_memories"`
	TotalProjects     int                  `json:"total_projects"`
	TotalUsers        int                  `json:"total_users"`
	TotalEntities     int                  `json:"total_entities"`
	TotalOrgs         int                  `json:"total_organizations"`
	MemoriesByProject []ProjectMemoryCount `json:"memories_by_project"`
	EnrichmentQueue   *DashboardQueueStats `json:"enrichment_queue,omitempty"`
}

// ProjectMemoryCount holds a per-project memory count.
type ProjectMemoryCount struct {
	ProjectID   uuid.UUID `json:"project_id"`
	ProjectName string    `json:"project_name"`
	Count       int       `json:"count"`
}

// DashboardQueueStats holds enrichment queue depth statistics.
type DashboardQueueStats struct {
	Pending    int `json:"pending"`
	Processing int `json:"processing"`
	Failed     int `json:"failed"`
}

// ActivityEvent represents a single recent activity entry.
//
// Privacy: Preview is populated only on the self-tier (caller's own
// memories), that is, when the store is called with both orgID and userID
// set to the caller's IDs. Org and system tiers (userID nil) leave Preview
// nil so cross-tenant feeds remain content-free.
type ActivityEvent struct {
	ID          string     `json:"id"`
	Type        string     `json:"type"`
	ProjectID   *uuid.UUID `json:"project_id,omitempty"`
	UserID      *uuid.UUID `json:"user_id,omitempty"`
	LengthChars int        `json:"length_chars,omitempty"`
	Preview     *string    `json:"preview,omitempty"`
	Timestamp   time.Time  `json:"timestamp"`
}

// DashboardConfig holds the dependencies for the dashboard and activity handlers.
type DashboardConfig struct {
	Store DashboardStore
}

// NewAdminDashboardHandler returns an http.HandlerFunc that responds with
// aggregate dashboard statistics including memory counts per project,
// provider health, and queue depth.
func NewAdminDashboardHandler(cfg DashboardConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Self-tier: caller's own dashboard. Admin viewing /v1/dashboard sees
		// admin's own data, not the system-wide view. Cross-tenant dashboards
		// move to /v1/admin/system/dashboard and /v1/orgs/{org_id}/dashboard.
		// userID is passed through so the per-project breakdown can scope to
		// the caller's own projects; without it, MemoriesByProject would
		// emit every project name in the org (including other users'), which
		// is the cross-tenant name leak the org-tier dreaming/enrichment fix
		// closed at the same time.
		orgID, userID := SelfScope(auth.FromContext(r.Context()))

		stats, err := cfg.Store.DashboardStats(r.Context(), orgID, userID)
		if err != nil {
			slog.Error("api: AdminDashboard failed", "err", err)
			WriteError(w, ErrInternal("failed to retrieve dashboard stats"))
			return
		}

		if stats.MemoriesByProject == nil {
			stats.MemoriesByProject = []ProjectMemoryCount{}
		}

		writeJSON(w, http.StatusOK, stats)
	}
}

const (
	defaultActivityLimit = 20
	maxActivityLimit     = 100
)

// NewAdminActivityHandler returns an http.HandlerFunc that responds with
// the most recent activity events. It accepts an optional ?limit=N query
// parameter (default 20, max 100).
func NewAdminActivityHandler(cfg DashboardConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := defaultActivityLimit

		if raw := r.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err == nil && parsed > 0 {
				limit = parsed
			}
		}

		if limit > maxActivityLimit {
			limit = maxActivityLimit
		}

		// Self-tier: caller's own activity feed. Admin viewing /v1/activity
		// sees admin's own activity. Both org and user scope are passed so
		// the store can attach a content preview for the caller's own memories.
		orgID, userID := SelfScope(auth.FromContext(r.Context()))

		events, err := cfg.Store.RecentActivity(r.Context(), limit, orgID, userID)
		if err != nil {
			slog.Error("api: AdminActivity failed", "err", err)
			WriteError(w, ErrInternal("failed to retrieve activity events"))
			return
		}

		if events == nil {
			events = []ActivityEvent{}
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"events": events,
		})
	}
}
