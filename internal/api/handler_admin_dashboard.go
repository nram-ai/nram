package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// DashboardStore abstracts storage operations for dashboard/activity.
type DashboardStore interface {
	// DashboardStats returns aggregate stats for the dashboard.
	DashboardStats(ctx context.Context) (*DashboardStatsData, error)
	// RecentActivity returns the most recent activity events, up to limit.
	RecentActivity(ctx context.Context, limit int) ([]ActivityEvent, error)
}

// DashboardStatsData holds aggregate statistics for the admin dashboard.
type DashboardStatsData struct {
	TotalMemories     int                  `json:"total_memories"`
	TotalProjects     int                  `json:"total_projects"`
	TotalUsers        int                  `json:"total_users"`
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
type ActivityEvent struct {
	ID        string     `json:"id"`
	Type      string     `json:"type"`
	Summary   string     `json:"summary"`
	ProjectID *uuid.UUID `json:"project_id,omitempty"`
	UserID    *uuid.UUID `json:"user_id,omitempty"`
	Timestamp time.Time  `json:"timestamp"`
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
		stats, err := cfg.Store.DashboardStats(r.Context())
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve dashboard stats"))
			return
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

		events, err := cfg.Store.RecentActivity(r.Context(), limit)
		if err != nil {
			WriteError(w, ErrInternal("failed to retrieve activity events"))
			return
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"events": events,
		})
	}
}
