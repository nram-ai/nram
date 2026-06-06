package api

import "github.com/google/uuid"

// Shared aggregate-shape primitives used by tier-B (org-aggregate) and
// tier-C (system-aggregate) handlers. These intentionally carry NO content
// fields, only counts, totals, distributions, and tenancy-metadata labels
// (org name, project name, type label). The privacy contract lives in the
// type system: a system-tier handler returns SystemAnalyticsData which has
// no place to put a memory body, so a future scope bug cannot re-leak.

// HistogramBucket is a labeled count for a value-range distribution
// (e.g. recall buckets "0", "1-2", "3-10", ...).
type HistogramBucket struct {
	Range string `json:"range"`
	Count int    `json:"count"`
}

// DailyBucket is a labeled count for a per-day time-series.
type DailyBucket struct {
	Date  string `json:"date"`
	Count int    `json:"count"`
}

// TypeBucket is a labeled count for an enumerable type (entity_type,
// relation, etc.). Type names are schema-level metadata ("person",
// "organization", "owns", "works_at"), not user-authored content.
type TypeBucket struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

// OrgAggregate is one row of a per-org breakdown returned by tier-C
// handlers. Carries org metadata + counts only: no per-user, no
// per-memory data.
type OrgAggregate struct {
	OrgID         uuid.UUID `json:"org_id"`
	OrgName       string    `json:"org_name"`
	TotalMemories int       `json:"total_memories"`
	TotalUsers    int       `json:"total_users"`
	TotalProjects int       `json:"total_projects"`
	TotalEntities int       `json:"total_entities"`
}

// UserAggregate is one row of a per-user breakdown returned by tier-B
// handlers. Bucketed inside a single org so an org_owner sees how their
// org's storage is distributed across users without learning anything
// project-shaped; projects in nram are typically user-owned, so a
// per-project view inside an org is itself a per-user signal.
//
// Email is the only identity field carried; display_name is excluded by
// design.
type UserAggregate struct {
	UserID        uuid.UUID `json:"user_id"`
	Email         string    `json:"email"`
	TotalMemories int       `json:"total_memories"`
	TotalProjects int       `json:"total_projects"`
	TotalEntities int       `json:"total_entities"`
}

// OrgAnalyticsData is the tier-B (org-aggregate) analytics response.
// Returned by NewOrgAnalyticsHandler at /v1/orgs/{org_id}/analytics. The
// breakdown is bucketed per-USER (not per-project); see UserAggregate for
// the rationale.
type OrgAnalyticsData struct {
	MemoryCounts              MemoryCountsData    `json:"memory_counts"`
	RecallDistribution        []HistogramBucket   `json:"recall_distribution"`
	EnrichmentStats           EnrichmentStatsData `json:"enrichment_stats"`
	UserBreakdown             []UserAggregate     `json:"user_breakdown"`
	EntityTypeHistogram       []TypeBucket        `json:"entity_type_histogram"`
	RelationshipTypeHistogram []TypeBucket        `json:"relationship_type_histogram"`
}

// SystemAnalyticsData is the tier-C (system-aggregate) analytics response.
// Returned by NewSystemAnalyticsHandler at /v1/admin/system/analytics.
type SystemAnalyticsData struct {
	TotalMemoryCounts         MemoryCountsData    `json:"total_memory_counts"`
	RecallDistribution        []HistogramBucket   `json:"recall_distribution"`
	EnrichmentStats           EnrichmentStatsData `json:"enrichment_stats"`
	OrgBreakdown              []OrgAggregate      `json:"org_breakdown"`
	EntityTypeHistogram       []TypeBucket        `json:"entity_type_histogram"`
	RelationshipTypeHistogram []TypeBucket        `json:"relationship_type_histogram"`
}

// OrgDashboardData is the tier-B (org-aggregate) dashboard response. The
// breakdown is bucketed per-USER (not per-project); a per-project view
// inside an org leaks individual users' memory activity to every org_owner,
// since projects are user-owned.
type OrgDashboardData struct {
	TotalMemories   int                  `json:"total_memories"`
	TotalProjects   int                  `json:"total_projects"`
	TotalUsers      int                  `json:"total_users"`
	TotalEntities   int                  `json:"total_entities"`
	UserBreakdown   []UserAggregate      `json:"user_breakdown"`
	EnrichmentQueue *DashboardQueueStats `json:"enrichment_queue,omitempty"`
}

// SystemDashboardData is the tier-C (system-aggregate) dashboard response.
type SystemDashboardData struct {
	TotalMemories   int                  `json:"total_memories"`
	TotalProjects   int                  `json:"total_projects"`
	TotalUsers      int                  `json:"total_users"`
	TotalEntities   int                  `json:"total_entities"`
	TotalOrgs       int                  `json:"total_organizations"`
	OrgBreakdown    []OrgAggregate       `json:"org_breakdown"`
	EnrichmentQueue *DashboardQueueStats `json:"enrichment_queue,omitempty"`
}

// OrgActivityResponse is the tier-B (org-aggregate) activity feed: per-day
// memory creation histogram + audit events whose target_org_id matches.
type OrgActivityResponse struct {
	DailyCreation []DailyBucket `json:"daily_creation"`
	AuditEvents   []AuditEvent  `json:"audit_events"`
}

// SystemActivityResponse is the tier-C (system-aggregate) activity feed:
// the full audit-event stream across all tenants. Plus a daily-creation
// histogram across the system.
type SystemActivityResponse struct {
	DailyCreation []DailyBucket `json:"daily_creation"`
	AuditEvents   []AuditEvent  `json:"audit_events"`
}
