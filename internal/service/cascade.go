package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

// CascadeProjectReader exposes the project lookup the cascade resolver needs.
type CascadeProjectReader interface {
	GetByNamespaceID(ctx context.Context, namespaceID uuid.UUID) (*model.Project, error)
}

// CascadeUserReader exposes the user lookup the cascade resolver needs.
type CascadeUserReader interface {
	GetByNamespaceID(ctx context.Context, namespaceID uuid.UUID) (*model.User, error)
}

// CascadeResolver merges system, user, and project layers of the JSON-blob
// override settings (enrichment_enabled, dedup_threshold). Cascade order is
// system → user → project → effective; project beats user beats system. A
// namespace is either a project's namespace or a user's personal namespace,
// not both, so at most one of those layers contributes per call.
type CascadeResolver struct {
	settings *SettingsService
	projects CascadeProjectReader
	users    CascadeUserReader
}

// NewCascadeResolver wires the cascade against the system settings service
// and the project + user readers needed to fetch JSON overrides.
func NewCascadeResolver(s *SettingsService, projects CascadeProjectReader, users CascadeUserReader) *CascadeResolver {
	return &CascadeResolver{settings: s, projects: projects, users: users}
}

// effectiveRoot represents the parsed JSON shape we pull each override out of.
// Both project.Settings and user.Settings serialize this top-level object,
// matching what the legacy UI has been writing.
type effectiveRoot struct {
	EnrichmentEnabled json.RawMessage `json:"enrichment_enabled"`
	DedupThreshold   json.RawMessage `json:"dedup_threshold"`
}

// resolveOverrideJSON walks namespace → project (if project namespace) →
// user (if personal namespace) and returns the parsed override root for
// the layer that owns this namespace. When the namespace owner cannot be
// identified, the zero value is returned (system-level only).
func (r *CascadeResolver) resolveOverrideJSON(ctx context.Context, namespaceID uuid.UUID) effectiveRoot {
	if namespaceID == uuid.Nil {
		return effectiveRoot{}
	}
	if r.projects != nil {
		proj, err := r.projects.GetByNamespaceID(ctx, namespaceID)
		if err == nil && proj != nil {
			return parseEffectiveRoot(proj.Settings)
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			// A non-ErrNoRows error is logged at higher layers; we still
			// fall through to attempt the user lookup so a project-side DB
			// hiccup does not silently disable the user override path.
		}
	}
	if r.users != nil {
		user, err := r.users.GetByNamespaceID(ctx, namespaceID)
		if err == nil && user != nil {
			return parseEffectiveRoot(user.Settings)
		}
	}
	return effectiveRoot{}
}

func parseEffectiveRoot(raw json.RawMessage) effectiveRoot {
	if len(raw) == 0 {
		return effectiveRoot{}
	}
	var root effectiveRoot
	_ = json.Unmarshal(raw, &root)
	return root
}

// ResolveEnrichmentEnabled returns the effective enrichment_enabled value for
// the given namespace. Order: system default → user override → project
// override. Failed lookups fall back to the next-broader layer; the system
// default is the canonical fallback when nothing else applies. Bad JSON in a
// project/user Settings blob is treated as "no override," not an error —
// validation at write-time is the right place to reject malformed payloads.
func (r *CascadeResolver) ResolveEnrichmentEnabled(ctx context.Context, namespaceID uuid.UUID) bool {
	base := true
	if r.settings != nil {
		base = r.settings.ResolveBool(ctx, "enrichment.enabled", "global")
	}
	root := r.resolveOverrideJSON(ctx, namespaceID)
	if ov, err := ParseEnrichmentEnabledOverride(root.EnrichmentEnabled); err == nil {
		base = MergeEnrichmentEnabled(base, ov)
	}
	return base
}

// ResolveDedupThreshold returns the effective dedup threshold for the given
// namespace. Reads ingestion_decision.threshold first; falls back to the
// legacy dedup_threshold key when ingestion_decision.threshold is unset.
// Project / user overrides (the dedup_threshold JSON field) layer on top.
func (r *CascadeResolver) ResolveDedupThreshold(ctx context.Context, namespaceID uuid.UUID) float64 {
	base := 0.92
	if r.settings != nil {
		if v, err := r.settings.ResolveFloat(ctx, SettingIngestionDecisionThreshold, "global"); err == nil && v > 0 && v <= 1 {
			base = v
		} else if v, err := r.settings.ResolveFloat(ctx, SettingDedupThreshold, "global"); err == nil && v > 0 && v <= 1 {
			base = v
		}
	}
	root := r.resolveOverrideJSON(ctx, namespaceID)
	if ov, err := ParseDedupOverride(root.DedupThreshold); err == nil {
		base = MergeDedupThreshold(base, ov)
	}
	return base
}
