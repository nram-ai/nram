package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"

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

// Cascade cache TTL bounds how long a parsed override blob stays in memory.
// Per-namespace cascade lookups happen inside hot paths (every enrichment
// job, every recall fallback to system weights), so without a cache a
// worker draining 16 jobs from one namespace pays 16 project lookups.
// Operator changes to project/user settings hit eventual consistency
// within the TTL window — same model as SettingsService. Read once at
// resolver construction; runtime changes require server restart. See
// service.SettingCascadeCacheTTLSeconds.

type cascadeCacheEntry struct {
	root      effectiveRoot
	expiresAt time.Time
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
	mu       sync.RWMutex
	cache    map[uuid.UUID]cascadeCacheEntry
	cacheTTL time.Duration
}

// NewCascadeResolver wires the cascade against the system settings service
// and the project + user readers needed to fetch JSON overrides. The cache
// TTL is read once from SettingCascadeCacheTTLSeconds; nil settings falls
// through to the registered default.
func NewCascadeResolver(s *SettingsService, projects CascadeProjectReader, users CascadeUserReader) *CascadeResolver {
	ttl := s.ResolveDurationSecondsWithDefault(context.Background(),
		SettingCascadeCacheTTLSeconds, "global")
	if ttl < time.Second {
		ttl = time.Second
	}
	return &CascadeResolver{
		settings: s,
		projects: projects,
		users:    users,
		cache:    make(map[uuid.UUID]cascadeCacheEntry),
		cacheTTL: ttl,
	}
}

type effectiveRoot struct {
	EnrichmentEnabled json.RawMessage `json:"enrichment_enabled"`
	DedupThreshold    json.RawMessage `json:"dedup_threshold"`
}

// resolveOverrideJSON walks namespace → project (if project namespace) →
// user (if personal namespace) and returns the parsed override root for
// the layer that owns this namespace. uuid.Nil and namespaces with no
// owner short-circuit to the zero value so callers fall back to the
// system layer. Hits a small TTL cache to amortize same-namespace
// resolutions across batch processing.
func (r *CascadeResolver) resolveOverrideJSON(ctx context.Context, namespaceID uuid.UUID) effectiveRoot {
	if namespaceID == uuid.Nil {
		return effectiveRoot{}
	}
	now := time.Now()
	r.mu.RLock()
	if entry, ok := r.cache[namespaceID]; ok && entry.expiresAt.After(now) {
		r.mu.RUnlock()
		return entry.root
	}
	r.mu.RUnlock()

	root := r.lookupOverrideJSON(ctx, namespaceID)

	r.mu.Lock()
	r.cache[namespaceID] = cascadeCacheEntry{root: root, expiresAt: now.Add(r.cacheTTL)}
	r.mu.Unlock()
	return root
}

func (r *CascadeResolver) lookupOverrideJSON(ctx context.Context, namespaceID uuid.UUID) effectiveRoot {
	if r.projects != nil {
		proj, err := r.projects.GetByNamespaceID(ctx, namespaceID)
		if err == nil && proj != nil {
			return parseEffectiveRoot(proj.Settings)
		}
		// On a real (non-ErrNoRows) project error fall through to the user
		// lookup so a project-side DB hiccup does not silently disable the
		// user override path.
		_ = errors.Is(err, sql.ErrNoRows)
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

// InvalidateNamespace drops the cached override blob for one namespace so
// the next resolve hits the repo. Callers writing through the cascade
// (project / user settings updates) should invoke this to make changes
// visible immediately rather than waiting out the TTL.
func (r *CascadeResolver) InvalidateNamespace(namespaceID uuid.UUID) {
	r.mu.Lock()
	delete(r.cache, namespaceID)
	r.mu.Unlock()
}

// ResolveEnrichmentEnabled returns the effective enrichment_enabled value for
// the given namespace. Bad JSON in a project/user Settings blob is treated
// as "no override," not an error — write-time validation is the right place
// to reject malformed payloads.
func (r *CascadeResolver) ResolveEnrichmentEnabled(ctx context.Context, namespaceID uuid.UUID) bool {
	base := true
	if r.settings != nil {
		base = r.settings.ResolveBool(ctx, SettingEnrichmentEnabled, "global")
	}
	root := r.resolveOverrideJSON(ctx, namespaceID)
	if ov, err := ParseEnrichmentEnabledOverride(root.EnrichmentEnabled); err == nil {
		base = MergeEnrichmentEnabled(base, ov)
	}
	return base
}

// ResolveDedupThreshold returns the effective dedup threshold for the given
// namespace. ingestion_decision.threshold takes precedence over the legacy
// dedup_threshold key; per-namespace overrides (the dedup_threshold JSON
// field) layer on top.
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
