package api

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
)

// GraphSettingsResolver narrows the SettingsService surface to just the
// methods this handler needs. Keeps the api package decoupled from the
// concrete service type.
type GraphSettingsResolver interface {
	ResolveFloatWithDefault(ctx context.Context, key, scope string) float64
	ResolveIntWithDefault(ctx context.Context, key, scope string) int
}

// GraphEntity represents an entity node for graph visualization.
type GraphEntity struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Canonical    string   `json:"canonical"`
	EntityType   string   `json:"entity_type"`
	MentionCount int      `json:"mention_count"`
	Aliases      []string `json:"aliases"`
	CreatedAt    string   `json:"created_at"`
	UpdatedAt    string   `json:"updated_at"`
}

// GraphRelationship represents a relationship edge for graph visualization.
type GraphRelationship struct {
	ID       string  `json:"id"`
	SourceID string  `json:"source_id"`
	TargetID string  `json:"target_id"`
	Relation string  `json:"relation"`
	Weight   float64 `json:"weight"`
}

// GraphResponse is the response payload for the admin graph endpoint.
// Truncated is set when the namespace's eligible edge count exceeds
// graph.max_edges and the response carries the top-N edges by weight.
// TotalEdges then reflects the pre-truncation count so the UI can
// surface a partial-view banner; ReturnedEdges mirrors len(Relationships)
// for convenience.
type GraphResponse struct {
	Entities      []GraphEntity       `json:"entities"`
	Relationships []GraphRelationship `json:"relationships"`
	Truncated     bool                `json:"truncated,omitempty"`
	TotalEdges    int                 `json:"total_edges,omitempty"`
	ReturnedEdges int                 `json:"returned_edges,omitempty"`
}

// GraphProjectStore retrieves a project by ID.
type GraphProjectStore interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Project, error)
}

// GraphEntityStore retrieves entities for a namespace.
type GraphEntityStore interface {
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Entity, error)
}

// GraphRelationshipStore retrieves relationships for a namespace.
type GraphRelationshipStore interface {
	ListByNamespace(ctx context.Context, namespaceID uuid.UUID) ([]model.Relationship, error)
}

// GraphAliasStore retrieves aliases for an entity.
type GraphAliasStore interface {
	ListByEntity(ctx context.Context, entityID uuid.UUID, namespaces []uuid.UUID) ([]model.EntityAlias, error)
}

// GraphMemoryStore batch-fetches memories so the handler can drop edges whose
// sourcing memory is gone. GetBatch already excludes soft-deleted rows, so a
// soft-deleted source simply does not come back; superseded rows do come back
// and are filtered on SupersededBy.
type GraphMemoryStore interface {
	GetBatch(ctx context.Context, ids, namespaces []uuid.UUID) ([]model.Memory, error)
}

// GraphNamespaceLookup retrieves a namespace by ID to check path ancestry.
type GraphNamespaceLookup interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Namespace, error)
}

// GraphOrgLookup retrieves an organization by ID to resolve its namespace.
type GraphOrgLookup interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Organization, error)
}

// GraphAdminConfig holds dependencies for the admin graph handler.
type GraphAdminConfig struct {
	Projects      GraphProjectStore
	Entities      GraphEntityStore
	Relationships GraphRelationshipStore
	Aliases       GraphAliasStore
	Namespaces    GraphNamespaceLookup
	Orgs          GraphOrgLookup
	Settings      GraphSettingsResolver
	// Memories is used to drop edges whose sourcing memory is gone
	// (lost-provenance). When nil, provenance filtering is skipped and every
	// edge is shown (legacy behavior).
	Memories GraphMemoryStore
}

// NewAdminGraphHandler returns an http.HandlerFunc that serves graph data
// for a given project. Query parameter: project (project UUID).
func NewAdminGraphHandler(cfg GraphAdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "method not allowed",
			})
			return
		}

		projectIDStr := r.URL.Query().Get("project")
		if projectIDStr == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "project query parameter is required",
			})
			return
		}

		projectID, err := uuid.Parse(projectIDStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid project ID",
			})
			return
		}

		// Parse optional min_weight filter; default falls through to
		// graph.default_min_weight (0.1 by default).
		minWeight := cfg.Settings.ResolveFloatWithDefault(r.Context(),
			"graph.default_min_weight", "global")
		if mwStr := r.URL.Query().Get("min_weight"); mwStr != "" {
			if parsed, err := strconv.ParseFloat(mwStr, 64); err == nil && parsed >= 0 {
				minWeight = parsed
			}
		}

		project, err := cfg.Projects.GetByID(r.Context(), projectID)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{
				"error": "project not found",
			})
			return
		}

		// Verify the requesting user has access to this project's namespace.
		// Privacy: the previous implementation allowed administrators to
		// bypass this check, exposing every tenant's entity names and
		// relationship labels through /v1/graph. The bypass is removed;
		// admin views only their own org's project graphs through this
		// endpoint, like any other role. Cross-tenant graph aggregates
		// (entity-type / relationship-type histograms) are exposed via the
		// org-aggregate and system-aggregate analytics handlers instead.
		ac := auth.FromContext(r.Context())
		if ac == nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{
				"error": "authentication required",
			})
			return
		}
		if ac.OrgID == uuid.Nil {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error": "user does not have an organization assigned",
			})
			return
		}
		if cfg.Namespaces == nil || cfg.Orgs == nil {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error": "access denied: org verification unavailable",
			})
			return
		}

		ns, nsErr := cfg.Namespaces.GetByID(r.Context(), project.NamespaceID)
		if nsErr != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error": "access denied to this project",
			})
			return
		}

		org, orgErr := cfg.Orgs.GetByID(r.Context(), ac.OrgID)
		if orgErr != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error": "access denied to this project",
			})
			return
		}

		orgNS, orgNSErr := cfg.Namespaces.GetByID(r.Context(), org.NamespaceID)
		if orgNSErr != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error": "access denied to this project",
			})
			return
		}

		prefix := orgNS.Path + "/"
		if !strings.HasPrefix(ns.Path, prefix) && ns.Path != orgNS.Path {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error": "access denied to this project",
			})
			return
		}

		entities, err := cfg.Entities.ListByNamespace(r.Context(), project.NamespaceID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "failed to retrieve entities",
			})
			return
		}

		relationships, err := cfg.Relationships.ListByNamespace(r.Context(), project.NamespaceID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "failed to retrieve relationships",
			})
			return
		}

		// Resolve which edges still have live provenance. An edge whose
		// source memory is NULL (the memory was hard-deleted, firing
		// ON DELETE SET NULL) or points at a soft-deleted/superseded memory is
		// stale graph data the lifecycle sweep reaps; hide it here so the
		// console graph stays consistent with recall and the reaped store.
		// GetBatch omits soft-deleted rows, so a soft-deleted source never
		// lands in liveProvenance; superseded rows are filtered explicitly.
		liveProvenance := resolveLiveProvenance(r.Context(), cfg.Memories, project.NamespaceID, relationships)
		// filterProvenance is true only when a memory store is wired AND the
		// lookup succeeded; a nil map (no store, or a lookup error) falls open
		// to legacy show-all behavior rather than blanking the graph.
		filterProvenance := liveProvenance != nil

		// Namespace entity set: relationships only connect entities here.
		namespaceEntityIDs := make(map[string]bool, len(entities))
		for _, e := range entities {
			namespaceEntityIDs[e.ID.String()] = true
		}

		// Filter edges (validity, weight, namespace, provenance) and record
		// which entities survive via at least one live edge.
		referencedEntityIDs := make(map[string]bool)
		graphRelationships := make([]GraphRelationship, 0, len(relationships))
		for _, rel := range relationships {
			// Skip expired relationships.
			if rel.ValidUntil != nil {
				continue
			}
			// Skip relationships below the minimum weight threshold.
			if rel.Weight < minWeight {
				continue
			}
			// Drop lost-provenance edges when provenance filtering is active.
			if filterProvenance {
				if rel.SourceMemory == nil || !liveProvenance[*rel.SourceMemory] {
					continue
				}
			}
			srcID := rel.SourceID.String()
			tgtID := rel.TargetID.String()
			// Only include relationships where both ends are in the entity set.
			if namespaceEntityIDs[srcID] && namespaceEntityIDs[tgtID] {
				graphRelationships = append(graphRelationships, GraphRelationship{
					ID:       rel.ID.String(),
					SourceID: srcID,
					TargetID: tgtID,
					Relation: rel.Relation,
					Weight:   rel.Weight,
				})
				referencedEntityIDs[srcID] = true
				referencedEntityIDs[tgtID] = true
			}
		}

		// Emit entities. With provenance filtering on, hide entities not
		// connected by any surviving edge; these are orphan-only nodes
		// (referenced solely by reaped edges) or rare edgeless mention-only
		// entities; both are graph pollution once their provenance is gone.
		graphEntities := make([]GraphEntity, 0, len(entities))
		for _, e := range entities {
			if filterProvenance && !referencedEntityIDs[e.ID.String()] {
				continue
			}

			aliases, _ := cfg.Aliases.ListByEntity(r.Context(), e.ID, []uuid.UUID{project.NamespaceID})
			aliasNames := make([]string, 0, len(aliases))
			for _, a := range aliases {
				aliasNames = append(aliasNames, a.Alias)
			}

			graphEntities = append(graphEntities, GraphEntity{
				ID:           e.ID.String(),
				Name:         e.Name,
				Canonical:    e.Canonical,
				EntityType:   e.EntityType,
				MentionCount: e.MentionCount,
				Aliases:      aliasNames,
				CreatedAt:    e.CreatedAt.Format("2006-01-02T15:04:05Z"),
				UpdatedAt:    e.UpdatedAt.Format("2006-01-02T15:04:05Z"),
			})
		}

		maxEdges := cfg.Settings.ResolveIntWithDefault(r.Context(),
			service.SettingGraphMaxEdges, "global")
		writeJSON(w, http.StatusOK, applyEdgeCap(graphEntities, graphRelationships, maxEdges))
	}
}

// resolveLiveProvenance returns the set of source-memory IDs that are still
// live (present and not superseded) among the given relationships. A nil store
// yields a nil map; callers treat that as "provenance filtering disabled".
// GetBatch already excludes soft-deleted memories, so they never appear in the
// result and their edges are correctly treated as lost-provenance.
func resolveLiveProvenance(ctx context.Context, store GraphMemoryStore, namespaceID uuid.UUID, relationships []model.Relationship) map[uuid.UUID]bool {
	if store == nil {
		return nil
	}
	idSet := make(map[uuid.UUID]struct{})
	for _, rel := range relationships {
		if rel.SourceMemory != nil {
			idSet[*rel.SourceMemory] = struct{}{}
		}
	}
	if len(idSet) == 0 {
		return map[uuid.UUID]bool{}
	}
	ids := make([]uuid.UUID, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}
	live := make(map[uuid.UUID]bool, len(ids))
	mems, err := store.GetBatch(ctx, ids, []uuid.UUID{namespaceID})
	if err != nil {
		// On a lookup error, fail open to the prior behavior (show edges)
		// rather than blanking the graph; a non-nil map with no entries would
		// drop every edge.
		return nil
	}
	for _, m := range mems {
		if m.IsLiveProvenance() {
			live[m.ID] = true
		}
	}
	return live
}

// applyEdgeCap enforces the graph.max_edges ceiling on a response payload.
// When the eligible edge count exceeds maxEdges the function selects a
// connectivity-aware subset: a maximum-weight spanning forest first (Kruskal
// over the weight-descending edge list), then the strongest remaining edges
// until the budget is spent. The forest pass connects every node that has at
// least one eligible edge using its highest-weight links, so when
// nodesWithEdges - components <= maxEdges no node is stranded by the cap;
// the fill pass then restores the "strongest edges win" character for the
// bulk of the budget. Edge ID breaks weight ties so the selection is stable
// across replicas and post-VACUUM row reorderings. The Truncated /
// TotalEdges / ReturnedEdges fields are populated so the admin UI can surface
// a partial-view banner. The cap exists because the THREE.js force-graph
// renderer stalls past low thousands of edges; throttling the data layer (the
// historical workaround was a low transitive namespace_hard_cap) belongs on
// the rendering boundary, not on dream-cycle work. maxEdges <= 0 disables the
// cap.
//
// Entities are returned unchanged regardless of whether truncation fires.
// Filtering entities to only those touched by retained edges would change the
// response shape at the cap boundary; operators investigating namespace
// inventory rely on the entity set being a stable view of "what exists,"
// independent of edge rendering. The cap is an edge concern; entities pass
// through, and the viz hides any node left without a visible edge.
//
// The function does not mutate the input rels slice. The sort runs on a
// copy so callers can safely retain the original ordering for telemetry,
// audit, or any future side-channel use.
func applyEdgeCap(entities []GraphEntity, rels []GraphRelationship, maxEdges int) GraphResponse {
	total := len(rels)
	if maxEdges <= 0 || total <= maxEdges {
		return GraphResponse{Entities: entities, Relationships: rels}
	}

	sorted := make([]GraphRelationship, len(rels))
	copy(sorted, rels)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Weight != sorted[j].Weight {
			return sorted[i].Weight > sorted[j].Weight
		}
		return sorted[i].ID < sorted[j].ID
	})

	// Phase 1: mark the maximum-weight spanning forest. Walking the
	// weight-descending list and keeping only edges that join two distinct
	// components attaches every node-with-edges to the graph using its
	// strongest available links.
	uf := newUnionFind(len(entities))
	forest := make([]bool, len(sorted))
	forestCount := 0
	for i := range sorted {
		if forestCount == maxEdges {
			break
		}
		if uf.union(sorted[i].SourceID, sorted[i].TargetID) {
			forest[i] = true
			forestCount++
		}
	}

	// Phase 2: emit the forest edges plus the strongest remaining edges to
	// fill the budget (the fill adds density within already-connected
	// components without stranding anyone). A single weight-descending pass
	// over the already-sorted list keeps the payload deterministic and
	// replica-stable with no second sort.
	fill := maxEdges - forestCount
	chosen := make([]GraphRelationship, 0, maxEdges)
	for i := range sorted {
		if len(chosen) == maxEdges {
			break
		}
		if forest[i] {
			chosen = append(chosen, sorted[i])
		} else if fill > 0 {
			chosen = append(chosen, sorted[i])
			fill--
		}
	}

	return GraphResponse{
		Entities:      entities,
		Relationships: chosen,
		Truncated:     true,
		TotalEdges:    total,
		ReturnedEdges: len(chosen),
	}
}

// unionFind is a disjoint-set structure over entity IDs (string keys) used to
// build the spanning forest in applyEdgeCap. Components are created lazily on
// first reference, so only entities that appear in an edge are tracked.
type unionFind struct {
	parent map[string]string
	rank   map[string]int
}

// newUnionFind sizes the backing maps for sizeHint distinct entities (an upper
// bound is the namespace entity count) to avoid incremental rehashing.
func newUnionFind(sizeHint int) *unionFind {
	return &unionFind{
		parent: make(map[string]string, sizeHint),
		rank:   make(map[string]int, sizeHint),
	}
}

// find returns the representative of x's component, creating a singleton
// component for x on first reference and halving the path it walks.
func (u *unionFind) find(x string) string {
	if _, ok := u.parent[x]; !ok {
		u.parent[x] = x
		return x
	}
	for u.parent[x] != x {
		u.parent[x] = u.parent[u.parent[x]]
		x = u.parent[x]
	}
	return x
}

// union merges the components of a and b. It reports whether a merge happened;
// false means they were already connected (the caller skips that edge).
func (u *unionFind) union(a, b string) bool {
	ra, rb := u.find(a), u.find(b)
	if ra == rb {
		return false
	}
	if u.rank[ra] < u.rank[rb] {
		ra, rb = rb, ra
	}
	u.parent[rb] = ra
	if u.rank[ra] == u.rank[rb] {
		u.rank[ra]++
	}
	return true
}
