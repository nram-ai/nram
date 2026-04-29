package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
)

type mockCascadeProjects struct {
	byNS  map[uuid.UUID]*model.Project
	err   error
	calls int
}

func (m *mockCascadeProjects) GetByNamespaceID(ctx context.Context, ns uuid.UUID) (*model.Project, error) {
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	if p, ok := m.byNS[ns]; ok {
		return p, nil
	}
	return nil, sql.ErrNoRows
}

type mockCascadeUsers struct {
	byNS map[uuid.UUID]*model.User
}

func (m *mockCascadeUsers) GetByNamespaceID(ctx context.Context, ns uuid.UUID) (*model.User, error) {
	if u, ok := m.byNS[ns]; ok {
		return u, nil
	}
	return nil, sql.ErrNoRows
}

func TestCascade_ResolveEnrichmentEnabled_NoOverrides(t *testing.T) {
	r := NewCascadeResolver(nil, &mockCascadeProjects{}, &mockCascadeUsers{})
	if !r.ResolveEnrichmentEnabled(context.Background(), uuid.Nil) {
		t.Error("nil namespace + no settings should return base=true")
	}
}

func TestCascade_ResolveEnrichmentEnabled_ProjectOverride(t *testing.T) {
	projNS := uuid.New()
	r := NewCascadeResolver(nil,
		&mockCascadeProjects{byNS: map[uuid.UUID]*model.Project{
			projNS: {Settings: json.RawMessage(`{"enrichment_enabled": false}`)},
		}},
		&mockCascadeUsers{},
	)
	if r.ResolveEnrichmentEnabled(context.Background(), projNS) {
		t.Error("project override=false should win over base=true")
	}
}

func TestCascade_ResolveEnrichmentEnabled_UserOverride(t *testing.T) {
	userNS := uuid.New()
	r := NewCascadeResolver(nil,
		&mockCascadeProjects{},
		&mockCascadeUsers{byNS: map[uuid.UUID]*model.User{
			userNS: {Settings: json.RawMessage(`{"enrichment_enabled": false}`)},
		}},
	)
	if r.ResolveEnrichmentEnabled(context.Background(), userNS) {
		t.Error("user override=false should win over base=true")
	}
}

func TestCascade_ResolveEnrichmentEnabled_ProjectBeatsUser(t *testing.T) {
	// A namespace cannot be owned by both a user AND a project under the
	// real data model, but the resolver's ordering should prefer the project
	// lookup if both happen to return a row (defensive against future
	// schema changes).
	bothNS := uuid.New()
	r := NewCascadeResolver(nil,
		&mockCascadeProjects{byNS: map[uuid.UUID]*model.Project{
			bothNS: {Settings: json.RawMessage(`{"enrichment_enabled": false}`)},
		}},
		&mockCascadeUsers{byNS: map[uuid.UUID]*model.User{
			bothNS: {Settings: json.RawMessage(`{"enrichment_enabled": true}`)},
		}},
	)
	if r.ResolveEnrichmentEnabled(context.Background(), bothNS) {
		t.Error("project layer should be consulted before user layer")
	}
}

func TestCascade_ResolveEnrichmentEnabled_NoOverrideFallsThrough(t *testing.T) {
	projNS := uuid.New()
	r := NewCascadeResolver(nil,
		&mockCascadeProjects{byNS: map[uuid.UUID]*model.Project{
			projNS: {Settings: json.RawMessage(`{}`)},
		}},
		&mockCascadeUsers{},
	)
	if !r.ResolveEnrichmentEnabled(context.Background(), projNS) {
		t.Error("project with empty settings should fall through to base=true")
	}
}

func TestCascade_ResolveEnrichmentEnabled_BadJSONFallsThrough(t *testing.T) {
	projNS := uuid.New()
	r := NewCascadeResolver(nil,
		&mockCascadeProjects{byNS: map[uuid.UUID]*model.Project{
			projNS: {Settings: json.RawMessage(`{"enrichment_enabled":"not a bool"}`)},
		}},
		&mockCascadeUsers{},
	)
	// Override parser errors are treated as "no override", not a hard fail.
	if !r.ResolveEnrichmentEnabled(context.Background(), projNS) {
		t.Error("malformed JSON should not flip the effective value")
	}
}

func TestCascade_ResolveDedupThreshold_NoOverrides(t *testing.T) {
	r := NewCascadeResolver(nil, &mockCascadeProjects{}, &mockCascadeUsers{})
	if got := r.ResolveDedupThreshold(context.Background(), uuid.Nil); got != 0.92 {
		t.Errorf("nil namespace + nil settings should return base=0.92, got %v", got)
	}
}

func TestCascade_ResolveDedupThreshold_ProjectOverride(t *testing.T) {
	projNS := uuid.New()
	r := NewCascadeResolver(nil,
		&mockCascadeProjects{byNS: map[uuid.UUID]*model.Project{
			projNS: {Settings: json.RawMessage(`{"dedup_threshold": 0.85}`)},
		}},
		&mockCascadeUsers{},
	)
	if got := r.ResolveDedupThreshold(context.Background(), projNS); got != 0.85 {
		t.Errorf("project override 0.85 should win, got %v", got)
	}
}

func TestCascade_ResolveDedupThreshold_UserOverride(t *testing.T) {
	userNS := uuid.New()
	r := NewCascadeResolver(nil,
		&mockCascadeProjects{},
		&mockCascadeUsers{byNS: map[uuid.UUID]*model.User{
			userNS: {Settings: json.RawMessage(`{"dedup_threshold": 0.97}`)},
		}},
	)
	if got := r.ResolveDedupThreshold(context.Background(), userNS); got != 0.97 {
		t.Errorf("user override 0.97 should win, got %v", got)
	}
}

func TestCascade_ResolveDedupThreshold_NilNamespace(t *testing.T) {
	// uuid.Nil should skip both project and user lookups (master-toggle path).
	projNS := uuid.New()
	r := NewCascadeResolver(nil,
		&mockCascadeProjects{byNS: map[uuid.UUID]*model.Project{
			projNS: {Settings: json.RawMessage(`{"dedup_threshold": 0.50}`)},
		}},
		&mockCascadeUsers{},
	)
	if got := r.ResolveDedupThreshold(context.Background(), uuid.Nil); got != 0.92 {
		t.Errorf("uuid.Nil should NOT pick up project overrides, got %v", got)
	}
}

func TestCascade_RepeatedLookupsHitCache(t *testing.T) {
	// A worker draining a batch of jobs from the same namespace should hit
	// the project repo only once, not once per job.
	projNS := uuid.New()
	projects := &mockCascadeProjects{byNS: map[uuid.UUID]*model.Project{
		projNS: {Settings: json.RawMessage(`{"dedup_threshold": 0.85}`)},
	}}
	r := NewCascadeResolver(nil, projects, &mockCascadeUsers{})

	for i := 0; i < 5; i++ {
		_ = r.ResolveDedupThreshold(context.Background(), projNS)
		_ = r.ResolveEnrichmentEnabled(context.Background(), projNS)
	}
	if projects.calls != 1 {
		t.Errorf("expected 1 project lookup (cache hit on repeat), got %d", projects.calls)
	}
}

func TestCascade_InvalidateNamespaceClearsCache(t *testing.T) {
	projNS := uuid.New()
	projects := &mockCascadeProjects{byNS: map[uuid.UUID]*model.Project{
		projNS: {Settings: json.RawMessage(`{"dedup_threshold": 0.85}`)},
	}}
	r := NewCascadeResolver(nil, projects, &mockCascadeUsers{})

	_ = r.ResolveDedupThreshold(context.Background(), projNS)
	r.InvalidateNamespace(projNS)
	_ = r.ResolveDedupThreshold(context.Background(), projNS)

	if projects.calls != 2 {
		t.Errorf("expected 2 project lookups (1 + invalidate + 1), got %d", projects.calls)
	}
}
