package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/events"
	"github.com/nram-ai/nram/internal/model"
)

// tenantFixture wires two orgs (A and B) into a ProjectAccessConfig backed by the
// shared fake lookups, so the SSE authorization path exercises the real
// CheckProjectOrgAccess / CheckNamespaceOrgAccess namespace-prefix rule.
//
// Namespace paths:
//
//	org A namespace:    "org-a/"
//	org A project ns:   "org-a/proj-a/"   (project projA)
//	org B namespace:    "org-b/"
//	org B project ns:   "org-b/proj-b/"   (project projB)
//
// memberA is a non-admin member of org A. Scopes are the fully-formed strings
// the emit sites actually produce.
type tenantFixture struct {
	cfg     ProjectAccessConfig
	memberA *auth.AuthContext

	projAScope string // "project:<projA>"  — owned by org A
	projBScope string // "project:<projB>"  — owned by org B (foreign to memberA)
	nsAScope   string // "namespace:<nsProjA>" — within org A
	nsBScope   string // "namespace:<nsProjB>" — within org B (foreign to memberA)
}

func newTenantFixture() tenantFixture {
	nsOrgA := uuid.New()
	nsProjA := uuid.New()
	nsOrgB := uuid.New()
	nsProjB := uuid.New()
	orgAID := uuid.New()
	orgBID := uuid.New()
	userAID := uuid.New()
	projAID := uuid.New()
	projBID := uuid.New()

	namespaces := map[uuid.UUID]*model.Namespace{
		nsOrgA:  {ID: nsOrgA, Path: "org-a/"},
		nsProjA: {ID: nsProjA, Path: "org-a/proj-a/"},
		nsOrgB:  {ID: nsOrgB, Path: "org-b/"},
		nsProjB: {ID: nsProjB, Path: "org-b/proj-b/"},
	}
	projects := map[uuid.UUID]*model.Project{
		projAID: {ID: projAID, NamespaceID: nsProjA},
		projBID: {ID: projBID, NamespaceID: nsProjB},
	}
	orgs := map[uuid.UUID]*model.Organization{
		orgAID: {ID: orgAID, NamespaceID: nsOrgA},
		orgBID: {ID: orgBID, NamespaceID: nsOrgB},
	}
	users := map[uuid.UUID]*model.User{
		userAID: {ID: userAID, OrgID: orgAID, NamespaceID: nsOrgA, Role: auth.RoleMember},
	}

	return tenantFixture{
		cfg: ProjectAccessConfig{
			Projects:   fakeProjLookup{projects: projects},
			Namespaces: fakeNSLookup{namespaces: namespaces},
			Orgs:       fakeOrgLookup{orgs: orgs},
			Users:      fakeUserLookup{users: users},
		},
		memberA:    &auth.AuthContext{UserID: userAID, OrgID: orgAID, Role: auth.RoleMember},
		projAScope: "project:" + projAID.String(),
		projBScope: "project:" + projBID.String(),
		nsAScope:   "namespace:" + nsProjA.String(),
		nsBScope:   "namespace:" + nsProjB.String(),
	}
}

func TestAuthorizeEventScope(t *testing.T) {
	fx := newTenantFixture()
	admin := adminCtx()

	tests := []struct {
		name     string
		ac       *auth.AuthContext
		scope    string
		expected bool
	}{
		// Non-admin member of org A.
		{"member own project", fx.memberA, fx.projAScope, true},
		{"member foreign project", fx.memberA, fx.projBScope, false},
		{"member own namespace", fx.memberA, fx.nsAScope, true},
		{"member foreign namespace", fx.memberA, fx.nsBScope, false},
		{"member maintenance", fx.memberA, events.EventScopeMaintenance, true},
		{"member global", fx.memberA, "global", false},
		{"member db-migration", fx.memberA, events.EventScopeDBMigration, false},
		{"member vector-migration", fx.memberA, events.EventScopeVectorMigration, false},
		{"member empty (pool tick)", fx.memberA, "", false},
		{"member bare project prefix", fx.memberA, "project:", false},
		{"member malformed project uuid", fx.memberA, "project:not-a-uuid", false},
		{"member bare namespace prefix", fx.memberA, "namespace:", false},
		{"member unknown scope", fx.memberA, "something-else", false},

		// Administrator sees everything.
		{"admin foreign project", admin, fx.projBScope, true},
		{"admin global", admin, "global", true},
		{"admin db-migration", admin, events.EventScopeDBMigration, true},
		{"admin vector-migration", admin, events.EventScopeVectorMigration, true},
		{"admin empty (pool tick)", admin, "", true},
		{"admin unknown scope", admin, "something-else", true},

		// No identity → denied.
		{"nil auth own project", nil, fx.projAScope, false},
		{"nil auth maintenance", nil, events.EventScopeMaintenance, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := authorizeEventScope(context.Background(), fx.cfg, tc.ac, tc.scope)
			if got != tc.expected {
				t.Errorf("authorizeEventScope(scope=%q) = %v, want %v", tc.scope, got, tc.expected)
			}
		})
	}
}

// runEventsStream drives the SSE handler for a caller, publishes the given
// events after the subscription is live, and returns the response body.
func runEventsStream(t *testing.T, ac *auth.AuthContext, cfg ProjectAccessConfig, query string, publish func(bus events.EventBus)) string {
	t.Helper()
	bus := events.NewMemoryBus(0, 0)
	defer func() { _ = bus.Close() }()

	handler := NewEventsHandler(bus, 0, cfg)

	ctx, cancel := context.WithCancel(auth.WithContext(context.Background(), ac))
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/events"+query, nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	// Let the handler subscribe before publishing.
	time.Sleep(50 * time.Millisecond)
	publish(bus)
	// Let the handler write.
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	return rec.Body.String()
}

// TestEventsHandler_TenantIsolation_Live: a non-admin member with an empty
// (firehose) subscription receives their own org's events but not another org's.
func TestEventsHandler_TenantIsolation_Live(t *testing.T) {
	fx := newTenantFixture()

	body := runEventsStream(t, fx.memberA, fx.cfg, "", func(bus events.EventBus) {
		publishTestEvent(t, bus, "own", events.MemoryCreated, fx.projAScope)
		publishTestEvent(t, bus, "foreign", events.MemoryCreated, fx.projBScope)
	})

	if !strings.Contains(body, "id: own") {
		t.Errorf("member must receive their own org's event, got:\n%s", body)
	}
	if strings.Contains(body, "id: foreign") {
		t.Errorf("member must NOT receive another org's event, got:\n%s", body)
	}
}

// TestEventsHandler_MemberSeesOwnDreamAndEnrichment guards against over-filtering:
// a member's own dreaming (project:) and enrichment (namespace:) activity must
// still stream through the empty-scope firehose.
func TestEventsHandler_MemberSeesOwnDreamAndEnrichment(t *testing.T) {
	fx := newTenantFixture()

	body := runEventsStream(t, fx.memberA, fx.cfg, "", func(bus events.EventBus) {
		publishTestEvent(t, bus, "dream", events.DreamCycleHeartbeat, fx.projAScope)
		publishTestEvent(t, bus, "enrich", events.EnrichmentJobStarted, fx.nsAScope)
		publishTestEvent(t, bus, "foreign-enrich", events.EnrichmentJobStarted, fx.nsBScope)
	})

	if !strings.Contains(body, "id: dream") {
		t.Errorf("member must receive their own dream event, got:\n%s", body)
	}
	if !strings.Contains(body, "id: enrich") {
		t.Errorf("member must receive their own enrichment event, got:\n%s", body)
	}
	if strings.Contains(body, "id: foreign-enrich") {
		t.Errorf("member must NOT receive another org's enrichment event, got:\n%s", body)
	}
}

// TestEventsHandler_ForeignScopeSpoofing: subscribing with an explicit foreign
// scope still yields nothing, because authorization uses the event's own scope.
func TestEventsHandler_ForeignScopeSpoofing(t *testing.T) {
	fx := newTenantFixture()

	body := runEventsStream(t, fx.memberA, fx.cfg, "?scope="+fx.projBScope, func(bus events.EventBus) {
		publishTestEvent(t, bus, "foreign", events.MemoryCreated, fx.projBScope)
	})

	if strings.Contains(body, "id: foreign") {
		t.Errorf("spoofing a foreign scope must not leak the other org's events, got:\n%s", body)
	}
}

// TestEventsHandler_BareProjectPrefixSpoofing: the bus prefix filter would match
// every project:* event for a "project:" subscriber, but per-event authorization
// still drops foreign events (and the malformed-scope path denies).
func TestEventsHandler_BareProjectPrefixSpoofing(t *testing.T) {
	fx := newTenantFixture()

	body := runEventsStream(t, fx.memberA, fx.cfg, "?scope=project:", func(bus events.EventBus) {
		publishTestEvent(t, bus, "own", events.MemoryCreated, fx.projAScope)
		publishTestEvent(t, bus, "foreign", events.MemoryCreated, fx.projBScope)
	})

	if !strings.Contains(body, "id: own") {
		t.Errorf("member must still receive their own project events, got:\n%s", body)
	}
	if strings.Contains(body, "id: foreign") {
		t.Errorf("bare project: prefix must not leak another org's events, got:\n%s", body)
	}
}

// TestEventsHandler_AdminFirehose: an administrator's empty subscription receives
// every scope class, including the system/aggregate scopes and the empty-scope
// pool tick.
func TestEventsHandler_AdminFirehose(t *testing.T) {
	fx := newTenantFixture()

	body := runEventsStream(t, adminCtx(), fx.cfg, "", func(bus events.EventBus) {
		publishTestEvent(t, bus, "foreign-proj", events.MemoryCreated, fx.projBScope)
		publishTestEvent(t, bus, "global", events.MemoryReinforced, "global")
		publishTestEvent(t, bus, "dbmig", events.DBMigrationProgress, events.EventScopeDBMigration)
		publishTestEvent(t, bus, "pooltick", events.EnrichmentPoolTick, "")
	})

	for _, id := range []string{"foreign-proj", "global", "dbmig", "pooltick"} {
		if !strings.Contains(body, "id: "+id) {
			t.Errorf("admin firehose must receive %q, got:\n%s", id, body)
		}
	}
}

// TestEventsHandler_MaintenanceForNonAdmin: the non-tenant maintenance banner
// reaches any authenticated caller.
func TestEventsHandler_MaintenanceForNonAdmin(t *testing.T) {
	fx := newTenantFixture()

	body := runEventsStream(t, fx.memberA, fx.cfg, "", func(bus events.EventBus) {
		publishTestEvent(t, bus, "maint", events.MaintenanceStarted, events.EventScopeMaintenance)
	})

	if !strings.Contains(body, "id: maint") {
		t.Errorf("member must receive the maintenance banner event, got:\n%s", body)
	}
}

// TestEventsHandler_TenantIsolation_Replay: the same gate applies on the
// Last-Event-ID replay path.
func TestEventsHandler_TenantIsolation_Replay(t *testing.T) {
	fx := newTenantFixture()
	bus := events.NewMemoryBus(0, 0)
	defer func() { _ = bus.Close() }()

	// Buffer events before the client connects.
	publishTestEvent(t, bus, "seed", events.MemoryCreated, fx.projAScope)
	publishTestEvent(t, bus, "own-replay", events.MemoryUpdated, fx.projAScope)
	publishTestEvent(t, bus, "foreign-replay", events.MemoryCreated, fx.projBScope)

	handler := NewEventsHandler(bus, 0, fx.cfg)

	ctx, cancel := context.WithCancel(auth.WithContext(context.Background(), fx.memberA))
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil).WithContext(ctx)
	req.Header.Set("Last-Event-ID", "seed")
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	body := rec.Body.String()
	if !strings.Contains(body, "id: own-replay") {
		t.Errorf("member must receive their own org's replayed event, got:\n%s", body)
	}
	if strings.Contains(body, "id: foreign-replay") {
		t.Errorf("member must NOT receive another org's replayed event, got:\n%s", body)
	}
}
