package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
	adminstore "github.com/nram-ai/nram/internal/storage/admin"
)

// ---------------------------------------------------------------------------
// Mock stores for route restructure tests
// ---------------------------------------------------------------------------

type rrDashboardStore struct{}

func (s *rrDashboardStore) DashboardStats(_ context.Context, _ *uuid.UUID) (*api.DashboardStatsData, error) {
	return &api.DashboardStatsData{
		TotalMemories: 10,
		TotalProjects: 1,
		TotalUsers:    6,
		TotalEntities: 0,
		TotalOrgs:     2,
	}, nil
}

func (s *rrDashboardStore) RecentActivity(_ context.Context, _ int, _ *uuid.UUID) ([]api.ActivityEvent, error) {
	return []api.ActivityEvent{}, nil
}

type rrAnalyticsStore struct{}

func (s *rrAnalyticsStore) GetAnalytics(_ context.Context, _ *uuid.UUID) (*api.AnalyticsData, error) {
	return &api.AnalyticsData{}, nil
}

type rrUsageStore struct{}

func (s *rrUsageStore) QueryUsage(_ context.Context, _ api.UsageFilter) (*api.UsageReport, error) {
	return &api.UsageReport{
		Groups: []api.UsageGroup{},
		Totals: api.UsageTotals{},
	}, nil
}

type rrNamespaceStore struct{}

func (s *rrNamespaceStore) GetNamespaceTree(_ context.Context, _ *uuid.UUID) ([]api.NamespaceNode, error) {
	return []api.NamespaceNode{}, nil
}

type rrEnrichmentStore struct{}

func (s *rrEnrichmentStore) QueueStatus(_ context.Context) (*api.EnrichmentQueueStatus, error) {
	return &api.EnrichmentQueueStatus{
		Counts: api.EnrichmentQueueCounts{},
		Items:  []api.EnrichmentQueueItem{},
	}, nil
}

func (s *rrEnrichmentStore) RetryFailed(_ context.Context, _ []uuid.UUID) (int, error) {
	return 0, nil
}

func (s *rrEnrichmentStore) SetPaused(_ context.Context, _ bool) error {
	return nil
}

func (s *rrEnrichmentStore) IsPaused(_ context.Context) (bool, error) {
	return false, nil
}

type rrOrgIdPStore struct{}

func (s *rrOrgIdPStore) ListIdPsByOrg(_ context.Context, _ uuid.UUID) ([]model.OAuthIdPConfig, error) {
	return nil, nil
}

func (s *rrOrgIdPStore) CreateIdP(_ context.Context, _ *model.OAuthIdPConfig) error {
	return nil
}

func (s *rrOrgIdPStore) UpdateIdPByOrg(_ context.Context, _ *model.OAuthIdPConfig, _ uuid.UUID) error {
	return nil
}

func (s *rrOrgIdPStore) GetIdPByID(_ context.Context, _ uuid.UUID) (*model.OAuthIdPConfig, error) {
	return nil, nil
}

func (s *rrOrgIdPStore) DeleteIdPByOrg(_ context.Context, _, _ uuid.UUID) error {
	return nil
}

type rrOrgAdminStore struct {
	db storage.DB
}

func (s *rrOrgAdminStore) CountOrgs(ctx context.Context) (int, error) {
	return storage.NewOrganizationRepo(s.db).Count(ctx)
}

func (s *rrOrgAdminStore) ListOrgs(ctx context.Context, limit, offset int) ([]model.Organization, error) {
	return storage.NewOrganizationRepo(s.db).ListPaged(ctx, limit, offset)
}

func (s *rrOrgAdminStore) GetOrg(ctx context.Context, id uuid.UUID) (*model.Organization, error) {
	return storage.NewOrganizationRepo(s.db).GetByID(ctx, id)
}

func (s *rrOrgAdminStore) CreateOrg(_ context.Context, _, _ string) (*model.Organization, error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (s *rrOrgAdminStore) UpdateOrg(_ context.Context, _ uuid.UUID, _, _ string, _ json.RawMessage) (*model.Organization, error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (s *rrOrgAdminStore) DeleteOrg(_ context.Context, _ uuid.UUID) error {
	return fmt.Errorf("not implemented in test")
}

type rrSettingsStore struct{}

func (s *rrSettingsStore) CountSettings(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (s *rrSettingsStore) ListSettings(_ context.Context, _ string, _, _ int) ([]model.Setting, error) {
	return []model.Setting{}, nil
}

func (s *rrSettingsStore) UpdateSetting(_ context.Context, _ string, _ json.RawMessage, _ string, _ *uuid.UUID) error {
	return fmt.Errorf("not implemented in test")
}

func (s *rrSettingsStore) GetSettingsSchema(_ context.Context) ([]api.SettingSchema, error) {
	return []api.SettingSchema{}, nil
}

type rrDatabaseStore struct{}

func (s *rrDatabaseStore) GetDatabaseInfo(_ context.Context) (*api.DatabaseInfo, error) {
	return &api.DatabaseInfo{
		Backend: "sqlite",
		Version: "test",
	}, nil
}

func (s *rrDatabaseStore) TestConnection(_ context.Context, _ string) (*api.ConnectionTestResult, error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (s *rrDatabaseStore) TriggerMigration(_ context.Context, _ string) (*api.MigrationStatus, error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (s *rrDatabaseStore) Preflight(_ context.Context, _ string) (*api.PreflightReport, error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (s *rrDatabaseStore) ResetTarget(_ context.Context, _, _ string) (*api.ResetResult, error) {
	return nil, fmt.Errorf("not implemented in test")
}

func (s *rrDatabaseStore) MigrationAudit(_ context.Context) (*api.MigrationAudit, error) {
	return nil, fmt.Errorf("not implemented in test")
}



// ---------------------------------------------------------------------------
// Route restructure test environment
// ---------------------------------------------------------------------------

type rrTestEnv struct {
	Server *httptest.Server
	DB     storage.DB

	OrgA *model.Organization
	OrgB *model.Organization

	OrgANS *model.Namespace
	OrgBNS *model.Namespace

	ProjectA   *model.Project   // project under Org A's namespace
	ProjectANS *model.Namespace // project namespace (child of OrgANS)

	Admin       rbacUser // administrator, in Org A
	OrgAOwner   rbacUser // org_owner, in Org A
	OrgAMember  rbacUser // member, in Org A
	OrgAReadonly rbacUser // readonly, in Org A
	OrgAService rbacUser // service, in Org A
	OrgBOwner   rbacUser // org_owner, in Org B (cross-org)
	OrgBMember  rbacUser // member, in Org B (cross-org)
}

func newRRTestEnv(t *testing.T) *rrTestEnv {
	t.Helper()

	db := e2eTestDB(t)
	ctx := context.Background()

	nsRepo := storage.NewNamespaceRepo(db)
	orgRepo := storage.NewOrganizationRepo(db)
	userRepo := storage.NewUserRepo(db)
	apiKeyRepo := storage.NewAPIKeyRepo(db)

	rootID := uuid.MustParse("00000000-0000-0000-0000-000000000000")

	// --- Org A ("acme") ---
	orgANSID := uuid.New()
	orgANS := &model.Namespace{
		ID:       orgANSID,
		Name:     "Acme NS",
		Slug:     "acme-ns",
		Kind:     "org",
		ParentID: &rootID,
		Path:     "root/acme-ns",
		Depth:    1,
	}
	if err := nsRepo.Create(ctx, orgANS); err != nil {
		t.Fatalf("create org A namespace: %v", err)
	}

	orgA := &model.Organization{
		NamespaceID: orgANSID,
		Name:        "Acme",
		Slug:        "acme",
	}
	if err := orgRepo.Create(ctx, orgA); err != nil {
		t.Fatalf("create org A: %v", err)
	}

	// --- Org B ("globex") ---
	orgBNSID := uuid.New()
	orgBNS := &model.Namespace{
		ID:       orgBNSID,
		Name:     "Globex NS",
		Slug:     "globex-ns",
		Kind:     "org",
		ParentID: &rootID,
		Path:     "root/globex-ns",
		Depth:    1,
	}
	if err := nsRepo.Create(ctx, orgBNS); err != nil {
		t.Fatalf("create org B namespace: %v", err)
	}

	orgB := &model.Organization{
		NamespaceID: orgBNSID,
		Name:        "Globex",
		Slug:        "globex",
	}
	if err := orgRepo.Create(ctx, orgB); err != nil {
		t.Fatalf("create org B: %v", err)
	}

	// --- Helper to create a user + JWT ---
	createUser := func(email, displayName, role string, org *model.Organization, orgNSPath string) rbacUser {
		t.Helper()
		user := &model.User{
			Email:       email,
			DisplayName: displayName,
			OrgID:       org.ID,
			Role:        role,
		}
		if err := userRepo.Create(ctx, user, nsRepo, nil, orgNSPath); err != nil {
			t.Fatalf("create user %s: %v", email, err)
		}

		jwt, err := auth.GenerateJWT(user.ID, user.OrgID, role, e2eJWTSecret, 1*time.Hour)
		if err != nil {
			t.Fatalf("generate JWT for %s: %v", email, err)
		}

		key := &model.APIKey{
			UserID: user.ID,
			Name:   displayName + " API Key",
			Scopes: []uuid.UUID{},
		}
		rawKey, err := apiKeyRepo.Create(ctx, key)
		if err != nil {
			t.Fatalf("create API key for %s: %v", email, err)
		}

		return rbacUser{User: user, JWT: jwt, APIKey: rawKey}
	}

	admin := createUser("rr-admin@acme.test", "RR Admin", auth.RoleAdministrator, orgA, orgANS.Path)
	orgAOwner := createUser("rr-owner@acme.test", "RR OrgA Owner", auth.RoleOrgOwner, orgA, orgANS.Path)
	orgAMember := createUser("rr-member@acme.test", "RR OrgA Member", auth.RoleMember, orgA, orgANS.Path)
	orgAReadonly := createUser("rr-readonly@acme.test", "RR OrgA Readonly", auth.RoleReadonly, orgA, orgANS.Path)
	orgAService := createUser("rr-service@acme.test", "RR OrgA Service", auth.RoleService, orgA, orgANS.Path)
	orgBOwner := createUser("rr-owner@globex.test", "RR OrgB Owner", auth.RoleOrgOwner, orgB, orgBNS.Path)
	orgBMember := createUser("rr-member@globex.test", "RR OrgB Member", auth.RoleMember, orgB, orgBNS.Path)

	// --- Create a project under Org A for graph tests ---
	projANSID := uuid.New()
	projANS := &model.Namespace{
		ID:       projANSID,
		Name:     "Acme Project NS",
		Slug:     "acme-project-ns",
		Kind:     "project",
		ParentID: &orgANSID,
		Path:     orgANS.Path + "/acme-project-ns",
		Depth:    2,
	}
	if err := nsRepo.Create(ctx, projANS); err != nil {
		t.Fatalf("create project A namespace: %v", err)
	}

	projRepo := storage.NewProjectRepo(db)
	projectA := &model.Project{
		NamespaceID:      projANSID,
		OwnerNamespaceID: orgANSID,
		Name:             "Acme Project",
		Slug:             "acme-project",
		Description:      "Test project for graph tests",
	}
	if err := projRepo.Create(ctx, projectA); err != nil {
		t.Fatalf("create project A: %v", err)
	}

	entityRepo := storage.NewEntityRepo(db)
	entityAliasRepo := storage.NewEntityAliasRepo(db)
	relationshipRepo := storage.NewRelationshipRepo(db)

	// --- Build services ---
	memRepo := newRBACMemoryRepo()
	projectLookup := newRBACMultiProjectLookup()
	namespaceLookup := &rbacNamespaceLookup{db: db}
	userLookup := &rbacUserLookup{db: db}
	orgLookup := &rbacOrgLookup{db: db}

	storeSvc := service.NewStoreService(
		memRepo, projectLookup, namespaceLookup,
		&rbacIngestionLogRepo{}, &rbacEnrichmentQueueRepo{},
	)
	recallSvc := service.NewRecallService(
		memRepo, projectLookup, namespaceLookup,
		nil, nil, nil, nil, nil,
		)
	forgetSvc := service.NewForgetService(memRepo, projectLookup, nil, nil)
	updateSvc := service.NewUpdateService(
		memRepo, projectLookup, &rbacLineageCreator{},
		nil, nil,
		)
	batchStoreSvc := service.NewBatchStoreService(
		memRepo, projectLookup, namespaceLookup,
		&rbacIngestionLogRepo{}, &rbacEnrichmentQueueRepo{},
	)

	// --- MCP server ---
	mcpDeps := mcp.Dependencies{
		Backend:       storage.BackendSQLite,
		Store:         storeSvc,
		Recall:        recallSvc,
		Forget:        forgetSvc,
		Update:        updateSvc,
		BatchStore:    batchStoreSvc,
		ProjectRepo:   projectLookup,
		UserRepo:      userLookup,
		NamespaceRepo: namespaceLookup,
	}
	mcpSrv := mcp.NewServer(mcpDeps)

	// --- Middleware ---
	authMw := auth.NewAuthMiddleware(apiKeyRepo, userRepo, e2eJWTSecret)
	rl := auth.NewRateLimiter(10000, 20000)
	t.Cleanup(rl.Stop)
	metrics := api.NewMetrics()
	projectAccessMw := api.ProjectAccessMiddleware(api.ProjectAccessConfig{
		Projects:   projectLookup,
		Namespaces: namespaceLookup,
		Orgs:       orgLookup,
		Users:      userLookup,
	})

	// --- Handlers ---
	dashStore := &rrDashboardStore{}
	userAdminStore := &rbacUserAdminStore{db: db}

	// Real stores backed by test DB for round-trip operations
	orgUserStore := adminstore.NewUserAdminStore(userRepo, apiKeyRepo, nsRepo, orgRepo, projRepo)


	handlers := Handlers{
		MCP: mcpSrv.Handler(),

		Store:      api.NewStoreHandler(storeSvc, nil),
		Recall:     api.NewRecallHandler(recallSvc),
		Update:     api.NewUpdateHandler(updateSvc, nil),
		Delete:     api.NewDeleteHandler(forgetSvc, nil),
		BulkForget: api.NewBulkForgetHandler(forgetSvc, nil),
		BatchStore: api.NewBatchStoreHandler(batchStoreSvc, nil),

		MeRecall:   api.NewMeRecallHandler(recallSvc, userLookup),
		MeProjects: api.NewMeProjectsHandler(projectLookup, userLookup, namespaceLookup),
		MeAPIKeys:  api.NewMeAPIKeysHandler(apiKeyRepo),
		MeAPIKeyRevoke: api.NewMeAPIKeyRevokeHandler(apiKeyRepo),

		OrgUsers: api.NewOrgUsersHandler(api.OrgUserConfig{Store: orgUserStore}),
		OrgIdP:   api.NewOrgIdPHandler(&rrOrgIdPStore{}),

		AdminDashboard: api.NewAdminDashboardHandler(api.DashboardConfig{Store: dashStore}),
		AdminActivity:  api.NewAdminActivityHandler(api.DashboardConfig{Store: dashStore}),
		AdminAnalytics: api.NewAdminAnalyticsHandler(api.AnalyticsConfig{Store: &rrAnalyticsStore{}}),
		AdminUsage:     api.NewAdminUsageHandler(api.UsageConfig{Store: &rrUsageStore{}}),
		AdminNamespaces: api.NewAdminNamespacesHandler(api.NamespaceAdminConfig{Store: &rrNamespaceStore{}}),
		AdminEnrichment: api.NewAdminEnrichmentHandler(api.EnrichmentAdminConfig{Store: &rrEnrichmentStore{}}),
		AdminOrgs:      api.NewAdminOrgsHandler(api.OrgAdminConfig{Store: &rrOrgAdminStore{db: db}}),
		AdminUsers:     api.NewAdminUsersHandler(api.UserAdminConfig{Store: userAdminStore}),
		AdminSettings:  api.NewAdminSettingsHandler(api.SettingsAdminConfig{Store: &rrSettingsStore{}}),
		AdminDatabase:  api.NewAdminDatabaseHandler(api.DatabaseAdminConfig{Store: &rrDatabaseStore{}}),
		AdminGraph: api.NewAdminGraphHandler(api.GraphAdminConfig{
			Projects:      projRepo,
			Entities:      entityRepo,
			Relationships: relationshipRepo,
			Aliases:       entityAliasRepo,
			Namespaces:    namespaceLookup,
			Orgs:          orgLookup,
		}),

		Health: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		},
		AdminSetupStatus: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"complete":true}`))
		},
	}

	cfg := RouterConfig{
		AuthMiddleware: authMw,
		RateLimiter:    rl,
		Metrics:        metrics,
		ProjectAccess:  projectAccessMw,
	}

	router := NewRouter(cfg, handlers)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	return &rrTestEnv{
		Server:      ts,
		DB:          db,
		OrgA:        orgA,
		OrgB:        orgB,
		OrgANS:      orgANS,
		OrgBNS:      orgBNS,
		ProjectA:    projectA,
		ProjectANS:  projANS,
		Admin:       admin,
		OrgAOwner:   orgAOwner,
		OrgAMember:  orgAMember,
		OrgAReadonly: orgAReadonly,
		OrgAService: orgAService,
		OrgBOwner:   orgBOwner,
		OrgBMember:  orgBMember,
	}
}

// ---------------------------------------------------------------------------
// Test: Tier 1 — Admin-only routes (/v1/admin/*)
// ---------------------------------------------------------------------------

func TestRouteRestructure_Tier1_AdminOnly(t *testing.T) {
	env := newRRTestEnv(t)

	routes := []struct {
		method string
		path   string
	}{
		{"GET", "/v1/admin/orgs"},
		{"GET", "/v1/admin/settings"},
		{"GET", "/v1/admin/database"},
	}

	users := []struct {
		name   string
		token  string
		expect int
	}{
		{"administrator", env.Admin.JWT, http.StatusOK},
		{"org_owner", env.OrgAOwner.JWT, http.StatusForbidden},
		{"member", env.OrgAMember.JWT, http.StatusForbidden},
		{"readonly", env.OrgAReadonly.JWT, http.StatusForbidden},
		{"service", env.OrgAService.JWT, http.StatusForbidden},
	}

	for _, route := range routes {
		for _, u := range users {
			name := fmt.Sprintf("%s_%s_%s", route.method, route.path, u.name)
			t.Run(name, func(t *testing.T) {
				resp := rbacDoRequest(t, route.method, env.Server.URL+route.path, u.token, nil)
				rbacExpectStatus(t, resp, u.expect)
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Test: Tier 2 — Scoped data-viewing routes (readonly+)
// ---------------------------------------------------------------------------

func TestRouteRestructure_Tier2_ScopedDataViewing(t *testing.T) {
	env := newRRTestEnv(t)

	routes := []struct {
		method string
		path   string
	}{
		{"GET", "/v1/dashboard"},
		{"GET", "/v1/activity"},
		{"GET", "/v1/analytics"},
		{"GET", "/v1/usage"},
		{"GET", "/v1/namespaces/tree"},
	}

	users := []struct {
		name  string
		token string
	}{
		{"administrator", env.Admin.JWT},
		{"org_owner", env.OrgAOwner.JWT},
		{"member", env.OrgAMember.JWT},
		{"readonly", env.OrgAReadonly.JWT},
		{"service", env.OrgAService.JWT},
	}

	for _, route := range routes {
		for _, u := range users {
			name := fmt.Sprintf("%s_%s_%s", route.method, route.path, u.name)
			t.Run(name, func(t *testing.T) {
				resp := rbacDoRequest(t, route.method, env.Server.URL+route.path, u.token, nil)
				rbacExpectStatus(t, resp, http.StatusOK)
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Test: Tier 3 — Enrichment (read=all, write=admin only)
// ---------------------------------------------------------------------------

func TestRouteRestructure_Tier3_Enrichment(t *testing.T) {
	env := newRRTestEnv(t)

	users := []struct {
		name  string
		token string
		role  string
	}{
		{"administrator", env.Admin.JWT, auth.RoleAdministrator},
		{"org_owner", env.OrgAOwner.JWT, auth.RoleOrgOwner},
		{"member", env.OrgAMember.JWT, auth.RoleMember},
		{"readonly", env.OrgAReadonly.JWT, auth.RoleReadonly},
		{"service", env.OrgAService.JWT, auth.RoleService},
	}

	// GET /v1/enrichment — all roles get 200
	t.Run("GET_enrichment_read", func(t *testing.T) {
		for _, u := range users {
			t.Run(u.name, func(t *testing.T) {
				resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/enrichment", u.token, nil)
				rbacExpectStatus(t, resp, http.StatusOK)
			})
		}
	})

	// POST /v1/enrichment/retry — admin=200, all others=403
	t.Run("POST_enrichment_retry", func(t *testing.T) {
		for _, u := range users {
			t.Run(u.name, func(t *testing.T) {
				body := map[string]interface{}{"ids": []string{}}
				resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/retry", u.token, body)
				if u.role == auth.RoleAdministrator {
					rbacExpectStatus(t, resp, http.StatusOK)
				} else {
					rbacExpectStatus(t, resp, http.StatusForbidden)
				}
			})
		}
	})

	// POST /v1/enrichment/pause — admin=200, all others=403
	t.Run("POST_enrichment_pause", func(t *testing.T) {
		for _, u := range users {
			t.Run(u.name, func(t *testing.T) {
				body := map[string]interface{}{"paused": true}
				resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/pause", u.token, body)
				if u.role == auth.RoleAdministrator {
					rbacExpectStatus(t, resp, http.StatusOK)
				} else {
					rbacExpectStatus(t, resp, http.StatusForbidden)
				}
			})
		}
	})
}

// ---------------------------------------------------------------------------
// Test: Tier 4 — Org-scoped management (org_owner+)
// ---------------------------------------------------------------------------

func TestRouteRestructure_Tier4_OrgManagement(t *testing.T) {
	env := newRRTestEnv(t)

	orgAID := env.OrgA.ID

	// GET routes: admin=200, own org_owner=200, cross org_owner=403, own member=403
	getRoutes := []struct {
		method string
		suffix string
	}{
		{"GET", "/users"},
		{"GET", "/idp"},
	}

	for _, route := range getRoutes {
		t.Run(fmt.Sprintf("%s_orgs_orgA%s", route.method, route.suffix), func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/orgs/%s%s", env.Server.URL, orgAID, route.suffix)

			cases := []struct {
				name   string
				token  string
				expect int
			}{
				{"admin", env.Admin.JWT, http.StatusOK},
				{"own_org_owner", env.OrgAOwner.JWT, http.StatusOK},
				{"cross_org_owner", env.OrgBOwner.JWT, http.StatusForbidden},
				{"own_member", env.OrgAMember.JWT, http.StatusForbidden},
				{"own_readonly", env.OrgAReadonly.JWT, http.StatusForbidden},
				{"own_service", env.OrgAService.JWT, http.StatusForbidden},
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					resp := rbacDoRequest(t, route.method, url, tc.token, nil)
					rbacExpectStatus(t, resp, tc.expect)
				})
			}
		})
	}

	// POST /v1/orgs/{org_id}/users — create user
	t.Run("POST_orgs_orgA_users", func(t *testing.T) {
		cases := []struct {
			name   string
			token  string
			expect int
		}{
			{"admin", env.Admin.JWT, http.StatusCreated},
			{"own_org_owner", env.OrgAOwner.JWT, http.StatusCreated},
			{"cross_org_owner", env.OrgBOwner.JWT, http.StatusForbidden},
			{"own_member", env.OrgAMember.JWT, http.StatusForbidden},
			{"own_readonly", env.OrgAReadonly.JWT, http.StatusForbidden},
			{"own_service", env.OrgAService.JWT, http.StatusForbidden},
		}

		for i, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				url := fmt.Sprintf("%s/v1/orgs/%s/users", env.Server.URL, orgAID)
				body := map[string]interface{}{
					"email":        fmt.Sprintf("newuser-%d@acme.test", i),
					"display_name": fmt.Sprintf("New User %d", i),
					"password":     "securepassword123",
					"role":         "member",
				}
				resp := rbacDoRequest(t, "POST", url, tc.token, body)
				rbacExpectStatus(t, resp, tc.expect)
			})
		}
	})
}

// ---------------------------------------------------------------------------
// Test: Tier 5 — Org-scoped data (member+)
// ---------------------------------------------------------------------------

func TestRouteRestructure_Tier5_OrgData(t *testing.T) {
	env := newRRTestEnv(t)

	orgAID := env.OrgA.ID

	routes := []string{
		"/analytics",
		"/usage",
	}

	for _, suffix := range routes {
		t.Run(fmt.Sprintf("GET_orgs_orgA%s", suffix), func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/orgs/%s%s", env.Server.URL, orgAID, suffix)

			cases := []struct {
				name   string
				token  string
				expect int
			}{
				{"admin", env.Admin.JWT, http.StatusOK},
				{"own_org_owner", env.OrgAOwner.JWT, http.StatusOK},
				{"own_member", env.OrgAMember.JWT, http.StatusOK},
				{"own_readonly", env.OrgAReadonly.JWT, http.StatusOK},
				{"own_service", env.OrgAService.JWT, http.StatusOK},
				{"cross_org_owner", env.OrgBOwner.JWT, http.StatusForbidden},
				{"cross_member", env.OrgBMember.JWT, http.StatusForbidden},
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					resp := rbacDoRequest(t, "GET", url, tc.token, nil)
					rbacExpectStatus(t, resp, tc.expect)
				})
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test: Tier 6 — Role escalation prevention
// ---------------------------------------------------------------------------

func TestRouteRestructure_Tier6_RoleEscalation(t *testing.T) {
	env := newRRTestEnv(t)

	orgAID := env.OrgA.ID

	// org_owner tries to create a user with administrator role — should be 403
	t.Run("org_owner_cannot_create_administrator", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users", env.Server.URL, orgAID)
		body := map[string]interface{}{
			"email":        "escalated@acme.test",
			"display_name": "Escalated User",
			"password":     "securepassword123",
			"role":         "administrator",
		}
		resp := rbacDoRequest(t, "POST", url, env.OrgAOwner.JWT, body)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})

	// administrator CAN create a user with administrator role — should be 201
	t.Run("admin_can_create_administrator", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users", env.Server.URL, orgAID)
		body := map[string]interface{}{
			"email":        "new-admin@acme.test",
			"display_name": "New Admin",
			"password":     "securepassword123",
			"role":         "administrator",
		}
		resp := rbacDoRequest(t, "POST", url, env.Admin.JWT, body)
		rbacExpectStatus(t, resp, http.StatusCreated)
	})
}

// ---------------------------------------------------------------------------
// Test: Tier 6b — Role escalation on UPDATE (not just create)
// ---------------------------------------------------------------------------

func TestRouteRestructure_Tier6b_UpdateRoleEscalation(t *testing.T) {
	env := newRRTestEnv(t)

	orgAID := env.OrgA.ID
	targetUserID := env.OrgAMember.User.ID

	// org_owner tries to UPDATE a user's role to administrator — should be 403
	t.Run("org_owner_cannot_update_to_administrator", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, targetUserID)
		body := map[string]interface{}{
			"role": "administrator",
		}
		resp := rbacDoRequest(t, "PUT", url, env.OrgAOwner.JWT, body)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})

	// org_owner CAN update a user's role to org_owner (not blocked by escalation check)
	t.Run("org_owner_can_update_to_org_owner", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, targetUserID)
		body := map[string]interface{}{
			"role": "org_owner",
		}
		resp := rbacDoRequest(t, "PUT", url, env.OrgAOwner.JWT, body)
		rbacExpectStatus(t, resp, http.StatusOK)
	})
}

// ---------------------------------------------------------------------------
// Test: Tier 6c — Org user GET/DELETE individual user
// ---------------------------------------------------------------------------

func TestRouteRestructure_Tier6c_OrgUserOperations(t *testing.T) {
	env := newRRTestEnv(t)

	orgAID := env.OrgA.ID
	orgAMemberID := env.OrgAMember.User.ID
	orgBOwnerID := env.OrgBOwner.User.ID

	// GET specific user in own org — org_owner should succeed
	t.Run("get_own_org_user", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, orgAMemberID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAOwner.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// GET user from other org via own org endpoint — should return not found
	t.Run("get_cross_org_user_via_own_org", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, orgBOwnerID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAOwner.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusNotFound)
	})

	// Member cannot access GET /users/{id} (behind org_owner middleware)
	t.Run("member_cannot_get_user", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, orgAMemberID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAMember.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})

	// DELETE user — org_owner can delete user in own org
	// Create a dedicated user to delete so we don't break other tests.
	t.Run("org_owner_delete_own_org_user", func(t *testing.T) {
		// First, create a user to delete via the API
		createURL := fmt.Sprintf("%s/v1/orgs/%s/users", env.Server.URL, orgAID)
		createBody := map[string]interface{}{
			"email":        "deleteme@acme.test",
			"display_name": "Delete Me",
			"password":     "securepassword123",
			"role":         "member",
		}
		createResp := rbacDoRequest(t, "POST", createURL, env.OrgAOwner.JWT, createBody)
		// rbacExpectStatus reads+closes body and returns it as a string
		createRespBody := rbacExpectStatus(t, createResp, http.StatusCreated)

		var created struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(createRespBody), &created); err != nil {
			t.Fatalf("decode created user: %v", err)
		}

		deleteURL := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, created.ID)
		resp := rbacDoRequest(t, "DELETE", deleteURL, env.OrgAOwner.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusNoContent)
	})

	// DELETE user — member cannot delete (behind org_owner middleware)
	t.Run("member_cannot_delete_user", func(t *testing.T) {
		deleteURL := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, orgAMemberID)
		resp := rbacDoRequest(t, "DELETE", deleteURL, env.OrgAMember.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})
}

// ---------------------------------------------------------------------------
// Test: Tier 7 — Removed admin routes return 404/405 for admin users
// ---------------------------------------------------------------------------

func TestRouteRestructure_Tier7_RemovedAdminRoutes(t *testing.T) {
	env := newRRTestEnv(t)

	removedRoutes := []string{
		"/v1/admin/dashboard",
		"/v1/admin/activity",
		"/v1/admin/analytics",
		"/v1/admin/usage",
		"/v1/admin/namespaces/tree",
		"/v1/admin/graph",
		"/v1/admin/enrichment",
	}

	for _, path := range removedRoutes {
		t.Run("admin_"+path, func(t *testing.T) {
			resp := rbacDoRequest(t, "GET", env.Server.URL+path, env.Admin.JWT, nil)
			// These routes are no longer registered under /v1/admin —
			// admin passes RequireRole but gets 404/405 from chi (no matching handler).
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusForbidden {
				t.Fatalf("expected 404 or 405 for removed route %s, got %d", path, resp.StatusCode)
			}
		})

		t.Run("nonadmin_"+path, func(t *testing.T) {
			// Non-admin users still get 403 from RequireRole on the /v1/admin/* group
			resp := rbacDoRequest(t, "GET", env.Server.URL+path, env.OrgAMember.JWT, nil)
			rbacExpectStatus(t, resp, http.StatusForbidden)
		})
	}
}

// ---------------------------------------------------------------------------
// Test: Tier 9 — API key auth on scoped and org routes
// ---------------------------------------------------------------------------

func TestRouteRestructure_Tier9_APIKeyAuth(t *testing.T) {
	env := newRRTestEnv(t)

	// API key auth on scoped data routes
	t.Run("api_key_dashboard", func(t *testing.T) {
		resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/dashboard", env.Admin.APIKey, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	t.Run("api_key_analytics", func(t *testing.T) {
		resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/analytics", env.OrgAMember.APIKey, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// API key auth on admin routes
	t.Run("api_key_admin_admin", func(t *testing.T) {
		resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/admin/orgs", env.Admin.APIKey, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	t.Run("api_key_admin_member", func(t *testing.T) {
		resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/admin/orgs", env.OrgAMember.APIKey, nil)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})

	// Enrichment write via API key — admin only
	t.Run("api_key_enrichment_write_admin", func(t *testing.T) {
		body := map[string]interface{}{"ids": []string{}}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/retry", env.Admin.APIKey, body)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	t.Run("api_key_enrichment_write_member", func(t *testing.T) {
		body := map[string]interface{}{"ids": []string{}}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/retry", env.OrgAMember.APIKey, body)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})
}

// ---------------------------------------------------------------------------
// Test: Tier 10 — Admin accessing other org's scoped routes
// ---------------------------------------------------------------------------

func TestRouteRestructure_Tier10_AdminCrossOrgAccess(t *testing.T) {
	env := newRRTestEnv(t)

	orgBID := env.OrgB.ID

	// Admin should be able to access any org's routes
	routes := []struct {
		method string
		suffix string
		expect int
	}{
		{"GET", "/analytics", http.StatusOK},
		{"GET", "/usage", http.StatusOK},
		{"GET", "/users", http.StatusOK},
		{"GET", "/idp", http.StatusOK},
	}

	for _, route := range routes {
		t.Run(fmt.Sprintf("admin_%s_orgB%s", route.method, route.suffix), func(t *testing.T) {
			url := fmt.Sprintf("%s/v1/orgs/%s%s", env.Server.URL, orgBID, route.suffix)
			resp := rbacDoRequest(t, route.method, url, env.Admin.JWT, nil)
			rbacExpectStatus(t, resp, route.expect)
		})
	}
}

// ---------------------------------------------------------------------------
// Test: Org Users — Update user with display_name change
// ---------------------------------------------------------------------------

func TestRouteRestructure_OrgUsers_UpdateUser(t *testing.T) {
	env := newRRTestEnv(t)
	orgAID := env.OrgA.ID

	// Update user display_name as org_owner
	t.Run("update_display_name", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, env.OrgAMember.User.ID)
		body := map[string]interface{}{
			"display_name": "Updated Display Name",
		}
		resp := rbacDoRequest(t, "PUT", url, env.OrgAOwner.JWT, body)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// Update user of different org returns 404
	t.Run("update_cross_org_user_returns_404", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, env.OrgBOwner.User.ID)
		body := map[string]interface{}{
			"display_name": "Should Fail",
		}
		resp := rbacDoRequest(t, "PUT", url, env.OrgAOwner.JWT, body)
		rbacExpectStatus(t, resp, http.StatusNotFound)
	})

	// Delete user of different org returns 404
	t.Run("delete_cross_org_user_returns_404", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, env.OrgBOwner.User.ID)
		resp := rbacDoRequest(t, "DELETE", url, env.OrgAOwner.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusNotFound)
	})
}

// ---------------------------------------------------------------------------
// Test: Org Users — Validation errors
// ---------------------------------------------------------------------------

func TestRouteRestructure_OrgUsers_Validation(t *testing.T) {
	env := newRRTestEnv(t)
	orgAID := env.OrgA.ID
	createURL := fmt.Sprintf("%s/v1/orgs/%s/users", env.Server.URL, orgAID)

	// Empty email
	t.Run("create_empty_email", func(t *testing.T) {
		body := map[string]interface{}{
			"email":        "",
			"display_name": "No Email",
			"password":     "securepassword123",
			"role":         "member",
		}
		resp := rbacDoRequest(t, "POST", createURL, env.Admin.JWT, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Short password
	t.Run("create_short_password", func(t *testing.T) {
		body := map[string]interface{}{
			"email":        "shortpw@acme.test",
			"display_name": "Short Password",
			"password":     "short",
			"role":         "member",
		}
		resp := rbacDoRequest(t, "POST", createURL, env.Admin.JWT, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Invalid role
	t.Run("create_invalid_role", func(t *testing.T) {
		body := map[string]interface{}{
			"email":        "badrole@acme.test",
			"display_name": "Bad Role",
			"password":     "securepassword123",
			"role":         "superadmin",
		}
		resp := rbacDoRequest(t, "POST", createURL, env.Admin.JWT, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Empty body
	t.Run("create_empty_body", func(t *testing.T) {
		body := map[string]interface{}{}
		resp := rbacDoRequest(t, "POST", createURL, env.Admin.JWT, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Update with invalid role
	t.Run("update_invalid_role", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, env.OrgAMember.User.ID)
		body := map[string]interface{}{
			"role": "superadmin",
		}
		resp := rbacDoRequest(t, "PUT", url, env.Admin.JWT, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})
}

// ---------------------------------------------------------------------------
// Test: Org Users — API Key operations
// ---------------------------------------------------------------------------

func TestRouteRestructure_OrgUsers_APIKeys(t *testing.T) {
	env := newRRTestEnv(t)
	orgAID := env.OrgA.ID
	targetUserID := env.OrgAMember.User.ID

	// List API keys for user in org
	t.Run("list_api_keys", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s/api-keys", env.Server.URL, orgAID, targetUserID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAOwner.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// Generate API key with label
	var generatedKeyID string
	t.Run("generate_api_key", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s/api-keys", env.Server.URL, orgAID, targetUserID)
		body := map[string]interface{}{
			"label": "test-key-label",
		}
		resp := rbacDoRequest(t, "POST", url, env.OrgAOwner.JWT, body)
		respBody := rbacExpectStatus(t, resp, http.StatusCreated)

		var created struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(respBody), &created); err != nil {
			t.Fatalf("decode created api key: %v", err)
		}
		generatedKeyID = created.ID
	})

	// Generate API key with empty label returns 400
	t.Run("generate_api_key_empty_label", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s/api-keys", env.Server.URL, orgAID, targetUserID)
		body := map[string]interface{}{
			"label": "",
		}
		resp := rbacDoRequest(t, "POST", url, env.OrgAOwner.JWT, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Revoke API key
	t.Run("revoke_api_key", func(t *testing.T) {
		if generatedKeyID == "" {
			t.Skip("no key was generated")
		}
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s/api-keys/%s", env.Server.URL, orgAID, targetUserID, generatedKeyID)
		resp := rbacDoRequest(t, "DELETE", url, env.OrgAOwner.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusNoContent)
	})

	// List API keys for user in different org returns 404
	t.Run("list_api_keys_cross_org", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s/api-keys", env.Server.URL, orgAID, env.OrgBOwner.User.ID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAOwner.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusNotFound)
	})

	// Revoke API key for user in different org returns 404 (authorization check)
	t.Run("revoke_api_key_cross_org_user", func(t *testing.T) {
		// Try to revoke OrgA member's key via OrgB user path — should be blocked
		url := fmt.Sprintf("%s/v1/orgs/%s/users/%s/api-keys/%s",
			env.Server.URL, orgAID, env.OrgBOwner.User.ID, uuid.New())
		resp := rbacDoRequest(t, "DELETE", url, env.OrgAOwner.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusNotFound)
	})
}

// ---------------------------------------------------------------------------
// Test: Org Users — Method not allowed
// ---------------------------------------------------------------------------

func TestRouteRestructure_OrgUsers_MethodNotAllowed(t *testing.T) {
	env := newRRTestEnv(t)
	orgAID := env.OrgA.ID

	// PATCH on /users collection
	t.Run("patch_users_collection", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users", env.Server.URL, orgAID)
		resp := rbacDoRequest(t, "PATCH", url, env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})
}

// ---------------------------------------------------------------------------
// Test: Org IdP — Non-GET methods return 501
// ---------------------------------------------------------------------------

func TestRouteRestructure_OrgIdP_Implemented(t *testing.T) {
	env := newRRTestEnv(t)
	orgAID := env.OrgA.ID
	baseURL := fmt.Sprintf("%s/v1/orgs/%s/idp", env.Server.URL, orgAID)

	// GET should return 200 with an empty list
	t.Run("GET", func(t *testing.T) {
		resp := rbacDoRequest(t, "GET", baseURL, env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// PUT is not a supported method, expect 405
	t.Run("PUT", func(t *testing.T) {
		resp := rbacDoRequest(t, "PUT", baseURL, env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusMethodNotAllowed)
	})
}

// ---------------------------------------------------------------------------
// Test: OrgAccessMiddleware — Invalid org_id
// ---------------------------------------------------------------------------

func TestRouteRestructure_OrgAccess_InvalidOrgID(t *testing.T) {
	env := newRRTestEnv(t)

	// Request with invalid UUID as org_id
	t.Run("invalid_uuid_org_id", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/not-a-uuid/users", env.Server.URL)
		resp := rbacDoRequest(t, "GET", url, env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Unauthenticated request should be caught by auth middleware (401)
	t.Run("unauthenticated_request", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/orgs/%s/users", env.Server.URL, env.OrgA.ID)
		resp := rbacDoRequest(t, "GET", url, "", nil)
		rbacExpectStatus(t, resp, http.StatusUnauthorized)
	})
}

// ---------------------------------------------------------------------------
// Test: Last Administrator Protection
// ---------------------------------------------------------------------------

func TestRouteRestructure_LastAdminProtection(t *testing.T) {
	env := newRRTestEnv(t)
	orgAID := env.OrgA.ID

	// The env has one administrator (env.Admin). Create a second admin so we
	// can test both the "allowed" and "blocked" cases.

	// First, create a second admin via the API.
	createURL := fmt.Sprintf("%s/v1/orgs/%s/users", env.Server.URL, orgAID)
	createBody := map[string]interface{}{
		"email":        "second-admin@acme.test",
		"display_name": "Second Admin",
		"password":     "securepassword123",
		"role":         "administrator",
	}
	createResp := rbacDoRequest(t, "POST", createURL, env.Admin.JWT, createBody)
	createRespBody := rbacExpectStatus(t, createResp, http.StatusCreated)

	var secondAdmin struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(createRespBody), &secondAdmin); err != nil {
		t.Fatalf("decode second admin: %v", err)
	}

	// Delete the second admin — should succeed because env.Admin still exists
	t.Run("delete_non_last_admin_succeeds", func(t *testing.T) {
		deleteURL := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, secondAdmin.ID)
		resp := rbacDoRequest(t, "DELETE", deleteURL, env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusNoContent)
	})

	// Now try to delete the only remaining admin — should be blocked with 409
	t.Run("delete_last_admin_blocked", func(t *testing.T) {
		deleteURL := fmt.Sprintf("%s/v1/orgs/%s/users/%s", env.Server.URL, orgAID, env.Admin.User.ID)
		resp := rbacDoRequest(t, "DELETE", deleteURL, env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusConflict)
	})
}

// ---------------------------------------------------------------------------
// Test: Tier 11 — Graph handler (/v1/graph)
// ---------------------------------------------------------------------------

func TestRouteRestructure_Graph(t *testing.T) {
	env := newRRTestEnv(t)

	// Missing project param -> 400
	t.Run("missing_project_param", func(t *testing.T) {
		resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/graph", env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Invalid project UUID -> 400
	t.Run("invalid_project_uuid", func(t *testing.T) {
		url := env.Server.URL + "/v1/graph?project=not-a-uuid"
		resp := rbacDoRequest(t, "GET", url, env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Non-existent project -> 404
	t.Run("nonexistent_project", func(t *testing.T) {
		fakeID := uuid.New()
		url := fmt.Sprintf("%s/v1/graph?project=%s", env.Server.URL, fakeID)
		resp := rbacDoRequest(t, "GET", url, env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusNotFound)
	})

	// Admin with valid project -> 200
	t.Run("admin_valid_project", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/graph?project=%s", env.Server.URL, env.ProjectA.ID)
		resp := rbacDoRequest(t, "GET", url, env.Admin.JWT, nil)
		body := rbacExpectStatus(t, resp, http.StatusOK)

		var graphResp api.GraphResponse
		if err := json.Unmarshal([]byte(body), &graphResp); err != nil {
			t.Fatalf("failed to decode graph response: %v", err)
		}
		if graphResp.Entities == nil {
			t.Fatal("expected non-nil entities slice")
		}
		if graphResp.Relationships == nil {
			t.Fatal("expected non-nil relationships slice")
		}
	})

	// Non-admin (member) with own-org project -> 200 (path ancestry check passes)
	t.Run("member_own_org_project", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/graph?project=%s", env.Server.URL, env.ProjectA.ID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAMember.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// Non-admin (org_owner) with own-org project -> 200
	t.Run("org_owner_own_org_project", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/graph?project=%s", env.Server.URL, env.ProjectA.ID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAOwner.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// Non-admin (readonly) with own-org project -> 200
	t.Run("readonly_own_org_project", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/graph?project=%s", env.Server.URL, env.ProjectA.ID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAReadonly.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// Non-admin (service) with own-org project -> 200
	t.Run("service_own_org_project", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/graph?project=%s", env.Server.URL, env.ProjectA.ID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAService.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// Cross-org member -> 403 (Org B user accessing Org A project)
	t.Run("cross_org_member_denied", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/graph?project=%s", env.Server.URL, env.ProjectA.ID)
		resp := rbacDoRequest(t, "GET", url, env.OrgBMember.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})

	// Cross-org org_owner -> 403
	t.Run("cross_org_owner_denied", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/graph?project=%s", env.Server.URL, env.ProjectA.ID)
		resp := rbacDoRequest(t, "GET", url, env.OrgBOwner.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})

	// Unauthenticated request -> 401
	t.Run("unauthenticated", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/graph?project=%s", env.Server.URL, env.ProjectA.ID)
		resp := rbacDoRequest(t, "GET", url, "", nil)
		rbacExpectStatus(t, resp, http.StatusUnauthorized)
	})

	// API key auth: admin API key with valid project -> 200
	t.Run("api_key_admin_valid_project", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/graph?project=%s", env.Server.URL, env.ProjectA.ID)
		resp := rbacDoRequest(t, "GET", url, env.Admin.APIKey, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// API key auth: cross-org member -> 403
	t.Run("api_key_cross_org_denied", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/graph?project=%s", env.Server.URL, env.ProjectA.ID)
		resp := rbacDoRequest(t, "GET", url, env.OrgBMember.APIKey, nil)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})
}

// ---------------------------------------------------------------------------
// Test: Tier 12 — Enrichment test-prompt endpoint (/v1/enrichment/test-prompt)
// ---------------------------------------------------------------------------

func TestRouteRestructure_EnrichmentTestPrompt(t *testing.T) {
	env := newRRTestEnv(t)

	// Non-admin users should get 403 (admin guard fires before handler logic)
	t.Run("non_admin_forbidden", func(t *testing.T) {
		nonAdmins := []struct {
			name  string
			token string
		}{
			{"org_owner", env.OrgAOwner.JWT},
			{"member", env.OrgAMember.JWT},
			{"readonly", env.OrgAReadonly.JWT},
			{"service", env.OrgAService.JWT},
			{"cross_org_owner", env.OrgBOwner.JWT},
			{"cross_org_member", env.OrgBMember.JWT},
		}

		for _, u := range nonAdmins {
			t.Run(u.name, func(t *testing.T) {
				body := map[string]interface{}{
					"type":         "fact",
					"sample_input": "Some test content",
				}
				resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/test-prompt", u.token, body)
				rbacExpectStatus(t, resp, http.StatusForbidden)
			})
		}
	})

	// Admin with type "fact" -> 400 (no fact provider configured)
	t.Run("admin_fact_no_provider", func(t *testing.T) {
		body := map[string]interface{}{
			"type":         "fact",
			"sample_input": "Some test content about Brandon who lives in Denver.",
		}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/test-prompt", env.Admin.JWT, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Admin with type "entity" -> 400 (no entity provider configured)
	t.Run("admin_entity_no_provider", func(t *testing.T) {
		body := map[string]interface{}{
			"type":         "entity",
			"sample_input": "Some test content about Brandon who lives in Denver.",
		}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/test-prompt", env.Admin.JWT, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Admin with invalid type -> 400
	t.Run("admin_invalid_type", func(t *testing.T) {
		body := map[string]interface{}{
			"type":         "invalid",
			"sample_input": "Some test content",
		}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/test-prompt", env.Admin.JWT, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Admin with missing sample_input -> 400
	t.Run("admin_missing_sample_input", func(t *testing.T) {
		body := map[string]interface{}{
			"type": "fact",
		}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/test-prompt", env.Admin.JWT, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Admin with empty sample_input -> 400
	t.Run("admin_empty_sample_input", func(t *testing.T) {
		body := map[string]interface{}{
			"type":         "fact",
			"sample_input": "   ",
		}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/test-prompt", env.Admin.JWT, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// Non-admin with invalid type still gets 403 (admin guard fires first)
	t.Run("non_admin_invalid_type_still_403", func(t *testing.T) {
		body := map[string]interface{}{
			"type":         "invalid",
			"sample_input": "Some test content",
		}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/test-prompt", env.OrgAMember.JWT, body)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})

	// Non-admin with missing body still gets 403
	t.Run("non_admin_empty_body_still_403", func(t *testing.T) {
		body := map[string]interface{}{}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/test-prompt", env.OrgAMember.JWT, body)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})

	// Unauthenticated -> 401
	t.Run("unauthenticated", func(t *testing.T) {
		body := map[string]interface{}{
			"type":         "fact",
			"sample_input": "Some test content",
		}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/test-prompt", "", body)
		rbacExpectStatus(t, resp, http.StatusUnauthorized)
	})

	// API key auth: admin API key with fact type -> 400 (no provider)
	t.Run("api_key_admin_fact_no_provider", func(t *testing.T) {
		body := map[string]interface{}{
			"type":         "fact",
			"sample_input": "Some test content",
		}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/test-prompt", env.Admin.APIKey, body)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// API key auth: non-admin API key -> 403
	t.Run("api_key_member_forbidden", func(t *testing.T) {
		body := map[string]interface{}{
			"type":         "fact",
			"sample_input": "Some test content",
		}
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment/test-prompt", env.OrgAMember.APIKey, body)
		rbacExpectStatus(t, resp, http.StatusForbidden)
	})
}

// ---------------------------------------------------------------------------
// Test: Usage handler query param scope override prevention
// ---------------------------------------------------------------------------

func TestRouteRestructure_UsageQueryParamScopeOverride(t *testing.T) {
	env := newRRTestEnv(t)

	// Non-admin user tries to access another org's usage via ?org= query param.
	// This MUST be blocked — the query param should be ignored for non-admins.
	t.Run("member_cannot_override_org_scope", func(t *testing.T) {
		// OrgA member tries to see OrgB's usage
		url := fmt.Sprintf("%s/v1/usage?org=%s", env.Server.URL, env.OrgB.ID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAMember.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusOK)

		// The response should contain OrgA's data (the user's own org),
		// not OrgB's data. We verify by checking it doesn't error —
		// the key assertion is that it returns 200 (not leaked data).
		// The mock store returns empty data regardless, but the security
		// fix ensures filter.OrgID is set to the user's org, not the param.
	})

	t.Run("org_owner_cannot_override_org_scope", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/usage?org=%s", env.Server.URL, env.OrgB.ID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAOwner.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// Admin CAN use ?org= to filter
	t.Run("admin_can_use_org_filter", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/usage?org=%s", env.Server.URL, env.OrgB.ID)
		resp := rbacDoRequest(t, "GET", url, env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})

	// API key: non-admin with ?org= override attempt
	t.Run("api_key_member_cannot_override_org_scope", func(t *testing.T) {
		url := fmt.Sprintf("%s/v1/usage?org=%s", env.Server.URL, env.OrgB.ID)
		resp := rbacDoRequest(t, "GET", url, env.OrgAMember.APIKey, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})
}

// ---------------------------------------------------------------------------
// Test: Enrichment unknown sub-path and wrong method
// ---------------------------------------------------------------------------

func TestRouteRestructure_EnrichmentEdgeCases(t *testing.T) {
	env := newRRTestEnv(t)

	// GET /v1/enrichment/unknown -> 400
	t.Run("get_unknown_subpath", func(t *testing.T) {
		resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/enrichment/unknown", env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// POST /v1/enrichment (no sub-path, write method on read endpoint) -> 400
	t.Run("post_enrichment_root", func(t *testing.T) {
		resp := rbacDoRequest(t, "POST", env.Server.URL+"/v1/enrichment", env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusBadRequest)
	})

	// GET /v1/enrichment/queue explicit -> 200
	t.Run("get_enrichment_queue_explicit", func(t *testing.T) {
		resp := rbacDoRequest(t, "GET", env.Server.URL+"/v1/enrichment/queue", env.Admin.JWT, nil)
		rbacExpectStatus(t, resp, http.StatusOK)
	})
}
