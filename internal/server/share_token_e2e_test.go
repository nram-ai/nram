package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/mcp"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/observability/metrics"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// HTTP-level integration test for the share-token end-to-end flow. Stitches
// together: real DB, real auth middleware, real OAuth server, real MCP
// server, real router. Drives the full share-paste consent → token mint →
// /mcp tool call chain and asserts the per-tier scoping holds when crossed
// over real HTTP. A regression at any layer would break this test.

// e2eShareProjectLookup is a multi-project mock that lets share-bearer
// scoping tests register more than one project against a single user
// namespace.
type e2eShareProjectLookup struct {
	projects []*model.Project
}

func (m *e2eShareProjectLookup) GetBySlug(_ context.Context, _ uuid.UUID, slug string) (*model.Project, error) {
	for _, p := range m.projects {
		if p.Slug == slug {
			return p, nil
		}
	}
	return nil, fmt.Errorf("project not found: %s", slug)
}

func (m *e2eShareProjectLookup) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	for _, p := range m.projects {
		if p.ID == id {
			return p, nil
		}
	}
	return nil, fmt.Errorf("project not found by id: %s", id)
}

func (m *e2eShareProjectLookup) GetByNamespaceID(_ context.Context, ns uuid.UUID) (*model.Project, error) {
	for _, p := range m.projects {
		if p.NamespaceID == ns {
			return p, nil
		}
	}
	return nil, fmt.Errorf("project not found by namespace: %s", ns)
}

func (m *e2eShareProjectLookup) ListByUser(_ context.Context, _ uuid.UUID) ([]model.Project, error) {
	out := make([]model.Project, 0, len(m.projects))
	for _, p := range m.projects {
		out = append(out, *p)
	}
	return out, nil
}

func (m *e2eShareProjectLookup) Create(_ context.Context, _ *model.Project) error {
	return fmt.Errorf("Create not supported in share e2e fixture")
}

func (m *e2eShareProjectLookup) UpdateDescription(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

type shareE2EEnv struct {
	Server      *httptest.Server
	DB          storage.DB
	User        *model.User
	OAuthRepo   *storage.OAuthRepo
	ShareRepo   *storage.ShareTokenRepo
	ShareSvc    *service.ShareTokenService
	ProjectA    *model.Project
	ProjectB    *model.Project
	OAuthClient *model.OAuthClient
}

func newShareE2EEnv(t *testing.T) *shareE2EEnv {
	t.Helper()

	db := e2eTestDB(t)
	user := e2eTestUser(t, db)

	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)
	apiKeyRepo := storage.NewAPIKeyRepo(db)
	shareRepo := storage.NewShareTokenRepo(db)
	shareSvc := service.NewShareTokenService(shareRepo, oauthRepo)

	nsID := user.NamespaceID
	ns := &model.Namespace{ID: nsID, Path: "/users/sharee2e", Depth: 2}

	// Each project needs its own child namespace; projects.namespace_id is
	// UNIQUE. Seed the project namespaces directly so we can then seed the
	// projects.
	projectANS := uuid.New()
	projectBNS := uuid.New()
	for _, n := range []struct {
		id   uuid.UUID
		slug string
	}{{projectANS, "alpha-ns"}, {projectBNS, "beta-ns"}} {
		insertNS := `INSERT INTO namespaces (id, name, slug, kind, parent_id, path, depth) VALUES (?, ?, ?, 'project', ?, ?, 3)`
		if _, err := db.Exec(context.Background(), insertNS,
			n.id.String(), n.slug, n.id.String(), nsID.String(), "/users/sharee2e/"+n.slug,
		); err != nil {
			t.Fatalf("seed namespace %s: %v", n.slug, err)
		}
	}

	projectA := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      projectANS,
		OwnerNamespaceID: nsID,
		Name:             "Alpha",
		Slug:             "alpha",
	}
	projectB := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      projectBNS,
		OwnerNamespaceID: nsID,
		Name:             "Beta",
		Slug:             "beta",
	}

	for _, p := range []*model.Project{projectA, projectB} {
		insert := `INSERT INTO projects (id, namespace_id, owner_namespace_id, name, slug, default_tags, settings) VALUES (?, ?, ?, ?, ?, '[]', '{}')`
		if _, err := db.Exec(context.Background(), insert,
			p.ID.String(), p.NamespaceID.String(), p.OwnerNamespaceID.String(), p.Name, p.Slug,
		); err != nil {
			t.Fatalf("seed project %s: %v", p.Slug, err)
		}
	}

	projectLookup := &e2eShareProjectLookup{projects: []*model.Project{projectA, projectB}}
	nsLookup := &e2eNamespaceLookup{ns: ns}

	oauthSrv := auth.NewOAuthServer(oauthRepo, userRepo, e2eJWTSecret).
		WithShareTokens(shareSvc, projectLookup)
	authMw := auth.NewAuthMiddleware(apiKeyRepo, userRepo, e2eJWTSecret, nil).
		WithShareTokens(shareSvc, shareRepo)

	rl := auth.NewRateLimiter(1000, 2000, 0, 0)
	t.Cleanup(rl.Stop)

	promMetrics := metrics.New()

	memRepo := newE2EMemoryRepo()
	storeSvc := service.NewStoreService(memRepo, projectLookup, nsLookup, &e2eIngestionLogRepo{}, &e2eEnrichmentQueueRepo{}, nil)
	recallSvc := service.NewRecallService(memRepo, projectLookup, nsLookup, nil, nil, nil, nil)
	forgetSvc := service.NewForgetService(memRepo, projectLookup, nil, nil)
	updateSvc := service.NewUpdateService(memRepo, projectLookup, nil, nil, &e2eEnrichmentQueueRepo{})
	batchStoreSvc := service.NewBatchStoreService(memRepo, projectLookup, nsLookup, &e2eIngestionLogRepo{}, &e2eEnrichmentQueueRepo{}, nil)

	mcpDeps := mcp.Dependencies{
		Backend:       storage.BackendSQLite,
		Store:         storeSvc,
		Recall:        recallSvc,
		Forget:        forgetSvc,
		Update:        updateSvc,
		BatchStore:    batchStoreSvc,
		ProjectRepo:   projectLookup,
		UserRepo:      &e2eUserRepoMCP{user: user},
		NamespaceRepo: nsLookup,
		Metrics:       metrics.New(),
	}
	mcpSrv := mcp.NewServer(mcpDeps)

	// Register an OAuth client we can drive the consent flow against.
	clientSecretHash := "fake-hash"
	oauthClient := &model.OAuthClient{
		ClientID:       "share-e2e-" + uuid.New().String()[:8],
		ClientSecret:   &clientSecretHash,
		Name:           "Share E2E Test Client",
		RedirectURIs:   []string{"https://example.com/cb"},
		GrantTypes:     []string{"authorization_code"},
		AutoRegistered: false,
	}
	if err := oauthRepo.CreateClient(context.Background(), oauthClient); err != nil {
		t.Fatalf("create oauth client: %v", err)
	}

	handlers := Handlers{
		MCP:                    mcpSrv.Handler(),
		OAuthAuthorize:         oauthSrv.AuthorizeHandler(),
		OAuthAuthorizeContext:  oauthSrv.AuthorizeContextHandler(),
		OAuthSharePreview:      oauthSrv.SharePreviewHandler(),
		OAuthToken:             oauthSrv.TokenHandler(),
		OAuthRegister:          oauthSrv.RegisterClientHandler(),
		OAuthUserInfo:          oauthSrv.UserInfoHandler(),
		OAuthMetadata:          oauthSrv.MetadataHandler(),
		OAuthProtectedResource: oauthSrv.ProtectedResourceHandler(),
		ShareAccept:            oauthSrv.ShareAcceptHandler(),
		Health: func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	}

	cfg := RouterConfig{
		AuthMiddleware: authMw,
		RateLimiter:    rl,
		Metrics:        promMetrics,
	}
	router := NewRouter(cfg, handlers)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	return &shareE2EEnv{
		Server:      ts,
		DB:          db,
		User:        user,
		OAuthRepo:   oauthRepo,
		ShareRepo:   shareRepo,
		ShareSvc:    shareSvc,
		ProjectA:    projectA,
		ProjectB:    projectB,
		OAuthClient: oauthClient,
	}
}

// driveShareConsent drives the full consent + token exchange flow with a
// share-paste (no resource indicator), returning the resulting access JWT.
func driveShareConsent(t *testing.T, env *shareE2EEnv, secret string) string {
	t.Helper()
	return driveShareConsentWithResource(t, env, secret, "")
}

// initializeMCPSession runs the MCP initialize handshake and returns the
// Mcp-Session-Id required for subsequent tools/call requests.
func initializeMCPSession(t *testing.T, env *shareE2EEnv, accessToken string) string {
	t.Helper()
	initReq := e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": "share-e2e", "version": "1.0"},
		},
	}
	resp := e2eMCPPost(t, env.Server.URL, accessToken, initReq, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("initialize: status %d, body: %s", resp.StatusCode, bodyBytes)
	}
	sessionID := resp.Header.Get("Mcp-Session-Id")
	if sessionID == "" {
		t.Fatal("initialize: no Mcp-Session-Id header in response")
	}

	// Send the initialized notification so the server transitions to "ready"
	// state before we send tools/call. mcp-go enforces this ordering.
	notif := e2eJSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	}
	notifResp := e2eMCPPost(t, env.Server.URL, accessToken, notif, sessionID)
	_ = notifResp.Body.Close()

	return sessionID
}

// callRecall issues a tools/call recall request against /mcp with the given
// Bearer token. Returns (succeeded, errorText). Initializes a session if
// none is supplied.
func callRecall(t *testing.T, env *shareE2EEnv, accessToken, projectSlug string) (bool, string) {
	t.Helper()
	sessionID := initializeMCPSession(t, env, accessToken)

	args := map[string]any{"query": "anything"}
	if projectSlug != "" {
		args["project"] = projectSlug
	}
	rpcReq := e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params: map[string]any{
			"name":      "recall",
			"arguments": args,
		},
	}
	resp := e2eMCPPost(t, env.Server.URL, accessToken, rpcReq, sessionID)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusUnauthorized {
		return false, "unauthorized"
	}

	rpc := e2eParseJSONRPC(t, resp)
	if rpc.Error != nil {
		return false, rpc.Error.Message
	}

	// Tool errors come back in the result envelope's content[].text with
	// isError=true, NOT through the JSON-RPC error channel.
	var toolResult struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(rpc.Result, &toolResult); err != nil {
		t.Fatalf("decode tool result: %v (raw: %s)", err, string(rpc.Result))
	}
	text := ""
	if len(toolResult.Content) > 0 {
		text = toolResult.Content[0].Text
	}
	if toolResult.IsError ||
		strings.Contains(text, "not authorized") ||
		strings.Contains(text, "must specify project") ||
		strings.Contains(text, "project not found") ||
		strings.Contains(text, "share-bearer") {
		return false, text
	}
	return true, text
}

func TestShareE2E_OAuthChain_EnforcesScoping(t *testing.T) {
	env := newShareE2EEnv(t)

	// Create a share with read access to projectA only.
	result, err := env.ShareSvc.Create(context.Background(), service.CreateShareRequest{
		OwnerUserID: env.User.ID,
		Name:        "Alpha-only share",
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		Grants: []model.ShareTokenGrant{{
			ProjectID:  env.ProjectA.ID,
			Permission: model.SharePermissionRead,
		}},
	})
	if err != nil {
		t.Fatalf("create share: %v", err)
	}

	accessToken := driveShareConsent(t, env, result.RawSecret)

	t.Run("recall on allowlisted project succeeds", func(t *testing.T) {
		ok, msg := callRecall(t, env, accessToken, env.ProjectA.Slug)
		if !ok {
			t.Fatalf("recall on alpha (allowlisted) should succeed, got: %s", msg)
		}
	})

	t.Run("recall on off-allowlist project rejects", func(t *testing.T) {
		ok, msg := callRecall(t, env, accessToken, env.ProjectB.Slug)
		if ok {
			t.Fatalf("recall on beta (NOT in share) should reject; got success: %s", msg)
		}
		if !strings.Contains(msg, "not authorized") && !strings.Contains(msg, "share-bearer") {
			t.Fatalf("expected scoping rejection; got: %s", msg)
		}
	})

	t.Run("recall without project arg rejects (no global fan-out)", func(t *testing.T) {
		ok, msg := callRecall(t, env, accessToken, "")
		if ok {
			t.Fatalf("recall with no project should reject for share-bearer; got success: %s", msg)
		}
		if !strings.Contains(msg, "must specify project") {
			t.Fatalf("expected 'must specify project' rejection; got: %s", msg)
		}
	})
}

func TestShareE2E_GrantEditTakesEffectImmediately(t *testing.T) {
	env := newShareE2EEnv(t)

	// Start with grants on projectA only.
	result, err := env.ShareSvc.Create(context.Background(), service.CreateShareRequest{
		OwnerUserID: env.User.ID,
		Name:        "Grant edit test",
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		Grants: []model.ShareTokenGrant{{
			ProjectID:  env.ProjectA.ID,
			Permission: model.SharePermissionRead,
		}},
	})
	if err != nil {
		t.Fatalf("create share: %v", err)
	}

	accessToken := driveShareConsent(t, env, result.RawSecret)

	// Initial state: beta is NOT allowed.
	if ok, _ := callRecall(t, env, accessToken, env.ProjectB.Slug); ok {
		t.Fatal("pre-edit: recall on beta should reject")
	}

	// Owner adds projectB to the share's grants.
	newGrants := []model.ShareTokenGrant{
		{ProjectID: env.ProjectA.ID, Permission: model.SharePermissionRead},
		{ProjectID: env.ProjectB.ID, Permission: model.SharePermissionRead},
	}
	if err := env.ShareSvc.SetGrants(context.Background(), env.User.ID, result.Share.ID, newGrants); err != nil {
		t.Fatalf("set grants: %v", err)
	}

	// The SAME access token (no refresh) should now succeed on beta: the
	// middleware re-resolves grants on every request, so edits take effect
	// without a token rotation.
	if ok, msg := callRecall(t, env, accessToken, env.ProjectB.Slug); !ok {
		t.Fatalf("post-edit: recall on beta should succeed, got: %s", msg)
	}

	// Owner removes projectA.
	postRemovalGrants := []model.ShareTokenGrant{
		{ProjectID: env.ProjectB.ID, Permission: model.SharePermissionRead},
	}
	if err := env.ShareSvc.SetGrants(context.Background(), env.User.ID, result.Share.ID, postRemovalGrants); err != nil {
		t.Fatalf("set grants (remove A): %v", err)
	}

	// Alpha now rejects; Beta still passes.
	if ok, _ := callRecall(t, env, accessToken, env.ProjectA.Slug); ok {
		t.Fatal("post-removal: recall on alpha should reject (removed from share)")
	}
	if ok, msg := callRecall(t, env, accessToken, env.ProjectB.Slug); !ok {
		t.Fatalf("post-removal: recall on beta should still succeed, got: %s", msg)
	}
}

func TestShareE2E_RevokeKillsActiveToken(t *testing.T) {
	env := newShareE2EEnv(t)

	result, err := env.ShareSvc.Create(context.Background(), service.CreateShareRequest{
		OwnerUserID: env.User.ID,
		Name:        "Revoke test",
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		Grants: []model.ShareTokenGrant{{
			ProjectID:  env.ProjectA.ID,
			Permission: model.SharePermissionRead,
		}},
	})
	if err != nil {
		t.Fatalf("create share: %v", err)
	}

	accessToken := driveShareConsent(t, env, result.RawSecret)

	if ok, msg := callRecall(t, env, accessToken, env.ProjectA.Slug); !ok {
		t.Fatalf("pre-revoke recall should succeed; got: %s", msg)
	}

	// Owner revokes the share.
	if err := env.ShareSvc.Revoke(context.Background(), env.User.ID, result.Share.ID); err != nil {
		t.Fatalf("revoke share: %v", err)
	}

	// The same access token must now fail at the auth boundary (401),
	// regardless of JWT expiry, because the middleware re-checks the
	// share's revoked_at on every request. We send an initialize call
	// since that is the simplest request that exercises the auth path.
	initReq := e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params:  map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "x", "version": "1"}},
	}
	resp := e2eMCPPost(t, env.Server.URL, accessToken, initReq, "")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("post-revoke: expected 401, got %d", resp.StatusCode)
	}
}

// TestShareE2E_BearerDirect_ScopingMatchesOAuth verifies that pasting
// nram_s_<secret> directly as Bearer enforces the SAME per-project scoping
// as the OAuth-bound chain: the design promises identical behavior.
func TestShareE2E_BearerDirect_ScopingMatchesOAuth(t *testing.T) {
	env := newShareE2EEnv(t)
	result, err := env.ShareSvc.Create(context.Background(), service.CreateShareRequest{
		OwnerUserID: env.User.ID,
		Name:        "Bearer scoping test",
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		Grants: []model.ShareTokenGrant{{
			ProjectID:  env.ProjectA.ID,
			Permission: model.SharePermissionRead,
		}},
	})
	if err != nil {
		t.Fatalf("create share: %v", err)
	}

	t.Run("bearer-direct on allowlisted project succeeds", func(t *testing.T) {
		if ok, msg := callRecall(t, env, result.RawSecret, env.ProjectA.Slug); !ok {
			t.Fatalf("bearer-direct on alpha should succeed, got: %s", msg)
		}
	})
	t.Run("bearer-direct on off-allowlist project rejects", func(t *testing.T) {
		if ok, msg := callRecall(t, env, result.RawSecret, env.ProjectB.Slug); ok {
			t.Fatalf("bearer-direct on beta should reject; got success: %s", msg)
		}
	})
	t.Run("bearer-direct without project arg rejects", func(t *testing.T) {
		if ok, msg := callRecall(t, env, result.RawSecret, ""); ok {
			t.Fatalf("bearer-direct with no project should reject; got success: %s", msg)
		}
	})
}

// TestShareE2E_BearerDirect_OneShotConsumedRejected verifies the design
// decision recorded 2026-05-27: once a one-shot share has been consumed via
// the OAuth consent flow, the bearer-direct path is rejected outright. The
// recipient must either use the OAuth chain (still valid) or request a
// fresh share from the owner.
func TestShareE2E_BearerDirect_OneShotConsumedRejected(t *testing.T) {
	env := newShareE2EEnv(t)
	result, err := env.ShareSvc.Create(context.Background(), service.CreateShareRequest{
		OwnerUserID: env.User.ID,
		Name:        "One-shot consume test",
		IsOneShot:   true,
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		Grants: []model.ShareTokenGrant{{
			ProjectID:  env.ProjectA.ID,
			Permission: model.SharePermissionRead,
		}},
	})
	if err != nil {
		t.Fatalf("create one-shot share: %v", err)
	}

	// Drive the consent flow: that path consumes the one-shot.
	_ = driveShareConsent(t, env, result.RawSecret)

	// Now try bearer-direct with the same secret. Must reject (one-shot is
	// no longer reusable as a Bearer credential once consumed).
	client := e2eNoRedirectClient()
	rpcReq := e2eJSONRPCRequest{
		JSONRPC: "2.0", ID: 1, Method: "initialize",
		Params: map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "x", "version": "1"}},
	}
	body, _ := json.Marshal(rpcReq)
	req, _ := http.NewRequest(http.MethodPost, env.Server.URL+"/mcp", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+result.RawSecret)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("bearer-direct probe: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bearer-direct on consumed one-shot must return 401, got %d", resp.StatusCode)
	}
}

// TestShareE2E_ExpiredShareRejected verifies the middleware rejects shares
// past their expires_at, on both the OAuth chain and bearer-direct paths.
func TestShareE2E_ExpiredShareRejected(t *testing.T) {
	env := newShareE2EEnv(t)
	ctx := context.Background()

	// Create a share, then directly stamp expires_at into the past. The
	// service won't allow expires_at < now() at create time, so we mutate
	// post-creation.
	result, err := env.ShareSvc.Create(ctx, service.CreateShareRequest{
		OwnerUserID: env.User.ID,
		Name:        "Expiry test",
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		Grants: []model.ShareTokenGrant{{
			ProjectID:  env.ProjectA.ID,
			Permission: model.SharePermissionRead,
		}},
	})
	if err != nil {
		t.Fatalf("create share: %v", err)
	}
	past := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
	if _, err := env.DB.Exec(ctx, `UPDATE share_tokens SET expires_at = ? WHERE id = ?`,
		past, result.Share.ID.String()); err != nil {
		t.Fatalf("backdate expiry: %v", err)
	}

	// Bearer-direct must reject as expired.
	client := e2eNoRedirectClient()
	rpcReq := e2eJSONRPCRequest{
		JSONRPC: "2.0", ID: 1, Method: "initialize",
		Params: map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "x", "version": "1"}},
	}
	body, _ := json.Marshal(rpcReq)
	req, _ := http.NewRequest(http.MethodPost, env.Server.URL+"/mcp", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+result.RawSecret)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("bearer-direct probe: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bearer-direct on expired share must return 401, got %d", resp.StatusCode)
	}
}

// TestShareE2E_SweepRevokesZeroGrantShares verifies the project-delete
// cascade post-commit sweep: when the deleted project leaves a share with
// zero grants, the share is auto-revoked so it does not linger as "active"
// in the owner's UI.
func TestShareE2E_SweepRevokesZeroGrantShares(t *testing.T) {
	env := newShareE2EEnv(t)
	ctx := context.Background()

	result, err := env.ShareSvc.Create(ctx, service.CreateShareRequest{
		OwnerUserID: env.User.ID,
		Name:        "Sweep test",
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		Grants: []model.ShareTokenGrant{{
			ProjectID:  env.ProjectA.ID,
			Permission: model.SharePermissionRead,
		}},
	})
	if err != nil {
		t.Fatalf("create share: %v", err)
	}

	// Pre-condition: share is active.
	pre, _ := env.ShareRepo.GetByID(ctx, result.Share.ID)
	if pre.RevokedAt != nil {
		t.Fatal("share should start active")
	}

	// Delete projectA's grant directly (simulates the FK cascade outcome
	// without invoking the full project-delete tx, which fails in this
	// minimal e2e env because it expects a "global" project for token-usage
	// reassignment that this fixture does not stand up).
	if _, err := env.DB.Exec(ctx,
		`DELETE FROM share_token_grants WHERE project_id = ?`, env.ProjectA.ID.String(),
	); err != nil {
		t.Fatalf("delete grants: %v", err)
	}

	// Invoke the sweep directly: same call the project-delete service
	// would make post-commit.
	n, err := env.ShareSvc.SweepZeroGrantShares(ctx, env.User.ID)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 share swept, got %d", n)
	}

	post, _ := env.ShareRepo.GetByID(ctx, result.Share.ID)
	if post.RevokedAt == nil {
		t.Fatal("sweep should have revoked the zero-grant share")
	}
}

func TestShareE2E_BearerDirect_RestRejected(t *testing.T) {
	env := newShareE2EEnv(t)

	result, err := env.ShareSvc.Create(context.Background(), service.CreateShareRequest{
		OwnerUserID: env.User.ID,
		Name:        "Bearer REST test",
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		Grants: []model.ShareTokenGrant{{
			ProjectID:  env.ProjectA.ID,
			Permission: model.SharePermissionRead,
		}},
	})
	if err != nil {
		t.Fatalf("create share: %v", err)
	}

	client := e2eNoRedirectClient()

	// Share creds may reach /mcp. Smoke that path with a minimal initialize.
	req, _ := http.NewRequest(http.MethodGet, env.Server.URL+"/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+result.RawSecret)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("mcp probe: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		t.Fatalf("/mcp should accept share-bearer credentials, got 401")
	}

	// expectRejected fires an authenticated share-bearer request at a REST path
	// and asserts the guard returns 403. The body is irrelevant: the guard rejects
	// before any handler (or AskGate) reads it.
	expectRejected := func(method, path string) {
		t.Helper()
		req, _ := http.NewRequest(method, env.Server.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+result.RawSecret)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s must reject share-bearer with 403, got %d", path, resp.StatusCode)
		}
	}

	// Share creds must NOT reach REST endpoints: /v1/me/profile is a plain owner
	// route, and the ask routes sit under the same blanket guard, which rejects
	// before AskGate runs, so the 403 holds regardless of the ask feature flag.
	// Probing the ask routes explicitly locks in that they stay inside the guarded
	// group (a future move outside it, or a bespoke bypass, breaks here).
	expectRejected(http.MethodGet, "/v1/me/profile")
	expectRejected(http.MethodPost, "/v1/me/memories/ask")
	expectRejected(http.MethodPost, "/v1/projects/"+env.ProjectA.ID.String()+"/memories/ask")
}

// mcpPostTo posts a JSON-RPC request to an explicit MCP URL (used to target a
// per-share /mcp/{share_id} endpoint, which the e2eMCPPost helper cannot reach
// because it always appends "/mcp").
func mcpPostTo(t *testing.T, mcpURL, token string, rpcReq e2eJSONRPCRequest, sessionID string) *http.Response {
	t.Helper()
	body, err := json.Marshal(rpcReq)
	if err != nil {
		t.Fatalf("marshal rpc: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPost, mcpURL, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if sessionID != "" {
		req.Header.Set("Mcp-Session-Id", sessionID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("mcp post to %s: %v", mcpURL, err)
	}
	return resp
}

// recallAtURL runs initialize + recall against an explicit MCP URL and returns
// (statusCode, succeeded, message). A 401 at the initialize step short-circuits
// (the auth middleware rejects before the MCP transport runs).
func recallAtURL(t *testing.T, mcpURL, token, projectSlug string) (int, bool, string) {
	t.Helper()

	initReq := e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": "per-share-e2e", "version": "1.0"},
		},
	}
	resp := mcpPostTo(t, mcpURL, token, initReq, "")
	if resp.StatusCode == http.StatusUnauthorized {
		_ = resp.Body.Close()
		return http.StatusUnauthorized, false, "unauthorized"
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("initialize at %s: status %d, body: %s", mcpURL, resp.StatusCode, b)
	}
	sessionID := resp.Header.Get("Mcp-Session-Id")
	_ = resp.Body.Close()

	notif := e2eJSONRPCRequest{JSONRPC: "2.0", Method: "notifications/initialized"}
	_ = mcpPostTo(t, mcpURL, token, notif, sessionID).Body.Close()

	args := map[string]any{"query": "anything"}
	if projectSlug != "" {
		args["project"] = projectSlug
	}
	rpcReq := e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  map[string]any{"name": "recall", "arguments": args},
	}
	resp = mcpPostTo(t, mcpURL, token, rpcReq, sessionID)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusUnauthorized {
		return http.StatusUnauthorized, false, "unauthorized"
	}
	rpc := e2eParseJSONRPC(t, resp)
	if rpc.Error != nil {
		return resp.StatusCode, false, rpc.Error.Message
	}
	var toolResult struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(rpc.Result, &toolResult); err != nil {
		t.Fatalf("decode tool result: %v (raw: %s)", err, string(rpc.Result))
	}
	text := ""
	if len(toolResult.Content) > 0 {
		text = toolResult.Content[0].Text
	}
	if toolResult.IsError {
		return resp.StatusCode, false, text
	}
	return resp.StatusCode, true, text
}

// driveShareConsentWithResource is driveShareConsent with an RFC 8707 resource
// indicator threaded through both the authorize and token requests, so the
// minted access token is audience-bound to the per-share URL.
func driveShareConsentWithResource(t *testing.T, env *shareE2EEnv, secret, resource string) string {
	t.Helper()
	client := e2eNoRedirectClient()

	codeVerifier := "share-e2e-verifier-" + uuid.New().String()[:20] + "-padding-to-rfc7636-min-43chars"
	codeChallenge := e2eComputeCodeChallenge(codeVerifier)

	form := url.Values{}
	form.Set("client_id", env.OAuthClient.ClientID)
	form.Set("redirect_uri", "https://example.com/cb")
	form.Set("response_type", "code")
	form.Set("code_challenge", codeChallenge)
	form.Set("code_challenge_method", "S256")
	form.Set("share_token", secret)
	form.Set("auth_mode", "share")
	form.Set("decision", "approve")
	if resource != "" {
		form.Set("resource", resource)
	}

	req, _ := http.NewRequest(http.MethodPost, env.Server.URL+"/authorize", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("consent: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("consent: expected 302, got %d; body: %s", resp.StatusCode, body)
	}

	loc, _ := url.Parse(resp.Header.Get("Location"))
	code := loc.Query().Get("code")
	if code == "" {
		t.Fatalf("consent: no code in redirect: %s", resp.Header.Get("Location"))
	}

	tokenForm := url.Values{}
	tokenForm.Set("grant_type", "authorization_code")
	tokenForm.Set("code", code)
	tokenForm.Set("redirect_uri", "https://example.com/cb")
	tokenForm.Set("client_id", env.OAuthClient.ClientID)
	tokenForm.Set("code_verifier", codeVerifier)
	if resource != "" {
		tokenForm.Set("resource", resource)
	}

	resp, err = client.PostForm(env.Server.URL+"/token", tokenForm)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("token: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var tokResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &tokResp); err != nil {
		t.Fatalf("decode token: %v", err)
	}
	if tokResp.AccessToken == "" {
		t.Fatal("token: empty access_token")
	}
	return tokResp.AccessToken
}

// TestShareE2E_PerShareURL_OAuthRoundTrip exercises the full per-share connector
// URL path: path-scoped discovery, consent bound to the per-share resource, an
// MCP tool call at /mcp/{share_id}, and rejection of that token at both the bare
// /mcp and a different share's URL.
func TestShareE2E_PerShareURL_OAuthRoundTrip(t *testing.T) {
	env := newShareE2EEnv(t)

	result, err := env.ShareSvc.Create(context.Background(), service.CreateShareRequest{
		OwnerUserID: env.User.ID,
		Name:        "per-share-url share",
		ExpiresAt:   time.Now().Add(24 * time.Hour),
		Grants: []model.ShareTokenGrant{{
			ProjectID:  env.ProjectA.ID,
			Permission: model.SharePermissionRead,
		}},
	})
	if err != nil {
		t.Fatalf("create share: %v", err)
	}
	shareID := result.Share.ID
	perShareURL := env.Server.URL + "/mcp/" + shareID.String()

	// 1. Path-scoped discovery advertises the per-share resource.
	metaResp, err := http.Get(env.Server.URL + "/.well-known/oauth-protected-resource/mcp/" + shareID.String())
	if err != nil {
		t.Fatalf("discovery: %v", err)
	}
	var meta struct {
		Resource string `json:"resource"`
	}
	_ = json.NewDecoder(metaResp.Body).Decode(&meta)
	_ = metaResp.Body.Close()
	if meta.Resource != perShareURL {
		t.Fatalf("discovery resource = %q, want %q", meta.Resource, perShareURL)
	}

	// 2. Consent bound to the per-share resource yields an audience-bound token.
	token := driveShareConsentWithResource(t, env, result.RawSecret, perShareURL)

	// 3. recall on the allowlisted project at the per-share URL succeeds.
	if code, ok, msg := recallAtURL(t, perShareURL, token, env.ProjectA.Slug); !ok {
		t.Fatalf("recall at per-share URL failed (code %d): %s", code, msg)
	}

	// 4. The same token is rejected at the bare /mcp (audience mismatch)...
	if code, _, _ := recallAtURL(t, env.Server.URL+"/mcp", token, env.ProjectA.Slug); code != http.StatusUnauthorized {
		t.Fatalf("per-share token at bare /mcp: code %d, want 401", code)
	}
	// ...and at a different share's URL.
	otherURL := env.Server.URL + "/mcp/" + uuid.New().String()
	if code, _, _ := recallAtURL(t, otherURL, token, env.ProjectA.Slug); code != http.StatusUnauthorized {
		t.Fatalf("per-share token at another share URL: code %d, want 401", code)
	}
}
