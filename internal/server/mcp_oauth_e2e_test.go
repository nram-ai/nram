package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/api"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/config"
	"github.com/nram-ai/nram/internal/mcp"
	"github.com/nram-ai/nram/internal/migration"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// ---------------------------------------------------------------------------
// Shared test secret and helpers
// ---------------------------------------------------------------------------

var e2eJWTSecret = []byte("e2e-test-secret-32-bytes-long!!!")

// ---------------------------------------------------------------------------
// JSON-RPC types
// ---------------------------------------------------------------------------

type e2eJSONRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

type e2eJSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *e2eJSONRPCError `json:"error,omitempty"`
}

type e2eJSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ---------------------------------------------------------------------------
// Database setup (same pattern as oauth_flow_test.go)
// ---------------------------------------------------------------------------

func e2eTestDB(t *testing.T) storage.DB {
	t.Helper()

	tmpDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("e2eTestDB: getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("e2eTestDB: chdir: %v", err)
	}
	t.Cleanup(func() { os.Chdir(origDir) })

	db, err := storage.Open(config.DatabaseConfig{})
	if err != nil {
		t.Fatalf("e2eTestDB: open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	migrator, err := migration.NewMigrator(db.DB(), db.Backend())
	if err != nil {
		t.Fatalf("e2eTestDB: migrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("e2eTestDB: migrate up: %v", err)
	}

	return db
}

// e2eTestUser creates org + namespace + user hierarchy and returns the user.
func e2eTestUser(t *testing.T, db storage.DB) *model.User {
	t.Helper()
	ctx := context.Background()

	nsRepo := storage.NewNamespaceRepo(db)
	orgRepo := storage.NewOrganizationRepo(db)
	userRepo := storage.NewUserRepo(db)

	rootID := uuid.MustParse("00000000-0000-0000-0000-000000000000")

	orgNSID := uuid.New()
	orgNS := &model.Namespace{
		ID:       orgNSID,
		Name:     "E2E Test Org NS",
		Slug:     orgNSID.String(),
		Kind:     "org",
		ParentID: &rootID,
		Path:     orgNSID.String(),
		Depth:    1,
	}
	if err := nsRepo.Create(ctx, orgNS); err != nil {
		t.Fatalf("e2eTestUser: create org namespace: %v", err)
	}

	org := &model.Organization{
		NamespaceID: orgNSID,
		Name:        "E2E Test Org",
		Slug:        "e2e-org-" + orgNSID.String()[:8],
	}
	if err := orgRepo.Create(ctx, org); err != nil {
		t.Fatalf("e2eTestUser: create org: %v", err)
	}

	user := &model.User{
		Email:       "e2e-" + uuid.New().String()[:8] + "@example.com",
		DisplayName: "E2E Test User",
		OrgID:       org.ID,
		Role:        "admin",
	}
	if err := userRepo.Create(ctx, user, nsRepo, nil, orgNS.Path); err != nil {
		t.Fatalf("e2eTestUser: create user: %v", err)
	}
	return user
}

// ---------------------------------------------------------------------------
// Mock MCP service dependencies (in-memory memory store that actually works)
// ---------------------------------------------------------------------------

type e2eMemoryRepo struct {
	mu       sync.Mutex
	memories map[uuid.UUID]*model.Memory
}

func newE2EMemoryRepo() *e2eMemoryRepo {
	return &e2eMemoryRepo{memories: make(map[uuid.UUID]*model.Memory)}
}

func (m *e2eMemoryRepo) Create(_ context.Context, mem *model.Memory) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mem.ID == uuid.Nil {
		mem.ID = uuid.New()
	}
	clone := *mem
	m.memories[mem.ID] = &clone
	return nil
}

func (m *e2eMemoryRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mem, ok := m.memories[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	clone := *mem
	return &clone, nil
}

func (m *e2eMemoryRepo) LookupByContentHash(_ context.Context, namespaceID uuid.UUID, hash string) (*model.Memory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, mem := range m.memories {
		if mem.NamespaceID != namespaceID {
			continue
		}
		memHash := mem.ContentHash
		if memHash == "" {
			memHash = storage.HashContent(mem.Content)
		}
		if memHash == hash {
			clone := *mem
			return &clone, nil
		}
	}
	return nil, sql.ErrNoRows
}

func (m *e2eMemoryRepo) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []model.Memory
	for _, id := range ids {
		if mem, ok := m.memories[id]; ok {
			out = append(out, *mem)
		}
	}
	return out, nil
}

func (m *e2eMemoryRepo) ListByNamespace(_ context.Context, _ uuid.UUID, limit, _ int) ([]model.Memory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []model.Memory
	for _, mem := range m.memories {
		out = append(out, *mem)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *e2eMemoryRepo) ListByNamespaceFiltered(_ context.Context, _ uuid.UUID, filters storage.MemoryListFilters, limit, _ int) ([]model.Memory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []model.Memory
	for _, mem := range m.memories {
		if filters.HideSuperseded && mem.SupersededBy != nil {
			continue
		}
		out = append(out, *mem)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *e2eMemoryRepo) Update(_ context.Context, mem *model.Memory) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.memories[mem.ID] = mem
	return nil
}

func (m *e2eMemoryRepo) SoftDelete(_ context.Context, id uuid.UUID, _ uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.memories, id)
	return nil
}

func (m *e2eMemoryRepo) HardDelete(_ context.Context, id uuid.UUID, _ uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.memories, id)
	return nil
}

// e2eMCPStoreResult mirrors the slim mcpStoreResponse projection in
// internal/mcp/projection_store.go. Defined here because that type is
// unexported; if the projection grows a new field, update this struct so
// drift is caught at unmarshal time.
type e2eMCPStoreResult struct {
	ID               uuid.UUID `json:"id"`
	ProjectSlug      string    `json:"project_slug"`
	Enriched         bool      `json:"enriched"`
	EnrichmentQueued bool      `json:"enrichment_queued"`
}

// Mock repos that satisfy the interfaces needed by services

type e2eProjectLookup struct {
	project *model.Project
}

func (m *e2eProjectLookup) GetBySlug(_ context.Context, _ uuid.UUID, slug string) (*model.Project, error) {
	if m.project != nil && m.project.Slug == slug {
		return m.project, nil
	}
	return nil, fmt.Errorf("project not found: %s", slug)
}

func (m *e2eProjectLookup) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	if m.project != nil && m.project.ID == id {
		return m.project, nil
	}
	return nil, fmt.Errorf("project not found: %s", id)
}

func (m *e2eProjectLookup) GetByNamespaceID(_ context.Context, namespaceID uuid.UUID) (*model.Project, error) {
	if m.project != nil && m.project.NamespaceID == namespaceID {
		return m.project, nil
	}
	return nil, fmt.Errorf("project not found for namespace: %s", namespaceID)
}

func (m *e2eProjectLookup) ListByUser(_ context.Context, _ uuid.UUID) ([]model.Project, error) {
	if m.project != nil {
		return []model.Project{*m.project}, nil
	}
	return nil, nil
}

func (m *e2eProjectLookup) Create(_ context.Context, p *model.Project) error {
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	*m.project = *p
	return nil
}

func (m *e2eProjectLookup) UpdateDescription(_ context.Context, _ uuid.UUID, desc string) error {
	if m.project != nil {
		m.project.Description = desc
	}
	return nil
}

type e2eNamespaceLookup struct {
	ns *model.Namespace
}

func (m *e2eNamespaceLookup) GetByID(_ context.Context, _ uuid.UUID) (*model.Namespace, error) {
	return m.ns, nil
}

func (m *e2eNamespaceLookup) Create(_ context.Context, _ *model.Namespace) error {
	return nil
}

type e2eUserRepoMCP struct {
	user *model.User
}

func (m *e2eUserRepoMCP) GetByID(_ context.Context, _ uuid.UUID) (*model.User, error) {
	return m.user, nil
}

type e2eIngestionLogRepo struct{}

func (m *e2eIngestionLogRepo) Create(_ context.Context, _ *model.IngestionLog) error { return nil }

type e2eTokenUsageRepo struct{}

func (m *e2eTokenUsageRepo) Record(_ context.Context, _ *model.TokenUsage) error { return nil }

type e2eEnrichmentQueueRepo struct{}

func (m *e2eEnrichmentQueueRepo) Enqueue(_ context.Context, _ *model.EnrichmentJob) error {
	return nil
}

type e2eLineageCreator struct{}

func (m *e2eLineageCreator) Create(_ context.Context, _ *model.MemoryLineage) error { return nil }

// ---------------------------------------------------------------------------
// Build the REAL production router for E2E tests
// ---------------------------------------------------------------------------

type e2eEnv struct {
	Server  *httptest.Server
	DB      storage.DB
	User    *model.User
	MemRepo *e2eMemoryRepo
}

func newE2EEnv(t *testing.T) *e2eEnv {
	t.Helper()

	db := e2eTestDB(t)
	user := e2eTestUser(t, db)

	// Build real repos
	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)
	apiKeyRepo := storage.NewAPIKeyRepo(db)

	// Real OAuth server
	oauthSrv := auth.NewOAuthServer(oauthRepo, userRepo, e2eJWTSecret)

	// Real auth middleware (with real API key validator)
	authMw := auth.NewAuthMiddleware(apiKeyRepo, userRepo, e2eJWTSecret)

	// Real rate limiter
	rl := auth.NewRateLimiter(1000, 2000, 0, 0)
	t.Cleanup(rl.Stop)

	// Real metrics
	metrics := api.NewMetrics()

	// MCP server with in-memory mock services that actually store/recall
	memRepo := newE2EMemoryRepo()
	nsID := user.NamespaceID
	ns := &model.Namespace{ID: nsID, Path: "/users/e2etest", Depth: 2}
	project := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      nsID,
		OwnerNamespaceID: nsID,
		Name:             "claude-test",
		Slug:             "claude-test",
	}

	projectLookup := &e2eProjectLookup{project: project}
	namespaceLookup := &e2eNamespaceLookup{ns: ns}

	storeSvc := service.NewStoreService(
		memRepo,
		projectLookup,
		namespaceLookup,
		&e2eIngestionLogRepo{},
		&e2eEnrichmentQueueRepo{},
	)

	recallSvc := service.NewRecallService(
		memRepo,
		projectLookup,
		namespaceLookup,
		nil, nil, nil, nil, nil,
	)

	forgetSvc := service.NewForgetService(
		memRepo,
		projectLookup,
		nil, nil,
	)

	updateSvc := service.NewUpdateService(
		memRepo,
		projectLookup,
		&e2eLineageCreator{},
		nil,
		nil,
		)

	batchStoreSvc := service.NewBatchStoreService(
		memRepo,
		projectLookup,
		namespaceLookup,
		&e2eIngestionLogRepo{},
		&e2eEnrichmentQueueRepo{},
		nil,
	)

	mcpDeps := mcp.Dependencies{
		Backend:       storage.BackendSQLite,
		Store:         storeSvc,
		Recall:        recallSvc,
		Forget:        forgetSvc,
		Update:        updateSvc,
		BatchStore:    batchStoreSvc,
		ProjectRepo:   projectLookup,
		UserRepo:      &e2eUserRepoMCP{user: user},
		NamespaceRepo: namespaceLookup,
	}
	mcpSrv := mcp.NewServer(mcpDeps)

	// Build Handlers struct — same wiring as production
	handlers := Handlers{
		MCP: mcpSrv.Handler(),

		// OAuth handlers
		OAuthAuthorize:         oauthSrv.AuthorizeHandler(),
		OAuthToken:             oauthSrv.TokenHandler(),
		OAuthRegister:          oauthSrv.RegisterClientHandler(),
		OAuthUserInfo:          oauthSrv.UserInfoHandler(),
		OAuthMetadata:          oauthSrv.MetadataHandler(),
		OAuthProtectedResource: oauthSrv.ProtectedResourceHandler(),

		// Health
		Health: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		},
	}

	// Build the REAL production router
	cfg := RouterConfig{
		AuthMiddleware: authMw,
		RateLimiter:    rl,
		Metrics:        metrics,
		// No setup guard — setup is "complete"
	}

	router := NewRouter(cfg, handlers)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	return &e2eEnv{
		Server:  ts,
		DB:      db,
		User:    user,
		MemRepo: memRepo,
	}
}

// ---------------------------------------------------------------------------
// PKCE helpers
// ---------------------------------------------------------------------------

func e2eComputeCodeChallenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

// ---------------------------------------------------------------------------
// Session cookie helper
// ---------------------------------------------------------------------------

func e2eCreateSessionCookie(t *testing.T, userID uuid.UUID) *http.Cookie {
	t.Helper()
	token, err := auth.GenerateJWT(userID, uuid.Nil, "admin", e2eJWTSecret, 10*time.Minute)
	if err != nil {
		t.Fatalf("failed to create session JWT: %v", err)
	}
	return &http.Cookie{
		Name:     "nram_session",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
	}
}

// ---------------------------------------------------------------------------
// HTTP client that does NOT follow redirects
// ---------------------------------------------------------------------------

func e2eNoRedirectClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// ---------------------------------------------------------------------------
// SSE / JSON-RPC response parser
// ---------------------------------------------------------------------------

func e2eParseJSONRPC(t *testing.T, resp *http.Response) *e2eJSONRPCResponse {
	t.Helper()

	ct := resp.Header.Get("Content-Type")
	bodyBytes, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if strings.HasPrefix(ct, "application/json") {
		var rpc e2eJSONRPCResponse
		if err := json.Unmarshal(bodyBytes, &rpc); err != nil {
			t.Fatalf("failed to unmarshal JSON-RPC response (body=%s): %v", string(bodyBytes), err)
		}
		return &rpc
	}

	if strings.HasPrefix(ct, "text/event-stream") {
		scanner := bufio.NewScanner(bytes.NewReader(bodyBytes))
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				data := strings.TrimPrefix(line, "data: ")
				var rpc e2eJSONRPCResponse
				if err := json.Unmarshal([]byte(data), &rpc); err != nil {
					continue
				}
				if rpc.JSONRPC == "2.0" {
					return &rpc
				}
			}
		}
		t.Fatalf("no JSON-RPC response found in SSE stream: %s", string(bodyBytes))
	}

	t.Fatalf("unexpected Content-Type %q, body: %s", ct, string(bodyBytes))
	return nil
}

// e2eMCPPost sends a JSON-RPC request to /mcp and returns the raw HTTP response.
func e2eMCPPost(t *testing.T, baseURL, token string, rpcReq e2eJSONRPCRequest, sessionID string) *http.Response {
	t.Helper()

	body, err := json.Marshal(rpcReq)
	if err != nil {
		t.Fatalf("failed to marshal JSON-RPC: %v", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, baseURL+"/mcp", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/event-stream")
	if token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+token)
	}
	if sessionID != "" {
		httpReq.Header.Set("Mcp-Session-Id", sessionID)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	return resp
}

// e2eExtractToolResultText extracts the text content from a tools/call response.
func e2eExtractToolResultText(t *testing.T, rpc *e2eJSONRPCResponse) string {
	t.Helper()

	var toolResult struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(rpc.Result, &toolResult); err != nil {
		t.Fatalf("failed to unmarshal tool result: %v (raw: %s)", err, string(rpc.Result))
	}
	if toolResult.IsError {
		for _, c := range toolResult.Content {
			if c.Type == "text" {
				t.Fatalf("tool returned error: %s", c.Text)
			}
		}
		t.Fatal("tool returned error with no text content")
	}
	for _, c := range toolResult.Content {
		if c.Type == "text" {
			return c.Text
		}
	}
	t.Fatal("no text content in tool result")
	return ""
}

// ===========================================================================
// TestE2E_ClaudeCode_OAuthToMCPToolCall
// ===========================================================================

func TestE2E_ClaudeCode_OAuthToMCPToolCall(t *testing.T) {
	env := newE2EEnv(t)
	client := e2eNoRedirectClient()
	baseURL := env.Server.URL

	// -----------------------------------------------------------------------
	// Step 1: Hit POST /mcp with no auth — expect 401 + WWW-Authenticate
	// -----------------------------------------------------------------------
	t.Log("Step 1: POST /mcp with no auth")
	initBody, _ := json.Marshal(e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Claude Code", "version": "1.0"},
		},
	})
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/mcp", bytes.NewReader(initBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("step 1: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("step 1: expected 401, got %d", resp.StatusCode)
	}

	wwwAuth := resp.Header.Get("WWW-Authenticate")
	if wwwAuth == "" {
		t.Fatal("step 1: missing WWW-Authenticate header")
	}
	t.Logf("step 1: WWW-Authenticate: %s", wwwAuth)

	// Parse resource_metadata URL from WWW-Authenticate header
	const prefix = `Bearer resource_metadata="`
	if !strings.HasPrefix(wwwAuth, prefix) {
		t.Fatalf("step 1: WWW-Authenticate doesn't start with expected prefix: %s", wwwAuth)
	}
	resourceMetaURL := strings.TrimSuffix(strings.TrimPrefix(wwwAuth, prefix), `"`)
	t.Logf("step 1: resource_metadata URL: %s", resourceMetaURL)

	// -----------------------------------------------------------------------
	// Step 2: GET /.well-known/oauth-protected-resource
	// -----------------------------------------------------------------------
	t.Log("Step 2: GET /.well-known/oauth-protected-resource")
	resp, err = client.Get(resourceMetaURL)
	if err != nil {
		t.Fatalf("step 2: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("step 2: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var protectedMeta struct {
		Resource             string   `json:"resource"`
		AuthorizationServers []string `json:"authorization_servers"`
		BearerMethods        []string `json:"bearer_methods_supported"`
	}
	if err := json.Unmarshal(body, &protectedMeta); err != nil {
		t.Fatalf("step 2: unmarshal: %v", err)
	}
	if len(protectedMeta.AuthorizationServers) == 0 {
		t.Fatal("step 2: no authorization_servers")
	}
	if protectedMeta.Resource != baseURL+"/mcp" {
		t.Fatalf("step 2: expected resource=%s/mcp, got %s", baseURL, protectedMeta.Resource)
	}
	t.Logf("step 2: authorization_servers[0]: %s", protectedMeta.AuthorizationServers[0])

	// -----------------------------------------------------------------------
	// Step 3: GET /.well-known/oauth-authorization-server
	// -----------------------------------------------------------------------
	t.Log("Step 3: GET /.well-known/oauth-authorization-server")
	resp, err = client.Get(baseURL + "/.well-known/oauth-authorization-server")
	if err != nil {
		t.Fatalf("step 3: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("step 3: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var authServerMeta struct {
		Issuer                        string   `json:"issuer"`
		AuthorizationEndpoint         string   `json:"authorization_endpoint"`
		TokenEndpoint                 string   `json:"token_endpoint"`
		RegistrationEndpoint          string   `json:"registration_endpoint"`
		CodeChallengeMethodsSupported []string `json:"code_challenge_methods_supported"`
	}
	if err := json.Unmarshal(body, &authServerMeta); err != nil {
		t.Fatalf("step 3: unmarshal: %v", err)
	}

	if authServerMeta.AuthorizationEndpoint != baseURL+"/authorize" {
		t.Fatalf("step 3: expected authorization_endpoint=%s/authorize, got %s", baseURL, authServerMeta.AuthorizationEndpoint)
	}
	if authServerMeta.TokenEndpoint != baseURL+"/token" {
		t.Fatalf("step 3: expected token_endpoint=%s/token, got %s", baseURL, authServerMeta.TokenEndpoint)
	}
	if authServerMeta.RegistrationEndpoint != baseURL+"/register" {
		t.Fatalf("step 3: expected registration_endpoint=%s/register, got %s", baseURL, authServerMeta.RegistrationEndpoint)
	}

	foundS256 := false
	for _, m := range authServerMeta.CodeChallengeMethodsSupported {
		if m == "S256" {
			foundS256 = true
			break
		}
	}
	if !foundS256 {
		t.Fatalf("step 3: S256 not in code_challenge_methods_supported: %v", authServerMeta.CodeChallengeMethodsSupported)
	}

	// -----------------------------------------------------------------------
	// Step 4: POST /register (dynamic client registration)
	// -----------------------------------------------------------------------
	t.Log("Step 4: POST /register")
	regBody, _ := json.Marshal(map[string]interface{}{
		"client_name":   "Claude Code",
		"redirect_uris": []string{"http://localhost:3000/callback"},
		"grant_types":   []string{"authorization_code", "refresh_token"},
	})
	resp, err = client.Post(authServerMeta.RegistrationEndpoint, "application/json", bytes.NewReader(regBody))
	if err != nil {
		t.Fatalf("step 4: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("step 4: expected 201, got %d; body: %s", resp.StatusCode, body)
	}

	var regResp struct {
		ClientID     string   `json:"client_id"`
		ClientSecret string   `json:"client_secret"`
		ClientName   string   `json:"client_name"`
		RedirectURIs []string `json:"redirect_uris"`
	}
	if err := json.Unmarshal(body, &regResp); err != nil {
		t.Fatalf("step 4: unmarshal: %v", err)
	}
	if regResp.ClientID == "" {
		t.Fatal("step 4: empty client_id")
	}
	t.Logf("step 4: client_id: %s", regResp.ClientID)

	// -----------------------------------------------------------------------
	// Step 5: Build authorization URL with PKCE
	// -----------------------------------------------------------------------
	t.Log("Step 5: Build PKCE authorization URL")
	codeVerifier := "e2e-test-verifier-which-is-at-least-43-characters-long-for-pkce"
	codeChallenge := e2eComputeCodeChallenge(codeVerifier)
	state := "e2e-test-state-" + uuid.New().String()[:8]
	redirectURI := "http://localhost:3000/callback"
	resource := baseURL + "/mcp"

	authURL := fmt.Sprintf(
		"%s?client_id=%s&redirect_uri=%s&response_type=code&code_challenge=%s&code_challenge_method=S256&state=%s&resource=%s",
		authServerMeta.AuthorizationEndpoint,
		url.QueryEscape(regResp.ClientID),
		url.QueryEscape(redirectURI),
		url.QueryEscape(codeChallenge),
		url.QueryEscape(state),
		url.QueryEscape(resource),
	)
	t.Logf("step 5: authorize URL: %s", authURL)

	// -----------------------------------------------------------------------
	// Step 6: GET /authorize with nram_session cookie
	// -----------------------------------------------------------------------
	t.Log("Step 6: GET /authorize with session cookie")
	authReq, _ := http.NewRequest(http.MethodGet, authURL, nil)
	sessionCookie := e2eCreateSessionCookie(t, env.User.ID)
	authReq.AddCookie(sessionCookie)

	resp, err = client.Do(authReq)
	if err != nil {
		t.Fatalf("step 6: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("step 6: expected 302, got %d", resp.StatusCode)
	}

	location := resp.Header.Get("Location")
	if location == "" {
		t.Fatal("step 6: missing Location header")
	}
	t.Logf("step 6: redirect Location: %s", location)

	redirectURL, err := url.Parse(location)
	if err != nil {
		t.Fatalf("step 6: parse Location: %v", err)
	}

	code := redirectURL.Query().Get("code")
	if code == "" {
		t.Fatal("step 6: missing code in redirect")
	}
	returnedState := redirectURL.Query().Get("state")
	if returnedState != state {
		t.Fatalf("step 6: state mismatch: expected %s, got %s", state, returnedState)
	}
	t.Logf("step 6: authorization code: %s...", code[:16])

	// -----------------------------------------------------------------------
	// Step 7: POST /token with authorization code + PKCE verifier + resource
	// -----------------------------------------------------------------------
	t.Log("Step 7: POST /token")
	tokenForm := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {redirectURI},
		"client_id":     {regResp.ClientID},
		"code_verifier": {codeVerifier},
		"resource":      {resource},
	}
	resp, err = client.PostForm(authServerMeta.TokenEndpoint, tokenForm)
	if err != nil {
		t.Fatalf("step 7: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("step 7: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		TokenType    string `json:"token_type"`
		ExpiresIn    int    `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		t.Fatalf("step 7: unmarshal: %v", err)
	}
	if tokenResp.AccessToken == "" {
		t.Fatal("step 7: empty access_token")
	}
	if tokenResp.RefreshToken == "" {
		t.Fatal("step 7: empty refresh_token")
	}
	t.Logf("step 7: access_token length: %d, refresh_token length: %d", len(tokenResp.AccessToken), len(tokenResp.RefreshToken))

	// Verify the access_token is a valid JWT with the correct audience
	claims := &auth.Claims{}
	tok, err := jwt.ParseWithClaims(tokenResp.AccessToken, claims, func(t *jwt.Token) (interface{}, error) {
		return e2eJWTSecret, nil
	})
	if err != nil {
		t.Fatalf("step 7: parse access_token JWT: %v", err)
	}
	if !tok.Valid {
		t.Fatal("step 7: access_token JWT invalid")
	}
	aud, _ := claims.GetAudience()
	foundAud := false
	for _, a := range aud {
		if a == resource {
			foundAud = true
			break
		}
	}
	if !foundAud {
		t.Fatalf("step 7: JWT audience %v does not contain %s", aud, resource)
	}
	sub, _ := claims.GetSubject()
	if sub != env.User.ID.String() {
		t.Fatalf("step 7: JWT subject %s != user ID %s", sub, env.User.ID)
	}

	// -----------------------------------------------------------------------
	// Step 8: POST /mcp with Bearer access_token — JSON-RPC initialize
	// -----------------------------------------------------------------------
	t.Log("Step 8: POST /mcp initialize")
	mcpResp := e2eMCPPost(t, baseURL, tokenResp.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Claude Code", "version": "1.0"},
		},
	}, "")

	if mcpResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(mcpResp.Body)
		mcpResp.Body.Close()
		t.Fatalf("step 8: expected 200, got %d; body: %s", mcpResp.StatusCode, string(bodyBytes))
	}

	rpcResp := e2eParseJSONRPC(t, mcpResp)
	if rpcResp.Error != nil {
		t.Fatalf("step 8: JSON-RPC error: code=%d msg=%s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	var initResult map[string]interface{}
	if err := json.Unmarshal(rpcResp.Result, &initResult); err != nil {
		t.Fatalf("step 8: unmarshal result: %v", err)
	}
	serverInfo, ok := initResult["serverInfo"].(map[string]interface{})
	if !ok {
		t.Fatal("step 8: missing serverInfo in result")
	}
	if name, _ := serverInfo["name"].(string); name != "nram" {
		t.Fatalf("step 8: expected serverInfo.name=nram, got %q", name)
	}

	sessionID := mcpResp.Header.Get("Mcp-Session-Id")
	t.Logf("step 8: Mcp-Session-Id: %s", sessionID)

	// -----------------------------------------------------------------------
	// Step 9: POST /mcp — initialized notification
	// -----------------------------------------------------------------------
	t.Log("Step 9: POST /mcp notifications/initialized")
	notifResp := e2eMCPPost(t, baseURL, tokenResp.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	}, sessionID)

	notifBody, _ := io.ReadAll(notifResp.Body)
	notifResp.Body.Close()
	if notifResp.StatusCode != http.StatusAccepted && notifResp.StatusCode != http.StatusOK {
		t.Fatalf("step 9: expected 202 or 200, got %d; body: %s", notifResp.StatusCode, string(notifBody))
	}

	// -----------------------------------------------------------------------
	// Step 10: POST /mcp — tools/list
	// -----------------------------------------------------------------------
	t.Log("Step 10: POST /mcp tools/list")
	toolsListResp := e2eMCPPost(t, baseURL, tokenResp.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/list",
	}, sessionID)

	if toolsListResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(toolsListResp.Body)
		toolsListResp.Body.Close()
		t.Fatalf("step 10: expected 200, got %d; body: %s", toolsListResp.StatusCode, string(bodyBytes))
	}

	toolsRPC := e2eParseJSONRPC(t, toolsListResp)
	if toolsRPC.Error != nil {
		t.Fatalf("step 10: JSON-RPC error: %s", toolsRPC.Error.Message)
	}

	var toolsList struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(toolsRPC.Result, &toolsList); err != nil {
		t.Fatalf("step 10: unmarshal: %v", err)
	}

	foundMemoryStore := false
	for _, tool := range toolsList.Tools {
		if tool.Name == "memory_store" {
			foundMemoryStore = true
			break
		}
	}
	if !foundMemoryStore {
		names := make([]string, 0, len(toolsList.Tools))
		for _, tool := range toolsList.Tools {
			names = append(names, tool.Name)
		}
		t.Fatalf("step 10: memory_store not found in tools list: %v", names)
	}

	// -----------------------------------------------------------------------
	// Step 11: POST /mcp — tools/call memory_store
	// -----------------------------------------------------------------------
	t.Log("Step 11: POST /mcp tools/call memory_store")
	storeCallResp := e2eMCPPost(t, baseURL, tokenResp.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/call",
		Params: map[string]interface{}{
			"name": "memory_store",
			"arguments": map[string]interface{}{
				"project": "claude-test",
				"content": "The auth service uses JWT with 1h expiry",
				"tags":    []string{"architecture", "auth"},
			},
		},
	}, sessionID)

	if storeCallResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(storeCallResp.Body)
		storeCallResp.Body.Close()
		t.Fatalf("step 11: expected 200, got %d; body: %s", storeCallResp.StatusCode, string(bodyBytes))
	}

	storeRPC := e2eParseJSONRPC(t, storeCallResp)
	if storeRPC.Error != nil {
		t.Fatalf("step 11: JSON-RPC error: %s", storeRPC.Error.Message)
	}

	storeText := e2eExtractToolResultText(t, storeRPC)
	var storeResult e2eMCPStoreResult
	if err := json.Unmarshal([]byte(storeText), &storeResult); err != nil {
		t.Fatalf("step 11: unmarshal store result: %v", err)
	}
	if storeResult.ID == uuid.Nil {
		t.Fatal("step 11: stored memory has nil ID")
	}
	if storeResult.ProjectSlug != "claude-test" {
		t.Fatalf("step 11: expected project_slug=claude-test, got %q", storeResult.ProjectSlug)
	}
	t.Logf("step 11: stored memory ID: %s", storeResult.ID)

	// -----------------------------------------------------------------------
	// Step 12: POST /mcp — tools/call memory_recall
	// -----------------------------------------------------------------------
	t.Log("Step 12: POST /mcp tools/call memory_recall")
	recallCallResp := e2eMCPPost(t, baseURL, tokenResp.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      4,
		Method:  "tools/call",
		Params: map[string]interface{}{
			"name": "memory_recall",
			"arguments": map[string]interface{}{
				"query":   "auth JWT expiry",
				"project": "claude-test",
			},
		},
	}, sessionID)

	if recallCallResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(recallCallResp.Body)
		recallCallResp.Body.Close()
		t.Fatalf("step 12: expected 200, got %d; body: %s", recallCallResp.StatusCode, string(bodyBytes))
	}

	recallRPC := e2eParseJSONRPC(t, recallCallResp)
	if recallRPC.Error != nil {
		t.Fatalf("step 12: JSON-RPC error: %s", recallRPC.Error.Message)
	}

	recallText := e2eExtractToolResultText(t, recallRPC)
	var recallResult service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResult); err != nil {
		t.Fatalf("step 12: unmarshal recall result: %v", err)
	}
	if len(recallResult.Memories) == 0 {
		t.Fatal("step 12: expected at least 1 recalled memory, got 0")
	}

	foundRecall := false
	for _, mem := range recallResult.Memories {
		if strings.Contains(mem.Content, "JWT with 1h expiry") {
			foundRecall = true
			break
		}
	}
	if !foundRecall {
		t.Fatal("step 12: stored memory not found in recall results")
	}
	t.Log("step 12: recall successfully returned stored memory")

	// -----------------------------------------------------------------------
	// Step 13: POST /token with refresh_token grant
	// -----------------------------------------------------------------------
	t.Log("Step 13: POST /token refresh_token grant")
	refreshForm := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {tokenResp.RefreshToken},
		"client_id":     {regResp.ClientID},
	}
	resp, err = client.PostForm(authServerMeta.TokenEndpoint, refreshForm)
	if err != nil {
		t.Fatalf("step 13: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("step 13: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var refreshResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &refreshResp); err != nil {
		t.Fatalf("step 13: unmarshal: %v", err)
	}
	if refreshResp.AccessToken == "" {
		t.Fatal("step 13: empty new access_token")
	}
	if refreshResp.RefreshToken == "" {
		t.Fatal("step 13: empty new refresh_token")
	}
	if refreshResp.RefreshToken == tokenResp.RefreshToken {
		t.Fatal("step 13: refresh token was not rotated (old == new)")
	}
	t.Log("step 13: refresh token rotated successfully")

	// Verify old refresh token is revoked (using it again should fail)
	retryForm := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {tokenResp.RefreshToken},
		"client_id":     {regResp.ClientID},
	}
	resp, err = client.PostForm(authServerMeta.TokenEndpoint, retryForm)
	if err != nil {
		t.Fatalf("step 13 retry: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("step 13: old refresh token should have been revoked but was accepted")
	}

	// -----------------------------------------------------------------------
	// Step 14: POST /mcp with NEW access_token — verify it works
	// -----------------------------------------------------------------------
	t.Log("Step 14: POST /mcp with refreshed token")
	recallResp2 := e2eMCPPost(t, baseURL, refreshResp.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      5,
		Method:  "tools/call",
		Params: map[string]interface{}{
			"name": "memory_recall",
			"arguments": map[string]interface{}{
				"query":   "auth JWT expiry",
				"project": "claude-test",
			},
		},
	}, sessionID)

	if recallResp2.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(recallResp2.Body)
		recallResp2.Body.Close()
		t.Fatalf("step 14: expected 200, got %d; body: %s", recallResp2.StatusCode, string(bodyBytes))
	}

	recallRPC2 := e2eParseJSONRPC(t, recallResp2)
	if recallRPC2.Error != nil {
		t.Fatalf("step 14: JSON-RPC error: %s", recallRPC2.Error.Message)
	}

	recallText2 := e2eExtractToolResultText(t, recallRPC2)
	var recallResult2 service.RecallResponse
	if err := json.Unmarshal([]byte(recallText2), &recallResult2); err != nil {
		t.Fatalf("step 14: unmarshal: %v", err)
	}
	if len(recallResult2.Memories) == 0 {
		t.Fatal("step 14: expected recalled memories with refreshed token, got 0")
	}
	t.Log("step 14: refreshed token works for MCP tool calls")
}

// ===========================================================================
// TestE2E_ClaudeDesktop_OAuthToMCPToolCall
// ===========================================================================

func TestE2E_ClaudeDesktop_OAuthToMCPToolCall(t *testing.T) {
	env := newE2EEnv(t)
	client := e2eNoRedirectClient()
	baseURL := env.Server.URL

	// Step 1: Hit POST /mcp with no auth — 401
	initBody, _ := json.Marshal(e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Claude Desktop", "version": "1.0"},
		},
	})
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/mcp", bytes.NewReader(initBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("step 1: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("step 1: expected 401, got %d", resp.StatusCode)
	}

	// Step 2-3: Discovery (same as Claude Code, skip to registration)
	resp, err = client.Get(baseURL + "/.well-known/oauth-authorization-server")
	if err != nil {
		t.Fatalf("discovery: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var meta struct {
		AuthorizationEndpoint string `json:"authorization_endpoint"`
		TokenEndpoint         string `json:"token_endpoint"`
		RegistrationEndpoint  string `json:"registration_endpoint"`
	}
	json.Unmarshal(body, &meta)

	// Step 4: Register with Claude Desktop redirect URI
	regBody, _ := json.Marshal(map[string]interface{}{
		"client_name":   "Claude Desktop",
		"redirect_uris": []string{"https://claude.ai/api/mcp/auth_callback"},
		"grant_types":   []string{"authorization_code", "refresh_token"},
	})
	resp, err = client.Post(meta.RegistrationEndpoint, "application/json", bytes.NewReader(regBody))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register: expected 201, got %d; body: %s", resp.StatusCode, body)
	}

	var regResp struct {
		ClientID string `json:"client_id"`
	}
	json.Unmarshal(body, &regResp)

	// Step 5-6: PKCE + authorize
	codeVerifier := "claude-desktop-verifier-which-is-at-least-43-characters-long-for-pkce"
	codeChallenge := e2eComputeCodeChallenge(codeVerifier)
	state := "desktop-state-" + uuid.New().String()[:8]
	redirectURI := "https://claude.ai/api/mcp/auth_callback"
	resource := baseURL + "/mcp"

	authURL := fmt.Sprintf(
		"%s?client_id=%s&redirect_uri=%s&response_type=code&code_challenge=%s&code_challenge_method=S256&state=%s&resource=%s",
		meta.AuthorizationEndpoint,
		url.QueryEscape(regResp.ClientID),
		url.QueryEscape(redirectURI),
		url.QueryEscape(codeChallenge),
		url.QueryEscape(state),
		url.QueryEscape(resource),
	)

	authReq, _ := http.NewRequest(http.MethodGet, authURL, nil)
	authReq.AddCookie(e2eCreateSessionCookie(t, env.User.ID))
	resp, err = client.Do(authReq)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("authorize: expected 302, got %d", resp.StatusCode)
	}

	location := resp.Header.Get("Location")
	redirectURL, _ := url.Parse(location)
	code := redirectURL.Query().Get("code")
	if code == "" {
		t.Fatal("authorize: no code in redirect")
	}
	returnedState := redirectURL.Query().Get("state")
	if returnedState != state {
		t.Fatalf("authorize: state mismatch: %s vs %s", state, returnedState)
	}

	// Verify the redirect goes to the Claude Desktop callback URL
	if !strings.HasPrefix(location, "https://claude.ai/api/mcp/auth_callback") {
		t.Fatalf("authorize: redirect not to Claude Desktop callback: %s", location)
	}

	// Step 7: Token exchange
	tokenForm := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {redirectURI},
		"client_id":     {regResp.ClientID},
		"code_verifier": {codeVerifier},
		"resource":      {resource},
	}
	resp, err = client.PostForm(meta.TokenEndpoint, tokenForm)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("token: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	json.Unmarshal(body, &tokenResp)
	if tokenResp.AccessToken == "" {
		t.Fatal("token: empty access_token")
	}

	// Steps 8-10: Initialize + notification + tools/list
	mcpResp := e2eMCPPost(t, baseURL, tokenResp.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Claude Desktop", "version": "1.0"},
		},
	}, "")

	if mcpResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(mcpResp.Body)
		mcpResp.Body.Close()
		t.Fatalf("initialize: expected 200, got %d; body: %s", mcpResp.StatusCode, string(bodyBytes))
	}

	rpcResp := e2eParseJSONRPC(t, mcpResp)
	if rpcResp.Error != nil {
		t.Fatalf("initialize: JSON-RPC error: %s", rpcResp.Error.Message)
	}

	var initResult map[string]interface{}
	json.Unmarshal(rpcResp.Result, &initResult)
	si, _ := initResult["serverInfo"].(map[string]interface{})
	if name, _ := si["name"].(string); name != "nram" {
		t.Fatalf("initialize: expected serverInfo.name=nram, got %q", name)
	}

	sessionID := mcpResp.Header.Get("Mcp-Session-Id")

	// Send initialized notification
	notifResp := e2eMCPPost(t, baseURL, tokenResp.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	}, sessionID)
	io.ReadAll(notifResp.Body)
	notifResp.Body.Close()

	// Tools/list
	toolsResp := e2eMCPPost(t, baseURL, tokenResp.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/list",
	}, sessionID)

	if toolsResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(toolsResp.Body)
		toolsResp.Body.Close()
		t.Fatalf("tools/list: expected 200, got %d; body: %s", toolsResp.StatusCode, string(bodyBytes))
	}

	toolsRPC := e2eParseJSONRPC(t, toolsResp)
	if toolsRPC.Error != nil {
		t.Fatalf("tools/list: JSON-RPC error: %s", toolsRPC.Error.Message)
	}

	var toolsList struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	json.Unmarshal(toolsRPC.Result, &toolsList)

	found := false
	for _, tool := range toolsList.Tools {
		if tool.Name == "memory_store" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("tools/list: memory_store not found")
	}

	// Store and recall a memory
	storeResp := e2eMCPPost(t, baseURL, tokenResp.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/call",
		Params: map[string]interface{}{
			"name": "memory_store",
			"arguments": map[string]interface{}{
				"project": "claude-test",
				"content": "Claude Desktop stores architecture decisions",
				"tags":    []string{"desktop", "test"},
			},
		},
	}, sessionID)

	if storeResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(storeResp.Body)
		storeResp.Body.Close()
		t.Fatalf("store: expected 200, got %d; body: %s", storeResp.StatusCode, string(bodyBytes))
	}
	storeRPC := e2eParseJSONRPC(t, storeResp)
	if storeRPC.Error != nil {
		t.Fatalf("store: JSON-RPC error: %s", storeRPC.Error.Message)
	}
	e2eExtractToolResultText(t, storeRPC) // verify no error

	t.Log("Claude Desktop E2E: full OAuth + MCP flow passed")
}

// ===========================================================================
// TestE2E_APIKey_DirectMCPAccess
// ===========================================================================

func TestE2E_APIKey_DirectMCPAccess(t *testing.T) {
	env := newE2EEnv(t)
	baseURL := env.Server.URL

	// Create an API key in the DB for the test user
	apiKeyRepo := storage.NewAPIKeyRepo(env.DB)
	apiKey := &model.APIKey{
		UserID: env.User.ID,
		Name:   "E2E Test Key",
		Scopes: []uuid.UUID{},
	}
	rawKey, err := apiKeyRepo.Create(context.Background(), apiKey)
	if err != nil {
		t.Fatalf("create API key: %v", err)
	}
	t.Logf("API key created: %s...", rawKey[:15])

	// Verify the key starts with nram_k_
	if !strings.HasPrefix(rawKey, "nram_k_") {
		t.Fatalf("API key doesn't have nram_k_ prefix: %s", rawKey[:15])
	}

	// Step 1: POST /mcp with API key — initialize
	t.Log("Step 1: Initialize with API key")
	initResp := e2eMCPPost(t, baseURL, rawKey, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "API Key Client", "version": "1.0"},
		},
	}, "")

	if initResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(initResp.Body)
		initResp.Body.Close()
		t.Fatalf("initialize: expected 200, got %d; body: %s", initResp.StatusCode, string(bodyBytes))
	}

	rpcResp := e2eParseJSONRPC(t, initResp)
	if rpcResp.Error != nil {
		t.Fatalf("initialize: JSON-RPC error: code=%d msg=%s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	var initResult map[string]interface{}
	json.Unmarshal(rpcResp.Result, &initResult)
	si, _ := initResult["serverInfo"].(map[string]interface{})
	if name, _ := si["name"].(string); name != "nram" {
		t.Fatalf("initialize: expected serverInfo.name=nram, got %q", name)
	}

	sessionID := initResp.Header.Get("Mcp-Session-Id")

	// Step 2: Send initialized notification
	notifResp := e2eMCPPost(t, baseURL, rawKey, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	}, sessionID)
	io.ReadAll(notifResp.Body)
	notifResp.Body.Close()

	// Step 3: Store a memory
	t.Log("Step 2: Store memory with API key")
	storeResp := e2eMCPPost(t, baseURL, rawKey, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params: map[string]interface{}{
			"name": "memory_store",
			"arguments": map[string]interface{}{
				"project": "claude-test",
				"content": "API key direct access works perfectly",
				"tags":    []string{"api-key", "test"},
			},
		},
	}, sessionID)

	if storeResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(storeResp.Body)
		storeResp.Body.Close()
		t.Fatalf("store: expected 200, got %d; body: %s", storeResp.StatusCode, string(bodyBytes))
	}

	storeRPC := e2eParseJSONRPC(t, storeResp)
	if storeRPC.Error != nil {
		t.Fatalf("store: JSON-RPC error: %s", storeRPC.Error.Message)
	}

	storeText := e2eExtractToolResultText(t, storeRPC)
	var storeResult e2eMCPStoreResult
	if err := json.Unmarshal([]byte(storeText), &storeResult); err != nil {
		t.Fatalf("store: unmarshal: %v", err)
	}
	if storeResult.ID == uuid.Nil {
		t.Fatal("store: nil memory ID")
	}

	// Step 4: Recall the memory
	t.Log("Step 3: Recall memory with API key")
	recallResp := e2eMCPPost(t, baseURL, rawKey, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/call",
		Params: map[string]interface{}{
			"name": "memory_recall",
			"arguments": map[string]interface{}{
				"query":   "API key direct access",
				"project": "claude-test",
			},
		},
	}, sessionID)

	if recallResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(recallResp.Body)
		recallResp.Body.Close()
		t.Fatalf("recall: expected 200, got %d; body: %s", recallResp.StatusCode, string(bodyBytes))
	}

	recallRPC := e2eParseJSONRPC(t, recallResp)
	if recallRPC.Error != nil {
		t.Fatalf("recall: JSON-RPC error: %s", recallRPC.Error.Message)
	}

	recallText := e2eExtractToolResultText(t, recallRPC)
	var recallResult service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResult); err != nil {
		t.Fatalf("recall: unmarshal: %v", err)
	}
	if len(recallResult.Memories) == 0 {
		t.Fatal("recall: expected at least 1 memory, got 0")
	}

	foundRecall := false
	for _, mem := range recallResult.Memories {
		if strings.Contains(mem.Content, "API key direct access") {
			foundRecall = true
			break
		}
	}
	if !foundRecall {
		t.Fatal("recall: stored memory not found in recall results")
	}

	t.Log("API Key E2E: direct MCP access without OAuth passed")
}

// ---------------------------------------------------------------------------
// Helper: complete an OAuth flow and return tokens + metadata
// ---------------------------------------------------------------------------

type e2eOAuthResult struct {
	AccessToken  string
	RefreshToken string
	ClientID     string
	SessionID    string // MCP session ID from initialize
	TokenEndpoint string
	Resource     string
	RedirectURI  string
}

// e2eFullOAuthFlow registers a client, completes the OAuth authorization code
// flow with PKCE, and returns the resulting tokens. The clientName and
// redirectURI parameters control the client registration.
func e2eFullOAuthFlow(t *testing.T, env *e2eEnv, clientName, redirectURI string, userID uuid.UUID, role string) *e2eOAuthResult {
	t.Helper()
	client := e2eNoRedirectClient()
	baseURL := env.Server.URL

	// Discovery
	resp, err := client.Get(baseURL + "/.well-known/oauth-authorization-server")
	if err != nil {
		t.Fatalf("oauth flow: discovery: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("oauth flow: discovery: expected 200, got %d", resp.StatusCode)
	}
	var meta struct {
		AuthorizationEndpoint string `json:"authorization_endpoint"`
		TokenEndpoint         string `json:"token_endpoint"`
		RegistrationEndpoint  string `json:"registration_endpoint"`
	}
	json.Unmarshal(body, &meta)

	// Register client
	regBody, _ := json.Marshal(map[string]interface{}{
		"client_name":   clientName,
		"redirect_uris": []string{redirectURI},
		"grant_types":   []string{"authorization_code", "refresh_token"},
	})
	resp, err = client.Post(meta.RegistrationEndpoint, "application/json", bytes.NewReader(regBody))
	if err != nil {
		t.Fatalf("oauth flow: register: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("oauth flow: register: expected 201, got %d; body: %s", resp.StatusCode, body)
	}
	var regResp struct {
		ClientID string `json:"client_id"`
	}
	json.Unmarshal(body, &regResp)

	// PKCE + authorize
	codeVerifier := "e2e-flow-verifier-which-is-at-least-43-characters-long-for-pkce-" + uuid.New().String()[:8]
	codeChallenge := e2eComputeCodeChallenge(codeVerifier)
	state := "state-" + uuid.New().String()[:8]
	resource := baseURL + "/mcp"

	authURL := fmt.Sprintf(
		"%s?client_id=%s&redirect_uri=%s&response_type=code&code_challenge=%s&code_challenge_method=S256&state=%s&resource=%s",
		meta.AuthorizationEndpoint,
		url.QueryEscape(regResp.ClientID),
		url.QueryEscape(redirectURI),
		url.QueryEscape(codeChallenge),
		url.QueryEscape(state),
		url.QueryEscape(resource),
	)

	sessionCookie := e2eCreateSessionCookie(t, userID)
	if role != "" && role != "admin" {
		// Create a session cookie with the specified role
		token, err := auth.GenerateJWT(userID, uuid.Nil, role, e2eJWTSecret, 10*time.Minute)
		if err != nil {
			t.Fatalf("oauth flow: create session JWT: %v", err)
		}
		sessionCookie = &http.Cookie{
			Name:     "nram_session",
			Value:    token,
			Path:     "/",
			HttpOnly: true,
		}
	}

	authReq, _ := http.NewRequest(http.MethodGet, authURL, nil)
	authReq.AddCookie(sessionCookie)
	resp, err = client.Do(authReq)
	if err != nil {
		t.Fatalf("oauth flow: authorize: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("oauth flow: authorize: expected 302, got %d", resp.StatusCode)
	}

	location := resp.Header.Get("Location")
	redirectURL, _ := url.Parse(location)
	code := redirectURL.Query().Get("code")
	if code == "" {
		t.Fatal("oauth flow: no code in redirect")
	}

	// Token exchange
	tokenForm := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {redirectURI},
		"client_id":     {regResp.ClientID},
		"code_verifier": {codeVerifier},
		"resource":      {resource},
	}
	resp, err = client.PostForm(meta.TokenEndpoint, tokenForm)
	if err != nil {
		t.Fatalf("oauth flow: token: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("oauth flow: token: expected 200, got %d; body: %s", resp.StatusCode, body)
	}
	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	json.Unmarshal(body, &tokenResp)

	return &e2eOAuthResult{
		AccessToken:   tokenResp.AccessToken,
		RefreshToken:  tokenResp.RefreshToken,
		ClientID:      regResp.ClientID,
		TokenEndpoint: meta.TokenEndpoint,
		Resource:      resource,
		RedirectURI:   redirectURI,
	}
}

// e2eInitializeMCP sends the initialize + notifications/initialized handshake
// and returns the MCP session ID.
func e2eInitializeMCP(t *testing.T, baseURL, token string) string {
	t.Helper()
	resp := e2eMCPPost(t, baseURL, token, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "E2E Test Client", "version": "1.0"},
		},
	}, "")
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("initialize: expected 200, got %d; body: %s", resp.StatusCode, string(bodyBytes))
	}
	rpc := e2eParseJSONRPC(t, resp)
	if rpc.Error != nil {
		t.Fatalf("initialize: JSON-RPC error: %s", rpc.Error.Message)
	}
	sessionID := resp.Header.Get("Mcp-Session-Id")

	notifResp := e2eMCPPost(t, baseURL, token, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	}, sessionID)
	io.ReadAll(notifResp.Body)
	notifResp.Body.Close()

	return sessionID
}

// e2eGetAuthCodeParts returns the parts needed for auth code exchange without
// actually exchanging. Useful for testing error paths.
type e2eAuthCodeParts struct {
	Code          string
	CodeVerifier  string
	ClientID      string
	RedirectURI   string
	Resource      string
	TokenEndpoint string
}

func e2eGetAuthCode(t *testing.T, env *e2eEnv, clientName, redirectURI string) *e2eAuthCodeParts {
	t.Helper()
	client := e2eNoRedirectClient()
	baseURL := env.Server.URL

	// Discovery
	resp, err := client.Get(baseURL + "/.well-known/oauth-authorization-server")
	if err != nil {
		t.Fatalf("get auth code: discovery: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var meta struct {
		AuthorizationEndpoint string `json:"authorization_endpoint"`
		TokenEndpoint         string `json:"token_endpoint"`
		RegistrationEndpoint  string `json:"registration_endpoint"`
	}
	json.Unmarshal(body, &meta)

	// Register client
	regBody, _ := json.Marshal(map[string]interface{}{
		"client_name":   clientName,
		"redirect_uris": []string{redirectURI},
		"grant_types":   []string{"authorization_code", "refresh_token"},
	})
	resp, err = client.Post(meta.RegistrationEndpoint, "application/json", bytes.NewReader(regBody))
	if err != nil {
		t.Fatalf("get auth code: register: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("get auth code: register: expected 201, got %d; body: %s", resp.StatusCode, body)
	}
	var regResp struct {
		ClientID string `json:"client_id"`
	}
	json.Unmarshal(body, &regResp)

	// PKCE + authorize
	codeVerifier := "e2e-authcode-verifier-at-least-43-characters-long-for-pkce-" + uuid.New().String()[:8]
	codeChallenge := e2eComputeCodeChallenge(codeVerifier)
	state := "state-" + uuid.New().String()[:8]
	resource := baseURL + "/mcp"

	authURL := fmt.Sprintf(
		"%s?client_id=%s&redirect_uri=%s&response_type=code&code_challenge=%s&code_challenge_method=S256&state=%s&resource=%s",
		meta.AuthorizationEndpoint,
		url.QueryEscape(regResp.ClientID),
		url.QueryEscape(redirectURI),
		url.QueryEscape(codeChallenge),
		url.QueryEscape(state),
		url.QueryEscape(resource),
	)

	authReq, _ := http.NewRequest(http.MethodGet, authURL, nil)
	authReq.AddCookie(e2eCreateSessionCookie(t, env.User.ID))
	resp, err = client.Do(authReq)
	if err != nil {
		t.Fatalf("get auth code: authorize: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("get auth code: authorize: expected 302, got %d", resp.StatusCode)
	}

	location := resp.Header.Get("Location")
	redirectURL, _ := url.Parse(location)
	code := redirectURL.Query().Get("code")
	if code == "" {
		t.Fatal("get auth code: no code in redirect")
	}

	return &e2eAuthCodeParts{
		Code:          code,
		CodeVerifier:  codeVerifier,
		ClientID:      regResp.ClientID,
		RedirectURI:   redirectURI,
		Resource:      resource,
		TokenEndpoint: meta.TokenEndpoint,
	}
}

// newE2EEnvWithAdmin creates an E2E environment that includes an AdminDashboard
// handler (the default newE2EEnv leaves it nil which results in 501).
func newE2EEnvWithAdmin(t *testing.T) *e2eEnv {
	t.Helper()

	db := e2eTestDB(t)
	user := e2eTestUser(t, db)

	// Build real repos
	oauthRepo := storage.NewOAuthRepo(db)
	userRepo := storage.NewUserRepo(db)
	apiKeyRepo := storage.NewAPIKeyRepo(db)

	// Real OAuth server
	oauthSrv := auth.NewOAuthServer(oauthRepo, userRepo, e2eJWTSecret)

	// Real auth middleware (with real API key validator)
	authMw := auth.NewAuthMiddleware(apiKeyRepo, userRepo, e2eJWTSecret)

	// Real rate limiter
	rl := auth.NewRateLimiter(1000, 2000, 0, 0)
	t.Cleanup(rl.Stop)

	// Real metrics
	metrics := api.NewMetrics()

	// MCP server with in-memory mock services
	memRepo := newE2EMemoryRepo()
	nsID := user.NamespaceID
	ns := &model.Namespace{ID: nsID, Path: "/users/e2etest", Depth: 2}
	project := &model.Project{
		ID:               uuid.New(),
		NamespaceID:      nsID,
		OwnerNamespaceID: nsID,
		Name:             "claude-test",
		Slug:             "claude-test",
	}

	projectLookup := &e2eProjectLookup{project: project}
	namespaceLookup := &e2eNamespaceLookup{ns: ns}

	storeSvc := service.NewStoreService(
		memRepo,
		projectLookup,
		namespaceLookup,
		&e2eIngestionLogRepo{},
		&e2eEnrichmentQueueRepo{},
	)

	recallSvc := service.NewRecallService(
		memRepo,
		projectLookup,
		namespaceLookup,
		nil, nil, nil, nil, nil,
	)

	forgetSvc := service.NewForgetService(
		memRepo,
		projectLookup,
		nil, nil,
	)

	updateSvc := service.NewUpdateService(
		memRepo,
		projectLookup,
		&e2eLineageCreator{},
		nil,
		nil,
		)

	batchStoreSvc := service.NewBatchStoreService(
		memRepo,
		projectLookup,
		namespaceLookup,
		&e2eIngestionLogRepo{},
		&e2eEnrichmentQueueRepo{},
		nil,
	)

	mcpDeps := mcp.Dependencies{
		Backend:       storage.BackendSQLite,
		Store:         storeSvc,
		Recall:        recallSvc,
		Forget:        forgetSvc,
		Update:        updateSvc,
		BatchStore:    batchStoreSvc,
		ProjectRepo:   projectLookup,
		UserRepo:      &e2eUserRepoMCP{user: user},
		NamespaceRepo: namespaceLookup,
	}
	mcpSrv := mcp.NewServer(mcpDeps)

	handlers := Handlers{
		MCP: mcpSrv.Handler(),

		// OAuth handlers
		OAuthAuthorize:         oauthSrv.AuthorizeHandler(),
		OAuthToken:             oauthSrv.TokenHandler(),
		OAuthRegister:          oauthSrv.RegisterClientHandler(),
		OAuthUserInfo:          oauthSrv.UserInfoHandler(),
		OAuthMetadata:          oauthSrv.MetadataHandler(),
		OAuthProtectedResource: oauthSrv.ProtectedResourceHandler(),

		// Health
		Health: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		},

		// Admin setup status (returns setup complete)
		AdminSetupStatus: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"setup_complete":true}`))
		},

		// Admin dashboard stub (now mounted at /v1/dashboard for all authenticated users)
		AdminDashboard: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"total_memories":42,"total_projects":3}`))
		},

		// Admin orgs stub (admin-only route)
		AdminOrgs: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"data":[],"pagination":{"total":0,"limit":50,"offset":0}}`))
		},
	}

	cfg := RouterConfig{
		AuthMiddleware: authMw,
		RateLimiter:    rl,
		Metrics:        metrics,
	}

	router := NewRouter(cfg, handlers)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	return &e2eEnv{
		Server:  ts,
		DB:      db,
		User:    user,
		MemRepo: memRepo,
	}
}

// ===========================================================================
// TestE2E_ExpiredToken_MCPReturns401_ThenRefresh
// ===========================================================================

func TestE2E_ExpiredToken_MCPReturns401_ThenRefresh(t *testing.T) {
	env := newE2EEnv(t)
	baseURL := env.Server.URL

	// Complete OAuth flow to get tokens
	oauthResult := e2eFullOAuthFlow(t, env, "Expired Token Test", "http://localhost:3000/callback", env.User.ID, "admin")

	// Verify the valid token works first
	resp := e2eMCPPost(t, baseURL, oauthResult.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Expired Token Test", "version": "1.0"},
		},
	}, "")
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("valid token should work: got %d; body: %s", resp.StatusCode, string(bodyBytes))
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	// Manually create an EXPIRED JWT (exp set to the past)
	now := time.Now().UTC()
	expiredClaims := auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   env.User.ID.String(),
			IssuedAt:  jwt.NewNumericDate(now.Add(-2 * time.Hour)),
			ExpiresAt: jwt.NewNumericDate(now.Add(-1 * time.Hour)), // expired 1 hour ago
			Issuer:    "nram",
			Audience:  jwt.ClaimStrings{baseURL + "/mcp"},
		},
		Role: "admin",
	}
	expiredToken := jwt.NewWithClaims(jwt.SigningMethodHS256, expiredClaims)
	expiredTokenStr, err := expiredToken.SignedString(e2eJWTSecret)
	if err != nil {
		t.Fatalf("failed to sign expired JWT: %v", err)
	}

	// POST /mcp with expired token → expect 401 with WWW-Authenticate
	t.Log("POST /mcp with expired token")
	expiredResp := e2eMCPPost(t, baseURL, expiredTokenStr, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Expired Token Test", "version": "1.0"},
		},
	}, "")
	io.ReadAll(expiredResp.Body)
	expiredResp.Body.Close()

	if expiredResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired token: expected 401, got %d", expiredResp.StatusCode)
	}
	wwwAuth := expiredResp.Header.Get("WWW-Authenticate")
	if wwwAuth == "" {
		t.Fatal("expired token: missing WWW-Authenticate header")
	}
	if !strings.Contains(wwwAuth, "oauth-protected-resource") {
		t.Fatalf("expired token: WWW-Authenticate should reference oauth-protected-resource: %s", wwwAuth)
	}
	t.Logf("expired token correctly returned 401 with WWW-Authenticate: %s", wwwAuth)

	// Use refresh token to get a new access token
	client := e2eNoRedirectClient()
	refreshForm := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {oauthResult.RefreshToken},
		"client_id":     {oauthResult.ClientID},
	}
	refreshResp, err := client.PostForm(oauthResult.TokenEndpoint, refreshForm)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	body, _ := io.ReadAll(refreshResp.Body)
	refreshResp.Body.Close()
	if refreshResp.StatusCode != http.StatusOK {
		t.Fatalf("refresh: expected 200, got %d; body: %s", refreshResp.StatusCode, body)
	}

	var newTokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	json.Unmarshal(body, &newTokenResp)
	if newTokenResp.AccessToken == "" {
		t.Fatal("refresh: empty new access_token")
	}

	// POST /mcp with new token → expect success
	t.Log("POST /mcp with refreshed token")
	newResp := e2eMCPPost(t, baseURL, newTokenResp.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Expired Token Test", "version": "1.0"},
		},
	}, "")
	if newResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(newResp.Body)
		newResp.Body.Close()
		t.Fatalf("refreshed token: expected 200, got %d; body: %s", newResp.StatusCode, string(bodyBytes))
	}
	io.ReadAll(newResp.Body)
	newResp.Body.Close()

	t.Log("expired token → 401 → refresh → new token works: PASSED")
}

// ===========================================================================
// TestE2E_WrongAudience_MCPRejects
// ===========================================================================

func TestE2E_WrongAudience_MCPRejects(t *testing.T) {
	env := newE2EEnv(t)
	baseURL := env.Server.URL

	// Complete OAuth flow to verify things work
	oauthResult := e2eFullOAuthFlow(t, env, "Wrong Audience Test", "http://localhost:3000/callback", env.User.ID, "admin")

	// Verify the valid token works
	resp := e2eMCPPost(t, baseURL, oauthResult.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Wrong Aud Test", "version": "1.0"},
		},
	}, "")
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("valid token should work: got %d; body: %s", resp.StatusCode, string(bodyBytes))
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	// Craft a JWT with wrong audience
	now := time.Now().UTC()
	wrongAudClaims := auth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   env.User.ID.String(),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(1 * time.Hour)),
			Issuer:    "nram",
			Audience:  jwt.ClaimStrings{"https://other-server.example.com/mcp"},
		},
		Role: "admin",
	}
	wrongAudToken := jwt.NewWithClaims(jwt.SigningMethodHS256, wrongAudClaims)
	wrongAudTokenStr, err := wrongAudToken.SignedString(e2eJWTSecret)
	if err != nil {
		t.Fatalf("failed to sign wrong-audience JWT: %v", err)
	}

	// POST /mcp with wrong audience → expect 401
	t.Log("POST /mcp with wrong audience JWT")
	wrongResp := e2eMCPPost(t, baseURL, wrongAudTokenStr, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Wrong Aud Test", "version": "1.0"},
		},
	}, "")
	io.ReadAll(wrongResp.Body)
	wrongResp.Body.Close()

	if wrongResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong audience: expected 401, got %d", wrongResp.StatusCode)
	}
	t.Log("wrong audience JWT correctly rejected with 401: PASSED")
}

// ===========================================================================
// TestE2E_RevokedRefreshToken_Rejected
// ===========================================================================

func TestE2E_RevokedRefreshToken_Rejected(t *testing.T) {
	env := newE2EEnv(t)
	client := e2eNoRedirectClient()

	// Complete OAuth flow
	oauthResult := e2eFullOAuthFlow(t, env, "Revoked Refresh Test", "http://localhost:3000/callback", env.User.ID, "admin")

	// Use the refresh token once (it rotates)
	refreshForm := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {oauthResult.RefreshToken},
		"client_id":     {oauthResult.ClientID},
	}
	resp, err := client.PostForm(oauthResult.TokenEndpoint, refreshForm)
	if err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first refresh: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var newTokenResp struct {
		RefreshToken string `json:"refresh_token"`
	}
	json.Unmarshal(body, &newTokenResp)
	if newTokenResp.RefreshToken == "" {
		t.Fatal("first refresh: empty new refresh_token")
	}
	if newTokenResp.RefreshToken == oauthResult.RefreshToken {
		t.Fatal("first refresh: token was not rotated")
	}

	// Try the OLD refresh token again → should be rejected
	t.Log("trying old (revoked) refresh token")
	oldRefreshForm := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {oauthResult.RefreshToken},
		"client_id":     {oauthResult.ClientID},
	}
	resp2, err := client.PostForm(oauthResult.TokenEndpoint, oldRefreshForm)
	if err != nil {
		t.Fatalf("old refresh: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if resp2.StatusCode == http.StatusOK {
		t.Fatal("old refresh token should have been rejected but was accepted")
	}

	// Verify error response indicates invalid_grant
	var errResp struct {
		Error string `json:"error"`
	}
	json.Unmarshal(body2, &errResp)
	if errResp.Error != "invalid_grant" {
		t.Logf("note: error was %q (expected invalid_grant); status=%d", errResp.Error, resp2.StatusCode)
	}

	t.Log("revoked refresh token correctly rejected: PASSED")
}

// ===========================================================================
// TestE2E_OAuth_WrongPKCEVerifier
// ===========================================================================

func TestE2E_OAuth_WrongPKCEVerifier(t *testing.T) {
	env := newE2EEnv(t)
	client := e2eNoRedirectClient()

	// Get auth code through the full flow
	parts := e2eGetAuthCode(t, env, "Wrong PKCE Test", "http://localhost:3000/callback")

	// Exchange code with WRONG code_verifier
	t.Log("exchanging code with wrong PKCE verifier")
	tokenForm := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {parts.Code},
		"redirect_uri":  {parts.RedirectURI},
		"client_id":     {parts.ClientID},
		"code_verifier": {"this-is-definitely-the-wrong-verifier-and-at-least-43-characters-long"},
		"resource":      {parts.Resource},
	}
	resp, err := client.PostForm(parts.TokenEndpoint, tokenForm)
	if err != nil {
		t.Fatalf("token exchange: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		t.Fatal("wrong PKCE verifier should have been rejected but token was issued")
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("wrong PKCE verifier: expected 400, got %d; body: %s", resp.StatusCode, body)
	}

	var errResp struct {
		Error string `json:"error"`
	}
	json.Unmarshal(body, &errResp)
	if errResp.Error != "invalid_grant" {
		t.Logf("note: error was %q (expected invalid_grant)", errResp.Error)
	}

	t.Log("wrong PKCE verifier correctly rejected: PASSED")
}

// ===========================================================================
// TestE2E_OAuth_WrongRedirectURI
// ===========================================================================

func TestE2E_OAuth_WrongRedirectURI(t *testing.T) {
	env := newE2EEnv(t)
	client := e2eNoRedirectClient()
	baseURL := env.Server.URL

	// Discovery
	resp, err := client.Get(baseURL + "/.well-known/oauth-authorization-server")
	if err != nil {
		t.Fatalf("discovery: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var meta struct {
		AuthorizationEndpoint string `json:"authorization_endpoint"`
		RegistrationEndpoint  string `json:"registration_endpoint"`
	}
	json.Unmarshal(body, &meta)

	// Register client with redirect_uri A
	regBody, _ := json.Marshal(map[string]interface{}{
		"client_name":   "Wrong Redirect Test",
		"redirect_uris": []string{"http://localhost:3000/callback-a"},
		"grant_types":   []string{"authorization_code", "refresh_token"},
	})
	resp, err = client.Post(meta.RegistrationEndpoint, "application/json", bytes.NewReader(regBody))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register: expected 201, got %d; body: %s", resp.StatusCode, body)
	}
	var regResp struct {
		ClientID string `json:"client_id"`
	}
	json.Unmarshal(body, &regResp)

	// Call /authorize with redirect_uri B (different from registered)
	t.Log("calling /authorize with wrong redirect_uri")
	codeVerifier := "wrong-redirect-verifier-which-is-at-least-43-characters-long-for-pkce"
	codeChallenge := e2eComputeCodeChallenge(codeVerifier)
	state := "state-" + uuid.New().String()[:8]

	authURL := fmt.Sprintf(
		"%s?client_id=%s&redirect_uri=%s&response_type=code&code_challenge=%s&code_challenge_method=S256&state=%s",
		meta.AuthorizationEndpoint,
		url.QueryEscape(regResp.ClientID),
		url.QueryEscape("http://localhost:3000/callback-b"), // WRONG redirect URI
		url.QueryEscape(codeChallenge),
		url.QueryEscape(state),
	)

	authReq, _ := http.NewRequest(http.MethodGet, authURL, nil)
	authReq.AddCookie(e2eCreateSessionCookie(t, env.User.ID))
	resp, err = client.Do(authReq)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Should be rejected — either as a 400 error or a redirect with error param
	if resp.StatusCode == http.StatusFound {
		location := resp.Header.Get("Location")
		parsedLoc, _ := url.Parse(location)
		errorParam := parsedLoc.Query().Get("error")
		if errorParam == "" {
			t.Fatalf("wrong redirect URI: got redirect without error: %s", location)
		}
		t.Logf("wrong redirect URI rejected via redirect with error=%s", errorParam)
	} else if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusForbidden {
		t.Logf("wrong redirect URI rejected with status %d", resp.StatusCode)
	} else {
		t.Fatalf("wrong redirect URI: expected error response, got %d; body: %s", resp.StatusCode, body)
	}

	t.Log("wrong redirect URI correctly rejected: PASSED")
}

// ===========================================================================
// TestE2E_OAuth_ExpiredAuthCode
// ===========================================================================

func TestE2E_OAuth_ExpiredAuthCode(t *testing.T) {
	env := newE2EEnv(t)
	client := e2eNoRedirectClient()

	// Get an auth code
	parts := e2eGetAuthCode(t, env, "Expired Code Test", "http://localhost:3000/callback")

	// Directly update the DB to expire the auth code
	_, err := env.DB.DB().ExecContext(context.Background(),
		"UPDATE oauth_authorization_codes SET expires_at = ? WHERE code = ?",
		time.Now().UTC().Add(-1*time.Hour).Format(time.RFC3339), parts.Code)
	if err != nil {
		t.Fatalf("failed to expire auth code in DB: %v", err)
	}

	// Exchange the expired code
	t.Log("exchanging expired auth code")
	tokenForm := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {parts.Code},
		"redirect_uri":  {parts.RedirectURI},
		"client_id":     {parts.ClientID},
		"code_verifier": {parts.CodeVerifier},
		"resource":      {parts.Resource},
	}
	resp, err := client.PostForm(parts.TokenEndpoint, tokenForm)
	if err != nil {
		t.Fatalf("token exchange: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		t.Fatal("expired auth code should have been rejected but token was issued")
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expired auth code: expected 400, got %d; body: %s", resp.StatusCode, body)
	}

	// Check for "expired" or "invalid_grant" in the response
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "expired") && !strings.Contains(bodyStr, "invalid_grant") {
		t.Logf("note: response body doesn't mention expired or invalid_grant: %s", bodyStr)
	}

	t.Log("expired auth code correctly rejected: PASSED")
}

// ===========================================================================
// TestE2E_OAuth_CodeReuse
// ===========================================================================

func TestE2E_OAuth_CodeReuse(t *testing.T) {
	env := newE2EEnv(t)
	client := e2eNoRedirectClient()

	// Get an auth code
	parts := e2eGetAuthCode(t, env, "Code Reuse Test", "http://localhost:3000/callback")

	// First exchange — should succeed
	t.Log("first code exchange (should succeed)")
	tokenForm := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {parts.Code},
		"redirect_uri":  {parts.RedirectURI},
		"client_id":     {parts.ClientID},
		"code_verifier": {parts.CodeVerifier},
		"resource":      {parts.Resource},
	}
	resp, err := client.PostForm(parts.TokenEndpoint, tokenForm)
	if err != nil {
		t.Fatalf("first exchange: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first exchange: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	// Second exchange — same code, should fail
	t.Log("second code exchange (should fail — code reuse)")
	resp2, err := client.PostForm(parts.TokenEndpoint, tokenForm)
	if err != nil {
		t.Fatalf("second exchange: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if resp2.StatusCode == http.StatusOK {
		t.Fatal("code reuse should have been rejected but second exchange succeeded")
	}
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("code reuse: expected 400, got %d; body: %s", resp2.StatusCode, body2)
	}

	var errResp struct {
		Error string `json:"error"`
	}
	json.Unmarshal(body2, &errResp)
	if errResp.Error != "invalid_grant" {
		t.Logf("note: error was %q (expected invalid_grant)", errResp.Error)
	}

	t.Log("code reuse correctly rejected: PASSED")
}

// ===========================================================================
// TestE2E_OAuth_UnregisteredClientID
// ===========================================================================

func TestE2E_OAuth_UnregisteredClientID(t *testing.T) {
	env := newE2EEnv(t)
	client := e2eNoRedirectClient()
	baseURL := env.Server.URL

	// Discovery
	resp, err := client.Get(baseURL + "/.well-known/oauth-authorization-server")
	if err != nil {
		t.Fatalf("discovery: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var meta struct {
		AuthorizationEndpoint string `json:"authorization_endpoint"`
	}
	json.Unmarshal(body, &meta)

	// Call /authorize with a fake client_id that doesn't exist
	t.Log("calling /authorize with unregistered client_id")
	fakeClientID := "non-existent-client-" + uuid.New().String()
	codeVerifier := "unregistered-client-verifier-at-least-43-characters-long-for-pkce-test"
	codeChallenge := e2eComputeCodeChallenge(codeVerifier)
	state := "state-" + uuid.New().String()[:8]

	authURL := fmt.Sprintf(
		"%s?client_id=%s&redirect_uri=%s&response_type=code&code_challenge=%s&code_challenge_method=S256&state=%s",
		meta.AuthorizationEndpoint,
		url.QueryEscape(fakeClientID),
		url.QueryEscape("http://localhost:3000/callback"),
		url.QueryEscape(codeChallenge),
		url.QueryEscape(state),
	)

	authReq, _ := http.NewRequest(http.MethodGet, authURL, nil)
	authReq.AddCookie(e2eCreateSessionCookie(t, env.User.ID))
	resp, err = client.Do(authReq)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Should get an error — either 400 directly or a redirect with error
	if resp.StatusCode == http.StatusFound {
		location := resp.Header.Get("Location")
		parsedLoc, _ := url.Parse(location)
		errorParam := parsedLoc.Query().Get("error")
		if errorParam != "" {
			t.Logf("unregistered client rejected via redirect: error=%s", errorParam)
		} else {
			t.Fatalf("unregistered client got redirect without error: %s", location)
		}
	} else if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized {
		t.Logf("unregistered client rejected with status %d", resp.StatusCode)
	} else {
		t.Fatalf("unregistered client: expected error, got %d; body: %s", resp.StatusCode, body)
	}

	t.Log("unregistered client_id correctly rejected: PASSED")
}

// ===========================================================================
// TestE2E_OAuth_MissingPKCE
// ===========================================================================

func TestE2E_OAuth_MissingPKCE(t *testing.T) {
	env := newE2EEnv(t)
	client := e2eNoRedirectClient()
	baseURL := env.Server.URL

	// Discovery
	resp, err := client.Get(baseURL + "/.well-known/oauth-authorization-server")
	if err != nil {
		t.Fatalf("discovery: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var meta struct {
		AuthorizationEndpoint string `json:"authorization_endpoint"`
		RegistrationEndpoint  string `json:"registration_endpoint"`
	}
	json.Unmarshal(body, &meta)

	// Register a client
	regBody, _ := json.Marshal(map[string]interface{}{
		"client_name":   "Missing PKCE Test",
		"redirect_uris": []string{"http://localhost:3000/callback"},
		"grant_types":   []string{"authorization_code", "refresh_token"},
	})
	resp, err = client.Post(meta.RegistrationEndpoint, "application/json", bytes.NewReader(regBody))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register: expected 201, got %d; body: %s", resp.StatusCode, body)
	}
	var regResp struct {
		ClientID string `json:"client_id"`
	}
	json.Unmarshal(body, &regResp)

	// Call /authorize WITHOUT code_challenge (no PKCE)
	t.Log("calling /authorize without PKCE code_challenge")
	state := "state-" + uuid.New().String()[:8]
	authURL := fmt.Sprintf(
		"%s?client_id=%s&redirect_uri=%s&response_type=code&state=%s",
		meta.AuthorizationEndpoint,
		url.QueryEscape(regResp.ClientID),
		url.QueryEscape("http://localhost:3000/callback"),
		url.QueryEscape(state),
	)

	authReq, _ := http.NewRequest(http.MethodGet, authURL, nil)
	authReq.AddCookie(e2eCreateSessionCookie(t, env.User.ID))
	resp, err = client.Do(authReq)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	// Should be rejected — PKCE is required per MCP spec
	if resp.StatusCode == http.StatusFound {
		location := resp.Header.Get("Location")
		parsedLoc, _ := url.Parse(location)
		errorParam := parsedLoc.Query().Get("error")
		if errorParam != "" {
			t.Logf("missing PKCE rejected via redirect: error=%s", errorParam)
		} else {
			t.Fatalf("missing PKCE: got redirect without error (should require PKCE): %s", location)
		}
	} else if resp.StatusCode == http.StatusBadRequest {
		t.Logf("missing PKCE rejected with status 400")
	} else {
		t.Fatalf("missing PKCE: expected error, got %d; body: %s", resp.StatusCode, body)
	}

	t.Log("missing PKCE correctly rejected: PASSED")
}

// ===========================================================================
// TestE2E_TwoClients_SeparateTokens
// ===========================================================================

func TestE2E_TwoClients_SeparateTokens(t *testing.T) {
	env := newE2EEnv(t)
	baseURL := env.Server.URL
	client := e2eNoRedirectClient()

	// Register and complete OAuth for client A
	t.Log("completing OAuth flow for client A")
	clientA := e2eFullOAuthFlow(t, env, "Client A", "http://localhost:3001/callback", env.User.ID, "admin")

	// Register and complete OAuth for client B
	t.Log("completing OAuth flow for client B")
	clientB := e2eFullOAuthFlow(t, env, "Client B", "http://localhost:3002/callback", env.User.ID, "admin")

	// Client IDs must be different (different registrations)
	if clientA.ClientID == clientB.ClientID {
		t.Fatal("client A and client B should have different client IDs")
	}
	// Refresh tokens must be different (unique per client)
	if clientA.RefreshToken == clientB.RefreshToken {
		t.Fatal("client A and client B should have different refresh tokens")
	}
	// Access tokens may match if same user/claims/timestamp but verify both are non-empty
	if clientA.AccessToken == "" || clientB.AccessToken == "" {
		t.Fatal("both clients should have non-empty access tokens")
	}

	// Both can call /mcp independently
	t.Log("verifying client A can call /mcp")
	respA := e2eMCPPost(t, baseURL, clientA.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Client A", "version": "1.0"},
		},
	}, "")
	if respA.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(respA.Body)
		respA.Body.Close()
		t.Fatalf("client A MCP: expected 200, got %d; body: %s", respA.StatusCode, string(bodyBytes))
	}
	io.ReadAll(respA.Body)
	respA.Body.Close()

	t.Log("verifying client B can call /mcp")
	respB := e2eMCPPost(t, baseURL, clientB.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Client B", "version": "1.0"},
		},
	}, "")
	if respB.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(respB.Body)
		respB.Body.Close()
		t.Fatalf("client B MCP: expected 200, got %d; body: %s", respB.StatusCode, string(bodyBytes))
	}
	io.ReadAll(respB.Body)
	respB.Body.Close()

	// Revoke client A's refresh token by using it (rotation)
	t.Log("rotating client A's refresh token")
	refreshFormA := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {clientA.RefreshToken},
		"client_id":     {clientA.ClientID},
	}
	resp, err := client.PostForm(clientA.TokenEndpoint, refreshFormA)
	if err != nil {
		t.Fatalf("rotate client A: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rotate client A: expected 200, got %d", resp.StatusCode)
	}

	// Client B's refresh token should still work (independent)
	t.Log("verifying client B's refresh token still works")
	refreshFormB := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {clientB.RefreshToken},
		"client_id":     {clientB.ClientID},
	}
	resp, err = client.PostForm(clientB.TokenEndpoint, refreshFormB)
	if err != nil {
		t.Fatalf("refresh client B: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("refresh client B: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var newB struct {
		AccessToken string `json:"access_token"`
	}
	json.Unmarshal(body, &newB)
	if newB.AccessToken == "" {
		t.Fatal("client B refresh: empty access token")
	}

	t.Log("two clients with separate tokens: PASSED")
}

// ===========================================================================
// TestE2E_ChatGPT_OAuthFlow
// ===========================================================================

func TestE2E_ChatGPT_OAuthFlow(t *testing.T) {
	env := newE2EEnv(t)
	baseURL := env.Server.URL

	// Use ChatGPT-typical redirect URI and client name
	chatGPTRedirect := "https://chatgpt.com/aip/g-abc123/oauth/callback"
	oauthResult := e2eFullOAuthFlow(t, env, "ChatGPT Plugin", chatGPTRedirect, env.User.ID, "admin")

	if oauthResult.AccessToken == "" {
		t.Fatal("ChatGPT flow: empty access token")
	}
	if oauthResult.RefreshToken == "" {
		t.Fatal("ChatGPT flow: empty refresh token")
	}

	// Verify the token works with MCP
	t.Log("verifying ChatGPT token works with /mcp")
	resp := e2eMCPPost(t, baseURL, oauthResult.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "ChatGPT Plugin", "version": "1.0"},
		},
	}, "")
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("ChatGPT MCP: expected 200, got %d; body: %s", resp.StatusCode, string(bodyBytes))
	}

	rpc := e2eParseJSONRPC(t, resp)
	if rpc.Error != nil {
		t.Fatalf("ChatGPT MCP: JSON-RPC error: %s", rpc.Error.Message)
	}

	var initResult map[string]interface{}
	json.Unmarshal(rpc.Result, &initResult)
	si, _ := initResult["serverInfo"].(map[string]interface{})
	if name, _ := si["name"].(string); name != "nram" {
		t.Fatalf("ChatGPT MCP: expected serverInfo.name=nram, got %q", name)
	}

	t.Log("ChatGPT OAuth flow: PASSED (not hardcoded to Claude-specific values)")
}

// ===========================================================================
// TestE2E_AdminRoute_WithOAuthToken
// ===========================================================================

func TestE2E_AdminRoute_WithOAuthToken(t *testing.T) {
	env := newE2EEnvWithAdmin(t)
	baseURL := env.Server.URL
	ctx := context.Background()

	nsRepo := storage.NewNamespaceRepo(env.DB)
	orgRepo := storage.NewOrganizationRepo(env.DB)
	userRepo := storage.NewUserRepo(env.DB)
	rootID := uuid.MustParse("00000000-0000-0000-0000-000000000000")

	// Create an administrator user (the RBAC system requires "administrator", not "admin")
	adminOrgNSID := uuid.New()
	adminOrgNS := &model.Namespace{
		ID:       adminOrgNSID,
		Name:     "Admin RBAC Org NS",
		Slug:     adminOrgNSID.String(),
		Kind:     "org",
		ParentID: &rootID,
		Path:     adminOrgNSID.String(),
		Depth:    1,
	}
	if err := nsRepo.Create(ctx, adminOrgNS); err != nil {
		t.Fatalf("create admin org NS: %v", err)
	}
	adminOrg := &model.Organization{
		NamespaceID: adminOrgNSID,
		Name:        "Admin RBAC Org",
		Slug:        "admin-rbac-" + adminOrgNSID.String()[:8],
	}
	if err := orgRepo.Create(ctx, adminOrg); err != nil {
		t.Fatalf("create admin org: %v", err)
	}
	adminUser := &model.User{
		Email:       "admin-rbac-" + uuid.New().String()[:8] + "@example.com",
		DisplayName: "Admin RBAC User",
		OrgID:       adminOrg.ID,
		Role:        "administrator",
	}
	if err := userRepo.Create(ctx, adminUser, nsRepo, nil, adminOrgNS.Path); err != nil {
		t.Fatalf("create admin user: %v", err)
	}

	// Complete OAuth flow for admin user
	t.Log("completing OAuth flow for administrator user")
	adminOAuth := e2eFullOAuthFlow(t, env, "Admin RBAC Test", "http://localhost:3000/callback", adminUser.ID, "administrator")

	// Hit GET /v1/admin/orgs with admin token → expect 200
	t.Log("hitting /v1/admin/orgs with administrator token")
	req, _ := http.NewRequest(http.MethodGet, baseURL+"/v1/admin/orgs", nil)
	req.Header.Set("Authorization", "Bearer "+adminOAuth.AccessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("admin orgs: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("admin orgs with administrator token: expected 200, got %d; body: %s", resp.StatusCode, body)
	}
	t.Log("admin orgs accessible with administrator token: OK")

	// Create a member user
	t.Log("creating member user")
	memberOrgNSID := uuid.New()
	memberOrgNS := &model.Namespace{
		ID:       memberOrgNSID,
		Name:     "Member Org NS",
		Slug:     memberOrgNSID.String(),
		Kind:     "org",
		ParentID: &rootID,
		Path:     memberOrgNSID.String(),
		Depth:    1,
	}
	if err := nsRepo.Create(ctx, memberOrgNS); err != nil {
		t.Fatalf("create member org NS: %v", err)
	}
	memberOrg := &model.Organization{
		NamespaceID: memberOrgNSID,
		Name:        "Member Org",
		Slug:        "member-org-" + memberOrgNSID.String()[:8],
	}
	if err := orgRepo.Create(ctx, memberOrg); err != nil {
		t.Fatalf("create member org: %v", err)
	}
	memberUser := &model.User{
		Email:       "member-" + uuid.New().String()[:8] + "@example.com",
		DisplayName: "Member User",
		OrgID:       memberOrg.ID,
		Role:        "member",
	}
	if err := userRepo.Create(ctx, memberUser, nsRepo, nil, memberOrgNS.Path); err != nil {
		t.Fatalf("create member user: %v", err)
	}

	// Complete OAuth flow for the member user
	t.Log("completing OAuth flow for member user")
	memberOAuth := e2eFullOAuthFlow(t, env, "Member RBAC Test", "http://localhost:3000/callback-member", memberUser.ID, "member")

	// Hit GET /v1/admin/orgs with member token → expect 403
	t.Log("hitting /v1/admin/orgs with member token")
	memberReq, _ := http.NewRequest(http.MethodGet, baseURL+"/v1/admin/orgs", nil)
	memberReq.Header.Set("Authorization", "Bearer "+memberOAuth.AccessToken)
	resp2, err := http.DefaultClient.Do(memberReq)
	if err != nil {
		t.Fatalf("member admin orgs: %v", err)
	}
	io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("member admin orgs: expected 403, got %d", resp2.StatusCode)
	}
	t.Log("member correctly denied admin access with 403: PASSED")
}

// ===========================================================================
// TestE2E_OAuth_OriginValidation
// ===========================================================================

func TestE2E_OAuth_OriginValidation(t *testing.T) {
	env := newE2EEnv(t)
	baseURL := env.Server.URL

	// Complete OAuth flow to get a valid token
	oauthResult := e2eFullOAuthFlow(t, env, "Origin Test", "http://localhost:3000/callback", env.User.ID, "admin")

	// POST /mcp with valid token BUT a bad Origin header
	t.Log("POST /mcp with valid token but bad Origin header")
	initBody, _ := json.Marshal(e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Origin Test", "version": "1.0"},
		},
	})

	req, _ := http.NewRequest(http.MethodPost, baseURL+"/mcp", bytes.NewReader(initBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+oauthResult.AccessToken)
	req.Header.Set("Origin", "http://evil-site.example.com") // bad Origin

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("bad origin request: %v", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	// Authenticated requests with any Origin are allowed — the OAuth token
	// proves legitimacy, enabling browser-based clients like Claude.ai.
	if resp.StatusCode == http.StatusForbidden {
		t.Fatalf("bad Origin with valid token: should be allowed, got 403")
	}

	// Verify that no Origin header also works
	t.Log("POST /mcp with valid token and no Origin header (should work)")
	goodResp := e2eMCPPost(t, baseURL, oauthResult.AccessToken, e2eJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":   map[string]interface{}{},
			"clientInfo":     map[string]interface{}{"name": "Origin Test", "version": "1.0"},
		},
	}, "")
	if goodResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(goodResp.Body)
		goodResp.Body.Close()
		t.Fatalf("good origin: expected 200, got %d; body: %s", goodResp.StatusCode, string(bodyBytes))
	}
	io.ReadAll(goodResp.Body)
	goodResp.Body.Close()

	t.Log("Origin validation allows authenticated cross-origin requests: PASSED")
}

// ===========================================================================
// TestE2E_HealthEndpoint_NoAuth
// ===========================================================================

func TestE2E_HealthEndpoint_NoAuth(t *testing.T) {
	env := newE2EEnv(t)
	baseURL := env.Server.URL

	// GET /v1/health with no auth → expect 200
	t.Log("GET /v1/health with no auth")
	resp, err := http.Get(baseURL + "/v1/health")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	var healthResp struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &healthResp); err != nil {
		t.Fatalf("health: unmarshal: %v", err)
	}
	if healthResp.Status != "ok" {
		t.Fatalf("health: expected status=ok, got %q", healthResp.Status)
	}

	t.Log("health endpoint accessible without auth: PASSED")
}

// ===========================================================================
// TestE2E_SetupStatusEndpoint_NoAuth
// ===========================================================================

func TestE2E_SetupStatusEndpoint_NoAuth(t *testing.T) {
	env := newE2EEnvWithAdmin(t) // Use admin env which has the setup status handler
	baseURL := env.Server.URL

	// GET /v1/admin/setup/status with no auth → expect 200
	t.Log("GET /v1/admin/setup/status with no auth")
	resp, err := http.Get(baseURL + "/v1/admin/setup/status")
	if err != nil {
		t.Fatalf("setup status: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("setup status: expected 200, got %d; body: %s", resp.StatusCode, body)
	}

	// Verify it returns JSON with setup_complete field
	var statusResp struct {
		SetupComplete bool `json:"setup_complete"`
	}
	if err := json.Unmarshal(body, &statusResp); err != nil {
		t.Fatalf("setup status: unmarshal: %v", err)
	}

	t.Logf("setup status: setup_complete=%v", statusResp.SetupComplete)
	t.Log("setup status endpoint accessible without auth: PASSED")
}

// Ensure imports are used.
var _ jwt.Claims
var _ *model.APIKey
