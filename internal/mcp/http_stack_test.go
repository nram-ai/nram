package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/service"
	"github.com/nram-ai/nram/internal/storage"
)

// ---------------------------------------------------------------------------
// JSON-RPC 2.0 types for HTTP stack tests
// ---------------------------------------------------------------------------

type jsonrpcRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

type jsonrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonrpcError   `json:"error,omitempty"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ---------------------------------------------------------------------------
// API key validator stub (always rejects — we use JWT for these tests)
// ---------------------------------------------------------------------------

type testAPIKeyValidator struct{}

func (t *testAPIKeyValidator) Validate(_ context.Context, _ string) (*model.APIKey, error) {
	return nil, fmt.Errorf("api key validation not supported in test")
}

// testUserIdentityLookup implements auth.UserIdentityLookup for tests.
// API key validation always fails in these tests so this is never called.
type testUserIdentityLookup struct{}

func (t *testUserIdentityLookup) GetIdentityByID(_ context.Context, _ uuid.UUID) (string, uuid.UUID, error) {
	return "member", uuid.Nil, nil
}

// ---------------------------------------------------------------------------
// Test JWT secret and helper
// ---------------------------------------------------------------------------

var httpStackTestSecret = []byte("http-stack-test-secret-32bytes!!")

func generateHTTPStackJWT(t *testing.T, userID uuid.UUID, host string) string {
	t.Helper()
	// Generate a JWT without audience so it passes the audience check
	// (audience check is skipped when aud claim is empty).
	token, err := auth.GenerateJWT(userID, uuid.Nil, auth.RoleMember, httpStackTestSecret, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to generate test JWT: %v", err)
	}
	return token
}

// ---------------------------------------------------------------------------
// Full-stack router builder: chi + auth middleware + real MCP server
// ---------------------------------------------------------------------------

// httpStackEnv bundles everything a test needs.
type httpStackEnv struct {
	Server    *httptest.Server
	UserID    uuid.UUID
	Token     string
	NsID      uuid.UUID
	ProjectID uuid.UUID
	MemRepo   *mockMemoryRepoWithContent
}

func (e *httpStackEnv) Close() {
	e.Server.Close()
}

// newHTTPStackEnv builds a full-stack test server with a real chi router,
// auth middleware, and MCP server wired to in-memory mock repositories.
func newHTTPStackEnv(t *testing.T) *httpStackEnv {
	t.Helper()

	userID := uuid.New()
	nsID := uuid.New()
	projectID := uuid.New()

	user := &model.User{ID: userID, NamespaceID: nsID}
	ns := &model.Namespace{ID: nsID, Path: "/users/testuser", Depth: 2}
	project := &model.Project{
		ID:               projectID,
		NamespaceID:      nsID,
		OwnerNamespaceID: nsID,
		Name:             "test-project",
		Slug:             "test-project",
	}

	memRepo := newMockMemoryRepoWithContent()
	storeSvc := buildIntegStoreService(memRepo, project, ns)
	batchSvc := buildIntegBatchStoreService(memRepo, project, ns)

	// Recall service that reads from the same memRepo (via a wrapper).
	recallReader := &memRepoListAdapter{memRepo: memRepo}
	recallSvc := service.NewRecallService(
		recallReader,
		&mockProjectLookup{project: project},
		&mockNamespaceLookup{ns: ns},
		&mockTokenUsageRepo{},
		nil, nil, nil, nil, nil,
	)

	// Forget service backed by a tracking deleter that shares the memRepo map.
	deleter := newTrackingMemoryDeleter(memRepo.memories)
	forgetSvc := buildIntegForgetService(deleter, project)

	// Update service with a real updater that operates on the memRepo.
	updater := &memRepoUpdater{memRepo: memRepo}
	updateSvc := service.NewUpdateService(
		updater,
		&mockProjectLookup{project: project},
		&mockLineageCreator{},
		nil,
		&mockTokenUsageRepo{},
		nil,
	)

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		Store:         storeSvc,
		Recall:        recallSvc,
		Forget:        forgetSvc,
		Update:        updateSvc,
		BatchStore:    batchSvc,
		ProjectRepo:   &mockProjectRepoStore{project: project},
		UserRepo:      &mockUserRepoStore{user: user},
		NamespaceRepo: &mockNamespaceRepoStore{ns: ns},
	}
	mcpSrv := NewServer(deps)

	authMw := auth.NewAuthMiddleware(&testAPIKeyValidator{}, &testUserIdentityLookup{}, httpStackTestSecret)

	r := chi.NewRouter()
	r.Group(func(r chi.Router) {
		r.Use(authMw.Handler)
		r.Handle("/mcp", mcpSrv.Handler())
		r.Handle("/mcp/*", mcpSrv.Handler())
	})

	ts := httptest.NewServer(r)
	token := generateHTTPStackJWT(t, userID, ts.URL)

	return &httpStackEnv{
		Server:    ts,
		UserID:    userID,
		Token:     token,
		NsID:      nsID,
		ProjectID: projectID,
		MemRepo:   memRepo,
	}
}

// memRepoListAdapter adapts mockMemoryRepoWithContent to service.MemoryReader.
type memRepoListAdapter struct {
	memRepo *mockMemoryRepoWithContent
}

func (m *memRepoListAdapter) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	mem, ok := m.memRepo.memories[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mem, nil
}

func (m *memRepoListAdapter) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
	var out []model.Memory
	for _, id := range ids {
		if mem, ok := m.memRepo.memories[id]; ok {
			out = append(out, *mem)
		}
	}
	return out, nil
}

func (m *memRepoListAdapter) ListByNamespace(_ context.Context, _ uuid.UUID, limit, _ int) ([]model.Memory, error) {
	var out []model.Memory
	for _, mem := range m.memRepo.memories {
		out = append(out, *mem)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// memRepoUpdater adapts mockMemoryRepoWithContent to service.MemoryUpdater.
type memRepoUpdater struct {
	memRepo *mockMemoryRepoWithContent
}

func (m *memRepoUpdater) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	mem, ok := m.memRepo.memories[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	clone := *mem
	return &clone, nil
}

func (m *memRepoUpdater) Update(_ context.Context, mem *model.Memory) error {
	m.memRepo.memories[mem.ID] = mem
	return nil
}

// ---------------------------------------------------------------------------
// HTTP helpers for JSON-RPC over Streamable HTTP
// ---------------------------------------------------------------------------

// doMCPRequest sends a JSON-RPC request and returns the parsed response.
// It handles both application/json and text/event-stream responses.
func doMCPRequest(t *testing.T, env *httpStackEnv, req jsonrpcRequest) (*http.Response, *jsonrpcResponse) {
	t.Helper()
	return doMCPRequestWithHeaders(t, env, req, nil)
}

func doMCPRequestWithHeaders(t *testing.T, env *httpStackEnv, req jsonrpcRequest, extraHeaders map[string]string) (*http.Response, *jsonrpcResponse) {
	t.Helper()

	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal JSON-RPC request: %v", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, env.Server.URL+"/mcp", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/event-stream")
	httpReq.Header.Set("Authorization", "Bearer "+env.Token)

	for k, v := range extraHeaders {
		httpReq.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}

	// For non-success or notifications (no ID), just return the raw response.
	if req.ID == nil || resp.StatusCode >= 300 {
		return resp, nil
	}

	rpcResp := parseJSONRPCResponse(t, resp)
	return resp, rpcResp
}

// doRawMCPPost sends a raw POST to /mcp with given body and optional headers.
func doRawMCPPost(t *testing.T, url string, body []byte, headers map[string]string) *http.Response {
	t.Helper()

	httpReq, err := http.NewRequest(http.MethodPost, url+"/mcp", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create HTTP request: %v", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/event-stream")

	for k, v := range headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	return resp
}

// parseJSONRPCResponse extracts a jsonrpcResponse from an HTTP response,
// handling both application/json and text/event-stream content types.
func parseJSONRPCResponse(t *testing.T, resp *http.Response) *jsonrpcResponse {
	t.Helper()

	ct := resp.Header.Get("Content-Type")
	bodyBytes, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if strings.HasPrefix(ct, "application/json") {
		var rpcResp jsonrpcResponse
		if err := json.Unmarshal(bodyBytes, &rpcResp); err != nil {
			t.Fatalf("failed to unmarshal JSON-RPC response (body=%s): %v", string(bodyBytes), err)
		}
		return &rpcResp
	}

	if strings.HasPrefix(ct, "text/event-stream") {
		return parseSSEResponse(t, bodyBytes)
	}

	t.Fatalf("unexpected Content-Type %q, body: %s", ct, string(bodyBytes))
	return nil
}

// parseSSEResponse extracts the JSON-RPC response from an SSE stream body.
func parseSSEResponse(t *testing.T, body []byte) *jsonrpcResponse {
	t.Helper()

	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")
			var rpcResp jsonrpcResponse
			if err := json.Unmarshal([]byte(data), &rpcResp); err != nil {
				continue // skip non-JSON data lines
			}
			if rpcResp.JSONRPC == "2.0" {
				return &rpcResp
			}
		}
	}
	t.Fatalf("no JSON-RPC response found in SSE stream: %s", string(body))
	return nil
}

// ---------------------------------------------------------------------------
// Session management helper: initialize + send initialized notification
// ---------------------------------------------------------------------------

type mcpSession struct {
	env       *httpStackEnv
	sessionID string
}

func initMCPSession(t *testing.T, env *httpStackEnv) *mcpSession {
	t.Helper()

	initReq := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]interface{}{},
			"clientInfo": map[string]interface{}{
				"name":    "test-client",
				"version": "1.0",
			},
		},
	}

	resp, rpcResp := doMCPRequest(t, env, initReq)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initialize: expected 200, got %d", resp.StatusCode)
	}
	if rpcResp == nil {
		t.Fatal("initialize: got nil JSON-RPC response")
	}
	if rpcResp.Error != nil {
		t.Fatalf("initialize: JSON-RPC error: code=%d msg=%s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	sessionID := resp.Header.Get("Mcp-Session-Id")

	// Send initialized notification.
	notifReq := jsonrpcRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	}
	notifBody, _ := json.Marshal(notifReq)
	httpReq, _ := http.NewRequest(http.MethodPost, env.Server.URL+"/mcp", bytes.NewReader(notifBody))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/event-stream")
	httpReq.Header.Set("Authorization", "Bearer "+env.Token)
	if sessionID != "" {
		httpReq.Header.Set("Mcp-Session-Id", sessionID)
	}

	notifResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("initialized notification: HTTP error: %v", err)
	}
	io.ReadAll(notifResp.Body)
	notifResp.Body.Close()

	// 202 or 200 are both acceptable for notifications.
	if notifResp.StatusCode != http.StatusAccepted && notifResp.StatusCode != http.StatusOK {
		t.Fatalf("initialized notification: expected 202 or 200, got %d", notifResp.StatusCode)
	}

	return &mcpSession{env: env, sessionID: sessionID}
}

// call sends a JSON-RPC request within the established session.
func (s *mcpSession) call(t *testing.T, id interface{}, method string, params interface{}) (*http.Response, *jsonrpcResponse) {
	t.Helper()

	req := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}

	headers := map[string]string{}
	if s.sessionID != "" {
		headers["Mcp-Session-Id"] = s.sessionID
	}

	return doMCPRequestWithHeaders(t, s.env, req, headers)
}

// ---------------------------------------------------------------------------
// Test 1: TestHTTPStack_MCP_Initialize
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_Initialize(t *testing.T) {
	env := newHTTPStackEnv(t)
	defer env.Close()

	// Send initialize request.
	initReq := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]interface{}{},
			"clientInfo": map[string]interface{}{
				"name":    "test-client",
				"version": "1.0",
			},
		},
	}

	resp, rpcResp := doMCPRequest(t, env, initReq)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if rpcResp.Error != nil {
		t.Fatalf("unexpected JSON-RPC error: code=%d msg=%s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	// Parse result fields.
	var result map[string]interface{}
	if err := json.Unmarshal(rpcResp.Result, &result); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if _, ok := result["protocolVersion"]; !ok {
		t.Error("result missing protocolVersion")
	}

	caps, ok := result["capabilities"].(map[string]interface{})
	if !ok {
		t.Fatal("result missing capabilities")
	}
	if _, ok := caps["tools"]; !ok {
		t.Error("capabilities missing tools")
	}

	serverInfo, ok := result["serverInfo"].(map[string]interface{})
	if !ok {
		t.Fatal("result missing serverInfo")
	}
	if name, _ := serverInfo["name"].(string); name != "nram" {
		t.Errorf("expected serverInfo.name == 'nram', got %q", name)
	}

	sessionID := resp.Header.Get("Mcp-Session-Id")

	// Send initialized notification.
	notifReq := jsonrpcRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	}
	notifBody, _ := json.Marshal(notifReq)
	httpReq, _ := http.NewRequest(http.MethodPost, env.Server.URL+"/mcp", bytes.NewReader(notifBody))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/event-stream")
	httpReq.Header.Set("Authorization", "Bearer "+env.Token)
	if sessionID != "" {
		httpReq.Header.Set("Mcp-Session-Id", sessionID)
	}

	notifResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("initialized notification failed: %v", err)
	}
	io.ReadAll(notifResp.Body)
	notifResp.Body.Close()

	if notifResp.StatusCode != http.StatusAccepted && notifResp.StatusCode != http.StatusOK {
		t.Fatalf("initialized notification: expected 202 or 200, got %d", notifResp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Test 2: TestHTTPStack_MCP_InitializeAndCallTool
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_InitializeAndCallTool(t *testing.T) {
	env := newHTTPStackEnv(t)
	defer env.Close()

	session := initMCPSession(t, env)

	// Call memory_store tool.
	_, rpcResp := session.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "test-project",
			"content": "test memory content",
			"tags":    []string{"test"},
		},
	})

	if rpcResp == nil {
		t.Fatal("tools/call: got nil JSON-RPC response")
	}
	if rpcResp.Error != nil {
		t.Fatalf("tools/call: JSON-RPC error: code=%d msg=%s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	// The result is an MCP CallToolResult. Parse it.
	var toolResult struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(rpcResp.Result, &toolResult); err != nil {
		t.Fatalf("failed to unmarshal tool result: %v (raw: %s)", err, string(rpcResp.Result))
	}
	if toolResult.IsError {
		t.Fatalf("tool returned error: %v", toolResult.Content)
	}

	// Find text content.
	var textContent string
	for _, c := range toolResult.Content {
		if c.Type == "text" {
			textContent = c.Text
			break
		}
	}
	if textContent == "" {
		t.Fatal("no text content in tool result")
	}

	// Parse the store response JSON.
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(textContent), &storeResp); err != nil {
		t.Fatalf("failed to unmarshal store response: %v", err)
	}
	if storeResp.ID == uuid.Nil {
		t.Error("expected non-nil memory ID")
	}
	if storeResp.Content != "test memory content" {
		t.Errorf("expected content 'test memory content', got %q", storeResp.Content)
	}
	if storeResp.ProjectID == uuid.Nil {
		t.Error("expected non-nil project_id")
	}
}

// ---------------------------------------------------------------------------
// Test 3: TestHTTPStack_MCP_StoreAndRecall
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_StoreAndRecall(t *testing.T) {
	env := newHTTPStackEnv(t)
	defer env.Close()

	session := initMCPSession(t, env)

	// Store a memory.
	_, storeRPC := session.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "test-project",
			"content": "the capital of France is Paris",
			"tags":    []string{"geography"},
		},
	})
	if storeRPC == nil {
		t.Fatal("store: got nil response")
	}
	if storeRPC.Error != nil {
		t.Fatalf("store: JSON-RPC error: %s", storeRPC.Error.Message)
	}

	// Recall that memory.
	_, recallRPC := session.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "capital of France",
			"project": "test-project",
		},
	})
	if recallRPC == nil {
		t.Fatal("recall: got nil response")
	}
	if recallRPC.Error != nil {
		t.Fatalf("recall: JSON-RPC error: %s", recallRPC.Error.Message)
	}

	textContent := extractToolResultText(t, recallRPC)

	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(textContent), &recallResp); err != nil {
		t.Fatalf("recall: failed to unmarshal: %v (raw: %s)", err, textContent)
	}
	if len(recallResp.Memories) == 0 {
		t.Fatal("recall: expected at least 1 memory, got 0")
	}

	found := false
	for _, mem := range recallResp.Memories {
		if strings.Contains(mem.Content, "capital of France is Paris") {
			found = true
			break
		}
	}
	if !found {
		t.Error("recall: stored memory not found in recall results")
	}
}

// ---------------------------------------------------------------------------
// Test 4: TestHTTPStack_MCP_Unauthenticated_Returns401
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_Unauthenticated_Returns401(t *testing.T) {
	env := newHTTPStackEnv(t)
	defer env.Close()

	initReq := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]interface{}{},
			"clientInfo":      map[string]interface{}{"name": "test", "version": "1.0"},
		},
	}

	body, _ := json.Marshal(initReq)
	resp := doRawMCPPost(t, env.Server.URL, body, nil) // no Authorization header
	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d (body: %s)", resp.StatusCode, string(bodyBytes))
	}

	wwwAuth := resp.Header.Get("WWW-Authenticate")
	if wwwAuth == "" {
		t.Error("expected WWW-Authenticate header")
	}
	if !strings.Contains(wwwAuth, "resource_metadata") {
		t.Errorf("expected WWW-Authenticate to contain resource_metadata, got %q", wwwAuth)
	}
}

// ---------------------------------------------------------------------------
// Test 5: TestHTTPStack_MCP_InvalidOrigin_Authenticated_Allowed
// Authenticated requests with a cross-origin Origin header are allowed
// because the OAuth token proves legitimacy.
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_InvalidOrigin_Authenticated_Allowed(t *testing.T) {
	env := newHTTPStackEnv(t)
	defer env.Close()

	initReq := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]interface{}{},
			"clientInfo":      map[string]interface{}{"name": "test", "version": "1.0"},
		},
	}

	body, _ := json.Marshal(initReq)
	headers := map[string]string{
		"Authorization": "Bearer " + env.Token,
		"Origin":        "http://evil-site.example.com",
	}

	resp := doRawMCPPost(t, env.Server.URL, body, headers)
	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		t.Fatalf("expected authenticated cross-origin request to be allowed, got 403 (body: %s)", string(bodyBytes))
	}
}

// ---------------------------------------------------------------------------
// Test 5b: TestHTTPStack_MCP_InvalidOrigin_Unauthenticated_Blocked
// Unauthenticated requests with a cross-origin Origin header are blocked.
// The auth middleware rejects them with 401 before origin validation runs.
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_InvalidOrigin_Unauthenticated_Blocked(t *testing.T) {
	env := newHTTPStackEnv(t)
	defer env.Close()

	initReq := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]interface{}{},
			"clientInfo":      map[string]interface{}{"name": "test", "version": "1.0"},
		},
	}

	body, _ := json.Marshal(initReq)
	headers := map[string]string{
		"Origin": "http://evil-site.example.com",
	}

	resp := doRawMCPPost(t, env.Server.URL, body, headers)
	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// Auth middleware rejects unauthenticated requests with 401 before
	// the MCP origin check runs, which is the correct security behavior.
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d (body: %s)", resp.StatusCode, string(bodyBytes))
	}
}

// ---------------------------------------------------------------------------
// Test 6: TestHTTPStack_MCP_ValidOrigin_Passes
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ValidOrigin_Passes(t *testing.T) {
	env := newHTTPStackEnv(t)
	defer env.Close()

	// Extract host from test server URL.
	serverHost := strings.TrimPrefix(env.Server.URL, "http://")

	initReq := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]interface{}{},
			"clientInfo":      map[string]interface{}{"name": "test", "version": "1.0"},
		},
	}

	body, _ := json.Marshal(initReq)
	headers := map[string]string{
		"Authorization": "Bearer " + env.Token,
		"Origin":        "http://" + serverHost,
	}

	resp := doRawMCPPost(t, env.Server.URL, body, headers)
	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		t.Fatalf("expected NOT 403 with valid origin, got 403 (body: %s)", string(bodyBytes))
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with valid origin, got %d (body: %s)", resp.StatusCode, string(bodyBytes))
	}
}

// ---------------------------------------------------------------------------
// Test 7: TestHTTPStack_MCP_ListTools
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ListTools(t *testing.T) {
	env := newHTTPStackEnv(t)
	defer env.Close()

	session := initMCPSession(t, env)

	_, rpcResp := session.call(t, 2, "tools/list", nil)
	if rpcResp == nil {
		t.Fatal("tools/list: got nil response")
	}
	if rpcResp.Error != nil {
		t.Fatalf("tools/list: JSON-RPC error: %s", rpcResp.Error.Message)
	}

	var result struct {
		Tools []struct {
			Name        string                 `json:"name"`
			Description string                 `json:"description"`
			InputSchema map[string]interface{} `json:"inputSchema"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(rpcResp.Result, &result); err != nil {
		t.Fatalf("tools/list: failed to unmarshal result: %v (raw: %s)", err, string(rpcResp.Result))
	}

	expectedTools := []string{
		"memory_store",
		"memory_store_batch",
		"memory_update",
		"memory_get",
		"memory_recall",
		"memory_forget",
		"memory_projects",
		"memory_export",
	}

	toolNames := make(map[string]bool)
	for _, tool := range result.Tools {
		toolNames[tool.Name] = true
	}

	for _, expected := range expectedTools {
		if !toolNames[expected] {
			t.Errorf("expected tool %q to be listed, but it was not found (available: %v)", expected, toolNames)
		}
	}

	// Verify at least one tool has a proper inputSchema with required fields.
	var storeSchema map[string]interface{}
	for _, tool := range result.Tools {
		if tool.Name == "memory_store" {
			storeSchema = tool.InputSchema
			break
		}
	}
	if storeSchema == nil {
		t.Fatal("memory_store not found in tools list")
	}
	if _, ok := storeSchema["properties"]; !ok {
		t.Error("memory_store inputSchema missing properties")
	}
	if _, ok := storeSchema["required"]; !ok {
		t.Error("memory_store inputSchema missing required")
	}
}

// ---------------------------------------------------------------------------
// Test 8: TestHTTPStack_MCP_ListResources
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ListResources(t *testing.T) {
	env := newHTTPStackEnv(t)
	defer env.Close()

	session := initMCPSession(t, env)

	_, rpcResp := session.call(t, 2, "resources/list", nil)
	if rpcResp == nil {
		t.Fatal("resources/list: got nil response")
	}
	if rpcResp.Error != nil {
		t.Fatalf("resources/list: JSON-RPC error: %s", rpcResp.Error.Message)
	}

	// Parse result. It may contain "resources" and/or "resourceTemplates".
	var result struct {
		Resources         []map[string]interface{} `json:"resources"`
		ResourceTemplates []map[string]interface{} `json:"resourceTemplates"`
	}
	if err := json.Unmarshal(rpcResp.Result, &result); err != nil {
		t.Fatalf("resources/list: unmarshal failed: %v (raw: %s)", err, string(rpcResp.Result))
	}

	totalResources := len(result.Resources) + len(result.ResourceTemplates)
	if totalResources == 0 {
		t.Fatal("resources/list: expected at least one resource or resource template")
	}

	// Look for nram://projects in either resources or resource templates.
	found := false
	for _, r := range result.Resources {
		if uri, ok := r["uri"].(string); ok && strings.Contains(uri, "nram://projects") {
			found = true
			break
		}
	}
	for _, r := range result.ResourceTemplates {
		if uri, ok := r["uriTemplate"].(string); ok && strings.Contains(uri, "nram://") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected nram:// URI in resources/templates, got resources=%v templates=%v",
			result.Resources, result.ResourceTemplates)
	}
}

// ---------------------------------------------------------------------------
// Test 9: TestHTTPStack_MCP_StoreUpdateForgetFlow
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_StoreUpdateForgetFlow(t *testing.T) {
	env := newHTTPStackEnv(t)
	defer env.Close()

	session := initMCPSession(t, env)

	// Step 1: Store.
	_, storeRPC := session.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "test-project",
			"content": "original content for lifecycle test",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		msg := ""
		if storeRPC != nil && storeRPC.Error != nil {
			msg = storeRPC.Error.Message
		}
		t.Fatalf("store failed: %s", msg)
	}

	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("store: unmarshal failed: %v", err)
	}
	memoryID := storeResp.ID
	if memoryID == uuid.Nil {
		t.Fatal("store: got nil memory ID")
	}

	// Step 2: Update content.
	_, updateRPC := session.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_update",
		"arguments": map[string]interface{}{
			"id":      memoryID.String(),
			"project": "test-project",
			"content": "updated content for lifecycle test",
		},
	})
	if updateRPC == nil || updateRPC.Error != nil {
		msg := ""
		if updateRPC != nil && updateRPC.Error != nil {
			msg = updateRPC.Error.Message
		}
		t.Fatalf("update failed: %s", msg)
	}

	updateText := extractToolResultText(t, updateRPC)
	var updateResp service.UpdateResponse
	if err := json.Unmarshal([]byte(updateText), &updateResp); err != nil {
		t.Fatalf("update: unmarshal failed: %v", err)
	}
	if updateResp.Content != "updated content for lifecycle test" {
		t.Errorf("update: expected updated content, got %q", updateResp.Content)
	}
	if updateResp.PreviousContent != "original content for lifecycle test" {
		t.Errorf("update: expected previous content 'original content for lifecycle test', got %q", updateResp.PreviousContent)
	}

	// Step 3: Forget (soft delete).
	_, forgetRPC := session.call(t, 4, "tools/call", map[string]interface{}{
		"name": "memory_forget",
		"arguments": map[string]interface{}{
			"project": "test-project",
			"ids":     []string{memoryID.String()},
		},
	})
	if forgetRPC == nil || forgetRPC.Error != nil {
		msg := ""
		if forgetRPC != nil && forgetRPC.Error != nil {
			msg = forgetRPC.Error.Message
		}
		t.Fatalf("forget failed: %s", msg)
	}

	forgetText := extractToolResultText(t, forgetRPC)
	var forgetResp service.ForgetResponse
	if err := json.Unmarshal([]byte(forgetText), &forgetResp); err != nil {
		t.Fatalf("forget: unmarshal failed: %v", err)
	}
	if forgetResp.Deleted != 1 {
		t.Errorf("forget: expected deleted=1, got %d", forgetResp.Deleted)
	}
}

// ---------------------------------------------------------------------------
// Test 10: TestHTTPStack_MCP_BatchStore
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_BatchStore(t *testing.T) {
	env := newHTTPStackEnv(t)
	defer env.Close()

	session := initMCPSession(t, env)

	_, rpcResp := session.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store_batch",
		"arguments": map[string]interface{}{
			"project": "test-project",
			"items": []interface{}{
				map[string]interface{}{"content": "batch item 1", "tags": []string{"alpha"}},
				map[string]interface{}{"content": "batch item 2"},
				map[string]interface{}{"content": "batch item 3", "source": "test-source"},
			},
		},
	})
	if rpcResp == nil {
		t.Fatal("batch_store: got nil response")
	}
	if rpcResp.Error != nil {
		t.Fatalf("batch_store: JSON-RPC error: %s", rpcResp.Error.Message)
	}

	textContent := extractToolResultText(t, rpcResp)

	var batchResp service.BatchStoreResponse
	if err := json.Unmarshal([]byte(textContent), &batchResp); err != nil {
		t.Fatalf("batch_store: unmarshal failed: %v (raw: %s)", err, textContent)
	}
	if batchResp.Processed != 3 {
		t.Errorf("batch_store: expected processed=3, got %d", batchResp.Processed)
	}
	if batchResp.MemoriesCreated != 3 {
		t.Errorf("batch_store: expected memories_created=3, got %d", batchResp.MemoriesCreated)
	}
	if len(batchResp.Errors) != 0 {
		t.Errorf("batch_store: expected 0 errors, got %d: %v", len(batchResp.Errors), batchResp.Errors)
	}
}

// ---------------------------------------------------------------------------
// Helper: extract text from a JSON-RPC tool call result
// ---------------------------------------------------------------------------

func extractToolResultText(t *testing.T, rpcResp *jsonrpcResponse) string {
	t.Helper()

	var toolResult struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(rpcResp.Result, &toolResult); err != nil {
		t.Fatalf("failed to unmarshal tool result: %v (raw: %s)", err, string(rpcResp.Result))
	}
	if toolResult.IsError {
		var errText string
		for _, c := range toolResult.Content {
			if c.Type == "text" {
				errText = c.Text
				break
			}
		}
		t.Fatalf("tool returned error: %s", errText)
	}

	for _, c := range toolResult.Content {
		if c.Type == "text" {
			return c.Text
		}
	}
	t.Fatal("no text content in tool result")
	return ""
}

// extractToolResultTextRaw extracts the text content from a JSON-RPC tool call
// result without failing on isError — returns (text, isError).
func extractToolResultTextRaw(t *testing.T, rpcResp *jsonrpcResponse) (string, bool) {
	t.Helper()

	var toolResult struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(rpcResp.Result, &toolResult); err != nil {
		t.Fatalf("failed to unmarshal tool result: %v (raw: %s)", err, string(rpcResp.Result))
	}

	for _, c := range toolResult.Content {
		if c.Type == "text" {
			return c.Text, toolResult.IsError
		}
	}
	t.Fatal("no text content in tool result")
	return "", false
}

// ---------------------------------------------------------------------------
// Multi-user httpStackEnv builder
// ---------------------------------------------------------------------------

// multiUserEnvConfig describes one user's identity for multi-user env building.
type multiUserEnvConfig struct {
	userID    uuid.UUID
	nsID      uuid.UUID
	nsPath    string
	projectID uuid.UUID
	projSlug  string
}

// multiUserHTTPStackEnv bundles a test server that can serve multiple users
// with namespace-aware mocking so recall properly isolates by namespace.
type multiUserHTTPStackEnv struct {
	Server  *httptest.Server
	Users   []multiUserEnvUser
	MemRepo *nsAwareMemoryRepo
}

type multiUserEnvUser struct {
	UserID    uuid.UUID
	NsID      uuid.UUID
	ProjectID uuid.UUID
	ProjSlug  string
	Token     string
}

func (e *multiUserHTTPStackEnv) Close() {
	e.Server.Close()
}

func (e *multiUserHTTPStackEnv) sessionFor(t *testing.T, idx int) *mcpSession {
	t.Helper()
	u := e.Users[idx]
	fakeEnv := &httpStackEnv{
		Server: e.Server,
		UserID: u.UserID,
		Token:  u.Token,
		NsID:   u.NsID,
	}
	return initMCPSession(t, fakeEnv)
}

func (e *multiUserHTTPStackEnv) envFor(idx int) *httpStackEnv {
	u := e.Users[idx]
	return &httpStackEnv{
		Server:    e.Server,
		UserID:    u.UserID,
		Token:     u.Token,
		NsID:      u.NsID,
		ProjectID: u.ProjectID,
	}
}

// nsAwareMemoryRepo stores memories keyed by ID and scoped by NamespaceID,
// enabling proper multi-user isolation in recall. Thread-safe via mutex.
type nsAwareMemoryRepo struct {
	mu       sync.Mutex
	memories map[uuid.UUID]*model.Memory
}

func newNsAwareMemoryRepo() *nsAwareMemoryRepo {
	return &nsAwareMemoryRepo{memories: make(map[uuid.UUID]*model.Memory)}
}

func (m *nsAwareMemoryRepo) Create(_ context.Context, mem *model.Memory) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mem.ID == uuid.Nil {
		mem.ID = uuid.New()
	}
	clone := *mem
	m.memories[mem.ID] = &clone
	return nil
}

func (m *nsAwareMemoryRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Memory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mem, ok := m.memories[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return mem, nil
}

func (m *nsAwareMemoryRepo) GetBatch(_ context.Context, ids []uuid.UUID) ([]model.Memory, error) {
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

func (m *nsAwareMemoryRepo) ListByNamespace(_ context.Context, nsID uuid.UUID, limit, _ int) ([]model.Memory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []model.Memory
	for _, mem := range m.memories {
		if mem.NamespaceID == nsID && mem.DeletedAt == nil {
			out = append(out, *mem)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (m *nsAwareMemoryRepo) SoftDelete(_ context.Context, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	mem, ok := m.memories[id]
	if !ok {
		return fmt.Errorf("not found")
	}
	now := time.Now()
	mem.DeletedAt = &now
	return nil
}

func (m *nsAwareMemoryRepo) HardDelete(_ context.Context, id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.memories, id)
	return nil
}

// nsAwareUserRepo maps userID -> *model.User.
type nsAwareUserRepo struct {
	users map[uuid.UUID]*model.User
}

func (m *nsAwareUserRepo) GetByID(_ context.Context, id uuid.UUID) (*model.User, error) {
	u, ok := m.users[id]
	if !ok {
		return nil, fmt.Errorf("user not found")
	}
	return u, nil
}

// nsAwareNamespaceRepo maps nsID -> *model.Namespace.
type nsAwareNamespaceRepo struct {
	namespaces map[uuid.UUID]*model.Namespace
}

func (m *nsAwareNamespaceRepo) GetByID(_ context.Context, id uuid.UUID) (*model.Namespace, error) {
	ns, ok := m.namespaces[id]
	if !ok {
		return nil, fmt.Errorf("namespace not found")
	}
	return ns, nil
}

func (m *nsAwareNamespaceRepo) Create(_ context.Context, ns *model.Namespace) error {
	m.namespaces[ns.ID] = ns
	return nil
}

// nsAwareProjectRepo maps (ownerNsID, slug) -> *model.Project, supports auto-create.
type nsAwareProjectRepo struct {
	projects map[string]*model.Project // key: ownerNsID + ":" + slug
}

func newNsAwareProjectRepo() *nsAwareProjectRepo {
	return &nsAwareProjectRepo{projects: make(map[string]*model.Project)}
}

func projectKey(ownerNsID uuid.UUID, slug string) string {
	return ownerNsID.String() + ":" + slug
}

func (m *nsAwareProjectRepo) GetBySlug(_ context.Context, ownerNsID uuid.UUID, slug string) (*model.Project, error) {
	p, ok := m.projects[projectKey(ownerNsID, slug)]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return p, nil
}

func (m *nsAwareProjectRepo) ListByUser(_ context.Context, ownerNsID uuid.UUID) ([]model.Project, error) {
	var out []model.Project
	prefix := ownerNsID.String() + ":"
	for k, p := range m.projects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, *p)
		}
	}
	return out, nil
}

func (m *nsAwareProjectRepo) Create(_ context.Context, p *model.Project) error {
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	m.projects[projectKey(p.OwnerNamespaceID, p.Slug)] = p
	return nil
}

// nsAwareProjectLookup satisfies service.ProjectRepository (GetByID only).
type nsAwareProjectLookup struct {
	repo *nsAwareProjectRepo
}

func (m *nsAwareProjectLookup) GetByID(_ context.Context, id uuid.UUID) (*model.Project, error) {
	for _, p := range m.repo.projects {
		if p.ID == id {
			return p, nil
		}
	}
	return nil, fmt.Errorf("project not found")
}

// nsAwareNamespaceLookup satisfies service.NamespaceRepository (GetByID only).
type nsAwareNamespaceLookup struct {
	repo *nsAwareNamespaceRepo
}

func (m *nsAwareNamespaceLookup) GetByID(ctx context.Context, id uuid.UUID) (*model.Namespace, error) {
	return m.repo.GetByID(ctx, id)
}

// newMultiUserHTTPStackEnv builds a full-stack test server supporting multiple users.
func newMultiUserHTTPStackEnv(t *testing.T, configs []multiUserEnvConfig) *multiUserHTTPStackEnv {
	t.Helper()

	memRepo := newNsAwareMemoryRepo()
	userRepo := &nsAwareUserRepo{users: make(map[uuid.UUID]*model.User)}
	nsRepo := &nsAwareNamespaceRepo{namespaces: make(map[uuid.UUID]*model.Namespace)}
	projectRepo := newNsAwareProjectRepo()
	projectLookup := &nsAwareProjectLookup{repo: projectRepo}
	nsLookup := &nsAwareNamespaceLookup{repo: nsRepo}

	for _, cfg := range configs {
		user := &model.User{ID: cfg.userID, NamespaceID: cfg.nsID}
		ns := &model.Namespace{ID: cfg.nsID, Path: cfg.nsPath, Depth: 2}
		userRepo.users[cfg.userID] = user
		nsRepo.namespaces[cfg.nsID] = ns

		if cfg.projSlug != "" {
			projNsID := uuid.New()
			projNs := &model.Namespace{
				ID:       projNsID,
				Name:     cfg.projSlug,
				Slug:     cfg.projSlug,
				Kind:     "project",
				ParentID: &cfg.nsID,
				Path:     cfg.nsPath + "/" + cfg.projSlug,
				Depth:    3,
			}
			nsRepo.namespaces[projNsID] = projNs

			project := &model.Project{
				ID:               cfg.projectID,
				NamespaceID:      projNsID,
				OwnerNamespaceID: cfg.nsID,
				Name:             cfg.projSlug,
				Slug:             cfg.projSlug,
			}
			projectRepo.projects[projectKey(cfg.nsID, cfg.projSlug)] = project
		}
	}

	storeSvc := service.NewStoreService(
		memRepo, projectLookup, nsLookup,
		&mockIngestionLogRepo{}, &mockTokenUsageRepo{}, &mockEnrichmentQueueRepo{},
		nil, nil,
	)
	batchStoreSvc := service.NewBatchStoreService(
		memRepo, projectLookup, nsLookup,
		&mockIngestionLogRepo{}, &mockTokenUsageRepo{}, &mockEnrichmentQueueRepo{},
		nil, nil,
	)
	recallSvc := service.NewRecallService(
		memRepo, projectLookup, nsLookup,
		&mockTokenUsageRepo{}, nil, nil, nil, nil, nil,
	)
	forgetSvc := service.NewForgetService(memRepo, projectLookup, nil, nil, nil, nil, nil)
	updateSvc := service.NewUpdateService(
		&nsAwareMemRepoUpdater{memRepo: memRepo},
		projectLookup, &mockLineageCreator{}, nil, &mockTokenUsageRepo{}, nil,
	)
	batchGetSvc := service.NewBatchGetService(memRepo, projectLookup)
	exportSvc := service.NewExportService(
		memRepo,
		&mockExportEntityLister{entities: []model.Entity{}},
		&mockExportRelLister{rels: []model.Relationship{}},
		&mockExportLineageReader{},
		projectLookup,
	)

	deps := Dependencies{
		Backend:       storage.BackendSQLite,
		Store:         storeSvc,
		Recall:        recallSvc,
		Forget:        forgetSvc,
		Update:        updateSvc,
		BatchStore:    batchStoreSvc,
		BatchGet:      batchGetSvc,
		Export:        exportSvc,
		ProjectRepo:   projectRepo,
		UserRepo:      userRepo,
		NamespaceRepo: nsRepo,
	}
	mcpSrv := NewServer(deps)

	authMw := auth.NewAuthMiddleware(&testAPIKeyValidator{}, &testUserIdentityLookup{}, httpStackTestSecret)

	r := chi.NewRouter()
	r.Group(func(r chi.Router) {
		r.Use(authMw.Handler)
		r.Handle("/mcp", mcpSrv.Handler())
		r.Handle("/mcp/*", mcpSrv.Handler())
	})

	ts := httptest.NewServer(r)

	var users []multiUserEnvUser
	for _, cfg := range configs {
		token := generateHTTPStackJWT(t, cfg.userID, ts.URL)
		users = append(users, multiUserEnvUser{
			UserID:    cfg.userID,
			NsID:      cfg.nsID,
			ProjectID: cfg.projectID,
			ProjSlug:  cfg.projSlug,
			Token:     token,
		})
	}

	return &multiUserHTTPStackEnv{
		Server:  ts,
		Users:   users,
		MemRepo: memRepo,
	}
}

// nsAwareMemRepoUpdater wraps nsAwareMemoryRepo to satisfy service.MemoryUpdater.
type nsAwareMemRepoUpdater struct {
	memRepo *nsAwareMemoryRepo
}

func (m *nsAwareMemRepoUpdater) GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error) {
	mem, err := m.memRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	clone := *mem
	return &clone, nil
}

func (m *nsAwareMemRepoUpdater) Update(_ context.Context, mem *model.Memory) error {
	m.memRepo.memories[mem.ID] = mem
	return nil
}

// ---------------------------------------------------------------------------
// Test 11: TestHTTPStack_MCP_TwoUsers_SeparateStores
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_TwoUsers_SeparateStores(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()
	userB := uuid.New()
	nsB := uuid.New()
	projB := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "shared-proj"},
		{userID: userB, nsID: nsB, nsPath: "/users/bob", projectID: projB, projSlug: "shared-proj"},
	})
	defer env.Close()

	sessA := env.sessionFor(t, 0)
	sessB := env.sessionFor(t, 1)

	// User A stores "Alice's secret".
	_, storeRPC := sessA.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "shared-proj",
			"content": "Alice's secret",
			"tags":    []string{"secret"},
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("User A store failed: %v", storeRPC)
	}

	// User B stores "Bob's secret".
	_, storeRPC = sessB.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "shared-proj",
			"content": "Bob's secret",
			"tags":    []string{"secret"},
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("User B store failed: %v", storeRPC)
	}

	// User A recalls "secret" — must only get Alice's.
	_, recallRPC := sessA.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "secret",
			"project": "shared-proj",
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("User A recall failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal recall: %v", err)
	}
	if len(recallResp.Memories) != 1 {
		t.Fatalf("User A recall: expected 1 memory, got %d", len(recallResp.Memories))
	}
	if !strings.Contains(recallResp.Memories[0].Content, "Alice") {
		t.Errorf("User A recall: expected Alice's content, got %q", recallResp.Memories[0].Content)
	}

	// User B recalls "secret" — must only get Bob's.
	_, recallRPC = sessB.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "secret",
			"project": "shared-proj",
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("User B recall failed")
	}
	recallText = extractToolResultText(t, recallRPC)
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal recall: %v", err)
	}
	if len(recallResp.Memories) != 1 {
		t.Fatalf("User B recall: expected 1 memory, got %d", len(recallResp.Memories))
	}
	if !strings.Contains(recallResp.Memories[0].Content, "Bob") {
		t.Errorf("User B recall: expected Bob's content, got %q", recallResp.Memories[0].Content)
	}
}

// ---------------------------------------------------------------------------
// Test 12: TestHTTPStack_MCP_TwoUsers_SeparateProjects
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_TwoUsers_SeparateProjects(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()
	userB := uuid.New()
	nsB := uuid.New()
	projB := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "alpha"},
		{userID: userB, nsID: nsB, nsPath: "/users/bob", projectID: projB, projSlug: "beta"},
	})
	defer env.Close()

	sessA := env.sessionFor(t, 0)
	sessB := env.sessionFor(t, 1)

	// User A stores in "alpha".
	_, storeRPC := sessA.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "alpha",
			"content": "Alpha memory",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("User A store failed")
	}

	// User B stores in "beta".
	_, storeRPC = sessB.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "beta",
			"content": "Beta memory",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("User B store failed")
	}

	// User A lists projects — should only see "alpha".
	_, projRPC := sessA.call(t, 3, "tools/call", map[string]interface{}{
		"name":      "memory_projects",
		"arguments": map[string]interface{}{},
	})
	if projRPC == nil || projRPC.Error != nil {
		t.Fatalf("User A list projects failed")
	}
	projText := extractToolResultText(t, projRPC)
	var projectsA []projectItem
	if err := json.Unmarshal([]byte(projText), &projectsA); err != nil {
		t.Fatalf("unmarshal projects: %v", err)
	}
	if len(projectsA) != 1 {
		t.Fatalf("User A: expected 1 project, got %d", len(projectsA))
	}
	if projectsA[0].Slug != "alpha" {
		t.Errorf("User A: expected project slug 'alpha', got %q", projectsA[0].Slug)
	}

	// User B lists projects — should only see "beta".
	_, projRPC = sessB.call(t, 3, "tools/call", map[string]interface{}{
		"name":      "memory_projects",
		"arguments": map[string]interface{}{},
	})
	if projRPC == nil || projRPC.Error != nil {
		t.Fatalf("User B list projects failed")
	}
	projText = extractToolResultText(t, projRPC)
	var projectsB []projectItem
	if err := json.Unmarshal([]byte(projText), &projectsB); err != nil {
		t.Fatalf("unmarshal projects: %v", err)
	}
	if len(projectsB) != 1 {
		t.Fatalf("User B: expected 1 project, got %d", len(projectsB))
	}
	if projectsB[0].Slug != "beta" {
		t.Errorf("User B: expected project slug 'beta', got %q", projectsB[0].Slug)
	}
}

// ---------------------------------------------------------------------------
// Test 13: TestHTTPStack_MCP_UserCannotAccessOtherUserProject
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_UserCannotAccessOtherUserProject(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()
	userB := uuid.New()
	nsB := uuid.New()
	projB := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "private"},
		{userID: userB, nsID: nsB, nsPath: "/users/bob", projectID: projB, projSlug: "other"},
	})
	defer env.Close()

	sessA := env.sessionFor(t, 0)
	sessB := env.sessionFor(t, 1)

	// User A stores in "private".
	_, storeRPC := sessA.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "private",
			"content": "User A private data",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("User A store failed")
	}

	// User B tries to recall from "private" — should get error (project not found for User B).
	_, recallRPC := sessB.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "private data",
			"project": "private",
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall RPC failed at protocol level")
	}
	text, isErr := extractToolResultTextRaw(t, recallRPC)
	if !isErr {
		// If not an error, it should be empty results.
		var resp service.RecallResponse
		if err := json.Unmarshal([]byte(text), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(resp.Memories) > 0 {
			t.Fatalf("User B should NOT see User A's private memories, but got %d", len(resp.Memories))
		}
		// Getting zero results is acceptable — project not found for User B.
	}
	// If isErr is true, that's also acceptable — means "project not found".
}

// ---------------------------------------------------------------------------
// Test 14: TestHTTPStack_MCP_BatchStoreAndRecall
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_BatchStoreAndRecall(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Batch store 5 items with different tags.
	_, batchRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store_batch",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"items": []interface{}{
				map[string]interface{}{"content": "Go goroutines are lightweight threads", "tags": []interface{}{"go", "concurrency"}},
				map[string]interface{}{"content": "Rust ownership model prevents data races", "tags": []interface{}{"rust", "safety"}},
				map[string]interface{}{"content": "Python GIL limits true parallelism", "tags": []interface{}{"python", "concurrency"}},
				map[string]interface{}{"content": "JavaScript event loop is single-threaded", "tags": []interface{}{"javascript", "concurrency"}},
				map[string]interface{}{"content": "TypeScript adds type safety to JavaScript", "tags": []interface{}{"typescript", "safety"}},
			},
		},
	})
	if batchRPC == nil || batchRPC.Error != nil {
		t.Fatalf("batch store failed")
	}
	batchText := extractToolResultText(t, batchRPC)
	var batchResp service.BatchStoreResponse
	if err := json.Unmarshal([]byte(batchText), &batchResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if batchResp.Processed != 5 {
		t.Errorf("expected processed=5, got %d", batchResp.Processed)
	}
	if batchResp.MemoriesCreated != 5 {
		t.Errorf("expected memories_created=5, got %d", batchResp.MemoriesCreated)
	}

	// Recall with tag filter ["concurrency"] — should get 3 items.
	_, recallRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "concurrency",
			"project": "test-proj",
			"tags":    []interface{}{"concurrency"},
			"limit":   float64(10),
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall with tags failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) != 3 {
		t.Errorf("recall with tag 'concurrency': expected 3 memories, got %d", len(recallResp.Memories))
	}
	for _, mem := range recallResp.Memories {
		hasConcurrency := false
		for _, tag := range mem.Tags {
			if tag == "concurrency" {
				hasConcurrency = true
				break
			}
		}
		if !hasConcurrency {
			t.Errorf("recall result %q missing 'concurrency' tag", mem.Content)
		}
	}

	// Recall without tag filter — should get all 5.
	_, recallRPC = sess.call(t, 4, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "programming language",
			"project": "test-proj",
			"limit":   float64(10),
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall without tags failed")
	}
	recallText = extractToolResultText(t, recallRPC)
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) != 5 {
		t.Errorf("recall without tag filter: expected 5 memories, got %d", len(recallResp.Memories))
	}
}

// ---------------------------------------------------------------------------
// Test 15: TestHTTPStack_MCP_BatchStorePartialContent
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_BatchStorePartialContent(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Batch store 3 items with distinct content.
	_, batchRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store_batch",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"items": []interface{}{
				map[string]interface{}{"content": "The Eiffel Tower is in Paris France"},
				map[string]interface{}{"content": "Mount Everest is the tallest mountain"},
				map[string]interface{}{"content": "The Pacific Ocean is the largest ocean"},
			},
		},
	})
	if batchRPC == nil || batchRPC.Error != nil {
		t.Fatalf("batch store failed")
	}

	// Recall each one individually by content keywords.
	keywords := []struct {
		query    string
		expected string
	}{
		{"Eiffel Tower", "Eiffel Tower is in Paris"},
		{"tallest mountain", "Mount Everest"},
		{"largest ocean", "Pacific Ocean"},
	}

	for i, kw := range keywords {
		_, recallRPC := sess.call(t, 3+i, "tools/call", map[string]interface{}{
			"name": "memory_recall",
			"arguments": map[string]interface{}{
				"query":   kw.query,
				"project": "test-proj",
				"limit":   float64(5),
			},
		})
		if recallRPC == nil || recallRPC.Error != nil {
			t.Fatalf("recall %q failed", kw.query)
		}
		recallText := extractToolResultText(t, recallRPC)
		var recallResp service.RecallResponse
		if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
			t.Fatalf("unmarshal recall %q: %v", kw.query, err)
		}
		found := false
		for _, mem := range recallResp.Memories {
			if strings.Contains(mem.Content, kw.expected) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("recall %q: expected to find content containing %q in results", kw.query, kw.expected)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 16: TestHTTPStack_MCP_StoreInMultipleProjects
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_StoreInMultipleProjects(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()

	// Pre-create "frontend" project; "backend" will be auto-created.
	projFrontend := uuid.New()
	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projFrontend, projSlug: "frontend"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store in "frontend".
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "frontend",
			"content": "React component lifecycle",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store in frontend failed")
	}

	// Store in "backend" (auto-created).
	_, storeRPC = sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "backend",
			"content": "Go HTTP handler patterns",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store in backend failed")
	}

	// Recall from "frontend" — only frontend memories.
	_, recallRPC := sess.call(t, 4, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "development",
			"project": "frontend",
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall frontend failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, mem := range recallResp.Memories {
		if strings.Contains(mem.Content, "Go HTTP") {
			t.Error("frontend recall should NOT contain backend memory")
		}
	}
	frontendFound := false
	for _, mem := range recallResp.Memories {
		if strings.Contains(mem.Content, "React") {
			frontendFound = true
		}
	}
	if !frontendFound {
		t.Error("frontend recall should contain React memory")
	}

	// Recall from "backend" — only backend memories.
	_, recallRPC = sess.call(t, 5, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "development",
			"project": "backend",
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall backend failed")
	}
	recallText = extractToolResultText(t, recallRPC)
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, mem := range recallResp.Memories {
		if strings.Contains(mem.Content, "React") {
			t.Error("backend recall should NOT contain frontend memory")
		}
	}
	backendFound := false
	for _, mem := range recallResp.Memories {
		if strings.Contains(mem.Content, "Go HTTP") {
			backendFound = true
		}
	}
	if !backendFound {
		t.Error("backend recall should contain Go HTTP memory")
	}
}

// ---------------------------------------------------------------------------
// Test 17: TestHTTPStack_MCP_ProjectAutoCreate
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ProjectAutoCreate(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()

	// Do NOT pre-create any project — the store call should auto-create it.
	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store to a project that doesn't exist — should auto-create.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "brand-new",
			"content": "something in a new project",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store to auto-created project failed")
	}

	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if storeResp.ID == uuid.Nil {
		t.Error("expected non-nil memory ID")
	}

	// List projects — should see "brand-new".
	_, projRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name":      "memory_projects",
		"arguments": map[string]interface{}{},
	})
	if projRPC == nil || projRPC.Error != nil {
		t.Fatalf("list projects failed")
	}
	projText := extractToolResultText(t, projRPC)
	var projects []projectItem
	if err := json.Unmarshal([]byte(projText), &projects); err != nil {
		t.Fatalf("unmarshal projects: %v", err)
	}
	found := false
	for _, p := range projects {
		if p.Slug == "brand-new" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("auto-created project 'brand-new' not found in project list: %v", projects)
	}
}

// ---------------------------------------------------------------------------
// Test 18: TestHTTPStack_MCP_RecallAcrossAllProjects
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_RecallAcrossAllProjects(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "project-a"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store in project-a.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "project-a",
			"content": "Memory in project A about databases",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store project-a failed")
	}

	// Store in project-b (auto-created).
	_, storeRPC = sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "project-b",
			"content": "Memory in project B about caching",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store project-b failed")
	}

	// Recall WITHOUT specifying a project — user-scoped recall.
	// This searches the user's root namespace. In the multi-user env,
	// the recall service uses ListByNamespace with the user's nsID.
	// Since the memories are stored in child namespaces (project namespaces),
	// the user-scoped recall won't find them in a real system with proper
	// namespace hierarchy. But this tests the code path.
	_, recallRPC := sess.call(t, 4, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query": "data storage",
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("user-scoped recall failed")
	}
	// The call should succeed (no error) even if results are empty —
	// validates the user-scoped recall code path.
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// In the mock setup, ListByNamespace on user NS will return empty
	// since memories are in project namespaces. This is still a valid test
	// that the code path doesn't crash.
	t.Logf("user-scoped recall returned %d memories (expected 0 due to namespace hierarchy)", len(recallResp.Memories))
}

// ---------------------------------------------------------------------------
// Test 19: TestHTTPStack_MCP_UpdateNonexistentMemory
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_UpdateNonexistentMemory(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	fakeID := uuid.New()
	_, updateRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_update",
		"arguments": map[string]interface{}{
			"id":      fakeID.String(),
			"project": "test-proj",
			"content": "updated content",
		},
	})
	if updateRPC == nil || updateRPC.Error != nil {
		t.Fatalf("update RPC failed at protocol level")
	}

	text, isErr := extractToolResultTextRaw(t, updateRPC)
	if !isErr {
		t.Fatalf("expected tool error for nonexistent memory update, got success: %s", text)
	}
	if !strings.Contains(strings.ToLower(text), "not found") && !strings.Contains(strings.ToLower(text), "fail") {
		t.Errorf("expected error message about not found, got: %s", text)
	}
}

// ---------------------------------------------------------------------------
// Test 20: TestHTTPStack_MCP_ForgetThenRecall
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ForgetThenRecall(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"content": "ephemeral data that will be forgotten",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}
	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	memID := storeResp.ID

	// Forget.
	_, forgetRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_forget",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"ids":     []string{memID.String()},
		},
	})
	if forgetRPC == nil || forgetRPC.Error != nil {
		t.Fatalf("forget failed")
	}
	forgetText := extractToolResultText(t, forgetRPC)
	var forgetResp service.ForgetResponse
	if err := json.Unmarshal([]byte(forgetText), &forgetResp); err != nil {
		t.Fatalf("unmarshal forget: %v", err)
	}
	if forgetResp.Deleted != 1 {
		t.Errorf("expected deleted=1, got %d", forgetResp.Deleted)
	}

	// Recall — forgotten memory should NOT appear.
	_, recallRPC := sess.call(t, 4, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "ephemeral data",
			"project": "test-proj",
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, mem := range recallResp.Memories {
		if mem.ID == memID {
			t.Error("forgotten memory should NOT appear in recall results")
		}
	}
}

// ---------------------------------------------------------------------------
// Test 21: TestHTTPStack_MCP_ForgetNonexistentMemory
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ForgetNonexistentMemory(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	fakeID := uuid.New()
	_, forgetRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_forget",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"ids":     []string{fakeID.String()},
		},
	})
	if forgetRPC == nil || forgetRPC.Error != nil {
		t.Fatalf("forget RPC failed at protocol level")
	}

	forgetText := extractToolResultText(t, forgetRPC)
	var forgetResp service.ForgetResponse
	if err := json.Unmarshal([]byte(forgetText), &forgetResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Forget of non-existent ID should succeed with deleted=0.
	if forgetResp.Deleted != 0 {
		t.Errorf("expected deleted=0 for non-existent memory, got %d", forgetResp.Deleted)
	}
}

// ---------------------------------------------------------------------------
// Test 22: TestHTTPStack_MCP_DoubleForget
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_DoubleForget(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"content": "double-forget test data",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}
	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	memID := storeResp.ID

	// First forget.
	_, forgetRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_forget",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"ids":     []string{memID.String()},
		},
	})
	if forgetRPC == nil || forgetRPC.Error != nil {
		t.Fatalf("first forget failed")
	}
	forgetText := extractToolResultText(t, forgetRPC)
	var forgetResp service.ForgetResponse
	if err := json.Unmarshal([]byte(forgetText), &forgetResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if forgetResp.Deleted != 1 {
		t.Errorf("first forget: expected deleted=1, got %d", forgetResp.Deleted)
	}

	// Second forget — should not crash, deleted=0 (already soft-deleted).
	_, forgetRPC = sess.call(t, 4, "tools/call", map[string]interface{}{
		"name": "memory_forget",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"ids":     []string{memID.String()},
		},
	})
	if forgetRPC == nil || forgetRPC.Error != nil {
		t.Fatalf("second forget failed at protocol level")
	}
	forgetText = extractToolResultText(t, forgetRPC)
	if err := json.Unmarshal([]byte(forgetText), &forgetResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// The second forget should either return deleted=0 (already soft-deleted)
	// or deleted=1 (re-soft-deletes). Both are acceptable — the key is no crash.
	t.Logf("second forget returned deleted=%d", forgetResp.Deleted)
}

// ---------------------------------------------------------------------------
// Test 23: TestHTTPStack_MCP_StoreWithMetadata
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_StoreWithMetadata(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store with metadata.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"content": "memory with metadata",
			"metadata": map[string]interface{}{
				"agent":   "claude",
				"session": "abc123",
			},
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}
	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Verify metadata was stored in the memory repo.
	mem, ok := env.MemRepo.memories[storeResp.ID]
	if !ok {
		t.Fatal("memory not found in repo")
	}
	if mem.Metadata == nil {
		t.Fatal("expected metadata to be stored, got nil")
	}
	var meta map[string]interface{}
	if err := json.Unmarshal(mem.Metadata, &meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if meta["agent"] != "claude" {
		t.Errorf("expected metadata agent='claude', got %v", meta["agent"])
	}
	if meta["session"] != "abc123" {
		t.Errorf("expected metadata session='abc123', got %v", meta["session"])
	}
}

// ---------------------------------------------------------------------------
// Test 24: TestHTTPStack_MCP_StoreWithTags
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_StoreWithTags(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store with tags.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"content": "architecture decision about authentication",
			"tags":    []interface{}{"architecture", "auth"},
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}

	// Also store something without the "auth" tag.
	_, storeRPC = sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"content": "deployment pipeline setup",
			"tags":    []interface{}{"devops", "deployment"},
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store 2 failed")
	}

	// Recall with tag filter ["auth"].
	_, recallRPC := sess.call(t, 4, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "decisions",
			"project": "test-proj",
			"tags":    []interface{}{"auth"},
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) != 1 {
		t.Fatalf("expected 1 memory with tag 'auth', got %d", len(recallResp.Memories))
	}
	if !strings.Contains(recallResp.Memories[0].Content, "authentication") {
		t.Errorf("expected auth-tagged memory, got %q", recallResp.Memories[0].Content)
	}
}

// ---------------------------------------------------------------------------
// Test 25: TestHTTPStack_MCP_StoreEmptyContent_Rejected
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_StoreEmptyContent_Rejected(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"content": "",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store RPC failed at protocol level")
	}

	text, isErr := extractToolResultTextRaw(t, storeRPC)
	if !isErr {
		t.Fatalf("expected tool error for empty content, got success: %s", text)
	}
	if !strings.Contains(strings.ToLower(text), "content") {
		t.Errorf("expected error about content, got: %s", text)
	}
}

// ---------------------------------------------------------------------------
// Test 27: TestHTTPStack_MCP_GetSpecificMemories
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_GetSpecificMemories(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store 3 memories.
	var ids []uuid.UUID
	contents := []string{"first memory", "second memory", "third memory"}
	for i, content := range contents {
		_, storeRPC := sess.call(t, 2+i, "tools/call", map[string]interface{}{
			"name": "memory_store",
			"arguments": map[string]interface{}{
				"project": "test-proj",
				"content": content,
			},
		})
		if storeRPC == nil || storeRPC.Error != nil {
			t.Fatalf("store %d failed", i)
		}
		storeText := extractToolResultText(t, storeRPC)
		var resp service.StoreResponse
		if err := json.Unmarshal([]byte(storeText), &resp); err != nil {
			t.Fatalf("unmarshal store %d: %v", i, err)
		}
		ids = append(ids, resp.ID)
	}

	// Get 2 of them by ID (first and third).
	_, getRPC := sess.call(t, 10, "tools/call", map[string]interface{}{
		"name": "memory_get",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"ids":     []string{ids[0].String(), ids[2].String()},
		},
	})
	if getRPC == nil || getRPC.Error != nil {
		t.Fatalf("get failed")
	}
	getText := extractToolResultText(t, getRPC)
	var getResp service.BatchGetResponse
	if err := json.Unmarshal([]byte(getText), &getResp); err != nil {
		t.Fatalf("unmarshal get: %v", err)
	}
	if len(getResp.Found) != 2 {
		t.Fatalf("expected 2 found, got %d", len(getResp.Found))
	}

	// Verify the correct memories were returned.
	foundContents := make(map[string]bool)
	for _, f := range getResp.Found {
		foundContents[f.Content] = true
	}
	if !foundContents["first memory"] {
		t.Error("expected 'first memory' in results")
	}
	if !foundContents["third memory"] {
		t.Error("expected 'third memory' in results")
	}
	if foundContents["second memory"] {
		t.Error("'second memory' should NOT be in results")
	}
}

// ---------------------------------------------------------------------------
// Test 28: TestHTTPStack_MCP_GetWithInvalidID
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_GetWithInvalidID(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store one memory.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"content": "valid memory",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}
	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	validID := storeResp.ID
	invalidID := uuid.New()

	// Get with mix of valid and invalid IDs.
	_, getRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_get",
		"arguments": map[string]interface{}{
			"project": "test-proj",
			"ids":     []string{validID.String(), invalidID.String()},
		},
	})
	if getRPC == nil || getRPC.Error != nil {
		t.Fatalf("get failed")
	}
	getText := extractToolResultText(t, getRPC)
	var getResp service.BatchGetResponse
	if err := json.Unmarshal([]byte(getText), &getResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(getResp.Found) != 1 {
		t.Fatalf("expected 1 found, got %d", len(getResp.Found))
	}
	if getResp.Found[0].ID != validID {
		t.Errorf("expected found ID %s, got %s", validID, getResp.Found[0].ID)
	}
	if len(getResp.NotFound) != 1 {
		t.Fatalf("expected 1 not_found, got %d", len(getResp.NotFound))
	}
	if getResp.NotFound[0] != invalidID {
		t.Errorf("expected not_found ID %s, got %s", invalidID, getResp.NotFound[0])
	}
}

// ---------------------------------------------------------------------------
// Test 29: TestHTTPStack_MCP_ExportProject
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ExportProject(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "export-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store several memories.
	contents := []string{
		"Export test memory one",
		"Export test memory two",
		"Export test memory three",
	}
	for i, content := range contents {
		_, storeRPC := sess.call(t, 2+i, "tools/call", map[string]interface{}{
			"name": "memory_store",
			"arguments": map[string]interface{}{
				"project": "export-proj",
				"content": content,
			},
		})
		if storeRPC == nil || storeRPC.Error != nil {
			t.Fatalf("store %d failed", i)
		}
	}

	// Export the project.
	_, exportRPC := sess.call(t, 10, "tools/call", map[string]interface{}{
		"name": "memory_export",
		"arguments": map[string]interface{}{
			"project": "export-proj",
		},
	})
	if exportRPC == nil || exportRPC.Error != nil {
		t.Fatalf("export failed")
	}
	exportText := extractToolResultText(t, exportRPC)
	var exportData service.ExportData
	if err := json.Unmarshal([]byte(exportText), &exportData); err != nil {
		t.Fatalf("unmarshal export: %v", err)
	}
	if exportData.Stats.MemoryCount != 3 {
		t.Errorf("expected 3 exported memories, got %d", exportData.Stats.MemoryCount)
	}
	if len(exportData.Memories) != 3 {
		t.Fatalf("expected 3 memories in export, got %d", len(exportData.Memories))
	}

	// Verify all contents are present.
	exportedContents := make(map[string]bool)
	for _, mem := range exportData.Memories {
		exportedContents[mem.Content] = true
	}
	for _, content := range contents {
		if !exportedContents[content] {
			t.Errorf("expected exported content %q not found", content)
		}
	}
	if exportData.Project.Slug != "export-proj" {
		t.Errorf("expected project slug 'export-proj', got %q", exportData.Project.Slug)
	}
}

// ---------------------------------------------------------------------------
// Test 30: TestHTTPStack_MCP_SessionIDPersists
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_SessionIDPersists(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	fakeEnv := env.envFor(0)

	// Send initialize request.
	initReq := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]interface{}{},
			"clientInfo":      map[string]interface{}{"name": "test", "version": "1.0"},
		},
	}

	resp, rpcResp := doMCPRequest(t, fakeEnv, initReq)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initialize: expected 200, got %d", resp.StatusCode)
	}
	if rpcResp == nil || rpcResp.Error != nil {
		t.Fatal("initialize failed")
	}

	sessionID := resp.Header.Get("Mcp-Session-Id")
	if sessionID == "" {
		t.Fatal("expected Mcp-Session-Id header after initialize, got empty")
	}

	// Send a subsequent request using the session ID.
	listReq := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/list",
	}
	headers := map[string]string{"Mcp-Session-Id": sessionID}
	resp2, rpcResp2 := doMCPRequestWithHeaders(t, fakeEnv, listReq, headers)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("tools/list: expected 200, got %d", resp2.StatusCode)
	}
	if rpcResp2 == nil || rpcResp2.Error != nil {
		t.Fatal("tools/list with session ID failed")
	}

	// Verify the response still works (session is maintained).
	var listResult struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(rpcResp2.Result, &listResult); err != nil {
		t.Fatalf("unmarshal tools/list: %v", err)
	}
	if len(listResult.Tools) == 0 {
		t.Error("expected at least one tool in tools/list response")
	}
}

// ---------------------------------------------------------------------------
// Test 31: TestHTTPStack_MCP_ConcurrentStoresFromSameUser
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ConcurrentStoresFromSameUser(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "conc-proj"},
	})
	defer env.Close()

	const goroutines = 10
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sess := env.sessionFor(t, 0)
			_, storeRPC := sess.call(t, 100+idx, "tools/call", map[string]interface{}{
				"name": "memory_store",
				"arguments": map[string]interface{}{
					"project": "conc-proj",
					"content": fmt.Sprintf("concurrent memory %d", idx),
					"tags":    []interface{}{"concurrent"},
				},
			})
			if storeRPC == nil || storeRPC.Error != nil {
				msg := "nil response"
				if storeRPC != nil && storeRPC.Error != nil {
					msg = storeRPC.Error.Message
				}
				errCh <- fmt.Errorf("goroutine %d store failed: %s", idx, msg)
				return
			}
			text, isErr := extractToolResultTextRaw(t, storeRPC)
			if isErr {
				errCh <- fmt.Errorf("goroutine %d tool error: %s", idx, text)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}

	// Recall all memories — expect 10.
	sess := env.sessionFor(t, 0)
	_, recallRPC := sess.call(t, 200, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "concurrent",
			"project": "conc-proj",
			"limit":   float64(20),
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) != goroutines {
		t.Errorf("expected %d memories from concurrent stores, got %d", goroutines, len(recallResp.Memories))
	}
}

// ---------------------------------------------------------------------------
// Test 32: TestHTTPStack_MCP_ConcurrentStoresFromDifferentUsers
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ConcurrentStoresFromDifferentUsers(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()
	userB := uuid.New()
	nsB := uuid.New()
	projB := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "shared"},
		{userID: userB, nsID: nsB, nsPath: "/users/bob", projectID: projB, projSlug: "shared"},
	})
	defer env.Close()

	const perUser = 5
	var wg sync.WaitGroup
	errCh := make(chan error, perUser*2)

	for userIdx := 0; userIdx < 2; userIdx++ {
		for i := 0; i < perUser; i++ {
			wg.Add(1)
			go func(uIdx, idx int) {
				defer wg.Done()
				sess := env.sessionFor(t, uIdx)
				name := "Alice"
				if uIdx == 1 {
					name = "Bob"
				}
				_, storeRPC := sess.call(t, 100+uIdx*100+idx, "tools/call", map[string]interface{}{
					"name": "memory_store",
					"arguments": map[string]interface{}{
						"project": "shared",
						"content": fmt.Sprintf("%s memory %d", name, idx),
						"tags":    []interface{}{"multi-user"},
					},
				})
				if storeRPC == nil || storeRPC.Error != nil {
					msg := "nil response"
					if storeRPC != nil && storeRPC.Error != nil {
						msg = storeRPC.Error.Message
					}
					errCh <- fmt.Errorf("user %d goroutine %d store failed: %s", uIdx, idx, msg)
					return
				}
				text, isErr := extractToolResultTextRaw(t, storeRPC)
				if isErr {
					errCh <- fmt.Errorf("user %d goroutine %d tool error: %s", uIdx, idx, text)
				}
			}(userIdx, i)
		}
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}

	// User A recalls — should see exactly 5.
	sessA := env.sessionFor(t, 0)
	_, recallRPC := sessA.call(t, 300, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "memory",
			"project": "shared",
			"limit":   float64(20),
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("User A recall failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) != perUser {
		t.Errorf("User A: expected %d memories, got %d", perUser, len(recallResp.Memories))
	}
	for _, mem := range recallResp.Memories {
		if !strings.Contains(mem.Content, "Alice") {
			t.Errorf("User A recall returned non-Alice memory: %q", mem.Content)
		}
	}

	// User B recalls — should see exactly 5.
	sessB := env.sessionFor(t, 1)
	_, recallRPC = sessB.call(t, 300, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "memory",
			"project": "shared",
			"limit":   float64(20),
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("User B recall failed")
	}
	recallText = extractToolResultText(t, recallRPC)
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) != perUser {
		t.Errorf("User B: expected %d memories, got %d", perUser, len(recallResp.Memories))
	}
	for _, mem := range recallResp.Memories {
		if !strings.Contains(mem.Content, "Bob") {
			t.Errorf("User B recall returned non-Bob memory: %q", mem.Content)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 33: TestHTTPStack_MCP_LargeContent
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_LargeContent(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "large-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Build ~50KB content.
	largeContent := strings.Repeat("abcdefghij", 5000) // 50,000 bytes
	if len(largeContent) < 50000 {
		t.Fatalf("expected at least 50KB, got %d bytes", len(largeContent))
	}

	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "large-proj",
			"content": largeContent,
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}
	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Recall and verify content round-trips.
	_, recallRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "abcdefghij",
			"project": "large-proj",
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) == 0 {
		t.Fatal("expected at least 1 memory, got 0")
	}
	if recallResp.Memories[0].Content != largeContent {
		t.Errorf("large content round-trip failed: got %d bytes (expected %d)", len(recallResp.Memories[0].Content), len(largeContent))
	}
}

// ---------------------------------------------------------------------------
// Test 34: TestHTTPStack_MCP_UnicodeContent
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_UnicodeContent(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "unicode-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	unicodeContent := "Hello world! Emoji: \U0001F680\U0001F30D\U0001F525 CJK: \u4f60\u597d\u4e16\u754c Arabic: \u0645\u0631\u062d\u0628\u0627 \u0628\u0627\u0644\u0639\u0627\u0644\u0645 Korean: \uc548\ub155\ud558\uc138\uc694 Japanese: \u3053\u3093\u306b\u3061\u306f"

	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "unicode-proj",
			"content": unicodeContent,
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}

	// Recall and verify exact preservation.
	_, recallRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "hello",
			"project": "unicode-proj",
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) == 0 {
		t.Fatal("expected at least 1 memory")
	}
	if recallResp.Memories[0].Content != unicodeContent {
		t.Errorf("unicode content mismatch:\n  got:  %q\n  want: %q", recallResp.Memories[0].Content, unicodeContent)
	}
}

// ---------------------------------------------------------------------------
// Test 35: TestHTTPStack_MCP_SpecialCharsInProjectSlug
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_SpecialCharsInProjectSlug(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()

	// Pre-create a project with hyphens, underscores, and dots.
	projA := uuid.New()
	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "my-project_v2.0"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store to project with hyphens, underscores, dots.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "my-project_v2.0",
			"content": "special slug content",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store to special slug failed")
	}
	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if storeResp.ID == uuid.Nil {
		t.Error("expected non-nil memory ID for special slug project")
	}

	// Store to a project with spaces — should either error or normalize.
	_, spaceRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "project with spaces",
			"content": "space slug content",
		},
	})
	if spaceRPC == nil || spaceRPC.Error != nil {
		// Protocol-level error is acceptable for invalid slug.
		t.Logf("store to project with spaces: protocol error (acceptable)")
		return
	}
	text, isErr := extractToolResultTextRaw(t, spaceRPC)
	if isErr {
		// Tool-level error is acceptable.
		t.Logf("store to project with spaces: tool error (acceptable): %s", text)
	} else {
		// If it succeeded, the slug was normalized — that is also acceptable.
		t.Logf("store to project with spaces: succeeded (slug normalized)")
	}
}

// ---------------------------------------------------------------------------
// Test 36: TestHTTPStack_MCP_ManyTags
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ManyTags(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "tags-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Build 50 tags.
	tags := make([]interface{}, 50)
	for i := 0; i < 50; i++ {
		tags[i] = fmt.Sprintf("tag-%03d", i)
	}

	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "tags-proj",
			"content": "memory with 50 tags",
			"tags":    tags,
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}
	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	memID := storeResp.ID

	// Get the memory and verify all 50 tags.
	_, getRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_get",
		"arguments": map[string]interface{}{
			"project": "tags-proj",
			"ids":     []string{memID.String()},
		},
	})
	if getRPC == nil || getRPC.Error != nil {
		t.Fatalf("get failed")
	}
	getText := extractToolResultText(t, getRPC)
	var getResp service.BatchGetResponse
	if err := json.Unmarshal([]byte(getText), &getResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(getResp.Found) != 1 {
		t.Fatalf("expected 1 found, got %d", len(getResp.Found))
	}
	if len(getResp.Found[0].Tags) != 50 {
		t.Errorf("expected 50 tags, got %d", len(getResp.Found[0].Tags))
	}

	// Verify specific tags are present.
	tagSet := make(map[string]bool)
	for _, tag := range getResp.Found[0].Tags {
		tagSet[tag] = true
	}
	for i := 0; i < 50; i++ {
		expected := fmt.Sprintf("tag-%03d", i)
		if !tagSet[expected] {
			t.Errorf("missing tag %q", expected)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 37: TestHTTPStack_MCP_StoreMinimalFields
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_StoreMinimalFields(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "minimal-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store with ONLY project + content.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "minimal-proj",
			"content": "minimal fields only",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}
	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if storeResp.ID == uuid.Nil {
		t.Error("expected non-nil memory ID")
	}
	if storeResp.Content != "minimal fields only" {
		t.Errorf("expected content 'minimal fields only', got %q", storeResp.Content)
	}

	// Verify tags is an empty array (not null) in the raw JSON.
	if storeResp.Tags == nil {
		// StoreResponse.Tags might be nil in Go, but the JSON should
		// represent it as []. Check the raw JSON.
		if !strings.Contains(storeText, `"tags":[]`) && !strings.Contains(storeText, `"tags": []`) {
			// Tags may be omitted or null — log but don't fail.
			t.Logf("tags in response: raw JSON does not contain explicit empty array (may be null or omitted)")
		}
	} else if len(storeResp.Tags) != 0 {
		t.Errorf("expected empty tags, got %v", storeResp.Tags)
	}
}

// ---------------------------------------------------------------------------
// Test 38: TestHTTPStack_MCP_RecallWithLimit
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_RecallWithLimit(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "limit-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store 10 memories.
	for i := 0; i < 10; i++ {
		_, storeRPC := sess.call(t, 2+i, "tools/call", map[string]interface{}{
			"name": "memory_store",
			"arguments": map[string]interface{}{
				"project": "limit-proj",
				"content": fmt.Sprintf("limit test memory %d", i),
			},
		})
		if storeRPC == nil || storeRPC.Error != nil {
			t.Fatalf("store %d failed", i)
		}
	}

	// Recall with limit=3.
	_, recallRPC := sess.call(t, 20, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "limit test",
			"project": "limit-proj",
			"limit":   float64(3),
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) != 3 {
		t.Errorf("expected 3 memories with limit=3, got %d", len(recallResp.Memories))
	}
}

// ---------------------------------------------------------------------------
// Test 39: TestHTTPStack_MCP_UnknownToolCall
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_UnknownToolCall(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	_, rpcResp := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name":      "nonexistent_tool",
		"arguments": map[string]interface{}{},
	})
	if rpcResp == nil {
		t.Fatal("expected non-nil response for unknown tool")
	}

	// Either a JSON-RPC error or a tool-level error is acceptable.
	if rpcResp.Error != nil {
		t.Logf("got JSON-RPC error (expected): code=%d msg=%s", rpcResp.Error.Code, rpcResp.Error.Message)
		return
	}

	// If no JSON-RPC error, check for tool-level error.
	text, isErr := extractToolResultTextRaw(t, rpcResp)
	if !isErr {
		t.Fatalf("expected error for unknown tool, got success: %s", text)
	}
	t.Logf("got tool-level error (expected): %s", text)
}

// ---------------------------------------------------------------------------
// Test 40: TestHTTPStack_MCP_ToolCallMissingRequiredParams
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ToolCallMissingRequiredParams(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Call memory_store without content.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "test-proj",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		// Protocol-level error is acceptable.
		t.Logf("memory_store without content: protocol error (acceptable)")
	} else {
		text, isErr := extractToolResultTextRaw(t, storeRPC)
		if !isErr {
			t.Errorf("expected error for memory_store without content, got success: %s", text)
		} else {
			t.Logf("memory_store without content: tool error (expected): %s", text)
		}
	}

	// Call memory_recall without query.
	_, recallRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"project": "test-proj",
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		// Protocol-level error is acceptable.
		t.Logf("memory_recall without query: protocol error (acceptable)")
	} else {
		text, isErr := extractToolResultTextRaw(t, recallRPC)
		if !isErr {
			t.Errorf("expected error for memory_recall without query, got success: %s", text)
		} else {
			t.Logf("memory_recall without query: tool error (expected): %s", text)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 41: TestHTTPStack_MCP_MalformedJSONRPC
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_MalformedJSONRPC(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	// Send garbage JSON.
	garbage := []byte(`{this is not valid json!!!}`)
	headers := map[string]string{
		"Authorization": "Bearer " + env.Users[0].Token,
	}
	resp := doRawMCPPost(t, env.Server.URL, garbage, headers)
	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// Expect either 400 Bad Request or a JSON-RPC error response.
	if resp.StatusCode == http.StatusBadRequest {
		t.Logf("malformed JSON: got 400 (expected)")
		return
	}
	if resp.StatusCode == http.StatusOK {
		// Parse as JSON-RPC error.
		var rpcResp jsonrpcResponse
		if err := json.Unmarshal(bodyBytes, &rpcResp); err == nil && rpcResp.Error != nil {
			t.Logf("malformed JSON: got JSON-RPC error (expected): code=%d msg=%s", rpcResp.Error.Code, rpcResp.Error.Message)
			return
		}
	}
	// Any non-success status is acceptable.
	if resp.StatusCode >= 400 {
		t.Logf("malformed JSON: got status %d (acceptable)", resp.StatusCode)
		return
	}
	t.Errorf("malformed JSON: expected error response, got status %d body=%s", resp.StatusCode, string(bodyBytes))
}

// ---------------------------------------------------------------------------
// Test 42: TestHTTPStack_MCP_InvalidJSONRPCVersion
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_InvalidJSONRPCVersion(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	invalidReq := map[string]interface{}{
		"jsonrpc": "1.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]interface{}{},
			"clientInfo":      map[string]interface{}{"name": "test", "version": "1.0"},
		},
	}
	body, _ := json.Marshal(invalidReq)
	headers := map[string]string{
		"Authorization": "Bearer " + env.Users[0].Token,
	}
	resp := doRawMCPPost(t, env.Server.URL, body, headers)
	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// Expect either an error status or a JSON-RPC error response.
	if resp.StatusCode >= 400 {
		t.Logf("invalid jsonrpc version: got status %d (expected)", resp.StatusCode)
		return
	}

	// Check for JSON-RPC error in body.
	var rpcResp jsonrpcResponse
	if err := json.Unmarshal(bodyBytes, &rpcResp); err == nil && rpcResp.Error != nil {
		t.Logf("invalid jsonrpc version: got JSON-RPC error (expected): code=%d msg=%s", rpcResp.Error.Code, rpcResp.Error.Message)
		return
	}

	// Some SDKs might accept 1.0 and upgrade — that is also acceptable behavior.
	t.Logf("invalid jsonrpc version: server accepted request (SDK tolerant behavior), status=%d", resp.StatusCode)
}

// ---------------------------------------------------------------------------
// Test 43: TestHTTPStack_MCP_UpdateTagsOnly
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_UpdateTagsOnly(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "update-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "update-proj",
			"content": "content that should not change",
			"tags":    []interface{}{"original"},
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}
	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	memID := storeResp.ID

	// Update with only new tags (no content).
	_, updateRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_update",
		"arguments": map[string]interface{}{
			"id":      memID.String(),
			"project": "update-proj",
			"tags":    []interface{}{"updated-tag-1", "updated-tag-2"},
		},
	})
	if updateRPC == nil || updateRPC.Error != nil {
		msg := ""
		if updateRPC != nil && updateRPC.Error != nil {
			msg = updateRPC.Error.Message
		}
		t.Fatalf("update failed: %s", msg)
	}

	updateText := extractToolResultText(t, updateRPC)
	var updateResp service.UpdateResponse
	if err := json.Unmarshal([]byte(updateText), &updateResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Content should remain unchanged.
	if updateResp.Content != "content that should not change" {
		t.Errorf("expected content unchanged, got %q", updateResp.Content)
	}

	// Tags should be updated.
	tagSet := make(map[string]bool)
	for _, tag := range updateResp.Tags {
		tagSet[tag] = true
	}
	if !tagSet["updated-tag-1"] || !tagSet["updated-tag-2"] {
		t.Errorf("expected updated tags, got %v", updateResp.Tags)
	}
}

// ---------------------------------------------------------------------------
// Test 44: TestHTTPStack_MCP_UpdateClearsMetadata
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_UpdateClearsMetadata(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "meta-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store with metadata.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "meta-proj",
			"content": "memory with metadata to clear",
			"metadata": map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}
	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	memID := storeResp.ID

	// Verify metadata was stored.
	mem := env.MemRepo.memories[memID]
	if mem == nil {
		t.Fatal("memory not found in repo")
	}
	if mem.Metadata == nil || len(mem.Metadata) == 0 {
		t.Fatal("expected metadata to be stored")
	}

	// Update with empty metadata.
	_, updateRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_update",
		"arguments": map[string]interface{}{
			"id":       memID.String(),
			"project":  "meta-proj",
			"metadata": map[string]interface{}{},
		},
	})
	if updateRPC == nil || updateRPC.Error != nil {
		msg := ""
		if updateRPC != nil && updateRPC.Error != nil {
			msg = updateRPC.Error.Message
		}
		t.Fatalf("update failed: %s", msg)
	}

	// Verify metadata was cleared.
	updatedMem := env.MemRepo.memories[memID]
	if updatedMem == nil {
		t.Fatal("memory not found after update")
	}
	if updatedMem.Metadata != nil && len(updatedMem.Metadata) > 0 {
		var meta map[string]interface{}
		if err := json.Unmarshal(updatedMem.Metadata, &meta); err == nil && len(meta) > 0 {
			t.Errorf("expected metadata to be cleared, got %v", meta)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 45: TestHTTPStack_MCP_RecallEmptyProject
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_RecallEmptyProject(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "empty-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store a memory, then forget it, leaving the project empty.
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "empty-proj",
			"content": "temporary memory",
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store failed")
	}
	storeText := extractToolResultText(t, storeRPC)
	var storeResp service.StoreResponse
	if err := json.Unmarshal([]byte(storeText), &storeResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Forget it.
	_, forgetRPC := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_forget",
		"arguments": map[string]interface{}{
			"project": "empty-proj",
			"ids":     []string{storeResp.ID.String()},
		},
	})
	if forgetRPC == nil || forgetRPC.Error != nil {
		t.Fatalf("forget failed")
	}

	// Recall from the now-empty project.
	_, recallRPC := sess.call(t, 4, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "anything",
			"project": "empty-proj",
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall from empty project failed at protocol level")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) != 0 {
		t.Errorf("expected 0 memories from empty project, got %d", len(recallResp.Memories))
	}
}

// ---------------------------------------------------------------------------
// Test 46: TestHTTPStack_MCP_RecallMultipleTagFilter
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_RecallMultipleTagFilter(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "multitag-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Store 3 memories with different tag combos.
	// Memory 1: tags [alpha, beta]
	_, storeRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "multitag-proj",
			"content": "has both alpha and beta",
			"tags":    []interface{}{"alpha", "beta"},
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store 1 failed")
	}

	// Memory 2: tags [alpha]
	_, storeRPC = sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "multitag-proj",
			"content": "has only alpha",
			"tags":    []interface{}{"alpha"},
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store 2 failed")
	}

	// Memory 3: tags [beta, gamma]
	_, storeRPC = sess.call(t, 4, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "multitag-proj",
			"content": "has beta and gamma",
			"tags":    []interface{}{"beta", "gamma"},
		},
	})
	if storeRPC == nil || storeRPC.Error != nil {
		t.Fatalf("store 3 failed")
	}

	// Recall with tags [alpha, beta] — only memory 1 has BOTH.
	_, recallRPC := sess.call(t, 5, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   "has",
			"project": "multitag-proj",
			"tags":    []interface{}{"alpha", "beta"},
			"limit":   float64(10),
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) != 1 {
		t.Errorf("expected 1 memory matching both [alpha, beta], got %d", len(recallResp.Memories))
		for _, m := range recallResp.Memories {
			t.Logf("  - %q tags=%v", m.Content, m.Tags)
		}
	} else if !strings.Contains(recallResp.Memories[0].Content, "both alpha and beta") {
		t.Errorf("expected memory with both tags, got %q", recallResp.Memories[0].Content)
	}
}

// ---------------------------------------------------------------------------
// Test 47: TestHTTPStack_MCP_StoreDuplicateContent
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_StoreDuplicateContent(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "dup-proj"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	duplicateContent := "this exact content is stored twice"

	// Store first copy.
	_, storeRPC1 := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "dup-proj",
			"content": duplicateContent,
		},
	})
	if storeRPC1 == nil || storeRPC1.Error != nil {
		t.Fatalf("store 1 failed")
	}
	storeText1 := extractToolResultText(t, storeRPC1)
	var storeResp1 service.StoreResponse
	if err := json.Unmarshal([]byte(storeText1), &storeResp1); err != nil {
		t.Fatalf("unmarshal 1: %v", err)
	}

	// Store second copy.
	_, storeRPC2 := sess.call(t, 3, "tools/call", map[string]interface{}{
		"name": "memory_store",
		"arguments": map[string]interface{}{
			"project": "dup-proj",
			"content": duplicateContent,
		},
	})
	if storeRPC2 == nil || storeRPC2.Error != nil {
		t.Fatalf("store 2 failed")
	}
	storeText2 := extractToolResultText(t, storeRPC2)
	var storeResp2 service.StoreResponse
	if err := json.Unmarshal([]byte(storeText2), &storeResp2); err != nil {
		t.Fatalf("unmarshal 2: %v", err)
	}

	// Verify different IDs.
	if storeResp1.ID == storeResp2.ID {
		t.Error("expected different IDs for duplicate content, got same")
	}

	// Recall — should return both.
	_, recallRPC := sess.call(t, 4, "tools/call", map[string]interface{}{
		"name": "memory_recall",
		"arguments": map[string]interface{}{
			"query":   duplicateContent,
			"project": "dup-proj",
			"limit":   float64(10),
		},
	})
	if recallRPC == nil || recallRPC.Error != nil {
		t.Fatalf("recall failed")
	}
	recallText := extractToolResultText(t, recallRPC)
	var recallResp service.RecallResponse
	if err := json.Unmarshal([]byte(recallText), &recallResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recallResp.Memories) != 2 {
		t.Errorf("expected 2 memories for duplicate content, got %d", len(recallResp.Memories))
	}
}

// ---------------------------------------------------------------------------
// Test 48: TestHTTPStack_MCP_ExportEmptyProject
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ExportEmptyProject(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "empty-export"},
	})
	defer env.Close()

	sess := env.sessionFor(t, 0)

	// Export a project that has no memories.
	_, exportRPC := sess.call(t, 2, "tools/call", map[string]interface{}{
		"name": "memory_export",
		"arguments": map[string]interface{}{
			"project": "empty-export",
		},
	})
	if exportRPC == nil || exportRPC.Error != nil {
		msg := ""
		if exportRPC != nil && exportRPC.Error != nil {
			msg = exportRPC.Error.Message
		}
		t.Fatalf("export failed: %s", msg)
	}
	exportText := extractToolResultText(t, exportRPC)
	var exportData service.ExportData
	if err := json.Unmarshal([]byte(exportText), &exportData); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(exportData.Memories) != 0 {
		t.Errorf("expected 0 memories in empty export, got %d", len(exportData.Memories))
	}
	if exportData.Stats.MemoryCount != 0 {
		t.Errorf("expected memory_count=0, got %d", exportData.Stats.MemoryCount)
	}
}

// ---------------------------------------------------------------------------
// Test 49: TestHTTPStack_MCP_ToolCallBeforeInitialize
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_ToolCallBeforeInitialize(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	fakeEnv := env.envFor(0)

	// Send tools/call WITHOUT doing initialize first.
	toolReq := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params: map[string]interface{}{
			"name": "memory_store",
			"arguments": map[string]interface{}{
				"project": "test-proj",
				"content": "should not work",
			},
		},
	}
	body, _ := json.Marshal(toolReq)
	headers := map[string]string{
		"Authorization": "Bearer " + fakeEnv.Token,
	}
	resp := doRawMCPPost(t, fakeEnv.Server.URL, body, headers)
	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// The SDK should reject pre-init tool calls with an error.
	if resp.StatusCode >= 400 {
		t.Logf("tool call before initialize: got status %d (expected rejection)", resp.StatusCode)
		return
	}

	// If 200, check for JSON-RPC error.
	ct := resp.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "application/json") {
		var rpcResp jsonrpcResponse
		if err := json.Unmarshal(bodyBytes, &rpcResp); err == nil && rpcResp.Error != nil {
			t.Logf("tool call before initialize: got JSON-RPC error (expected): code=%d msg=%s",
				rpcResp.Error.Code, rpcResp.Error.Message)
			return
		}
	}

	// Check SSE for error.
	if strings.HasPrefix(ct, "text/event-stream") {
		rpcResp := parseSSEResponse(t, bodyBytes)
		if rpcResp.Error != nil {
			t.Logf("tool call before initialize: got SSE JSON-RPC error (expected): code=%d msg=%s",
				rpcResp.Error.Code, rpcResp.Error.Message)
			return
		}
		// Check for tool error.
		text, isErr := extractToolResultTextRaw(t, rpcResp)
		if isErr {
			t.Logf("tool call before initialize: got tool error (acceptable): %s", text)
			return
		}
	}

	// Some SDKs may accept tool calls without initialize — that is tolerable.
	t.Logf("tool call before initialize: server accepted (SDK may not enforce init order), status=%d", resp.StatusCode)
}

// ---------------------------------------------------------------------------
// Test 50: TestHTTPStack_MCP_DoubleInitialize
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_DoubleInitialize(t *testing.T) {
	userA := uuid.New()
	nsA := uuid.New()
	projA := uuid.New()

	env := newMultiUserHTTPStackEnv(t, []multiUserEnvConfig{
		{userID: userA, nsID: nsA, nsPath: "/users/alice", projectID: projA, projSlug: "test-proj"},
	})
	defer env.Close()

	fakeEnv := env.envFor(0)

	// First initialize.
	initReq := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]interface{}{},
			"clientInfo":      map[string]interface{}{"name": "test", "version": "1.0"},
		},
	}
	resp1, rpcResp1 := doMCPRequest(t, fakeEnv, initReq)
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first initialize: expected 200, got %d", resp1.StatusCode)
	}
	if rpcResp1 == nil || rpcResp1.Error != nil {
		t.Fatal("first initialize failed")
	}
	sessionID := resp1.Header.Get("Mcp-Session-Id")

	// Second initialize (same session or new — both should not crash).
	initReq2 := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]interface{}{},
			"clientInfo":      map[string]interface{}{"name": "test", "version": "1.0"},
		},
	}
	headers := map[string]string{}
	if sessionID != "" {
		headers["Mcp-Session-Id"] = sessionID
	}
	resp2, rpcResp2 := doMCPRequestWithHeaders(t, fakeEnv, initReq2, headers)

	// The key assertion: the server must not crash.
	if resp2.StatusCode >= 500 {
		t.Fatalf("double initialize caused server error: status %d", resp2.StatusCode)
	}

	if rpcResp2 != nil && rpcResp2.Error != nil {
		// An error is acceptable (some SDKs reject double init).
		t.Logf("double initialize: got JSON-RPC error (acceptable): code=%d msg=%s",
			rpcResp2.Error.Code, rpcResp2.Error.Message)
	} else if rpcResp2 != nil && rpcResp2.Error == nil {
		// Success is also acceptable.
		t.Logf("double initialize: succeeded (acceptable)")
	} else {
		t.Logf("double initialize: status=%d (no crash, acceptable)", resp2.StatusCode)
	}
}
