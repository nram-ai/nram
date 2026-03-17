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

// ---------------------------------------------------------------------------
// Test JWT secret and helper
// ---------------------------------------------------------------------------

var httpStackTestSecret = []byte("http-stack-test-secret-32bytes!!")

func generateHTTPStackJWT(t *testing.T, userID uuid.UUID, host string) string {
	t.Helper()
	// Generate a JWT without audience so it passes the audience check
	// (audience check is skipped when aud claim is empty).
	token, err := auth.GenerateJWT(userID, auth.RoleMember, httpStackTestSecret, 1*time.Hour)
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

	authMw := auth.NewAuthMiddleware(&testAPIKeyValidator{}, httpStackTestSecret)

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
// Test 5: TestHTTPStack_MCP_InvalidOrigin_Returns403
// ---------------------------------------------------------------------------

func TestHTTPStack_MCP_InvalidOrigin_Returns403(t *testing.T) {
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

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d (body: %s)", resp.StatusCode, string(bodyBytes))
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
