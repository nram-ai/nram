package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/auth"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/service"
)

// --- nil-detection helpers ---

// nullFieldPattern matches JSON patterns like "field":null (with optional whitespace).
var nullFieldPattern = regexp.MustCompile(`"[^"]+"\s*:\s*null`)

// assertNoNullCollections scans the raw JSON body string for any field whose value
// is literally `null`. Fields that legitimately carry null (e.g. nullable scalars
// like "source":null, "last_accessed":null) are excluded via an allowlist.
func assertNoNullCollections(t *testing.T, body string) {
	t.Helper()

	// Scalar fields that are legitimately nullable (pointers to scalars).
	allowed := map[string]bool{
		"source":        true,
		"similarity":    true,
		"shared_from":   true, // nullable: only set when surfaced via cross-namespace sharing
		"last_accessed": true,
		"expires_at":    true,
		"superseded_by": true,
		"deleted_at":    true,
		"purge_after":   true,
		"embedding_dim": true,
		"details":       true, // error details may be omitempty but still null in edge cases
	}

	matches := nullFieldPattern.FindAllString(body, -1)
	for _, m := range matches {
		// Extract the field name from "field":null.
		idx := strings.Index(m, "\":")
		if idx < 0 {
			continue
		}
		fieldName := m[1:idx]
		if allowed[fieldName] {
			continue
		}
		t.Errorf("found null for collection/object field %q in response body: %s", fieldName, m)
	}
}

// assertFieldIsArray unmarshals the body into a generic map and asserts that the
// given top-level field is a JSON array ([]interface{}) rather than nil.
func assertFieldIsArray(t *testing.T, body string, field string) {
	t.Helper()
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	val, ok := raw[field]
	if !ok {
		t.Fatalf("field %q not found in response", field)
	}
	if val == nil {
		t.Errorf("field %q is null, expected empty array []", field)
		return
	}
	if _, ok := val.([]interface{}); !ok {
		t.Errorf("field %q is type %T, expected []interface{} (JSON array)", field, val)
	}
}

// --- Test 1: Recall with no matches returns empty arrays, not null ---

func TestRecallHandler_EmptyResults_NoNull(t *testing.T) {
	projectID := uuid.New()

	svc := &mockRecallService{
		recallFn: func(_ context.Context, _ *service.RecallRequest) (*service.RecallResponse, error) {
			// Return a response where slices are nil (the default zero value).
			return &service.RecallResponse{
				Memories:  nil,
				LatencyMs: 1,
			}, nil
		},
	}

	router := newRecallRouter(NewRecallHandler(svc))
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	reqBody := map[string]interface{}{
		"query": "nonexistent topic",
	}

	w := doRecallRequest(router, "/v1/projects/"+projectID.String()+"/memories/recall", reqBody, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()

	// Verify no null leaks for collection fields.
	assertNoNullCollections(t, body)

	// Verify specific fields are arrays.
	assertFieldIsArray(t, body, "memories")

	// Verify graph sub-fields are arrays (entities/relationships moved under graph).
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	graphObj, ok := raw["graph"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected graph object in response, got %T", raw["graph"])
	}
	if ents := graphObj["entities"]; ents == nil {
		t.Error("graph.entities is null, expected empty array []")
	} else if _, ok := ents.([]interface{}); !ok {
		t.Errorf("graph.entities is type %T, expected []interface{}", ents)
	}
	if rels := graphObj["relationships"]; rels == nil {
		t.Error("graph.relationships is null, expected empty array []")
	} else if _, ok := rels.([]interface{}); !ok {
		t.Errorf("graph.relationships is type %T, expected []interface{}", rels)
	}

	// Also verify with strings.Contains as a belt-and-suspenders check.
	if strings.Contains(body, `"memories":null`) {
		t.Error("response contains \"memories\":null, expected empty array")
	}
}

// --- Test 2: Store with no tags returns empty array, not null ---

func TestStoreHandler_NilTags_ReturnsEmptyArray(t *testing.T) {
	svc := newTestStoreService(&mockProjectRepo{})

	router := newTestRouter(NewStoreHandler(svc, nil))
	projectID := uuid.New()
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	// Deliberately omit tags from the request body.
	reqBody := map[string]interface{}{
		"content": "A memory with no tags at all.",
		"source":  "test",
	}

	w := doStoreRequest(router, projectID.String(), reqBody, ac)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()

	// The tags field must be an empty array, not null.
	assertNoNullCollections(t, body)
	assertFieldIsArray(t, body, "tags")

	if strings.Contains(body, `"tags":null`) {
		t.Error("response contains \"tags\":null, expected empty array []")
	}
}

// --- Test 3: Update with nil tags returns empty array, not null ---

// nilTagsUpdateService is a mock that returns a response with nil Tags.
type nilTagsUpdateService struct{}

func (s *nilTagsUpdateService) Update(_ context.Context, req *service.UpdateRequest) (*service.UpdateResponse, error) {
	return &service.UpdateResponse{
		ID:              req.MemoryID,
		ProjectID:       req.ProjectID,
		Content:         "updated content",
		Tags:            nil, // deliberately nil
		PreviousContent: "old content",
		ReEmbedded:      false,
		LatencyMs:       1,
	}, nil
}

func TestUpdateHandler_NilTags_ReturnsEmptyArray(t *testing.T) {
	projectID := uuid.New()
	memoryID := uuid.New()

	svc := &nilTagsUpdateService{}
	router := newUpdateTestRouter(NewUpdateHandler(svc, nil))
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	reqBody := map[string]interface{}{
		"content": "updated content",
	}

	w := doUpdateRequest(router, projectID.String(), memoryID.String(), reqBody, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()

	assertNoNullCollections(t, body)
	assertFieldIsArray(t, body, "tags")

	if strings.Contains(body, `"tags":null`) {
		t.Error("response contains \"tags\":null, expected empty array []")
	}
}

// --- Test 4: Batch get with no found IDs returns empty arrays ---

func TestBatchGetHandler_NoMatches_ReturnsEmptyArrays(t *testing.T) {
	requestedID := uuid.New()

	svc := &mockBatchGetServicer{
		resp: &service.BatchGetResponse{
			Found:     nil, // deliberately nil
			NotFound:  []uuid.UUID{requestedID},
			LatencyMs: 2,
		},
	}

	router := newBatchGetRouter(NewBatchGetHandler(svc))
	projectID := uuid.New()

	reqBody := map[string]interface{}{
		"ids": []string{requestedID.String()},
	}

	w := doBatchGetRequest(router, projectID.String(), reqBody)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()

	assertNoNullCollections(t, body)
	assertFieldIsArray(t, body, "found")
	assertFieldIsArray(t, body, "not_found")

	if strings.Contains(body, `"found":null`) {
		t.Error("response contains \"found\":null, expected empty array []")
	}
}

// --- Test 5: List memories in empty namespace returns empty array ---

func TestListHandler_EmptyNamespace_ReturnsEmptyArray(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()

	proj := &model.Project{ID: projectID, Slug: "empty-project", NamespaceID: nsID}

	memRepo := &mockMemoryLister{
		listFn: func(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
			return nil, nil // deliberately nil slice
		},
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) {
			return 0, nil
		},
	}
	projRepo := &mockProjectGetter{project: proj}

	router := newListRouter(memRepo, projRepo)

	w := doListRequest(router, projectID.String(), "")

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()

	assertNoNullCollections(t, body)
	assertFieldIsArray(t, body, "data")

	if strings.Contains(body, `"data":null`) {
		t.Error("response contains \"data\":null, expected empty array []")
	}
}

// --- Test 6: Error response has no null details ---

func TestErrorResponse_NoNullDetails(t *testing.T) {
	// Trigger a 400 error by sending an invalid project_id.
	svc := &mockRecallService{}
	router := newRecallRouter(NewRecallHandler(svc))

	reqBody := map[string]interface{}{
		"query": "test",
	}

	w := doRecallRequest(router, "/v1/projects/not-a-uuid/memories/recall", reqBody, nil)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()

	// The error response should not contain "details":null — it should either
	// be omitted entirely (omitempty) or be an empty object/array.
	if strings.Contains(body, `"details":null`) {
		t.Error("error response contains \"details\":null, expected omission or empty value")
	}

	// Decode and verify structure.
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("failed to unmarshal error response: %v", err)
	}

	errObj, ok := raw["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected error object in response, got %T", raw["error"])
	}

	// "details" should either be absent or non-null.
	if details, exists := errObj["details"]; exists && details == nil {
		t.Error("error.details is null, expected omission or non-null value")
	}

	// Verify error code is present and correct.
	if code, ok := errObj["code"].(string); !ok || code != "bad_request" {
		t.Errorf("expected error code 'bad_request', got %v", errObj["code"])
	}
}

// --- Test 7: Store response with nil metadata gets sanitized ---

func TestStoreHandler_NilMetadata_ReturnsEmptyObject(t *testing.T) {
	// Use a custom memory repo that returns a memory with nil Metadata.
	memRepo := &mockMemoryRepo{
		createFn: func(_ context.Context, mem *model.Memory) error {
			// Simulate the repo leaving Metadata nil.
			mem.Metadata = nil
			return nil
		},
	}

	svc := service.NewStoreService(
		memRepo,
		&mockProjectRepo{},
		&mockNamespaceRepo{},
		&mockIngestionLogRepo{},
		&mockTokenUsageRepo{},
		&mockEnrichmentQueueRepo{},
		&mockVectorStore{},
		func() provider.EmbeddingProvider { return nil },
	)

	router := newTestRouter(NewStoreHandler(svc, nil))
	projectID := uuid.New()
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	reqBody := map[string]interface{}{
		"content": "A memory with no metadata.",
	}

	w := doStoreRequest(router, projectID.String(), reqBody, ac)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()
	assertNoNullCollections(t, body)
}

// --- Test 8: Recall with nil Tags in RecallResult ---

func TestRecallHandler_NilTagsInResult_ReturnsEmptyArray(t *testing.T) {
	projectID := uuid.New()
	memoryID := uuid.New()

	svc := &mockRecallService{
		recallFn: func(_ context.Context, _ *service.RecallRequest) (*service.RecallResponse, error) {
			return &service.RecallResponse{
				Memories: []service.RecallResult{
					{
						ID:        memoryID,
						ProjectID: projectID,
						Content:   "result with nil tags",
						Tags:      nil, // deliberately nil
						Score:     0.9,
						CreatedAt: time.Now(),
					},
				},
				Graph: service.RecallGraph{
					Entities:      []service.RecallEntity{},
					Relationships: []service.RecallRelationship{},
				},
				LatencyMs: 1,
			}, nil
		},
	}

	router := newRecallRouter(NewRecallHandler(svc))
	ac := &auth.AuthContext{UserID: uuid.New(), Role: "user"}

	reqBody := map[string]interface{}{
		"query": "test query",
	}

	w := doRecallRequest(router, "/v1/projects/"+projectID.String()+"/memories/recall", reqBody, ac)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()
	assertNoNullCollections(t, body)

	// Parse and check the tags field inside the first memory.
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	memories, ok := raw["memories"].([]interface{})
	if !ok || len(memories) == 0 {
		t.Fatalf("expected non-empty memories array")
	}
	firstMem, ok := memories[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected memory object, got %T", memories[0])
	}
	tags := firstMem["tags"]
	if tags == nil {
		t.Error("tags in recalled memory is null, expected empty array []")
	}
	if arr, ok := tags.([]interface{}); ok {
		if arr == nil {
			t.Error("tags array is nil, expected []")
		}
	}
}

// --- Test 9: Batch get with nil Tags in MemoryDetail ---

func TestBatchGetHandler_NilTagsInFound_ReturnsEmptyArray(t *testing.T) {
	foundID := uuid.New()

	svc := &mockBatchGetServicer{
		resp: &service.BatchGetResponse{
			Found: []service.MemoryDetail{
				{
					ID:        foundID,
					Content:   "found memory with nil tags",
					Tags:      nil, // deliberately nil
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
			},
			NotFound:  []uuid.UUID{},
			LatencyMs: 1,
		},
	}

	router := newBatchGetRouter(NewBatchGetHandler(svc))
	projectID := uuid.New()

	reqBody := map[string]interface{}{
		"ids": []string{foundID.String()},
	}

	w := doBatchGetRequest(router, projectID.String(), reqBody)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()
	assertNoNullCollections(t, body)

	// Parse and check the tags field inside the found memory.
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	found, ok := raw["found"].([]interface{})
	if !ok || len(found) == 0 {
		t.Fatalf("expected non-empty found array")
	}
	firstMem, ok := found[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected memory object, got %T", found[0])
	}
	tags := firstMem["tags"]
	if tags == nil {
		t.Error("tags in found memory is null, expected empty array []")
	}
}

// --- Test 10: List with memories that have nil Tags ---

func TestListHandler_MemoryWithNilTags_ReturnsEmptyArray(t *testing.T) {
	nsID := uuid.New()
	projectID := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)

	proj := &model.Project{ID: projectID, Slug: "test", NamespaceID: nsID}

	memRepo := &mockMemoryLister{
		listFn: func(_ context.Context, _ uuid.UUID, _, _ int) ([]model.Memory, error) {
			return []model.Memory{
				{
					ID:          uuid.New(),
					NamespaceID: nsID,
					Content:     "memory with nil tags",
					Tags:        nil, // deliberately nil
					Confidence:  1.0,
					Importance:  0.5,
					Metadata:    json.RawMessage(`{}`),
					CreatedAt:   now,
					UpdatedAt:   now,
				},
			}, nil
		},
		countFn: func(_ context.Context, _ uuid.UUID) (int, error) {
			return 1, nil
		},
	}
	projRepo := &mockProjectGetter{project: proj}

	router := newListRouter(memRepo, projRepo)

	w := doListRequest(router, projectID.String(), "")

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	body := w.Body.String()
	assertNoNullCollections(t, body)

	// Parse and check the tags field inside the memory.
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	data, ok := raw["data"].([]interface{})
	if !ok || len(data) == 0 {
		t.Fatalf("expected non-empty data array")
	}
	firstMem, ok := data[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected memory object, got %T", data[0])
	}
	tags := firstMem["tags"]
	if tags == nil {
		t.Error("tags in listed memory is null, expected empty array []")
	}
}

// --- Test 11: Multiple error types produce no null fields ---

func TestErrorResponses_NoNullFields(t *testing.T) {
	tests := []struct {
		name string
		// buildRequest returns a recorder with the error response.
		buildRequest func() *httptest.ResponseRecorder
		wantCode     int
	}{
		{
			name: "recall_invalid_project_id",
			buildRequest: func() *httptest.ResponseRecorder {
				svc := &mockRecallService{}
				router := newRecallRouter(NewRecallHandler(svc))
				return doRecallRequest(router, "/v1/projects/bad-id/memories/recall",
					map[string]interface{}{"query": "test"}, nil)
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "recall_missing_query",
			buildRequest: func() *httptest.ResponseRecorder {
				svc := &mockRecallService{}
				router := newRecallRouter(NewRecallHandler(svc))
				projectID := uuid.New()
				return doRecallRequest(router, "/v1/projects/"+projectID.String()+"/memories/recall",
					map[string]interface{}{"limit": 10}, nil)
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "store_missing_content",
			buildRequest: func() *httptest.ResponseRecorder {
				svc := newTestStoreService(&mockProjectRepo{})
				r := chi.NewRouter()
				r.Post("/v1/projects/{project_id}/memories", NewStoreHandler(svc, nil))
				projectID := uuid.New()
				return doStoreRequest(r, projectID.String(),
					map[string]interface{}{"source": "test"}, nil)
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "batch_get_empty_ids",
			buildRequest: func() *httptest.ResponseRecorder {
				svc := &mockBatchGetServicer{}
				router := newBatchGetRouter(NewBatchGetHandler(svc))
				projectID := uuid.New()
				return doBatchGetRequest(router, projectID.String(),
					map[string]interface{}{"ids": []string{}})
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "store_invalid_json",
			buildRequest: func() *httptest.ResponseRecorder {
				svc := newTestStoreService(&mockProjectRepo{})
				r := chi.NewRouter()
				r.Post("/v1/projects/{project_id}/memories", NewStoreHandler(svc, nil))
				projectID := uuid.New()

				var buf bytes.Buffer
				buf.WriteString("{bad json")
				req := httptest.NewRequest(http.MethodPost,
					"/v1/projects/"+projectID.String()+"/memories", &buf)
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				r.ServeHTTP(w, req)
				return w
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := tt.buildRequest()

			if w.Code != tt.wantCode {
				t.Fatalf("expected status %d, got %d: %s", tt.wantCode, w.Code, w.Body.String())
			}

			body := w.Body.String()

			// Error responses should never have "details":null — it should be
			// omitted via omitempty.
			if strings.Contains(body, `"details":null`) {
				t.Error("error response contains \"details\":null, expected omission")
			}

			// Verify the error envelope structure is valid.
			var raw map[string]interface{}
			if err := json.Unmarshal([]byte(body), &raw); err != nil {
				t.Fatalf("failed to unmarshal error body: %v", err)
			}

			errObj, ok := raw["error"].(map[string]interface{})
			if !ok {
				t.Fatalf("expected error object, got %T", raw["error"])
			}

			if _, ok := errObj["code"].(string); !ok {
				t.Error("error.code is not a string")
			}
			if _, ok := errObj["message"].(string); !ok {
				t.Error("error.message is not a string")
			}
		})
	}
}
