package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// --- Extract-specific mock implementations ---

type mockLLMProvider struct {
	name    string
	models  []string
	resp    *provider.CompletionResponse
	err     error
	called  int
	lastReq *provider.CompletionRequest
}

func (m *mockLLMProvider) Complete(_ context.Context, req *provider.CompletionRequest) (*provider.CompletionResponse, error) {
	m.called++
	m.lastReq = req
	if m.err != nil {
		return nil, m.err
	}
	return m.resp, nil
}

func (m *mockLLMProvider) Name() string      { return m.name }
func (m *mockLLMProvider) Models() []string   { return m.models }

type mockEntityRepo struct {
	entities []*model.Entity
	err      error
}

func (m *mockEntityRepo) Upsert(_ context.Context, entity *model.Entity) error {
	if m.err != nil {
		return m.err
	}
	m.entities = append(m.entities, entity)
	return nil
}

type mockRelationshipRepo struct {
	relationships []*model.Relationship
	err           error
}

func (m *mockRelationshipRepo) Create(_ context.Context, rel *model.Relationship) error {
	if m.err != nil {
		return m.err
	}
	m.relationships = append(m.relationships, rel)
	return nil
}

type mockLineageRepo struct {
	lineages []*model.MemoryLineage
}

func (m *mockLineageRepo) Create(_ context.Context, lineage *model.MemoryLineage) error {
	m.lineages = append(m.lineages, lineage)
	return nil
}

// --- Test helper ---

type extractTestDeps struct {
	memories      *mockMemoryRepo
	ingestion     *mockIngestionLogRepo
	tokenUsage    *mockTokenUsageRepo
	entities      *mockEntityRepo
	relationships *mockRelationshipRepo
	lineage       *mockLineageRepo
	vectors       *mockVectorStore
}

func newExtractTestService(
	projects *mockProjectRepo,
	namespaces *mockNamespaceRepo,
	factProvider func() provider.LLMProvider,
	entityProvider func() provider.LLMProvider,
	embedProvider provider.EmbeddingProvider,
) (*ExtractionService, *extractTestDeps) {
	deps := &extractTestDeps{
		memories:      &mockMemoryRepo{},
		ingestion:     &mockIngestionLogRepo{},
		tokenUsage:    &mockTokenUsageRepo{},
		entities:      &mockEntityRepo{},
		relationships: &mockRelationshipRepo{},
		lineage:       &mockLineageRepo{},
		vectors:       &mockVectorStore{},
	}

	// Wrap test provider stubs in the UsageRecordingProvider middleware so
	// the middleware writes token_usage rows to deps.tokenUsage on every
	// Complete/Embed call — matches production wiring (registry wrap)
	// without spinning up a registry in unit tests.
	wrappedFact := provider.WrapLLMForTest(factProvider, deps.tokenUsage)
	wrappedEntity := provider.WrapLLMForTest(entityProvider, deps.tokenUsage)
	var embedFn func() provider.EmbeddingProvider
	if embedProvider != nil {
		ep := embedProvider
		embedFn = provider.WrapEmbeddingForTest(
			func() provider.EmbeddingProvider { return ep },
			deps.tokenUsage,
		)
	}

	svc := NewExtractionService(
		deps.memories,
		projects,
		namespaces,
		deps.ingestion,
		deps.entities,
		deps.relationships,
		deps.lineage,
		deps.vectors,
		wrappedFact,
		wrappedEntity,
		embedFn,
	)

	return svc, deps
}

func makeFactLLM(content string, promptTokens, completionTokens int) *mockLLMProvider {
	return &mockLLMProvider{
		name:   "fact-llm",
		models: []string{"fact-model"},
		resp: &provider.CompletionResponse{
			Content:      content,
			Model:        "fact-model",
			FinishReason: "stop",
			Usage: provider.TokenUsage{
				PromptTokens:     promptTokens,
				CompletionTokens: completionTokens,
				TotalTokens:      promptTokens + completionTokens,
			},
		},
	}
}

func makeEntityLLM(content string, promptTokens, completionTokens int) *mockLLMProvider {
	return &mockLLMProvider{
		name:   "entity-llm",
		models: []string{"entity-model"},
		resp: &provider.CompletionResponse{
			Content:      content,
			Model:        "entity-model",
			FinishReason: "stop",
			Usage: provider.TokenUsage{
				PromptTokens:     promptTokens,
				CompletionTokens: completionTokens,
				TotalTokens:      promptTokens + completionTokens,
			},
		},
	}
}

// --- Tests ---

func TestExtract_SuccessfulFactsAndEntities(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	factResp := `[{"fact":"Alice works at Acme Corp","confidence":0.95},{"fact":"Alice lives in Denver","confidence":0.9}]`
	entityResp := `{"entities":[{"name":"Alice","type":"person","properties":{"role":"engineer"}},{"name":"Acme Corp","type":"org","properties":{}}],"relationships":[{"source":"Alice","target":"Acme Corp","relation":"works_at","temporal":"current"}]}`

	factLLM := makeFactLLM(factResp, 100, 50)
	entityLLM := makeEntityLLM(entityResp, 120, 60)

	svc, deps := newExtractTestService(
		projects, namespaces,
		func() provider.LLMProvider { return factLLM },
		func() provider.LLMProvider { return entityLLM },
		nil,
	)

	resp, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Alice works at Acme Corp and lives in Denver.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 2 extracted memories (the facts).
	if len(resp.Memories) != 2 {
		t.Fatalf("expected 2 extracted memories, got %d", len(resp.Memories))
	}
	if resp.Memories[0].Content != "Alice works at Acme Corp" {
		t.Errorf("expected first fact content, got %q", resp.Memories[0].Content)
	}
	if resp.Memories[0].Confidence != 0.95 {
		t.Errorf("expected confidence 0.95, got %f", resp.Memories[0].Confidence)
	}
	if resp.Memories[1].Content != "Alice lives in Denver" {
		t.Errorf("expected second fact content, got %q", resp.Memories[1].Content)
	}

	// 1 raw + 2 fact children = 3 memories.
	if len(deps.memories.created) != 3 {
		t.Fatalf("expected 3 memories created, got %d", len(deps.memories.created))
	}

	// Entities.
	if resp.EntitiesCreated != 2 {
		t.Errorf("expected 2 entities created, got %d", resp.EntitiesCreated)
	}
	if len(deps.entities.entities) != 2 {
		t.Fatalf("expected 2 entity upserts, got %d", len(deps.entities.entities))
	}

	// Relationships.
	if resp.RelationshipsCreated != 1 {
		t.Errorf("expected 1 relationship created, got %d", resp.RelationshipsCreated)
	}
	if len(deps.relationships.relationships) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(deps.relationships.relationships))
	}
	rel := deps.relationships.relationships[0]
	if rel.Relation != "works_at" {
		t.Errorf("expected relation 'works_at', got %q", rel.Relation)
	}

	// Token totals.
	if resp.TokensUsed.Input != 220 {
		t.Errorf("expected 220 input tokens, got %d", resp.TokensUsed.Input)
	}
	if resp.TokensUsed.Output != 110 {
		t.Errorf("expected 110 output tokens, got %d", resp.TokensUsed.Output)
	}

	// Latency should be non-negative.
	if resp.LatencyMs < 0 {
		t.Errorf("expected non-negative latency, got %d", resp.LatencyMs)
	}
}

func TestExtract_FactsOnly_NoEntityProvider(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	factResp := `[{"fact":"Bob likes Go","confidence":0.85}]`
	factLLM := makeFactLLM(factResp, 50, 20)

	svc, deps := newExtractTestService(
		projects, namespaces,
		func() provider.LLMProvider { return factLLM },
		nil, // no entity provider
		nil,
	)

	resp, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Bob likes Go.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 extracted memory, got %d", len(resp.Memories))
	}
	if resp.EntitiesCreated != 0 {
		t.Errorf("expected 0 entities, got %d", resp.EntitiesCreated)
	}
	if resp.RelationshipsCreated != 0 {
		t.Errorf("expected 0 relationships, got %d", resp.RelationshipsCreated)
	}
	if len(deps.entities.entities) != 0 {
		t.Errorf("expected no entity upserts, got %d", len(deps.entities.entities))
	}
}

func TestExtract_EntitiesOnly_NoFactProvider(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	entityResp := `{"entities":[{"name":"Go","type":"tech","properties":{}}],"relationships":[]}`
	entityLLM := makeEntityLLM(entityResp, 80, 30)

	svc, deps := newExtractTestService(
		projects, namespaces,
		nil, // no fact provider
		func() provider.LLMProvider { return entityLLM },
		nil,
	)

	resp, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Go is a programming language.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 0 {
		t.Errorf("expected 0 extracted memories (no fact provider), got %d", len(resp.Memories))
	}
	if resp.EntitiesCreated != 1 {
		t.Errorf("expected 1 entity, got %d", resp.EntitiesCreated)
	}
	// 1 raw memory only.
	if len(deps.memories.created) != 1 {
		t.Errorf("expected 1 memory (raw only), got %d", len(deps.memories.created))
	}
}

func TestExtract_BothProvidersUnavailable(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	svc, deps := newExtractTestService(
		projects, namespaces,
		nil, // no fact provider
		nil, // no entity provider
		nil,
	)

	resp, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Nothing to extract.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Raw memory should still be stored.
	if len(deps.memories.created) != 1 {
		t.Fatalf("expected 1 memory (raw), got %d", len(deps.memories.created))
	}
	if len(resp.Memories) != 0 {
		t.Errorf("expected 0 extracted memories, got %d", len(resp.Memories))
	}
	if resp.EntitiesCreated != 0 {
		t.Errorf("expected 0 entities, got %d", resp.EntitiesCreated)
	}
	if resp.TokensUsed.Input != 0 {
		t.Errorf("expected 0 input tokens, got %d", resp.TokensUsed.Input)
	}
}

func TestExtract_FactLLMCleanJSON(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	// With JSON mode enabled, LLM returns clean JSON (no markdown fences).
	factResp := `[{"fact":"Clean JSON fact","confidence":0.8}]`
	factLLM := makeFactLLM(factResp, 40, 15)

	svc, _ := newExtractTestService(
		projects, namespaces,
		func() provider.LLMProvider { return factLLM },
		nil,
		nil,
	)

	resp, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Some text.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 memory from clean JSON response, got %d", len(resp.Memories))
	}
	if resp.Memories[0].Content != "Clean JSON fact" {
		t.Errorf("expected clean JSON fact content, got %q", resp.Memories[0].Content)
	}
}

func TestExtract_EntityExtractionFailsButFactsSucceed(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	factResp := `[{"fact":"Partial success fact","confidence":0.7}]`
	factLLM := makeFactLLM(factResp, 30, 10)

	entityLLM := &mockLLMProvider{
		name:   "entity-llm",
		models: []string{"entity-model"},
		err:    fmt.Errorf("entity LLM error"),
	}

	svc, deps := newExtractTestService(
		projects, namespaces,
		func() provider.LLMProvider { return factLLM },
		func() provider.LLMProvider { return entityLLM },
		nil,
	)

	resp, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Partial extraction test.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Facts should succeed.
	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 extracted memory, got %d", len(resp.Memories))
	}
	// Entities should fail gracefully.
	if resp.EntitiesCreated != 0 {
		t.Errorf("expected 0 entities (entity LLM failed), got %d", resp.EntitiesCreated)
	}
	// 1 raw + 1 fact = 2.
	if len(deps.memories.created) != 2 {
		t.Errorf("expected 2 memories, got %d", len(deps.memories.created))
	}
}

func TestExtract_FactExtractionFailsButEntitiesSucceed(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	factLLM := &mockLLMProvider{
		name:   "fact-llm",
		models: []string{"fact-model"},
		err:    fmt.Errorf("fact LLM error"),
	}

	entityResp := `{"entities":[{"name":"Python","type":"tech","properties":{}}],"relationships":[]}`
	entityLLM := makeEntityLLM(entityResp, 60, 25)

	svc, deps := newExtractTestService(
		projects, namespaces,
		func() provider.LLMProvider { return factLLM },
		func() provider.LLMProvider { return entityLLM },
		nil,
	)

	resp, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Python is great.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Facts should fail gracefully: no extracted memories.
	if len(resp.Memories) != 0 {
		t.Errorf("expected 0 extracted memories (fact LLM failed), got %d", len(resp.Memories))
	}
	// Entities should succeed.
	if resp.EntitiesCreated != 1 {
		t.Errorf("expected 1 entity, got %d", resp.EntitiesCreated)
	}
	// Only raw memory.
	if len(deps.memories.created) != 1 {
		t.Errorf("expected 1 memory (raw only), got %d", len(deps.memories.created))
	}
	if len(deps.entities.entities) != 1 {
		t.Errorf("expected 1 entity upsert, got %d", len(deps.entities.entities))
	}
}

func TestExtract_TokenUsageRecorded(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	factResp := `[{"fact":"Token tracked fact","confidence":0.9}]`
	factLLM := makeFactLLM(factResp, 100, 50)

	entityResp := `{"entities":[{"name":"Token","type":"concept","properties":{}}],"relationships":[]}`
	entityLLM := makeEntityLLM(entityResp, 80, 40)

	userID := uuid.New()
	orgID := uuid.New()
	apiKeyID := uuid.New()

	svc, deps := newExtractTestService(
		projects, namespaces,
		func() provider.LLMProvider { return factLLM },
		func() provider.LLMProvider { return entityLLM },
		nil,
	)

	_, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Track tokens.",
		Source:    "test",
		UserID:    &userID,
		OrgID:     &orgID,
		APIKeyID:  &apiKeyID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 2 token usage records: one for fact extraction, one for entity extraction.
	if len(deps.tokenUsage.usages) != 2 {
		t.Fatalf("expected 2 token usage records, got %d", len(deps.tokenUsage.usages))
	}

	// Check fact extraction usage.
	factUsage := deps.tokenUsage.usages[0]
	if factUsage.Operation != "fact_extraction" {
		t.Errorf("expected operation 'fact_extraction', got %q", factUsage.Operation)
	}
	if factUsage.TokensInput != 100 {
		t.Errorf("expected 100 input tokens, got %d", factUsage.TokensInput)
	}
	if factUsage.TokensOutput != 50 {
		t.Errorf("expected 50 output tokens, got %d", factUsage.TokensOutput)
	}
	if *factUsage.UserID != userID {
		t.Errorf("expected user ID %s, got %s", userID, *factUsage.UserID)
	}

	// Check entity extraction usage.
	entityUsage := deps.tokenUsage.usages[1]
	if entityUsage.Operation != "entity_extraction" {
		t.Errorf("expected operation 'entity_extraction', got %q", entityUsage.Operation)
	}
	if entityUsage.TokensInput != 80 {
		t.Errorf("expected 80 input tokens, got %d", entityUsage.TokensInput)
	}
}

func TestExtract_LineageRecordsCreated(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	factResp := `[{"fact":"Lineage fact 1","confidence":0.9},{"fact":"Lineage fact 2","confidence":0.8}]`
	factLLM := makeFactLLM(factResp, 50, 20)

	svc, deps := newExtractTestService(
		projects, namespaces,
		func() provider.LLMProvider { return factLLM },
		nil,
		nil,
	)

	_, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Create lineage.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 2 lineage records for 2 facts.
	if len(deps.lineage.lineages) != 2 {
		t.Fatalf("expected 2 lineage records, got %d", len(deps.lineage.lineages))
	}

	// All lineage should reference the raw memory as parent.
	rawMemID := deps.memories.created[0].ID
	for i, l := range deps.lineage.lineages {
		if l.Relation != "extracted_from" {
			t.Errorf("lineage[%d]: expected relation 'extracted_from', got %q", i, l.Relation)
		}
		if l.ParentID == nil || *l.ParentID != rawMemID {
			t.Errorf("lineage[%d]: expected parent ID %s", i, rawMemID)
		}
	}
}

func TestExtract_EntitiesUpserted(t *testing.T) {
	projectID, nsID, projects, namespaces := setupTestFixtures()

	entityResp := `{"entities":[{"name":"Alice","type":"person","properties":{"role":"dev"}},{"name":"NRAM","type":"tech","properties":{}}],"relationships":[]}`
	entityLLM := makeEntityLLM(entityResp, 60, 30)

	svc, deps := newExtractTestService(
		projects, namespaces,
		nil,
		func() provider.LLMProvider { return entityLLM },
		nil,
	)

	_, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Alice develops NRAM.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(deps.entities.entities) != 2 {
		t.Fatalf("expected 2 entities, got %d", len(deps.entities.entities))
	}

	alice := deps.entities.entities[0]
	if alice.Name != "Alice" {
		t.Errorf("expected entity name 'Alice', got %q", alice.Name)
	}
	if alice.Canonical != "alice" {
		t.Errorf("expected canonical 'alice', got %q", alice.Canonical)
	}
	if alice.EntityType != "person" {
		t.Errorf("expected entity type 'person', got %q", alice.EntityType)
	}
	if alice.NamespaceID != nsID {
		t.Errorf("expected namespace ID %s, got %s", nsID, alice.NamespaceID)
	}

	nram := deps.entities.entities[1]
	if nram.Canonical != "nram" {
		t.Errorf("expected canonical 'nram', got %q", nram.Canonical)
	}
}

func TestExtract_RelationshipsWithCorrectSourceTarget(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	entityResp := `{"entities":[{"name":"Bob","type":"person","properties":{}},{"name":"Google","type":"org","properties":{}}],"relationships":[{"source":"Bob","target":"Google","relation":"employed_by","temporal":"current"}]}`
	entityLLM := makeEntityLLM(entityResp, 70, 35)

	svc, deps := newExtractTestService(
		projects, namespaces,
		nil,
		func() provider.LLMProvider { return entityLLM },
		nil,
	)

	_, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Bob works at Google.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(deps.relationships.relationships) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(deps.relationships.relationships))
	}

	rel := deps.relationships.relationships[0]

	// Source should be Bob's entity ID, target should be Google's entity ID.
	bobID := deps.entities.entities[0].ID
	googleID := deps.entities.entities[1].ID

	if rel.SourceID != bobID {
		t.Errorf("expected source ID %s (Bob), got %s", bobID, rel.SourceID)
	}
	if rel.TargetID != googleID {
		t.Errorf("expected target ID %s (Google), got %s", googleID, rel.TargetID)
	}
	if rel.Relation != "employed_by" {
		t.Errorf("expected relation 'employed_by', got %q", rel.Relation)
	}
}

func TestExtract_EmptyContentError(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	svc, _ := newExtractTestService(projects, namespaces, nil, nil, nil)

	_, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "",
		Source:    "test",
	})
	if err == nil {
		t.Error("expected error for empty content")
	}

	_, err = svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "   ",
		Source:    "test",
	})
	if err == nil {
		t.Error("expected error for whitespace-only content")
	}
}

func TestExtract_LatencyTracked(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	svc, _ := newExtractTestService(projects, namespaces, nil, nil, nil)

	resp, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Latency test.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.LatencyMs < 0 {
		t.Errorf("expected non-negative latency, got %d", resp.LatencyMs)
	}
}

// --- Parse function unit tests ---

func TestParseFactResponse_CleanJSON(t *testing.T) {
	input := `[{"fact":"Clean JSON fact","confidence":0.9},{"fact":"Another fact","confidence":0.8}]`
	facts, err := parseFactResponse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(facts) != 2 {
		t.Fatalf("expected 2 facts, got %d", len(facts))
	}
	if facts[0].Fact != "Clean JSON fact" {
		t.Errorf("expected 'Clean JSON fact', got %q", facts[0].Fact)
	}
	if facts[0].Confidence != 0.9 {
		t.Errorf("expected confidence 0.9, got %f", facts[0].Confidence)
	}
}

func TestParseFactResponse_MarkdownFenced(t *testing.T) {
	// With JSON mode enabled, LLM output should never contain markdown fences.
	// The parser no longer strips fences, so fenced input is treated as invalid.
	input := "```json\n[{\"fact\":\"Fenced fact\",\"confidence\":0.85}]\n```"
	_, err := parseFactResponse(input)
	if err == nil {
		t.Error("expected error for markdown-fenced input (JSON mode makes fence stripping unnecessary)")
	}
}

func TestParseFactResponse_EmbeddedInText(t *testing.T) {
	// With JSON mode enabled, LLM output should be pure JSON.
	// The parser no longer extracts JSON from surrounding text.
	input := "Here are the facts:\n[{\"fact\":\"Embedded fact\",\"confidence\":0.7}]\nEnd."
	_, err := parseFactResponse(input)
	if err == nil {
		t.Error("expected error for text-wrapped input (JSON mode makes extraction unnecessary)")
	}
}

func TestParseFactResponse_InvalidJSON(t *testing.T) {
	input := "This is not JSON at all"
	_, err := parseFactResponse(input)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseEntityResponse_CleanJSON(t *testing.T) {
	input := `{"entities":[{"name":"Go","type":"tech","properties":{"version":"1.21"}}],"relationships":[{"source":"Go","target":"Google","relation":"created_by","temporal":"current"}]}`
	result, err := parseEntityResponse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Entities) != 1 {
		t.Fatalf("expected 1 entity, got %d", len(result.Entities))
	}
	if result.Entities[0].Name != "Go" {
		t.Errorf("expected entity name 'Go', got %q", result.Entities[0].Name)
	}
	if len(result.Relationships) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(result.Relationships))
	}
	if result.Relationships[0].Relation != "created_by" {
		t.Errorf("expected relation 'created_by', got %q", result.Relationships[0].Relation)
	}
}

func TestParseEntityResponse_MarkdownFenced(t *testing.T) {
	// With JSON mode enabled, LLM output should never contain markdown fences.
	// The parser no longer strips fences, so fenced input is treated as invalid.
	input := "```json\n{\"entities\":[{\"name\":\"Python\",\"type\":\"tech\",\"properties\":{}}],\"relationships\":[]}\n```"
	_, err := parseEntityResponse(input)
	if err == nil {
		t.Error("expected error for markdown-fenced input (JSON mode makes fence stripping unnecessary)")
	}
}

func TestParseEntityResponse_InvalidJSON(t *testing.T) {
	input := "Not valid entity JSON"
	_, err := parseEntityResponse(input)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestExtract_EmbeddingForExtractedFacts(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	factResp := `[{"fact":"Embedded extracted fact","confidence":0.9}]`
	factLLM := makeFactLLM(factResp, 50, 20)

	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{128},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 128)},
			Model:      "embed-model",
			Usage:      provider.TokenUsage{PromptTokens: 5, TotalTokens: 5},
		},
	}

	svc, deps := newExtractTestService(
		projects, namespaces,
		func() provider.LLMProvider { return factLLM },
		nil,
		embProvider,
	)

	resp, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Embed this fact.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.Memories) != 1 {
		t.Fatalf("expected 1 extracted memory, got %d", len(resp.Memories))
	}

	// Should have embedding token usage recorded (fact extraction + embedding).
	if len(deps.tokenUsage.usages) != 2 {
		t.Fatalf("expected 2 token usage records (fact + embedding), got %d", len(deps.tokenUsage.usages))
	}

	// Check that vector was upserted.
	if len(deps.vectors.upserted) != 1 {
		t.Fatalf("expected 1 vector upsert, got %d", len(deps.vectors.upserted))
	}

	// Total tokens should include embedding tokens.
	if resp.TokensUsed.Input != 55 { // 50 (fact) + 5 (embed)
		t.Errorf("expected 55 total input tokens, got %d", resp.TokensUsed.Input)
	}
}

func TestExtract_RelationshipWithTemporalQualifier(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	entityResp := `{"entities":[{"name":"Carol","type":"person","properties":{}},{"name":"Meta","type":"org","properties":{}}],"relationships":[{"source":"Carol","target":"Meta","relation":"worked_at","temporal":"previously"}]}`
	entityLLM := makeEntityLLM(entityResp, 70, 35)

	svc, deps := newExtractTestService(
		projects, namespaces,
		nil,
		func() provider.LLMProvider { return entityLLM },
		nil,
	)

	_, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Carol previously worked at Meta.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(deps.relationships.relationships) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(deps.relationships.relationships))
	}

	rel := deps.relationships.relationships[0]
	if rel.Properties == nil {
		t.Fatal("expected properties with temporal qualifier")
	}
	var props map[string]string
	if jsonErr := json.Unmarshal(rel.Properties, &props); jsonErr != nil {
		t.Fatalf("failed to unmarshal properties: %v", jsonErr)
	}
	if props["temporal"] != "previously" {
		t.Errorf("expected temporal 'previously', got %q", props["temporal"])
	}
}

func TestExtract_NilProjectIDError(t *testing.T) {
	_, _, projects, namespaces := setupTestFixtures()
	svc, _ := newExtractTestService(projects, namespaces, nil, nil, nil)

	_, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: uuid.Nil,
		Content:   "test",
		Source:    "test",
	})
	if err == nil {
		t.Error("expected error for nil project ID")
	}
}

func TestExtract_RelationshipSkippedForUnknownEntity(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	// Relationship references an entity not in the entities list.
	entityResp := `{"entities":[{"name":"Dave","type":"person","properties":{}}],"relationships":[{"source":"Dave","target":"UnknownCo","relation":"works_at","temporal":"current"}]}`
	entityLLM := makeEntityLLM(entityResp, 50, 25)

	svc, deps := newExtractTestService(
		projects, namespaces,
		nil,
		func() provider.LLMProvider { return entityLLM },
		nil,
	)

	resp, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Dave works at UnknownCo.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.EntitiesCreated != 1 {
		t.Errorf("expected 1 entity, got %d", resp.EntitiesCreated)
	}
	// Relationship should be skipped because UnknownCo is not in entity list.
	if resp.RelationshipsCreated != 0 {
		t.Errorf("expected 0 relationships (target not found), got %d", resp.RelationshipsCreated)
	}
	if len(deps.relationships.relationships) != 0 {
		t.Errorf("expected no relationship records, got %d", len(deps.relationships.relationships))
	}
}

// TestExtract_VectorUpsertFailure_ClearsEmbeddingDim drives the
// embedMemory failure path: when the vector store rejects an Upsert
// during fact-child embedding, the extracted child memory must be
// persisted WITHOUT an embedding_dim. The embedding-backfill phase
// owns the repair on the next dream cycle.
func TestExtract_VectorUpsertFailure_ClearsEmbeddingDim(t *testing.T) {
	projectID, _, projects, namespaces := setupTestFixtures()

	factResp := `[{"fact":"Fact whose vector write fails","confidence":0.9}]`
	factLLM := makeFactLLM(factResp, 50, 20)

	embProvider := &mockEmbeddingProvider{
		name:       "test-embed",
		dimensions: []int{384},
		resp: &provider.EmbeddingResponse{
			Embeddings: [][]float32{make([]float32, 384)},
			Model:      "embed-model",
			Usage:      provider.TokenUsage{PromptTokens: 5, TotalTokens: 5},
		},
	}

	svc, deps := newExtractTestService(
		projects, namespaces,
		func() provider.LLMProvider { return factLLM },
		nil,
		embProvider,
	)
	deps.vectors.upsertErr = fmt.Errorf("vector store offline")

	_, err := svc.Extract(context.Background(), &StoreRequest{
		ProjectID: projectID,
		Content:   "Some content to embed.",
		Source:    "test",
	})
	if err != nil {
		t.Fatalf("Extract should succeed even when vector Upsert fails; got err=%v", err)
	}

	// Find the fact-child memory: it carries the fact text as content.
	const factText = "Fact whose vector write fails"
	var child *model.Memory
	for _, m := range deps.memories.created {
		if m.Content == factText {
			child = m
			break
		}
	}
	if child == nil {
		t.Fatalf("extracted child memory %q not found in created list", factText)
	}
	if child.EmbeddingDim != nil {
		t.Errorf("child EmbeddingDim must be cleared when vector Upsert failed; got %v", *child.EmbeddingDim)
	}
	if len(deps.vectors.upserted) != 0 {
		t.Errorf("Upsert calls should have failed; got %d successful upserts", len(deps.vectors.upserted))
	}
}

