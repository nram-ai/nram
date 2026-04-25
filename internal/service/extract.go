package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage"
)

// EntityCreator defines the entity persistence operations needed by the extraction service.
type EntityCreator interface {
	Upsert(ctx context.Context, entity *model.Entity) error
}

// RelationshipCreator defines the relationship persistence operations needed by the extraction service.
type RelationshipCreator interface {
	Create(ctx context.Context, rel *model.Relationship) error
}

// ExtractedFact represents a single fact extracted by an LLM.
type ExtractedFact struct {
	Fact       string  `json:"fact"`
	Content    string  `json:"content"` // alternate field name some LLMs use
	Confidence float64 `json:"confidence"`
}

// text returns the fact content, preferring "fact" over "content".
func (f ExtractedFact) text() string {
	if f.Fact != "" {
		return f.Fact
	}
	return f.Content
}

// ExtractedEntityData represents a single entity extracted by an LLM.
type ExtractedEntityData struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}

// ExtractedRelation represents a single relationship extracted by an LLM.
type ExtractedRelation struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	Relation string `json:"relation"`
	Temporal string `json:"temporal"`
}

// EntityExtractionResult holds the combined entity and relationship extraction output.
type EntityExtractionResult struct {
	Entities      []ExtractedEntityData `json:"entities"`
	Relationships []ExtractedRelation   `json:"relationships"`
}

// ExtractResponse contains the result of a synchronous extraction operation.
type ExtractResponse struct {
	Memories             []ExtractedMemory `json:"memories"`
	EntitiesCreated      int               `json:"entities_created"`
	RelationshipsCreated int               `json:"relationships_created"`
	TokensUsed           ExtractTokens     `json:"tokens_used"`
	LatencyMs            int64             `json:"latency_ms"`
}

// ExtractedMemory represents a single memory created during extraction.
type ExtractedMemory struct {
	ID         uuid.UUID `json:"id"`
	Content    string    `json:"content"`
	Confidence float64   `json:"confidence"`
}

// ExtractTokens tracks total token consumption across all extraction LLM calls.
type ExtractTokens struct {
	Input  int `json:"input"`
	Output int `json:"output"`
}

// ExtractionService orchestrates synchronous fact and entity extraction from memory content.
type ExtractionService struct {
	memories      MemoryRepository
	projects      ProjectRepository
	namespaces    NamespaceRepository
	ingestionLogs IngestionLogRepository
	tokenUsage    TokenUsageRepository
	entities      EntityCreator
	relationships RelationshipCreator
	lineage       LineageCreator
	vectorStore   VectorStoreWriter
	factProvider  func() provider.LLMProvider
	entityProvider func() provider.LLMProvider
	embedProvider func() provider.EmbeddingProvider
}

// NewExtractionService creates a new ExtractionService with the given dependencies.
func NewExtractionService(
	memories MemoryRepository,
	projects ProjectRepository,
	namespaces NamespaceRepository,
	ingestionLogs IngestionLogRepository,
	tokenUsage TokenUsageRepository,
	entities EntityCreator,
	relationships RelationshipCreator,
	lineage LineageCreator,
	vectorStore VectorStoreWriter,
	factProvider func() provider.LLMProvider,
	entityProvider func() provider.LLMProvider,
	embedProvider func() provider.EmbeddingProvider,
) *ExtractionService {
	return &ExtractionService{
		memories:       memories,
		projects:       projects,
		namespaces:     namespaces,
		ingestionLogs:  ingestionLogs,
		tokenUsage:     tokenUsage,
		entities:       entities,
		relationships:  relationships,
		lineage:        lineage,
		vectorStore:    vectorStore,
		factProvider:   factProvider,
		entityProvider: entityProvider,
		embedProvider:  embedProvider,
	}
}

const factExtractionPrompt = `You are a memory extraction system. Given the following text, extract discrete, standalone facts that would be useful to remember about the user or context in future conversations.

Rules:
- Each fact must be self-contained (understandable without the original text)
- Prefer specific over vague ("lives in Denver" not "lives somewhere in Colorado")
- Include temporal context when relevant ("as of March 2026")
- Assign confidence 0.0-1.0 based on how explicitly the fact was stated vs inferred
- Skip pleasantries, filler, and procedural content

Respond ONLY as a JSON array, no markdown fences, no preamble:
[{"fact": "...", "confidence": 0.95}, ...]`

const entityExtractionPrompt = `You are an entity and relationship extraction system. Given the following text, extract entities (people, organizations, technologies, places, concepts) and the relationships between them.

Rules:
- Each entity needs a name, a type, and optionally key properties
- Each relationship needs a source entity, target entity, relationship label, and temporal qualifier
- Temporal qualifiers: "current" (default), "as of <date>", "previously", "no longer"
- Normalize entity names
- Include relationship directionality

Respond ONLY as JSON, no markdown fences, no preamble:
{
  "entities": [{"name": "...", "type": "person|org|tech|place|concept", "properties": {}}],
  "relationships": [{"source": "...", "target": "...", "relation": "...", "temporal": "current"}]
}`

// Extract performs synchronous fact and entity extraction on the given store request.
func (s *ExtractionService) Extract(ctx context.Context, req *StoreRequest) (*ExtractResponse, error) {
	start := time.Now()

	// Validate required fields.
	if strings.TrimSpace(req.Content) == "" {
		return nil, fmt.Errorf("content is required")
	}
	if req.ProjectID == uuid.Nil {
		return nil, fmt.Errorf("project_id is required")
	}

	// Look up project.
	project, err := s.projects.GetByID(ctx, req.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("project not found: %w", err)
	}

	// Look up project's namespace.
	ns, err := s.namespaces.GetByID(ctx, project.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("namespace not found: %w", err)
	}

	// Store the original raw memory first.
	rawMemID := uuid.New()
	now := time.Now()
	var source *string
	if req.Source != "" {
		source = &req.Source
	}

	rawMem := &model.Memory{
		ID:          rawMemID,
		NamespaceID: ns.ID,
		Content:     req.Content,
		Source:      source,
		Tags:        req.Tags,
		Confidence:  1.0,
		Importance:  0.5,
		Metadata:    req.Metadata,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := s.memories.Create(ctx, rawMem); err != nil {
		return nil, fmt.Errorf("failed to create raw memory: %w", err)
	}

	// Create ingestion log for the raw memory.
	ingLog := &model.IngestionLog{
		ID:          uuid.New(),
		NamespaceID: ns.ID,
		Source:      req.Source,
		RawContent:  req.Content,
		MemoryIDs:   []uuid.UUID{rawMemID},
		Status:      "completed",
		Metadata:    req.Metadata,
		CreatedAt:   time.Now(),
	}
	_ = s.ingestionLogs.Create(ctx, ingLog)

	var totalTokens ExtractTokens
	var extractedMemories []ExtractedMemory

	// --- Fact extraction ---
	var factErr error
	var facts []ExtractedFact
	if s.factProvider != nil {
		fp := s.factProvider()
		if fp != nil {
			facts, factErr = s.extractFacts(ctx, fp, req.Content, project, ns, rawMemID, req, &totalTokens)
		}
	}

	// Build extracted memories from successful facts.
	if factErr == nil && len(facts) > 0 {
		for _, fact := range facts {
			childID := uuid.New()
			childNow := time.Now()
			childMem := &model.Memory{
				ID:          childID,
				NamespaceID: ns.ID,
				Content:     fact.Fact,
				Source:      source,
				Tags:        req.Tags,
				Confidence:  fact.Confidence,
				Importance:  0.5,
				Metadata:    req.Metadata,
				CreatedAt:   childNow,
				UpdatedAt:   childNow,
			}

			if createErr := s.memories.Create(ctx, childMem); createErr != nil {
				continue
			}

			// Create lineage: child extracted_from parent.
			lineageRecord := &model.MemoryLineage{
				ID:          uuid.New(),
				NamespaceID: ns.ID,
				MemoryID:    childID,
				ParentID:    &rawMemID,
				Relation:    model.LineageExtractedFrom,
				CreatedAt:   time.Now(),
			}
			_ = s.lineage.Create(ctx, lineageRecord)

			// Embed the extracted fact if provider is available.
			s.embedMemory(ctx, childMem, project, ns, req, &totalTokens)

			extractedMemories = append(extractedMemories, ExtractedMemory{
				ID:         childID,
				Content:    fact.Fact,
				Confidence: fact.Confidence,
			})
		}
	}

	// --- Entity extraction ---
	var entitiesCreated int
	var relationshipsCreated int
	if s.entityProvider != nil {
		ep := s.entityProvider()
		if ep != nil {
			ec, rc := s.extractEntities(ctx, ep, req.Content, project, ns, rawMemID, req, &totalTokens)
			entitiesCreated = ec
			relationshipsCreated = rc
		}
	}

	latency := time.Since(start).Milliseconds()

	return &ExtractResponse{
		Memories:             extractedMemories,
		EntitiesCreated:      entitiesCreated,
		RelationshipsCreated: relationshipsCreated,
		TokensUsed:           totalTokens,
		LatencyMs:            latency,
	}, nil
}

// extractFacts calls the fact extraction LLM and returns parsed facts.
func (s *ExtractionService) extractFacts(
	ctx context.Context,
	fp provider.LLMProvider,
	content string,
	project *model.Project,
	ns *model.Namespace,
	rawMemID uuid.UUID,
	req *StoreRequest,
	tokens *ExtractTokens,
) ([]ExtractedFact, error) {
	completionReq := &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "system", Content: factExtractionPrompt},
			{Role: "user", Content: content},
		},
		MaxTokens:   2048,
		Temperature: 0.1,
		JSONMode:    true,
	}

	resp, err := fp.Complete(ctx, completionReq)
	if err != nil {
		return nil, fmt.Errorf("fact extraction LLM call failed: %w", err)
	}

	// Record token usage.
	tokens.Input += resp.Usage.PromptTokens
	tokens.Output += resp.Usage.CompletionTokens

	projectID := project.ID
	usage := &model.TokenUsage{
		ID:           uuid.New(),
		OrgID:        req.OrgID,
		UserID:       req.UserID,
		ProjectID:    &projectID,
		NamespaceID:  ns.ID,
		Operation:    "fact_extraction",
		Provider:     fp.Name(),
		Model:        resp.Model,
		TokensInput:  resp.Usage.PromptTokens,
		TokensOutput: resp.Usage.CompletionTokens,
		MemoryID:     &rawMemID,
		APIKeyID:     req.APIKeyID,
		CreatedAt:    time.Now(),
	}
	_ = s.tokenUsage.Record(ctx, usage)

	facts, err := parseFactResponse(resp.Content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse fact response: %w", err)
	}

	return facts, nil
}

// extractEntities calls the entity extraction LLM and persists entities and relationships.
func (s *ExtractionService) extractEntities(
	ctx context.Context,
	ep provider.LLMProvider,
	content string,
	project *model.Project,
	ns *model.Namespace,
	rawMemID uuid.UUID,
	req *StoreRequest,
	tokens *ExtractTokens,
) (entitiesCreated int, relationshipsCreated int) {
	completionReq := &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "system", Content: entityExtractionPrompt},
			{Role: "user", Content: content},
		},
		MaxTokens:   2048,
		Temperature: 0.1,
		JSONMode:    true,
	}

	resp, err := ep.Complete(ctx, completionReq)
	if err != nil {
		return 0, 0
	}

	// Record token usage.
	tokens.Input += resp.Usage.PromptTokens
	tokens.Output += resp.Usage.CompletionTokens

	projectID := project.ID
	usage := &model.TokenUsage{
		ID:           uuid.New(),
		OrgID:        req.OrgID,
		UserID:       req.UserID,
		ProjectID:    &projectID,
		NamespaceID:  ns.ID,
		Operation:    "entity_extraction",
		Provider:     ep.Name(),
		Model:        resp.Model,
		TokensInput:  resp.Usage.PromptTokens,
		TokensOutput: resp.Usage.CompletionTokens,
		MemoryID:     &rawMemID,
		APIKeyID:     req.APIKeyID,
		CreatedAt:    time.Now(),
	}
	_ = s.tokenUsage.Record(ctx, usage)

	result, err := parseEntityResponse(resp.Content)
	if err != nil {
		return 0, 0
	}

	// Build a name->ID map for entity resolution.
	entityMap := make(map[string]uuid.UUID)

	for _, ed := range result.Entities {
		entityID := uuid.New()
		canonical := strings.ToLower(strings.TrimSpace(ed.Name))

		var props json.RawMessage
		if ed.Properties != nil {
			propsBytes, _ := json.Marshal(ed.Properties)
			props = propsBytes
		}

		entity := &model.Entity{
			ID:           entityID,
			NamespaceID:  ns.ID,
			Name:         ed.Name,
			Canonical:    canonical,
			EntityType:   ed.Type,
			Properties:   props,
			MentionCount: 1,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		if upsertErr := s.entities.Upsert(ctx, entity); upsertErr != nil {
			continue
		}

		entityMap[canonical] = entityID
		entitiesCreated++
	}

	for _, rel := range result.Relationships {
		srcCanonical := strings.ToLower(strings.TrimSpace(rel.Source))
		tgtCanonical := strings.ToLower(strings.TrimSpace(rel.Target))

		srcID, srcOK := entityMap[srcCanonical]
		tgtID, tgtOK := entityMap[tgtCanonical]
		if !srcOK || !tgtOK {
			continue
		}

		relationship := &model.Relationship{
			ID:           uuid.New(),
			NamespaceID:  ns.ID,
			SourceID:     srcID,
			TargetID:     tgtID,
			Relation:     rel.Relation,
			Weight:       1.0,
			ValidFrom:    time.Now(),
			SourceMemory: &rawMemID,
			CreatedAt:    time.Now(),
		}

		if rel.Temporal != "" && rel.Temporal != "current" {
			propsBytes, _ := json.Marshal(map[string]string{"temporal": rel.Temporal})
			relationship.Properties = propsBytes
		}

		if createErr := s.relationships.Create(ctx, relationship); createErr != nil {
			continue
		}

		relationshipsCreated++
	}

	return entitiesCreated, relationshipsCreated
}

// embedMemory embeds a memory's content if an embedding provider is available.
func (s *ExtractionService) embedMemory(
	ctx context.Context,
	mem *model.Memory,
	project *model.Project,
	ns *model.Namespace,
	req *StoreRequest,
	tokens *ExtractTokens,
) {
	if s.embedProvider == nil {
		return
	}
	ep := s.embedProvider()
	if ep == nil {
		return
	}

	dim := bestEmbeddingDimension(ep.Dimensions())

	embReq := &provider.EmbeddingRequest{
		Input:     []string{mem.Content},
		Dimension: dim,
	}

	resp, err := ep.Embed(ctx, embReq)
	if err != nil || len(resp.Embeddings) == 0 {
		return
	}

	tokens.Input += resp.Usage.PromptTokens
	tokens.Output += resp.Usage.CompletionTokens

	embDim := len(resp.Embeddings[0])
	mem.EmbeddingDim = &embDim

	if s.vectorStore != nil {
		_ = s.vectorStore.Upsert(ctx, storage.VectorKindMemory, mem.ID, ns.ID, resp.Embeddings[0], embDim)
	}

	projectID := project.ID
	usage := &model.TokenUsage{
		ID:           uuid.New(),
		OrgID:        req.OrgID,
		UserID:       req.UserID,
		ProjectID:    &projectID,
		NamespaceID:  ns.ID,
		Operation:    "embedding",
		Provider:     ep.Name(),
		Model:        resp.Model,
		TokensInput:  resp.Usage.PromptTokens,
		TokensOutput: resp.Usage.CompletionTokens,
		MemoryID:     &mem.ID,
		APIKeyID:     req.APIKeyID,
		CreatedAt:    time.Now(),
	}
	_ = s.tokenUsage.Record(ctx, usage)
}

// parseFactResponse parses an LLM fact extraction response. With JSON mode
// enabled the LLM is constrained to produce valid JSON, so only direct
// unmarshal with a string-array fallback is needed.
func parseFactResponse(raw string) ([]ExtractedFact, error) {
	raw = strings.TrimSpace(raw)

	// Try array of structured facts.
	var facts []ExtractedFact
	if err := json.Unmarshal([]byte(raw), &facts); err == nil && len(facts) > 0 {
		for i := range facts {
			facts[i].Fact = facts[i].text()
		}
		return facts, nil
	}

	// Single fact object instead of array.
	var single ExtractedFact
	if err := json.Unmarshal([]byte(raw), &single); err == nil && single.text() != "" {
		single.Fact = single.text()
		return []ExtractedFact{single}, nil
	}

	// Wrapper object with a "facts" key.
	var wrapper struct {
		Facts []ExtractedFact `json:"facts"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapper); err == nil && len(wrapper.Facts) > 0 {
		for i := range wrapper.Facts {
			wrapper.Facts[i].Fact = wrapper.Facts[i].text()
		}
		return wrapper.Facts, nil
	}

	// Plain string array.
	var strs []string
	if err := json.Unmarshal([]byte(raw), &strs); err == nil && len(strs) > 0 {
		facts = make([]ExtractedFact, len(strs))
		for i, s := range strs {
			facts[i] = ExtractedFact{Fact: s, Confidence: 0.8}
		}
		return facts, nil
	}

	preview := raw
	if len(preview) > 200 {
		preview = preview[:200] + "..."
	}
	return nil, fmt.Errorf("failed to parse fact extraction response as JSON array: %q", preview)
}

// parseEntityResponse parses an LLM entity extraction response. With JSON mode
// enabled the LLM is constrained to produce valid JSON.
func parseEntityResponse(raw string) (*EntityExtractionResult, error) {
	raw = strings.TrimSpace(raw)

	var result EntityExtractionResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		preview := raw
		if len(preview) > 200 {
			preview = preview[:200] + "..."
		}
		return nil, fmt.Errorf("failed to parse entity extraction response as JSON object: %q", preview)
	}
	return &result, nil
}
