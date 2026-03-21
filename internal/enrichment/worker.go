// Package enrichment provides background worker pool processing for the nram
// enrichment pipeline. Workers claim jobs from the enrichment queue, run fact
// and entity extraction via LLM providers, generate embeddings, persist
// results, and record token usage.
package enrichment

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
)

// ---------------------------------------------------------------------------
// Dependency-inversion interfaces
// ---------------------------------------------------------------------------

// MemoryReader retrieves a memory by ID.
type MemoryReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*model.Memory, error)
}

// MemoryUpdater persists changes to an existing memory.
type MemoryUpdater interface {
	Update(ctx context.Context, mem *model.Memory) error
}

// MemoryCreator persists a new memory record.
type MemoryCreator interface {
	Create(ctx context.Context, mem *model.Memory) error
}

// QueueClaimer manages enrichment job lifecycle in the queue.
type QueueClaimer interface {
	ClaimNext(ctx context.Context, workerID string) (*model.EnrichmentJob, error)
	Complete(ctx context.Context, id uuid.UUID) error
	Fail(ctx context.Context, id uuid.UUID, errMsg string) error
}

// EntityUpserter creates or updates an entity record.
type EntityUpserter interface {
	Upsert(ctx context.Context, entity *model.Entity) error
}

// RelationshipCreator persists a new relationship between entities.
type RelationshipCreator interface {
	Create(ctx context.Context, rel *model.Relationship) error
}

// LineageCreator records parent-child lineage between memories.
type LineageCreator interface {
	Create(ctx context.Context, lineage *model.MemoryLineage) error
}

// TokenRecorder persists token-usage telemetry.
type TokenRecorder interface {
	Record(ctx context.Context, usage *model.TokenUsage) error
}

// VectorWriter upserts an embedding vector for a memory or entity.
type VectorWriter interface {
	Upsert(ctx context.Context, id uuid.UUID, namespaceID uuid.UUID, embedding []float32, dimension int) error
}

// ---------------------------------------------------------------------------
// Extraction prompt constants
// ---------------------------------------------------------------------------

const factExtractionPrompt = `You are a fact extraction engine. Given a text, extract all discrete facts as a JSON array. Each fact should be a JSON object with these fields:
- "content": the fact statement (string)
- "confidence": how confident you are in this fact, 0.0 to 1.0 (number)
- "tags": relevant tags for categorization (array of strings)

Return ONLY valid JSON. Do not include markdown fences or explanation.

Text:
%s`

const entityExtractionPrompt = `You are an entity and relationship extraction engine. Given a text, extract all named entities and relationships between them as JSON.

Return a JSON object with two fields:
- "entities": array of objects with fields:
  - "name": the entity name (string)
  - "type": the entity type, e.g. "person", "organization", "location", "concept" (string)
  - "properties": optional key-value pairs (object)
- "relationships": array of objects with fields:
  - "source": source entity name (string)
  - "target": target entity name (string)
  - "relation": the relationship type (string)
  - "weight": confidence/strength 0.0 to 1.0 (number)

Return ONLY valid JSON. Do not include markdown fences or explanation.

Text:
%s`

// ---------------------------------------------------------------------------
// Parsed extraction types
// ---------------------------------------------------------------------------

type extractedFact struct {
	Content    string   `json:"content"`
	Confidence float64  `json:"confidence"`
	Tags       []string `json:"tags"`
}

type extractedEntity struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}

type extractedRelationship struct {
	Source   string  `json:"source"`
	Target   string  `json:"target"`
	Relation string  `json:"relation"`
	Weight   float64 `json:"weight"`
}

type entityExtractionResult struct {
	Entities      []extractedEntity       `json:"entities"`
	Relationships []extractedRelationship `json:"relationships"`
}

// ---------------------------------------------------------------------------
// WorkerConfig / WorkerPool
// ---------------------------------------------------------------------------

// WorkerConfig controls the behavior of the enrichment worker pool.
type WorkerConfig struct {
	Workers      int           // number of concurrent workers, default 2
	PollInterval time.Duration // how often idle workers poll for jobs, default 5s
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.Workers <= 0 {
		c.Workers = 2
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Second
	}
	return c
}

// WorkerPool manages a set of background goroutines that process enrichment
// jobs from the queue.
type WorkerPool struct {
	config         WorkerConfig
	memories       MemoryReader
	memUpdater     MemoryUpdater
	memCreator     MemoryCreator
	queue          QueueClaimer
	entities       EntityUpserter
	relationships  RelationshipCreator
	lineage        LineageCreator
	tokenUsage     TokenRecorder
	vectorStore    VectorWriter
	factProvider   func() provider.LLMProvider
	entityProvider func() provider.LLMProvider
	embedProvider  func() provider.EmbeddingProvider

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWorkerPool creates a new enrichment worker pool. Provider functions may
// return nil to indicate that particular capability is unavailable.
func NewWorkerPool(
	config WorkerConfig,
	memories MemoryReader,
	memUpdater MemoryUpdater,
	memCreator MemoryCreator,
	queue QueueClaimer,
	entities EntityUpserter,
	relationships RelationshipCreator,
	lineage LineageCreator,
	tokenUsage TokenRecorder,
	vectorStore VectorWriter,
	factProvider func() provider.LLMProvider,
	entityProvider func() provider.LLMProvider,
	embedProvider func() provider.EmbeddingProvider,
) *WorkerPool {
	return &WorkerPool{
		config:         config.withDefaults(),
		memories:       memories,
		memUpdater:     memUpdater,
		memCreator:     memCreator,
		queue:          queue,
		entities:       entities,
		relationships:  relationships,
		lineage:        lineage,
		tokenUsage:     tokenUsage,
		vectorStore:    vectorStore,
		factProvider:   factProvider,
		entityProvider: entityProvider,
		embedProvider:  embedProvider,
	}
}

// Start launches the configured number of worker goroutines. Each loops
// until the pool is stopped: claim a job, process it, repeat (or sleep on
// empty queue).
func (wp *WorkerPool) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	wp.cancel = cancel

	for i := range wp.config.Workers {
		workerID := fmt.Sprintf("worker-%d", i)
		wp.wg.Add(1)
		go wp.run(ctx, workerID)
	}
}

// Stop cancels the background context and blocks until all workers finish.
func (wp *WorkerPool) Stop() {
	if wp.cancel != nil {
		wp.cancel()
	}
	wp.wg.Wait()
}

func (wp *WorkerPool) run(ctx context.Context, workerID string) {
	defer wp.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := wp.queue.ClaimNext(ctx, workerID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// No rows = empty queue, not an error.
			if !errors.Is(err, sql.ErrNoRows) {
				slog.Error("enrichment: claim error", "worker", workerID, "err", err)
			}
			wp.sleep(ctx)
			continue
		}
		if job == nil {
			wp.sleep(ctx)
			continue
		}

		if err := wp.processJob(ctx, workerID, job); err != nil {
			slog.Error("enrichment: job failed", "worker", workerID, "job", job.ID, "err", err)
		}
	}
}

func (wp *WorkerPool) sleep(ctx context.Context) {
	t := time.NewTimer(wp.config.PollInterval)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// ---------------------------------------------------------------------------
// processJob — the full enrichment pipeline for a single job
// ---------------------------------------------------------------------------

func (wp *WorkerPool) processJob(ctx context.Context, workerID string, job *model.EnrichmentJob) error {
	// a. Fetch the memory
	mem, err := wp.memories.GetByID(ctx, job.MemoryID)
	if err != nil {
		failErr := wp.queue.Fail(ctx, job.ID, fmt.Sprintf("memory lookup: %v", err))
		if failErr != nil {
			slog.Error("enrichment: fail-mark error", "job", job.ID, "err", failErr)
		}
		return fmt.Errorf("memory lookup: %w", err)
	}

	var (
		facts         []extractedFact
		entResult     *entityExtractionResult
		factUsage     *provider.TokenUsage
		entityUsage   *provider.TokenUsage
		factModel     string
		entityModel   string
		factProvider  string
		entityProv    string
		factErr       error
		entityErr     error
	)

	// b-c. Fact extraction (independent)
	hasFact := wp.factProvider() != nil
	hasEntity := wp.entityProvider() != nil

	if !hasFact && !hasEntity {
		// No providers configured — requeue by failing so it can be retried once
		// providers are set up.
		_ = wp.queue.Fail(ctx, job.ID, "no LLM providers configured")
		return fmt.Errorf("no LLM providers configured, job requeued")
	}

	if hasFact {
		facts, factUsage, factModel, factProvider, factErr = wp.extractFacts(ctx, wp.factProvider(), mem.Content)
	}

	// d-e. Entity extraction (independent)
	if hasEntity {
		entResult, entityUsage, entityModel, entityProv, entityErr = wp.extractEntities(ctx, wp.entityProvider(), mem.Content)
	}

	// If all configured providers failed, mark the job as failed.
	if (hasFact && factErr != nil) && (hasEntity && entityErr != nil) {
		errMsg := fmt.Sprintf("fact: %v; entity: %v", factErr, entityErr)
		_ = wp.queue.Fail(ctx, job.ID, errMsg)
		return fmt.Errorf("all extractions failed: %s", errMsg)
	}

	// If fact extraction was configured and failed, that is a fatal failure.
	if hasFact && factErr != nil {
		_ = wp.queue.Fail(ctx, job.ID, fmt.Sprintf("fact extraction: %v", factErr))
		return fmt.Errorf("fact extraction: %w", factErr)
	}

	// f. Create child memories and lineage for each extracted fact.
	for _, fact := range facts {
		childID := uuid.New()
		child := &model.Memory{
			ID:          childID,
			NamespaceID: mem.NamespaceID,
			Content:     fact.Content,
			Confidence:  fact.Confidence,
			Tags:        fact.Tags,
			Enriched:    false,
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		}
		if err := wp.memCreator.Create(ctx, child); err != nil {
			slog.Error("enrichment: create child memory", "job", job.ID, "err", err)
			continue
		}

		parentID := mem.ID
		lin := &model.MemoryLineage{
			ID:        uuid.New(),
			MemoryID:  childID,
			ParentID:  &parentID,
			Relation:  "extracted_fact",
			CreatedAt: time.Now().UTC(),
		}
		if err := wp.lineage.Create(ctx, lin); err != nil {
			slog.Error("enrichment: create lineage", "job", job.ID, "err", err)
		}
	}

	// g. Embed extracted facts (if embedding provider available).
	if ep := wp.embedProvider(); ep != nil && len(facts) > 0 {
		texts := make([]string, len(facts))
		for i, f := range facts {
			texts[i] = f.Content
		}
		embedResp, embedErr := ep.Embed(ctx, &provider.EmbeddingRequest{
			Input: texts,
		})
		if embedErr != nil {
			slog.Error("enrichment: embed facts", "job", job.ID, "err", embedErr)
		} else {
			// Record embedding token usage.
			wp.recordUsage(ctx, mem, "embedding", ep.Name(), "", embedResp.Usage)

			// Store vectors for original memory using first embedding if available.
			if len(embedResp.Embeddings) > 0 && wp.vectorStore != nil {
				dim := len(embedResp.Embeddings[0])
				if err := wp.vectorStore.Upsert(ctx, mem.ID, mem.NamespaceID, embedResp.Embeddings[0], dim); err != nil {
					slog.Error("enrichment: upsert vector", "job", job.ID, "err", err)
				}
			}
		}
	}

	// h-i. Upsert entities and create relationships.
	entityNameToID := make(map[string]uuid.UUID)
	if entResult != nil {
		for idx := range entResult.Entities {
			ent := &entResult.Entities[idx]
			entID := uuid.New()
			props, _ := json.Marshal(ent.Properties)

			modelEntity := &model.Entity{
				ID:           entID,
				NamespaceID:  mem.NamespaceID,
				Name:         ent.Name,
				Canonical:    strings.ToLower(ent.Name),
				EntityType:   ent.Type,
				Properties:   props,
				MentionCount: 1,
				CreatedAt:    time.Now().UTC(),
				UpdatedAt:    time.Now().UTC(),
			}
			if err := wp.entities.Upsert(ctx, modelEntity); err != nil {
				slog.Error("enrichment: upsert entity", "job", job.ID, "entity", ent.Name, "err", err)
				continue
			}
			entityNameToID[ent.Name] = modelEntity.ID
		}

		for _, rel := range entResult.Relationships {
			srcID, srcOK := entityNameToID[rel.Source]
			tgtID, tgtOK := entityNameToID[rel.Target]
			if !srcOK || !tgtOK {
				slog.Warn("enrichment: skip relationship, entity not found",
					"source", rel.Source, "target", rel.Target)
				continue
			}
			memID := mem.ID
			r := &model.Relationship{
				ID:           uuid.New(),
				NamespaceID:  mem.NamespaceID,
				SourceID:     srcID,
				TargetID:     tgtID,
				Relation:     rel.Relation,
				Weight:       rel.Weight,
				SourceMemory: &memID,
				ValidFrom:    time.Now().UTC(),
				CreatedAt:    time.Now().UTC(),
			}
			if err := wp.relationships.Create(ctx, r); err != nil {
				slog.Error("enrichment: create relationship", "job", job.ID, "err", err)
			}
		}
	}

	// j. Mark original memory as enriched.
	mem.Enriched = true
	mem.UpdatedAt = time.Now().UTC()
	if err := wp.memUpdater.Update(ctx, mem); err != nil {
		_ = wp.queue.Fail(ctx, job.ID, fmt.Sprintf("update memory enriched: %v", err))
		return fmt.Errorf("update memory: %w", err)
	}

	// k. Record token usage for LLM calls.
	if factUsage != nil {
		wp.recordUsage(ctx, mem, "fact_extraction", factProvider, factModel, *factUsage)
	}
	if entityUsage != nil {
		wp.recordUsage(ctx, mem, "entity_extraction", entityProv, entityModel, *entityUsage)
	}

	// l. Mark job complete.
	if err := wp.queue.Complete(ctx, job.ID); err != nil {
		return fmt.Errorf("complete job: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// LLM extraction helpers
// ---------------------------------------------------------------------------

func (wp *WorkerPool) extractFacts(
	ctx context.Context,
	llm provider.LLMProvider,
	content string,
) ([]extractedFact, *provider.TokenUsage, string, string, error) {
	prompt := fmt.Sprintf(factExtractionPrompt, content)
	resp, err := llm.Complete(ctx, &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "user", Content: prompt},
		},
		MaxTokens:   2048,
		Temperature: 0.2,
	})
	if err != nil {
		return nil, nil, "", "", fmt.Errorf("fact LLM call: %w", err)
	}

	facts, parseErr := parseFactResponse(resp.Content)
	if parseErr != nil {
		return nil, &resp.Usage, resp.Model, llm.Name(), fmt.Errorf("parse facts: %w", parseErr)
	}
	return facts, &resp.Usage, resp.Model, llm.Name(), nil
}

func (wp *WorkerPool) extractEntities(
	ctx context.Context,
	llm provider.LLMProvider,
	content string,
) (*entityExtractionResult, *provider.TokenUsage, string, string, error) {
	prompt := fmt.Sprintf(entityExtractionPrompt, content)
	resp, err := llm.Complete(ctx, &provider.CompletionRequest{
		Messages: []provider.Message{
			{Role: "user", Content: prompt},
		},
		MaxTokens:   2048,
		Temperature: 0.2,
	})
	if err != nil {
		return nil, nil, "", "", fmt.Errorf("entity LLM call: %w", err)
	}

	result, parseErr := parseEntityResponse(resp.Content)
	if parseErr != nil {
		return nil, &resp.Usage, resp.Model, llm.Name(), fmt.Errorf("parse entities: %w", parseErr)
	}
	return result, &resp.Usage, resp.Model, llm.Name(), nil
}

// ---------------------------------------------------------------------------
// Response parsing with three-tier recovery
// ---------------------------------------------------------------------------

// stripCodeFences removes optional markdown code fences from LLM output.
func stripCodeFences(s string) string {
	s = strings.TrimSpace(s)
	re := regexp.MustCompile("(?s)^```(?:json)?\\s*\n?(.*?)\\s*```$")
	if m := re.FindStringSubmatch(s); len(m) == 2 {
		return strings.TrimSpace(m[1])
	}
	return s
}

// extractJSONBlock uses a regex to find the first JSON array or object in the
// string, acting as a last-resort fallback.
func extractJSONBlock(s string) string {
	// Try array first.
	reArr := regexp.MustCompile(`(?s)(\[.*\])`)
	if m := reArr.FindString(s); m != "" {
		return m
	}
	// Try object.
	reObj := regexp.MustCompile(`(?s)(\{.*\})`)
	if m := reObj.FindString(s); m != "" {
		return m
	}
	return s
}

// parseFactResponse applies multi-tier recovery to parse a fact extraction
// response. It handles: structured fact objects, plain string arrays, code
// fences, and regex-extracted JSON blocks.
func parseFactResponse(raw string) ([]extractedFact, error) {
	candidates := []string{
		raw,
		stripCodeFences(raw),
		extractJSONBlock(stripCodeFences(raw)),
	}

	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}

		// Try structured facts first.
		var facts []extractedFact
		if err := json.Unmarshal([]byte(c), &facts); err == nil {
			return facts, nil
		}

		// Fallback: LLM returned an array of plain strings.
		var strings []string
		if err := json.Unmarshal([]byte(c), &strings); err == nil {
			facts = make([]extractedFact, len(strings))
			for i, s := range strings {
				facts[i] = extractedFact{Content: s, Confidence: 0.8}
			}
			return facts, nil
		}
	}

	return nil, fmt.Errorf("unable to parse fact JSON from LLM response")
}

// parseEntityResponse applies multi-tier recovery to parse an entity/relationship
// extraction response. Handles structured objects, code fences, regex-extracted
// blocks, and NDJSON (newline-delimited) fragments.
func parseEntityResponse(raw string) (*entityExtractionResult, error) {
	candidates := []string{
		raw,
		stripCodeFences(raw),
		extractJSONBlock(stripCodeFences(raw)),
	}

	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}

		var result entityExtractionResult
		if err := json.Unmarshal([]byte(c), &result); err == nil {
			return &result, nil
		}
	}

	// Last resort: try to find any JSON object in the response via line-by-line
	// scanning. Some models output multiple JSON fragments separated by newlines.
	stripped := stripCodeFences(raw)
	for _, line := range strings.Split(stripped, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var result entityExtractionResult
		if err := json.Unmarshal([]byte(line), &result); err == nil && len(result.Entities) > 0 {
			return &result, nil
		}
	}

	return nil, fmt.Errorf("unable to parse entity JSON from LLM response")
}

// ---------------------------------------------------------------------------
// Token usage recording helper
// ---------------------------------------------------------------------------

func (wp *WorkerPool) recordUsage(
	ctx context.Context,
	mem *model.Memory,
	operation, providerName, modelName string,
	usage provider.TokenUsage,
) {
	memID := mem.ID
	u := &model.TokenUsage{
		ID:           uuid.New(),
		NamespaceID:  mem.NamespaceID,
		Operation:    operation,
		Provider:     providerName,
		Model:        modelName,
		TokensInput:  usage.PromptTokens,
		TokensOutput: usage.CompletionTokens,
		MemoryID:     &memID,
		CreatedAt:    time.Now().UTC(),
	}
	if err := wp.tokenUsage.Record(ctx, u); err != nil {
		slog.Error("enrichment: record token usage", "operation", operation, "err", err)
	}
}
