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
	"math/rand"
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

// EntityUpserter creates or updates an entity record and supports lookup
// by name similarity so that relationship resolution can find entities
// created by prior enrichment jobs.
type EntityUpserter interface {
	Upsert(ctx context.Context, entity *model.Entity) error
	FindBySimilarity(ctx context.Context, namespaceID uuid.UUID, name string, kind string, limit int) ([]model.Entity, error)
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

// UsageContextResolver resolves org, user, and project IDs from a project
// namespace ID so that token usage records can be attributed correctly.
type UsageContextResolver interface {
	ResolveUsageContext(ctx context.Context, namespaceID uuid.UUID) (*model.UsageContext, error)
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
	Fact       string   `json:"fact"` // alternate field name some LLMs use
	Confidence float64  `json:"confidence"`
	Tags       []string `json:"tags"`
}

// text returns the fact content, preferring "content" over "fact".
func (f extractedFact) text() string {
	if f.Content != "" {
		return f.Content
	}
	return f.Fact
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
	Workers      int           // number of concurrent workers (default: 1 for SQLite, 2 for Postgres)
	PollInterval time.Duration // how often idle workers poll for jobs, default 5s
	Backend      string        // "sqlite" or "postgres" — used to tune defaults
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.Workers <= 0 {
		if c.Backend == "sqlite" {
			c.Workers = 1 // single writer makes multiple workers pointless on SQLite
		} else {
			c.Workers = 2
		}
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
	usageCtxRes    UsageContextResolver
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
	usageCtxRes UsageContextResolver,
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
		usageCtxRes:    usageCtxRes,
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

	// Consecutive empty polls — used for backoff.
	emptyPolls := 0
	const maxBackoff = 30 // seconds

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
			emptyPolls++
			wp.sleepWithBackoff(ctx, emptyPolls, maxBackoff)
			continue
		}
		if job == nil {
			emptyPolls++
			wp.sleepWithBackoff(ctx, emptyPolls, maxBackoff)
			continue
		}

		// Reset backoff on successful claim.
		emptyPolls = 0

		if err := wp.processJob(ctx, workerID, job); err != nil {
			slog.Error("enrichment: job failed", "worker", workerID, "job", job.ID, "err", err)
		}
	}
}

// sleepWithBackoff waits for the poll interval plus jitter and exponential
// backoff based on consecutive empty polls. This reduces contention when the
// queue is idle and prevents synchronized polling spikes from multiple workers.
func (wp *WorkerPool) sleepWithBackoff(ctx context.Context, emptyPolls, maxBackoffSec int) {
	base := wp.config.PollInterval

	// Exponential backoff: double the interval for each consecutive empty poll,
	// capped at maxBackoffSec.
	backoff := base
	for i := 0; i < emptyPolls && i < 5; i++ {
		backoff *= 2
	}
	maxDuration := time.Duration(maxBackoffSec) * time.Second
	if backoff > maxDuration {
		backoff = maxDuration
	}

	// Add jitter: ±25% to prevent synchronized polling.
	jitter := time.Duration(rand.Int63n(int64(backoff/2))) - backoff/4
	wait := backoff + jitter
	if wait < time.Second {
		wait = time.Second
	}

	t := time.NewTimer(wait)
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
			Tags:        mergeTags(mem.Tags, fact.Tags),
			Source:      mem.Source,
			Importance:  0.5,
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

			// Resolve missing entities — look up in DB or create stubs so
			// that relationships extracted by the LLM are never dropped.
			if !srcOK {
				ent, err := wp.resolveOrCreateEntity(ctx, mem.NamespaceID, rel.Source)
				if err != nil {
					slog.Error("enrichment: resolve source entity", "entity", rel.Source, "err", err)
				} else {
					srcID, srcOK = ent.ID, true
					entityNameToID[rel.Source] = ent.ID
				}
			}
			if !tgtOK {
				ent, err := wp.resolveOrCreateEntity(ctx, mem.NamespaceID, rel.Target)
				if err != nil {
					slog.Error("enrichment: resolve target entity", "entity", rel.Target, "err", err)
				} else {
					tgtID, tgtOK = ent.ID, true
					entityNameToID[rel.Target] = ent.ID
				}
			}

			if !srcOK || !tgtOK {
				slog.Warn("enrichment: skip relationship, entity resolution failed",
					"source", rel.Source, "srcResolved", srcOK,
					"target", rel.Target, "tgtResolved", tgtOK)
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
				ValidFrom:    mem.CreatedAt,
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

// resolveOrCreateEntity looks up an existing entity in the database by
// canonical name within the given namespace. If no match is found, it creates
// a stub entity so that relationships extracted by the LLM are never dropped.
func (wp *WorkerPool) resolveOrCreateEntity(ctx context.Context, namespaceID uuid.UUID, name string) (*model.Entity, error) {
	canonical := strings.ToLower(strings.TrimSpace(name))
	similar, err := wp.entities.FindBySimilarity(ctx, namespaceID, canonical, "", 10)
	if err != nil {
		return nil, err
	}
	for i := range similar {
		if similar[i].Canonical == canonical {
			return &similar[i], nil
		}
	}

	// Entity doesn't exist — create it so the relationship is preserved.
	now := time.Now().UTC()
	entity := &model.Entity{
		ID:           uuid.New(),
		NamespaceID:  namespaceID,
		Name:         name,
		Canonical:    canonical,
		EntityType:   "unknown",
		Properties:   json.RawMessage(`{}`),
		MentionCount: 1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := wp.entities.Upsert(ctx, entity); err != nil {
		return nil, fmt.Errorf("create stub entity %q: %w", name, err)
	}
	slog.Info("enrichment: created stub entity from relationship reference", "entity", name, "id", entity.ID)
	return entity, nil
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
		JSONMode:    true,
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
		JSONMode:    true,
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
// Response parsing (JSON mode guarantees valid JSON from the LLM)
// ---------------------------------------------------------------------------

// parseFactResponse parses a fact extraction response. With JSON mode enabled
// the LLM is constrained to produce valid JSON, so only direct unmarshal with
// a string-array fallback is needed.
func parseFactResponse(raw string) ([]extractedFact, error) {
	raw = strings.TrimSpace(raw)

	// Try array of structured facts.
	var facts []extractedFact
	if err := json.Unmarshal([]byte(raw), &facts); err == nil && len(facts) > 0 {
		for i := range facts {
			facts[i].Content = facts[i].text()
		}
		return facts, nil
	}

	// Single fact object instead of array.
	var single extractedFact
	if err := json.Unmarshal([]byte(raw), &single); err == nil && single.text() != "" {
		single.Content = single.text()
		return []extractedFact{single}, nil
	}

	// Wrapper object with a "facts" key.
	var wrapper struct {
		Facts []extractedFact `json:"facts"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapper); err == nil && len(wrapper.Facts) > 0 {
		for i := range wrapper.Facts {
			wrapper.Facts[i].Content = wrapper.Facts[i].text()
		}
		return wrapper.Facts, nil
	}

	// Plain string array.
	var strs []string
	if err := json.Unmarshal([]byte(raw), &strs); err == nil && len(strs) > 0 {
		facts = make([]extractedFact, len(strs))
		for i, s := range strs {
			facts[i] = extractedFact{Content: s, Confidence: 0.8}
		}
		return facts, nil
	}

	preview := raw
	if len(preview) > 200 {
		preview = preview[:200] + "..."
	}
	return nil, fmt.Errorf("unable to parse fact JSON from LLM response: %q", preview)
}

// parseEntityResponse parses an entity/relationship extraction response.
// With JSON mode enabled the LLM is constrained to produce valid JSON.
func parseEntityResponse(raw string) (*entityExtractionResult, error) {
	raw = strings.TrimSpace(raw)

	var result entityExtractionResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		preview := raw
		if len(preview) > 200 {
			preview = preview[:200] + "..."
		}
		return nil, fmt.Errorf("unable to parse entity JSON from LLM response: %q", preview)
	}
	return &result, nil
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

	// Resolve ownership context so the record can be filtered by org/user/project.
	if wp.usageCtxRes != nil {
		if uc, err := wp.usageCtxRes.ResolveUsageContext(ctx, mem.NamespaceID); err == nil && uc != nil {
			u.OrgID = uc.OrgID
			u.UserID = uc.UserID
			u.ProjectID = uc.ProjectID
		}
	}

	if err := wp.tokenUsage.Record(ctx, u); err != nil {
		slog.Error("enrichment: record token usage", "operation", operation, "err", err)
	}
}

func mergeTags(parent, child []string) []string {
	seen := make(map[string]bool, len(parent)+len(child))
	result := make([]string, 0, len(parent)+len(child))
	for _, t := range parent {
		if !seen[t] {
			seen[t] = true
			result = append(result, t)
		}
	}
	for _, t := range child {
		if !seen[t] {
			seen[t] = true
			result = append(result, t)
		}
	}
	return result
}
