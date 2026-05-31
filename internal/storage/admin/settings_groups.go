package admin

import (
	"github.com/nram-ai/nram/internal/api"
)

// settingsGroups is the canonical parent-group taxonomy for the admin Settings
// UI: the ordered list of tabs, each an ordered set of sub-sections bound to a
// setting category. It is the single source of truth for how settings are
// organized; the React page renders it generically (GET
// /admin/settings?groups=true) instead of hardcoding the structure.
//
// Every non-prompt setting category must be claimed by exactly one sub-section
// here; TestEverySettingCategoryMapsToGroup enforces that a new category cannot
// be added without a home (mirroring TestEverySettingHasSchemaEntry).
//
// Prompt-typed settings (categories enrichment_prompts, dreaming_prompts) are
// intentionally absent: prompt keys are edited on the Prompt Templates page,
// not the Settings page, and are exempt from the grouping requirement.
var settingsGroups = []api.SettingGroup{
	{
		ID:          "memory",
		Label:       "Memory",
		Description: "Defaults applied to new memories and how long deleted ones are retained.",
		SubSections: []api.SettingSubSection{{Category: "memory"}},
	},
	{
		ID:                 "enrichment",
		Label:              "Enrichment",
		Description:        "Background pipeline that pulls facts and entities out of new memories.",
		RequiresEnrichment: true,
		SubSections: []api.SettingSubSection{
			{
				Category:    "enrichment",
				Label:       "General",
				Description: "Master switches and basic batch sizing for the enrichment pipeline.",
			},
			{
				Category:    "enrichment_ingestion",
				Label:       "Ingestion Decision",
				Description: "When a new memory looks like a near-duplicate, the model decides whether to add, update, delete, or skip. Off by default. Turn shadow mode on first to observe the decisions before acting on them.",
			},
			{
				Category:    "enrichment_performance",
				Label:       "Worker Performance",
				Description: "Throughput and concurrency for the enrichment worker pool, plus the model parameters used for fact and entity extraction. Also covers gone-worker recovery: heartbeat interval, stuck-job sweep cadence, and the staleness threshold past which an in-flight job is auto-requeued. Most fields hot-reload; the worker count, poll interval, heartbeat interval, and sweep interval need a restart.",
			},
			{
				Category:    "enrichment_query_augment",
				Label:       "Query Augmentation",
				Description: "Off by default. When enabled, the enrichment worker asks the LLM for N paraphrased queries per memory and prepends them to the content before embedding so a single vector captures both the fact and the ways someone would ask about it. After flipping the switch, use the Backfill Augmentation button to re-embed memories whose vector pre-dates the flag.",
			},
		},
	},
	{
		ID:                 "dreaming",
		Label:              "Dreaming",
		Description:        "Background consolidation that audits syntheses, reinforces confidence, and merges related memories.",
		RequiresEnrichment: true,
		SubSections: []api.SettingSubSection{
			{
				Category:    "dreaming",
				Label:       "General",
				Description: "Scheduler, token budgets, and the confidence floor for new syntheses.",
			},
			{
				Category:    "dreaming_novelty",
				Label:       "Novelty Audit",
				Description: "Discards syntheses that don't actually add anything new compared to the memories they were built from.",
			},
			{
				Category:    "dreaming_phase_budget",
				Label:       "Phase Budget Allocation",
				Description: "Reserve a fraction of the per-cycle token budget for each phase. Without reservations, an LLM-heavy phase running early can consume the entire envelope and starve later phases. SQL-only phases default to 0 (share the root); LLM phases default to a share that protects downstream synthesis.",
			},
			{
				Category:    "dreaming_consolidation",
				Label:       "Consolidation Budget",
				Description: "How the per-cycle token budget is split across the audit, reinforce, and consolidate sub-phases so none can starve the others.",
			},
			{
				Category:    "dreaming_contradiction",
				Label:       "Contradiction Detection",
				Description: "Per-cycle cap on the model calls used to find contradicting memory pairs, plus the confidence haircuts applied to winners, losers, and ties.",
			},
			{
				Category:    "dreaming_paraphrase",
				Label:       "Paraphrase Sweep",
				Description: "Catches near-duplicate memories the contradiction phase misses by running a vector similarity sweep directly on every eligible memory.",
			},
			{
				Category:    "dreaming_embedding_backfill",
				Label:       "Embedding Backfill",
				Description: "Repairs memories whose embedding row is missing. Re-embeds when the embedder is healthy; otherwise clears the orphan dimension marker.",
			},
			{
				Category:    "dreaming_performance",
				Label:       "Performance",
				Description: "How many neighbors to consider, how similar two entities must be to merge, and how often the scheduler wakes up.",
			},
		},
	},
	{
		ID:          "recall",
		Label:       "Recall & Ranking",
		Description: "How memories are scored, fused, and reinforced at retrieval time.",
		SubSections: []api.SettingSubSection{
			{
				Category:    "reconsolidation",
				Label:       "Reconsolidation",
				Description: "Each recall reinforces a memory's confidence; idle memories slowly decay during dream cycles.",
			},
			{
				Category:    "recall_fusion",
				Label:       "Hybrid Fusion",
				Description: "Run vector and lexical (BM25) search side by side and merge the results with Reciprocal Rank Fusion. Off by default. Turn on after migration 18 has been applied.",
			},
			{
				Category:    "ranking",
				Label:       "Ranking",
				Description: "Weights for the recall ranking formula: similarity, recency, importance, frequency, graph relevance, and confidence.",
			},
			{
				Category:    "recall",
				Label:       "Retrieval Limits",
				Description: "Default and maximum result counts, graph traversal depth and reserve, and the overfetch multipliers the recall endpoints use before ranking trims the set.",
			},
		},
	},
	{
		ID:          "api",
		Label:       "API",
		Description: "Public API rate limits, per-request caps, and graph defaults.",
		SubSections: []api.SettingSubSection{
			{
				Category:    "api",
				Label:       "General",
				Description: "Per-user rate limit and burst size for the public API.",
			},
			{
				Category:    "api_performance",
				Label:       "Performance",
				Description: "Rate-limiter cleanup cadence, batch-store item cap, and the default minimum edge weight for the graph endpoint. Advanced.",
			},
			{
				Category:    "mcp",
				Label:       "MCP",
				Description: "Caps for responses served over the Model Context Protocol (MCP) server surface.",
			},
		},
	},
	{
		ID:          "graph_visualization",
		Label:       "Graph Visualization",
		Description: "System-default d3-force parameters for the 3D entity graph (gravity, repulsion, link distance). Each project can override these from the Layout panel on the graph page; values here apply when no override is stored.",
		SubSections: []api.SettingSubSection{{Category: "graph_visualization"}},
	},
	{
		ID:          "auth",
		Label:       "Auth",
		Description: "Authentication and authorization.",
		SubSections: []api.SettingSubSection{{Category: "auth"}},
	},
	{
		ID:              "vector_db",
		Label:           "Vector Database",
		Description:     "Connection settings for the Qdrant vector database.",
		RequiresBackend: []string{"postgres"},
		SubSections:     []api.SettingSubSection{{Category: "qdrant"}},
	},
	{
		ID:              "hnsw",
		Label:           "Vector Index (HNSW)",
		Description:     "Pure-Go HNSW index used for semantic search when the database backend is SQLite. M and ef_construction are baked into each index at build time, so changes apply only to newly-built indexes; ef_search and the cache size apply at next boot.",
		RequiresBackend: []string{"sqlite"},
		SubSections:     []api.SettingSubSection{{Category: "hnsw"}},
	},
	{
		ID:          "lifecycle",
		Label:       "Lifecycle Sweep",
		Description: "Background sweep that expires time-to-live (TTL) memories, hard-purges soft-deleted ones past their retention window, and prunes orphaned graph data.",
		SubSections: []api.SettingSubSection{{Category: "lifecycle"}},
	},
	{
		ID:          "events",
		Label:       "Events & Streaming",
		Description: "Buffer sizes and keepalive timing for server-sent events (SSE) and the in-process event bus. Advanced: incorrect values can stall subscribers or grow memory unboundedly.",
		SubSections: []api.SettingSubSection{{Category: "events"}},
	},
	{
		ID:          "caches",
		Label:       "Service Caches",
		Description: "Cache lifetimes for the cascade resolver and settings service, plus the export pagination size.",
		SubSections: []api.SettingSubSection{{Category: "performance"}},
	},
	{
		ID:          "usage_export",
		Label:       "Usage & Export",
		Description: "Token-usage retention and cost accounting, plus where export artifacts are stored and how long they live.",
		SubSections: []api.SettingSubSection{
			{
				Category:    "usage",
				Label:       "Usage",
				Description: "Token-usage retention window and the per-model cost rates used for usage accounting.",
			},
			{
				Category:    "export",
				Label:       "Export",
				Description: "Where export artifacts are written, how long they are retained, and the per-user daily creation cap.",
			},
		},
	},
}
