# nram - Neural RAM

> **Work in Progress:** This project is under active development. Some features may be incomplete, unstable, or subject to change. Contributions and feedback are welcome, but expect rough edges as things continue to evolve.

Persistent memory layer for AI agents. Store, recall, and enrich memories with vector embeddings, knowledge graphs, and a built-in admin UI.

nram provides a self-hosted server that any AI agent can use to persist long-term memory across conversations. It supports semantic search via vector embeddings, automatic fact and entity extraction, a knowledge graph, and multi-tenant organization management - all accessible through a REST API, MCP (Model Context Protocol), or the built-in web UI.

## Features

- **Persistent Memory** - Store, retrieve, update, and soft-delete memories with tags, metadata, TTL, content-hash dedup-on-ingest, and supersession tracking. Superseded memories are hidden from list/recall/MCP results by default.
- **Hybrid Recall** - Parallel vector + lexical retrieval (FTS5 on SQLite, `tsvector`/`ts_rank_cd` on Postgres) fused with Reciprocal Rank Fusion. Off by default; flip `recall.fusion.enabled` once embeddings are populated.
- **Semantic Search** - Vector embedding support via pgvector (PostgreSQL), pure-Go HNSW (SQLite), or Qdrant. Embedding runs off the write path in the enrichment worker, so stores stay fast.
- **Enrichment Pipeline** - Background workers extract facts, entities, and relationships using configurable LLM providers. The first phase is an optional context-aware ingestion judge that decides ADD / UPDATE / DELETE / NONE on near-duplicate matches before extraction runs (shadow mode by default).
- **Knowledge Graph** - Automatically constructed from enriched entities and relationships with multi-hop traversal and entity-vector lookup
- **Dreaming** - Offline background consolidation cycle with eight phases: entity dedup, embedding backfill (repairs rows whose `embedding_dim` is recorded but whose vector row is missing — re-embeds when the provider is healthy, clears `embedding_dim` otherwise), paraphrase dedup, transitive-relationship inference, contradiction detection, consolidation, pruning (with optional confidence decay), and weight recalculation
- **Novelty Audit** - LLM-judged audit on dream syntheses; low-novelty consolidations are demoted, vectors are purged, and surfacing in recall is suppressed unless explicitly opted in
- **Adaptive Confidence** - Optional reconsolidation hook on recall nudges `access_count`, `last_accessed`, and `confidence` on surfaced memories; pruning applies a complementary confidence decay so unused memories fade over time. Shadow mode by default for observable-only rollout. `confidence` is one of six terms in the recall ranking score (similarity, recency, importance, frequency, graph relevance, confidence), each operator-tunable.
- **Per-Project Tuning** - System-level ranking weights, `dedup_threshold`, and `enrichment_enabled` cascade through optional per-user and per-project JSON overrides. Recall scores each candidate under its owning project's effective weights, so cross-project results (globals, shared namespaces) honor each row's owner's tuning. Sparse: unset fields fall through to system defaults.
- **Model Context Protocol (MCP)** - Full MCP server at `/mcp` (Streamable HTTP) with 13 tools covering store, recall (including tag-axis diversification), update, get, list, forget, enrich, graph traversal, project management, and export
- **Authentication** - JWT (password login), per-user API keys, WebAuthn passkeys, and per-organization OIDC single sign-on
- **OAuth 2.0** - Authorization Code + PKCE, dynamic client registration (RFC 7591), resource indicators (RFC 8707), discovery metadata (RFC 8414, RFC 9728)
- **RBAC** - Five roles (administrator, org_owner, member, readonly, service) enforced across REST and MCP
- **Multi-Tenancy** - Organizations, hierarchical namespaces, and projects for memory isolation
- **Real-Time Events** - Server-Sent Events (SSE) with scope filtering and reconnection replay; webhook delivery with HMAC-SHA256 signatures
- **Admin UI** - React-based dashboard for managing organizations, users, projects, providers, enrichment, dreaming, OAuth clients, webhooks, SSO, database, and analytics. Surfaces today's settings (fusion, ingestion-decision, novelty, reconsolidation) and per-provider token usage.
- **Dual Database Support** - SQLite (zero-config default) or PostgreSQL (with pgvector and LISTEN/NOTIFY); both support enrichment, dreaming, knowledge graph, and hybrid recall
- **Migration Tooling** - SQLite-to-Postgres migration with preflight checks (connectivity, pgvector, privileges, target row counts), orphan audit against foreign-key relationships, and gated reset (truncate or drop-schema)
- **LLM Provider Agnostic** - OpenAI, Anthropic, Google Gemini, Ollama, OpenRouter, or any OpenAI-compatible endpoint, with a centralized provider middleware that records token usage for every call
- **Import/Export** - JSON and NDJSON formats for full project snapshots
- **Prometheus Metrics** - `/metrics` endpoint for monitoring

## Quick Start

### Prerequisites

- Go 1.26+ (for building from source)
- Node.js 18+ (for building the admin UI)

### Build

```bash
# Full build (UI + server binary)
make build

# Or build components separately
make build-ui       # Build React UI and embed into Go binary
make build-server   # Compile Go server to ./nram

# Operator-only auxiliary binary (drains the dream novelty-audit backlog
# for a single project without going through the scheduler)
go build -o ./backfill-audit ./cmd/backfill-audit
```

### Run

```bash
# Start with defaults (SQLite, port 8674)
./nram

# Start with a config file
./nram --config config.yaml

# Start with environment variables
DATABASE_URL=postgres://user:pass@localhost/nram PORT=8674 ./nram
```

On first launch, navigate to `http://localhost:8674` to complete the setup wizard (create the initial admin account).

> **Required for enrichment, dreaming, and semantic recall:** nram ships with **no LLM provider configured**, so memories are stored as raw text only — embeddings, fact/entity extraction, the knowledge graph, dreaming consolidation, and the novelty audit are all **disabled** until you add a provider. After completing the setup wizard, open **Provider Config** in the admin UI and configure at minimum an **embedding** provider (for semantic search) and a **fact** + **entity** provider (for enrichment and dreaming). OpenAI, Anthropic, Google Gemini, Ollama, OpenRouter, or any OpenAI-compatible endpoint works. Provider changes hot-reload — no restart needed.

#### Operator Flags

| Flag | Description |
|---|---|
| `--config <path>` | Override the config file path |
| `--backfill-enrichment` | Enqueue enrichment jobs for memories missing vectors, then exit. Worker skips fact/entity extraction for memories whose lineage/relationships already exist, so re-running is cheap. |
| `--reembed-all-memories` | Force re-embed every live memory (e.g. after switching embedding models), then exit |
| `migrate up` / `migrate down` / `migrate version` | Migration CLI commands (run before normal startup) |

Setting `NRAM_ENABLE_ENRICHMENT_BACKFILL=1` runs the enrichment backfill at startup without forcing an exit.

> **Renamed:** the previous `NRAM_ENABLE_EMBED_BACKFILL` env var and `--backfill-embeddings` flag are no longer honored — update your deployment env. The flag and var were renamed alongside a fix that makes backfill skip fact/entity extraction for memories whose lineage and relationship rows already exist, so re-runs cost only the embed call.

## Configuration

nram loads configuration from (in order of precedence):

1. `--config` flag
2. `NRAM_CONFIG` environment variable
3. `config.yaml` in the working directory
4. Built-in defaults

### Config File

```yaml
server:
  host: "0.0.0.0"
  port: 8674

database:
  url: ""                    # PostgreSQL URL; empty = SQLite (nram.db)
  max_connections: 20
  migrate_on_start: true

log_level: "info"

admin:
  email: ""                  # Initial admin email (or use setup wizard)
  password: ""               # Initial admin password

# Embedding provider (required for semantic search)
embed:
  provider: ""               # openai, anthropic, gemini, ollama, openrouter
  url: ""                    # Custom base URL (optional)
  key: ""                    # API key
  model: ""                  # Model name

# Fact extraction provider (optional)
fact:
  provider: ""
  url: ""
  key: ""
  model: ""

# Entity extraction provider (optional)
entity:
  provider: ""
  url: ""
  key: ""
  model: ""

# External vector database (optional)
qdrant:
  addr: ""                   # gRPC address, e.g. localhost:6334

# Pure-Go HNSW vector index settings (SQLite backend)
hnsw:
  m: 16                      # Max neighbors per layer
  ef_construction: 200       # Construction candidate pool size
  ef_search: 50              # Search candidate pool size
  max_loaded_indexes: 64     # Max in-memory indexes before LRU eviction
```

YAML values support environment variable interpolation: `${VAR_NAME:-default}`.

Most runtime knobs (ranking weights, recall fusion weights, ingestion-decision thresholds, novelty audit, reconsolidation, dreaming budgets, retention, prompts) are stored in the `settings` table and edited at `/v1/admin/settings` (or in the admin UI). `config.yaml` provides bootstrap defaults and provider credentials; persisted settings always win at runtime so operators do not need to redeploy to retune.

Per-project and per-user overrides for `ranking_weights`, `dedup_threshold`, and `enrichment_enabled` live on the project and user records as sparse JSON. The cascade is `system → user → project → effective`; unset fields fall through. Edit at `/v1/me/projects/{id}` (project) or `/v1/admin/users/{id}` (user). User-scope `ranking_weights` is rejected with a 400 — the cascade for weights lands at project, not user.

### Environment Variables

| Variable | Description |
|---|---|
| `PORT` | Server port (default: 8674) |
| `DATABASE_URL` | PostgreSQL connection string |
| `LOG_LEVEL` | Log level: debug, info, warn, error |
| `NRAM_ADMIN_EMAIL` | Initial admin email |
| `NRAM_ADMIN_PASS` | Initial admin password |
| `NRAM_EMBED_PROVIDER` | Embedding provider name |
| `NRAM_EMBED_URL` | Embedding provider base URL |
| `NRAM_EMBED_KEY` | Embedding provider API key |
| `NRAM_EMBED_MODEL` | Embedding model name |
| `NRAM_FACT_PROVIDER` | Fact extraction provider |
| `NRAM_FACT_KEY` | Fact extraction API key |
| `NRAM_FACT_MODEL` | Fact extraction model |
| `NRAM_ENTITY_PROVIDER` | Entity extraction provider |
| `NRAM_ENTITY_KEY` | Entity extraction API key |
| `NRAM_ENTITY_MODEL` | Entity extraction model |

## Database

### SQLite (Default)

No configuration required. Creates `nram.db` in the working directory with WAL mode, foreign keys, and FTS5 full-text search.

SQLite mode uses a pure-Go HNSW index for vector search and FTS5 for the lexical channel of hybrid recall. Enrichment, dreaming, knowledge graph, and all MCP tools are fully supported.

### PostgreSQL

Set `DATABASE_URL` or `database.url` in your config file:

```bash
DATABASE_URL=postgres://nram:password@localhost:5432/nram ./nram
```

PostgreSQL enables pgvector for semantic search, a generated `content_tsv` column with `ts_rank_cd` for the lexical channel of hybrid recall, and LISTEN/NOTIFY for multi-instance event propagation.

### Qdrant (Optional)

For dedicated vector search, configure Qdrant as an alternative to pgvector:

```yaml
qdrant:
  addr: "localhost:6334"
```

### Migrations

Migrations run automatically on startup when `migrate_on_start: true` (the default). Manual control:

```bash
./nram migrate up       # Apply pending migrations
./nram migrate down     # Roll back one migration
./nram migrate version  # Show current migration version
```

## API

An OpenAPI 3.1.0 specification lives at [`docs/openapi.yaml`](docs/openapi.yaml). It may lag the code — the tables below reflect the current router source of truth.

### Authentication

All authenticated API requests carry a Bearer token via the `Authorization` header. A token can be:

- A JWT obtained from `POST /v1/auth/login` (password) or the passkey / OIDC flows
- A per-user API key generated via `/v1/me/api-keys` (prefix `nram_k_`)
- An OAuth 2.0 access token from `/token`

Setup-guarded routes return 503 until the initial admin has been created via the setup wizard.

### Health & Observability

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/health` | Health check |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/v1/events` | Server-Sent Events stream (scope filter + replay) |

### Login & Account Bootstrap

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/admin/setup/status` | Whether the initial admin has been provisioned |
| `POST` | `/v1/admin/setup` | Complete first-run setup (creates the administrator) |
| `POST` | `/v1/auth/lookup` | Resolve an email to its available login methods |
| `POST` | `/v1/auth/login` | Password login → JWT |
| `POST` | `/v1/auth/passkey/begin` / `/finish` | WebAuthn login challenge + completion |
| `GET` | `/auth/idp/login` / `/auth/idp/callback` | Per-organization OIDC single sign-on |

### OAuth 2.0

| Path | Description |
|---|---|
| `/.well-known/oauth-authorization-server` | Authorization server metadata (RFC 8414) |
| `/.well-known/oauth-protected-resource` | Protected resource metadata (RFC 9728) |
| `/authorize` | Authorization endpoint (PKCE required) |
| `/token` | Token endpoint |
| `/register` | Dynamic client registration (RFC 7591) |
| `/userinfo` | OpenID userinfo endpoint |

### Memories (project-scoped)

All under `/v1/projects/{project_id}/memories`. Read operations are available to any authenticated role; write operations require non-readonly.

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | List memories (filters: tags, date range, source, search, enriched) |
| `GET` | `/ids` | List matching memory IDs (for "select all") |
| `GET` | `/{id}` | Get a memory by ID |
| `POST` | `/get` | Batch-get by ID list |
| `POST` | `/recall` | Hybrid recall (vector + optional BM25/tsvector + graph + ranking; fires reconsolidation) |
| `GET` | `/export` | Export as JSON / NDJSON |
| `POST` | `/` | Store a memory |
| `PUT` | `/{id}` | Update a memory |
| `DELETE` | `/{id}` | Soft-delete a memory |
| `POST` | `/batch` | Batch store |
| `POST` | `/forget` | Bulk soft-delete |
| `POST` | `/enrich` | Trigger enrichment |
| `POST` | `/import` | Import a project snapshot |

### User Self-Service

All under `/v1/me`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/memories/recall` | Cross-project recall for the current user |
| `GET` / `POST` | `/projects` | List or create projects owned by the user |
| `GET` / `PUT` / `DELETE` | `/projects/{id}` | Manage a specific project |
| `GET` / `POST` | `/api-keys` | List or mint API keys |
| `DELETE` | `/api-keys/{id}` | Revoke an API key |
| `GET` / `POST` | `/oauth-clients` | List or register OAuth clients |
| `DELETE` | `/oauth-clients/{id}` | Revoke an OAuth client |
| `POST` | `/password` | Change password |
| `GET` | `/passkeys` | List registered passkeys |
| `POST` | `/passkeys/register/begin` / `/finish` | Register a new passkey |
| `DELETE` | `/passkeys/{id}` | Remove a passkey |

### Scoped Views (authenticated; results scoped to caller's role)

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/dashboard` | Counts and headline metrics |
| `GET` | `/v1/activity` | Recent memory activity |
| `GET` | `/v1/analytics` | Memory, recall, and enrichment analytics |
| `GET` | `/v1/usage` | Token usage aggregation |
| `GET` | `/v1/graph` | Knowledge graph data |
| `GET` | `/v1/namespaces/tree` | Namespace hierarchy |
| `*` | `/v1/enrichment/...` | Enrichment queue monitoring and retry |
| `*` | `/v1/dreaming/...` | Dream cycle inspection and triggers |

### Organization Management

All under `/v1/orgs/{org_id}`, gated by org membership.

| Method | Path | Description |
|---|---|---|
| `GET` | `/analytics` / `/usage` | Org-scoped views (member+) |
| `*` | `/users/...` | Manage users in the org (org_owner+) |
| `*` | `/idp/...` | Manage per-org OIDC configuration (org_owner+) |

### Administration

All under `/v1/admin`, gated by `administrator` role.

| Method | Path | Description |
|---|---|---|
| `*` | `/orgs/...` | Organization CRUD |
| `*` | `/users/...` | Global user CRUD |
| `*` | `/projects/...` | Global project CRUD |
| `*` | `/providers/...` | LLM / embedding provider configuration |
| `*` | `/settings` | Global settings (ranking weights, recall fusion, ingestion decision, novelty audit, reconsolidation, dreaming budgets, retention, prompts) |
| `*` | `/oauth/...` | OAuth client administration |
| `*` | `/webhooks/...` | Webhook registration and delivery audit |
| `*` | `/database/...` | Database info, test, preflight, migration audit, reset |

### MCP (Model Context Protocol)

The MCP server is available at `POST /mcp` using Streamable HTTP transport.

**Tools:**

| Tool | Description |
|---|---|
| `memory_store` | Store a single memory. Identical content within the same project is deduplicated on ingest — the existing memory's ID is returned and tags / metadata on the new request are ignored. |
| `memory_store_batch` | Batch store memories (same dedup-on-ingest behavior) |
| `memory_update` | Update a memory |
| `memory_get` | Retrieve a memory by ID |
| `memory_list` | List memories with filtering. Superseded rows are hidden by default. |
| `memory_recall` | Hybrid (vector + lexical) recall with optional `diversify_by_tag_prefix` for round-robin coverage across a tag axis |
| `memory_forget` | Soft-delete a memory; cascades restricted to extraction lineage |
| `memory_enrich` | Trigger enrichment |
| `memory_graph` | Knowledge graph traversal |
| `memory_projects` | List projects |
| `memory_update_project` | Update a project |
| `memory_delete_project` | Delete a project |
| `memory_export` | Export project data |

**Resources:**

| URI | Description |
|---|---|
| `nram://projects` | List all projects |
| `nram://projects/{slug}/entities` | Entities in a project |
| `nram://projects/{slug}/graph` | Knowledge graph data |

## Admin UI

The embedded web UI is served at the root path (`/`). It provides:

- Setup wizard for initial configuration
- Organization and user management
- Project management
- LLM / embedding provider configuration with hot-reload
- Settings editor (ranking weights, recall fusion weights, ingestion decision, novelty audit, reconsolidation mode and decay, dreaming budgets and retention, prompts)
- Project edit panel with sparse per-project override editor (six ranking weights, dedup threshold, enrichment toggle) — empty fields inherit system defaults; effective merged weights and sum displayed inline
- Memory detail panel surfaces `confidence`, `importance`, `access_count`, and `last_accessed` so operators can verify reinforcement and decay are moving the values
- Enrichment queue monitoring and retry; ingestion-decision shadow vs persist toggle
- Dreaming cycle inspection, log replay, manual triggers, and rollback
- Memory browser with parent / enrichment-child grouping
- Knowledge graph visualization
- OAuth client management, webhook management, per-org OIDC SSO configuration
- Passkey management (per-user registration and removal)
- Database management (info, test, preflight, migration audit, reset)
- Token usage analytics (per-provider, per-model, per-tenant) and real-time activity feed

## Project Structure

```
cmd/
  server/            Server entrypoint
  backfill-audit/    Operator tool: drains the dream novelty-audit backlog
                     for a single project without going through the scheduler
internal/
  api/               HTTP handlers (REST + admin)
  auth/              OAuth 2.0, JWT, WebAuthn, RBAC
  config/            Configuration loading
  dreaming/          Offline consolidation cycle (entity dedup, embedding
                     backfill, paraphrase dedup, transitive inference,
                     contradiction, consolidation, pruning, weight adjustment)
                     with rollback and retention sweeps
  enrichment/        Background enrichment worker pool, context-aware ingestion
                     decision, dedup, conflict resolution, re-embed
  events/            Event bus, SSE, webhooks
  mcp/               MCP server and tool handlers
  migration/         Database migration runner
  model/             Data models
  provider/          LLM / embedding provider adapters with token-usage middleware
  server/            HTTP router setup
  service/           Business logic layer (recall, store, fusion, settings, lifecycle)
  storage/           Database repositories (incl. HNSW, pgvector, Qdrant adapters)
  ui/                Embedded React UI assets
migrations/
  sqlite/            SQLite migration SQL files
  postgres/          PostgreSQL migration SQL files
ui/                  React admin UI source (TypeScript, Tailwind)
docs/                OpenAPI specification
```

## Development

```bash
# Install UI dependencies
make install-ui

# Run React dev server (hot-reload on port 5173)
make dev

# Build everything
make build

# Run the server
./nram --config config.yaml
```

## License

MIT
