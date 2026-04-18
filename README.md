# nram - Neural RAM

> **Work in Progress:** This project is under active development. Some features may be incomplete, unstable, or subject to change. Contributions and feedback are welcome, but expect rough edges as things continue to evolve.

Persistent memory layer for AI agents. Store, recall, and enrich memories with vector embeddings, knowledge graphs, and a built-in admin UI.

nram provides a self-hosted server that any AI agent can use to persist long-term memory across conversations. It supports semantic search via vector embeddings, automatic fact and entity extraction, a knowledge graph, and multi-tenant organization management - all accessible through a REST API, MCP (Model Context Protocol), or the built-in web UI.

## Features

- **Persistent Memory** - Store, retrieve, update, and soft-delete memories with tags, metadata, TTL, and supersession tracking
- **Semantic Search** - Vector embedding support via pgvector (PostgreSQL), pure-Go HNSW (SQLite), or Qdrant for similarity-based recall
- **Enrichment Pipeline** - Background workers extract facts, entities, and relationships from stored memories using configurable LLM providers
- **Knowledge Graph** - Automatically constructed from enriched entities and relationships with multi-hop traversal
- **Dreaming** - Offline background process with six phases: entity dedup, transitive-relationship inference, contradiction detection, consolidation, pruning (with optional confidence decay), and weight recalculation
- **Adaptive Confidence** - Optional reconsolidation hook on recall nudges `access_count`, `last_accessed`, and `confidence` on surfaced memories; the pruning phase applies a complementary confidence decay so unused memories fade over time. Shadow mode by default for observable-only rollout.
- **Model Context Protocol (MCP)** - Full MCP server at `/mcp` (Streamable HTTP) with 13 tools covering store, recall, update, get, list, forget, enrich, graph traversal, project management, and export
- **Authentication** - JWT (password login), per-user API keys, WebAuthn passkeys, and per-organization OIDC single sign-on
- **OAuth 2.0** - Authorization Code + PKCE, dynamic client registration (RFC 7591), resource indicators (RFC 8707), discovery metadata (RFC 8414, RFC 9728)
- **RBAC** - Five roles (administrator, org_owner, member, readonly, service) enforced across REST and MCP
- **Multi-Tenancy** - Organizations, hierarchical namespaces, and projects for memory isolation
- **Real-Time Events** - Server-Sent Events (SSE) with scope filtering and reconnection replay; webhook delivery with HMAC-SHA256 signatures
- **Admin UI** - React-based dashboard for managing organizations, users, projects, providers, enrichment, dreaming, OAuth clients, webhooks, SSO, database, and analytics
- **Dual Database Support** - SQLite (zero-config default) or PostgreSQL (with pgvector and LISTEN/NOTIFY); both support enrichment and knowledge graph
- **Migration Tooling** - SQLite-to-Postgres migration with preflight checks (connectivity, pgvector, privileges, target row counts), orphan audit against foreign-key relationships, and gated reset (truncate or drop-schema)
- **LLM Provider Agnostic** - OpenAI, Anthropic, Google Gemini, Ollama, OpenRouter, or any OpenAI-compatible endpoint
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

SQLite mode uses a pure-Go HNSW index for vector search and FTS5 for full-text search. Enrichment, knowledge graph, and all MCP tools are fully supported.

### PostgreSQL

Set `DATABASE_URL` or `database.url` in your config file:

```bash
DATABASE_URL=postgres://nram:password@localhost:5432/nram ./nram
```

PostgreSQL enables pgvector for semantic search and LISTEN/NOTIFY for multi-instance event propagation.

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
| `POST` | `/recall` | Semantic recall (vector + graph + ranking; fires reconsolidation) |
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
| `*` | `/settings` | Global settings (including reconsolidation tunables) |
| `*` | `/oauth/...` | OAuth client administration |
| `*` | `/webhooks/...` | Webhook registration and delivery audit |
| `*` | `/database/...` | Database info, test, preflight, migration audit, reset |

### MCP (Model Context Protocol)

The MCP server is available at `POST /mcp` using Streamable HTTP transport.

**Tools:**

| Tool | Description |
|---|---|
| `memory_store` | Store a single memory |
| `memory_store_batch` | Batch store memories |
| `memory_update` | Update a memory |
| `memory_get` | Retrieve a memory by ID |
| `memory_list` | List memories with filtering |
| `memory_recall` | Semantic search |
| `memory_forget` | Soft-delete a memory |
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
- Settings editor (including reconsolidation mode and decay tuning)
- Enrichment queue monitoring and retry
- Dreaming cycle inspection and manual triggers
- Knowledge graph visualization
- OAuth client management, webhook management, per-org OIDC SSO configuration
- Passkey management (per-user registration and removal)
- Database management (info, test, preflight, migration audit, reset)
- Token usage analytics and real-time activity feed

## Project Structure

```
cmd/server/          Server entrypoint
internal/
  api/               HTTP handlers
  auth/              OAuth 2.0, JWT, RBAC
  config/            Configuration loading
  dreaming/          Offline consolidation, dedup, pruning, and inference
  enrichment/        Background enrichment workers
  events/            Event bus, SSE, webhooks
  mcp/               MCP server and tool handlers
  migration/         Database migration runner
  model/             Data models
  provider/          LLM/embedding provider adapters
  server/            HTTP router setup
  service/           Business logic layer
  storage/           Database repositories
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
