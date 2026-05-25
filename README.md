# nram: Neural RAM

> **Work in Progress:** This project is under active development. Some features may be incomplete, unstable, or subject to change. Contributions and feedback are welcome, but expect rough edges as things continue to evolve.

A self-hosted, long-term memory layer that any LLM-using tool can plug into. Store, recall, and enrich memories with vector embeddings, knowledge graphs, sleep-style consolidation, and a built-in admin UI.

## What is nram?

nram is a long-term memory layer that any LLM-using tool can plug into. AI assistants and chat interfaces forget everything between conversations; reference libraries (PDFs, textbooks, research notes, project decisions) sit in folders nobody searches; the tools you use day to day (Claude, ChatGPT, Cursor, Obsidian, custom apps) each invent their own half-baked memory and none of them talk to each other. nram is the shared substrate underneath all of that. You run it on your own machine or server, and everything else connects in.

nram serves four overlapping memory shapes, each one a different use of the same store:

| Use case | What nram provides | Comparable tools |
|---|---|---|
| Conversational memory | MCP backend that survives across sessions, tools, and vendors. The session-binding behavior is a prompt-side job (see procedural and behavioral memory below). | Claude Memory, ChatGPT Memory |
| Document-corpus memory | Semantic search, entity-deduped knowledge graph, and consolidation over a stored corpus. Substrate, not a chat UI. | NotebookLM, AnythingLLM, Khoj |
| Procedural and behavioral memory | Stored rules, conventions, named failure modes, and standing protocols that an assistant pulls at session start. nram ships the substrate; the contract content is the operator's work. Convention: keep it in a project named `behavioral-contract`. | (no direct equivalent) |
| Agent memory | Persistent memory for coding, research, and custom agents, with sleep-style consolidation and a knowledge graph on top. | Mem0, Letta, Zep, Graphiti |

What makes it more than a database with vector search: semantic recall (find by meaning, not by keyword) plus automatic fact and entity extraction (the system pulls structured information out of your free-text memories) plus a knowledge graph (entities and relationships built up over time). On top of all of that, a *dreaming* cycle runs in the background. It dedups near-duplicates, detects contradictions between memories, consolidates related memories into higher-level summaries, and prunes the stuff you never use. Think of it as a notebook that quietly reorganizes itself overnight.

How things connect: **MCP** is the standard way Claude, ChatGPT, Cursor, or a custom agent plugs in tools. Streamable HTTP transport at `/mcp`, OAuth discovery published at the well-known paths. The **REST API** lets any code that can speak HTTP store and recall memories. The **web UI** is the dashboard for managing organizations, projects, providers, the knowledge graph, and the dreaming cycle.

## Features

- **Persistent Memory**: store, retrieve, update, and soft-delete memories with tags, metadata, TTL, content-hash dedup-on-ingest, and supersession tracking. Superseded memories are hidden from list/recall/MCP results by default.
- **Hybrid Recall**: parallel vector + lexical retrieval (FTS5 on SQLite, `tsvector`/`ts_rank_cd` on Postgres) fused with Reciprocal Rank Fusion. Off by default; flip `recall.fusion.enabled` once embeddings are populated.
- **Semantic Search**: vector embedding support via pgvector (PostgreSQL), pure-Go HNSW (SQLite), or Qdrant. Embedding runs off the write path in the enrichment worker, so stores stay fast.
- **Enrichment Pipeline**: background workers extract facts, entities, and relationships using configurable LLM providers. The first phase is an optional context-aware ingestion judge that decides ADD / UPDATE / DELETE / NONE on near-duplicate matches before extraction runs (shadow mode by default).
- **Query Augmentation**: optional enrichment phase that paraphrases each memory into a small set of short retrieval queries via the configured LLM and embeds them upstream of the original content, so natural-language recall matches the way users actually ask. Off by default behind `enrichment.query_augment.enabled`; tunable count, model override, prompt, and input cap; standalone backfill against existing memories via the admin endpoint `POST /enrichment/backfill-augmentation`.
- **Knowledge Graph**: automatically constructed from enriched entities and relationships with multi-hop traversal and entity-vector lookup.
- **Dreaming**: offline background consolidation cycle with eight phases. Entity dedup, embedding backfill (repairs rows whose `embedding_dim` is recorded but whose vector row is missing; re-embeds when the provider is healthy, clears `embedding_dim` otherwise), paraphrase dedup, transitive-relationship inference, contradiction detection, consolidation, pruning (with optional confidence decay), and weight recalculation.
- **Novelty Audit**: LLM-judged audit on dream syntheses; low-novelty consolidations are demoted, vectors are purged, and surfacing in recall is suppressed unless explicitly opted in.
- **Adaptive Confidence**: optional reconsolidation hook on recall nudges `access_count`, `last_accessed`, and `confidence` on surfaced memories; pruning applies a complementary confidence decay so unused memories fade over time. Shadow mode by default for observable-only rollout. `confidence` is one of six terms in the recall ranking score (similarity, recency, importance, frequency, graph relevance, confidence), each operator-tunable.
- **MMR Rerank**: Maximal Marginal Relevance reorder of recall candidates after composite scoring, so near-duplicate clusters that paraphrase the same fact get demoted in favor of orthogonal results. Cosine-redundancy aware via upstream vector hydration (`VectorHydrator` interface backed by HNSW, pgvector, and Qdrant); per-project `mmr_lambda` (default 0.75) through the same ranking-weights cascade; edge values short-circuit to composite-order truncation; lexical-only and unbackfilled rows stay anchored to their composite-rank position.
- **Per-Project Tuning**: system-level ranking weights, `dedup_threshold`, and `enrichment_enabled` cascade through optional per-user and per-project JSON overrides. Recall scores each candidate under its owning project's effective weights, so cross-project results (globals, shared namespaces) honor each row's owner's tuning. Sparse: unset fields fall through to system defaults.
- **Model Context Protocol (MCP)**: full MCP server at `/mcp` (Streamable HTTP) with 13 tools covering store, recall (including tag-axis diversification), update, get, list, forget, enrich, graph traversal, project management, and export.
- **Authentication**: JWT (password login), per-user API keys, WebAuthn passkeys, and per-organization OIDC single sign-on.
- **OAuth 2.0**: Authorization Code + PKCE, dynamic client registration (RFC 7591), resource indicators (RFC 8707), discovery metadata (RFC 8414, RFC 9728).
- **RBAC**: five roles (administrator, org_owner, member, readonly, service) enforced across REST and MCP.
- **Multi-Tenancy**: organizations, hierarchical namespaces, and projects for memory isolation.
- **Real-Time Events**: Server-Sent Events (SSE) with scope filtering and reconnection replay; webhook delivery with HMAC-SHA256 signatures.
- **Admin UI**: React-based dashboard for managing organizations, users, projects, providers, enrichment, dreaming, OAuth clients, webhooks, SSO, database, and analytics. Surfaces today's settings (fusion, ingestion-decision, novelty, reconsolidation) and per-provider token usage.
- **Dual Database Support**: SQLite (zero-config default) or PostgreSQL (with pgvector and LISTEN/NOTIFY). Both support enrichment, dreaming, knowledge graph, and hybrid recall.
- **Migration Tooling**: SQLite-to-Postgres migration with preflight checks (connectivity, pgvector, privileges, target row counts), orphan audit against foreign-key relationships, and gated reset (truncate or drop-schema).
- **LLM Provider Agnostic**: OpenAI, Anthropic, Google Gemini, Ollama, OpenRouter, or any OpenAI-compatible endpoint, with a centralized provider middleware that records token usage for every call.
- **Import/Export**: JSON and NDJSON formats for full project snapshots.
- **Prometheus Metrics**: `/metrics` endpoint for monitoring.

## Quick Start

> **Read this before starting.** Most setup issues trace back to one of these
> three points:
>
> - **nram needs an LLM provider to do anything beyond storing raw text.**
>   Without one, semantic search falls back to keyword-only matches and the
>   knowledge graph stays empty. No error is raised; the system runs in a
>   degraded mode.
> - **The setup wizard does not configure providers.** Providers are configured
>   on the **Provider Config** page after the wizard finishes. This is the most
>   common cause of "search doesn't work" reports.
> - **Pick the right embedding model.** Ollama's commonly-suggested
>   `nomic-embed-text` has a 2048-token context window. Memories longer than
>   that are silently truncated before embedding. See
>   [Recommended Models](#recommended-models) below for alternatives.

### Step 1: Install prerequisites

Versions, with a one-liner check for each:

| Tool | Required | Check |
|---|---|---|
| Go | 1.26+ | `go version` |
| Node.js | 18+ | `node --version` |
| npm | any (build uses `npm ci`, **not** pnpm or yarn) | `npm --version` |
| Ollama | optional, for local LLMs | `ollama --version`. Skip if you're using OpenAI / Anthropic / Gemini / OpenRouter |

### Step 2: Build

```bash
git clone <repo-url> nram && cd nram
make build
```

Output: a single binary `./nram` with the React UI embedded. To build the UI
and server separately:

```bash
make build-ui       # Build React UI and embed into Go binary
make build-server   # Compile Go server to ./nram
```

### Step 3: Run

```bash
./nram
```

Defaults: port **8674**, database **SQLite** (`nram.db` in the working directory).
To use Postgres, set `DATABASE_URL` and restart:

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/nram ./nram
```

### Step 4: Open the setup wizard

Navigate to `http://localhost:8674`. Create the initial admin account. **Save the
API key shown on the completion screen. It is not shown again.**

### Step 5: Configure an LLM provider (required)

Open **Settings → Providers** in the admin UI. Configure at minimum:

- **Embedding** slot: for semantic search
- **Fact Extraction** slot: for the knowledge graph and dreaming
- **Entity Extraction** slot: for the knowledge graph and dreaming

See [Recommended Models](#recommended-models) below for what to put in each slot.
Any of OpenAI, Anthropic (chat slots only; Anthropic does not offer embeddings),
Google Gemini, Ollama, OpenRouter, or any OpenAI-compatible endpoint works.
Provider changes hot-reload; no restart needed.

Without configured providers, recall falls back to keyword-only matches, the
knowledge graph stays empty, and enrichment jobs queue without running. No
warning is raised. This is the intended behavior when providers are absent,
not a bug.

### Step 6: Verify

```bash
curl http://localhost:8674/v1/health
```

All three providers should report `"status": "ok"`. Example healthy response:

```json
{
  "status": "ok",
  "backend": "sqlite",
  "providers": {
    "embedding":        { "status": "ok", "provider": "ollama" },
    "fact_extraction":  { "status": "ok", "provider": "ollama", "model": "qwen3:8b" },
    "entity_extraction":{ "status": "ok", "provider": "ollama", "model": "qwen3:8b" }
  },
  "enrichment_queue": { "pending": 0, "processing": 0, "failed": 0 }
}
```

If any provider slot is missing or reports a status other than `ok`, resolve
it before storing memories. Memories stored without working providers will not
be embedded or enriched, and re-embedding via `--reembed-all-memories` will be
required afterward.

### Step 7: Connect a client (MCP)

For Claude Code:

```bash
claude mcp add --transport http nram http://localhost:8674/mcp
```

For Claude Desktop, ChatGPT, Cursor, or any other MCP client: point the client at
`http://localhost:8674/mcp`. OAuth discovery metadata is published at
`/.well-known/oauth-authorization-server` and `/.well-known/oauth-protected-resource`;
the client negotiates a token from there.

For tools that don't support OAuth, use the API key from Step 4 as a Bearer token.

### Operator Flags

| Flag | Description |
|---|---|
| `--config <path>` | Override the config file path |
| `--backfill-enrichment` | Enqueue enrichment jobs for memories missing vectors, then exit. Worker skips fact/entity extraction for memories whose lineage/relationships already exist, so re-running is cheap. |
| `--reembed-all-memories` | Force re-embed every live memory (e.g. after switching embedding models), then exit |
| `migrate up` / `migrate down` / `migrate version` | Migration CLI commands (run before normal startup) |

Setting `NRAM_ENABLE_ENRICHMENT_BACKFILL=1` runs the enrichment backfill at startup without forcing an exit.

> **Renamed:** the previous `NRAM_ENABLE_EMBED_BACKFILL` env var and `--backfill-embeddings` flag are no longer honored; update your deployment env. The flag and var were renamed alongside a fix that makes backfill skip fact/entity extraction for memories whose lineage and relationship rows already exist, so re-runs cost only the embed call.

### Troubleshooting

**"I stored memories but recall returns nothing relevant, only literal keyword
matches."** No embedding provider configured, or the configured embedding provider
is unhealthy. Check `curl /v1/health` → `providers.embedding.status` should be
`ok`. Without a working embedding provider, recall falls back to lexical-only
(BM25 / FTS5). This is by design, but can be unexpected. Fix: configure an
embedding provider in Step 5, then run
`./nram --backfill-enrichment` once to embed the memories you stored before
configuring it.

**"The Enrichment Queue page shows jobs, but the count never goes down."**
Fact-extraction or entity-extraction provider not configured. The worker claims
each job, sees the provider registry is incomplete, and silently re-releases the
job. No failure row is recorded, and no error log appears unless logging is at
INFO or higher. Fix: configure both fact and entity slots in Step 5.

**"I changed `embed.url` / `qdrant.addr` / `NRAM_FACT_*` / `NRAM_EMBED_*` in
config.yaml or my env and the change didn't take."** Those keys were removed
2026-04-30 and are silently ignored (with a WARN log only). All provider, vector,
fact, and entity settings live in the database now and are managed at
`/admin/providers` and `/admin/settings`. Provider changes hot-reload; no
restart needed.

**"My recall quality got worse after a long memory ingest."** Likely the
`nomic-embed-text` 2048-token context limit. See
[Recommended Models](#recommended-models) below. Switch to
`qwen3-embedding:0.6b` (or another long-context embedding model) and run
`./nram --reembed-all-memories` once to re-embed your existing memories with the
new model.

## Recommended Models

nram is provider-agnostic, but the choice of **embedding model** in particular has
a big effect on recall quality. Three tiers below; pick one and move on.

### Tier 1: Lite (fits on a laptop, slow but works)

| Slot | Model | Where | Notes |
|---|---|---|---|
| Embedding | `qwen3-embedding:0.6b` | Ollama | ~600M params, ~1.2GB on disk |
| Fact | `qwen3:4b` | Ollama | 4B params, ~2.5GB on disk, Q4_K_M |
| Entity | `qwen3:4b` | Ollama | Same model is fine for both extraction slots |

### Tier 2: Recommended (the configuration nram's author runs)

| Slot | Model | Where | Notes |
|---|---|---|---|
| Embedding | `qwen3-embedding:0.6b` (with bumped `num_ctx`) | Ollama | Trained at 32K context. Bump Ollama's default `num_ctx` of 2048 via a Modelfile (see below) to use the full trained context |
| Fact | `qwen3:8b` | Ollama | 8.2B params, ~5.2GB on disk, Q4_K_M quantization |
| Entity | `qwen3:8b` | Ollama | Same model |

### Tier 3: Cloud (no local GPU needed)

| Slot | Model | Where | Notes |
|---|---|---|---|
| Embedding | `text-embedding-3-small` | OpenAI | 8K context, 1536 dims |
| Fact | `gpt-4o-mini` *or* `claude-haiku-4-5-20251001` | OpenAI / Anthropic | Hosted; charges per token |
| Entity | `gpt-4o-mini` *or* `claude-haiku-4-5-20251001` | OpenAI / Anthropic | Same model |

> Anthropic does **not** offer an embeddings API. If you want Claude for fact /
> entity extraction, pair it with OpenAI or Ollama for the embedding slot.

### Why not `nomic-embed-text`?

`nomic-embed-text` is a commonly suggested Ollama embedding model, but it has
a limitation worth knowing about before choosing it:

- `nomic-embed-text` has a **2048-token training context**.
- Ollama's default `num_ctx` is also 2048, so anything past roughly 1500 words
  of a memory is truncated before embedding.
- nram does not pre-truncate the text or surface a warning. Ollama returns a
  vector computed from the truncated prefix and nram stores it as if it
  represented the whole memory.
- Result: long memories are embedded as if they were short. Recall quality
  degrades silently. No error surfaces, but longer memories produce
  progressively worse results.

Using `qwen3-embedding:0.6b` (or any embedding model with a longer trained
context) avoids this issue.

### Bumping `num_ctx` for Ollama embeddings

By default, Ollama caps context at 2048 tokens regardless of what the underlying
model was trained for. To actually use `qwen3-embedding:0.6b`'s 32K trained
context, create a Modelfile that pins a larger `num_ctx`:

```
FROM qwen3-embedding:0.6b
PARAMETER num_ctx 8192
```

```bash
ollama create qwen3-embedding-8k -f Modelfile
```

Then point nram's embedding slot at `qwen3-embedding-8k` instead of the base
`qwen3-embedding:0.6b` tag. 8K is a reasonable default; raise it further if your
memories are long-form documents and you have the VRAM.

### Embedding dimensions

You do **not** need to enter the embedding model's dimension count. nram
auto-detects dimensions on the first call to a new embedding provider by sending
a probe string and reading the response shape. The detected dimension count is
displayed in the provider status read-back after the first successful call.

## Configuration

nram has two configuration surfaces:

- **Bootstrap config** (this file / env vars): the small set of values needed
  before the database is open. Listener, DSN, log level, and the optional
  headless admin credentials. Any change requires a restart.
- **Runtime config** (admin UI / `/v1/admin/settings`): everything else.
  Providers, vector backends, dreaming, ranking, retention, prompts.
  Stored in the `settings` table and (mostly) hot-reloadable.

The loader reads (in order of precedence):

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

# Headless administrator bootstrap. When both fields are set AND no users
# exist in the database, the first administrator is created on startup,
# bypassing the setup wizard. Otherwise these values are ignored.
admin:
  email: ""
  password: ""
```

YAML values support environment variable interpolation: `${VAR_NAME:-default}`.

> **Removed surface (2026-04-30):** `embed.*`, `fact.*`, `entity.*`, `qdrant.*`,
> `hnsw.*`, and `enrichment_orphan_grace_seconds` are no longer accepted in
> `config.yaml`. Likewise `NRAM_EMBED_*`, `NRAM_FACT_*`, `NRAM_ENTITY_*`, and
> `NRAM_ENRICHMENT_ORPHAN_GRACE_SECONDS` are no longer accepted as env vars.
> All of these are now managed exclusively at runtime through the admin UI
> at `/admin/settings` (or the `/v1/admin/settings` API). The loader logs a
> WARN line for each deprecated key it sees and ignores the value.

### Runtime Configuration

Everything outside the bootstrap surface is managed through the admin UI:

- **Providers** (embedding, fact extraction, entity extraction): `/admin/providers`
- **Vector backend** (Qdrant address/credentials, HNSW tuning): `/admin/settings`
- **Dreaming**, **enrichment**, **ranking**, **recall fusion**, **reconsolidation**,
  **retention**, **rate limits**, **lifecycle sweep**, **events**, and prompt
  templates: `/admin/settings`

Per-project and per-user overrides for `ranking_weights`, `dedup_threshold`,
and `enrichment_enabled` live on the project and user records as sparse JSON.
The cascade is `system → user → project → effective`; unset fields fall
through. Edit at `/v1/me/projects/{id}` (project) or `/v1/admin/users/{id}`
(user). User-scope `ranking_weights` is rejected with a 400; the cascade
for weights lands at project, not user.

### Environment Variables

| Variable | Description |
|---|---|
| `PORT` | Server port (default: 8674) |
| `DATABASE_URL` | PostgreSQL connection string |
| `LOG_LEVEL` | Log level: debug, info, warn, error |
| `NRAM_CONFIG` | Path to a config file (alternative to `--config`) |
| `NRAM_ADMIN_EMAIL` | Headless bootstrap administrator email (first boot only) |
| `NRAM_ADMIN_PASS` | Headless bootstrap administrator password (first boot only) |

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

An OpenAPI 3.1.0 specification lives at [`docs/openapi.yaml`](docs/openapi.yaml). It may lag the code; the tables below reflect the current router source of truth.

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

#### Update semantics: in-place vs. supersede chain

`PUT /{id}` and the MCP `memory_update` tool branch on whether the request changes content:

- **Tag-only or metadata-only updates** mutate the row in place. The response `id` matches the path id.
- **Content updates** create a NEW memory row and mark the old row `superseded_by = new id`. The response `id` is the new (active) id; `previous_memory_id` echoes the path id. Recall, list, and graph reads filter superseded rows by default, so the new id is what surfaces; `include_superseded=true` is required to access prior versions. Old enrichment (entities, relationships, embedding, accumulated weights from recall reinforcement) stays attached to the old id, frozen with the old content. Dream pruning eventually sweeps superseded rows after a 7-day grace window.

Forget on the active head walks `superseded_by` and soft-deletes the entire chain; forgetting one memory thread forgets it through all its prior versions. Pass `hard_delete=true` to bypass the soft delete; the chain walk runs in either mode.

This is a breaking change from the prior in-place semantics: callers that hold the path id for follow-up reads must use `response.id` instead. Subscribers to `memory.updated` events should likewise read `memory_id` (active) and treat `previous_memory_id` as the correlation key.

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
| `memory_store` | Store a single memory. Identical content within the same project is deduplicated on ingest; the existing memory's ID is returned and tags / metadata on the new request are ignored. |
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
- Project edit panel with sparse per-project override editor (six ranking weights, dedup threshold, enrichment toggle); empty fields inherit system defaults, effective merged weights and sum displayed inline
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

## FAQ

### Dreaming backlog

**How do I clear my dreaming backlog quickly?**

Three knobs do the heavy lifting. Raise `dreaming.max_tokens_per_cycle` (default `1024000`) by 2x to 3x so each cycle does more work end to end. Lower `dreaming.min_interval_seconds` (default `600`) toward `120` so cycles fire more often per project. Lower `dreaming.cooldown_seconds` (default `300`) if you want the scheduler to start a cycle sooner after the last write. All three live at `/admin/settings` and hot-reload per cycle, no restart needed. Trade-off: sustained higher LLM token spend, and aggressive settings will hit provider rate limits faster, so raise gradually and watch token usage at `/admin/usage`.

**How do I drain the novelty-audit backlog without waiting on the scheduler?**

Two options. The in-band knob is `dreaming.novelty.backfill_per_cycle` (default `500`); raise to `2000` or higher to audit more historical dream rows per cycle. The out-of-band tool is `backfill-audit`, a standalone operator binary that bypasses the scheduler, cooldown, dirty flag, and `min_interval` entirely:

```bash
go build ./cmd/backfill-audit
./backfill-audit --config=config.yaml --project=<slug> --max=5000 --budget=2000000
```

Flags: `--project=<slug>` (required), `--max=2000` (audit cap; raise for larger drains), `--budget=500000` (total token budget), `--per-call-cap=10240` (per-LLM-call token cap), `--dry-run` (report eligible count only, do not audit). `make build` only compiles the server binary, so `backfill-audit` has to be built explicitly.

**One specific phase is the bottleneck. How do I speed just that phase up?**

Each LLM-spending phase has a per-cycle cap that operators can raise during a backlog drain, then restore once the residual clears:

- `dreaming.paraphrase.cap_per_cycle` (default `5000`) for the paraphrase-dedup sweep
- `dreaming.contradiction.cap_per_cycle` (default `2000`) for LLM pair-contradiction checks
- `dreaming.embedding_backfill.cap_per_cycle` (default `1000`) for repairing rows whose vector is missing
- `dreaming.pruning.batch_size` (default `5000`) for the streaming prune sweep

If one phase is being starved of token budget by the others, the `dreaming.<phase>.budget_fraction` settings rebalance the cycle envelope. Default split: `dreaming.contradiction.budget_fraction = 0.40`, `dreaming.consolidation.budget_fraction = 0.40`, `dreaming.embedding_backfill.budget_fraction = 0.10`, `dreaming.paraphrase_dedup.budget_fraction = 0.05`. SQL-only phases (`entity_dedup`, `transitive`, `pruning`, `weight_adjustment`) default to `0.0` so they share the root budget without a per-phase slice.

**What should I restore once the backlog has drained?**

Restore every dreaming setting you touched back to its default. The defaults are tuned for steady-state load, not first-pass backfill; leaving them elevated keeps LLM token spend permanently higher than it needs to be. The admin UI Settings page shows the default value inline beside each field for cross-reference, and the per-cycle counters in the Dreaming admin page will plateau once the residual clears, which is the signal to restore.

## License

MIT
