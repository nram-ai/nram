# API Reference

REST and MCP surfaces for nram. An OpenAPI 3.1 specification also lives at [openapi.yaml](openapi.yaml); it may lag the code, so the tables below reflect the current router source of truth (`internal/server/router.go`).

Back to the [README](../README.md).

## Authentication

All authenticated requests carry a Bearer token in the `Authorization` header. A token can be:

- A JWT from `POST /v1/auth/login` (password) or the passkey / OIDC flows.
- A per-user API key from `/v1/me/api-keys` (prefix `nram_k_`).
- An OAuth 2.0 access token from `/token`.

Setup-guarded routes return 503 until the initial admin has been created via the setup wizard. Share-token credentials are restricted to `/mcp` and `/userinfo`.

## Health and observability

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/health` | Health check (backend, provider slots, enrichment queue) |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/v1/events` | Server-Sent Events stream (scope filter + replay) |

## Login and account bootstrap

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/admin/setup/status` | Whether the initial admin has been provisioned |
| `POST` | `/v1/admin/setup` | Complete first-run setup (creates the administrator) |
| `POST` | `/v1/auth/lookup` | Resolve an email to its available login methods |
| `POST` | `/v1/auth/login` | Password login, returns a JWT |
| `POST` | `/v1/auth/passkey/begin` and `/finish` | WebAuthn login challenge and completion |
| `GET` | `/auth/idp/login` and `/auth/idp/callback` | Per-organization OIDC single sign-on |

## OAuth 2.0

| Path | Description |
|---|---|
| `/.well-known/oauth-authorization-server` | Authorization server metadata (RFC 8414) |
| `/.well-known/oauth-protected-resource` | Protected resource metadata (RFC 9728) |
| `GET POST /authorize` | Authorization endpoint: GET serves the consent page, POST records the decision (PKCE required) |
| `/token` | Token endpoint |
| `/register` | Dynamic client registration (RFC 7591) |
| `/userinfo` | OpenID userinfo endpoint |
| `GET /v1/oauth/authorize/context` | Validates an in-flight OAuth request for the consent page |
| `POST /v1/oauth/share/preview` | Previews a pasted share secret's grants without consuming it |
| `GET /v1/share/accept` | Backs the share-accept landing page (`/share/accept`): renders a share's grants and the MCP URL to configure |

## Memories (project-scoped)

All under `/v1/projects/{project_id}/memories`. Read operations are available to any authenticated role; write operations require a non-readonly role.

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | List memories (filters: tags, date range, source, search, enriched) |
| `GET` | `/ids` | List matching memory IDs (for "select all") |
| `GET` | `/{id}` | Get a memory by ID |
| `POST` | `/get` | Batch-get by ID list |
| `POST` | `/recall` | Hybrid recall (vector + optional lexical + graph + ranking; fires reconsolidation) |
| `GET` | `/export` | Export as JSON / NDJSON |
| `POST` | `/` | Store a memory |
| `PUT` | `/{id}` | Update a memory |
| `DELETE` | `/{id}` | Soft-delete a memory |
| `POST` | `/batch` | Batch store |
| `POST` | `/forget` | Bulk soft-delete |
| `POST` | `/enrich` | Trigger enrichment (gated: 503 until all three provider slots are configured) |
| `POST` | `/import` | Import a project snapshot |
| `POST` | `/{id}/move` | Move a memory to another project |
| `POST` | `/move` | Bulk-move memories to another project |
| `POST` | `/{id}/preview-augmentation` | Run query augmentation against one memory without persisting (write-tier: it incurs an LLM call) |

Move re-stores each memory into the destination project and hard-deletes the source only after the store succeeds, so a failure between the two steps leaves at most a transient duplicate, never a hole. The caller must own both projects, and the target must differ from the source.

### Update semantics: in-place vs. supersede chain

`PUT /{id}` and the MCP `update` tool branch on whether the request changes content:

- **Tag-only or metadata-only updates** mutate the row in place. The response `id` matches the path id.
- **Content updates** create a new memory row and mark the old row `superseded_by = new id`. The response `id` is the new (active) id; `previous_memory_id` echoes the path id. Recall, list, and graph reads filter superseded rows by default, so the new id is what surfaces; `include_superseded=true` is required to access prior versions. Old enrichment (entities, relationships, embedding, accumulated recall weights) stays attached to the old id, frozen with the old content. Dream pruning sweeps superseded rows after a grace window.

Forget on the active head walks `superseded_by` and soft-deletes the entire chain. Pass `hard_delete=true` to bypass the soft delete; the chain walk runs in either mode.

This is a breaking change from the prior in-place semantics: callers that hold the path id for follow-up reads must use `response.id`. Subscribers to `memory.updated` should read `memory_id` (active) and treat `previous_memory_id` as the correlation key.

## User self-service

All under `/v1/me`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/memories/recall` | Cross-project recall for the current user |
| `GET` `POST` | `/procedural` | List (paginated, includes disabled) or create verbatim procedural entries |
| `GET` `PUT` | `/procedural/{id}` | Get or partial-update a procedural entry |
| `DELETE` | `/procedural/{id}` | Soft-delete a procedural entry |
| `GET` | `/procedural/export` | Export procedural entries |
| `POST` | `/procedural/import` | Import procedural entries |
| `GET` `POST` | `/projects` | List or create projects owned by the user |
| `GET` `PUT` `DELETE` | `/projects/{id}` | Manage a specific project |
| `GET` `POST` | `/api-keys` | List or mint API keys |
| `DELETE` | `/api-keys/{id}` | Revoke an API key |
| `GET` `POST` | `/oauth-clients` | List or register OAuth clients |
| `DELETE` | `/oauth-clients/{id}` | Revoke an OAuth client |
| `POST` | `/password` | Change password |
| `GET` `PATCH` | `/profile` | Read or update the caller's profile |
| `GET` | `/passkeys` | List registered passkeys |
| `POST` | `/passkeys/register/begin` and `/finish` | Register a new passkey |
| `DELETE` | `/passkeys/{id}` | Remove a passkey |
| `GET` | `/capabilities` | Self-tier capability flags (drives sidebar nav; no provider details) |
| `GET` | `/ranking-weights/defaults` | The ranking-weight schema and effective global defaults, for the per-project editor |
| `GET` `POST` | `/exports` | List or create asynchronous export jobs |
| `GET` `DELETE` | `/exports/{job_id}` | Get status of, or delete, an export job |
| `GET` | `/exports/{job_id}/download` | Download a completed export artifact |
| `GET` `POST` | `/shares` | List or create share tokens |
| `GET` `PATCH` `DELETE` | `/shares/{id}` | Manage a specific share token |
| `*` | `/dreaming`, `/dreaming/*` | Read-only view of the caller's own dream cycles (gated) |
| `*` | `/enrichment`, `/enrichment/*` | Read-only view of the caller's own enrichment queue (gated) |

The export pipeline replaced the former MCP `export` tool: large multi-project exports run asynchronously, the artifact lands on disk, and the caller downloads it through these handlers. Per-project and per-user ranking/dedup/enrichment overrides are edited via `PUT /v1/me/projects/{id}` (see [configuration.md](configuration.md#per-project-and-per-user-overrides)).

## Scoped data views

Authenticated; each handler self-scopes to the caller (an administrator sees only their own data on these surfaces).

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/dashboard` | Counts and headline metrics |
| `GET` | `/v1/activity` | Recent memory activity |
| `GET` | `/v1/analytics` | Memory, recall, and enrichment analytics |
| `GET` | `/v1/usage` | Token usage aggregation |
| `GET` | `/v1/usage/cost_rates` | Per-model cost rates used to value token usage |
| `GET` | `/v1/graph` | Knowledge graph data |
| `GET` | `/v1/namespaces/tree` | Namespace hierarchy |

The cross-tenant enrichment and dreaming surfaces moved off `/v1/` to `/v1/admin/` (system-wide) and `/v1/orgs/{id}/` (org-scoped); self-tier read views live under `/v1/me/` above.

## Organization management

All under `/v1/orgs/{org_id}`, gated by org membership.

| Method | Path | Description |
|---|---|---|
| `GET` | `/dashboard`, `/activity`, `/analytics`, `/usage` | Org-scoped aggregate views (org_owner+; admin via short-circuit). Aggregate counts and distributions only, no row-level or content data |
| `*` | `/dreaming`, `/enrichment` (and `/*`) | Org-tier dream and enrichment control (retry, abandon, rollback within the org; gated) |
| `*` | `/users/...` | Manage users in the org (org_owner+) |
| `*` | `/idp/...` | Manage per-org OIDC configuration (org_owner+) |

## Administration

All under `/v1/admin`, gated by the `administrator` role.

| Method | Path | Description |
|---|---|---|
| `*` | `/orgs/...` | Organization CRUD |
| `*` | `/users/...` | Global user CRUD |
| `*` | `/providers/...` | LLM / embedding provider configuration |
| `*` | `/settings` | Global settings (ranking, recall fusion, ingestion decision, novelty audit, reconsolidation, dreaming budgets, retention, prompts) |
| `*` | `/settings/reset` | Reset settings to defaults |
| `*` | `/oauth/...` | OAuth client and IdP-config administration |
| `*` | `/webhooks/...` | Webhook registration and delivery audit |
| `*` | `/database/...` | Database info, test, preflight, migration audit, reset |
| `*` | `/graph/*` | Knowledge-graph maintenance: lost-provenance health count (`/graph/health`) and orphan reap/repair (`/graph/repair`) |
| `GET` | `/system/dashboard`, `/system/activity`, `/system/analytics`, `/system/usage` | System-aggregate views: totals plus per-org breakdown rows, no per-user or content data |
| `*` | `/enrichment`, `/dreaming` (and `/*`) | System-wide enrichment and dreaming control with full cross-tenant visibility (gated) |

There is no `/v1/admin/projects`: administrators use the self-tier `/v1/me/projects` like every other role, because cross-tenant project listing would expose user-authored names and descriptions.

## MCP (Model Context Protocol)

The MCP server is available at `POST /mcp` using Streamable HTTP transport. It exposes 16 tools.

| Tool | Description |
|---|---|
| `store` | Store a single memory. Identical content within the same project is deduplicated on ingest; the existing memory's ID is returned and tags / metadata on the new request are ignored. Every store unconditionally enqueues an enrichment job; the worker drains it when enrichment is enabled and providers are configured. |
| `store_batch` | Batch store memories (same dedup-on-ingest behavior, same automatic enrichment). |
| `update` | Update a memory (in-place for tag/metadata-only; supersede chain for content changes; see update semantics above). |
| `get` | Retrieve a memory by ID. |
| `list` | List memories with pagination. Superseded rows are always hidden on the MCP path; use the REST endpoint with `include_superseded=true` for a diagnostic view. |
| `recall` | Hybrid (vector + lexical) recall. Graph entities and relationships are included when the graph is populated. `limit` is server-capped at `recall.max_limit` (default 50); `graph_depth` at `recall.graph.max_depth` (default 5). Optional `diversify_by_tag_prefix` for round-robin coverage across a tag axis. Similarity-threshold knobs are REST-only. |
| `forget` | Soft-delete a memory; cascades restricted to extraction lineage. Set `hard: true` for an unrecoverable hard delete. |
| `graph` | Knowledge graph traversal. `depth` is server-capped at `recall.graph.max_depth` (default 5). |
| `list_projects` | List projects. |
| `update_project` | Update a project. Reserved projects (`global`, `about_me`) reject name/description changes; only `default_tags` is editable. |
| `delete_project` | Delete a project. Reserved projects (`global`, `about_me`) cannot be deleted. |
| `about_me` | Read the per-user persona tier (`about_me` project), ordered most-defining-first by entity centrality, surfacing frequency, and recency. Paginated; an over-budget page emits a `_truncated` marker with a paging hint. Share-token callers are denied. |
| `procedural_fetch` | Return all enabled procedural entries verbatim, ordered by priority then recency. Paginated; an over-budget page drops whole low-priority entries and emits a `_truncated` marker so the caller pages until every entry is loaded. Share-token callers are denied. |
| `procedural_store` | Create a verbatim procedural entry (optional title, category, tags, priority, enabled, metadata). Never embedded or enriched. |
| `procedural_update` | Partial-update a procedural entry's mutable fields. |
| `procedural_forget` | Soft-delete a procedural entry by id. |

Export is not an MCP tool: its payload travelled inline through the transport, which truncates beyond the byte budget, so it moved to the asynchronous REST + UI pipeline at `/v1/me/exports`. Admin-only operations (settings cascade, provider health, paraphrase backfill) and diagnostic flags (`include_superseded`, `include_audit`, `include_low_novelty`) are REST-only. The MCP tool surface is intentionally narrow.

### Resources

| URI | Description |
|---|---|
| `nram://projects` | List all projects |
| `nram://projects/{slug}/entities` | Entities in a project |
| `nram://projects/{slug}/graph` | Knowledge graph data for a project |
