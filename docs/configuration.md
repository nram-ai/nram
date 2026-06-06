# Configuration

nram has two configuration surfaces.

- **Bootstrap config** (config file / env vars): the small set of values needed before the database is open. Listener, DSN, log level, and the optional headless admin credentials. Any change requires a restart.
- **Runtime config** (Web Console / `/v1/admin/settings`): everything else. Providers, vector backends, dreaming, ranking, retention, prompts. Stored in the `settings` table and mostly hot-reloadable.

Back to the [README](../README.md).

## Bootstrap config

The loader reads, in order of precedence:

1. `--config` flag
2. `NRAM_CONFIG` environment variable
3. `config.yaml` in the working directory
4. Built-in defaults

### Config file

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

YAML values support environment-variable interpolation: `${VAR_NAME:-default}`.

### Environment variables

| Variable | Description |
|---|---|
| `PORT` | Server port (default 8674) |
| `DATABASE_URL` | PostgreSQL connection string |
| `LOG_LEVEL` | Log level: debug, info, warn, error |
| `NRAM_CONFIG` | Path to a config file (alternative to `--config`) |
| `NRAM_ADMIN_EMAIL` | Headless bootstrap administrator email (first boot only) |
| `NRAM_ADMIN_PASS` | Headless bootstrap administrator password (first boot only) |
| `NRAM_ENABLE_ENRICHMENT_BACKFILL` | Set to `1` to run the enrichment backfill at startup without exiting |

> **Removed surface (2026-04-30).** Provider, vector, and tuning settings are no longer accepted in bootstrap config. The YAML keys `embed.*`, `fact.*`, `entity.*`, `qdrant.*`, `hnsw.*`, and `enrichment_orphan_grace_seconds`, and the env vars `NRAM_EMBED_*`, `NRAM_FACT_*`, `NRAM_ENTITY_*`, `NRAM_ENRICHMENT_ORPHAN_GRACE_SECONDS`, and `NRAM_MCP_MAX_RESULT_TOKENS` now live exclusively at runtime in the DB-backed settings registry. The loader logs a WARN line for each deprecated key it sees and ignores the value. Manage them at `/admin/settings`.

## Runtime configuration

Everything outside the bootstrap surface is managed through the Web Console:

- **Providers** (embedding, fact extraction, entity extraction): `/admin/providers`
- **Vector backend** (Qdrant address/credentials, HNSW tuning): `/admin/settings`
- **Dreaming**, **enrichment**, **ranking**, **recall fusion**, **reconsolidation**, **retention**, **rate limits**, **lifecycle sweep**, **events**, and prompt templates: `/admin/settings`

Provider changes hot-reload; no restart needed.

### Per-project and per-user overrides

`ranking_weights`, `dedup_threshold`, and `enrichment_enabled` cascade `system → user → project → effective`, stored as sparse JSON on the project and user records; unset fields fall through to system defaults. Recall scores each candidate under its owning project's effective weights, so cross-project results (globals, shared namespaces) honor each row's owner's tuning. Edit at `PUT /v1/me/projects/{id}` (project) or `/v1/admin/users/{id}` (user). User-scope `ranking_weights` is rejected with a 400; the cascade for weights lands at project, not user.

## Database

### SQLite (default)

No configuration required. Creates `nram.db` in the working directory with WAL mode, foreign keys, and FTS5 full-text search. SQLite mode uses a pure-Go HNSW index for vector search and FTS5 for the lexical channel of hybrid recall. Enrichment, dreaming, the knowledge graph, and all MCP tools are fully supported.

### PostgreSQL

Set `DATABASE_URL` or `database.url`:

```bash
DATABASE_URL=postgres://nram:password@localhost:5432/nram ./nram
```

PostgreSQL enables pgvector for semantic search, a generated `content_tsv` column with `ts_rank_cd` for the lexical channel, and LISTEN/NOTIFY for multi-instance event propagation.

### Qdrant (optional)

For dedicated vector search, configure Qdrant as an alternative to pgvector at runtime under `/admin/settings` (address and credentials). Qdrant is no longer configured in `config.yaml`; the old `qdrant.*` bootstrap keys are deprecated and ignored.

### Migrations

Migrations run automatically on startup when `migrate_on_start: true` (the default). Manual control:

```bash
./nram migrate up       # Apply pending migrations
./nram migrate down     # Roll back one migration
./nram migrate version  # Show current migration version
```

## Operator flags

| Flag / variable | Description |
|---|---|
| `--config <path>` | Override the config file path |
| `--backfill-enrichment` | Enqueue enrichment jobs for memories missing vectors, then exit. The worker skips fact/entity extraction for memories whose lineage/relationships already exist, so re-running is cheap. |
| `--reembed-all-memories` | Force re-embed every live memory (e.g. after switching embedding models), then exit |
| `--normalize-memory-tags` | Rewrite tags on all memory rows to the canonical normalized form, then exit |
| `migrate up` / `migrate down` / `migrate version` | Migration CLI commands (run before normal startup) |
| `NRAM_ENABLE_ENRICHMENT_BACKFILL=1` | Run the enrichment backfill at startup without forcing an exit |

The `backfill-audit` operator binary is documented in [operations.md](operations.md#draining-the-novelty-audit-backlog).
