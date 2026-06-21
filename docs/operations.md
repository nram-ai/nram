# Operations

Troubleshooting and the dreaming / backfill operations guide.

Back to the [README](../README.md).

## Troubleshooting

**"I stored memories but recall returns nothing relevant, only literal keyword matches."**
No embedding provider configured, or the configured one is unhealthy. Check `curl /v1/health`; `providers.embedding.status` should be `ok`. Without a working embedding provider, recall falls back to lexical-only (FTS5 / BM25 on SQLite, `tsvector` on Postgres). This is by design but can be unexpected. Fix: configure an embedding provider, then run `./nram --backfill-enrichment` once to embed the memories you stored beforehand.

**"The enrichment queue shows jobs, but the count never goes down."**
The fact-extraction or entity-extraction provider is not configured. The worker claims each job, sees the provider registry is incomplete, and silently re-releases it. No failure row is recorded, and no error logs unless logging is at INFO or higher. Fix: configure both the fact and entity slots.

**"I changed a provider/vector key in `config.yaml` or my env and it didn't take."**
Those keys were removed 2026-04-30 and are ignored (with a WARN log). All provider, vector, fact, and entity settings live in the database now and are managed at `/admin/providers` and `/admin/settings`. See [configuration.md](configuration.md#environment-variables). Provider changes hot-reload; no restart needed.

**"Recall quality got worse after a long memory ingest."**
Likely the `nomic-embed-text` 2048-token context limit truncating long memories at embed time. Switch to `qwen3-embedding:0.6b` (or another long-context embedding model) and run `./nram --reembed-all-memories` once. See [models.md](models.md#why-not-nomic-embed-text).

## Dreaming and backfill

Dreaming is the offline consolidation cycle (nine phases). Its settings live at `/admin/settings` and hot-reload per cycle. The knobs below help when you have a backlog to drain; restore the defaults afterward.

### Clearing a dreaming backlog quickly

Three knobs do the heavy lifting:

- Raise `dreaming.max_tokens_per_cycle` (default `1024000`) by 2x to 3x so each cycle does more work end to end.
- Lower `dreaming.min_interval_seconds` (default `600`) toward `120` so cycles fire more often per project.
- Lower `dreaming.cooldown_seconds` (default `300`) so the scheduler starts a cycle sooner after the last write.

Trade-off: sustained higher LLM token spend, and aggressive settings hit provider rate limits faster, so raise gradually and watch `/admin/usage`.

### Draining the novelty-audit backlog

The in-band knob is `dreaming.novelty.backfill_per_cycle` (default `500`); raise to `2000`+ to audit more historical dream rows per cycle.

The out-of-band tool is `backfill-audit`, a standalone operator binary that bypasses the scheduler, cooldown, dirty flag, and `min_interval` entirely. `make build` only compiles the server binary, so build it explicitly:

```bash
go build ./cmd/backfill-audit
./backfill-audit --config=config.yaml --project=<slug> --max=5000 --budget=2000000
```

Flags: `--project=<slug>` (required), `--max=2000` (audit cap; raise for larger drains), `--budget=500000` (total token budget), `--per-call-cap=10240` (per-LLM-call token cap), `--dry-run` (report eligible count only).

### Speeding up one phase

Each LLM-spending phase has a per-cycle cap you can raise during a drain, then restore:

- `dreaming.paraphrase.cap_per_cycle` (default `5000`) for the paraphrase-dedup sweep
- `dreaming.contradiction.cap_per_cycle` (default `2000`) for LLM pair-contradiction checks
- `dreaming.embedding_backfill.cap_per_cycle` (default `1000`) for repairing rows whose vector is missing
- `dreaming.augmentation_backfill.cap_per_cycle` (default `1000`) for re-enqueuing memories whose embedding fell back to raw content (gated by `dreaming.augmentation_backfill.enabled`, default `true`; this phase issues no LLM calls itself)
- `dreaming.pruning.batch_size` (default `5000`) for the streaming prune sweep

If one phase is starved of budget by the others, the `dreaming.<phase>.budget_fraction` settings rebalance the cycle envelope. Default split: `contradiction = 0.40`, `consolidation = 0.40`, `embedding_backfill = 0.10`, `paraphrase_dedup = 0.05`. SQL-only phases (`entity_dedup`, `transitive`, `pruning`, `weight_adjustment`) default to `0.0` and share the root budget without a per-phase slice.

### Restoring after a drain

Restore every dreaming setting you touched back to its default. The defaults are tuned for steady-state load, not first-pass backfill; leaving them elevated keeps token spend permanently higher than it needs to be. The Settings page shows the default value inline beside each field, and the per-cycle counters on the Dreaming page plateau once the residual clears, which is the signal to restore.

## Multi-vector facet backfill

After enabling `enrichment.multi_vector.enabled`, use the Settings page backfill button (or `POST /v1/admin/enrichment/backfill-multi-vector`, optional `project_id`, `dry_run`, and `limit`) to facet memories stored beforehand. Dream syntheses are enqueued first because they are the population most prone to multi-topic dilution; a single-topic memory is left with just its whole-memory vector. Re-running is safe: faceting replaces a memory's facet set.

The backfill is facet-only. Each job reuses the memory's existing whole-memory vector (facet 0) and runs only the per-topic sentence embeds, so it makes no LLM (ingestion-decision or query-augmentation) calls and does not re-embed whole memories. A memory whose stored vector is missing (its dimension was never recorded, or its original embed failed) is skipped: it belongs to the query-augmentation/embedding backfill, not this one.

The sentence embeds still add load: each backfilled memory's sentences are embedded, so a full sweep multiplies embedding calls across every enrichment worker. `enrichment.multi_vector.embed_concurrency` (default `4`) bounds how many memories are facet-embedded at once across the whole pool, so a bulk backfill cannot stampede a modest embedder; lower it if a backfill destabilises the embedder, raise it on a provider that sustains parallel calls (takes effect on restart). `enrichment.multi_vector.max_facets` (default `8`) caps vectors per memory; the faceted search candidate window scales with it automatically.
