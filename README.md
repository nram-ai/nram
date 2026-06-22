<p align="center">
  <img src="ui/public/brand.png" alt="Neural Ram" width="150" />
</p>

<h1 align="center">Neural Ram</h1>

<p align="center"><sub><code>nram</code> for short</sub></p>

<p align="center">
  <strong>The continuity layer for everything you do with AI.</strong><br />
  One open source server you run yourself: every tool you use, every machine you work from, and the continuity is yours.
</p>

<p align="center">
  <a href="https://github.com/nram-ai/nram/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/nram-ai/nram/ci.yml?branch=master&style=flat-square&logo=githubactions&logoColor=white&label=CI" alt="CI" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-7FCFFA?style=flat-square" alt="License: MIT" /></a>
  <img src="https://img.shields.io/badge/Go-1.26%2B-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go 1.26+" />
  <img src="https://img.shields.io/badge/MCP-Streamable_HTTP-7FCFFA?style=flat-square" alt="MCP over Streamable HTTP" />
  <img src="https://img.shields.io/badge/storage-SQLite_%7C_PostgreSQL-7FCFFA?style=flat-square" alt="SQLite or PostgreSQL" />
  <a href="https://github.com/nram-ai/nram/stargazers"><img src="https://img.shields.io/github/stars/nram-ai/nram?style=flat-square&color=7FCFFA" alt="GitHub stars" /></a>
  <img src="https://img.shields.io/github/last-commit/nram-ai/nram?style=flat-square&color=7FCFFA" alt="Last commit" />
</p>

<p align="center">
  <a href="#download">Download</a> &middot;
  <a href="#quick-start">Quick Start</a> &middot;
  <a href="#reference">Docs</a> &middot;
  <a href="https://nram.ai">nram.ai</a>
</p>

> Work in progress: under active development. Expect rough edges, and feedback is welcome.

## What is Neural Ram?

Right now, you are the continuity layer between your AI tools. You copy context from Claude into ChatGPT, write handoff docs, re-explain the same decisions, and lose a little more each time you switch tools or machines.

nram is a continuity substrate: one self-hosted server that keeps what mattered across every tool, every conversation, and every machine, on infrastructure that belongs to you. Your agent already reads the PDF, watches the video, runs the test, scrapes the page. nram's job is to keep what mattered. Context, not storage.

It is a real server, not a library or a localhost shim: a single MIT-licensed binary with OAuth, passkeys, multi-tenancy, and MCP over HTTP, so your laptop, desktop, and phone all see the same brain. And it is more than a database with vector search: it pulls facts and entities out of your free-text notes, builds a knowledge graph of how they connect, and runs a background *dreaming* cycle that consolidates, dedups, and prunes while the server is idle, like a notebook that quietly reorganizes itself overnight.

### One substrate, many jobs

nram is not another memory tool bolted onto one app. It is the layer underneath them, so a single server covers work that today is split across separate products:

| Job | What nram provides | Comparable tools |
|---|---|---|
| Conversational continuity | Memory that survives across sessions, tools, and vendors, reachable over MCP. | Claude Memory, ChatGPT Memory |
| Document-corpus recall | Semantic search, an entity-deduped knowledge graph, and consolidation over a stored corpus. A substrate, not a chat UI. | NotebookLM, AnythingLLM, Khoj |
| Procedural rules | A first-class verbatim tier for standing rules, conventions, and protocols an assistant loads at session start. Returned byte-for-byte, never embedded or paraphrased. | (no direct equivalent) |
| Persona / self-knowledge (`about_me`) | A reserved, fully-indexed tier for identity, preferences, and ongoing context that surfaces by association on every recall. | (no direct equivalent) |
| Agent memory | Persistent memory for coding, research, and custom agents, with consolidation and a knowledge graph on top. | Mem0, Letta, Zep, Graphiti |

### How clients connect

- **MCP** is how Claude, ChatGPT, Cursor, or a custom agent connects. Streamable HTTP transport at `/mcp`, with OAuth discovery published at the well-known paths.
- **REST API** lets any code that can speak HTTP store and recall. See [docs/api.md](docs/api.md).
- **Web Console** is the dashboard for organizations, projects, providers, the knowledge graph, and the dreaming cycle.

## Features

**Memory and recall.** Hybrid retrieval fuses vector and lexical search (FTS5 on SQLite, `tsvector` on Postgres) with Reciprocal Rank Fusion, boosted by the knowledge graph. Ranking is relevance-first: query relevance (similarity plus graph connection) forms the base score, and the query-independent priors (recency, importance, frequency, confidence, origin) multiply it rather than adding to it, so a high-importance or recently-touched but off-topic memory can refine the order of relevant results yet never outrank the on-topic answer. MMR reranking demotes near-duplicate results so recall stays diverse. Embedding runs off the write path, so stores stay fast. Semantic search backs onto pgvector, a pure-Go HNSW index, or Qdrant. Multi-vector facets split a multi-topic memory into per-topic vectors (plus the whole-memory vector) and score against the best-matching facet, so a query about one sub-topic retrieves the memory at that sub-topic's strength instead of a diluted average; on by default, with a backfill to facet memories stored beforehand.

**Synthesis (`ask`).** Beyond returning a ranked list, the optional `ask` tool runs retrieval for you and writes one grounded, cited answer over your memories in a single model call. It recalls across a wide aperture (every project plus the global and persona tiers, or a single project when scoped), expands the top hits with graph-connected and sibling memories that clear a relevance bar against the query (so connected-but-off-topic memories are gated out rather than diluting the answer), and synthesizes a paragraph with inline footnote citations back to the source memories. Confidence is derived from the cited sources' vector evidence, and an answer that is not grounded in the retrieved neighborhood is returned as "not in neighborhood" rather than fabricated. Untrusted memory content and the question itself are fenced before synthesis as a prompt-injection guard. Off by default behind a feature flag and a dedicated provider slot, surfaced as both an MCP tool and a REST endpoint only when enabled; share-token callers are scoped strictly to the projects their share grants.

**Enrichment and the knowledge graph.** Background workers extract facts, entities, and relationships from your free-text memories and build a multi-hop graph that connects them over time. An optional ingestion judge decides add / update / delete / none against near-duplicates before extraction runs. Query augmentation paraphrases each memory into short retrieval queries so recall matches the way people actually ask.

**Dreaming.** An offline nine-phase consolidation cycle: entity dedup, embedding and augmentation backfill, paraphrase dedup, transitive-relationship inference, contradiction detection, consolidation, pruning (with optional confidence decay), and weight recalculation. Consolidation clusters related memories by embedding similarity (cosine) so syntheses stay semantically coherent, and an LLM novelty audit demotes low-value syntheses. See [docs/operations.md](docs/operations.md).

**Tiers.** Procedural memory is a verbatim per-user tier for rules and protocols, stored byte-for-byte and never embedded, enriched, or rewritten. The persona (`about_me`) and global tiers are reserved, auto-provisioned, and always join the recall aperture so relevant self-knowledge and world-knowledge surface alongside project results.

**Access and multi-tenancy.** Authentication via JWT, per-user API keys, WebAuthn passkeys, and per-organization OIDC SSO. Full OAuth 2.0 (Authorization Code + PKCE, dynamic client registration, resource indicators, discovery metadata). Five RBAC roles across REST and MCP. Organizations, hierarchical namespaces, and projects for isolation, plus share tokens for granting scoped external access without an account.

**Operability.** The Web Console, a React app, manages providers, settings, the graph, dreaming, the enrichment queue, and analytics. Run on SQLite (zero-config) or PostgreSQL, with SQLite-to-Postgres migration tooling. Provider-agnostic across OpenAI, Anthropic, Google Gemini, Ollama, OpenRouter, vLLM, SGLang, and any OpenAI-compatible endpoint, with per-call token accounting. Real-time updates over SSE, HMAC-signed webhooks, Prometheus metrics at `/metrics`, and JSON / NDJSON import/export.

A full feature-by-feature reference lives across the [docs](#reference).

## Download

Prebuilt packages are published for every platform as ready-to-run archives, so you can run nram without installing Go or Node. Each contains a single self-contained binary with the Web Console embedded. These are **nightly** builds: they track `master`, are refreshed every night, and are pre-releases (bleeding edge, not a stable version). Tagged stable releases, once published, appear on the [Releases page](https://github.com/nram-ai/nram/releases).

Every nightly asset lives under one rolling pre-release with stable URLs at [`releases/tag/nightly`](https://github.com/nram-ai/nram/releases/tag/nightly). After downloading, skip to [Step 3 (Run)](#3-run) below; the prerequisites and Build steps are only for building from source.

### macOS

nram on macOS is a terminal server, not a GUI app: download the `.tar.gz`, extract the binary, and run it from a terminal. Builds are published for `arm64` (Apple Silicon) and `amd64` (Intel). They are not code-signed or notarized, so a download is quarantined by Gatekeeper. Clear the quarantine flag once (this is what resolves the **"damaged / cannot be opened"** message) and run it:

```bash
# Apple Silicon; swap arm64 -> amd64 for Intel Macs
curl -fL -o nram-macos.tar.gz https://github.com/nram-ai/nram/releases/download/nightly/nram_nightly_darwin_arm64.tar.gz
tar -xzf nram-macos.tar.gz
xattr -d com.apple.quarantine nram   # clears the Gatekeeper "damaged / cannot be opened" block
chmod +x nram
./nram
```

### Linux

A native package (`.deb` or `.rpm`, which installs a desktop launcher and icons) or a `.tar.gz` archive, for `amd64` or `arm64`.

```bash
# Debian / Ubuntu (amd64)
curl -fLO https://github.com/nram-ai/nram/releases/download/nightly/nram_nightly_linux_amd64.deb
sudo apt install ./nram_nightly_linux_amd64.deb && nram
```

```bash
# Fedora / RHEL (amd64)
curl -fLO https://github.com/nram-ai/nram/releases/download/nightly/nram_nightly_linux_amd64.rpm
sudo dnf install ./nram_nightly_linux_amd64.rpm && nram
```

```bash
# Tarball, any distro (amd64)
curl -fL -o nram-linux.tar.gz https://github.com/nram-ai/nram/releases/download/nightly/nram_nightly_linux_amd64.tar.gz
tar -xzf nram-linux.tar.gz && ./nram
```

### Windows

Download [`nram_nightly_windows_amd64.zip`](https://github.com/nram-ai/nram/releases/download/nightly/nram_nightly_windows_amd64.zip) (or the `arm64` build), unzip it, and run `nram.exe`; the icon and version metadata are embedded. SmartScreen may warn on an unsigned binary, choose **More info → Run anyway**.

### Verify a download

Each release ships a `SHA256SUMS` manifest covering every asset. Download it alongside the file you grabbed and check it:

```bash
curl -fLO https://github.com/nram-ai/nram/releases/download/nightly/SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS   # macOS: shasum -a 256 -c --ignore-missing SHA256SUMS
```

## Quick Start

> nram needs an LLM provider to do anything beyond storing raw text. Without one, recall falls back to keyword-only matching and the knowledge graph stays empty. No error is raised; the system runs degraded. Configure providers in Step 5, and pick a long-context embedding model (see [docs/models.md](docs/models.md)).

### 1. Install prerequisites

| Tool | Required | Check |
|---|---|---|
| Go | 1.26.1+ | `go version` |
| Node.js | 18+ | `node --version` |
| npm | any (build uses `npm ci`, not pnpm or yarn) | `npm --version` |
| Ollama | optional, for local LLMs | `ollama --version` (skip if using a hosted provider) |

### 2. Build

```bash
git clone <repo-url> nram && cd nram
make build
```

Output is a single `./nram` binary with the Web Console embedded.

### 3. Run

```bash
./nram
```

Defaults: port **8674**, **SQLite** (`nram.db` in the working directory). For Postgres, set `DATABASE_URL` and restart:

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/nram ./nram
```

### 4. Open the setup wizard

Navigate to `http://localhost:8674`, create the initial admin account, and **save the API key shown on the completion screen. It is not shown again.**

### 5. Configure an LLM provider (required)

Open **Settings → Providers** and configure three slots: **Embedding** (semantic search), **Fact Extraction**, and **Entity Extraction** (the knowledge graph and dreaming). Any of OpenAI, Anthropic (chat slots only; it has no embeddings API), Google Gemini, Ollama, OpenRouter, vLLM, SGLang, or an OpenAI-compatible endpoint works. The **vLLM** and **SGLang** types automatically send `chat_template_kwargs.enable_thinking=false` to suppress a Qwen3-style reasoning pass (the analog of `reasoning_effort:none` on Ollama); override it, or send any other OpenAI `extra_body`, via the slot's **Extra Body** field. Changes hot-reload; no restart needed.

Each slot also accepts **Custom Headers**: arbitrary key/value HTTP headers sent on every request to that provider, for proxies or gateways between nram and the endpoint (auth tokens, routing, tenant ids). They are available for all provider types. `Content-Type` (and, for Anthropic, `anthropic-version`) are reserved; everything else, including auth headers, can be set or overridden. Because a header can carry auth, the API key is optional: a slot may authenticate through a header alone. Header names are shown in the console; their values are write-only and never returned.

See [docs/models.md](docs/models.md) for which model to put in each slot and how to size local models.

### 6. Verify

```bash
curl http://localhost:8674/v1/health
```

Each provider slot should report `"status": "ok"`. If a slot is missing or unhealthy, fix it before storing memories, otherwise they will not be embedded or enriched and you will need `./nram --reembed-all-memories` afterward.

### 7. Connect a client (MCP)

Local clients that can reach the server over your own network (Claude Code, Codex, Cursor, and other CLI or IDE tools) can use the plain HTTP URL directly, whether that's `localhost` or a LAN IP like `192.168.1.x`. For Claude Code:

```bash
claude mcp add --transport http nram http://localhost:8674/mcp
```

OAuth discovery is published at `/.well-known/oauth-authorization-server` and `/.well-known/oauth-protected-resource`, so OAuth-capable clients negotiate a token automatically. For clients without OAuth, use the API key from Step 4 as a Bearer token.

> **Hosted web tools need a public HTTPS URL.** ChatGPT, Claude on the web (claude.ai), and the Claude desktop and mobile apps reach your server from the vendor's cloud, not from your machine, so `http://localhost` will not work. They require a real, publicly resolvable hostname served over HTTPS with a valid (not self-signed) TLS certificate. nram serves plain HTTP and does not terminate TLS itself, so put it behind a reverse proxy that handles TLS (Caddy, nginx, Traefik) or expose it through a tunnel (Cloudflare Tunnel, ngrok, Tailscale Funnel), then point the connector at your public `https://your-host/mcp` URL.

Hitting trouble? See [Troubleshooting](docs/operations.md#troubleshooting).

## Reference

The deep reference is split out to keep this page approachable:

- **[docs/api.md](docs/api.md)**: full REST API and MCP tool/resource reference, including update/supersede and move semantics.
- **[docs/models.md](docs/models.md)**: recommended models per slot, VRAM sizing for local models, Ollama `num_ctx` and keep-alive tuning.
- **[docs/configuration.md](docs/configuration.md)**: bootstrap vs runtime config, environment variables, databases (SQLite, Postgres, Qdrant), migrations, and operator flags.
- **[docs/operations.md](docs/operations.md)**: troubleshooting and the dreaming / backfill operations guide.
- **[docs/openapi.yaml](docs/openapi.yaml)**: OpenAPI 3.1 specification, also served by the running server at `GET /openapi.yaml`. A conformance test keeps it in sync with the router.

## Development

```bash
make install-ui   # install UI dependencies
make dev          # React dev server with hot-reload on port 5173
make build        # build everything into ./nram
./nram --config config.yaml
```

Repository layout:

```
cmd/server/        Server entrypoint
internal/
  api/             HTTP handlers (REST + admin)
  auth/            OAuth 2.0, JWT, WebAuthn, RBAC
  config/          Bootstrap configuration loading
  dreaming/        Offline consolidation cycle (nine phases) with rollback and retention sweeps
  enrichment/      Background enrichment worker pool, ingestion decision, dedup, re-embed
  events/          Event bus, SSE, webhooks
  mcp/             MCP server and tool handlers
  migration/       Database migration runner
  model/           Data models
  provider/        LLM / embedding provider adapters with token-usage middleware
  server/          HTTP router setup
  service/         Business logic (recall, store, fusion, settings, lifecycle, export jobs)
  storage/         Database repositories (incl. HNSW, pgvector, Qdrant adapters)
  ui/              Embedded Web Console assets
migrations/        SQLite and PostgreSQL migration SQL
ui/                React Web Console source (TypeScript, Tailwind)
docs/              Reference docs and the OpenAPI spec
```

## License

MIT
