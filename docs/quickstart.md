# Quick Start

Get nram running and connected to a client. If you grabbed a prebuilt binary from [Download and Install](install.md), skip to [Step 3 (Run)](#3-run).

Back to the [README](../README.md).

> nram needs an LLM provider to do anything beyond storing raw text. Without one, recall falls back to keyword-only matching and the knowledge graph stays empty. No error is raised; the system runs degraded. Configure providers in [Step 5](#5-configure-an-llm-provider-required), and pick a long-context embedding model (see [models.md](models.md)).

## 1. Install prerequisites

Only needed when building from source. A prebuilt binary needs none of these.

| Tool | Required | Check |
|---|---|---|
| Go | 1.26.1+ | `go version` |
| Node.js | 18+ | `node --version` |
| npm | any (build uses `npm ci`, not pnpm or yarn) | `npm --version` |
| Ollama | optional, for local LLMs | `ollama --version` (skip if using a hosted provider) |

## 2. Build

```bash
git clone <repo-url> nram && cd nram
make build
```

Output is a single `./nram` binary with the Web Console embedded.

## 3. Run

```bash
./nram
```

Defaults: port **8674**, **SQLite** (`nram.db` in the working directory). For Postgres, set `DATABASE_URL` and restart:

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/nram ./nram
```

## 4. Create your administrator account and run guided setup

Navigate to `http://localhost:8674` and create the initial admin account. **Save the API key shown right after; it is displayed only once.** You are then taken into a short guided setup that walks you, one step at a time, through the three required providers, the optional providers, and the high-level feature toggles (enrichment, dreaming, ask synthesis, reranking), finishing with the MCP-connect details. Each provider step has a **Test Connection** check that must pass before its config is saved (a failed test still offers a "Save anyway" escape). You can skip any step and configure it later under **Settings**; the section below is the same configuration the wizard walks you through.

## 5. Configure an LLM provider (required)

The guided setup walks you through these slots; afterward they live under **Settings → Providers**, where you can edit them anytime. The three required slots are **Embedding** (semantic search), **Fact Extraction**, and **Entity Extraction** (the knowledge graph and dreaming). The optional slots (Query Augmentation, Ingestion Decision, Ask Synthesis, and a relevance Reranker) can be left unset: the first two fall back to Fact Extraction, and the last two stay inert until configured.

Any of OpenAI, Anthropic (chat slots only; it has no embeddings API), Google Gemini, Ollama, OpenRouter, vLLM, SGLang, llama.cpp's llama-server, or an OpenAI-compatible endpoint works. Changes hot-reload; no restart needed.

Each LLM slot has a **Disable Thinking** toggle, on by default, that sends the provider-appropriate "thinking off" knob to skip a Qwen3-style reasoning pass: `reasoning_effort:none` on Ollama, `reasoning.enabled:false` on OpenRouter, `chat_template_kwargs.enable_thinking=false` on vLLM, SGLang, and llama-server, and a zero thinking budget on Gemini. It is inert for OpenAI, Anthropic, and the generic openai-compatible type, which reject an explicit disable, so the toggle is not offered there. The Reranker slot shows it only when its detected method is the LLM judge: a cross-encoder does not generate, so the knob would do nothing. Send any other OpenAI `extra_body` keys via the slot's **Extra Body** field.

Each slot also accepts **Custom Headers**: arbitrary key/value HTTP headers sent on every request to that provider, for proxies or gateways between nram and the endpoint (auth tokens, routing, tenant ids). They are available for all provider types. `Content-Type` (and, for Anthropic, `anthropic-version`) are reserved; everything else, including auth headers, can be set or overridden. Because a header can carry auth, the API key is optional: a slot may authenticate through a header alone. Header names are shown in the console; their values are write-only and never returned.

See [models.md](models.md) for which model to put in each slot and how to size local models.

## 6. Verify

```bash
curl http://localhost:8674/v1/health
```

Each provider slot should report `"status": "ok"`. If a slot is missing or unhealthy, fix it before storing memories, otherwise they will not be embedded or enriched and you will need `./nram --reembed-all-memories` afterward.

## 7. Connect a client

Local clients that can reach the server over your own network (Claude Code, Codex, Cursor, and other CLI or IDE tools) can use the plain HTTP URL directly, whether that's `localhost` or a LAN IP like `192.168.1.x`. For Claude Code:

```bash
claude mcp add --transport http nram http://localhost:8674/mcp
```

OAuth discovery is published at `/.well-known/oauth-authorization-server` and `/.well-known/oauth-protected-resource`, so OAuth-capable clients negotiate a token automatically. For clients without OAuth, use the API key from Step 4 as a Bearer token.

> **Hosted web tools need a public HTTPS URL.** ChatGPT, Claude on the web (claude.ai), and the Claude desktop and mobile apps reach your server from the vendor's cloud, not from your machine, so `http://localhost` will not work. They require a real, publicly resolvable hostname served over HTTPS with a valid (not self-signed) TLS certificate. nram serves plain HTTP and does not terminate TLS itself, so put it behind a reverse proxy that handles TLS (Caddy, nginx, Traefik) or expose it through a tunnel (Cloudflare Tunnel, ngrok, Tailscale Funnel), then point the connector at your public `https://your-host/mcp` URL.

Hitting trouble? See [Troubleshooting](operations.md#troubleshooting).
