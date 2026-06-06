# Choosing Models

nram is provider-agnostic, but the **embedding model** has an outsized effect on recall quality, and local models have to fit in VRAM together. Pick a tier below and move on.

Back to the [README](../README.md).

## Recommended tiers

### Tier 1: Lite (fits on a laptop, slow but works)

| Slot | Model | Where | Notes |
|---|---|---|---|
| Embedding | `qwen3-embedding:0.6b` | Ollama | ~600M params, ~1.2 GB on disk |
| Fact | `qwen3:4b` | Ollama | 4B params, ~2.5 GB on disk, Q4_K_M |
| Entity | `qwen3:4b` | Ollama | The same model is fine for both extraction slots |

### Tier 2: Recommended (the configuration nram's author runs)

| Slot | Model | Where | Notes |
|---|---|---|---|
| Embedding | `qwen3-embedding:0.6b` (with a bumped `num_ctx`) | Ollama | Trained at 32K context; raise Ollama's default `num_ctx` of 2048 to use it (see below) |
| Fact | `qwen3:8b` | Ollama | 8.2B params, ~5.2 GB on disk, Q4_K_M |
| Entity | `qwen3:8b` | Ollama | Same model |

### Tier 3: Cloud (no local GPU needed)

| Slot | Model | Where | Notes |
|---|---|---|---|
| Embedding | `text-embedding-3-small` | OpenAI | 8K context, 1536 dims |
| Fact | `gpt-4o-mini` or `claude-haiku-4-5-20251001` | OpenAI / Anthropic | Hosted; charges per token |
| Entity | `gpt-4o-mini` or `claude-haiku-4-5-20251001` | OpenAI / Anthropic | Same model |

> Anthropic does not offer an embeddings API. To use Claude for fact / entity extraction, pair it with OpenAI or Ollama for the embedding slot.

## Local models must all fit in VRAM at once

If you run the embedding and the fact/entity slots on Ollama (or any local backend), budget VRAM for the **sum** of every selected model, not the largest single one. The embedding model and the extraction model(s) are loaded and called independently, and the enrichment pipeline alternates between them on essentially every job, so both must be resident on the GPU at the same time.

When they don't all fit, one of two things happens, both bad:

- **Partial CPU offload.** Ollama spills the overflow layers to system RAM. Inference still completes, an order of magnitude slower.
- **Model thrashing.** Ollama unloads one model to make room for the other on each call. Because enrichment switches between the embedding slot and the extraction slots constantly, this swap fires on nearly every job, and each swap pays a full multi-GB cold load. The visible symptom is enrichments that appear to hang.

Concrete example: the Tier 2 combo (`qwen3-embedding:0.6b` ~1.2 GB plus `qwen3:8b` ~5.2 GB of weights, **plus** KV-cache and context buffers that grow with `num_ctx`) does not comfortably coexist on a 12 GB card once those buffers are counted.

> Setting `OLLAMA_KEEP_ALIVE` does not fix thrashing when the models can't fit together. Keep-alive only stops idle eviction; if there isn't room for both, Ollama must still evict one to load the other. Keep-alive helps after everything fits, not instead of fitting.

Mitigations, in rough order of preference:

- **Split the slots across machines.** Each provider slot is configured independently, so they can point at different Ollama hosts on different machines or GPUs. Run the embedding model on one box and the extraction models on another.
- **Pick smaller models** (e.g. drop the extraction slots to Tier 1's `qwen3:4b`).
- **Move one slot to a cloud provider** (Tier 3) so only one model occupies the GPU.
- **Add VRAM.**

Confirm what's actually on the GPU with `ollama ps` (or `curl -s http://<ollama-host>:11434/api/ps`): the `SIZE` / `PROCESSOR` columns show whether each model is fully GPU-resident or spilling to CPU.

## Why not `nomic-embed-text`?

`nomic-embed-text` is a commonly suggested Ollama embedding model, but it has a limitation worth knowing before choosing it:

- It has a **2048-token training context**, and Ollama's default `num_ctx` is also 2048, so anything past roughly 1500 words of a memory is truncated before embedding.
- nram does not pre-truncate or warn. Ollama returns a vector computed from the truncated prefix, and nram stores it as if it represented the whole memory.
- Result: long memories are embedded as if they were short, and recall quality degrades silently. No error surfaces.

Using `qwen3-embedding:0.6b` (or any embedding model with a longer trained context) avoids this.

## Bumping `num_ctx` for Ollama embeddings

By default, Ollama caps context at 2048 tokens regardless of what the model was trained for. To actually use `qwen3-embedding:0.6b`'s 32K trained context, create a Modelfile that pins a larger `num_ctx`:

```
FROM qwen3-embedding:0.6b
PARAMETER num_ctx 8192
```

```bash
ollama create qwen3-embedding-8k -f Modelfile
```

Then point nram's embedding slot at `qwen3-embedding-8k` instead of the base tag. 8K is a reasonable default; raise it further for long-form documents if you have the VRAM.

## Keeping Ollama models loaded (`OLLAMA_KEEP_ALIVE`)

Ollama evicts an idle model after 5 minutes by default. On slow CPUs or weak GPUs, the first call after eviction pays the full cold-load cost (often minutes for a multi-GB quantized model), which looks like a hang to the calling client.

Pin loaded models for a week with `OLLAMA_KEEP_ALIVE=168h` (or `-1` for indefinite) in the Ollama server's environment, then restart the service:

- **Linux (systemd):** `sudo systemctl edit ollama.service`, add `Environment="OLLAMA_KEEP_ALIVE=168h"` under `[Service]`, then `sudo systemctl daemon-reload && sudo systemctl restart ollama`.
- **macOS:** `launchctl setenv OLLAMA_KEEP_ALIVE 168h`, then quit and relaunch the Ollama app.
- **Windows:** add `OLLAMA_KEEP_ALIVE=168h` to user environment variables, then quit Ollama from the tray and reopen it.

Verify with `curl -s http://<ollama-host>:11434/api/ps` after a call; the loaded model's `expires_at` should be ~168h out.

This must live on the Ollama server because nram inferences run through Ollama's OpenAI-compatibility endpoint (`/v1/chat/completions`, `/v1/embeddings`), and that path drops `keep_alive` from request bodies. Only the server-side env var controls eviction for `/v1/*` traffic.

## Embedding dimensions

You do not need to enter the embedding model's dimension count. nram auto-detects dimensions on the first call to a new embedding provider by sending a probe string and reading the response shape. The detected count appears in the provider status read-back after the first successful call.
