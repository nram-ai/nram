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

If you run the embedding and the fact/entity slots on Ollama (or another local backend such as vLLM, SGLang, or llama.cpp's llama-server), budget VRAM for the **sum** of every selected model, not the largest single one. The embedding model and the extraction model(s) are loaded and called independently, and the enrichment pipeline alternates between them on essentially every job, so both must be resident on the GPU at the same time.

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

## Sampling parameters for extraction (`repeat_penalty`, `top_k`, `min_p`)

The same OpenAI-compatibility endpoint also ignores the Ollama-native sampling extensions `repeat_penalty`, `top_k`, and `num_ctx`/`min_p` when they are passed in the request body, so nram does not expose them as settings. To curb runaway repetition on small models (the original reason these existed), bake the parameters into a Modelfile and point the extraction slot at the derived model:

```
FROM qwen3:8b
PARAMETER repeat_penalty 1.15
PARAMETER top_k 40
PARAMETER min_p 0.05
```

```bash
ollama create qwen3-8b-extract -f Modelfile
```

nram still controls `temperature` and `max_tokens` per call (both are standard OpenAI fields that the `/v1` path honors).

## Embedding dimensions

You do not need to enter the embedding model's dimension count. nram auto-detects dimensions on the first call to a new embedding provider by sending a probe string and reading the response shape. The detected count appears in the provider status read-back after the first successful call.

## The reranker slot (optional)

The reranker is off by default and not required. When configured, it re-scores the top recall and `ask` candidates for relevance before they are returned, demoting results that ranked high on vector or lexical similarity but are not actually on topic. nram detects which of two methods the configured endpoint supports at save time:

- **Cross-encoder** (`cross_encoder`): a dedicated relevance model served over a `/v1/rerank` endpoint (for example a `bge-reranker`-class model). It scores every `(query, candidate)` pair in one call, is deterministic, and is cheap per call. Prefer it when you can run one.
- **LLM judge** (`judge`): a generative chat model, scored one candidate at a time. It needs no separate reranker server, but is non-deterministic and costs more tokens. Use it to reuse a chat model you already host when a dedicated cross-encoder is not available.

Detection keys off the endpoint, not the model: a server that answers `/v1/rerank` is a cross-encoder, and anything else is driven as a judge. That distinction matters, because a cross-encoder *model* served over a chat-only endpoint (Ollama, say) is detected as a judge and cannot act as one. **Test** the slot to find out: when the method resolves to `judge`, the test drives the real judge against a known-answer pair and reports the scores it got, rather than only checking reachability.

A judge must reply with the bare relevance number and nothing else, which is what the test is checking. Two things stop that from happening, and both used to fail silently by flattening every score to the same value:

- **A reasoning pass.** Leave the slot's **Disable Thinking** toggle on (its default). A thinking model spends the token budget on its trace and never reaches the number. The toggle is offered on the reranker only when the method is `judge`; a cross-encoder does not generate, so it has no use for it.
- **Too small a token budget.** `ranking.rerank.judge.max_tokens` (default 16) has to leave room for the number. The test calibrates this: if the model cannot reach a number within the current cap, it raises the setting to the smallest value that works and saves it.

Editing `ranking.rerank.judge.system_prompt` is subject to the same contract: prompt for a bare number, or the reranker parses nothing and falls back to the prior order.

Sizing is modest: a cross-encoder in the `bge-reranker` class is far smaller than the extraction models, and the judge reuses an extraction-tier chat model. The reranker only runs over the small candidate window already retrieved, so it adds a single extra call per recall or `ask`, not per memory.
