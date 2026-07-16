// Shared provider-slot configuration constants, helpers, and form types.
//
// Extracted verbatim from ui/src/pages/ProviderConfiguration.tsx so the full
// provider editor can be reused by both the admin Providers page and the
// first-run onboarding wizard without drift. Pure module: no React, no hooks.

export const PROVIDER_TYPES = [
  "openai",
  "ollama",
  "gemini",
  "anthropic",
  "openrouter",
  "openai-compatible",
  "vllm",
  "sglang",
  "llama-server",
] as const;

// Human-facing labels for the type dropdown and badge. The raw type strings
// don't title-case cleanly (e.g. "openai" -> "Openai", "openrouter" ->
// "Openrouter"), so brands are spelled out here; anything missing falls back to
// a plain capitalize.
export const PROVIDER_DISPLAY_NAMES: Record<string, string> = {
  openai: "OpenAI",
  ollama: "Ollama",
  gemini: "Gemini",
  anthropic: "Anthropic",
  openrouter: "OpenRouter",
  "openai-compatible": "OpenAI-Compatible",
  vllm: "vLLM",
  sglang: "SGLang",
  "llama-server": "llama.cpp",
};

export function providerDisplayName(type: string): string {
  return (
    PROVIDER_DISPLAY_NAMES[type] ||
    (type ? type.charAt(0).toUpperCase() + type.slice(1) : type)
  );
}

// Provider types in dropdown order (alphabetical by display label). PROVIDER_TYPES
// and the labels are static, so sort once at module load rather than per render.
export const SORTED_PROVIDER_TYPES = [...PROVIDER_TYPES].sort((a, b) =>
  providerDisplayName(a).localeCompare(providerDisplayName(b)),
);

export const PROVIDER_BADGE_COLORS: Record<string, string> = {
  openai:
    "bg-success/10 text-success",
  ollama:
    "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300",
  gemini: "bg-info/10 text-info",
  anthropic:
    "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300",
  openrouter:
    "bg-cyan-100 text-cyan-800 dark:bg-cyan-900 dark:text-cyan-300",
  vllm:
    "bg-emerald-100 text-emerald-800 dark:bg-emerald-900 dark:text-emerald-300",
  sglang:
    "bg-sky-100 text-sky-800 dark:bg-sky-900 dark:text-sky-300",
  "llama-server":
    "bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-300",
};

// Neutral badge for any type without a dedicated color above (e.g.
// openai-compatible).
export const DEFAULT_BADGE_COLOR = "bg-muted text-muted-foreground";

// Base URLs only; the backend appends each provider's versioned route path (e.g. /v1/...).
export const DEFAULT_URLS: Record<string, string> = {
  openai: "https://api.openai.com",
  ollama: "http://localhost:11434",
  gemini: "https://generativelanguage.googleapis.com",
  anthropic: "https://api.anthropic.com",
  openrouter: "https://openrouter.ai/api",
  "openai-compatible": "",
  vllm: "http://localhost:8000",
  sglang: "http://localhost:30000",
  "llama-server": "http://localhost:8080",
};

export const CLOUD_PROVIDERS = new Set(["openai", "gemini", "anthropic", "openrouter"]);

// Provider types that serve exactly one model per process (fixed at launch), so
// the served id can be detected from GET /v1/models and offered via the picker.
// Mirrors the CLOUD_PROVIDERS set so adding a new single-model type needs no other
// edit here.
export const SINGLE_MODEL_PROVIDERS = new Set(["vllm", "sglang", "llama-server"]);

// Provider types where the per-slot "Disable Thinking" toggle actually emits a
// knob (Ollama reasoning_effort, OpenRouter reasoning, vLLM/SGLang/llama-server
// chat_template_kwargs.enable_thinking, Gemini thinkingConfig.thinkingBudget).
// openai/anthropic/openai-compatible are omitted: an explicit disable 400s on
// current OpenAI/Anthropic models, so the toggle would be a dead control there.
//
// Type is necessary but not sufficient: the reranker slot additionally requires
// the judge method, since a cross-encoder does not generate (see
// showThinkingToggle in ProviderSlotEditor). Membership here does not by itself
// mean the toggle should render.
export const SUPPORTS_THINKING_TOGGLE = new Set([
  "ollama",
  "openrouter",
  "vllm",
  "sglang",
  "llama-server",
  "gemini",
]);

// Gemini and Anthropic use non-OpenAI request bodies, so extra_body does not
// apply to them; every other provider type is served by the OpenAI-compatible
// adapter and accepts it. Expressed as the exclusion so new compatible types
// (the common case) need no edit here.
export const NON_OPENAI_BODY_TYPES = new Set(["gemini", "anthropic"]);

// Model suggestions per provider type, keyed by slot name.
export const MODEL_HINTS: Record<string, Record<string, string>> = {
  openai: {
    embedding: "e.g. text-embedding-3-small",
    fact: "e.g. gpt-4o-mini",
    entity: "e.g. gpt-4o-mini",
    reranker: "a chat model for the LLM-judge path, e.g. gpt-4o-mini",
  },
  ollama: {
    embedding: "e.g. qwen3-embedding:0.6b, avoid nomic-embed-text (2048-tok ctx silently truncates long memories). See README → Recommended Models.",
    fact: "e.g. qwen3:8b, llama3.1, gemma2",
    entity: "e.g. qwen3:8b, llama3.1, gemma2",
    reranker: "a chat model for the LLM-judge path, e.g. qwen3:8b",
  },
  gemini: {
    embedding: "e.g. text-embedding-004",
    fact: "e.g. gemini-2.0-flash",
    entity: "e.g. gemini-2.0-flash",
    reranker: "a chat model for the LLM-judge path, e.g. gemini-2.0-flash",
  },
  anthropic: {
    embedding: "Not supported; use OpenAI or Ollama",
    fact: "e.g. claude-sonnet-4-6-20250514",
    entity: "e.g. claude-haiku-4-5-20251001",
    reranker: "a chat model for the LLM-judge path, e.g. claude-haiku-4-5-20251001",
  },
  openrouter: {
    embedding: "e.g. openai/text-embedding-3-small",
    fact: "e.g. anthropic/claude-sonnet-4-6",
    entity: "e.g. anthropic/claude-haiku-4-5",
    reranker: "a chat model for the LLM-judge path, e.g. anthropic/claude-haiku-4-5",
  },
  vllm: {
    embedding: "served model id, e.g. Qwen/Qwen3-Embedding-0.6B, or click Load Models to detect",
    fact: "served model id, e.g. Qwen/Qwen3-8B (thinking auto-disabled), or click Load Models",
    entity: "served model id, e.g. Qwen/Qwen3-8B (thinking auto-disabled), or click Load Models",
    reranker: "served reranker model id, e.g. BAAI/bge-reranker-v2-m3, or click Load Models to detect",
  },
  sglang: {
    embedding: "served model id, e.g. Qwen/Qwen3-Embedding-0.6B, or click Load Models to detect",
    fact: "served model id, e.g. Qwen/Qwen3-8B (thinking auto-disabled), or click Load Models",
    entity: "served model id, e.g. Qwen/Qwen3-8B (thinking auto-disabled), or click Load Models",
    reranker: "served reranker model id, e.g. BAAI/bge-reranker-v2-m3, or click Load Models to detect",
  },
  "llama-server": {
    embedding: "served model id, e.g. Qwen3-Embedding-0.6B-Q8_0, or click Load Models to detect",
    fact: "served model id from GET /v1/models, or click Load Models",
    entity: "served model id from GET /v1/models, or click Load Models",
    reranker: "served reranker model id, e.g. bge-reranker-v2-m3 or ms-marco-MiniLM-L6, or click Load Models",
  },
  "openai-compatible": {
    embedding: "the model id exposed by your endpoint",
    fact: "the model id exposed by your endpoint",
    entity: "the model id exposed by your endpoint",
    reranker: "the rerank model id exposed by your endpoint",
  },
};

// Heuristic: model names containing these substrings are embedding-only models.
export const EMBEDDING_MODEL_PATTERNS = ["embed", "bge-", "e5-", "gte-", "minilm", "arctic-embed"];

export function isEmbeddingModel(name: string): boolean {
  const lower = name.toLowerCase();
  return EMBEDDING_MODEL_PATTERNS.some((p) => lower.includes(p));
}

export function maskUrl(url: string): string {
  try {
    const u = new URL(url);
    if (
      u.hostname === "localhost" ||
      u.hostname === "127.0.0.1" ||
      u.hostname === "0.0.0.0"
    ) {
      return url;
    }
    return `${u.protocol}//${u.hostname}/***`;
  } catch {
    return url;
  }
}

export interface HeaderRow {
  key: string;
  value: string;
}

export interface EditFormState {
  type: string;
  url: string;
  model: string;
  api_key: string;
  timeout: string;
  // Opt-in embedding output dimension as raw text (blank = model's native size).
  // Embedding slot only; sent on save only when non-blank.
  dimension: string;
  // Custom HTTP headers as an ordered, editable list. Pre-existing headers are
  // seeded with a blank value (their stored value is masked); a blank value on
  // save tells the backend to keep the stored one.
  custom_headers: HeaderRow[];
  // Raw JSON text for the slot's extra_body (merged onto every OpenAI-compatible
  // request body). Stored as text so an in-progress/invalid edit is preserved
  // and validated on save; blank means "no extra_body".
  extra_body: string;
  // Whether nram suppresses the model's reasoning pass on completions. Checked
  // (true) = disable thinking, the default. Only meaningful for the types in
  // SUPPORTS_THINKING_TOGGLE; sent on save only for those.
  disable_thinking: boolean;
}

// parseExtraBody validates the Extra Body textarea. Blank is valid (no body).
// Otherwise it must parse to a plain JSON object; arrays, primitives, and
// syntax errors are rejected so the backend never stores a non-object.
export function parseExtraBody(text: string): { value?: Record<string, unknown>; error?: string } {
  const trimmed = text.trim();
  if (trimmed === "") {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return { error: "Not valid JSON." };
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { error: "Must be a JSON object, e.g. {\"chat_template_kwargs\": {\"enable_thinking\": false}}." };
  }
  return { value: parsed as Record<string, unknown> };
}

// headerRowsToRecord drops rows with a blank name and collapses the rest into a
// map. Blank values are preserved (sent as "") so the backend keeps the stored
// value for pre-existing headers.
export function headerRowsToRecord(rows: HeaderRow[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const row of rows) {
    const name = row.key.trim();
    if (name) {
      out[name] = row.value;
    }
  }
  return out;
}

// initialFormStateForSlot derives the edit-form seed from a slot's live config.
// Pre-existing headers seed with blank values (their stored values are masked;
// a blank value on save preserves the stored one). extra_body round-trips
// pretty-printed. The thinking checkbox defaults checked unless the slot stored
// an explicit false.
export function initialFormStateForSlot(slot: {
  configured?: boolean;
  type?: string;
  url?: string;
  model?: string;
  timeout?: number | null;
  dimension?: number | null;
  custom_header_keys?: string[];
  extra_body?: Record<string, unknown> | null;
  disable_thinking?: boolean | null;
}): EditFormState {
  return {
    type: slot.configured ? (slot.type ?? "") : "",
    url: slot.configured ? (slot.url ?? "") : "",
    model: slot.configured ? (slot.model ?? "") : "",
    api_key: "",
    timeout: slot.timeout != null ? String(slot.timeout) : "",
    dimension: slot.dimension != null && slot.dimension > 0 ? String(slot.dimension) : "",
    custom_headers: (slot.custom_header_keys ?? []).map((key) => ({ key, value: "" })),
    extra_body:
      slot.extra_body && Object.keys(slot.extra_body).length > 0
        ? JSON.stringify(slot.extra_body, null, 2)
        : "",
    disable_thinking: slot.disable_thinking ?? true,
  };
}
