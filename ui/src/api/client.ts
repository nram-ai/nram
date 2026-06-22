import { decodeUserFromJWT, type UserInfo } from "../context/AuthContext";
import { triggerBlobDownload, parseAttachmentFilename } from "../lib/download";

/** Base URL is auto-detected: same origin in production, proxied in dev. */
const BASE_URL = "/v1";

/**
 * Header set by the auth middleware when it slides a session JWT forward.
 * The fetch wrapper rotates localStorage transparently when this is present.
 */
const SESSION_REFRESH_HEADER = "X-Refreshed-Token";

/** Apply a freshly-issued session JWT from the X-Refreshed-Token header. */
function applyRefreshedToken(token: string): void {
  localStorage.setItem("nram_token", token);
  const user = decodeUserFromJWT(token);
  if (user) {
    localStorage.setItem("nram_user", JSON.stringify(user));
  }
}

export class APIError extends Error {
  constructor(
    public status: number,
    public body: unknown,
  ) {
    super(APIError.deriveMessage(status, body));
    this.name = "APIError";
  }

  // Surface the server's descriptive message when present so callers that show
  // err.message get "qdrant is not configured…" instead of a bare "API error
  // 400". Handles nram's standard envelope ({error:{code,message}}), a
  // string-valued error field, and a top-level message; falls back to the
  // generic status string for opaque/non-JSON bodies.
  private static deriveMessage(status: number, body: unknown): string {
    if (body && typeof body === "object") {
      const err = (body as { error?: unknown }).error;
      if (typeof err === "string" && err) return err;
      if (err && typeof err === "object") {
        const m = (err as { message?: unknown }).message;
        if (typeof m === "string" && m) return m;
      }
      const msg = (body as { message?: unknown }).message;
      if (typeof msg === "string" && msg) return msg;
    }
    return `API error ${status}`;
  }
}

function getAuthHeaders(): Record<string, string> {
  const token = localStorage.getItem("nram_token");
  if (token) {
    return { Authorization: `Bearer ${token}` };
  }
  return {};
}

/**
 * Prefix used to forward in-progress custom provider headers on GET endpoints
 * (where there is no request body). Must match forwardedHeaderPrefix on the
 * server, which strips it and forwards the remainder as the target header.
 */
const FORWARDED_PROVIDER_HEADER_PREFIX = "X-Nram-Provider-Header-";

/** Wraps a custom-header map into prefixed request headers, or undefined when empty. */
function forwardedProviderHeaders(
  headers?: Record<string, string>,
): Record<string, string> | undefined {
  if (!headers) return undefined;
  const out: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers)) {
    if (!name || !value) continue;
    out[`${FORWARDED_PROVIDER_HEADER_PREFIX}${name}`] = value;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

export async function request<T>(
  method: string,
  path: string,
  body?: unknown,
  extraHeaders?: Record<string, string>,
): Promise<T> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...getAuthHeaders(),
    ...extraHeaders,
  };

  const res = await fetch(`${BASE_URL}${path}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  // Sliding-expiry refresh: when the auth middleware detects an aged session
  // JWT, it mints a fresh one and emits it on this header. Capture before
  // any branching on res.ok so refresh still applies to non-2xx responses
  // (the middleware ran successfully if the response wasn't 401).
  const refreshed = res.headers.get(SESSION_REFRESH_HEADER);
  if (refreshed) {
    applyRefreshedToken(refreshed);
  }

  if (!res.ok) {
    // On 401, the token is invalid or expired; clear it and redirect to login.
    if (res.status === 401) {
      localStorage.removeItem("nram_token");
      localStorage.removeItem("nram_user");
      if (window.location.pathname !== "/login" && window.location.pathname !== "/setup") {
        window.location.href = "/login";
        return new Promise<T>(() => {}); // never resolves; page is navigating
      }
    }
    // On 403, the token is valid but role is insufficient.
    if (res.status === 403) {
      const msg = await res.text();
      throw new APIError(403, msg || "forbidden: insufficient permissions");
    }
    const errText = await res.text();
    let errBody: unknown;
    try {
      errBody = JSON.parse(errText);
    } catch {
      errBody = errText;
    }
    throw new APIError(res.status, errBody);
  }

  if (res.status === 204) {
    return undefined as T;
  }

  return res.json() as Promise<T>;
}

/**
 * Fetch a root-level public endpoint as raw text. Unlike request(), it does not
 * force application/json or JSON-parse the body, and it hits paths outside the
 * /v1 BASE_URL.
 */
async function fetchText(path: string, init?: RequestInit): Promise<string> {
  const res = await fetch(path, init);
  if (!res.ok) {
    throw new APIError(res.status, await res.text());
  }
  return res.text();
}

/**
 * Fetch the server's Prometheus metrics as raw exposition text from the public
 * /metrics endpoint. The auth header is sent but harmless: /metrics is
 * unauthenticated and ignores it.
 */
export function fetchMetricsText(): Promise<string> {
  return fetchText("/metrics", { headers: getAuthHeaders() });
}

/** Flavor of the agent instructions/rules served at /instructions. */
export type InstructionsFormat = "claude" | "agents" | "cursor";

/**
 * Fetch the canonical agent instructions/rules as plain text from the public
 * /instructions endpoint. This is the single source of truth for the snippets
 * shown on the MCP config page; the backend owns the wording.
 */
export function getInstructions(format: InstructionsFormat): Promise<string> {
  return fetchText(`/instructions?format=${format}`);
}

// --- Type definitions ---

export interface SetupStatus {
  setup_complete: boolean;
  backend: "sqlite" | "postgres";
}

export interface SetupRequest {
  email: string;
  password: string;
}

export interface SetupResponse {
  user: User;
  api_key: string;
  token: string;
  message: string;
}

export interface ProjectMemoryCount {
  project_id: string;
  project_name: string;
  count: number;
}

export interface EnrichmentQueueStats {
  pending: number;
  processing: number;
  failed: number;
}

export interface DashboardData {
  total_memories: number;
  total_projects: number;
  total_users: number;
  total_entities: number;
  total_organizations: number;
  memories_by_project: ProjectMemoryCount[];
  enrichment_queue?: EnrichmentQueueStats;
}

/**
 * Privacy: this interface intentionally does NOT carry a memory content
 * preview. The previous design returned the first 100 chars of memory.content
 * in `summary`, which leaked content into the dashboard UI. Length is exposed
 * as a size hint instead.
 */
export interface ActivityEvent {
  id: string;
  type: string;
  project_id?: string;
  user_id?: string;
  length_chars?: number;
  // Populated only on the self-tier (caller's own data); first ~100 chars
  // of memory content as a preview. Org/system tiers stay aggregate-only.
  preview?: string;
  timestamp: string;
}

// memoryRowLabel renders the primary display text for a memory row in the
// dashboard activity feed and the analytics ranked-list tables. Self-tier
// shows a content preview when present; org/system tiers fall back to a
// length hint, then to a truncated ID. Both consumers go through this so
// the privacy-tier rendering rule lives in one place.
export function memoryRowLabel(row: {
  preview?: string;
  length_chars?: number;
  id: string;
}): string {
  if (row.preview) return row.preview;
  if (row.length_chars != null) return `${row.length_chars.toLocaleString()} chars`;
  return row.id.slice(0, 8) + "…";
}

export interface ActivityResponse {
  events: ActivityEvent[];
}

export interface StoreMemoryRequest {
  content: string;
  tags?: string[];
  source?: string;
  metadata?: Record<string, unknown>;
  options?: {
    enrich?: boolean;
    extract?: boolean;
    ttl?: string;
  };
}

export interface StoredMemory {
  id: string;
  project_id: string;
  project_slug: string;
  path: string;
  content: string;
  tags?: string[];
  enriched: boolean;
  enrichment_queued: boolean;
  latency_ms: number;
}

/** Source format accepted by the bulk import endpoint. */
export type ImportFormat = "nram" | "mem0" | "zep";

/** Result of a server-side bulk import (POST .../memories/import). */
export interface ImportResponse {
  imported: number;
  skipped: number;
  entities_imported: number;
  relationships_imported: number;
  lineage_imported: number;
  errors: { index: number; message: string }[];
  latency_ms: number;
}

/**
 * Coarse, server-assigned provenance category. Authoritative discriminator for
 * dream syntheses (the free-form `source` string no longer carries "dream").
 */
export type MemoryOrigin = "user" | "dream" | "import";

export interface Memory {
  id: string;
  namespace_id?: string;
  content: string;
  embedding_dim?: number | null;
  source: string | null;
  origin: MemoryOrigin;
  tags: string[];
  confidence?: number;
  importance?: number;
  access_count?: number;
  last_accessed?: string | null;
  expires_at?: string | null;
  superseded_by?: string | null;
  superseded_at?: string | null;
  enriched: boolean;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  deleted_at?: string | null;
  purge_after?: string | null;
  parent_id?: string | null;
  /**
   * Populated when query augmentation was enabled at the time this memory's
   * vector was written. Null when the row was embedded against raw content;
   * the backfill query keys off augmented_embedding_at IS NULL.
   */
  augmented_queries?: string[] | null;
  augmented_embedding_at?: string | null;
  /**
   * Populated only when the list endpoint was called with
   * group_by_parent=true. Carries enrichment-derived child memories so a
   * parent and its extracted facts always render together.
   */
  children?: Memory[];
}

export interface RecallResult {
  id: string;
  project_id: string;
  project_slug: string;
  path: string;
  content: string;
  tags: string[];
  source: string | null;
  origin: MemoryOrigin;
  score: number;
  similarity?: number | null;
  confidence?: number;
  access_count?: number;
  enriched?: boolean;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface RecallRelationship {
  source_id: string;
  target_id: string;
  relation: string;
  weight: number;
}

export interface RecallResponse {
  memories: RecallResult[];
  graph?: {
    entities: RecallEntity[];
    relationships: RecallRelationship[];
  };
  total_searched: number;
  latency_ms: number;
}

export interface RecallEntity {
  id: string;
  name: string;
  type: string;
}

export interface RecallRequest {
  query: string;
  limit?: number;
  tags?: string[];
  threshold?: number;
  include_graph?: boolean;
  graph_depth?: number;
}

export interface AskRequest {
  query: string;
  /** Optional project slug. Omit for a wide cross-project synthesis; supply a
   * slug to scope to that project + global + about_me. */
  project?: string;
}

export interface AskSource {
  memory_id: string;
  project_slug: string;
  /** Absolute vector cosine to the query. Absent for sources that entered via
   * graph or sibling expansion (no direct query match). */
  score?: number;
  /** Footnote number ([1], [2], …) carried inline in the answer; absent on the
   * uncited fallback. */
  citation?: number;
}

export interface AskSynthesisMeta {
  latency_ms: number;
  neighborhood_size: number;
  synthesis_failed?: boolean;
}

export interface AskResponse {
  answer: string;
  sources: AskSource[];
  confidence: number;
  synthesis_meta: AskSynthesisMeta;
}

export interface MemoryListParams {
  limit?: number;
  offset?: number;
  /** AND semantics: memory must contain all listed tags */
  tags?: string[];
  /** RFC3339 or YYYY-MM-DD */
  date_from?: string;
  /** RFC3339 or YYYY-MM-DD; inclusive of the entire day when YYYY-MM-DD */
  date_to?: string;
  /** "true" → enriched only, "false" → not-enriched only, undefined → no filter */
  enriched?: "true" | "false";
  /** restrict to a single provenance; undefined → no filter */
  origin?: MemoryOrigin;
  /** "true" → augmented embedding only, "false" → not-augmented only, undefined → no filter */
  augmented?: "true" | "false";
  /** "true" surfaces superseded (paraphrase/contradiction loser) rows; default hides them */
  include_superseded?: "true";
  /** case-insensitive substring against the source column */
  source?: string;
  /** case-insensitive substring against the content column */
  search?: string;
  /**
   * Parent-anchored list: pagination is over non-enrichment parents and each
   * row carries its enrichment-derived children inline. The total reflects
   * parent count, not memory count.
   */
  group_by_parent?: boolean;
}

export interface ListIDsResponse {
  ids: string[];
  truncated: boolean;
  total_matching: number;
}

export interface MemoryListResponse {
  data: Memory[];
  pagination: {
    total: number;
    limit: number;
    offset: number;
  };
}

export interface MemoryUpdateRequest {
  content?: string;
  tags?: string[];
  metadata?: Record<string, unknown>;
}

export interface ForgetRequest {
  ids?: string[];
  tags?: string[];
  hard?: boolean;
}

export interface ForgetResponse {
  deleted: number;
  latency_ms: number;
}

export interface MoveResult {
  old_id: string;
  new_id: string;
}

export interface MoveResponse {
  moved: number;
  results: MoveResult[];
  latency_ms: number;
}

export interface EnrichResponse {
  queued: number;
  skipped: number;
  latency_ms: number;
}

export interface ExportData {
  version: string;
  exported_at: string;
  project: { id: string; name: string; slug: string };
  memories: ExportMemory[];
  entities?: ExportEntity[];
  relationships?: ExportRelationship[];
  stats?: { memory_count: number; entity_count: number; relationship_count: number };
}

export interface ExportMemory {
  id: string;
  content: string;
  tags: string[];
  source?: string | null;
  confidence?: number;
  importance?: number;
  enriched: boolean;
  metadata?: Record<string, unknown>;
  lineage?: { parent_id: string | null; relation: string }[];
  created_at: string;
}

export interface ExportEntity {
  id: string;
  name: string;
  type: string;
  canonical: string;
  properties?: Record<string, unknown>;
  mention_count: number;
}

export interface ExportRelationship {
  id: string;
  source_id: string;
  target_id: string;
  relation: string;
  weight: number;
  valid_from?: string;
  valid_until?: string | null;
}

export interface EnrichRequest {
  ids?: string[];
  all?: boolean;
  priority?: number;
}

export interface OrgUser {
  id: string;
  email: string;
  display_name?: string;
  role: string;
}

export interface Organization {
  id: string;
  namespace_id?: string;
  name: string;
  slug: string;
  settings?: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  user_count?: number;
  memory_count?: number;
  project_count?: number;
  users?: OrgUser[];
  owners?: OrgUser[];
}

export interface CreateOrgRequest {
  name: string;
  slug: string;
}

export interface UpdateOrgRequest {
  name?: string;
  slug?: string;
  settings?: Record<string, unknown>;
}

export interface APIKey {
  id: string;
  user_id?: string;
  key_prefix: string;
  name: string;
  scopes: string[];
  last_used?: string | null;
  expires_at?: string | null;
  created_at: string;
}

export interface User {
  id: string;
  namespace_id?: string;
  email: string;
  display_name: string;
  role: string;
  org_id: string;
  organization?: { id: string; name: string };
  last_login?: string;
  disabled_at: string | null;
  // Settings is typed loosely (Record<string, unknown>) rather than as
  // UserSettings because legacy rows may still carry the stripped
  // ranking_weights field until migration 26 runs. Editors should narrow
  // through UserSettings at use-site.
  settings: Record<string, unknown>;
  api_keys?: APIKey[];
  created_at: string;
  updated_at: string;
}

export interface CreateUserRequest {
  email: string;
  display_name?: string;
  password: string;
  role: string;
  organization_id?: string;
}

export interface UpdateUserRequest {
  display_name?: string;
  role?: string;
  // Settings stays loosely typed (Record<string, unknown>) because the
  // org-update path shares this request type and tolerates any JSON shape;
  // the server validates the actual fields it cares about. Forms should
  // construct payloads matching UserSettings; see that interface for the
  // accepted keys (ranking_weights at user scope is rejected with 400).
  settings?: Record<string, unknown>;
}

export interface GenerateAPIKeyRequest {
  label: string;
  scopes?: string[];
  expires_at?: string;
}

export interface GenerateAPIKeyResponse {
  id: string;
  key: string;
  label: string;
  prefix: string;
  scopes: string[];
  expires_at: string | null;
  created_at: string;
}

// SystemRankingWeights is the fully-resolved view of the ranking weight
// settings, used as the placeholder/effective baseline in the project edit
// panel. All fields are required because the system layer always has a
// value (operator override or built-in default). `origin` is the
// project-affinity term: it lifts candidates whose home namespace is the
// recall's primary project; default 0 leaves ranking math unchanged.
// `mmr_lambda` is the MMR redundancy-aware rerank trade-off: not a linear
// combination weight, but it shares the cascade and override surface
// because operators tune it through the same per-project ranking_weights
// JSON. 1.0 disables MMR (pure relevance order); 0.7-0.8 is the standard
// mild-nudge range.
export interface SystemRankingWeights {
  similarity: number;
  recency: number;
  importance: number;
  frequency: number;
  graph_relevance: number;
  confidence: number;
  origin: number;
  mmr_lambda: number;
}

// ProjectRankingWeights mirrors the canonical sparse override shape parsed
// by service.ParseRankingOverride. Every field is optional so the editor
// can persist partial overrides; unset fields fall through to the system-
// level ranking.weight.* setting at recall time. The legacy `relevance`
// field has been migrated in-place by 000025/000022; readers that need to
// defend against pre-migration cached data should use a type-narrowing
// cast at the use site rather than carrying a deprecated alias on the
// canonical interface.
export interface ProjectRankingWeights {
  similarity?: number;
  recency?: number;
  importance?: number;
  frequency?: number;
  graph_relevance?: number;
  confidence?: number;
  origin?: number;
  mmr_lambda?: number;
}

// ProjectSettings is the full per-project override blob. All fields are
// optional so the editor can write partial payloads; the backend's parsers
// treat absent fields as "no override" and merge in the system default.
export interface ProjectSettings {
  dedup_threshold?: number;
  enrichment_enabled?: boolean;
  dreaming_enabled?: boolean;
  ranking_weights?: ProjectRankingWeights;
  graph_center_gravity?: number;
  graph_charge_strength?: number;
  graph_link_distance?: number;
}

// UserSettings carries the per-user JSON overrides honored by the cascade
// resolver when a memory lives in the user's personal namespace. Note that
// ranking_weights is deliberately absent: the cascade lands at project for
// weights, and the API rejects user-scope ranking_weights with a 400.
export interface UserSettings {
  dedup_threshold?: number;
  enrichment_enabled?: boolean;
  dreaming_enabled?: boolean;
}

export interface ProjectOwner {
  id: string;
  email: string;
}

export interface ProjectOrganization {
  id: string;
  name: string;
}

export interface Project {
  id: string;
  namespace_id?: string;
  owner_namespace_id?: string;
  name: string;
  slug: string;
  path?: string;
  description: string;
  memory_count?: number;
  entity_count?: number;
  relationship_count?: number;
  default_tags: string[];
  settings: ProjectSettings;
  owner?: ProjectOwner;
  organization?: ProjectOrganization;
  org_id?: string;
  // reserved is a computed, read-only flag set by the backend for the reserved
  // per-user tiers (global, about_me): they cannot be deleted and their
  // name/description are managed by nram.
  reserved?: boolean;
  created_at: string;
  updated_at: string;
}

export interface ProjectUpdateRequest {
  name?: string;
  slug?: string;
  description?: string;
  default_tags?: string[];
  settings?: Partial<ProjectSettings>;
}

export interface AdminCreateProjectRequest {
  name: string;
  slug: string;
  owner_namespace_id: string;
  description?: string;
  default_tags?: string[];
  settings?: Partial<ProjectSettings>;
}

// ProceduralEntry is a verbatim standing-rule in the procedural memory tier.
// It is not a project and not a memory; it is fetched whole and never enriched
// or surfaced by recall.
export interface ProceduralEntry {
  id: string;
  namespace_id?: string;
  content: string;
  title: string;
  category: string;
  tags: string[];
  priority: number;
  enabled: boolean;
  origin?: string;
  created_at: string;
  updated_at: string;
}

export interface CreateProceduralRequest {
  content: string;
  title?: string;
  category?: string;
  tags?: string[];
  priority?: number;
  enabled?: boolean;
}

export interface UpdateProceduralRequest {
  content?: string;
  title?: string;
  category?: string;
  tags?: string[];
  priority?: number;
  enabled?: boolean;
}

// ProceduralExportEntry is one entry in an export/import payload. It mirrors the
// Go service.ProceduralExportEntry struct.
export interface ProceduralExportEntry {
  id: string;
  content: string;
  title: string;
  category: string;
  tags: string[];
  priority: number;
  enabled: boolean;
  metadata?: unknown;
  created_at: string;
}

// ProceduralExportData is the versioned envelope returned by the export endpoint
// and accepted by the import endpoint.
export interface ProceduralExportData {
  version: string;
  exported_at: string;
  entries: ProceduralExportEntry[];
  stats: { count: number };
}

// ProceduralImportResult summarizes an import run.
export interface ProceduralImportResult {
  imported: number;
  updated: number;
  skipped: number;
  errors: { index: number; message: string }[];
}

export interface WebhookCreateRequest {
  url: string;
  events: string[];
  scope?: string;
  secret?: string | null;
  active?: boolean;
}

export interface WebhookUpdateRequest {
  url: string;
  events: string[];
  scope?: string;
  secret?: string | null;
  active?: boolean;
}

export interface NamespaceNode {
  id: string;
  name: string;
  slug: string;
  kind: string;
  path: string;
  depth: number;
  children: NamespaceNode[];
}

export interface ProviderSlot {
  slot: string;
  /** Human label and help text, served from the backend canonical slot list. */
  label: string;
  description: string;
  /** True for slots required for enrichment (embedding/fact/entity). */
  required: boolean;
  configured: boolean;
  type: string;
  url: string;
  model: string;
  dimensions?: number | null;
  /**
   * Effective context window in tokens: the actual budget the pipeline
   * can spend. For Ollama slots this is min(model GGUF max, runtime
   * num_ctx); for OpenRouter it equals the model's reported
   * context_length. Populated only for providers that report it via API
   * (Ollama via /api/show + /api/ps, OpenRouter via /models). Other
   * providers (OpenAI, Anthropic, Gemini, Custom) leave this null and
   * the UI shows a "see provider docs" placeholder.
   */
  context_window?: number | null;
  /**
   * Model's GGUF-declared maximum context, set only when it is strictly
   * greater than context_window (i.e., an Ollama-side num_ctx is the
   * binding constraint). When present, the UI surfaces it as a muted
   * "(model max N)" suffix so users see the headroom story.
   */
  context_window_max?: number | null;
  timeout?: number | null;
  status?: string;
  latency_ms?: number | null;
  /**
   * Whether a non-empty api_key is stored for this slot. The key value is
   * never returned; this drives the "key configured" indicator and the honest
   * "leave blank to keep" affordance.
   */
  api_key_set?: boolean;
  /**
   * Names (sorted) of the custom HTTP headers configured for this slot. Values
   * are never returned because they may carry secrets (e.g. a proxy token).
   */
  custom_header_keys?: string[];
  /**
   * The slot's configured extra_body, returned verbatim. Unlike custom headers
   * these values are not secret (e.g. chat_template_kwargs), so the UI renders
   * and re-edits them.
   */
  extra_body?: Record<string, unknown> | null;
}

export interface UpdateProviderSlotRequest {
  type: string;
  url?: string;
  model?: string;
  api_key?: string;
  timeout?: number;
  /**
   * Arbitrary HTTP headers attached to every outbound request to this slot's
   * provider host (for proxies/gateways). The map is the new full set (omitted
   * names are removed); a header sent with a blank value keeps its previously
   * stored value, so masked headers survive a re-save.
   */
  custom_headers?: Record<string, string>;
  /**
   * Merged onto the top level of every OpenAI-compatible request body (chat and
   * embeddings), mirroring the OpenAI SDK's extra_body. The map is the new full
   * set; omitting it clears any stored value. Ignored for Gemini/Anthropic.
   */
  extra_body?: Record<string, unknown>;
  /**
   * Request-only flag: when true, the stored api_key is cleared instead of
   * preserved-on-blank (e.g. switching a slot to header-only auth).
   */
  clear_api_key?: boolean;
  /**
   * Set to true to authorize the destructive embedding-model switch
   * cascade (truncate every vector table, NULL embedding_dim columns,
   * force re-embed). Without this flag a model-change request returns
   * HTTP 409 with row counts so the UI can show a confirmation modal.
   */
  confirm_invalidate?: boolean;
}

/**
 * UpdateProviderSlotResult is the server's response to PUT /providers/{slot}.
 * needs_confirmation=true means the embedding model changed and the cascade
 * was withheld pending user authorization (re-submit with
 * confirm_invalidate=true).
 */
export interface UpdateProviderSlotResult {
  needs_confirmation?: boolean;
  old_model?: string;
  new_model?: string;
  memories_affected?: number;
  entities_affected?: number;
  memory_jobs_enqueued?: number;
  entity_reembed_queued?: boolean;
}

// The backend returns the ordered list of slots (one entry per canonical
// provider slot), each carrying its own identity and metadata.
export type ProviderConfigResponse = ProviderSlot[];

export interface TestProviderResult {
  success: boolean;
  message?: string;
  latency_ms: number;
}

export interface OllamaModel {
  name: string;
  size: number;
  modified_at: string;
}

export interface Setting {
  key: string;
  value: unknown;
  scope: string;
  updated_by?: string;
  updated_at: string;
}

export interface SettingSchema {
  key: string;
  type: string;
  default_value: unknown;
  description: string;
  category: string;
  enum_values?: string[];
  requires_restart?: boolean;
  // Recognized values: "sqlite", "postgres", "hnsw", "pgvector", "qdrant".
  // Empty / omitted means the setting applies regardless of backend.
  applies_to_backend?: string[];
  // min/max/step describe the operator-tunable range for numeric settings.
  // The UI binds input increments to step so editor controls stay
  // schema-driven. Pointer-typed on the server (omitted vs. zero are
  // distinguishable in JSON), so undefined here means "not constrained".
  min?: number;
  max?: number;
  step?: number;
}

// SettingSubSection binds one setting category to a heading inside a group.
// label/description are optional: a single-subsection group with neither
// renders its items flat under the group header.
export interface SettingSubSection {
  category: string;
  label?: string;
  description?: string;
}

// SettingGroup is one tab in the admin Settings UI. The group taxonomy is the
// backend's single source of truth (GET /admin/settings?groups=true); the page
// renders it generically rather than hardcoding the structure.
export interface SettingGroup {
  id: string;
  label: string;
  description?: string;
  // Hide the whole group (and its settings) when enrichment is unavailable.
  requires_enrichment?: boolean;
  // Hide the whole group unless the active database backend is in this list.
  requires_backend?: string[];
  subsections: SettingSubSection[];
}

export interface Webhook {
  id: string;
  url: string;
  events: string[];
  scope: string;
  active: boolean;
  last_fired?: string | null;
  last_status?: number | null;
  failure_count: number;
  created_at: string;
  updated_at: string;
}

/**
 * Privacy: this interface intentionally does NOT carry the memory body.
 * The previous design returned `content`, leaking memory text into the
 * dashboard layer. The backend now returns `length_chars` as a size hint
 * and the body stays in the database.
 */
export interface MemoryRankItem {
  id: string;
  access_count: number;
  project_id?: string | null;
  length_chars: number;
  // Populated only on the self-tier (caller's own data); first ~100 chars
  // of memory content as a preview. Org/system tiers stay aggregate-only.
  preview?: string;
  created_at: string;
}

// Aggregate types for tier-B (org) and tier-C (system) responses. See
// internal/api/aggregate_types.go for the Go-side authoritative shapes.
// These intentionally carry NO content fields: only counts, distributions,
// and tenancy-metadata labels (org name, project name, type label).

export interface HistogramBucket {
  range: string;
  count: number;
}

export interface DailyBucket {
  date: string;
  count: number;
}

export interface TypeBucket {
  type: string;
  count: number;
}

export interface OrgAggregate {
  org_id: string;
  org_name: string;
  total_memories: number;
  total_users: number;
  total_projects: number;
  total_entities: number;
}

export interface OrgAnalyticsData {
  memory_counts: AnalyticsData["memory_counts"];
  recall_distribution: HistogramBucket[];
  enrichment_stats: AnalyticsData["enrichment_stats"];
  user_breakdown: UserAggregate[];
  entity_type_histogram: TypeBucket[];
  relationship_type_histogram: TypeBucket[];
}

export interface SystemAnalyticsData {
  total_memory_counts: AnalyticsData["memory_counts"];
  recall_distribution: HistogramBucket[];
  enrichment_stats: AnalyticsData["enrichment_stats"];
  org_breakdown: OrgAggregate[];
  entity_type_histogram: TypeBucket[];
  relationship_type_histogram: TypeBucket[];
}

export interface UserAggregate {
  user_id: string;
  email: string;
  total_memories: number;
  total_projects: number;
  total_entities: number;
}

export interface OrgDashboardData {
  total_memories: number;
  total_projects: number;
  total_users: number;
  total_entities: number;
  user_breakdown: UserAggregate[];
  enrichment_queue?: EnrichmentQueueStats;
}

export interface SystemDashboardData {
  total_memories: number;
  total_projects: number;
  total_users: number;
  total_entities: number;
  total_organizations: number;
  org_breakdown: OrgAggregate[];
  enrichment_queue?: EnrichmentQueueStats;
}

export interface AuditEvent {
  id: string;
  occurred_at: string;
  actor_user_id?: string;
  actor_role?: string;
  action: string;
  target_type?: string;
  target_id?: string;
  target_org_id?: string;
  source_ip?: string;
  user_agent?: string;
  details?: unknown;
}

export interface OrgActivityResponse {
  daily_creation: DailyBucket[];
  audit_events: AuditEvent[];
}

export interface SystemActivityResponse {
  daily_creation: DailyBucket[];
  audit_events: AuditEvent[];
}

export interface AnalyticsData {
  memory_counts: {
    total: number;
    active: number;
    deleted: number;
    enriched: number;
  };
  most_recalled: MemoryRankItem[];
  least_recalled: MemoryRankItem[];
  dead_weight: MemoryRankItem[];
  enrichment_stats: {
    total_processed: number;
    success_rate: number;
    failure_rate: number;
    avg_latency_ms: number;
  };
}

export type UsageGroupBy =
  | "operation"
  | "model"
  | "provider"
  | "user"
  | "project"
  | "org"
  | "success"
  | "error_code"
  | "request_id";

export interface UsageGroup {
  key: string;
  tokens_input: number;
  tokens_output: number;
  call_count: number;
  success_count: number;
  error_count: number;
  avg_latency_ms: number;
}

export interface UsageReport {
  groups: UsageGroup[];
  totals: {
    tokens_input: number;
    tokens_output: number;
    call_count: number;
  };
}

// Persisted server-side under the `usage.cost_rates` settings key.
// Applied client-side to UsageGroup token counts for breakdown columns.
export interface CostRate {
  key: string;
  inputCostPer1k: number;
  outputCostPer1k: number;
}

export interface SQLiteInfo {
  file_path: string;
  file_size_bytes: number;
}

export interface PostgresInfo {
  host: string;
  database: string;
  pgvector_version?: string;
  active_connections: number;
  idle_connections: number;
  max_connections: number;
}

export interface DataCounts {
  memories: number;
  entities: number;
  projects: number;
  users: number;
  organizations: number;
  vectors: number;
}

export interface DatabaseInfo {
  backend: string;
  version: string;
  sqlite?: SQLiteInfo;
  postgres?: PostgresInfo;
  data_counts: DataCounts;
}

export interface ConnectionTestResult {
  success: boolean;
  message: string;
  pgvector_installed: boolean;
  latency_ms: number;
}

export interface MigrationStats {
  inserted?: Record<string, number>;
  skipped_orphans?: Record<string, number>;
  skipped_updates?: Record<string, number>;
  reset_stuck?: Record<string, number>;
}

export interface MigrationStatus {
  status: string;
  message: string;
  stats?: MigrationStats;
}

// MigrationStartAck is the 202 response when a background migration is kicked
// off. The real result arrives over SSE, not on this response.
export interface MigrationStartAck {
  status: string;
}

export type VectorMigrationDirection = "to_qdrant" | "from_qdrant";

export interface VectorMigrationDimStat {
  kind: string;
  dimension: number;
  source_count: number;
  dest_count: number;
}

export interface VectorMigrationResult {
  direction: VectorMigrationDirection;
  dry_run: boolean;
  memory_count: number;
  entity_count: number;
  mismatch: boolean;
  verify: VectorMigrationDimStat[];
}

export interface PreflightCheck {
  name: string;
  status: "ok" | "warn" | "error";
  message: string;
  remediation?: string;
  table_counts?: Record<string, number>;
}

export interface PreflightReport {
  ok: boolean;
  checks: PreflightCheck[];
}

export type ResetMode = "truncate" | "drop_schema";

export interface ResetResult {
  status: string;
  message: string;
  mode: ResetMode;
  tables_dropped?: string[];
}

export interface OrphanCount {
  table: string;
  column: string;
  references: string;
  count: number;
}

export interface AuditError {
  table: string;
  column: string;
  message: string;
}

export interface MigrationAudit {
  backend: string;
  total_orphans: number;
  orphans: OrphanCount[];
  errors?: AuditError[];
}

// --- Dreaming Types ---

// Mirrors SubPhaseSummary in internal/dreaming/runner.go. Currently emitted
// only by the consolidation phase (backfill_audit / reinforce / consolidate).
export interface DreamSubPhaseSummary {
  name: string;
  tokens_used: number;
  slice_cap?: number;
  has_residual?: boolean;
}

// Mirrors PhaseSummaryEntry in internal/dreaming/runner.go.
export interface DreamPhaseSummary {
  phase: string;
  tokens_used: number;
  operations: number;
  duration_ms: number;
  slice_cap?: number;
  error?: string;
  skipped?: boolean;
  has_residual?: boolean;
  residual_reason?: string;
  residual_detail?: Record<string, unknown>;
  sub_phases?: DreamSubPhaseSummary[];
}

export interface DreamCycle {
  id: string;
  project_id: string;
  namespace_id: string;
  status: string;
  phase: string;
  tokens_used: number;
  token_budget: number;
  phase_summary: DreamPhaseSummary[] | null;
  error: string | null;
  started_at: string | null;
  completed_at: string | null;
  heartbeat_at: string | null;
  created_at: string;
  updated_at: string;
  // Computed server-side. is_stale_diagnostic flags running cycles whose
  // heartbeat hasn't ticked recently, diagnostic only. is_abandonable
  // flags running cycles whose updated_at is past the conservative stuck
  // threshold; only these are eligible for the Abandon action.
  is_stale_diagnostic: boolean;
  is_abandonable: boolean;
  // Populated only by /v1/me/dreaming/cycles. The org-tier
  // /v1/orgs/{orgId}/dreaming/cycles and admin /v1/admin/dreaming/cycles
  // intentionally leave this empty so cross-tenant viewers (org_owner,
  // admin) never learn the names of other users' projects and the UI falls
  // through to project_id.
  project_name?: string;
}

// Self-tier aggregate status returned by /v1/me/dreaming with no project_id.
export interface MeDreamingAggregateStatus {
  dirty_count: number;
  project_count: number;
}

export interface DreamLog {
  id: string;
  cycle_id: string;
  project_id: string;
  phase: string;
  // Empty for phases that don't subdivide. Today only the consolidation
  // phase populates this (backfill_audit / reinforce / consolidate).
  sub_phase: string;
  operation: string;
  target_type: string;
  target_id: string;
  before_state: Record<string, unknown>;
  after_state: Record<string, unknown>;
  created_at: string;
}

export interface DreamStatusResponse {
  enabled: boolean;
  dirty_count: number;
  stuck_count: number;
  recent_cycles: DreamCycle[];
}

// Org-tier dream status. Same shape as DreamStatusResponse minus the
// system-wide `enabled` flag (org tab does not surface global toggle).
export interface OrgDreamStatusResponse {
  dirty_count: number;
  stuck_count: number;
  recent_cycles: DreamCycle[];
}

// Per-project dream status, returned by /v1/me/dreaming?project_id=...
// Distinct from system-wide DreamStatusResponse: self-tier callers see
// only their own project's state, with last_dream + full cycle list.
export interface DreamProjectStatusResponse {
  enabled: boolean;
  dirty: boolean;
  last_dream: DreamCycle | null;
  cycles: DreamCycle[];
}

export interface DreamAbandonResponse {
  status: string;
  cycle_id: string;
}

export interface DreamCycleDetail {
  cycle: DreamCycle;
  logs: DreamLog[];
}

export interface DreamEnableResponse {
  enabled: boolean;
}

export interface DreamRollbackResponse {
  status: string;
  cycle_id: string;
}

// --- Enrichment Types ---

export interface EnrichmentQueueCounts {
  pending: number;
  processing: number;
  completed: number;
  failed: number;
}

export interface EnrichmentQueueItem {
  id: string;
  memory_id: string;
  // Populated whenever the memory's project is resolvable. project_name is
  // populated only on self-tier responses; the org and system tiers leave
  // it empty so cross-tenant viewers (org_owner, admin) never learn the
  // names of other users' projects and the UI falls through to the UUID.
  project_id?: string;
  project_name?: string;
  status: string;
  attempts: number;
  max_attempts?: number;
  last_error?: string;
  created_at: string;
  // Populated by the EnrichmentAdminStore so the EnrichmentMonitor can render
  // the StaleDiagnosticPill (yellow, before the StuckJobSweeper has fired)
  // and RequeuedPill (red, after the sweeper has auto-requeued the row).
  // claimed_at and claimed_at_age_ms are only populated when status ===
  // "processing"; both reset on retry/requeue (so the row's "no progress"
  // pill anchors to the current attempt, not cumulative wait since
  // created_at).
  claimed_by?: string;
  claimed_at?: string;
  claimed_at_age_ms?: number;
  is_stale_diagnostic: boolean;
  last_requeue_reason?: string;
  // Enrichment phases that finished for this job. Subset of:
  //   "fact_extraction", "entity_extraction", "query_augmentation", "embedding"
  // Always emitted by the server as an array (never null/undefined).
  steps_completed: string[];
  // Why the query-augmentation step is absent from steps_completed on a
  // completed job. One of: "disabled", "content_empty",
  // "provider_unavailable", "llm_error", "parse_error". Omitted when the
  // step ran successfully (look in steps_completed) or the row predates the
  // column.
  query_augment_skip_reason?: string;
  // Mirror of the joined memory row so EnrichmentMonitor's "Augmentation"
  // accordion can render the persisted badge ("✓ Augmented · N queries")
  // without a second roundtrip per expanded row. Omitted when the memory's
  // vector was built from raw content (the badge falls back to "Raw embed ·
  // not augmented") or when the memory has been deleted.
  augmented_queries?: string[];
  augmented_embedding_at?: string;
  // Number of multi-vector facets the facet pass produced for this memory
  // (1 = single topic / facet 0 only, N = facet 0 plus N-1 topic facets).
  // Omitted when the memory has not been faceted yet (or the row predates the
  // feature); the monitor renders "not faceted" in that case.
  facet_count?: number;
  // Per-phase LLM/embedding latency and token usage for this job's most recent
  // run, read from the recorded token_usage rows. Omitted when no usage rows
  // match the memory (pending job, skipped phase, or a row predating the
  // feature). One entry per operation, ordered by pipeline phase.
  phase_metrics?: EnrichmentPhaseMetric[];
}

// One enrichment phase's measured cost, mapped from a token_usage row.
// operation is the canonical provider operation name: "ingestion_decision",
// "fact_extraction", "entity_extraction", "query_augment", "embedding".
export interface EnrichmentPhaseMetric {
  operation: string;
  model?: string;
  provider?: string;
  prompt_tokens: number;
  completion_tokens: number;
  latency_ms?: number;
  success: boolean;
  at: string;
}

export interface EnrichmentQueueStatus {
  counts: EnrichmentQueueCounts;
  items: EnrichmentQueueItem[];
  paused: boolean;
}

// EnrichmentQueueStatus item status values, used for the status filter.
export type EnrichmentStatusFilter =
  | "pending"
  | "processing"
  | "completed"
  | "failed";

export type EnrichmentSortField = "created_at" | "status" | "attempts";

// EnrichmentQueueParams mirrors the server-side QueueListParams: pagination,
// ordering, and an optional single-status filter. The server caps limit at
// 200 and defaults to created_at/desc across all statuses.
export interface EnrichmentQueueParams {
  limit?: number;
  offset?: number;
  sort?: EnrichmentSortField;
  dir?: "asc" | "desc";
  status?: EnrichmentStatusFilter;
}

function buildEnrichmentQueueQuery(params?: EnrichmentQueueParams): string {
  const sp = new URLSearchParams();
  if (params?.limit !== undefined) sp.set("limit", String(params.limit));
  if (params?.offset !== undefined) sp.set("offset", String(params.offset));
  if (params?.sort) sp.set("sort", params.sort);
  if (params?.dir) sp.set("dir", params.dir);
  if (params?.status) sp.set("status", params.status);
  const qs = sp.toString();
  return qs ? `?${qs}` : "";
}

export interface EnrichmentRetryResponse {
  retried: number;
}

export interface EnrichmentPauseResponse {
  paused: boolean;
}

export interface ExtractionTestResult {
  output: string;
  parsed: unknown;
  // model is the model the test actually ran against, as reported by the
  // provider; the resolved provider slot supplies it (the dedicated slot for
  // augment/ingestion, falling back to fact when unconfigured).
  model?: string;
  error?: string;
  latency_ms: number;
}

// Response for POST /admin/enrichment/backfill-augmentation. enqueued is 0
// when dry_run was true; otherwise it counts the jobs newly inserted into the
// enrichment queue.
export interface AugmentationBackfillResponse {
  candidate_count: number;
  enqueued: number;
  dry_run: boolean;
}

// Response for POST /admin/enrichment/backfill-multi-vector. Same shape as the
// augmentation backfill: enqueued is 0 on a dry run, else the count of memories
// enqueued for re-faceting.
export interface MultiVectorBackfillResponse {
  candidate_count: number;
  enqueued: number;
  dry_run: boolean;
}

// Response for POST /v1/projects/{id}/memories/{id}/preview-augmentation.
// Mirrors the server's MemoryPreviewAugmentResponse: queries and the rendered
// embed-ready content the augmentation phase would have produced, without
// touching the DB.
export interface MemoryAugmentPreviewResponse {
  queries: string[];
  augmented_content: string;
  rendered_prompt: string;
  model: string;
  latency_ms: number;
  truncated_bytes: number;
  error?: string;
}

export interface OAuthClient {
  id: string;
  name: string;
  client_id: string;
  // type/client_type are emitted by the admin endpoint (GET /admin/oauth/clients).
  // The self-service endpoint (GET /me/oauth-clients) instead emits auto_registered
  // and last_used_at. The two endpoints share this type, so the differing fields
  // are optional.
  type?: "auto" | "manual";
  client_type?: "public" | "confidential";
  auto_registered?: boolean;
  redirect_uris: string[];
  created_at: string;
  last_used_at?: string | null;
}

export interface OAuthClientCreated extends OAuthClient {
  client_secret?: string;
}

export interface CreateOAuthClientRequest {
  name: string;
  redirect_uris?: string[];
  client_type?: "public" | "confidential";
}

export interface IdPConfig {
  id: string;
  org_id?: string | null;
  provider_type: string;
  client_id: string;
  issuer_url?: string | null;
  authorize_url?: string | null;
  token_url?: string | null;
  userinfo_url?: string | null;
  allowed_domains: string[];
  auto_provision: boolean;
  default_role?: string;
  created_at: string;
  updated_at: string;
}

export interface CreateIdPConfigRequest {
  org_id: string;
  provider_type: string;
  client_id: string;
  client_secret: string;
  issuer_url?: string;
  authorize_url?: string;
  token_url?: string;
  userinfo_url?: string;
  allowed_domains?: string[];
  auto_provision?: boolean;
  default_role?: string;
}

export interface UpdateIdPConfigRequest {
  client_id?: string;
  client_secret?: string;
  issuer_url?: string | null;
  authorize_url?: string | null;
  token_url?: string | null;
  userinfo_url?: string | null;
  allowed_domains?: string[];
  auto_provision?: boolean;
  default_role?: string;
}

export interface GraphEntity {
  id: string;
  name: string;
  canonical: string;
  entity_type: string;
  mention_count: number;
  aliases: string[];
  created_at: string;
  updated_at: string;
}

export interface GraphRelationship {
  id: string;
  source_id: string;
  target_id: string;
  relation: string;
  weight: number;
}

export interface GraphData {
  entities: GraphEntity[];
  relationships: GraphRelationship[];
  truncated?: boolean;
  total_edges?: number;
  returned_edges?: number;
}

// GraphHealth reports the lost-provenance edge backlog (relationships whose
// sourcing memory has been deleted or superseded) that a repair would reap.
export interface GraphHealth {
  lost_provenance_edges: number;
}

// GraphRepairResult summarizes an operator-triggered graph repair.
export interface GraphRepairResult {
  relationships_reaped: number;
  dangling_relationships_deleted: number;
  orphaned_entities_deleted: number;
}

export interface PaginatedResponse<T> {
  data: T[];
  pagination: {
    total: number;
    limit: number;
    offset: number;
  };
}

// --- Diagnostic logs (operator-only) ---

// LogEntry is one row from the diagnostic log store. attrs holds the structured
// slog fields as an object (preserved verbatim, not flattened into message).
export interface LogEntry {
  id: string;
  ts: string;
  level: string;
  component?: string;
  message: string;
  attrs: Record<string, unknown>;
  project_id?: string;
  namespace_id?: string;
  user_id?: string;
}

// LogListParams narrows a logs query. level is an OR-set; from/to are RFC3339.
export interface LogListParams {
  level?: string[];
  component?: string;
  search?: string;
  attrKey?: string;
  attrValue?: string;
  from?: string;
  to?: string;
  limit?: number;
  offset?: number;
}

// LogFacets supplies the Logs page filter dropdown values.
export interface LogFacets {
  levels: string[];
  components: string[];
}

// buildLogQuery serializes LogListParams into a query string. Levels are
// comma-joined (the backend also accepts repeated level params).
export function buildLogQuery(params?: LogListParams): string {
  const sp = new URLSearchParams();
  if (params?.level && params.level.length > 0) sp.set("level", params.level.join(","));
  if (params?.component) sp.set("component", params.component);
  if (params?.search) sp.set("search", params.search);
  if (params?.attrKey) sp.set("attr_key", params.attrKey);
  if (params?.attrValue !== undefined) sp.set("attr_value", params.attrValue);
  if (params?.from) sp.set("from", params.from);
  if (params?.to) sp.set("to", params.to);
  if (typeof params?.limit === "number") sp.set("limit", String(params.limit));
  if (typeof params?.offset === "number") sp.set("offset", String(params.offset));
  const qs = sp.toString();
  return qs ? `?${qs}` : "";
}

// downloadLogsExport streams the filtered log export through fetch (so the
// Authorization header travels with the request) and triggers a browser
// download. Filename comes from the Content-Disposition header.
export async function downloadLogsExport(
  format: "csv" | "json",
  params?: LogListParams,
): Promise<void> {
  const sp = new URLSearchParams(buildLogQuery(params).replace(/^\?/, ""));
  sp.set("format", format);
  const res = await fetch(`${BASE_URL}/admin/logs/export?${sp.toString()}`, {
    headers: getAuthHeaders(),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new APIError(res.status, text || "log export failed");
  }
  const blob = await res.blob();
  const filename =
    parseAttachmentFilename(res.headers.get("Content-Disposition")) ?? `logs.${format}`;
  triggerBlobDownload(blob, filename);
}

// --- Auth API ---

export interface LoginRequest {
  email: string;
  password: string;
}

export interface LoginResponse {
  token: string;
  user: { id: string; email: string; display_name: string; role: string; org_id: string };
}

export interface LookupRequest {
  email: string;
}

export interface LookupResponse {
  method: "local" | "idp" | "unknown";
  idp_id?: string;
  password_fallback?: boolean;
  has_passkeys?: boolean;
}

export interface Passkey {
  id: string;
  user_id: string;
  name: string;
  credential_id: string;
  aaguid: string;
  sign_count: number;
  transports: string[];
  user_verified: boolean;
  backup_eligible: boolean;
  backup_state: boolean;
  attestation_type: string;
  created_at: string;
  last_used_at?: string | null;
}

export const authAPI = {
  login: (data: LoginRequest) => request<LoginResponse>("POST", "/auth/login", data),
  lookup: (data: LookupRequest) => request<LookupResponse>("POST", "/auth/lookup", data),
  passkeyBegin: (data: { email: string }) =>
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    request<any>("POST", "/auth/passkey/begin", data),
  passkeyFinish: (body: unknown, sessionKey: string) =>
    request<LoginResponse>("POST", "/auth/passkey/finish", body, {
      "X-Webauthn-Session": sessionKey,
    }),
};

// --- Admin API client ---

export const adminAPI = {
  // Setup
  getSetupStatus: () => request<SetupStatus>("GET", "/admin/setup/status"),
  completeSetup: (data: SetupRequest) =>
    request<SetupResponse>("POST", "/admin/setup", data),

  // Dashboard
  getDashboard: () => request<DashboardData>("GET", "/dashboard"),

  // Activity
  getActivity: (limit = 20) =>
    request<ActivityResponse>("GET", `/activity?limit=${limit}`),

  // Organizations
  listOrgs: () => request<{ data: Organization[] }>("GET", "/admin/orgs").then(r => r.data),
  getOrg: (id: string) => request<Organization>("GET", `/admin/orgs/${id}`),
  createOrg: (data: CreateOrgRequest) =>
    request<Organization>("POST", "/admin/orgs", data),
  updateOrg: (id: string, data: UpdateOrgRequest) =>
    request<Organization>("PUT", `/admin/orgs/${id}`, data),
  deleteOrg: (id: string) => request<void>("DELETE", `/admin/orgs/${id}`),

  // Users
  listUsers: () => request<{ data: User[] }>("GET", "/admin/users").then(r => r.data),
  getUser: (id: string) => request<User>("GET", `/admin/users/${id}`),
  createUser: (data: CreateUserRequest) =>
    request<User>("POST", "/admin/users", data),
  updateUser: (id: string, data: UpdateUserRequest) =>
    request<User>("PUT", `/admin/users/${id}`, data),
  deleteUser: (id: string) => request<void>("DELETE", `/admin/users/${id}`),
  generateAPIKey: (userId: string, data: GenerateAPIKeyRequest) =>
    request<GenerateAPIKeyResponse>(
      "POST",
      `/admin/users/${userId}/api-keys`,
      data,
    ),
  revokeAPIKey: (userId: string, keyId: string) =>
    request<void>("DELETE", `/admin/users/${userId}/api-keys/${keyId}`),

  // Projects: repointed from /admin/projects (deleted) to /me/projects in
  // the 2026-04-30 leak fix. Cross-tenant project listings exposed
  // user-authored project names + descriptions and were a privacy leak;
  // admins now see and manage their own projects like every other role.
  listProjects: () => request<{ data: Project[] }>("GET", "/me/projects").then(r => r.data),
  createProject: (data: AdminCreateProjectRequest) =>
    request<Project>("POST", "/me/projects", data),
  // Provider slots: backend returns the ordered canonical slot list, each
  // entry carrying its own slot/label/description/required plus live status.
  getProviderSlots: () =>
    request<ProviderConfigResponse>("GET", "/admin/providers"),
  updateProviderSlot: (slot: string, data: UpdateProviderSlotRequest) =>
    request<UpdateProviderSlotResult | { status: string }>("PUT", `/admin/providers/${slot}`, data),
  testProviderSlot: (slot: string, config: UpdateProviderSlotRequest) =>
    request<TestProviderResult>("POST", "/admin/providers/test", { slot, config }),
  getOllamaModels: async (ollamaUrl?: string, customHeaders?: Record<string, string>) => {
    const params = ollamaUrl ? `?url=${encodeURIComponent(ollamaUrl)}` : "";
    // In-progress custom headers (for a proxied Ollama not yet saved) are
    // forwarded as request headers, not query params, so secret values never
    // land in the URL or access logs. See forwardedHeaderPrefix on the server.
    const models = await request<OllamaModel[]>(
      "GET",
      `/admin/providers/ollama/models${params}`,
      undefined,
      forwardedProviderHeaders(customHeaders),
    );
    return { models };
  },
  pullOllamaModel: (model: string, ollamaUrl?: string, customHeaders?: Record<string, string>) =>
    request<{ status: string; model: string }>("POST", "/admin/providers/ollama/pull", {
      model,
      url: ollamaUrl || undefined,
      custom_headers:
        customHeaders && Object.keys(customHeaders).length > 0 ? customHeaders : undefined,
    }),
  // Detects the models an OpenAI-compatible server (vLLM/SGLang) reports at
  // GET /v1/models. In-progress custom headers are forwarded as request headers
  // (not query params) so secret values never land in the URL or access logs.
  getProviderModels: async (url: string, customHeaders?: Record<string, string>) => {
    const params = new URLSearchParams({ url });
    const models = await request<string[]>(
      "GET",
      `/admin/providers/models?${params.toString()}`,
      undefined,
      forwardedProviderHeaders(customHeaders),
    );
    return { models };
  },

  // Settings
  getSettings: () => {
    // limit is pinned explicitly so the UI's correctness does not silently
    // depend on the server's default page size. The registry is bounded
    // compile-time data; we want the whole list, not a page. Settings are
    // global-only; there is no per-scope filtering.
    return request<{ data: Setting[] }>("GET", "/admin/settings?limit=500");
  },
  getSettingsSchema: () =>
    request<{ data: SettingSchema[] }>("GET", "/admin/settings?schema=true"),
  getSettingGroups: () =>
    request<{ data: SettingGroup[] }>("GET", "/admin/settings?groups=true"),
  updateSetting: (key: string, value: unknown) =>
    request<{ status: string }>("PUT", "/admin/settings", { key, value }),
  resetSettings: (body: { key?: string } = {}) =>
    request<{ status: string; reset: number }>("POST", "/admin/settings/reset", body),

  // Webhooks
  listWebhooks: () => request<{ data: Webhook[] }>("GET", "/admin/webhooks").then(r => r.data),
  createWebhook: (data: WebhookCreateRequest) =>
    request<Webhook>("POST", "/admin/webhooks", data),
  updateWebhook: (id: string, data: WebhookUpdateRequest) =>
    request<Webhook>("PUT", `/admin/webhooks/${id}`, data),
  deleteWebhook: (id: string) =>
    request<void>("DELETE", `/admin/webhooks/${id}`),
  testWebhook: (id: string) =>
    request<WebhookTestResult>(
      "POST",
      `/admin/webhooks/${id}/test`,
    ),

  // Analytics: tier-A self-scoped. The pre-fix `org` and `user` widening
  // params were removed; the server ignores them now (resolveAdminScope is
  // gone). Use orgAPI.getAnalytics(orgId) or systemAPI.getAnalytics() for
  // wider tiers. Org/user filters in the UI must drive tier selection,
  // not query-string widening.
  getAnalytics: () =>
    request<AnalyticsData>("GET", "/analytics"),
  getUsage: (params?: { project?: string; from?: string; to?: string; group_by?: UsageGroupBy; success_only?: boolean }) => {
    const sp = new URLSearchParams();
    if (params?.project) sp.set("project", params.project);
    if (params?.from) sp.set("from", params.from);
    if (params?.to) sp.set("to", params.to);
    if (params?.group_by) sp.set("group_by", params.group_by);
    if (params?.success_only !== undefined) sp.set("success_only", params.success_only ? "true" : "false");
    const qs = sp.toString();
    return request<UsageReport>("GET", `/usage${qs ? `?${qs}` : ""}`);
  },

  // Writes go through updateSetting("usage.cost_rates", ...); this
  // read is open to any authenticated role so non-admins can still see
  // dollar columns in the breakdown.
  getCostRates: () =>
    request<{ data: CostRate[] }>("GET", "/usage/cost_rates").then(r => r.data ?? []),

  // Database
  getDatabaseInfo: () => request<DatabaseInfo>("GET", "/admin/database"),
  testDatabaseConnection: (url: string) =>
    request<ConnectionTestResult>("POST", "/admin/database/test", { url }),
  preflightDatabase: (url: string) =>
    request<PreflightReport>("POST", "/admin/database/preflight", { url }),
  resetDatabase: (url: string, mode: ResetMode) =>
    request<ResetResult>("POST", "/admin/database/reset", { url, mode }),
  migrationAudit: () =>
    request<MigrationAudit>("GET", "/admin/database/migration-audit"),
  // Starts the SQLite->Postgres migration in the background; returns
  // immediately. Progress and the terminal result stream over /v1/events
  // under the "db-migration" scope.
  triggerMigration: (url: string) =>
    request<MigrationStartAck>("POST", "/admin/database/migrate", { url }),

  // Vector migration dry run. Counts the memory + entity vectors a migration in
  // the given direction would copy, without writing. Requires qdrant.addr.
  vectorMigrationDryRun: (direction: VectorMigrationDirection) =>
    request<VectorMigrationResult>("POST", "/admin/vector-migration", {
      direction,
      dry_run: true,
    }),

  // Starts a real vector migration in the background; returns immediately.
  // Progress and the terminal result stream over /v1/events under the
  // "vector-migration" scope. Reads/writes are upsert-only and re-runnable.
  startVectorMigration: (
    direction: VectorMigrationDirection,
    batchSize?: number,
  ) =>
    request<MigrationStartAck>("POST", "/admin/vector-migration", {
      direction,
      dry_run: false,
      ...(typeof batchSize === "number" ? { batch_size: batchSize } : {}),
    }),

  // Dreaming
  getDreamingStatus: () =>
    request<DreamStatusResponse>("GET", "/admin/dreaming"),
  getDreamingCycles: (projectId?: string) => {
    const qs = projectId ? `?project_id=${encodeURIComponent(projectId)}` : "";
    return request<DreamCycle[]>("GET", `/admin/dreaming/cycles${qs}`);
  },
  getDreamingCycleDetail: (cycleId: string) =>
    request<DreamCycleDetail>("GET", `/admin/dreaming/cycles/${cycleId}`),
  setDreamingEnabled: (enabled: boolean) =>
    request<DreamEnableResponse>("POST", "/admin/dreaming/enable", { enabled }),
  setProjectDreamingEnabled: (projectId: string, enabled: boolean) =>
    request<{ project_id: string; enabled: boolean }>("POST", "/admin/dreaming/project/enable", {
      project_id: projectId,
      enabled,
    }),
  rollbackDreamCycle: (cycleId: string) =>
    request<DreamRollbackResponse>("POST", "/admin/dreaming/rollback", { cycle_id: cycleId }),
  abandonDreamCycle: (cycleId: string) =>
    request<DreamAbandonResponse>("POST", `/admin/dreaming/cycles/${cycleId}/abandon`),

  // Diagnostic logs (operator-only)
  listLogs: (params?: LogListParams) =>
    request<PaginatedResponse<LogEntry>>("GET", `/admin/logs${buildLogQuery(params)}`),
  getLogFacets: () => request<LogFacets>("GET", "/admin/logs/facets"),

  // Enrichment
  getEnrichmentStatus: (params?: EnrichmentQueueParams) =>
    request<EnrichmentQueueStatus>(
      "GET",
      `/admin/enrichment${buildEnrichmentQueueQuery(params)}`,
    ),
  retryEnrichment: (ids?: string[]) =>
    request<EnrichmentRetryResponse>("POST", "/admin/enrichment/retry", { ids: ids ?? [] }),
  pauseEnrichment: (paused: boolean) =>
    request<EnrichmentPauseResponse>("POST", "/admin/enrichment/pause", { paused }),
  testExtractionPrompt: (
    type: "fact" | "entity" | "augment" | "ingestion",
    sampleInput: string,
    count?: number,
    systemPrompt?: string,
  ) =>
    request<ExtractionTestResult>("POST", "/admin/enrichment/test-prompt", {
      type,
      system_prompt: systemPrompt ?? "",
      sample_input: sampleInput,
      ...(typeof count === "number" ? { count } : {}),
    }),

  // Query augmentation backfill. dry_run=true returns candidate_count without
  // enqueueing. project_id omitted scans the whole deployment. limit caps how
  // many candidates land in the queue this call.
  backfillAugmentation: (req: {
    project_id?: string;
    dry_run?: boolean;
    limit?: number;
  }) =>
    request<AugmentationBackfillResponse>(
      "POST",
      "/admin/enrichment/backfill-augmentation",
      req,
    ),

  // Enqueue existing memories for multi-vector re-faceting. dry_run reports the
  // candidate count without enqueueing. project_id omitted scans the whole
  // deployment; limit caps how many memories land in the queue this call.
  backfillMultiVector: (req: {
    project_id?: string;
    dry_run?: boolean;
    limit?: number;
  }) =>
    request<MultiVectorBackfillResponse>(
      "POST",
      "/admin/enrichment/backfill-multi-vector",
      req,
    ),

  // Per-memory preview of the augmentation phase. Project-scoped, does not
  // persist; used by the MemoryDetailPanel Preview button.
  previewMemoryAugmentation: (projectId: string, memoryId: string) =>
    request<MemoryAugmentPreviewResponse>(
      "POST",
      `/projects/${encodeURIComponent(projectId)}/memories/${encodeURIComponent(memoryId)}/preview-augmentation`,
    ),

  // Graph
  getGraph: (projectId: string) =>
    request<GraphData>("GET", `/graph?project=${encodeURIComponent(projectId)}`),

  // Graph maintenance (system-wide, admin-only). getGraphHealth reports how
  // many lost-provenance edges (relationships whose sourcing memory is gone)
  // remain in the store; repairGraph reaps them, recomputes entity mention
  // counts, and prunes the orphaned entities they leave behind.
  getGraphHealth: () =>
    request<GraphHealth>("GET", "/admin/graph/health"),
  repairGraph: () =>
    request<GraphRepairResult>("POST", "/admin/graph/repair"),

  // Namespaces
  getNamespaceTree: () =>
    request<{ tree: NamespaceNode[] }>("GET", "/namespaces/tree"),

  // OAuth Clients
  listOAuthClients: () =>
    request<OAuthClient[]>("GET", "/admin/oauth/clients"),
  createOAuthClient: (data: CreateOAuthClientRequest) =>
    request<OAuthClientCreated>("POST", "/admin/oauth/clients", data),
  deleteOAuthClient: (id: string) =>
    request<void>("DELETE", `/admin/oauth/clients/${id}`),

  // IdP Config
  listIdPConfigs: () =>
    request<IdPConfig[]>("GET", "/admin/oauth/idp"),
  createIdPConfig: (data: CreateIdPConfigRequest) =>
    request<IdPConfig>("POST", "/admin/oauth/idp", data),
  updateIdPConfig: (id: string, data: UpdateIdPConfigRequest) =>
    request<IdPConfig>("PUT", `/admin/oauth/idp/${id}`, data),
  deleteIdPConfig: (id: string) =>
    request<void>("DELETE", `/admin/oauth/idp/${id}`),
};

/**
 * Per `service/update.go`: a content change creates a new memory row chained
 * to the old via SupersededBy and returns the NEW memory's id. Tags/metadata-
 * only updates mutate in place and id == previous_memory_id. previous_memory_id
 * always echoes the request's memory id so callers can correlate.
 */
export interface UpdateMemoryResponse {
  id: string;
  previous_memory_id: string;
  project_id: string;
  content: string;
  tags: string[];
  previous_content?: string;
  re_embedded: boolean;
  superseded: boolean;
  latency_ms: number;
}

export interface WebhookTestResult {
  success: boolean;
  status_code?: number;
  message?: string;
  latency_ms: number;
}

// --- Memory API (project-scoped) ---

function memoryListSearchParams(params?: MemoryListParams): URLSearchParams {
  const sp = new URLSearchParams();
  if (!params) return sp;
  if (params.limit !== undefined) sp.set("limit", String(params.limit));
  if (params.offset !== undefined) sp.set("offset", String(params.offset));
  if (params.tags && params.tags.length > 0) {
    for (const t of params.tags) sp.append("tag", t);
  }
  if (params.date_from) sp.set("date_from", params.date_from);
  if (params.date_to) sp.set("date_to", params.date_to);
  if (params.enriched) sp.set("enriched", params.enriched);
  if (params.origin) sp.set("origin", params.origin);
  if (params.augmented) sp.set("augmented", params.augmented);
  if (params.include_superseded) sp.set("include_superseded", params.include_superseded);
  if (params.source) sp.set("source", params.source);
  if (params.search) sp.set("search", params.search);
  if (params.group_by_parent) sp.set("group_by_parent", "true");
  return sp;
}

function buildMemoryListQuery(params?: MemoryListParams): string {
  return memoryListSearchParams(params).toString();
}

export const memoryAPI = {
  store: (projectId: string, data: StoreMemoryRequest) =>
    request<StoredMemory>("POST", `/projects/${projectId}/memories`, data),

  // Server-side bulk import. The request() helper JSON-stringifies the body, so
  // passing a parsed nram ExportData object re-emits the exact JSON the backend
  // parser accepts; entities and relationships round-trip for format "nram".
  import: (projectId: string, format: ImportFormat, data: unknown) =>
    request<ImportResponse>(
      "POST",
      `/projects/${projectId}/memories/import?format=${format}`,
      data,
    ),

  list: (projectId: string, params?: MemoryListParams) => {
    const qs = buildMemoryListQuery(params);
    return request<MemoryListResponse>(
      "GET",
      `/projects/${projectId}/memories${qs ? `?${qs}` : ""}`,
    );
  },

  listIDs: (
    projectId: string,
    params?: MemoryListParams & { max?: number },
  ) => {
    const sp = memoryListSearchParams(params);
    if (params?.max !== undefined) sp.set("max", String(params.max));
    const qs = sp.toString();
    return request<ListIDsResponse>(
      "GET",
      `/projects/${projectId}/memories/ids${qs ? `?${qs}` : ""}`,
    );
  },

  recall: (projectId: string, body: RecallRequest) =>
    request<RecallResponse>(
      "POST",
      `/projects/${projectId}/memories/recall`,
      body,
    ),

  ask: (projectId: string, body: AskRequest) =>
    request<AskResponse>("POST", `/projects/${projectId}/memories/ask`, body),

  get: (
    projectId: string,
    memoryId: string,
    opts?: { includeSuperseded?: boolean },
  ) => {
    const qs = opts?.includeSuperseded ? "?include_superseded=true" : "";
    return request<Memory>(
      "GET",
      `/projects/${projectId}/memories/${memoryId}${qs}`,
    );
  },

  update: (projectId: string, memoryId: string, body: MemoryUpdateRequest) =>
    request<UpdateMemoryResponse>(
      "PUT",
      `/projects/${projectId}/memories/${memoryId}`,
      body,
    ),

  remove: (projectId: string, memoryId: string) =>
    request<ForgetResponse>("DELETE", `/projects/${projectId}/memories/${memoryId}`),

  forget: (projectId: string, body: ForgetRequest) =>
    request<ForgetResponse>("POST", `/projects/${projectId}/memories/forget`, body),

  move: (projectId: string, memoryId: string, targetProjectId: string) =>
    request<MoveResponse>(
      "POST",
      `/projects/${projectId}/memories/${memoryId}/move`,
      { target_project_id: targetProjectId },
    ),

  bulkMove: (projectId: string, ids: string[], targetProjectId: string) =>
    request<MoveResponse>("POST", `/projects/${projectId}/memories/move`, {
      ids,
      target_project_id: targetProjectId,
    }),

  enrich: (projectId: string, body: EnrichRequest) =>
    request<EnrichResponse>("POST", `/projects/${projectId}/memories/enrich`, body),

  export: (projectId: string) =>
    request<ExportData>("GET", `/projects/${projectId}/memories/export`),
};

// --- Me API (self-service, any authenticated user) ---

export interface MeCreateProjectRequest {
  name: string;
  slug: string;
  description?: string;
  default_tags?: string[];
  settings?: Partial<ProjectSettings>;
}

export interface MeCreateAPIKeyRequest {
  name: string;
  scopes?: string[];
  expires_at?: string;
}

export interface MeCreateAPIKeyResponse {
  id: string;
  key: string;
  key_prefix: string;
  name: string;
  created_at: string;
}

export function changePassword(currentPassword: string, newPassword: string): Promise<{ changed: boolean }> {
  return request("POST", "/me/password", { current_password: currentPassword, new_password: newPassword });
}

// MeProfile is the shape returned by GET /v1/me/profile. It mirrors the
// login response (and the JWT session claims), so AuthContext.UserInfo and
// MeProfile share the same definition. Refetching from the server lets the
// SPA pick up profile changes that happened after the JWT was issued.
export type MeProfile = UserInfo;

// MeCapabilities is the shape returned by GET /v1/me/capabilities. Two
// booleans drive sidebar nav visibility for the Enrichment Queue and
// Dreaming entries; the endpoint is callable by any authenticated user, so
// non-admins no longer need to probe the admin-only /admin/providers route.
export interface MeCapabilities {
  enrichment_available: boolean;
  dreaming_enabled: boolean;
  ask_enabled: boolean;
}

// MeRankingWeightDefault is one row in the response of
// GET /v1/me/ranking-weights/defaults. `value` is the effective global-scope
// value (operator override if set, schema default otherwise); the per-project
// Ranking Weights editor uses it as the placeholder for each weight input.
// `default_value` is the registered schema default, exposed separately so
// the SPA can fall back to it if an override is unparseable.
export interface MeRankingWeightDefault {
  key: string;
  value: number;
  default_value: number;
  min?: number;
  max?: number;
  step?: number;
}

// MeSettingDefault is one row in the response of GET /v1/me/setting-defaults.
// `value` is the effective global-scope value (operator override if set,
// schema default otherwise); general-user pages use it as the live default for
// the matching control (graph layout sliders, dedup-threshold placeholder).
// `default_value` is the registered schema default; min/max/step come from the
// schema entry. Mirrors MeRankingWeightDefault.
export interface MeSettingDefault {
  key: string;
  value: number;
  default_value: number;
  min?: number;
  max?: number;
  step?: number;
}

export type Theme = "light" | "dark";

export interface MeProfilePatchRequest {
  theme?: Theme;
}

// ExportJob mirrors model.ExportJob on the backend. status transitions
// pending → processing → succeeded|failed, then later → expired once the
// cleanup sweep reclaims the artifact. project_id is set only for
// scope="project"; scope="account" produces a zip covering every project
// the user owns.
export type ExportJobScope = "account" | "project";
export type ExportJobStatus = "pending" | "processing" | "succeeded" | "failed" | "expired";
export type ExportJobFormat = "zip" | "json" | "ndjson";

export interface ExportJob {
  id: string;
  user_id: string;
  scope: ExportJobScope;
  project_id?: string | null;
  format: ExportJobFormat;
  include_superseded: boolean;
  status: ExportJobStatus;
  artifact_path?: string | null;
  artifact_bytes?: number | null;
  artifact_sha256?: string | null;
  error?: string | null;
  claimed_by?: string | null;
  claimed_at?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  expires_at?: string | null;
  created_at: string;
  updated_at: string;
}

export interface CreateExportJobRequest {
  scope: ExportJobScope;
  project_id?: string;
  format?: ExportJobFormat;
  include_superseded?: boolean;
}

// downloadExportJobArtifact streams the succeeded artifact through fetch
// (so the Authorization header travels with the request) and triggers a
// browser download via a temporary blob URL. Resolves once the download
// has been initiated; rejects on a non-2xx status. Filename is derived
// from the Content-Disposition header the handler sends, falling back to
// nram-export-{id}.zip if absent.
export async function downloadExportJobArtifact(id: string): Promise<void> {
  const res = await fetch(`${BASE_URL}/me/exports/${id}/download`, {
    headers: getAuthHeaders(),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new APIError(res.status, text || "download failed");
  }
  const blob = await res.blob();
  const filename = parseAttachmentFilename(res.headers.get("Content-Disposition")) ?? `nram-export-${id}.zip`;
  triggerBlobDownload(blob, filename);
}

// downloadProjectExport hits the synchronous per-project export endpoint
// and triggers a browser download. Used by the per-project Export action
// in ProjectManagement; bypasses the async job pipeline because the
// payload is built in memory and streamed back inline.
export async function downloadProjectExport(
  projectID: string,
  projectSlug: string,
  opts?: { format?: "json" | "ndjson"; includeSuperseded?: boolean },
): Promise<void> {
  const sp = new URLSearchParams();
  const format = opts?.format ?? "json";
  sp.set("format", format);
  if (opts?.includeSuperseded) sp.set("include_superseded", "true");
  const res = await fetch(`${BASE_URL}/projects/${projectID}/memories/export?${sp.toString()}`, {
    headers: getAuthHeaders(),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new APIError(res.status, text || "export failed");
  }
  const blob = await res.blob();
  const ext = format === "ndjson" ? "ndjson" : "json";
  triggerBlobDownload(blob, `nram-${projectSlug}.${ext}`);
}

export const meAPI = {
  getProfile: () => request<MeProfile>("GET", "/me/profile"),

  updateProfile: (data: MeProfilePatchRequest) =>
    request<MeProfile>("PATCH", "/me/profile", data),

  // Self-tier capability flags. Callable by any authenticated user; the
  // sidebar nav uses this to decide whether to render Enrichment Queue and
  // Dreaming entries without paying the admin-only /admin/providers probe.
  getCapabilities: () =>
    request<MeCapabilities>("GET", "/me/capabilities"),

  // Self-tier read of the eight ranking.weight.* schema entries with their
  // effective global-scope values. Powers the placeholders on the per-project
  // Ranking Weights editor for non-admin owners who cannot read
  // /admin/settings. Authentication required, no role gate.
  getRankingWeightDefaults: () =>
    request<{ data: MeRankingWeightDefault[] }>("GET", "/me/ranking-weights/defaults"),

  // Self-tier read of allow-listed numeric setting defaults (graph layout
  // keys + dedup threshold) with their effective global-scope values. Powers
  // the graph visualization layout controls and the dedup-threshold
  // placeholders for non-admin owners who cannot read /admin/settings.
  // Authentication required, no role gate.
  getSettingDefaults: () =>
    request<{ data: MeSettingDefault[] }>("GET", "/me/setting-defaults"),

  listPasskeys: () =>
    request<{ data: Passkey[] }>("GET", "/me/passkeys").then((r) => r.data),
  registerPasskeyBegin: (data: { name: string }) =>
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    request<any>("POST", "/me/passkeys/register/begin", data),
  registerPasskeyFinish: (body: unknown, name: string) =>
    request<Passkey>("POST", "/me/passkeys/register/finish", body, {
      "X-Passkey-Name": name,
    }),
  deletePasskey: (id: string) => request<void>("DELETE", `/me/passkeys/${id}`),

  listProjects: () =>
    request<{ data: Project[] }>("GET", "/me/projects").then((r) => r.data),
  getProject: (id: string) =>
    request<Project>("GET", `/me/projects/${id}`),
  createProject: (data: MeCreateProjectRequest) =>
    request<Project>("POST", "/me/projects", data),
  updateProject: (id: string, data: ProjectUpdateRequest) =>
    request<Project>("PUT", `/me/projects/${id}`, data),
  deleteProject: (id: string) =>
    request<void>("DELETE", `/me/projects/${id}`),

  // Procedural memory tier: verbatim standing rules, per-user, fetch-only.
  // limit defaults high so the whole (small) set loads once for client-side
  // search and sort; the server caps it at 500.
  listProcedural: (limit = 500) =>
    request<{ data: ProceduralEntry[] }>("GET", `/me/procedural?limit=${limit}`).then(
      (r) => r.data,
    ),
  getProcedural: (id: string) =>
    request<ProceduralEntry>("GET", `/me/procedural/${id}`),
  createProcedural: (data: CreateProceduralRequest) =>
    request<ProceduralEntry>("POST", "/me/procedural", data),
  updateProcedural: (id: string, data: UpdateProceduralRequest) =>
    request<ProceduralEntry>("PUT", `/me/procedural/${id}`, data),
  deleteProcedural: (id: string) =>
    request<void>("DELETE", `/me/procedural/${id}`),
  exportProcedural: () =>
    request<ProceduralExportData>("GET", "/me/procedural/export"),
  importProcedural: (data: ProceduralExportData | ProceduralExportEntry[]) =>
    request<ProceduralImportResult>("POST", "/me/procedural/import", data),

  listAPIKeys: () =>
    request<{ data: APIKey[] }>("GET", "/me/api-keys").then((r) => r.data),
  createAPIKey: (data: MeCreateAPIKeyRequest) =>
    request<MeCreateAPIKeyResponse>("POST", "/me/api-keys", data),
  revokeAPIKey: (id: string) =>
    request<void>("DELETE", `/me/api-keys/${id}`),

  listOAuthClients: () =>
    request<{ data: OAuthClient[] }>("GET", "/me/oauth-clients").then((r) => r.data),
  createOAuthClient: (data: CreateOAuthClientRequest) =>
    request<OAuthClientCreated>("POST", "/me/oauth-clients", data),
  revokeOAuthClient: (id: string) =>
    request<void>("DELETE", `/me/oauth-clients/${id}`),

  recall: (body: RecallRequest) =>
    request<RecallResponse>("POST", "/me/memories/recall", body),

  ask: (body: AskRequest) =>
    request<AskResponse>("POST", "/me/memories/ask", body),

  // Self-tier dreaming observability. Read-only; write operations remain
  // admin-only at /admin/dreaming/*. Status returns per-project state when
  // a project_id is supplied, otherwise the aggregate any-dirty indicator.
  // Cycles list filters to one project when project_id is supplied,
  // otherwise lists across all of the caller's projects.
  getDreamingProjectStatus: (projectId: string) =>
    request<DreamProjectStatusResponse>(
      "GET",
      `/me/dreaming?project_id=${encodeURIComponent(projectId)}`,
    ),
  getDreamingAggregateStatus: () =>
    request<MeDreamingAggregateStatus>("GET", "/me/dreaming"),
  getDreamingCycles: (projectId?: string) =>
    request<DreamCycle[]>(
      "GET",
      projectId
        ? `/me/dreaming/cycles?project_id=${encodeURIComponent(projectId)}`
        : "/me/dreaming/cycles",
    ),
  getDreamingCycleDetail: (cycleId: string) =>
    request<DreamCycleDetail>("GET", `/me/dreaming/cycles/${cycleId}`),

  // Self-tier enrichment queue: caller's own queue items + caller-scoped
  // counts. Pause/test-prompt remain admin-only at /admin/enrichment/*.
  getEnrichmentStatus: (params?: EnrichmentQueueParams) =>
    request<EnrichmentQueueStatus>(
      "GET",
      `/me/enrichment${buildEnrichmentQueueQuery(params)}`,
    ),
  retryEnrichment: (ids?: string[]) =>
    request<EnrichmentRetryResponse>("POST", "/me/enrichment/retry", { ids: ids ?? [] }),
  abandonDreamCycle: (cycleId: string) =>
    request<DreamAbandonResponse>("POST", `/me/dreaming/cycles/${cycleId}/abandon`),
  rollbackDreamCycle: (cycleId: string) =>
    request<DreamRollbackResponse>("POST", `/me/dreaming/cycles/${cycleId}/rollback`),

  // Self-service export jobs. The async pipeline replaces the
  // truncation-bound MCP export tool withdrawn 2026-05-27. List/create at
  // the root; status + delete at {job_id}; artifact download under
  // /download. Per-project synchronous exports remain at
  // /v1/projects/{id}/memories/export; see exportProjectURL below.
  listExportJobs: () =>
    request<{ data: ExportJob[] }>("GET", "/me/exports").then((r) => r.data ?? []),
  createExportJob: (data: CreateExportJobRequest) =>
    request<ExportJob>("POST", "/me/exports", data),
  getExportJob: (id: string) =>
    request<ExportJob>("GET", `/me/exports/${id}`),
  deleteExportJob: (id: string) =>
    request<void>("DELETE", `/me/exports/${id}`),

};

// Share-token types + sharesAPI live in ./shares.ts. Re-exported here so
// existing imports from "./client" continue to compile after the extraction.
export type {
  SharePermission,
  ShareGrantInput,
  ShareGrant,
  ShareBinding,
  ShareToken,
  CreateShareRequest,
  ShareCreatedResponse,
} from "./shares";
export { sharesAPI } from "./shares";

// --- Org API (org-scoped endpoints) ---

export interface OrgCreateUserRequest {
  email: string;
  display_name?: string;
  password: string;
  role: string;
}

export interface OrgUpdateUserRequest {
  display_name?: string;
  role?: string;
  settings?: Record<string, unknown>;
}

export const orgAPI = {
  listUsers: (orgId: string) =>
    request<{ data: User[] }>("GET", `/orgs/${orgId}/users`).then((r) => r.data),
  getUser: (orgId: string, userId: string) =>
    request<User>("GET", `/orgs/${orgId}/users/${userId}`),
  createUser: (orgId: string, data: OrgCreateUserRequest) =>
    request<User>("POST", `/orgs/${orgId}/users`, data),
  updateUser: (orgId: string, userId: string, data: OrgUpdateUserRequest) =>
    request<User>("PUT", `/orgs/${orgId}/users/${userId}`, data),
  deleteUser: (orgId: string, userId: string) =>
    request<void>("DELETE", `/orgs/${orgId}/users/${userId}`),

  listUserAPIKeys: (orgId: string, userId: string) =>
    request<{ data: APIKey[] }>("GET", `/orgs/${orgId}/users/${userId}/api-keys`).then((r) => r.data),
  generateUserAPIKey: (orgId: string, userId: string, data: GenerateAPIKeyRequest) =>
    request<GenerateAPIKeyResponse>("POST", `/orgs/${orgId}/users/${userId}/api-keys`, data),
  revokeUserAPIKey: (orgId: string, userId: string, keyId: string) =>
    request<void>("DELETE", `/orgs/${orgId}/users/${userId}/api-keys/${keyId}`),

  getAnalytics: (orgId: string) =>
    request<OrgAnalyticsData>("GET", `/orgs/${orgId}/analytics`),
  getUsage: (orgId: string, params?: { from?: string; to?: string; group_by?: string; user?: string; success_only?: boolean }) => {
    const sp = new URLSearchParams();
    if (params?.from) sp.set("from", params.from);
    if (params?.to) sp.set("to", params.to);
    if (params?.group_by) sp.set("group_by", params.group_by);
    if (params?.user) sp.set("user", params.user);
    if (params?.success_only !== undefined) sp.set("success_only", params.success_only ? "true" : "false");
    const qs = sp.toString();
    return request<UsageReport>("GET", `/orgs/${orgId}/usage${qs ? `?${qs}` : ""}`);
  },
  // Tier-B (org-aggregate) dashboard + activity. Added 2026-04-30 leak fix.
  getDashboard: (orgId: string) =>
    request<OrgDashboardData>("GET", `/orgs/${orgId}/dashboard`),
  getActivity: (orgId: string) =>
    request<OrgActivityResponse>("GET", `/orgs/${orgId}/activity`),

  // Org-tier dreaming + enrichment. Org owners (and admins) get
  // retry/abandon/rollback within their org; the global enable/disable +
  // pause/resume controls remain admin-only on /admin/*.
  getDreamingStatus: (orgId: string) =>
    request<OrgDreamStatusResponse>("GET", `/orgs/${orgId}/dreaming`),
  getDreamingCycles: (orgId: string) =>
    request<DreamCycle[]>("GET", `/orgs/${orgId}/dreaming/cycles`),
  getDreamingCycleDetail: (orgId: string, cycleId: string) =>
    request<DreamCycleDetail>("GET", `/orgs/${orgId}/dreaming/cycles/${cycleId}`),
  abandonDreamCycle: (orgId: string, cycleId: string) =>
    request<DreamAbandonResponse>("POST", `/orgs/${orgId}/dreaming/cycles/${cycleId}/abandon`),
  rollbackDreamCycle: (orgId: string, cycleId: string) =>
    request<DreamRollbackResponse>("POST", `/orgs/${orgId}/dreaming/cycles/${cycleId}/rollback`),
  getEnrichmentStatus: (orgId: string, params?: EnrichmentQueueParams) =>
    request<EnrichmentQueueStatus>(
      "GET",
      `/orgs/${orgId}/enrichment${buildEnrichmentQueueQuery(params)}`,
    ),
  retryEnrichment: (orgId: string, ids?: string[]) =>
    request<EnrichmentRetryResponse>("POST", `/orgs/${orgId}/enrichment/retry`, { ids: ids ?? [] }),

  listOrgIdPs: (orgId: string) =>
    request<IdPConfig[]>("GET", `/orgs/${orgId}/idp`),
  configureIdP: (orgId: string, data: CreateIdPConfigRequest) =>
    request<IdPConfig>("POST", `/orgs/${orgId}/idp`, data),
  updateOrgIdP: (orgId: string, id: string, data: UpdateIdPConfigRequest) =>
    request<IdPConfig>("PUT", `/orgs/${orgId}/idp/${id}`, data),
  deleteOrgIdP: (orgId: string, id: string) =>
    request<void>("DELETE", `/orgs/${orgId}/idp/${id}`),
};

// --- Health ---

export interface HealthProviderStatus {
  status: string;
  provider: string;
  model: string;
  latency_ms?: number | null;
}

export interface HealthResponse {
  status: "ok" | "degraded";
  version: string;
  // VCS build provenance stamped at build time. commit is "unknown" for builds
  // without a VCS stamp.
  build: { commit: string; dirty: boolean; time: string; go: string };
  backend: "sqlite" | "postgres";
  database: { status: "ok" | "error"; latency_ms: number };
  // Keyed by canonical provider-slot name (see internal/provider/slots.go).
  // The optional slots (query_augment, ingestion_decision) report their
  // fallback-resolved status. Indexed to tolerate future slots.
  providers: {
    embedding: HealthProviderStatus;
    fact: HealthProviderStatus;
    entity: HealthProviderStatus;
    query_augment: HealthProviderStatus;
    ingestion_decision: HealthProviderStatus;
    [slot: string]: HealthProviderStatus;
  };
  enrichment_queue?: { pending: number; processing: number; failed: number } | null;
  uptime_seconds: number;
}

export const healthAPI = {
  check: () => request<HealthResponse>("GET", "/health"),
};

// --- System API (tier-C: admin-only system aggregate views) ---
//
// Mounted server-side at /v1/admin/system/*. RoleAdministrator only;
// non-admin callers get 403. Returns system totals + per-org breakdown
// rows (no per-user, no per-memory, no content). See aggregate_types.go
// for the authoritative shapes.
export const systemAPI = {
  getDashboard: () =>
    request<SystemDashboardData>("GET", "/admin/system/dashboard"),
  getActivity: () =>
    request<SystemActivityResponse>("GET", "/admin/system/activity"),
  getAnalytics: () =>
    request<SystemAnalyticsData>("GET", "/admin/system/analytics"),
  getUsage: (params?: { from?: string; to?: string; group_by?: string; success_only?: boolean }) => {
    const sp = new URLSearchParams();
    if (params?.from) sp.set("from", params.from);
    if (params?.to) sp.set("to", params.to);
    if (params?.group_by) sp.set("group_by", params.group_by);
    if (params?.success_only !== undefined) sp.set("success_only", params.success_only ? "true" : "false");
    const qs = sp.toString();
    return request<UsageReport>("GET", `/admin/system/usage${qs ? `?${qs}` : ""}`);
  },
};

// --- OAuth consent + share-accept API ---
//
// Backs the pre-auth /authorize and /share/accept React pages. The OAuth
// consent flow keeps form-POSTs to /authorize for approve and deny so the
// browser follows the OAuth 302 redirect natively; these JSON endpoints
// only cover the read-side (validating the request, previewing a pasted
// share) and the share-accept landing.

export interface OAuthAuthorizeParams {
  client_id: string;
  redirect_uri: string;
  response_type: string;
  code_challenge: string;
  code_challenge_method: string;
  scope?: string;
  resource?: string;
  state?: string;
}

export interface AuthorizeContextResponse {
  client_id: string;
  client_name?: string;
  redirect_uri: string;
  response_type: string;
  code_challenge: string;
  code_challenge_method: string;
  scope?: string;
  resource?: string;
  state?: string;
  account_user: { display_name: string; email: string } | null;
  share_token_supported: boolean;
}

/**
 * Either the full context payload or a redirect_to URL (returned when an
 * OAuth error must be surfaced via the registered redirect_uri).
 */
export type AuthorizeContextResult =
  | AuthorizeContextResponse
  | { redirect_to: string };

export interface SharePreviewGrant {
  project_name: string;
  project_slug: string;
  permission: string;
}

export interface SharePreviewResponse {
  owner_name: string;
  share_name: string;
  description: string;
  expires_at: string;
  is_one_shot: boolean;
  grants: SharePreviewGrant[];
}

export interface ShareAcceptResponse {
  owner_name?: string;
  share_name?: string;
  description?: string;
  expires_at?: string;
  grants?: SharePreviewGrant[];
  mcp_server_url?: string;
  share_token?: string;
  error?: string;
}

export interface AuthorizeCompleteRequest extends OAuthAuthorizeParams {
  auth_mode: "account" | "share";
  decision: "approve" | "deny";
  share_token?: string;
}

export interface AuthorizeCompleteResponse {
  code: string;
  state?: string;
  callback_url: string;
  redirect_uri: string;
  // Present (set to "access_denied") when the user denied on the loopback path.
  error?: string;
}

// isLoopbackRedirectUri mirrors the server's isLoopbackRedirectURI
// (RFC 8252 §7.3): host "localhost", IPv6 ::1, or any IPv4 in 127.0.0.0/8.
// The consent page uses it to decide whether to complete via the loopback
// JSON flow (probe + manual fallback) instead of the standard 302 redirect.
export function isLoopbackRedirectUri(uri: string): boolean {
  let host: string;
  try {
    host = new URL(uri).hostname;
  } catch {
    return false;
  }
  if (host === "localhost") return true;
  // URL.hostname wraps IPv6 in brackets; strip them before comparing.
  if (host === "[::1]" || host === "::1") return true;
  return /^127(?:\.\d{1,3}){3}$/.test(host);
}

function oauthQueryString(params: OAuthAuthorizeParams): string {
  const sp = new URLSearchParams();
  sp.set("client_id", params.client_id);
  sp.set("redirect_uri", params.redirect_uri);
  sp.set("response_type", params.response_type);
  sp.set("code_challenge", params.code_challenge);
  sp.set("code_challenge_method", params.code_challenge_method);
  if (params.scope) sp.set("scope", params.scope);
  if (params.resource) sp.set("resource", params.resource);
  if (params.state) sp.set("state", params.state);
  return sp.toString();
}

export const oauthAPI = {
  getAuthorizeContext: (params: OAuthAuthorizeParams) =>
    request<AuthorizeContextResult>(
      "GET",
      `/oauth/authorize/context?${oauthQueryString(params)}`,
    ),
  previewShare: (params: OAuthAuthorizeParams & { share_token: string }) =>
    request<SharePreviewResponse>("POST", "/oauth/share/preview", params),
  completeAuthorize: (body: AuthorizeCompleteRequest) =>
    request<AuthorizeCompleteResponse>("POST", "/oauth/authorize/complete", body),
};

export const shareAcceptAPI = {
  get: (token: string) =>
    request<ShareAcceptResponse>(
      "GET",
      `/share/accept?token=${encodeURIComponent(token)}`,
    ),
};
