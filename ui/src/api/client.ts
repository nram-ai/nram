/** Base URL is auto-detected: same origin in production, proxied in dev. */
const BASE_URL = "/v1";

export class APIError extends Error {
  constructor(
    public status: number,
    public body: unknown,
  ) {
    super(`API error ${status}`);
    this.name = "APIError";
  }
}

function getAuthHeaders(): Record<string, string> {
  const token = localStorage.getItem("nram_token");
  if (token) {
    return { Authorization: `Bearer ${token}` };
  }
  return {};
}

async function request<T>(
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

  if (!res.ok) {
    // On 401, the token is invalid or expired — clear it and redirect to login.
    if (res.status === 401) {
      localStorage.removeItem("nram_token");
      localStorage.removeItem("nram_user");
      if (window.location.pathname !== "/login" && window.location.pathname !== "/setup") {
        window.location.href = "/login";
        return new Promise<T>(() => {}); // never resolves — page is navigating
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

export interface ActivityEvent {
  id: string;
  type: string;
  summary: string;
  project_id?: string;
  user_id?: string;
  timestamp: string;
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

export interface Memory {
  id: string;
  namespace_id?: string;
  content: string;
  embedding_dim?: number | null;
  source: string | null;
  tags: string[];
  confidence?: number;
  importance?: number;
  access_count?: number;
  last_accessed?: string | null;
  expires_at?: string | null;
  superseded_by?: string | null;
  enriched: boolean;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  deleted_at?: string | null;
  purge_after?: string | null;
  parent_id?: string | null;
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
  score: number;
  similarity?: number | null;
  confidence?: number;
  shared_from?: string | null;
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

export interface MemoryListParams {
  limit?: number;
  offset?: number;
  /** AND semantics — memory must contain all listed tags */
  tags?: string[];
  /** RFC3339 or YYYY-MM-DD */
  date_from?: string;
  /** RFC3339 or YYYY-MM-DD; inclusive of the entire day when YYYY-MM-DD */
  date_to?: string;
  /** "true" → enriched only, "false" → not-enriched only, undefined → no filter */
  enriched?: "true" | "false";
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

export interface ProjectRankingWeights {
  recency: number;
  relevance: number;
  importance: number;
}

export interface ProjectSettings {
  dedup_threshold: number;
  enrichment_enabled: boolean;
  ranking_weights: ProjectRankingWeights;
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
  default_tags: string[];
  settings: ProjectSettings;
  owner?: ProjectOwner;
  organization?: ProjectOrganization;
  org_id?: string;
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
  configured: boolean;
  type: string;
  url: string;
  model: string;
  dimensions?: number | null;
  timeout?: number | null;
  status?: string;
  latency_ms?: number | null;
}

export interface UpdateProviderSlotRequest {
  type: string;
  url?: string;
  model?: string;
  api_key?: string;
  timeout?: number;
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

export interface ProviderConfigResponse {
  embedding: Omit<ProviderSlot, "slot">;
  fact: Omit<ProviderSlot, "slot">;
  entity: Omit<ProviderSlot, "slot">;
}

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

export interface MemoryRankItem {
  id: string;
  content: string;
  access_count: number;
  project_id?: string | null;
  created_at: string;
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

export interface DreamCycle {
  id: string;
  project_id: string;
  namespace_id: string;
  status: string;
  phase: string;
  tokens_used: number;
  token_budget: number;
  phase_summary: unknown;
  error: string | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface DreamLog {
  id: string;
  cycle_id: string;
  project_id: string;
  phase: string;
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
  recent_cycles: DreamCycle[];
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
  status: string;
  attempts: number;
  last_error?: string;
  created_at: string;
}

export interface EnrichmentQueueStatus {
  counts: EnrichmentQueueCounts;
  items: EnrichmentQueueItem[];
  paused: boolean;
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
  error?: string;
  latency_ms: number;
}

export interface OAuthClient {
  id: string;
  name: string;
  client_id: string;
  type: "auto" | "manual";
  client_type: "public" | "confidential";
  redirect_uris: string[];
  created_at: string;
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
}

export interface PaginatedResponse<T> {
  data: T[];
  pagination: {
    total: number;
    limit: number;
    offset: number;
  };
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

  // Projects
  listProjects: () => request<{ data: Project[] }>("GET", "/admin/projects").then(r => r.data),
  createProject: (data: AdminCreateProjectRequest) =>
    request<Project>("POST", "/admin/projects", data),
  // Provider slots — backend returns { embedding: {...}, fact: {...}, entity: {...} }
  getProviderSlots: () =>
    request<ProviderConfigResponse>("GET", "/admin/providers").then((r) => {
      return (["embedding", "fact", "entity"] as const).map((slot) => ({
        slot,
        ...(r[slot] ?? {}),
      })) as ProviderSlot[];
    }),
  updateProviderSlot: (slot: string, data: UpdateProviderSlotRequest) =>
    request<UpdateProviderSlotResult | { status: string }>("PUT", `/admin/providers/${slot}`, data),
  testProviderSlot: (slot: string, config: UpdateProviderSlotRequest) =>
    request<TestProviderResult>("POST", "/admin/providers/test", { slot, config }),
  getOllamaModels: (ollamaUrl?: string) => {
    const params = ollamaUrl ? `?url=${encodeURIComponent(ollamaUrl)}` : "";
    return request<OllamaModel[]>("GET", `/admin/providers/ollama/models${params}`).then(
      (models) => ({ models }),
    );
  },
  pullOllamaModel: (model: string, ollamaUrl?: string) =>
    request<{ status: string; model: string }>("POST", "/admin/providers/ollama/pull", { model, url: ollamaUrl || undefined }),

  // Settings
  getSettings: (scope?: string) => {
    const params = scope ? `?scope=${encodeURIComponent(scope)}` : "";
    return request<{ data: Setting[] }>("GET", `/admin/settings${params}`);
  },
  getSettingsSchema: () =>
    request<{ data: SettingSchema[] }>("GET", "/admin/settings?schema=true"),
  updateSetting: (key: string, value: unknown, scope: string) =>
    request<{ status: string }>("PUT", "/admin/settings", { key, value, scope }),

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

  // Analytics
  getAnalytics: () => request<AnalyticsData>("GET", "/analytics"),
  getUsage: (params?: { org?: string; user?: string; project?: string; from?: string; to?: string; group_by?: UsageGroupBy; success_only?: boolean }) => {
    const sp = new URLSearchParams();
    if (params?.org) sp.set("org", params.org);
    if (params?.user) sp.set("user", params.user);
    if (params?.project) sp.set("project", params.project);
    if (params?.from) sp.set("from", params.from);
    if (params?.to) sp.set("to", params.to);
    if (params?.group_by) sp.set("group_by", params.group_by);
    if (params?.success_only !== undefined) sp.set("success_only", params.success_only ? "true" : "false");
    const qs = sp.toString();
    return request<UsageReport>("GET", `/usage${qs ? `?${qs}` : ""}`);
  },

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
  triggerMigration: (url: string) =>
    request<MigrationStatus>("POST", "/admin/database/migrate", { url }),

  // Dreaming
  getDreamingStatus: () =>
    request<DreamStatusResponse>("GET", "/dreaming"),
  getDreamingCycles: (projectId?: string) => {
    const qs = projectId ? `?project_id=${encodeURIComponent(projectId)}` : "";
    return request<DreamCycle[]>("GET", `/dreaming/cycles${qs}`);
  },
  getDreamingCycleDetail: (cycleId: string) =>
    request<DreamCycleDetail>("GET", `/dreaming/cycles/${cycleId}`),
  setDreamingEnabled: (enabled: boolean) =>
    request<DreamEnableResponse>("POST", "/dreaming/enable", { enabled }),
  setProjectDreamingEnabled: (projectId: string, enabled: boolean) =>
    request<{ project_id: string; enabled: boolean }>("POST", "/dreaming/project/enable", {
      project_id: projectId,
      enabled,
    }),
  rollbackDreamCycle: (cycleId: string) =>
    request<DreamRollbackResponse>("POST", "/dreaming/rollback", { cycle_id: cycleId }),

  // Enrichment
  getEnrichmentStatus: () =>
    request<EnrichmentQueueStatus>("GET", "/enrichment"),
  retryEnrichment: (ids?: string[]) =>
    request<EnrichmentRetryResponse>("POST", "/enrichment/retry", { ids: ids ?? [] }),
  pauseEnrichment: (paused: boolean) =>
    request<EnrichmentPauseResponse>("POST", "/enrichment/pause", { paused }),
  testExtractionPrompt: (
    type: "fact" | "entity",
    prompt: string,
    sampleInput: string,
  ) =>
    request<ExtractionTestResult>("POST", "/enrichment/test-prompt", {
      type,
      prompt,
      sample_input: sampleInput,
    }),

  // Graph
  getGraph: (projectId: string) =>
    request<GraphData>("GET", `/graph?project=${encodeURIComponent(projectId)}`),

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

export interface UpdateMemoryResponse {
  id: string;
  project_id: string;
  content: string;
  tags: string[];
  previous_content?: string;
  re_embedded: boolean;
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

  get: (projectId: string, memoryId: string) =>
    request<Memory>("GET", `/projects/${projectId}/memories/${memoryId}`),

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

export const meAPI = {
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
};

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
    request<AnalyticsData>("GET", `/orgs/${orgId}/analytics`),
  getUsage: (orgId: string, params?: { from?: string; to?: string; group_by?: string }) => {
    const sp = new URLSearchParams();
    if (params?.from) sp.set("from", params.from);
    if (params?.to) sp.set("to", params.to);
    if (params?.group_by) sp.set("group_by", params.group_by);
    const qs = sp.toString();
    return request<UsageReport>("GET", `/orgs/${orgId}/usage${qs ? `?${qs}` : ""}`);
  },

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
  backend: "sqlite" | "postgres";
  database: { status: "ok" | "error"; latency_ms: number };
  providers: {
    embedding: HealthProviderStatus;
    fact_extraction: HealthProviderStatus;
    entity_extraction: HealthProviderStatus;
  };
  enrichment_queue?: { pending: number; processing: number; failed: number } | null;
  uptime_seconds: number;
}

export const healthAPI = {
  check: () => request<HealthResponse>("GET", "/health"),
};
