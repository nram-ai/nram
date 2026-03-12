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
): Promise<T> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...getAuthHeaders(),
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
      if (window.location.pathname !== "/login" && window.location.pathname !== "/setup") {
        window.location.href = "/login";
        return new Promise<T>(() => {}); // never resolves — page is navigating
      }
    }
    let errBody: unknown;
    try {
      errBody = await res.json();
    } catch {
      errBody = await res.text();
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
}

export interface RecallResult {
  id: string;
  project_id: string;
  content: string;
  tags: string[];
  source: string | null;
  score: number;
  similarity?: number | null;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface RecallResponse {
  memories: RecallResult[];
  entities?: RecallEntity[];
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
  tag?: string;
  text?: string;
  from?: string;
  to?: string;
  enriched?: string;
  source?: string;
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
  status?: string;
  latency_ms?: number | null;
}

export interface UpdateProviderSlotRequest {
  type: string;
  url?: string;
  model?: string;
  api_key?: string;
  dimensions?: number;
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

export interface UsageGroup {
  key: string;
  tokens_input: number;
  tokens_output: number;
  call_count: number;
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

export interface MigrationStatus {
  status: string;
  message: string;
}

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
  allowed_domains?: string[];
  auto_provision?: boolean;
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
  total: number;
  page: number;
  per_page: number;
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
}

export const authAPI = {
  login: (data: LoginRequest) => request<LoginResponse>("POST", "/auth/login", data),
  lookup: (data: LookupRequest) => request<LookupResponse>("POST", "/auth/lookup", data),
};

// --- Admin API client ---

export const adminAPI = {
  // Setup
  getSetupStatus: () => request<SetupStatus>("GET", "/admin/setup/status"),
  completeSetup: (data: SetupRequest) =>
    request<SetupResponse>("POST", "/admin/setup", data),

  // Dashboard
  getDashboard: () => request<DashboardData>("GET", "/admin/dashboard"),

  // Activity
  getActivity: (limit = 50) =>
    request<ActivityResponse>("GET", `/admin/activity?limit=${limit}`),

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
  getProject: (id: string) => request<Project>("GET", `/admin/projects/${id}`),
  createProject: (data: AdminCreateProjectRequest) =>
    request<Project>("POST", "/admin/projects", data),
  updateProject: (id: string, data: ProjectUpdateRequest) =>
    request<Project>("PUT", `/admin/projects/${id}`, data),
  deleteProject: (id: string) =>
    request<void>("DELETE", `/admin/projects/${id}`),

  // Provider slots — backend returns { embedding: {...}, fact: {...}, entity: {...} }
  getProviderSlots: () =>
    request<ProviderConfigResponse>("GET", "/admin/providers").then((r) => {
      return (["embedding", "fact", "entity"] as const).map((slot) => ({
        slot,
        ...(r[slot] ?? {}),
      })) as ProviderSlot[];
    }),
  updateProviderSlot: (slot: string, data: UpdateProviderSlotRequest) =>
    request<{ status: string }>("PUT", `/admin/providers/${slot}`, data),
  testProviderSlot: (slot: string, config: UpdateProviderSlotRequest) =>
    request<TestProviderResult>("POST", "/admin/providers/test", { slot, config }),
  getOllamaModels: () =>
    request<OllamaModel[]>("GET", "/admin/providers/ollama/models").then(
      (models) => ({ models }),
    ),
  pullOllamaModel: (model: string) =>
    request<{ status: string; model: string }>("POST", "/admin/providers/ollama/pull", { model }),

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
  getAnalytics: () => request<AnalyticsData>("GET", "/admin/analytics"),
  getUsage: (params?: { org?: string; user?: string; project?: string; from?: string; to?: string; group_by?: string }) => {
    const sp = new URLSearchParams();
    if (params?.org) sp.set("org", params.org);
    if (params?.user) sp.set("user", params.user);
    if (params?.project) sp.set("project", params.project);
    if (params?.from) sp.set("from", params.from);
    if (params?.to) sp.set("to", params.to);
    if (params?.group_by) sp.set("group_by", params.group_by);
    const qs = sp.toString();
    return request<UsageReport>("GET", `/admin/usage${qs ? `?${qs}` : ""}`);
  },

  // Database
  getDatabaseInfo: () => request<DatabaseInfo>("GET", "/admin/database"),
  testDatabaseConnection: (url: string) =>
    request<ConnectionTestResult>("POST", "/admin/database/test", { url }),
  triggerMigration: (url: string) =>
    request<MigrationStatus>("POST", "/admin/database/migrate", { url }),

  // Enrichment
  getEnrichmentStatus: () =>
    request<EnrichmentQueueStatus>("GET", "/admin/enrichment"),
  retryEnrichment: (ids?: string[]) =>
    request<EnrichmentRetryResponse>("POST", "/admin/enrichment/retry", { ids: ids ?? [] }),
  pauseEnrichment: (paused: boolean) =>
    request<EnrichmentPauseResponse>("POST", "/admin/enrichment/pause", { paused }),
  testExtractionPrompt: (
    type: "fact" | "entity",
    prompt: string,
    sampleInput: string,
  ) =>
    request<ExtractionTestResult>("POST", "/admin/enrichment/test-prompt", {
      type,
      prompt,
      sample_input: sampleInput,
    }),

  // Graph
  getGraph: (projectId: string) =>
    request<GraphData>("GET", `/admin/graph?project=${encodeURIComponent(projectId)}`),

  // Namespaces
  getNamespaceTree: () =>
    request<{ tree: NamespaceNode[] }>("GET", "/admin/namespaces/tree"),

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

export const memoryAPI = {
  store: (projectId: string, data: StoreMemoryRequest) =>
    request<StoredMemory>("POST", `/projects/${projectId}/memories`, data),

  list: (projectId: string, params?: MemoryListParams) => {
    const sp = new URLSearchParams();
    if (params?.limit !== undefined) sp.set("limit", String(params.limit));
    if (params?.offset !== undefined) sp.set("offset", String(params.offset));
    if (params?.tag) sp.set("tag", params.tag);
    if (params?.text) sp.set("text", params.text);
    if (params?.from) sp.set("from", params.from);
    if (params?.to) sp.set("to", params.to);
    if (params?.enriched) sp.set("enriched", params.enriched);
    if (params?.source) sp.set("source", params.source);
    const qs = sp.toString();
    return request<MemoryListResponse>(
      "GET",
      `/projects/${projectId}/memories${qs ? `?${qs}` : ""}`,
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
