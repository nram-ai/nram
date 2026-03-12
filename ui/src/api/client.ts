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
}

export interface StoredMemory {
  id: string;
  content: string;
  tags?: string[];
  project_id: string;
  created_at: string;
}

export interface Memory {
  id: string;
  content: string;
  tags: string[];
  metadata: Record<string, unknown>;
  source: string;
  created_at: string;
  updated_at: string;
  enriched: boolean;
  entities?: { id: string; name: string; type: string }[];
  lineage?: { id: string; type: string; related_memory_id: string }[];
}

export interface RecallResult {
  memory: Memory;
  score: number;
}

export interface RecallRequest {
  query: string;
  limit?: number;
  tags?: string[];
  threshold?: number;
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
  memories: Memory[];
  total: number;
  offset: number;
  limit: number;
}

export interface MemoryUpdateRequest {
  content?: string;
  tags?: string[];
  metadata?: Record<string, unknown>;
}

export interface ForgetRequest {
  ids?: string[];
  filter?: { tags?: string[]; before?: string; after?: string };
}

export interface EnrichRequest {
  ids: string[];
}

export interface OrgUser {
  id: string;
  email: string;
  display_name?: string;
  role: string;
}

export interface Organization {
  id: string;
  name: string;
  slug: string;
  user_count: number;
  memory_count: number;
  project_count?: number;
  settings?: Record<string, unknown>;
  users?: OrgUser[];
  owners?: OrgUser[];
  created_at: string;
  updated_at: string;
}

export interface CreateOrgRequest {
  name: string;
  slug: string;
}

export interface UpdateOrgRequest {
  name?: string;
  settings?: Record<string, unknown>;
}

export interface APIKey {
  id: string;
  label: string;
  prefix: string;
  scopes: string[];
  expires_at?: string;
  last_used?: string;
  created_at: string;
}

export interface User {
  id: string;
  email: string;
  display_name: string;
  role: string;
  org_id: string;
  organization?: { id: string; name: string };
  last_login?: string;
  disabled: boolean;
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
  disabled?: boolean;
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
  expires_at?: string;
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
  name: string;
  slug: string;
  path: string;
  description: string;
  memory_count: number;
  entity_count: number;
  default_tags: string[];
  settings: ProjectSettings;
  owner?: ProjectOwner;
  organization?: ProjectOrganization;
  org_id: string;
  created_at: string;
  updated_at: string;
}

export interface ProjectUpdateRequest {
  name?: string;
  description?: string;
  default_tags?: string[];
  settings?: Partial<ProjectSettings>;
}

export interface Provider {
  id: string;
  name: string;
  type: string;
  enabled: boolean;
  config: Record<string, unknown>;
}

export interface ProviderSlot {
  slot: string;
  configured: boolean;
  type: string;
  url: string;
  model: string;
  dimensions?: number;
  api_key_set: boolean;
  healthy: boolean;
  last_health_check?: string;
  avg_latency_ms?: number;
}

export interface UpdateProviderSlotRequest {
  type: string;
  url: string;
  model: string;
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
  latency_ms: number;
  error?: string;
}

export interface OllamaModel {
  name: string;
  size: string;
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
  scope: string;
  enabled: boolean;
  created_at: string;
}

export interface AnalyticsData {
  period: string;
  memory_stores: number;
  memory_recalls: number;
  api_requests: number;
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
  last_used?: string;
}

export interface OAuthClientCreated extends OAuthClient {
  client_secret?: string;
}

export interface CreateOAuthClientRequest {
  name: string;
  redirect_uris: string[];
  client_type: "public" | "confidential";
}

export interface IdPConfig {
  id: string;
  org_id: string;
  org_name?: string;
  provider_type: "google" | "github" | "oidc";
  client_id: string;
  issuer_url?: string;
  authorization_url?: string;
  token_url?: string;
  userinfo_url?: string;
  allowed_domains: string[];
  auto_provision: boolean;
  created_at: string;
}

export interface CreateIdPConfigRequest {
  org_id: string;
  provider_type: "google" | "github" | "oidc";
  client_id: string;
  client_secret: string;
  issuer_url?: string;
  authorization_url?: string;
  token_url?: string;
  userinfo_url?: string;
  allowed_domains: string[];
  auto_provision: boolean;
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
  idp_type?: string | null;
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
  createProject: (data: Partial<Project>) =>
    request<Project>("POST", "/admin/projects", data),
  updateProject: (id: string, data: ProjectUpdateRequest) =>
    request<Project>("PUT", `/admin/projects/${id}`, data),
  deleteProject: (id: string) =>
    request<void>("DELETE", `/admin/projects/${id}`),

  // Providers (legacy)
  listProviders: () => request<Provider[]>("GET", "/admin/providers"),
  getProvider: (id: string) =>
    request<Provider>("GET", `/admin/providers/${id}`),
  createProvider: (data: Partial<Provider>) =>
    request<Provider>("POST", "/admin/providers", data),
  updateProvider: (id: string, data: Partial<Provider>) =>
    request<Provider>("PUT", `/admin/providers/${id}`, data),
  deleteProvider: (id: string) =>
    request<void>("DELETE", `/admin/providers/${id}`),

  // Provider slots — backend returns { embedding: {...}, fact: {...}, entity: {...} }
  getProviderSlots: () =>
    request<ProviderConfigResponse>("GET", "/admin/providers").then((r) => {
      const mapping: Record<string, string> = {
        embedding: "embedding",
        fact: "fact_extraction",
        entity: "entity_extraction",
      };
      return Object.entries(mapping).map(([key, slot]) => ({
        slot,
        ...(r[key as keyof ProviderConfigResponse] ?? {}),
      })) as ProviderSlot[];
    }),
  updateProviderSlot: (slot: string, data: UpdateProviderSlotRequest) =>
    request<ProviderSlot>("PUT", `/admin/providers/${slot}`, data),
  testProviderSlot: (slot: string) =>
    request<TestProviderResult>("POST", `/admin/providers/${slot}/test`),
  getOllamaModels: () =>
    request<OllamaModel[]>("GET", "/admin/providers/ollama/models").then(
      (models) => ({ models }),
    ),
  pullOllamaModel: (model: string) =>
    request<{ status: string }>("POST", "/admin/providers/ollama/pull", { model }),

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
  createWebhook: (data: Partial<Webhook>) =>
    request<Webhook>("POST", "/admin/webhooks", data),
  updateWebhook: (id: string, data: Partial<Webhook>) =>
    request<Webhook>("PUT", `/admin/webhooks/${id}`, data),
  deleteWebhook: (id: string) =>
    request<void>("DELETE", `/admin/webhooks/${id}`),
  testWebhook: (id: string) =>
    request<{ success: boolean; status_code?: number; error?: string }>(
      "POST",
      `/admin/webhooks/${id}/test`,
    ),

  // Analytics
  getAnalytics: () => request<AnalyticsData[]>("GET", "/admin/analytics"),
  getUsage: () => request<Record<string, unknown>>("GET", "/admin/usage"),

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
    request<Record<string, unknown>>("GET", "/admin/namespaces/tree"),

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
    request<RecallResult[]>(
      "POST",
      `/projects/${projectId}/memories/recall`,
      body,
    ),

  get: (projectId: string, memoryId: string) =>
    request<Memory>("GET", `/projects/${projectId}/memories/${memoryId}`),

  update: (projectId: string, memoryId: string, body: MemoryUpdateRequest) =>
    request<Memory>(
      "PUT",
      `/projects/${projectId}/memories/${memoryId}`,
      body,
    ),

  remove: (projectId: string, memoryId: string) =>
    request<void>("DELETE", `/projects/${projectId}/memories/${memoryId}`),

  forget: (projectId: string, body: ForgetRequest) =>
    request<void>("POST", `/projects/${projectId}/memories/forget`, body),

  enrich: (projectId: string, body: EnrichRequest) =>
    request<void>("POST", `/projects/${projectId}/memories/enrich`, body),

  export: (projectId: string) =>
    request<Memory[]>("GET", `/projects/${projectId}/memories/export`),
};

// --- Health ---

export const healthAPI = {
  check: () => request<{ status: string }>("GET", "/health"),
};
