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

export interface User {
  id: string;
  email: string;
  display_name: string;
  role: string;
  org_id: string;
  created_at: string;
  updated_at: string;
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

export interface Setting {
  key: string;
  value: string;
  type: string;
  description: string;
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

export interface DatabaseInfo {
  driver: string;
  version: string;
  migration_version: number;
  dirty: boolean;
}

export interface EnrichmentQueueStatus {
  pending: number;
  processing: number;
  failed: number;
}

export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  page: number;
  per_page: number;
}

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
  listOrgs: () => request<Organization[]>("GET", "/admin/orgs"),
  getOrg: (id: string) => request<Organization>("GET", `/admin/orgs/${id}`),
  createOrg: (data: CreateOrgRequest) =>
    request<Organization>("POST", "/admin/orgs", data),
  updateOrg: (id: string, data: UpdateOrgRequest) =>
    request<Organization>("PUT", `/admin/orgs/${id}`, data),
  deleteOrg: (id: string) => request<void>("DELETE", `/admin/orgs/${id}`),

  // Users
  listUsers: () => request<User[]>("GET", "/admin/users"),
  getUser: (id: string) => request<User>("GET", `/admin/users/${id}`),
  createUser: (data: Partial<User> & { password?: string }) =>
    request<User>("POST", "/admin/users", data),
  updateUser: (id: string, data: Partial<User>) =>
    request<User>("PUT", `/admin/users/${id}`, data),
  deleteUser: (id: string) => request<void>("DELETE", `/admin/users/${id}`),

  // Projects
  listProjects: () => request<Project[]>("GET", "/admin/projects"),
  getProject: (id: string) => request<Project>("GET", `/admin/projects/${id}`),
  createProject: (data: Partial<Project>) =>
    request<Project>("POST", "/admin/projects", data),
  updateProject: (id: string, data: ProjectUpdateRequest) =>
    request<Project>("PUT", `/admin/projects/${id}`, data),
  deleteProject: (id: string) =>
    request<void>("DELETE", `/admin/projects/${id}`),

  // Providers
  listProviders: () => request<Provider[]>("GET", "/admin/providers"),
  getProvider: (id: string) =>
    request<Provider>("GET", `/admin/providers/${id}`),
  createProvider: (data: Partial<Provider>) =>
    request<Provider>("POST", "/admin/providers", data),
  updateProvider: (id: string, data: Partial<Provider>) =>
    request<Provider>("PUT", `/admin/providers/${id}`, data),
  deleteProvider: (id: string) =>
    request<void>("DELETE", `/admin/providers/${id}`),

  // Settings
  getSettings: () => request<Setting[]>("GET", "/admin/settings"),
  updateSettings: (settings: Setting[]) =>
    request<void>("PUT", "/admin/settings", settings),

  // Webhooks
  listWebhooks: () => request<Webhook[]>("GET", "/admin/webhooks"),
  createWebhook: (data: Partial<Webhook>) =>
    request<Webhook>("POST", "/admin/webhooks", data),
  updateWebhook: (id: string, data: Partial<Webhook>) =>
    request<Webhook>("PUT", `/admin/webhooks/${id}`, data),
  deleteWebhook: (id: string) =>
    request<void>("DELETE", `/admin/webhooks/${id}`),

  // Analytics
  getAnalytics: () => request<AnalyticsData[]>("GET", "/admin/analytics"),
  getUsage: () => request<Record<string, unknown>>("GET", "/admin/usage"),

  // Database
  getDatabaseInfo: () => request<DatabaseInfo>("GET", "/admin/database"),

  // Enrichment
  getEnrichmentStatus: () =>
    request<EnrichmentQueueStatus>("GET", "/admin/enrichment"),

  // Namespaces
  getNamespaceTree: () =>
    request<Record<string, unknown>>("GET", "/admin/namespaces/tree"),
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
