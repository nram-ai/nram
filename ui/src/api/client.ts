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

export interface DashboardData {
  memory_count: number;
  project_count: number;
  user_count: number;
  org_count: number;
  recent_activity: ActivityEntry[];
}

export interface ActivityEntry {
  id: string;
  action: string;
  resource_type: string;
  resource_id: string;
  actor: string;
  timestamp: string;
}

export interface Organization {
  id: string;
  name: string;
  slug: string;
  created_at: string;
  updated_at: string;
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

export interface Project {
  id: string;
  name: string;
  slug: string;
  org_id: string;
  created_at: string;
  updated_at: string;
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
    request<ActivityEntry[]>("GET", `/admin/activity?limit=${limit}`),

  // Organizations
  listOrgs: () => request<Organization[]>("GET", "/admin/orgs"),
  getOrg: (id: string) => request<Organization>("GET", `/admin/orgs/${id}`),
  createOrg: (data: Partial<Organization>) =>
    request<Organization>("POST", "/admin/orgs", data),
  updateOrg: (id: string, data: Partial<Organization>) =>
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
  updateProject: (id: string, data: Partial<Project>) =>
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

// --- Health ---

export const healthAPI = {
  check: () => request<{ status: string }>("GET", "/health"),
};
