import {
  useQuery,
  useInfiniteQuery,
  useMutation,
  useQueryClient,
} from "@tanstack/react-query";
import { useMemo } from "react";
import {
  adminAPI,
  meAPI,
  orgAPI,
  systemAPI,
  authAPI,
  changePassword,
  memoryAPI,
  healthAPI,
  type SetupRequest,
  type SetupResponse,
  type CreateOrgRequest,
  type UpdateOrgRequest,
  type CreateUserRequest,
  type UpdateUserRequest,
  type GenerateAPIKeyRequest,
  type GenerateAPIKeyResponse,
  type AdminCreateProjectRequest,
  type ProjectUpdateRequest,
  type WebhookCreateRequest,
  type WebhookUpdateRequest,
  type StoreMemoryRequest,
  type MemoryListParams,
  type RecallRequest,
  type MemoryUpdateRequest,
  type ForgetRequest,
  type EnrichRequest,
  type UpdateProviderSlotRequest,
  type TestProviderResult,
  type ExtractionTestResult,
  type OAuthClientCreated,
  type CreateOAuthClientRequest,
  type IdPConfig,
  type CreateIdPConfigRequest,
  type UpdateIdPConfigRequest,
  type WebhookTestResult,
  type MeCreateProjectRequest,
  type MeCreateAPIKeyRequest,
  type MeCreateAPIKeyResponse,
  type OrgCreateUserRequest,
  type OrgUpdateUserRequest,
  type Passkey,
  type LoginResponse,
  type SystemRankingWeights,
  type UsageGroupBy,
} from "../api/client";
import {
  isWebAuthnAvailable,
  prepareCreationOptions,
  prepareRequestOptions,
  serializeCreationResponse,
  serializeAssertionResponse,
} from "../api/webauthn";

// --- Health ---

export function useHealth() {
  return useQuery({
    queryKey: ["health"],
    queryFn: healthAPI.check,
    refetchInterval: 30_000,
  });
}

// --- Setup ---

export function useSetupStatus() {
  return useQuery({
    queryKey: ["admin", "setup-status"],
    queryFn: adminAPI.getSetupStatus,
  });
}

export function useCompleteSetup() {
  const qc = useQueryClient();
  return useMutation<SetupResponse, Error, SetupRequest>({
    mutationFn: (data: SetupRequest) => adminAPI.completeSetup(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "setup-status"] });
    },
  });
}

// --- Dashboard / Activity (tier-A self-scoped) ---
//
// useDashboard / useActivity hit the self-tier endpoints (/v1/dashboard,
// /v1/activity). Admin sees admin's own data here, not system-wide. Use
// useOrgDashboard / useSystemDashboard for the wider tiers.

export function useDashboard() {
  return useQuery({
    queryKey: ["self", "dashboard"],
    queryFn: adminAPI.getDashboard,
    refetchInterval: 30_000,
  });
}

export function useActivity(limit = 20) {
  return useQuery({
    queryKey: ["self", "activity", limit],
    queryFn: () => adminAPI.getActivity(limit),
    refetchInterval: 30_000,
  });
}

// --- Tier-B (org-aggregate) hooks ---
//
// Caller must be RoleOrgOwner+ of the org. Aggregate counts + distributions
// only — no row-level user/memory data, no content fields.

export function useOrgDashboard(orgId: string | undefined) {
  return useQuery({
    queryKey: ["org", "dashboard", orgId],
    queryFn: () => orgAPI.getDashboard(orgId!),
    enabled: !!orgId,
    refetchInterval: 30_000,
  });
}

export function useOrgActivity(orgId: string | undefined) {
  return useQuery({
    queryKey: ["org", "activity", orgId],
    queryFn: () => orgAPI.getActivity(orgId!),
    enabled: !!orgId,
    refetchInterval: 30_000,
  });
}

export function useOrgAnalytics(orgId: string | undefined) {
  return useQuery({
    queryKey: ["org", "analytics", orgId],
    queryFn: () => orgAPI.getAnalytics(orgId!),
    enabled: !!orgId,
    staleTime: 30_000,
  });
}

export function useOrgUsage(
  orgId: string | undefined,
  params?: Parameters<typeof orgAPI.getUsage>[1],
) {
  return useQuery({
    queryKey: ["org", "usage", orgId, params ?? {}],
    queryFn: () => orgAPI.getUsage(orgId!, params),
    enabled: !!orgId,
    staleTime: 30_000,
  });
}

// --- Tier-C (system-aggregate) hooks ---
//
// RoleAdministrator only (server-enforced via /v1/admin/* gate). System
// totals + per-org breakdown rows.

export function useSystemDashboard(opts: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: ["system", "dashboard"],
    queryFn: systemAPI.getDashboard,
    refetchInterval: 30_000,
    enabled: opts.enabled ?? true,
  });
}

export function useSystemActivity(opts: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: ["system", "activity"],
    queryFn: systemAPI.getActivity,
    refetchInterval: 30_000,
    enabled: opts.enabled ?? true,
  });
}

export function useSystemAnalytics(opts: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: ["system", "analytics"],
    queryFn: systemAPI.getAnalytics,
    staleTime: 30_000,
    enabled: opts.enabled ?? true,
  });
}

export function useSystemUsage(
  params?: Parameters<typeof systemAPI.getUsage>[0],
  opts: { enabled?: boolean } = {},
) {
  return useQuery({
    queryKey: ["system", "usage", params ?? {}],
    queryFn: () => systemAPI.getUsage(params),
    staleTime: 30_000,
    enabled: opts.enabled ?? true,
  });
}

// --- Store Memory ---

export function useStoreMemory() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      data,
    }: {
      projectId: string;
      data: StoreMemoryRequest;
    }) => memoryAPI.store(projectId, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "dashboard"] });
      qc.invalidateQueries({ queryKey: ["admin", "activity"] });
    },
  });
}

// --- Organizations ---

export function useOrgs(enabled = true) {
  return useQuery({
    queryKey: ["admin", "orgs"],
    queryFn: adminAPI.listOrgs,
    enabled,
  });
}

export function useOrg(id: string) {
  return useQuery({
    queryKey: ["admin", "orgs", id],
    queryFn: () => adminAPI.getOrg(id),
    enabled: !!id,
  });
}

export function useCreateOrg() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: CreateOrgRequest) => adminAPI.createOrg(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "orgs"] });
    },
  });
}

export function useUpdateOrg() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: UpdateOrgRequest }) =>
      adminAPI.updateOrg(id, data),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["admin", "orgs"] });
      qc.invalidateQueries({ queryKey: ["admin", "orgs", vars.id] });
    },
  });
}

export function useDeleteOrg() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => adminAPI.deleteOrg(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "orgs"] });
    },
  });
}

// --- Users ---

export function useUsers(enabled = true) {
  return useQuery({
    queryKey: ["admin", "users"],
    queryFn: adminAPI.listUsers,
    enabled,
  });
}

export function useUser(id: string) {
  return useQuery({
    queryKey: ["admin", "users", id],
    queryFn: () => adminAPI.getUser(id),
    enabled: !!id,
  });
}

export function useCreateUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: CreateUserRequest) => adminAPI.createUser(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "users"] });
    },
  });
}

export function useUpdateUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: UpdateUserRequest }) =>
      adminAPI.updateUser(id, data),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["admin", "users"] });
      qc.invalidateQueries({ queryKey: ["admin", "users", vars.id] });
    },
  });
}

export function useDeleteUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => adminAPI.deleteUser(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "users"] });
    },
  });
}

export function useGenerateAPIKey() {
  const qc = useQueryClient();
  return useMutation<
    GenerateAPIKeyResponse,
    Error,
    { userId: string; data: GenerateAPIKeyRequest }
  >({
    mutationFn: ({ userId, data }) => adminAPI.generateAPIKey(userId, data),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["admin", "users", vars.userId] });
    },
  });
}

export function useRevokeAPIKey() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ userId, keyId }: { userId: string; keyId: string }) =>
      adminAPI.revokeAPIKey(userId, keyId),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["admin", "users", vars.userId] });
    },
  });
}

// --- Projects ---

export function useProjects() {
  return useQuery({
    queryKey: ["admin", "projects"],
    queryFn: adminAPI.listProjects,
  });
}

export function useProject(id: string) {
  return useQuery({
    queryKey: ["me", "projects", id],
    queryFn: () => meAPI.getProject(id),
    enabled: !!id,
  });
}

export function useCreateProject() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: AdminCreateProjectRequest) => adminAPI.createProject(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "projects"] });
    },
  });
}

export function useUpdateProject() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: ProjectUpdateRequest }) =>
      meAPI.updateProject(id, data),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["admin", "projects"] });
      qc.invalidateQueries({ queryKey: ["me", "projects"] });
      qc.invalidateQueries({ queryKey: ["me", "projects", vars.id] });
    },
  });
}

export function useDeleteProject() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => meAPI.deleteProject(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "projects"] });
      qc.invalidateQueries({ queryKey: ["me", "projects"] });
    },
  });
}

// --- Provider Slots ---

export function useProviderSlots() {
  return useQuery({
    queryKey: ["admin", "provider-slots"],
    queryFn: adminAPI.getProviderSlots,
  });
}

export function useUpdateProviderSlot() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      slot,
      data,
    }: {
      slot: string;
      data: UpdateProviderSlotRequest;
    }) => adminAPI.updateProviderSlot(slot, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "provider-slots"] });
      qc.invalidateQueries({ queryKey: ["me", "capabilities"] });
    },
  });
}

export function useTestProviderSlot() {
  return useMutation<TestProviderResult, Error, { slot: string; config: UpdateProviderSlotRequest }>({
    mutationFn: ({ slot, config }) => adminAPI.testProviderSlot(slot, config),
  });
}

export function useOllamaModels(ollamaUrl?: string) {
  return useQuery({
    queryKey: ["admin", "ollama-models", ollamaUrl],
    queryFn: () => adminAPI.getOllamaModels(ollamaUrl),
    enabled: false,
  });
}

export function usePullOllamaModel() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ model, ollamaUrl }: { model: string; ollamaUrl?: string }) =>
      adminAPI.pullOllamaModel(model, ollamaUrl),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "ollama-models"] });
    },
  });
}

// --- Settings ---

export function useSettings(scope?: string) {
  return useQuery({
    queryKey: ["admin", "settings", scope],
    queryFn: () => adminAPI.getSettings(scope),
  });
}

export function useSettingsSchema() {
  return useQuery({
    queryKey: ["admin", "settings-schema"],
    queryFn: adminAPI.getSettingsSchema,
  });
}

// useSystemRankingWeights resolves the six ranking.weight.* settings into a
// fully-populated baseline view. Used by the project edit panel to show
// system defaults as input placeholders and to compute the effective merged
// weights when a project sets sparse overrides.
//
// Resolution order per field: configured value (operator override at any
// scope) → schema default → built-in fallback. The fallback values match
// service.DefaultRankingWeights so the placeholders never disagree with the
// runtime baseline if both server queries are still loading.
const SYSTEM_RANKING_WEIGHT_KEYS = {
  similarity: "ranking.weight.similarity",
  recency: "ranking.weight.recency",
  importance: "ranking.weight.importance",
  frequency: "ranking.weight.frequency",
  graph_relevance: "ranking.weight.graph_relevance",
  confidence: "ranking.weight.confidence",
} as const;

function coerceWeight(raw: unknown): number | null {
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  if (typeof raw === "string") {
    const n = Number(raw);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

export interface SystemRankingWeightsResolution {
  weights: SystemRankingWeights | null;
  // missingKeys lists ranking.weight.* keys for which neither the operator
  // value nor the schema default could be resolved. The hook surfaces this
  // to its consumer so a schema/server mismatch becomes a visible error
  // rather than silently substituting a stale built-in fallback.
  missingKeys: string[];
}

// resolveSystemRankingWeights is the pure resolution logic. Exposed for
// unit testing without spinning up React Query infrastructure. Returns
// null weights when any key is missing — the schema endpoint is the
// authoritative source, no client-side fallback.
export function resolveSystemRankingWeights(
  configured: Iterable<{ key: string; value: unknown }>,
  schema: Iterable<{ key: string; default_value: unknown }>,
): SystemRankingWeightsResolution {
  const settingByKey = new Map<string, unknown>();
  for (const s of configured) settingByKey.set(s.key, s.value);
  const defaultByKey = new Map<string, unknown>();
  for (const s of schema) defaultByKey.set(s.key, s.default_value);

  const resolved = {} as SystemRankingWeights;
  const missing: string[] = [];
  for (const [field, key] of Object.entries(SYSTEM_RANKING_WEIGHT_KEYS) as [
    keyof SystemRankingWeights,
    string,
  ][]) {
    const fromSetting = coerceWeight(settingByKey.get(key));
    if (fromSetting !== null) {
      resolved[field] = fromSetting;
      continue;
    }
    const fromSchema = coerceWeight(defaultByKey.get(key));
    if (fromSchema !== null) {
      resolved[field] = fromSchema;
      continue;
    }
    missing.push(key);
  }
  if (missing.length > 0) {
    return { weights: null, missingKeys: missing };
  }
  return { weights: resolved, missingKeys: [] };
}

// SchemaRange is the operator-tunable range for a numeric setting,
// resolved from the schema endpoint. Each field falls back to the
// caller-supplied default when the schema hasn't loaded yet OR when
// the entry has no constraint set. Spread directly onto a numeric
// input: `<input type="number" {...range} />`.
export interface SchemaRange {
  min: number;
  max: number;
  step: number;
}

// useSchemaRange resolves the {min, max, step} for a numeric setting from
// the schema endpoint. Falls back per-field when the schema is missing or
// the entry has no constraint. Use the spread pattern in JSX:
//   const range = useSchemaRange("ranking.weight.similarity", { min: 0, max: 1, step: 0.05 });
//   return <input type="number" {...range} />;
export function useSchemaRange(key: string, fallback: SchemaRange): SchemaRange {
  const schemaQuery = useSettingsSchema();
  return useMemo(() => {
    const entries = schemaQuery.data?.data ?? [];
    const entry = entries.find((e) => e.key === key);
    if (!entry) return fallback;
    return {
      min: typeof entry.min === "number" && Number.isFinite(entry.min) ? entry.min : fallback.min,
      max: typeof entry.max === "number" && Number.isFinite(entry.max) ? entry.max : fallback.max,
      step:
        typeof entry.step === "number" && Number.isFinite(entry.step) && entry.step > 0
          ? entry.step
          : fallback.step,
    };
  }, [schemaQuery.data, key, fallback.min, fallback.max, fallback.step]);
}

// useSystemRankingWeights returns null weights when any required schema key
// is missing. Consumers MUST handle the null case (typically a red toast
// banner) instead of silently rendering stale defaults — that's the
// "AC6 contract" the contract test enforces.
export function useSystemRankingWeights(): SystemRankingWeightsResolution {
  const settingsQuery = useSettings("global");
  const schemaQuery = useSettingsSchema();
  // useMemo gives the consumer (ProjectManagement edit panel) a stable
  // reference between unrelated re-renders — without it every keystroke
  // in any form input rebuilds the resolution Maps.
  return useMemo(
    () =>
      resolveSystemRankingWeights(
        settingsQuery.data?.data ?? [],
        schemaQuery.data?.data ?? [],
      ),
    [settingsQuery.data, schemaQuery.data],
  );
}

export function useUpdateSetting() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ key, value, scope }: { key: string; value: unknown; scope: string }) =>
      adminAPI.updateSetting(key, value, scope),
    onSuccess: () => {
      // Settings, dreaming, and self-tier dreaming all read from the same
      // global key (dreaming.enabled). A change to one must invalidate the
      // others or each page renders a stale view of the same field.
      qc.invalidateQueries({ queryKey: ["admin", "settings"] });
      qc.invalidateQueries({ queryKey: ["admin", "dreaming"] });
      qc.invalidateQueries({ queryKey: ["me", "dreaming"] });
      qc.invalidateQueries({ queryKey: ["cost-rates"] });
    },
  });
}

// --- Webhooks ---

export function useWebhooks() {
  return useQuery({
    queryKey: ["admin", "webhooks"],
    queryFn: adminAPI.listWebhooks,
  });
}

export function useCreateWebhook() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: WebhookCreateRequest) => adminAPI.createWebhook(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "webhooks"] });
    },
  });
}

export function useUpdateWebhook() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: WebhookUpdateRequest }) =>
      adminAPI.updateWebhook(id, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "webhooks"] });
    },
  });
}

export function useDeleteWebhook() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => adminAPI.deleteWebhook(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "webhooks"] });
    },
  });
}

export function useTestWebhook() {
  return useMutation<
    WebhookTestResult,
    Error,
    string
  >({
    mutationFn: (id: string) => adminAPI.testWebhook(id),
  });
}

// --- Analytics ---
//
// useAnalytics is the self-tier hook (mounted at /v1/analytics). The
// pre-fix `org` / `user` widening params were removed; for cross-tenant
// views use useOrgAnalytics(orgId) or useSystemAnalytics() instead.
//
// The optional `params` argument is preserved for source-compat with
// callers that still pass `{ org, user }` — the values are now silently
// ignored by both the client (no params attached to the request URL)
// and the server (the widening primitive is gone). Callers should
// migrate to the tier-specific hook for their visibility intent.
export function useAnalytics(_params?: { org?: string; user?: string }) {
  return useQuery({
    queryKey: ["self", "analytics"],
    queryFn: () => adminAPI.getAnalytics(),
    staleTime: 30_000,
  });
}

// Token cost-rate blob exposed at /v1/usage/cost_rates. Any
// authenticated role can read; writes go through useUpdateSetting,
// which is admin-only at the backend.
export function useCostRates() {
  return useQuery({
    queryKey: ["cost-rates"],
    queryFn: adminAPI.getCostRates,
    staleTime: 60_000,
  });
}

// useUsage is the self-tier hook (mounted at /v1/usage). org/user widening
// params are deprecated and silently ignored. Use useOrgUsage(orgId, ...)
// or useSystemUsage(...) for wider tiers.
export function useUsage(params?: { org?: string; user?: string; project?: string; from?: string; to?: string; group_by?: UsageGroupBy; success_only?: boolean }) {
  // Strip the deprecated org/user keys before passing to the API client
  // so the typing matches and no widening lands in the URL.
  const apiParams = params
    ? {
        project: params.project,
        from: params.from,
        to: params.to,
        group_by: params.group_by,
        success_only: params.success_only,
      }
    : undefined;
  return useQuery({
    queryKey: ["self", "usage", apiParams ?? {}],
    queryFn: () => adminAPI.getUsage(apiParams),
    staleTime: 30_000,
  });
}

// --- Database ---

export function useDatabaseInfo() {
  return useQuery({
    queryKey: ["admin", "database"],
    queryFn: adminAPI.getDatabaseInfo,
  });
}

export function useTestDatabaseConnection() {
  return useMutation({
    mutationFn: (url: string) => adminAPI.testDatabaseConnection(url),
  });
}

export function useTriggerMigration() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (url: string) => adminAPI.triggerMigration(url),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "database"] });
      qc.invalidateQueries({ queryKey: ["admin", "setup-status"] });
    },
  });
}

export function usePreflightDatabase() {
  return useMutation({
    mutationFn: (url: string) => adminAPI.preflightDatabase(url),
  });
}

export function useResetDatabase() {
  return useMutation({
    mutationFn: ({ url, mode }: { url: string; mode: "truncate" | "drop_schema" }) =>
      adminAPI.resetDatabase(url, mode),
  });
}

export function useMigrationAudit() {
  return useMutation({
    mutationFn: () => adminAPI.migrationAudit(),
  });
}

// --- Dreaming ---

// Dynamic intervalMs lets DreamingMonitor drop polling frequency when SSE is
// disconnected (3s/5s) and relax it back to the defaults (10s/15s) when SSE
// is healthy. The detail hook only polls when a cycle is actively running.

export function useDreamingStatus(
  opts: { intervalMs?: number; enabled?: boolean } = {},
) {
  return useQuery({
    queryKey: ["admin", "dreaming"],
    queryFn: adminAPI.getDreamingStatus,
    refetchInterval: opts.intervalMs ?? 10_000,
    enabled: opts.enabled ?? true,
  });
}

// Per-project dream status for the self tier. The /v1/me/dreaming
// endpoint requires a project_id and returns a project-scoped shape
// (DreamProjectStatusResponse) distinct from the system-wide status.
export function useMyDreamingProjectStatus(
  projectId: string | null,
  opts: { intervalMs?: number } = {},
) {
  return useQuery({
    queryKey: ["me", "dreaming", "project", projectId],
    queryFn: () => meAPI.getDreamingProjectStatus(projectId!),
    enabled: !!projectId,
    refetchInterval: opts.intervalMs ?? 10_000,
  });
}

// Self-tier aggregate dreaming status: rolled-up any-of-mine-dirty badge
// plus the count of caller-owned projects.
export function useMyDreamingAggregateStatus(
  opts: { intervalMs?: number; enabled?: boolean } = {},
) {
  return useQuery({
    queryKey: ["me", "dreaming", "aggregate"],
    queryFn: meAPI.getDreamingAggregateStatus,
    enabled: opts.enabled ?? true,
    refetchInterval: opts.intervalMs ?? 10_000,
  });
}

// Org-tier dreaming hooks. Mirror the self/system pair but key the cache
// by orgId so admins viewing different orgs don't collide.
export function useOrgDreamingStatus(
  orgId: string | undefined,
  opts: { intervalMs?: number; enabled?: boolean } = {},
) {
  return useQuery({
    queryKey: ["org", orgId, "dreaming"],
    queryFn: () => orgAPI.getDreamingStatus(orgId!),
    enabled: !!orgId && (opts.enabled ?? true),
    refetchInterval: opts.intervalMs ?? 10_000,
  });
}

// Dream cycles list. tier="system" hits /admin/dreaming/cycles;
// tier="org" hits /orgs/{orgId}/dreaming/cycles; tier="self" hits
// /me/dreaming/cycles. project_id is optional and self/system only.
export function useDreamingCycles(
  projectId?: string,
  opts: {
    intervalMs?: number;
    tier?: "self" | "org" | "system";
    enabled?: boolean;
    orgId?: string;
  } = {},
) {
  const tier = opts.tier ?? "system";
  return useQuery({
    queryKey:
      tier === "org"
        ? ["org", opts.orgId, "dreaming", "cycles"]
        : [tier === "self" ? "me" : "admin", "dreaming", "cycles", projectId],
    queryFn: () => {
      if (tier === "org") return orgAPI.getDreamingCycles(opts.orgId!);
      if (tier === "self") return meAPI.getDreamingCycles(projectId);
      return adminAPI.getDreamingCycles(projectId);
    },
    enabled: (opts.enabled ?? true) && (tier !== "org" || !!opts.orgId),
    refetchInterval: opts.intervalMs ?? 15_000,
  });
}

export function useDreamingCycleDetail(
  cycleId: string | null,
  opts: {
    intervalMs?: number;
    tier?: "self" | "org" | "system";
    orgId?: string;
  } = {},
) {
  const tier = opts.tier ?? "system";
  return useQuery({
    queryKey:
      tier === "org"
        ? ["org", opts.orgId, "dreaming", "cycle", cycleId]
        : [tier === "self" ? "me" : "admin", "dreaming", "cycle", cycleId],
    queryFn: () => {
      if (tier === "org")
        return orgAPI.getDreamingCycleDetail(opts.orgId!, cycleId!);
      if (tier === "self") return meAPI.getDreamingCycleDetail(cycleId!);
      return adminAPI.getDreamingCycleDetail(cycleId!);
    },
    enabled: !!cycleId && (tier !== "org" || !!opts.orgId),
    refetchInterval: opts.intervalMs,
  });
}

export function useSetDreamingEnabled() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (enabled: boolean) => adminAPI.setDreamingEnabled(enabled),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "dreaming"] });
      qc.invalidateQueries({ queryKey: ["admin", "settings"] });
      qc.invalidateQueries({ queryKey: ["me", "dreaming"] });
      qc.invalidateQueries({ queryKey: ["me", "capabilities"] });
    },
  });
}

type TierWithOrg = { tier: "self" | "org" | "system"; orgId?: string };

function invalidateAllDreamingScopes(
  qc: ReturnType<typeof useQueryClient>,
  orgId?: string,
) {
  qc.invalidateQueries({ queryKey: ["admin", "dreaming"] });
  qc.invalidateQueries({ queryKey: ["me", "dreaming"] });
  if (orgId) qc.invalidateQueries({ queryKey: ["org", orgId, "dreaming"] });
}

function invalidateAllEnrichmentScopes(
  qc: ReturnType<typeof useQueryClient>,
  orgId?: string,
) {
  qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
  qc.invalidateQueries({ queryKey: ["me", "enrichment"] });
  if (orgId) qc.invalidateQueries({ queryKey: ["org", orgId, "enrichment"] });
}

export function useRollbackDreamCycle(scope: TierWithOrg = { tier: "system" }) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (cycleId: string) => {
      if (scope.tier === "org") return orgAPI.rollbackDreamCycle(scope.orgId!, cycleId);
      if (scope.tier === "self") return meAPI.rollbackDreamCycle(cycleId);
      return adminAPI.rollbackDreamCycle(cycleId);
    },
    onSuccess: () => invalidateAllDreamingScopes(qc, scope.orgId),
  });
}

export function useAbandonDreamCycle(scope: TierWithOrg = { tier: "system" }) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (cycleId: string) => {
      if (scope.tier === "org") return orgAPI.abandonDreamCycle(scope.orgId!, cycleId);
      if (scope.tier === "self") return meAPI.abandonDreamCycle(cycleId);
      return adminAPI.abandonDreamCycle(cycleId);
    },
    onSuccess: () => invalidateAllDreamingScopes(qc, scope.orgId),
  });
}

// --- Enrichment ---

export function useEnrichmentStatus(
  opts: {
    intervalMs?: number;
    tier?: "self" | "org" | "system";
    orgId?: string;
  } = {},
) {
  const tier = opts.tier ?? "system";
  return useQuery({
    queryKey:
      tier === "org"
        ? ["org", opts.orgId, "enrichment"]
        : [tier === "self" ? "me" : "admin", "enrichment"],
    queryFn: () => {
      if (tier === "org") return orgAPI.getEnrichmentStatus(opts.orgId!);
      if (tier === "self") return meAPI.getEnrichmentStatus();
      return adminAPI.getEnrichmentStatus();
    },
    enabled: tier !== "org" || !!opts.orgId,
    refetchInterval: opts.intervalMs ?? 10_000,
  });
}

export function useRetryEnrichment(scope: TierWithOrg = { tier: "system" }) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (ids?: string[]) => {
      if (scope.tier === "org") return orgAPI.retryEnrichment(scope.orgId!, ids);
      if (scope.tier === "self") return meAPI.retryEnrichment(ids);
      return adminAPI.retryEnrichment(ids);
    },
    onSuccess: () => invalidateAllEnrichmentScopes(qc, scope.orgId),
  });
}

export function usePauseEnrichment() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (paused: boolean) => adminAPI.pauseEnrichment(paused),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
    },
  });
}

export function useTestExtractionPrompt() {
  return useMutation<
    ExtractionTestResult,
    Error,
    { type: "fact" | "entity"; prompt: string; sampleInput: string }
  >({
    mutationFn: ({ type, prompt, sampleInput }) =>
      adminAPI.testExtractionPrompt(type, prompt, sampleInput),
  });
}

// --- Graph ---

export function useGraph(projectId: string) {
  return useQuery({
    queryKey: ["admin", "graph", projectId],
    queryFn: () => adminAPI.getGraph(projectId),
    enabled: !!projectId,
  });
}

// --- Namespaces ---

export function useNamespaceTree() {
  return useQuery({
    queryKey: ["admin", "namespaces"],
    queryFn: adminAPI.getNamespaceTree,
  });
}

// --- Memory Browser ---

export function useMemoryList(projectId: string, params?: MemoryListParams) {
  return useQuery({
    queryKey: ["memories", "list", projectId, params],
    queryFn: () => memoryAPI.list(projectId, params),
    enabled: !!projectId,
  });
}

/**
 * Infinite-scroll variant of the memory list. Pages over parents (or flat
 * memories, depending on the caller's params). Each page is appended to
 * `data.pages`; the consumer flattens with `data.pages.flatMap(p => p.data)`.
 * Requires `pageSize` so the hook can compute the next offset and detect
 * end-of-data deterministically.
 */
export function useMemoryListInfinite(
  projectId: string,
  pageSize: number,
  params?: Omit<MemoryListParams, "limit" | "offset">,
) {
  return useInfiniteQuery({
    queryKey: ["memories", "list-infinite", projectId, pageSize, params],
    enabled: !!projectId,
    initialPageParam: 0,
    queryFn: ({ pageParam }) =>
      memoryAPI.list(projectId, {
        ...params,
        limit: pageSize,
        offset: pageParam as number,
      }),
    getNextPageParam: (lastPage, _allPages, lastPageParam) => {
      const nextOffset = (lastPageParam as number) + pageSize;
      if (nextOffset >= lastPage.pagination.total) return undefined;
      return nextOffset;
    },
  });
}

export function useMemoryRecall(
  projectId: string,
  body: RecallRequest | null,
) {
  return useQuery({
    queryKey: ["memories", "recall", projectId, body],
    queryFn: () => memoryAPI.recall(projectId, body!),
    enabled: !!projectId && !!body,
  });
}

export function useMemoryDetail(projectId: string, memoryId: string) {
  return useQuery({
    queryKey: ["memories", "detail", projectId, memoryId],
    queryFn: () => memoryAPI.get(projectId, memoryId),
    enabled: !!projectId && !!memoryId,
  });
}

export function useUpdateMemory() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      memoryId,
      data,
    }: {
      projectId: string;
      memoryId: string;
      data: MemoryUpdateRequest;
    }) => memoryAPI.update(projectId, memoryId, data),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["memories", "list", vars.projectId] });
      qc.invalidateQueries({
        queryKey: ["memories", "detail", vars.projectId, vars.memoryId],
      });
      qc.invalidateQueries({
        queryKey: ["memories", "recall", vars.projectId],
      });
    },
  });
}

export function useDeleteMemory() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      memoryId,
    }: {
      projectId: string;
      memoryId: string;
    }) => memoryAPI.remove(projectId, memoryId),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["memories", "list", vars.projectId] });
      qc.invalidateQueries({
        queryKey: ["memories", "recall", vars.projectId],
      });
    },
  });
}

export function useForgetMemories() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      body,
    }: {
      projectId: string;
      body: ForgetRequest;
    }) => memoryAPI.forget(projectId, body),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["memories", "list", vars.projectId] });
      qc.invalidateQueries({
        queryKey: ["memories", "recall", vars.projectId],
      });
    },
  });
}

export function useEnrichMemories() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      body,
    }: {
      projectId: string;
      body: EnrichRequest;
    }) => memoryAPI.enrich(projectId, body),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["memories", "list", vars.projectId] });
    },
  });
}

export function useExportMemories(projectId: string, enabled: boolean) {
  return useQuery({
    queryKey: ["memories", "export", projectId],
    queryFn: () => memoryAPI.export(projectId),
    enabled: !!projectId && enabled,
  });
}

// --- OAuth Clients ---

export function useOAuthClients() {
  return useQuery({
    queryKey: ["admin", "oauth-clients"],
    queryFn: adminAPI.listOAuthClients,
  });
}

export function useCreateOAuthClient() {
  const qc = useQueryClient();
  return useMutation<OAuthClientCreated, Error, CreateOAuthClientRequest>({
    mutationFn: (data) => adminAPI.createOAuthClient(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "oauth-clients"] });
    },
  });
}

export function useDeleteOAuthClient() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => adminAPI.deleteOAuthClient(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "oauth-clients"] });
    },
  });
}

// --- IdP Config ---

export function useIdPConfigs() {
  return useQuery({
    queryKey: ["admin", "idp-configs"],
    queryFn: adminAPI.listIdPConfigs,
  });
}

export function useCreateIdPConfig() {
  const qc = useQueryClient();
  return useMutation<IdPConfig, Error, CreateIdPConfigRequest>({
    mutationFn: (data) => adminAPI.createIdPConfig(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "idp-configs"] });
    },
  });
}

export function useUpdateIdPConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: UpdateIdPConfigRequest }) =>
      adminAPI.updateIdPConfig(id, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "idp-configs"] });
    },
  });
}

export function useDeleteIdPConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => adminAPI.deleteIdPConfig(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "idp-configs"] });
    },
  });
}

// --- Me API hooks ---

// Self profile, refetched from /v1/me/profile so MyAccount renders fresh
// server truth instead of relying solely on whatever the JWT was issued with.
export function useMeProfile() {
  return useQuery({
    queryKey: ["me", "profile"],
    queryFn: meAPI.getProfile,
  });
}

export function useMeProjects() {
  return useQuery({
    queryKey: ["me", "projects"],
    queryFn: meAPI.listProjects,
  });
}

export function useCreateMeProject() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: MeCreateProjectRequest) => meAPI.createProject(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["me", "projects"] });
    },
  });
}

export function useMeAPIKeys() {
  return useQuery({
    queryKey: ["me", "api-keys"],
    queryFn: meAPI.listAPIKeys,
  });
}

export function useCreateMeAPIKey() {
  const qc = useQueryClient();
  return useMutation<MeCreateAPIKeyResponse, Error, MeCreateAPIKeyRequest>({
    mutationFn: (data) => meAPI.createAPIKey(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["me", "api-keys"] });
    },
  });
}

export function useRevokeMeAPIKey() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => meAPI.revokeAPIKey(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["me", "api-keys"] });
    },
  });
}

export function useMeOAuthClients() {
  return useQuery({
    queryKey: ["me", "oauth-clients"],
    queryFn: meAPI.listOAuthClients,
  });
}

export function useCreateMeOAuthClient() {
  const qc = useQueryClient();
  return useMutation<OAuthClientCreated, Error, CreateOAuthClientRequest>({
    mutationFn: (data) => meAPI.createOAuthClient(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["me", "oauth-clients"] });
    },
  });
}

export function useChangePassword() {
  return useMutation({
    mutationFn: (data: { currentPassword: string; newPassword: string }) =>
      changePassword(data.currentPassword, data.newPassword),
  });
}

export function useRevokeMeOAuthClient() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => meAPI.revokeOAuthClient(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["me", "oauth-clients"] });
    },
  });
}

// --- Passkey hooks ---

export function useMePasskeys() {
  return useQuery({
    queryKey: ["me", "passkeys"],
    queryFn: meAPI.listPasskeys,
  });
}

export function useRegisterPasskey() {
  const qc = useQueryClient();
  return useMutation<Passkey, Error, { name: string }>({
    mutationFn: async ({ name }) => {
      if (!isWebAuthnAvailable()) {
        throw new Error("WebAuthn is not supported in this browser");
      }

      // Step 1: Begin registration.
      const serverOptions = await meAPI.registerPasskeyBegin({ name });

      // Step 2: Create credential via browser API.
      const options = prepareCreationOptions(serverOptions);
      const credential = (await navigator.credentials.create(
        options,
      )) as PublicKeyCredential | null;
      if (!credential) {
        throw new Error("Passkey registration was cancelled");
      }

      // Step 3: Send attestation to server.
      const serialized = serializeCreationResponse(credential);
      return meAPI.registerPasskeyFinish(serialized, name);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["me", "passkeys"] });
    },
  });
}

export function useDeletePasskey() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => meAPI.deletePasskey(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["me", "passkeys"] });
    },
  });
}

export function usePasskeyLogin() {
  return useMutation<LoginResponse, Error, { email: string }>({
    mutationFn: async ({ email }) => {
      if (!isWebAuthnAvailable()) {
        throw new Error("WebAuthn is not supported in this browser");
      }

      // Step 1: Begin login.
      const beginResult = await authAPI.passkeyBegin({ email });

      // Step 2: Get assertion via browser API.
      const options = prepareRequestOptions(beginResult);
      const credential = (await navigator.credentials.get(
        options,
      )) as PublicKeyCredential | null;
      if (!credential) {
        throw new Error("Passkey authentication was cancelled");
      }

      // Step 3: Send assertion to server.
      const serialized = serializeAssertionResponse(credential);
      return authAPI.passkeyFinish(serialized, beginResult.session_key);
    },
  });
}

// --- Org API hooks ---

export function useOrgUsers(orgId: string) {
  return useQuery({
    queryKey: ["org", orgId, "users"],
    queryFn: () => orgAPI.listUsers(orgId),
    enabled: !!orgId,
  });
}

export function useOrgUser(orgId: string, userId: string) {
  return useQuery({
    queryKey: ["org", orgId, "users", userId],
    queryFn: () => orgAPI.getUser(orgId, userId),
    enabled: !!orgId && !!userId,
  });
}

export function useCreateOrgUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ orgId, data }: { orgId: string; data: OrgCreateUserRequest }) =>
      orgAPI.createUser(orgId, data),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["org", vars.orgId, "users"] });
    },
  });
}

export function useUpdateOrgUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      orgId,
      userId,
      data,
    }: {
      orgId: string;
      userId: string;
      data: OrgUpdateUserRequest;
    }) => orgAPI.updateUser(orgId, userId, data),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["org", vars.orgId, "users"] });
      qc.invalidateQueries({ queryKey: ["org", vars.orgId, "users", vars.userId] });
    },
  });
}

export function useDeleteOrgUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ orgId, userId }: { orgId: string; userId: string }) =>
      orgAPI.deleteUser(orgId, userId),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["org", vars.orgId, "users"] });
    },
  });
}

export function useOrgUserAPIKeys(orgId: string, userId: string) {
  return useQuery({
    queryKey: ["org", orgId, "users", userId, "api-keys"],
    queryFn: () => orgAPI.listUserAPIKeys(orgId, userId),
    enabled: !!orgId && !!userId,
  });
}

export function useGenerateOrgUserAPIKey() {
  const qc = useQueryClient();
  return useMutation<
    GenerateAPIKeyResponse,
    Error,
    { orgId: string; userId: string; data: GenerateAPIKeyRequest }
  >({
    mutationFn: ({ orgId, userId, data }) =>
      orgAPI.generateUserAPIKey(orgId, userId, data),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({
        queryKey: ["org", vars.orgId, "users", vars.userId, "api-keys"],
      });
    },
  });
}

export function useRevokeOrgUserAPIKey() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      orgId,
      userId,
      keyId,
    }: {
      orgId: string;
      userId: string;
      keyId: string;
    }) => orgAPI.revokeUserAPIKey(orgId, userId, keyId),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({
        queryKey: ["org", vars.orgId, "users", vars.userId, "api-keys"],
      });
    },
  });
}

// useOrgAnalytics + useOrgUsage are defined above in the tier-B hooks
// section (added 2026-04-30 leak fix); they replace the earlier
// declarations that lived here.

export function useOrgIdPConfigs(orgId: string) {
  return useQuery({
    queryKey: ["org-idp", orgId],
    queryFn: () => orgAPI.listOrgIdPs(orgId),
    enabled: !!orgId,
  });
}

export function useCreateOrgIdPConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: { orgId: string } & CreateIdPConfigRequest) =>
      orgAPI.configureIdP(data.orgId, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["org-idp"] });
    },
  });
}

export function useUpdateOrgIdPConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: { orgId: string; id: string; data: UpdateIdPConfigRequest }) =>
      orgAPI.updateOrgIdP(params.orgId, params.id, params.data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["org-idp"] });
    },
  });
}

export function useDeleteOrgIdPConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (params: { orgId: string; id: string }) =>
      orgAPI.deleteOrgIdP(params.orgId, params.id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["org-idp"] });
    },
  });
}
