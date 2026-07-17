import {
  useQuery,
  useInfiniteQuery,
  useMutation,
  useQueryClient,
  keepPreviousData,
} from "@tanstack/react-query";
import { useMemo } from "react";
import { useAuth } from "../context/AuthContext";
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
  type OnboardingProgressRequest,
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
  type ImportFormat,
  type MemoryListParams,
  type RecallRequest,
  type AskRequest,
  type AskResponse,
  type MemoryUpdateRequest,
  type ForgetRequest,
  type EnrichRequest,
  type UpdateProviderSlotRequest,
  type TestProviderResult,
  type ExtractionTestResult,
  type AugmentationBackfillResponse,
  type MultiVectorBackfillResponse,
  type MissingEmbeddingsBackfillResponse,
  type ConsolidationEntitiesBackfillResponse,
  type DeletedCountResponse,
  type RelabelGraphResponse,
  type ReExtractResponse,
  type VectorMigrationResult,
  type VectorMigrationDirection,
  type MigrationStartAck,
  type GraphHealth,
  type GraphRepairResult,
  type MemoryAugmentPreviewResponse,
  type OAuthClientCreated,
  type CreateOAuthClientRequest,
  type IdPConfig,
  type CreateIdPConfigRequest,
  type UpdateIdPConfigRequest,
  type WebhookTestResult,
  type MeCreateProjectRequest,
  type MeSettingDefault,
  type CreateProceduralRequest,
  type UpdateProceduralRequest,
  type ProceduralExportData,
  type ProceduralExportEntry,
  type MeCreateAPIKeyRequest,
  type MeCreateAPIKeyResponse,
  type OrgCreateUserRequest,
  type OrgUpdateUserRequest,
  type Passkey,
  type LoginResponse,
  type SystemRankingWeights,
  type UsageGroupBy,
  type ExportJob,
  type CreateExportJobRequest,
  type EnrichmentQueueCounts,
  type EnrichmentSortField,
  type EnrichmentStatusFilter,
  type LogListParams,
} from "../api/client";
import {
  sharesAPI,
  type ShareGrantInput,
  type CreateShareRequest,
  type ShareCreatedResponse,
} from "../api/shares";
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
    // Status only flips on setup completion and onboarding progress, both of
    // which invalidate this key explicitly (useCompleteSetup, useUpdateOnboarding).
    // A stale window avoids refetching on every window-focus and remount; the
    // SetupGuard and the AppLayout resume guard both read this shared query.
    staleTime: 5 * 60 * 1000,
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

// useUpdateOnboarding persists the guided wizard's step cursor and/or marks
// onboarding complete. Invalidates setup-status so the route guard re-evaluates.
export function useUpdateOnboarding() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: OnboardingProgressRequest) =>
      adminAPI.updateOnboarding(data),
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
// only: no row-level user/memory data, no content fields.

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

export function useImportMemories() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      format,
      data,
    }: {
      projectId: string;
      format: ImportFormat;
      data: unknown;
    }) => memoryAPI.import(projectId, format, data),
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
    onSuccess: (_data, id) => {
      qc.invalidateQueries({ queryKey: ["admin", "projects"] });
      qc.invalidateQueries({ queryKey: ["me", "projects"] });
      qc.invalidateQueries({ queryKey: ["me", "projects", id] });
    },
  });
}

// --- Procedural memory ---

const PROCEDURAL_KEY = ["self", "procedural"];

export function useProcedural() {
  return useQuery({
    queryKey: PROCEDURAL_KEY,
    queryFn: () => meAPI.listProcedural(),
  });
}

export function useCreateProcedural() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: CreateProceduralRequest) => meAPI.createProcedural(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: PROCEDURAL_KEY });
    },
  });
}

export function useUpdateProcedural() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: UpdateProceduralRequest }) =>
      meAPI.updateProcedural(id, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: PROCEDURAL_KEY });
    },
  });
}

export function useDeleteProcedural() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => meAPI.deleteProcedural(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: PROCEDURAL_KEY });
    },
  });
}

export function useImportProcedural() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: ProceduralExportData | ProceduralExportEntry[]) =>
      meAPI.importProcedural(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: PROCEDURAL_KEY });
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

export function useOllamaModels(ollamaUrl?: string, customHeaders?: Record<string, string>) {
  return useQuery({
    queryKey: ["admin", "ollama-models", ollamaUrl, customHeaders],
    queryFn: () => adminAPI.getOllamaModels(ollamaUrl, customHeaders),
    enabled: false,
  });
}

export function useProviderModels(url: string, customHeaders?: Record<string, string>) {
  return useQuery({
    queryKey: ["admin", "provider-models", url, customHeaders],
    queryFn: () => adminAPI.getProviderModels(url, customHeaders),
    enabled: false,
  });
}

export function usePullOllamaModel() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      model,
      ollamaUrl,
      customHeaders,
    }: {
      model: string;
      ollamaUrl?: string;
      customHeaders?: Record<string, string>;
    }) => adminAPI.pullOllamaModel(model, ollamaUrl, customHeaders),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "ollama-models"] });
    },
  });
}

// --- Settings ---

// useSettings and useSettingsSchema hit /admin/settings*, which the server
// gates on RoleAdministrator. Non-admin callers (the per-project edit panel
// uses useSchemaRange under the hood) would otherwise rack up 403s in the
// browser console for every page render. Gate the underlying React Query
// fetch on isAdmin so the request never goes out for non-admin users; the
// downstream useSchemaRange consumer already falls back to its caller-
// supplied {min, max, step} default when no data is present. The admin
// Settings, Provider Configuration, and Prompt Templates pages are admin-
// gated UI surfaces; the gating here matches the server-side authorization.
export function useSettings() {
  const { isAdmin } = useAuth();
  return useQuery({
    queryKey: ["admin", "settings"],
    queryFn: () => adminAPI.getSettings(),
    enabled: isAdmin,
  });
}

export function useSettingsSchema() {
  const { isAdmin } = useAuth();
  return useQuery({
    queryKey: ["admin", "settings-schema"],
    queryFn: adminAPI.getSettingsSchema,
    enabled: isAdmin,
  });
}

export function useSettingGroups() {
  const { isAdmin } = useAuth();
  return useQuery({
    queryKey: ["admin", "settings-groups"],
    queryFn: adminAPI.getSettingGroups,
    enabled: isAdmin,
  });
}

// useSystemRankingWeights resolves the ranking.weight.* settings into a
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
  origin: "ranking.weight.origin",
  mmr_lambda: "ranking.weight.mmr_lambda",
} as const;

// coerceFiniteNumber accepts a number or numeric string and returns a finite
// number, or null when the value is neither. Shared by the ranking-weight
// resolver and the admin settings numeric controls.
export function coerceFiniteNumber(raw: unknown): number | null {
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
  // rather than silently substituting a stale built-in fallback. Stays
  // empty while isLoading is true; the "missing" verdict only stands once
  // the schema query has resolved.
  missingKeys: string[];
  // isLoading is true only while the SCHEMA query is still in flight.
  // The settings (operator overrides) query is best-effort; a slow or
  // failed settings load does not block rendering because the resolver
  // can fall back to schema defaults per-key. Pivoting on schema alone
  // prevents the regression where a transient settings 5xx hides the
  // editor even when defaults are reachable.
  isLoading: boolean;
  // isError is true only when the SCHEMA query failed. Without schema
  // defaults the editor cannot render any meaningful baseline; settings
  // query failure is silently tolerated because schema defaults are
  // sufficient to drive the form.
  isError: boolean;
}

// resolveSystemRankingWeights is the pure resolution logic. Exposed for
// unit testing without spinning up React Query infrastructure. Returns
// null weights when any key is missing; the schema endpoint is the
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
    const fromSetting = coerceFiniteNumber(settingByKey.get(key));
    if (fromSetting !== null) {
      resolved[field] = fromSetting;
      continue;
    }
    const fromSchema = coerceFiniteNumber(defaultByKey.get(key));
    if (fromSchema !== null) {
      resolved[field] = fromSchema;
      continue;
    }
    missing.push(key);
  }
  if (missing.length > 0) {
    return { weights: null, missingKeys: missing, isLoading: false, isError: false };
  }
  return { weights: resolved, missingKeys: [], isLoading: false, isError: false };
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

// useSystemRankingWeights returns the effective system ranking weights to
// any authenticated caller via the self-tier /v1/me/ranking-weights/defaults
// endpoint. Each response entry carries both `value` (operator override at
// scope=global, falling back to the schema default) and `default_value` (the
// registry default) so the precedence used by the legacy two-query resolver
// is preserved. The endpoint is gated only on authentication, so non-admin
// project owners (org_owner, member) can populate their per-project Ranking
// Weights editor placeholders without 403-ing against /admin/settings.
//
// While the query is in flight isLoading is true and missingKeys is empty so
// the consumer can render a loading state instead of the deploy-incident
// banner. Consumers MUST handle the null case (typically a red banner)
// instead of silently rendering stale defaults; that is the "AC6 contract"
// the contract test enforces.
export function useSystemRankingWeights(): SystemRankingWeightsResolution {
  const query = useQuery({
    queryKey: ["me", "ranking-weight-defaults"],
    queryFn: meAPI.getRankingWeightDefaults,
  });
  // useMemo gives the consumer (ProjectManagement edit panel) a stable
  // reference between unrelated re-renders; without it every keystroke
  // in any form input rebuilds the resolution Maps.
  return useMemo(() => {
    if (query.isPending) {
      // Defer the missing-keys verdict; the response array is just not in
      // memory yet, not actually missing keys.
      return { weights: null, missingKeys: [], isLoading: true, isError: false };
    }
    if (query.isError) {
      // Endpoint failure is its own diagnostic. Suppress the missing-keys
      // list so the consumer renders a generic banner keyed on isError
      // rather than a misleading list of every required key as if the
      // server explicitly omitted them.
      return { weights: null, missingKeys: [], isLoading: false, isError: true };
    }
    // Reshape the unified payload back into the two-source contract the
    // pure resolver expects. value is the operator-effective number;
    // default_value is the registered default. Both feed in so the existing
    // override-then-default precedence and the eight-key invariant stay
    // exercised by the same unit tests.
    const entries = query.data?.data ?? [];
    const configured = entries.map((e) => ({ key: e.key, value: e.value }));
    const schema = entries.map((e) => ({
      key: e.key,
      default_value: e.default_value,
    }));
    const resolution = resolveSystemRankingWeights(configured, schema);
    return { ...resolution, isLoading: false, isError: false };
  }, [query.data, query.isPending, query.isError]);
}

// SettingDefaultsResolution is the result of useSettingDefaults. `byKey` maps
// each allow-listed setting key to its effective default row; consumers read
// `byKey["graph.link_distance"].value` etc. `isLoading` is true while the
// query is in flight (consumers should defer applying values until then so a
// missing entry is not mistaken for "no default"); `isError` is true on a
// failed fetch.
export interface SettingDefaultsResolution {
  byKey: Record<string, MeSettingDefault>;
  isLoading: boolean;
  isError: boolean;
}

// useSettingDefaults returns the operator-effective defaults for the
// allow-listed numeric settings (graph layout keys + dedup threshold) to any
// authenticated caller via the self-tier /v1/me/setting-defaults endpoint.
// General-user pages (GraphVisualization, the per-project / per-user override
// editors) read from here instead of /admin/settings so non-admin owners can
// render against the real operator defaults without 403-ing.
export function useSettingDefaults(): SettingDefaultsResolution {
  const query = useQuery({
    queryKey: ["me", "setting-defaults"],
    queryFn: meAPI.getSettingDefaults,
  });
  return useMemo(() => {
    const byKey: Record<string, MeSettingDefault> = {};
    for (const row of query.data?.data ?? []) byKey[row.key] = row;
    return {
      byKey,
      isLoading: query.isPending,
      isError: query.isError,
    };
  }, [query.data, query.isPending, query.isError]);
}

// formatSystemDefaultPlaceholder renders the operator-effective default as the
// placeholder for an override input ("system: 0.92"), or a neutral label while
// the value is still loading. Shared by the per-project and per-user override
// editors so the formatting lives in one place.
export function formatSystemDefaultPlaceholder(
  value: number | undefined,
  decimals = 2,
): string {
  return value !== undefined ? `system: ${value.toFixed(decimals)}` : "system default";
}

export function useUpdateSetting() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ key, value }: { key: string; value: unknown }) =>
      adminAPI.updateSetting(key, value),
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

// useResetSettings reverts one setting (when `key` is supplied) or every
// registered setting (when omitted) to the registered default. Mirrors
// useUpdateSetting's cache invalidation so the SettingsEditor, Dreaming admin
// panel, and cost-rates consumers all re-render against the post-reset state
// on success.
export function useResetSettings() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { key?: string } = {}) =>
      adminAPI.resetSettings(body),
    onSuccess: () => {
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
// callers that still pass `{ org, user }`; the values are now silently
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

// ENRICHMENT_PAGE_SIZE matches the server-side default limit. The queue list
// is paged through this many items per "Load more".
export const ENRICHMENT_PAGE_SIZE = 50;

// ENRICHMENT_MAX_PAGES caps how many pages the infinite query retains, so a deep
// scroll cannot make the poll refetch an unbounded number of offset queries each
// tick. 20 pages * 50 = 1000 rows kept in view; the per-status counts still
// report the true totals on the stat cards and filter pills.
export const ENRICHMENT_MAX_PAGES = 20;

// enrichmentTotalForFilter derives the total row count for the active status
// filter from the per-status counts the server always returns. With no filter
// it sums every status; with one selected it reads that bucket. This is what
// getNextPageParam uses to decide whether another page exists, so no extra
// total field is needed on the response.
export function enrichmentTotalForFilter(
  counts: EnrichmentQueueCounts,
  status?: EnrichmentStatusFilter,
): number {
  if (status) return counts[status] ?? 0;
  return counts.pending + counts.processing + counts.completed + counts.failed;
}

// useEnrichmentStatusInfinite pages the enrichment queue server-side. Sort,
// direction, and status filter are part of the query key, so changing any of
// them refetches from the first page. placeholderData keeps the previously
// loaded pages visible during background refetches (poll interval) so the
// table does not blank or reshuffle while the worker drains jobs.
export function useEnrichmentStatusInfinite(opts: {
  intervalMs?: number;
  tier?: "self" | "org" | "system";
  orgId?: string;
  sort?: EnrichmentSortField;
  dir?: "asc" | "desc";
  status?: EnrichmentStatusFilter;
  pageSize?: number;
}) {
  const tier = opts.tier ?? "system";
  const pageSize = opts.pageSize ?? ENRICHMENT_PAGE_SIZE;
  const sort = opts.sort ?? "created_at";
  const dir = opts.dir ?? "desc";
  const status = opts.status;
  const keyScope = { sort, dir, status: status ?? null, pageSize };
  return useInfiniteQuery({
    queryKey:
      tier === "org"
        ? ["org", opts.orgId, "enrichment", keyScope]
        : [tier === "self" ? "me" : "admin", "enrichment", keyScope],
    enabled: tier !== "org" || !!opts.orgId,
    initialPageParam: 0,
    queryFn: ({ pageParam }) => {
      const params = {
        limit: pageSize,
        offset: pageParam as number,
        sort,
        dir,
        status,
      };
      if (tier === "org")
        return orgAPI.getEnrichmentStatus(opts.orgId!, params);
      if (tier === "self") return meAPI.getEnrichmentStatus(params);
      return adminAPI.getEnrichmentStatus(params);
    },
    getNextPageParam: (lastPage, _allPages, lastPageParam) => {
      const next = (lastPageParam as number) + pageSize;
      const total = enrichmentTotalForFilter(lastPage.counts, status);
      return next >= total ? undefined : next;
    },
    // Bound how many pages are retained so a deep scroll cannot make the poll
    // re-issue an unbounded number of offset queries every tick. When the cap
    // is exceeded, fetching the next page drops the oldest one (the queue is a
    // monitor; nobody eyeballs tens of thousands of rows). ENRICHMENT_MAX_PAGES
    // * pageSize is the in-view ceiling; counts still report the true total.
    maxPages: ENRICHMENT_MAX_PAGES,
    refetchInterval: opts.intervalMs ?? 10_000,
    placeholderData: keepPreviousData,
  });
}

// LOGS_PAGE_SIZE is how many diagnostic log entries the Logs page pulls per
// "Load more" / infinite-scroll fetch.
export const LOGS_PAGE_SIZE = 100;

// useLogsInfinite pages the diagnostic log store server-side. The filter set is
// part of the query key, so changing any filter refetches from the first page.
// placeholderData keeps prior pages visible during the poll refetch so the list
// does not blank while new logs arrive.
export function useLogsInfinite(opts: {
  filter?: Omit<LogListParams, "limit" | "offset">;
  pageSize?: number;
  intervalMs?: number;
}) {
  const pageSize = opts.pageSize ?? LOGS_PAGE_SIZE;
  const filter = opts.filter ?? {};
  return useInfiniteQuery({
    queryKey: ["admin", "logs", { filter, pageSize }],
    initialPageParam: 0,
    queryFn: ({ pageParam }) =>
      adminAPI.listLogs({ ...filter, limit: pageSize, offset: pageParam as number }),
    getNextPageParam: (lastPage, _allPages, lastPageParam) => {
      const next = (lastPageParam as number) + pageSize;
      return next >= lastPage.pagination.total ? undefined : next;
    },
    refetchInterval: opts.intervalMs ?? 15_000,
    placeholderData: keepPreviousData,
  });
}

// useLogFacets loads the filter dropdown values (levels + distinct components).
export function useLogFacets() {
  return useQuery({
    queryKey: ["admin", "logs", "facets"],
    queryFn: () => adminAPI.getLogFacets(),
    staleTime: 60_000,
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

// useClearFailedEnrichment deletes failed enrichment jobs in the active tier's
// scope (self/org/system, mirroring useRetryEnrichment). The mutation argument
// is olderThanDays; 0 (the default) clears all failed rows in scope.
export function useClearFailedEnrichment(scope: TierWithOrg = { tier: "system" }) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (olderThanDays?: number) => {
      const req = { older_than_days: olderThanDays ?? 0 };
      if (scope.tier === "org") return orgAPI.clearFailedJobs(scope.orgId!, req);
      if (scope.tier === "self") return meAPI.clearFailedJobs(req);
      return adminAPI.clearFailedJobs(req);
    },
    onSuccess: () => invalidateAllEnrichmentScopes(qc, scope.orgId),
  });
}

// useReExtractMemories re-extracts an explicit set of memories (the queue's
// per-row "Re-extract" action). Unlike retry, this tombstones each memory's
// prior graph footprint and clears its enriched flag so extraction actually
// re-runs (a plain retry on an already-enriched memory is skipped by the
// worker). Takes memory IDs, not queue job IDs.
export function useReExtractMemories(scope: TierWithOrg = { tier: "system" }) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (memoryIds: string[]) => {
      if (scope.tier === "org")
        return orgAPI.reExtractMemories(scope.orgId!, memoryIds);
      if (scope.tier === "self") return meAPI.reExtractMemories(memoryIds);
      return adminAPI.reExtractMemories(memoryIds);
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
    {
      type: "fact" | "entity" | "relationship" | "augment" | "ingestion";
      sampleInput: string;
      count?: number;
      systemPrompt?: string;
    }
  >({
    mutationFn: ({ type, sampleInput, count, systemPrompt }) =>
      adminAPI.testExtractionPrompt(type, sampleInput, count, systemPrompt),
  });
}

export function useGraphHealth() {
  return useQuery<GraphHealth, Error>({
    queryKey: ["admin", "graph", "health"],
    queryFn: () => adminAPI.getGraphHealth(),
  });
}

export function useRepairGraph() {
  const qc = useQueryClient();
  return useMutation<GraphRepairResult, Error, void>({
    mutationFn: () => adminAPI.repairGraph(),
    onSuccess: () => {
      // Refresh the health count after a repair so the block reflects the
      // reaped backlog.
      qc.invalidateQueries({ queryKey: ["admin", "graph", "health"] });
    },
  });
}

export function useBackfillAugmentation() {
  const qc = useQueryClient();
  return useMutation<
    AugmentationBackfillResponse,
    Error,
    { project_id?: string; dry_run?: boolean; limit?: number }
  >({
    mutationFn: (req) => adminAPI.backfillAugmentation(req),
    onSuccess: (_, variables) => {
      // Only invalidate the queue view when we actually enqueued; a dry-run
      // does not change queue state and forcing a refetch would just be
      // wasted bandwidth.
      if (!variables.dry_run) {
        qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
      }
    },
  });
}

export function useBackfillMultiVector() {
  const qc = useQueryClient();
  return useMutation<
    MultiVectorBackfillResponse,
    Error,
    { project_id?: string; dry_run?: boolean; limit?: number }
  >({
    mutationFn: (req) => adminAPI.backfillMultiVector(req),
    onSuccess: (_, variables) => {
      if (!variables.dry_run) {
        qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
      }
    },
  });
}

export function useBackfillMissingEmbeddings() {
  const qc = useQueryClient();
  return useMutation<
    MissingEmbeddingsBackfillResponse,
    Error,
    { project_id?: string; dry_run?: boolean; limit?: number }
  >({
    mutationFn: (req) => adminAPI.backfillMissingEmbeddings(req),
    onSuccess: (_, variables) => {
      if (!variables.dry_run) {
        qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
      }
    },
  });
}

export function useBackfillConsolidationEntities() {
  const qc = useQueryClient();
  return useMutation<
    ConsolidationEntitiesBackfillResponse,
    Error,
    { project_id?: string; dry_run?: boolean; limit?: number }
  >({
    mutationFn: (req) => adminAPI.backfillConsolidationEntities(req),
    onSuccess: (_, variables) => {
      if (!variables.dry_run) {
        qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
      }
    },
  });
}

export function useClearCompletedJobs() {
  const qc = useQueryClient();
  return useMutation<
    DeletedCountResponse,
    Error,
    { older_than_days?: number }
  >({
    mutationFn: (req) => adminAPI.clearCompletedJobs(req),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
    },
  });
}

export function useRelabelGraph() {
  const qc = useQueryClient();
  return useMutation<RelabelGraphResponse, Error, { dry_run?: boolean }>({
    mutationFn: (req) => adminAPI.relabelGraph(req),
    onSuccess: (_, variables) => {
      if (!variables.dry_run) {
        qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
      }
    },
  });
}

export function useBackfillEmbeddingDims() {
  const qc = useQueryClient();
  return useMutation<{ updated: number }, Error, void>({
    mutationFn: () => adminAPI.backfillEmbeddingDims(),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
    },
  });
}

export function useReExtract() {
  const qc = useQueryClient();
  return useMutation<
    ReExtractResponse,
    Error,
    { project_id?: string; dry_run?: boolean; limit?: number }
  >({
    mutationFn: (req) => adminAPI.reExtract(req),
    onSuccess: (_, variables) => {
      if (!variables.dry_run) {
        qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
      }
    },
  });
}

export function useVectorMigrationDryRun() {
  return useMutation<
    VectorMigrationResult,
    Error,
    { direction: VectorMigrationDirection }
  >({
    mutationFn: ({ direction }) => adminAPI.vectorMigrationDryRun(direction),
  });
}

export function useStartVectorMigration() {
  return useMutation<
    MigrationStartAck,
    Error,
    { direction: VectorMigrationDirection; batch_size?: number }
  >({
    mutationFn: ({ direction, batch_size }) =>
      adminAPI.startVectorMigration(direction, batch_size),
  });
}

export function usePreviewMemoryAugmentation() {
  return useMutation<
    MemoryAugmentPreviewResponse,
    Error,
    { projectId: string; memoryId: string }
  >({
    mutationFn: ({ projectId, memoryId }) =>
      adminAPI.previewMemoryAugmentation(projectId, memoryId),
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

// useAsk drives the ask synthesis tool. It is a mutation, not a query, because
// each call spends an LLM token budget: the user explicitly submits a question
// rather than the answer recomputing on every keystroke. An empty project
// scopes wide (all of the caller's projects); a project slug narrows.
export function useAsk() {
  return useMutation<AskResponse, Error, AskRequest>({
    mutationFn: (body: AskRequest) => meAPI.ask(body),
  });
}

export function useMemoryDetail(
  projectId: string,
  memoryId: string,
  opts?: { includeSuperseded?: boolean },
) {
  const includeSuperseded = opts?.includeSuperseded ?? false;
  return useQuery({
    queryKey: [
      "memories",
      "detail",
      projectId,
      memoryId,
      includeSuperseded,
    ],
    queryFn: () => memoryAPI.get(projectId, memoryId, { includeSuperseded }),
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
      qc.invalidateQueries({
        queryKey: ["memories", "list-infinite", vars.projectId],
      });
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
      qc.invalidateQueries({
        queryKey: ["memories", "list-infinite", vars.projectId],
      });
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
      qc.invalidateQueries({
        queryKey: ["memories", "list-infinite", vars.projectId],
      });
      qc.invalidateQueries({
        queryKey: ["memories", "recall", vars.projectId],
      });
    },
  });
}

// invalidateMoveQueries refreshes the lists of BOTH the source and destination
// projects after a move, plus the project list (memory counts changed).
function invalidateMoveQueries(
  qc: ReturnType<typeof useQueryClient>,
  sourceProjectId: string,
  targetProjectId: string,
) {
  for (const pid of [sourceProjectId, targetProjectId]) {
    qc.invalidateQueries({ queryKey: ["memories", "list-infinite", pid] });
    qc.invalidateQueries({ queryKey: ["memories", "recall", pid] });
  }
  qc.invalidateQueries({ queryKey: ["me", "projects"] });
}

export function useMoveMemory() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      memoryId,
      targetProjectId,
    }: {
      projectId: string;
      memoryId: string;
      targetProjectId: string;
    }) => memoryAPI.move(projectId, memoryId, targetProjectId),
    onSuccess: (_data, vars) =>
      invalidateMoveQueries(qc, vars.projectId, vars.targetProjectId),
  });
}

export function useBulkMoveMemories() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      ids,
      targetProjectId,
    }: {
      projectId: string;
      ids: string[];
      targetProjectId: string;
    }) => memoryAPI.bulkMove(projectId, ids, targetProjectId),
    onSuccess: (_data, vars) =>
      invalidateMoveQueries(qc, vars.projectId, vars.targetProjectId),
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
      qc.invalidateQueries({
        queryKey: ["memories", "list-infinite", vars.projectId],
      });
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

// --- Share-token hooks ---

export function useMeShares() {
  return useQuery({
    queryKey: ["me", "shares"],
    queryFn: sharesAPI.list,
  });
}

export function useMeShareDetail(id: string | undefined) {
  return useQuery({
    queryKey: ["me", "shares", id],
    queryFn: () => sharesAPI.get(id ?? ""),
    enabled: Boolean(id),
  });
}

export function useCreateMeShare() {
  const qc = useQueryClient();
  return useMutation<ShareCreatedResponse, Error, CreateShareRequest>({
    mutationFn: (data) => sharesAPI.create(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["me", "shares"] });
    },
  });
}

export function useUpdateMeShareGrants() {
  const qc = useQueryClient();
  return useMutation<void, Error, { id: string; grants: ShareGrantInput[] }>({
    mutationFn: ({ id, grants }) => sharesAPI.updateGrants(id, grants),
    onSuccess: (_, vars) => {
      qc.invalidateQueries({ queryKey: ["me", "shares"] });
      qc.invalidateQueries({ queryKey: ["me", "shares", vars.id] });
    },
  });
}

export function useRevokeMeShare() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => sharesAPI.revoke(id),
    onSuccess: (_data, id) => {
      qc.invalidateQueries({ queryKey: ["me", "shares"] });
      qc.invalidateQueries({ queryKey: ["me", "shares", id] });
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

// --- Export job hooks ---
//
// The MyAccount page polls in-flight jobs at 3s while at least one row is
// in pending/processing; React Query's refetchInterval callback returns a
// number when polling should continue and false when terminal, mirroring
// the cadence used by EnrichmentMonitor.

export function useMeExportJobs() {
  return useQuery({
    queryKey: ["me", "exports"],
    queryFn: meAPI.listExportJobs,
    refetchInterval: (query) => {
      const jobs = query.state.data as ExportJob[] | undefined;
      if (!jobs) return false;
      const hasInflight = jobs.some(
        (j) => j.status === "pending" || j.status === "processing",
      );
      return hasInflight ? 3000 : false;
    },
  });
}

export function useCreateMeExportJob() {
  const qc = useQueryClient();
  return useMutation<ExportJob, Error, CreateExportJobRequest>({
    mutationFn: (data) => meAPI.createExportJob(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["me", "exports"] });
    },
  });
}

export function useDeleteMeExportJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => meAPI.deleteExportJob(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["me", "exports"] });
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
