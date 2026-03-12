import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  adminAPI,
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
  type WebhookTestResult,
} from "../api/client";

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

// --- Dashboard ---

export function useDashboard() {
  return useQuery({
    queryKey: ["admin", "dashboard"],
    queryFn: adminAPI.getDashboard,
    refetchInterval: 30_000,
  });
}

// --- Activity ---

export function useActivity(limit = 50) {
  return useQuery({
    queryKey: ["admin", "activity", limit],
    queryFn: () => adminAPI.getActivity(limit),
    refetchInterval: 30_000,
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

export function useOrgs() {
  return useQuery({
    queryKey: ["admin", "orgs"],
    queryFn: adminAPI.listOrgs,
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

export function useUsers() {
  return useQuery({
    queryKey: ["admin", "users"],
    queryFn: adminAPI.listUsers,
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
    queryKey: ["admin", "projects", id],
    queryFn: () => adminAPI.getProject(id),
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
      adminAPI.updateProject(id, data),
    onSuccess: (_data, vars) => {
      qc.invalidateQueries({ queryKey: ["admin", "projects"] });
      qc.invalidateQueries({ queryKey: ["admin", "projects", vars.id] });
    },
  });
}

export function useDeleteProject() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => adminAPI.deleteProject(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "projects"] });
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
    },
  });
}

export function useTestProviderSlot() {
  return useMutation<TestProviderResult, Error, { slot: string; config: UpdateProviderSlotRequest }>({
    mutationFn: ({ slot, config }) => adminAPI.testProviderSlot(slot, config),
  });
}

export function useOllamaModels() {
  return useQuery({
    queryKey: ["admin", "ollama-models"],
    queryFn: adminAPI.getOllamaModels,
    enabled: false,
  });
}

export function usePullOllamaModel() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (model: string) => adminAPI.pullOllamaModel(model),
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

export function useUpdateSetting() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ key, value, scope }: { key: string; value: unknown; scope: string }) =>
      adminAPI.updateSetting(key, value, scope),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "settings"] });
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

export function useAnalytics() {
  return useQuery({
    queryKey: ["admin", "analytics"],
    queryFn: adminAPI.getAnalytics,
  });
}

export function useUsage() {
  return useQuery({
    queryKey: ["admin", "usage"],
    queryFn: () => adminAPI.getUsage(),
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

// --- Enrichment ---

export function useEnrichmentStatus() {
  return useQuery({
    queryKey: ["admin", "enrichment"],
    queryFn: adminAPI.getEnrichmentStatus,
    refetchInterval: 10_000,
  });
}

export function useRetryEnrichment() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (ids?: string[]) => adminAPI.retryEnrichment(ids),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "enrichment"] });
    },
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

export function useDeleteIdPConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => adminAPI.deleteIdPConfig(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "idp-configs"] });
    },
  });
}
