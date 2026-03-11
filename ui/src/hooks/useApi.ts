import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  adminAPI,
  memoryAPI,
  healthAPI,
  type SetupRequest,
  type SetupResponse,
  type Organization,
  type User,
  type Project,
  type Provider,
  type Setting,
  type Webhook,
  type StoreMemoryRequest,
  type MemoryListParams,
  type RecallRequest,
  type MemoryUpdateRequest,
  type ForgetRequest,
  type EnrichRequest,
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
    mutationFn: (data: Partial<Organization>) => adminAPI.createOrg(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "orgs"] });
    },
  });
}

export function useUpdateOrg() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<Organization> }) =>
      adminAPI.updateOrg(id, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "orgs"] });
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
    mutationFn: (data: Partial<User> & { password?: string }) =>
      adminAPI.createUser(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "users"] });
    },
  });
}

export function useUpdateUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<User> }) =>
      adminAPI.updateUser(id, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "users"] });
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
    mutationFn: (data: Partial<Project>) => adminAPI.createProject(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "projects"] });
    },
  });
}

export function useUpdateProject() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<Project> }) =>
      adminAPI.updateProject(id, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "projects"] });
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

// --- Providers ---

export function useProviders() {
  return useQuery({
    queryKey: ["admin", "providers"],
    queryFn: adminAPI.listProviders,
  });
}

export function useProvider(id: string) {
  return useQuery({
    queryKey: ["admin", "providers", id],
    queryFn: () => adminAPI.getProvider(id),
    enabled: !!id,
  });
}

export function useCreateProvider() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: Partial<Provider>) => adminAPI.createProvider(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "providers"] });
    },
  });
}

export function useUpdateProvider() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<Provider> }) =>
      adminAPI.updateProvider(id, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "providers"] });
    },
  });
}

export function useDeleteProvider() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => adminAPI.deleteProvider(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "providers"] });
    },
  });
}

// --- Settings ---

export function useSettings() {
  return useQuery({
    queryKey: ["admin", "settings"],
    queryFn: adminAPI.getSettings,
  });
}

export function useUpdateSettings() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (settings: Setting[]) => adminAPI.updateSettings(settings),
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
    mutationFn: (data: Partial<Webhook>) => adminAPI.createWebhook(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["admin", "webhooks"] });
    },
  });
}

export function useUpdateWebhook() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<Webhook> }) =>
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
    queryFn: adminAPI.getUsage,
  });
}

// --- Database ---

export function useDatabaseInfo() {
  return useQuery({
    queryKey: ["admin", "database"],
    queryFn: adminAPI.getDatabaseInfo,
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
