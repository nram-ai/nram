/**
 * @vitest-environment happy-dom
 *
 * Regression test for the detail-panel "renders empty on re-open" race.
 *
 * The bug: ProjectDetailPanel initialized its edit form with two effects:
 * an initializer `if (project && !initialized) { ...; setInitialized(true) }`
 * keyed on `[project, initialized]`, and a reset `setInitialized(false)`
 * keyed on `[projectId]`. When `useProject` returns cached data
 * synchronously at mount (the case when re-opening a project whose query is
 * still warm in the React Query cache), both effects fire in the SAME mount
 * commit. The reset, declared second, clobbers the initializer's
 * `setInitialized(true)` within the batch, so `initialized` settles false
 * and the render guard `{project && initialized && (...)}` renders an empty
 * body: no form, no skeleton, no error.
 *
 * Mounting the panel with `useProject` returning data synchronously
 * reproduces the cached-at-mount condition exactly. With the bug present the
 * Name input never appears; with the fix (reset effect removed) it renders
 * populated.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";

import ProjectManagement from "../ProjectManagement";
import * as useApi from "../../hooks/useApi";
import { useAuth } from "../../context/AuthContext";
import type { Project } from "../../api/client";

vi.mock("../../context/AuthContext", () => ({
  useAuth: vi.fn(),
}));

vi.mock("../../hooks/useApi", async () => {
  const actual = await vi.importActual<typeof import("../../hooks/useApi")>(
    "../../hooks/useApi",
  );
  return {
    ...actual,
    useMeProjects: vi.fn(),
    useProject: vi.fn(),
    useUpdateProject: vi.fn(),
    useDeleteProject: vi.fn(),
    useSchemaRange: vi.fn(),
    useSystemRankingWeights: vi.fn(),
    useSettingDefaults: vi.fn(),
  };
});

const useAuthMock = vi.mocked(useAuth);
const useMeProjectsMock = vi.mocked(useApi.useMeProjects);
const useProjectMock = vi.mocked(useApi.useProject);
const useUpdateProjectMock = vi.mocked(useApi.useUpdateProject);
const useDeleteProjectMock = vi.mocked(useApi.useDeleteProject);
const useSchemaRangeMock = vi.mocked(useApi.useSchemaRange);
const useSystemRankingWeightsMock = vi.mocked(useApi.useSystemRankingWeights);
const useSettingDefaultsMock = vi.mocked(useApi.useSettingDefaults);

const PROJECT_ID = "p1";

function project(): Project {
  return {
    id: PROJECT_ID,
    namespace_id: "ns-p1",
    owner_namespace_id: "ns1",
    name: "Reopen Test Project",
    slug: "reopen-test-project",
    path: "",
    description: "A project used by the re-open regression test.",
    memory_count: 3,
    entity_count: 2,
    relationship_count: 1,
    default_tags: ["alpha"],
    settings: {},
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-02T00:00:00Z",
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function queryStub<T>(data: T, isLoading = false): any {
  return { data, isLoading, isError: false, error: null, refetch: vi.fn() };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function mutationStub(): any {
  return { mutate: vi.fn(), isPending: false, isError: false, error: null };
}

afterEach(() => {
  cleanup();
});

beforeEach(() => {
  useAuthMock.mockReturnValue({
    user: {
      id: "u1",
      email: "u@test",
      display_name: "U",
      role: "administrator",
      org_id: "org-self",
    },
    isAdmin: true,
    isOrgOwner: true,
    canWrite: true,
    hasMinRole: () => true,
    login: vi.fn(),
    logout: vi.fn(),
    setUser: vi.fn(),
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any);

  useMeProjectsMock.mockReturnValue(
    queryStub([project()]) as ReturnType<typeof useApi.useMeProjects>,
  );
  // The detail query returns its data synchronously; this is precisely the
  // "already cached at mount" condition that triggers the race on re-open.
  useProjectMock.mockReturnValue(
    queryStub(project()) as ReturnType<typeof useApi.useProject>,
  );
  useUpdateProjectMock.mockReturnValue(mutationStub());
  useDeleteProjectMock.mockReturnValue(mutationStub());
  useSchemaRangeMock.mockImplementation((_key, fallback) => fallback);
  useSystemRankingWeightsMock.mockReturnValue({
    weights: {
      similarity: 0.4,
      recency: 0.1,
      importance: 0.1,
      frequency: 0.1,
      graph_relevance: 0.1,
      confidence: 0.2,
      origin: 0,
      mmr_lambda: 0.5,
    },
    missingKeys: [],
    isLoading: false,
    isError: false,
  });
  useSettingDefaultsMock.mockReturnValue({
    byKey: {
      "enrichment.dedup_threshold": {
        key: "enrichment.dedup_threshold",
        value: 0.92,
        default_value: 0.92,
        min: 0,
        max: 1,
        step: 0.01,
      },
    },
    isLoading: false,
    isError: false,
  });
});

describe("ProjectManagement detail panel, cached-at-mount re-open", () => {
  it("renders the populated edit form when the project query is already cached at mount", async () => {
    render(<ProjectManagement />);

    // Open the panel by clicking the project row. The detail query already
    // has data (mocked synchronous), so the panel mounts with `project`
    // defined on its very first render: the re-open condition.
    fireEvent.click(screen.getByText("Reopen Test Project"));

    // The form body must render. Pre-fix, the reset effect clobbered
    // `initialized` and this query found nothing (empty panel body).
    const nameInput = await screen.findByDisplayValue("Reopen Test Project");
    expect(nameInput).toBeInTheDocument();
    expect(
      screen.getByDisplayValue("A project used by the re-open regression test."),
    ).toBeInTheDocument();

    // The Save button (only present inside the populated form) confirms the
    // body rendered rather than an empty shell.
    expect(
      screen.getByRole("button", { name: /Save Changes/i }),
    ).toBeInTheDocument();
  });
});
