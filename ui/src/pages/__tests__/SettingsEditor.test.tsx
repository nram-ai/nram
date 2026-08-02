/**
 * @vitest-environment happy-dom
 *
 * Covers the tabbed + searchable Settings page: the tab bar shows one tab per
 * visible group and renders only the active group; live search filters across
 * every group at once.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";
import { MemoryRouter } from "react-router-dom";

import SettingsEditor from "../SettingsEditor";
import * as useApi from "../../hooks/useApi";
import * as useEnrichment from "../../hooks/useEnrichmentAvailable";

vi.mock("../../hooks/useApi", async () => {
  const actual = await vi.importActual<typeof import("../../hooks/useApi")>(
    "../../hooks/useApi",
  );
  return {
    ...actual,
    useSettings: vi.fn(),
    useSettingsSchema: vi.fn(),
    useSettingGroups: vi.fn(),
    useUpdateSetting: vi.fn(),
    useResetSettings: vi.fn(),
    useSetupStatus: vi.fn(),
    useGraphHealth: vi.fn(),
    useRepairGraph: vi.fn(),
    useBackfillAugmentation: vi.fn(),
  };
});

vi.mock("../../hooks/useEnrichmentAvailable", () => ({
  useEnrichmentAvailable: vi.fn(),
}));

const useSettingsMock = vi.mocked(useApi.useSettings);
const useSettingsSchemaMock = vi.mocked(useApi.useSettingsSchema);
const useSettingGroupsMock = vi.mocked(useApi.useSettingGroups);
const useUpdateSettingMock = vi.mocked(useApi.useUpdateSetting);
const useResetSettingsMock = vi.mocked(useApi.useResetSettings);
const useSetupStatusMock = vi.mocked(useApi.useSetupStatus);
const useGraphHealthMock = vi.mocked(useApi.useGraphHealth);
const useRepairGraphMock = vi.mocked(useApi.useRepairGraph);
const useBackfillAugmentationMock = vi.mocked(useApi.useBackfillAugmentation);
const useEnrichmentAvailableMock = vi.mocked(useEnrichment.useEnrichmentAvailable);

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const loaded = (data: unknown): any => ({
  data,
  isLoading: false,
  isError: false,
});

const SCHEMAS = [
  { key: "enrichment.enabled", type: "boolean", default_value: true, description: "Enable enrichment", category: "enrichment" },
  { key: "enrichment.batch_size", type: "number", default_value: 32, description: "Batch size", category: "enrichment", min: 1, max: 100, step: 1 },
  { key: "enrichment.dedup_threshold", type: "number", default_value: 0.9, description: "Dedup threshold", category: "enrichment_ingestion", min: 0, max: 1, step: 0.01 },
  { key: "ranking.weight.similarity", type: "number", default_value: 0.5, description: "Similarity weight", category: "ranking", min: 0, max: 1, step: 0.05 },
  // usage category: token_retention shows here; cost_rates is edited on Analytics.
  { key: "usage.token_retention_days", type: "number", default_value: 90, description: "Token usage retention", category: "usage", min: 1, max: 3650, step: 1 },
  { key: "usage.cost_rates", type: "json", default_value: [], description: "Per-model cost rates", category: "usage" },
];

const GROUPS = [
  {
    id: "enrichment",
    label: "Enrichment",
    requires_enrichment: true,
    subsections: [
      { category: "enrichment", label: "General" },
      { category: "enrichment_ingestion", label: "Ingestion Decision" },
    ],
  },
  {
    id: "recall",
    label: "Recall & Ranking",
    subsections: [{ category: "ranking", label: "Ranking" }],
  },
  {
    id: "usage_export",
    label: "Usage & Export",
    subsections: [{ category: "usage", label: "Usage" }],
  },
];

function renderPage(path = "/settings") {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <SettingsEditor />
    </MemoryRouter>,
  );
}

describe("SettingsEditor tabs + search", () => {
  beforeEach(() => {
    useSettingsMock.mockReturnValue(loaded({ data: [] }));
    useSettingsSchemaMock.mockReturnValue(loaded({ data: SCHEMAS }));
    useSettingGroupsMock.mockReturnValue(loaded({ data: GROUPS }));
    useSetupStatusMock.mockReturnValue(loaded({ backend: "sqlite" }));
    useUpdateSettingMock.mockReturnValue({ mutate: vi.fn(), isPending: false } as never);
    useResetSettingsMock.mockReturnValue({ mutate: vi.fn(), isPending: false } as never);
    useGraphHealthMock.mockReturnValue(loaded({ lost_provenance_edges: 0 }));
    useRepairGraphMock.mockReturnValue({ mutate: vi.fn(), isPending: false } as never);
    useBackfillAugmentationMock.mockReturnValue({ mutate: vi.fn(), isPending: false } as never);
    useEnrichmentAvailableMock.mockReturnValue({ available: true } as never);
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
  });

  it("renders a tab per group and shows only the active group", () => {
    renderPage();

    // Both groups appear as tabs.
    expect(screen.getByRole("tab", { name: "Enrichment" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Recall & Ranking" })).toBeInTheDocument();

    // Default active tab is the first group: enrichment settings show, ranking does not.
    expect(screen.getByText("enrichment.enabled")).toBeInTheDocument();
    expect(screen.getByText("enrichment.dedup_threshold")).toBeInTheDocument();
    expect(screen.queryByText("ranking.weight.similarity")).not.toBeInTheDocument();
  });

  it("switches the rendered group when another tab is clicked", () => {
    renderPage();

    fireEvent.click(screen.getByRole("tab", { name: "Recall & Ranking" }));

    expect(screen.getByText("ranking.weight.similarity")).toBeInTheDocument();
    expect(screen.queryByText("enrichment.enabled")).not.toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Recall & Ranking" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
  });

  it("filters across all groups while searching, then restores the active tab", () => {
    renderPage();

    const box = screen.getByLabelText("Search settings");
    fireEvent.change(box, { target: { value: "similarity" } });

    // Match comes from the ranking group even though the enrichment tab is active.
    expect(screen.getByText("ranking.weight.similarity")).toBeInTheDocument();
    expect(screen.queryByText("enrichment.enabled")).not.toBeInTheDocument();
    expect(screen.getByText(/1 setting match/i)).toBeInTheDocument();
    // Tab bar is hidden while searching.
    expect(screen.queryByRole("tab")).not.toBeInTheDocument();

    // Clearing the search restores the active (enrichment) tab.
    fireEvent.click(screen.getByLabelText("Clear search"));
    expect(screen.getByText("enrichment.enabled")).toBeInTheDocument();
    expect(screen.queryByText("ranking.weight.similarity")).not.toBeInTheDocument();
  });

  it("shows a no-results message when nothing matches", () => {
    renderPage();

    fireEvent.change(screen.getByLabelText("Search settings"), {
      target: { value: "zzz-nonexistent" },
    });
    expect(screen.getByText(/No settings match/i)).toBeInTheDocument();
  });

  it("hides usage.cost_rates (edited on Analytics) but shows other usage settings", () => {
    renderPage("/settings?group=usage_export");

    expect(screen.getByText("usage.token_retention_days")).toBeInTheDocument();
    expect(screen.queryByText("usage.cost_rates")).not.toBeInTheDocument();
  });

  it("never surfaces usage.cost_rates via search either", () => {
    renderPage();

    fireEvent.change(screen.getByLabelText("Search settings"), {
      target: { value: "cost_rates" },
    });
    expect(screen.queryByText("usage.cost_rates")).not.toBeInTheDocument();
    expect(screen.getByText(/No settings match/i)).toBeInTheDocument();
  });

  // Regression guard: prompt-typed settings (edited on the Prompt Templates
  // page) must never leak onto the Settings page. Their categories are
  // intentionally absent from the group taxonomy, so if they were not hidden
  // they would be swept into a synthetic "Other" tab. The Ask prompts
  // (category ask_prompts) once did exactly that.
  it("hides prompt-typed settings instead of bucketing them into an Other tab", () => {
    useSettingsSchemaMock.mockReturnValue(
      loaded({
        data: [
          ...SCHEMAS,
          { key: "ask.synthesis.system_prompt", type: "prompt", default_value: "", description: "Ask answer synthesis prompt", category: "ask_prompts" },
          { key: "ask.decomposition.system_prompt", type: "prompt", default_value: "", description: "Ask query decomposition prompt", category: "ask_prompts" },
        ],
      }),
    );

    renderPage();

    // No synthetic "Other" tab is created for the unmapped ask_prompts category.
    expect(screen.queryByRole("tab", { name: "Other" })).not.toBeInTheDocument();

    // The prompt keys never render on the Settings page, in any tab or search.
    fireEvent.change(screen.getByLabelText("Search settings"), {
      target: { value: "system_prompt" },
    });
    expect(screen.queryByText("ask.synthesis.system_prompt")).not.toBeInTheDocument();
    expect(screen.queryByText("ask.decomposition.system_prompt")).not.toBeInTheDocument();
    expect(screen.getByText(/No settings match/i)).toBeInTheDocument();
  });

  it("marks a row 'Modified' only when its value differs from the default", () => {
    // enrichment.batch_size (default 32) is overridden to 64; enrichment.enabled
    // has no stored override and stays at its default.
    useSettingsMock.mockReturnValue(
      loaded({
        data: [
          {
            key: "enrichment.batch_size",
            value: 64,
            scope: "global",
            updated_at: "2026-06-01T00:00:00Z",
          },
        ],
      }),
    );
    renderPage();

    // Exactly one row is marked Modified, and it is the overridden one.
    const badges = screen.getAllByText("Modified");
    expect(badges).toHaveLength(1);

    const overriddenRow = screen.getByText("enrichment.batch_size").closest("div");
    expect(overriddenRow).toHaveTextContent("Modified");

    // The untouched boolean row still reads "(default)" and is not Modified.
    const defaultRow = screen.getByText("enrichment.enabled").closest("div");
    expect(defaultRow).toHaveTextContent("(default)");
    expect(defaultRow).not.toHaveTextContent("Modified");
  });

  // --- Secret-typed settings ---
  //
  // The server masks secret values on read (returns a redaction sentinel, not
  // the real value). The editor must render "configured" without leaking the
  // sentinel, start the edit input blank, treat a blank save as "keep", and
  // send a newly typed value on save.
  // Category "auth" carries no trailing operator block (unlike "qdrant", which
  // renders VectorMigrationBlock and would need a QueryClientProvider), so the
  // secret-input behavior is exercised in isolation.
  const SECRET_SCHEMA = {
    key: "qdrant.api_key",
    type: "secret",
    default_value: "",
    description: "API key for authenticating to Qdrant.",
    category: "auth",
  };
  const SECRET_GROUP = {
    id: "auth",
    label: "Authentication",
    subsections: [{ category: "auth", label: "Auth" }],
  };
  function mockConfiguredSecret() {
    useSettingsSchemaMock.mockReturnValue(loaded({ data: [SECRET_SCHEMA] }));
    useSettingGroupsMock.mockReturnValue(loaded({ data: [SECRET_GROUP] }));
    // Value is the sentinel the server returns for a configured secret.
    useSettingsMock.mockReturnValue(
      loaded({
        data: [
          {
            key: "qdrant.api_key",
            value: "__redacted__",
            scope: "global",
            updated_at: "2026-06-01T00:00:00Z",
          },
        ],
      }),
    );
  }

  it("renders a configured secret as masked dots, never the value the server sent", () => {
    mockConfiguredSecret();
    renderPage("/settings?group=auth");

    expect(screen.getByText("qdrant.api_key")).toBeInTheDocument();
    expect(screen.getByText("••••••••")).toBeInTheDocument();
    // The (already masked) sentinel value must not appear in the DOM.
    expect(screen.queryByText("__redacted__")).not.toBeInTheDocument();
  });

  it("starts the secret edit input blank and a blank save does not call the update mutation", () => {
    const mutate = vi.fn();
    useUpdateSettingMock.mockReturnValue({ mutate, isPending: false } as never);
    mockConfiguredSecret();
    renderPage("/settings?group=auth");

    fireEvent.click(screen.getByRole("button", { name: "Edit" }));
    const input = document.querySelector(
      'input[type="password"]',
    ) as HTMLInputElement | null;
    expect(input).not.toBeNull();
    // Never pre-filled with the masked value: a blank field can't clobber the secret.
    expect(input!.value).toBe("");

    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    expect(mutate).not.toHaveBeenCalled();
  });

  it("saves a newly typed secret value", () => {
    const mutate = vi.fn();
    useUpdateSettingMock.mockReturnValue({ mutate, isPending: false } as never);
    mockConfiguredSecret();
    renderPage("/settings?group=auth");

    fireEvent.click(screen.getByRole("button", { name: "Edit" }));
    const input = document.querySelector(
      'input[type="password"]',
    ) as HTMLInputElement;
    fireEvent.change(input, { target: { value: "new-api-key" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(mutate).toHaveBeenCalledTimes(1);
    expect(mutate.mock.calls[0][0]).toMatchObject({
      key: "qdrant.api_key",
      value: "new-api-key",
    });
  });

  // A category can carry an operator action block beneath its setting rows
  // (GraphMaintenanceBlock under lifecycle, QueryAugmentBackfillBlock under
  // enrichment_query_augment). These must render regardless of which
  // ParentGroupCard path the group takes: a group with a single unlabeled
  // subsection renders "flat" (no h3), and the lifecycle group is exactly that
  // shape, so its block only appears if the flat path renders trailing blocks.
  it("renders GraphMaintenanceBlock under the flattened Lifecycle Sweep group", () => {
    useSettingsSchemaMock.mockReturnValue(
      loaded({
        data: [
          {
            key: "lifecycle.sweep_interval_seconds",
            type: "number",
            default_value: 3600,
            description: "Sweep interval",
            category: "lifecycle",
            min: 1,
            max: 86400,
            step: 1,
          },
        ],
      }),
    );
    useSettingGroupsMock.mockReturnValue(
      loaded({
        data: [
          {
            id: "lifecycle",
            label: "Lifecycle Sweep",
            // Single subsection with no label/description => flatten path.
            subsections: [{ category: "lifecycle" }],
          },
        ],
      }),
    );

    renderPage("/settings?group=lifecycle");

    // The setting row renders flat (no sub-heading) ...
    expect(screen.getByText("lifecycle.sweep_interval_seconds")).toBeInTheDocument();
    // ... and the operator action block renders beneath it.
    expect(screen.getByText("Graph Maintenance")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Repair now" })).toBeInTheDocument();
    expect(screen.getByText(/No orphaned edges found/i)).toBeInTheDocument();
  });

  it("renders QueryAugmentBackfillBlock under the sectioned Enrichment group", () => {
    useSettingsSchemaMock.mockReturnValue(
      loaded({
        data: [
          { key: "enrichment.enabled", type: "boolean", default_value: true, description: "Enable enrichment", category: "enrichment" },
          { key: "enrichment.query_augment_enabled", type: "boolean", default_value: true, description: "Augment queries", category: "enrichment_query_augment" },
        ],
      }),
    );
    useSettingGroupsMock.mockReturnValue(
      loaded({
        data: [
          {
            id: "enrichment",
            label: "Enrichment",
            // Multiple labeled subsections => sectioned (non-flatten) path.
            subsections: [
              { category: "enrichment", label: "General" },
              { category: "enrichment_query_augment", label: "Query Augmentation" },
            ],
          },
        ],
      }),
    );

    renderPage("/settings?group=enrichment");

    expect(screen.getByText("enrichment.query_augment_enabled")).toBeInTheDocument();
    // The backfill block renders under the sectioned (non-flatten) path.
    expect(screen.getByText("Backfill Augmentation")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /Check candidates/i }),
    ).toBeInTheDocument();
  });
});
