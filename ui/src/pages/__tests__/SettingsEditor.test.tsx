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
];

function renderPage() {
  return render(
    <MemoryRouter initialEntries={["/settings"]}>
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
});
