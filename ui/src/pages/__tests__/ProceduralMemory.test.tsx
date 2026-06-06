/**
 * @vitest-environment happy-dom
 *
 * Covers the reworked Procedural Memory table: rows render with all fields up
 * front, the search box filters live, and the Export/Import actions are present.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";

import ProceduralMemory from "../ProceduralMemory";
import type { ProceduralEntry } from "../../api/client";

const entries: ProceduralEntry[] = [
  {
    id: "11111111-1111-1111-1111-111111111111",
    content: "Use zero em dashes in every output.",
    title: "Em dashes: zero",
    category: "formatting",
    tags: ["non-negotiable", "em-dash"],
    priority: 100,
    enabled: true,
    created_at: "2026-06-01T00:00:00Z",
    updated_at: "2026-06-01T00:00:00Z",
  },
  {
    id: "22222222-2222-2222-2222-222222222222",
    content: "Render before claiming a visual artifact is done.",
    title: "Render before claim",
    category: "verification",
    tags: ["visual"],
    priority: 80,
    enabled: false,
    created_at: "2026-06-02T00:00:00Z",
    updated_at: "2026-06-02T00:00:00Z",
  },
];

const noopMutation = () => ({ mutate: vi.fn(), mutateAsync: vi.fn(), isPending: false });

vi.mock("../../hooks/useApi", () => ({
  useProcedural: () => ({ data: entries, isLoading: false }),
  useCreateProcedural: () => noopMutation(),
  useUpdateProcedural: () => noopMutation(),
  useDeleteProcedural: () => noopMutation(),
  useImportProcedural: () => noopMutation(),
}));

vi.mock("../../api/client", () => ({
  meAPI: { exportProcedural: vi.fn() },
}));

describe("ProceduralMemory table", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });
  afterEach(() => cleanup());

  it("renders a row per entry with title, category, and tags up front", () => {
    render(<ProceduralMemory />);
    expect(screen.getByText("Em dashes: zero")).toBeInTheDocument();
    expect(screen.getByText("Render before claim")).toBeInTheDocument();
    expect(screen.getByText("em-dash")).toBeInTheDocument();
    // Priority is shown as a column value.
    expect(screen.getByText("100")).toBeInTheDocument();
  });

  it("exposes Export and Import actions", () => {
    render(<ProceduralMemory />);
    expect(screen.getByRole("button", { name: /export/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /import/i })).toBeInTheDocument();
  });

  it("filters rows live as the user types in search", () => {
    render(<ProceduralMemory />);
    const search = screen.getByPlaceholderText(/search title, content, category, tags/i);
    fireEvent.change(search, { target: { value: "render" } });
    expect(screen.queryByText("Em dashes: zero")).not.toBeInTheDocument();
    expect(screen.getByText("Render before claim")).toBeInTheDocument();
  });
});
