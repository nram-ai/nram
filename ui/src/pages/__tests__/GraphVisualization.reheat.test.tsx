/**
 * @vitest-environment happy-dom
 *
 * Size-adaptive layout-drag behaviour.
 *
 * Dragging a layout slider sets the d3-force strengths live (cheap O(1)
 * setters) on every change, but the expensive simulation reheat (which
 * restarts alpha and re-lays-out the whole graph) is gated on graph size:
 * below LIVE_LAYOUT_NODE_LIMIT (1000) the live morph is kept, at or above it
 * the graph re-lays-out only when the drag is released. This test forwards a
 * spy handle into the mocked ForceGraph3D so the reheat calls are observable.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import React from "react";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";

import GraphVisualization from "../GraphVisualization";
import * as useApi from "../../hooks/useApi";
import { ProjectProvider, STORAGE_KEY as PROJECT_STORAGE_KEY } from "../../context/ProjectContext";

const reheatSpy = vi.fn();
const centerStrength = vi.fn();
const chargeStrength = vi.fn();
const linkDistance = vi.fn();

// Forward a spy handle into the ref the page attaches to ForceGraph3D so the
// imperative force setters and the reheat are observable. Still renders a
// no-op div (no WebGL).
vi.mock("react-force-graph-3d", () => ({
  __esModule: true,
  default: React.forwardRef((_props: unknown, ref: React.Ref<unknown>) => {
    React.useImperativeHandle(ref, () => ({
      d3Force: (name: string) =>
        name === "charge"
          ? { strength: chargeStrength }
          : name === "link"
            ? { distance: linkDistance }
            : { strength: centerStrength },
      d3ReheatSimulation: reheatSpy,
      cameraPosition: vi.fn(),
    }));
    return <div data-testid="force-graph-3d-stub" />;
  }),
}));

vi.mock("../../hooks/useApi", async () => {
  const actual = await vi.importActual<typeof import("../../hooks/useApi")>(
    "../../hooks/useApi",
  );
  return {
    ...actual,
    useMeProjects: vi.fn(),
    useGraph: vi.fn(),
    useUpdateProject: vi.fn(),
    useSettingDefaults: vi.fn(),
  };
});

const useMeProjectsMock = vi.mocked(useApi.useMeProjects);
const useGraphMock = vi.mocked(useApi.useGraph);
const useUpdateProjectMock = vi.mocked(useApi.useUpdateProject);
const useSettingDefaultsMock = vi.mocked(useApi.useSettingDefaults);

const PROJECT_ID = "p1";

function settingDefaultsStub(): ReturnType<typeof useApi.useSettingDefaults> {
  return {
    byKey: {
      "graph.center_gravity": { key: "graph.center_gravity", value: 0.75, default_value: 0.75, min: 0, max: 3, step: 0.05 },
      "graph.charge_strength": { key: "graph.charge_strength", value: -100, default_value: -100, min: -100, max: 0, step: 1 },
      "graph.link_distance": { key: "graph.link_distance", value: 100, default_value: 100, min: 5, max: 100, step: 1 },
    },
    isLoading: false,
    isError: false,
  };
}

function project() {
  return {
    id: PROJECT_ID,
    namespace_id: "ns-p1",
    owner_namespace_id: "ns1",
    name: "Test Project",
    slug: "test-project",
    description: "",
    default_tags: [],
    settings: {},
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function queryStub<T>(data: T): any {
  return { data, isLoading: false, isError: false, error: null, refetch: vi.fn() };
}

function graphWith(nodeCount: number) {
  const entities = Array.from({ length: nodeCount }, (_v, i) => ({
    id: `ent${i}`,
    name: `Entity ${i}`,
    canonical: `entity_${i}`,
    entity_type: "concept",
    mention_count: 1,
    aliases: [],
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  }));
  return queryStub({ entities, relationships: [] }) as ReturnType<typeof useApi.useGraph>;
}

// Flush a single animation frame plus microtasks so requestReheat's rAF
// callback (and any pending effects) run under real timers.
async function flushFrame() {
  await act(async () => {
    await new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));
  });
}

afterEach(() => {
  cleanup();
  sessionStorage.clear();
  vi.clearAllMocks();
});

beforeEach(() => {
  useSettingDefaultsMock.mockReturnValue(settingDefaultsStub());
  useUpdateProjectMock.mockReturnValue({
    mutate: vi.fn(),
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any);
  useMeProjectsMock.mockReturnValue(
    queryStub([project()]) as ReturnType<typeof useApi.useMeProjects>,
  );
  sessionStorage.setItem(PROJECT_STORAGE_KEY, PROJECT_ID);
});

describe("GraphVisualization size-adaptive layout drag", () => {
  it("at/above the node limit, a drag sets forces live but reheats only on release", async () => {
    useGraphMock.mockReturnValue(graphWith(1001));

    render(
      <ProjectProvider>
        <GraphVisualization />
      </ProjectProvider>,
    );
    await flushFrame();

    fireEvent.click(screen.getByRole("button", { name: /Layout/i }));
    await flushFrame();

    reheatSpy.mockClear();
    centerStrength.mockClear();

    // Drag Gravity. Forces are pushed live; the sim is NOT reheated.
    fireEvent.change(screen.getAllByRole("slider")[0], { target: { value: "2.0" } });
    await flushFrame();

    expect(centerStrength).toHaveBeenCalledWith(2.0);
    expect(reheatSpy).not.toHaveBeenCalled();

    // Release the slider: now it re-lays-out once.
    fireEvent.pointerUp(screen.getAllByRole("slider")[0]);
    await flushFrame();

    expect(reheatSpy).toHaveBeenCalled();
  });

  it("below the node limit, a drag reheats live (no release needed)", async () => {
    useGraphMock.mockReturnValue(graphWith(5));

    render(
      <ProjectProvider>
        <GraphVisualization />
      </ProjectProvider>,
    );
    await flushFrame();

    fireEvent.click(screen.getByRole("button", { name: /Layout/i }));
    await flushFrame();

    reheatSpy.mockClear();

    fireEvent.change(screen.getAllByRole("slider")[0], { target: { value: "2.0" } });
    await flushFrame();

    expect(reheatSpy).toHaveBeenCalled();
  });
});
