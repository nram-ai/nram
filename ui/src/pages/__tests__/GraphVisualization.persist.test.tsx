/**
 * @vitest-environment happy-dom
 *
 * Regression test for the graph layout slider persistence race.
 *
 * The bug: on cold load, the persist effect inside GraphVisualization fired
 * exactly once when the projects query transitioned from loading to loaded,
 * at which point the local slider state still held the useState seed value
 * (the cascade default) and the useDebounce-mirrored value was identical.
 * The effect compared the stale seed to the just-arrived persisted override
 * and wrote the default back to the backend, clobbering the user's saved
 * configuration. The slider then snapped back to the default after the
 * mutation's refetch landed.
 *
 * The fix gates the persist effect on a userEditedRef that only latches to
 * true when the user actually drags a slider. This test asserts the gate:
 * mounting with persisted overrides must not trigger any PUT, but a real
 * user interaction must still persist as before.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";

import GraphVisualization from "../GraphVisualization";
import * as useApi from "../../hooks/useApi";
import { ProjectProvider, STORAGE_KEY as PROJECT_STORAGE_KEY } from "../../context/ProjectContext";

// react-force-graph-3d pulls in WebGL bindings on import. Stub it to a
// no-op component so happy-dom does not need to render WebGL, and so the
// nodeThreeObject / link-particle callbacks (which exercise three.js
// canvas sprites) never fire.
vi.mock("react-force-graph-3d", () => ({
  __esModule: true,
  default: () => <div data-testid="force-graph-3d-stub" />,
}));

// three.js itself is referenced at module scope by GraphVisualization for
// the sprite/material constructors. The real module loads fine under
// happy-dom (it does not touch WebGL on import); we leave it un-mocked.

// Mock the data hooks. We do not mock useDebounce: the real implementation
// uses setTimeout, which we drive with vi.useFakeTimers below so the test
// is deterministic.
vi.mock("../../hooks/useApi", async () => {
  const actual = await vi.importActual<typeof import("../../hooks/useApi")>(
    "../../hooks/useApi",
  );
  return {
    ...actual,
    useMeProjects: vi.fn(),
    useGraph: vi.fn(),
    useUpdateProject: vi.fn(),
    useSchemaRange: vi.fn(),
  };
});

const useMeProjectsMock = vi.mocked(useApi.useMeProjects);
const useGraphMock = vi.mocked(useApi.useGraph);
const useUpdateProjectMock = vi.mocked(useApi.useUpdateProject);
const useSchemaRangeMock = vi.mocked(useApi.useSchemaRange);

const PROJECT_ID = "p1";

function projectWithOverrides() {
  return {
    id: PROJECT_ID,
    namespace_id: "ns-p1",
    owner_namespace_id: "ns1",
    name: "Test Project",
    slug: "test-project",
    description: "",
    default_tags: [],
    settings: {
      graph_center_gravity: 1.2,
      graph_charge_strength: -42,
      graph_link_distance: 30,
    },
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function queryStub<T>(data: T, isLoading = false): any {
  return {
    data,
    isLoading,
    isError: false,
    error: null,
    refetch: vi.fn(),
  };
}

function rangeStub(min: number, max: number, step: number) {
  return { min, max, step };
}

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  sessionStorage.clear();
});

beforeEach(() => {
  useSchemaRangeMock.mockImplementation((key, fallback) => {
    if (key === "graph.center_gravity") return rangeStub(0, 3, 0.05);
    if (key === "graph.charge_strength") return rangeStub(-100, 0, 1);
    if (key === "graph.link_distance") return rangeStub(5, 100, 1);
    return fallback;
  });
  // At least one entity is required for the page to render the canvas
  // branch, which is the only branch that mounts the LayoutDrawer. The
  // ForceGraph3D component itself is mocked at module scope above, so no
  // WebGL is exercised.
  useGraphMock.mockReturnValue(
    queryStub({
      entities: [
        {
          id: "ent1",
          name: "Stub Entity",
          canonical: "stub_entity",
          entity_type: "concept",
          mention_count: 1,
          aliases: [],
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        },
      ],
      relationships: [],
    }) as ReturnType<typeof useApi.useGraph>,
  );
});

describe("GraphVisualization layout persistence", () => {
  it("does not write to the backend when hydrating a project that already has persisted overrides", async () => {
    // Selecting the project up-front matches the real flow: the picker is
    // a controlled select inside the page, and the persistence race fires
    // the same way regardless of how the project becomes "selected."
    const mutate = vi.fn();
    useUpdateProjectMock.mockReturnValue({
      mutate,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);

    // Start in the "projects still loading" state. This is the critical
    // condition for the regression: when projects are pending, the page's
    // useState seeds the sliders to cascade defaults. The persist effect
    // must not write those defaults once projects finally arrive carrying
    // persisted overrides.
    useMeProjectsMock.mockReturnValue(
      queryStub(undefined, true) as ReturnType<typeof useApi.useMeProjects>,
    );

    vi.useFakeTimers();

    const { rerender } = render(
      <ProjectProvider>
        <GraphVisualization />
      </ProjectProvider>,
    );

    // Flush effects and any pending timers from the loading render.
    await vi.runAllTimersAsync();

    // Now resolve the projects query with a project carrying overrides.
    useMeProjectsMock.mockReturnValue(
      queryStub([projectWithOverrides()]) as ReturnType<
        typeof useApi.useMeProjects
      >,
    );

    rerender(
      <ProjectProvider>
        <GraphVisualization />
      </ProjectProvider>,
    );

    // Settle the debounce window (300 ms) and any follow-on effects.
    await vi.advanceTimersByTimeAsync(1000);

    // The regression check: no PUT should have been fired purely as a
    // result of hydration. The user did not touch any slider.
    expect(mutate).not.toHaveBeenCalled();
  });

  it("persists a slider change driven by the user after the debounce window", async () => {
    const mutate = vi.fn();
    useUpdateProjectMock.mockReturnValue({
      mutate,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);
    useMeProjectsMock.mockReturnValue(
      queryStub([projectWithOverrides()]) as ReturnType<
        typeof useApi.useMeProjects
      >,
    );

    // Pre-select the project so the Layout button is enabled on first
    // render. The auto-select effect on the page would do this too, but
    // its state update does not deterministically flush under fake timers
    // before the synchronous fireEvent below; seeding sessionStorage takes
    // that timing question off the table.
    sessionStorage.setItem(PROJECT_STORAGE_KEY, PROJECT_ID);

    vi.useFakeTimers();

    render(
      <ProjectProvider>
        <GraphVisualization />
      </ProjectProvider>,
    );

    // Let hydration settle so any spurious writes (if the gate regressed)
    // would have been observed by now. We assert below that the only
    // write is the one from our deliberate slider change.
    await vi.advanceTimersByTimeAsync(1000);

    // Open the layout drawer. The button is wired to setLayoutDrawerOpen.
    fireEvent.click(screen.getByRole("button", { name: /Layout/i }));

    // The drawer renders three sliders in order: Gravity, Repulsion,
    // Link distance. Move Gravity to a new value the user has not seen
    // before.
    const sliders = screen.getAllByRole("slider");
    expect(sliders.length).toBe(3);
    const gravitySlider = sliders[0];
    fireEvent.change(gravitySlider, { target: { value: "2.0" } });

    // Advance past the 300 ms debounce window. waitFor cannot be used
    // under fake timers (its poll loop relies on wall-clock); we flush
    // pending timers and then assert directly.
    await vi.advanceTimersByTimeAsync(500);

    expect(mutate).toHaveBeenCalledTimes(1);

    const call = mutate.mock.calls[0][0] as {
      id: string;
      data: { settings: Record<string, unknown> };
    };
    expect(call.id).toBe(PROJECT_ID);
    expect(call.data.settings.graph_center_gravity).toBeCloseTo(2.0, 5);
    // The unchanged sliders must come through with their stored values
    // (NOT undefined and NOT the cascade defaults), so an unrelated drag
    // cannot accidentally erase other overrides.
    expect(call.data.settings.graph_charge_strength).toBeCloseTo(-42, 5);
    expect(call.data.settings.graph_link_distance).toBeCloseTo(30, 5);
  });

  it("reset does not fire a trailing default-value write from a still-pending debounce", async () => {
    // Edit-then-reset within the debounce window: a drag at T+0 schedules
    // a 300ms debounce, then a Reset click at T+50 fires its own direct
    // mutation. Without the latch reset in handleResetLayout, the
    // debounce would fire at T+300 with userEditedRef still true and
    // write the default value back as an explicit override, defeating
    // the intent of Reset (which is to clear the override entirely).
    const mutate = vi.fn();
    useUpdateProjectMock.mockReturnValue({
      mutate,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);
    useMeProjectsMock.mockReturnValue(
      queryStub([projectWithOverrides()]) as ReturnType<
        typeof useApi.useMeProjects
      >,
    );
    sessionStorage.setItem(PROJECT_STORAGE_KEY, PROJECT_ID);

    vi.useFakeTimers();

    render(
      <ProjectProvider>
        <GraphVisualization />
      </ProjectProvider>,
    );
    await vi.advanceTimersByTimeAsync(1000);

    fireEvent.click(screen.getByRole("button", { name: /Layout/i }));
    const gravitySlider = screen.getAllByRole("slider")[0];

    // Drag the slider but do NOT advance through the full debounce
    // window. The pending debounce timer is the danger.
    fireEvent.change(gravitySlider, { target: { value: "2.5" } });
    await vi.advanceTimersByTimeAsync(50);

    // Click Reset before the debounce would fire.
    fireEvent.click(screen.getByRole("button", { name: /Reset to system defaults/i }));

    // The reset mutation itself is a single direct call — that is
    // expected and not the regression we are guarding against.
    const resetCalls = mutate.mock.calls.length;
    expect(resetCalls).toBe(1);
    const resetCall = mutate.mock.calls[0][0] as {
      data: { settings: Record<string, unknown> };
    };
    expect(resetCall.data.settings.graph_center_gravity).toBeUndefined();

    // Advance well past the original 300ms debounce window. If the latch
    // was NOT reset by handleResetLayout, the debounce would fire here
    // and a second mutation would land carrying the default value back
    // as an explicit override.
    await vi.advanceTimersByTimeAsync(1000);

    expect(mutate).toHaveBeenCalledTimes(resetCalls);
  });
});
