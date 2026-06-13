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
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
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
    useSettingDefaults: vi.fn(),
  };
});

const useMeProjectsMock = vi.mocked(useApi.useMeProjects);
const useGraphMock = vi.mocked(useApi.useGraph);
const useUpdateProjectMock = vi.mocked(useApi.useUpdateProject);
const useSettingDefaultsMock = vi.mocked(useApi.useSettingDefaults);

const PROJECT_ID = "p1";

// Operator-effective layout defaults the component now sources from
// /me/setting-defaults. These are the new system defaults (link/charge at the
// repulsion maximum); a project without overrides renders these.
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

function projectWithoutOverrides() {
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

function reservedProject() {
  return {
    id: PROJECT_ID,
    namespace_id: "ns-global",
    owner_namespace_id: "ns1",
    name: "global",
    slug: "global",
    description: "",
    default_tags: [],
    settings: {},
    reserved: true,
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

// Advance fake timers inside act() so React deterministically flushes the
// state update -> passive effect -> mutate chain that a debounce fires. A bare
// advanceTimersByTimeAsync leaves that flush to the scheduler, which races the
// assertion under parallel-suite CPU load.
async function flushTimers(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
}

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  sessionStorage.clear();
});

beforeEach(() => {
  useSettingDefaultsMock.mockReturnValue(settingDefaultsStub());
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
    await act(async () => {
      await vi.runAllTimersAsync();
    });

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
    await flushTimers(1000);

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
    await flushTimers(1000);

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
    await flushTimers(500);

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
    await flushTimers(1000);

    fireEvent.click(screen.getByRole("button", { name: /Layout/i }));
    const gravitySlider = screen.getAllByRole("slider")[0];

    // Drag the slider but do NOT advance through the full debounce
    // window. The pending debounce timer is the danger.
    fireEvent.change(gravitySlider, { target: { value: "2.5" } });
    await flushTimers(50);

    // Click Reset before the debounce would fire.
    fireEvent.click(screen.getByRole("button", { name: /Reset to system defaults/i }));

    // The reset mutation itself is a single direct call; that is
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
    await flushTimers(1000);

    expect(mutate).toHaveBeenCalledTimes(resetCalls);
  });

  it("renders the operator default for a project without overrides (symptom A: global default applies)", async () => {
    // A project with no per-project overrides must render the operator
    // default sourced from /me/setting-defaults, NOT a hardcoded constant.
    // The store default link distance is 100; the old bug rendered 15.
    useUpdateProjectMock.mockReturnValue({
      mutate: vi.fn(),
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);
    useMeProjectsMock.mockReturnValue(
      queryStub([projectWithoutOverrides()]) as ReturnType<
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
    await flushTimers(1000);

    fireEvent.click(screen.getByRole("button", { name: /Layout/i }));
    const sliders = screen.getAllByRole("slider") as HTMLInputElement[];
    expect(sliders.length).toBe(3);
    // Gravity 0.75, Repulsion 100 (charge -100 shown positive), Link 100.
    expect(sliders[0].value).toBe("0.75");
    expect(sliders[1].value).toBe("100");
    expect(sliders[2].value).toBe("100");
  });

  it("persists a per-project value equal to the OLD hardcoded default (symptom B: it sticks)", async () => {
    // The old resolve-against-constant logic treated link distance 15 as
    // "equal to the default" and dropped the override, so it reverted on
    // reload. Now the default comes from the store (100), so 15 is a real
    // divergence and must persist as an explicit override.
    const mutate = vi.fn();
    useUpdateProjectMock.mockReturnValue({
      mutate,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);
    useMeProjectsMock.mockReturnValue(
      queryStub([projectWithoutOverrides()]) as ReturnType<
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
    await flushTimers(1000);

    fireEvent.click(screen.getByRole("button", { name: /Layout/i }));
    // Sliders: [0] Gravity, [1] Repulsion, [2] Link distance.
    const linkSlider = screen.getAllByRole("slider")[2];
    fireEvent.change(linkSlider, { target: { value: "15" } });
    await flushTimers(500);

    expect(mutate).toHaveBeenCalledTimes(1);
    const call = mutate.mock.calls[0][0] as {
      data: { settings: Record<string, unknown> };
    };
    // The dragged value persists as an override instead of being dropped.
    expect(call.data.settings.graph_link_distance).toBeCloseTo(15, 5);
    // The untouched sliders sit at the system default, so they carry no
    // override (undefined => the project tracks the system default).
    expect(call.data.settings.graph_center_gravity).toBeUndefined();
    expect(call.data.settings.graph_charge_strength).toBeUndefined();
  });

  it("shows the custom badge from the live slider value, before any backend round-trip", async () => {
    // The reported desync: dragging a slider off the default left the panel
    // saying "Using system defaults" with no badge until the full debounce +
    // mutation + refetch landed, because the badge was derived from the
    // persisted value. The mutate mock here is a no-op, so the backend project
    // cache NEVER updates; the badge must still flip purely from local state.
    const mutate = vi.fn();
    useUpdateProjectMock.mockReturnValue({
      mutate,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);
    useMeProjectsMock.mockReturnValue(
      queryStub([projectWithoutOverrides()]) as ReturnType<
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
    await flushTimers(1000);

    fireEvent.click(screen.getByRole("button", { name: /Layout/i }));

    // Baseline: no overrides, the panel reports system defaults, no badge.
    expect(screen.getByText("Using system defaults.")).toBeInTheDocument();
    expect(screen.queryAllByText("custom")).toHaveLength(0);

    // Drag Gravity off the default. No timers advanced, no mutation resolved.
    fireEvent.change(screen.getAllByRole("slider")[0], { target: { value: "2.0" } });

    expect(
      screen.getByText("This project has its own layout overrides."),
    ).toBeInTheDocument();
    expect(screen.queryAllByText("custom").length).toBeGreaterThanOrEqual(1);
    // The label updated with zero backend round-trip.
    expect(mutate).not.toHaveBeenCalled();
  });

  it("clears the custom badge immediately on reset, without waiting for the refetch", async () => {
    // The reported flicker: after Reset the badge lingered next to the default
    // value until the refetch dropped the override from the cache. The mutate
    // mock is a no-op, so the cache keeps the overrides; the badge must clear
    // from the reset (default) slider values alone.
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
    await flushTimers(1000);

    fireEvent.click(screen.getByRole("button", { name: /Layout/i }));

    // All three overrides differ from the system defaults => three badges.
    expect(
      screen.getByText("This project has its own layout overrides."),
    ).toBeInTheDocument();
    expect(screen.getAllByText("custom")).toHaveLength(3);

    fireEvent.click(
      screen.getByRole("button", { name: /Reset to system defaults/i }),
    );

    expect(screen.getByText("Using system defaults.")).toBeInTheDocument();
    expect(screen.queryAllByText("custom")).toHaveLength(0);
  });

  it("drops the per-project override when a slider is dragged back to exactly the system default", async () => {
    // Gravity default is 0.75; the project overrides it to 1.2. Dragging it
    // back to exactly the default must drop the override (target undefined),
    // not persist 0.75 as an explicit override.
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
    await flushTimers(1000);

    fireEvent.click(screen.getByRole("button", { name: /Layout/i }));
    fireEvent.change(screen.getAllByRole("slider")[0], { target: { value: "0.75" } });
    await flushTimers(500);

    expect(mutate).toHaveBeenCalledTimes(1);
    const settings = (
      mutate.mock.calls[0][0] as { data: { settings: Record<string, unknown> } }
    ).data.settings;
    // Dropped, not stored as 0.75.
    expect(settings.graph_center_gravity).toBeUndefined();
    // The untouched overrides survive.
    expect(settings.graph_charge_strength).toBeCloseTo(-42, 5);
    expect(settings.graph_link_distance).toBeCloseTo(30, 5);
  });

  it("hides the Layout control for a reserved project (overrides cannot be saved)", async () => {
    // Reserved projects reject settings writes, so the per-project Layout
    // control must not be offered (its save would 400). The graph still
    // renders them with the system defaults.
    const mutate = vi.fn();
    useUpdateProjectMock.mockReturnValue({
      mutate,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);
    useMeProjectsMock.mockReturnValue(
      queryStub([reservedProject()]) as ReturnType<typeof useApi.useMeProjects>,
    );
    sessionStorage.setItem(PROJECT_STORAGE_KEY, PROJECT_ID);

    vi.useFakeTimers();
    render(
      <ProjectProvider>
        <GraphVisualization />
      </ProjectProvider>,
    );
    await flushTimers(1000);

    // No Layout button is rendered for a reserved project.
    expect(screen.queryByRole("button", { name: /Layout/i })).toBeNull();
    // And nothing was persisted.
    expect(mutate).not.toHaveBeenCalled();
  });
});
