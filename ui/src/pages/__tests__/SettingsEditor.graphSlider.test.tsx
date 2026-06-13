/**
 * @vitest-environment happy-dom
 *
 * Unit test for the admin SettingsEditor graph-layout slider, focused on the
 * charge_strength "Repulsion" sign-flip: the stored value is negative
 * (-100..0) but the operator sees a positive Repulsion slider (0..100), and a
 * drag must persist the negated value. Also covers a plain (non-flipped)
 * layout key to confirm it round-trips unchanged.
 */
import { describe, it, expect, vi, afterEach } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";

import { GraphLayoutSliderEditor } from "../SettingsEditor";
import type { SettingSchema } from "../../api/client";

function chargeSchema(): SettingSchema {
  return {
    key: "graph.charge_strength",
    type: "number",
    default_value: -100,
    description: "spacing force",
    category: "graph_visualization",
    min: -100,
    max: 0,
    step: 1,
  };
}

function linkSchema(): SettingSchema {
  return {
    key: "graph.link_distance",
    type: "number",
    default_value: 100,
    description: "edge length",
    category: "graph_visualization",
    min: 5,
    max: 100,
    step: 1,
  };
}

// Advance fake timers inside act() so React deterministically flushes the
// useDebounce state update -> onSave chain before the assertion, rather than
// leaving the flush to race the assertion under parallel-suite load.
async function flushTimers(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
}

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("GraphLayoutSliderEditor", () => {
  it("shows charge_strength as a positive Repulsion value", () => {
    render(
      <GraphLayoutSliderEditor
        schema={chargeSchema()}
        currentValue={-100}
        label="Repulsion"
        signFlip
        onSave={vi.fn()}
      />,
    );
    const slider = screen.getByRole("slider") as HTMLInputElement;
    // Stored -100 presents as +100 on the slider; range runs 0..100.
    expect(slider.value).toBe("100");
    expect(slider.min).toBe("0");
    expect(slider.max).toBe("100");
  });

  it("persists the negated value when the Repulsion slider is dragged", async () => {
    vi.useFakeTimers();
    const onSave = vi.fn();
    render(
      <GraphLayoutSliderEditor
        schema={chargeSchema()}
        currentValue={-100}
        label="Repulsion"
        signFlip
        onSave={onSave}
      />,
    );
    const slider = screen.getByRole("slider");
    // Operator drags Repulsion down to 60; stored charge must become -60.
    fireEvent.change(slider, { target: { value: "60" } });
    await flushTimers(500);

    expect(onSave).toHaveBeenCalledTimes(1);
    expect(onSave).toHaveBeenCalledWith("graph.charge_strength", -60);
  });

  it("round-trips a non-flipped layout key unchanged", async () => {
    vi.useFakeTimers();
    const onSave = vi.fn();
    render(
      <GraphLayoutSliderEditor
        schema={linkSchema()}
        currentValue={100}
        label="Link distance"
        signFlip={false}
        onSave={onSave}
      />,
    );
    const slider = screen.getByRole("slider") as HTMLInputElement;
    expect(slider.value).toBe("100");
    fireEvent.change(slider, { target: { value: "30" } });
    await flushTimers(500);

    expect(onSave).toHaveBeenCalledTimes(1);
    expect(onSave).toHaveBeenCalledWith("graph.link_distance", 30);
  });

  it("does not persist on mount without an interaction", async () => {
    vi.useFakeTimers();
    const onSave = vi.fn();
    render(
      <GraphLayoutSliderEditor
        schema={chargeSchema()}
        currentValue={-100}
        label="Repulsion"
        signFlip
        onSave={onSave}
      />,
    );
    await flushTimers(1000);
    expect(onSave).not.toHaveBeenCalled();
  });
});
