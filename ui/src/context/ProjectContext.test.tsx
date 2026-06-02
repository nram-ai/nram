/**
 * Unit tests for useEnsureValidSelectedProject -- the shared defaulting /
 * self-healing logic behind the project selector on the Entity Browser,
 * Memory Browser, and Graph pages. Runs in happy-dom so sessionStorage and
 * React effects are available without a server.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { renderHook } from "@testing-library/react";
import type { Project } from "../api/client";
import {
  ProjectProvider,
  STORAGE_KEY,
  useSelectedProject,
  useEnsureValidSelectedProject,
} from "./ProjectContext";

// The hook only reads id and slug; cast minimal stubs rather than fabricate a
// full Project (settings, timestamps, counts, etc.).
function project(id: string, slug: string): Project {
  return { id, name: slug, slug } as unknown as Project;
}

function renderEnsure(projects: Project[] | undefined) {
  return renderHook(
    () => {
      useEnsureValidSelectedProject(projects);
      return useSelectedProject().selectedProjectId;
    },
    { wrapper: ProjectProvider },
  );
}

describe("useEnsureValidSelectedProject", () => {
  beforeEach(() => {
    sessionStorage.clear();
  });

  it("defaults an empty selection to the global project, not the first by name", () => {
    const projects = [project("id-nram", "nram"), project("id-global", "global")];
    const { result } = renderEnsure(projects);
    expect(result.current).toBe("id-global");
  });

  it("falls back to the first project when no global project exists", () => {
    const projects = [project("id-alpha", "alpha"), project("id-beta", "beta")];
    const { result } = renderEnsure(projects);
    expect(result.current).toBe("id-alpha");
  });

  it("self-heals a stale persisted id absent from the list back to global", () => {
    sessionStorage.setItem(STORAGE_KEY, "stale-deleted-id");
    const projects = [project("id-nram", "nram"), project("id-global", "global")];
    const { result } = renderEnsure(projects);
    expect(result.current).toBe("id-global");
  });

  it("leaves a valid persisted selection unchanged", () => {
    sessionStorage.setItem(STORAGE_KEY, "id-nram");
    const projects = [project("id-nram", "nram"), project("id-global", "global")];
    const { result } = renderEnsure(projects);
    expect(result.current).toBe("id-nram");
  });

  it("does not clobber a persisted selection while the list is still loading", () => {
    sessionStorage.setItem(STORAGE_KEY, "id-persisted");
    const { result } = renderEnsure(undefined);
    expect(result.current).toBe("id-persisted");
  });
});
