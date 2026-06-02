import {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  type ReactNode,
} from "react";
import type { Project } from "../api/client";

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

export interface ProjectContextValue {
  selectedProjectId: string;
  setSelectedProjectId: (id: string) => void;
}

const ProjectContext = createContext<ProjectContextValue | null>(null);

export const STORAGE_KEY = "nram_selected_project";

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

export function ProjectProvider({ children }: { children: ReactNode }) {
  const [selectedProjectId, setSelectedProjectIdRaw] = useState<string>(
    () => sessionStorage.getItem(STORAGE_KEY) ?? "",
  );

  const setSelectedProjectId = useCallback((id: string) => {
    setSelectedProjectIdRaw(id);
    if (id) {
      sessionStorage.setItem(STORAGE_KEY, id);
    } else {
      sessionStorage.removeItem(STORAGE_KEY);
    }
  }, []);

  return (
    <ProjectContext.Provider value={{ selectedProjectId, setSelectedProjectId }}>
      {children}
    </ProjectContext.Provider>
  );
}

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

export function useSelectedProject(): ProjectContextValue {
  const ctx = useContext(ProjectContext);
  if (!ctx) {
    throw new Error("useSelectedProject must be used within a ProjectProvider");
  }
  return ctx;
}

// useEnsureValidSelectedProject keeps the shared selection pointed at a project
// that actually exists in the given list. It defaults an empty selection to the
// "global" project (auto-created for every user; falls back to the first
// project) and, critically, re-defaults a stale/invalid persisted id — one left
// over from a prior login, a deleted project, or another tenant — which would
// otherwise drive the project-scoped views into a failed fetch. Pass the list
// from useMeProjects; it no-ops while the list is still loading so a valid
// persisted selection is never clobbered mid-load.
export function useEnsureValidSelectedProject(projects: Project[] | undefined) {
  const { selectedProjectId, setSelectedProjectId } = useSelectedProject();
  useEffect(() => {
    if (!projects?.length) return;
    if (projects.some((p) => p.id === selectedProjectId)) return;
    const fallback = projects.find((p) => p.slug === "global") ?? projects[0];
    setSelectedProjectId(fallback.id);
  }, [projects, selectedProjectId, setSelectedProjectId]);
}
