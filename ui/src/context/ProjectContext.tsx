import { createContext, useContext, useState, useCallback, type ReactNode } from "react";

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

export interface ProjectContextValue {
  selectedProjectId: string;
  setSelectedProjectId: (id: string) => void;
}

const ProjectContext = createContext<ProjectContextValue | null>(null);

const STORAGE_KEY = "nram_selected_project";

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
