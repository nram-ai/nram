import { useState, useMemo, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useMeProjects, useGraph } from "../hooks/useApi";
import { useSelectedProject } from "../context/ProjectContext";
import type { GraphEntity } from "../api/client";

const ENTITY_TYPE_COLORS: Record<string, { bg: string; border: string; text: string }> = {
  person: { bg: "#1e3a5f", border: "#60a5fa", text: "#bfdbfe" },
  organization: { bg: "#14532d", border: "#4ade80", text: "#bbf7d0" },
  concept: { bg: "#451a03", border: "#fbbf24", text: "#fef3c7" },
  location: { bg: "#4a1942", border: "#f472b6", text: "#fce7f3" },
  event: { bg: "#312e81", border: "#818cf8", text: "#e0e7ff" },
  technology: { bg: "#064e3b", border: "#34d399", text: "#d1fae5" },
  product: { bg: "#4c0519", border: "#fb7185", text: "#ffe4e6" },
  tool: { bg: "#2e1065", border: "#a78bfa", text: "#ede9fe" },
};

const DEFAULT_COLOR = { bg: "#1f2937", border: "#6b7280", text: "#d1d5db" };

function getEntityColor(entityType: string) {
  return ENTITY_TYPE_COLORS[entityType.toLowerCase()] || DEFAULT_COLOR;
}

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

interface DetailPanelProps {
  entity: GraphEntity;
  connections: { name: string; relation: string; direction: string }[];
  onClose: () => void;
}

function DetailPanel({ entity, connections, onClose }: DetailPanelProps) {
  const colors = getEntityColor(entity.entity_type);

  return (
    <div className="fixed inset-y-0 right-0 w-96 bg-card border-l border-border shadow-lg z-50 overflow-y-auto">
      <div className="p-5">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold">Entity Details</h3>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground transition-colors text-lg px-2"
          >
            x
          </button>
        </div>

        <div
          className="rounded-lg p-3 mb-4"
          style={{ background: colors.bg, border: `1px solid ${colors.border}` }}
        >
          <div style={{ color: colors.text }} className="font-semibold text-base">
            {entity.name}
          </div>
          <div
            style={{ color: colors.text, opacity: 0.7 }}
            className="text-xs uppercase tracking-wider mt-1"
          >
            {entity.entity_type}
          </div>
        </div>

        <div className="space-y-3">
          <div>
            <label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
              Canonical Name
            </label>
            <p className="text-sm mt-0.5">{entity.canonical}</p>
          </div>

          <div>
            <label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
              Mention Count
            </label>
            <p className="text-sm mt-0.5">{entity.mention_count}</p>
          </div>

          {(entity.aliases ?? []).length > 0 && (
            <div>
              <label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Aliases
              </label>
              <div className="flex flex-wrap gap-1 mt-1">
                {(entity.aliases ?? []).map((alias, i) => (
                  <span
                    key={i}
                    className="inline-block rounded-full bg-accent px-2 py-0.5 text-xs text-accent-foreground"
                  >
                    {alias}
                  </span>
                ))}
              </div>
            </div>
          )}

          <div className="grid grid-cols-2 gap-2">
            <div>
              <label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                First Seen
              </label>
              <p className="text-xs mt-0.5">{formatDate(entity.created_at)}</p>
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Last Updated
              </label>
              <p className="text-xs mt-0.5">{formatDate(entity.updated_at)}</p>
            </div>
          </div>

          {connections.length > 0 && (
            <div>
              <label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Connected Entities ({connections.length})
              </label>
              <div className="mt-1 space-y-1">
                {connections.map((conn, i) => (
                  <div
                    key={i}
                    className="flex items-center gap-2 text-xs p-1.5 rounded bg-accent/50"
                  >
                    <span className="text-muted-foreground">
                      {conn.direction === "outgoing" ? "->" : "<-"}
                    </span>
                    <span className="font-medium">{conn.name}</span>
                    <span className="text-muted-foreground ml-auto">{conn.relation}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function EntityBrowser() {
  const navigate = useNavigate();
  const projectsQuery = useMeProjects();
  const { data: projects, isLoading: projectsLoading } = projectsQuery;
  const { selectedProjectId, setSelectedProjectId } = useSelectedProject();
  const [searchText, setSearchText] = useState("");
  const [typeFilter, setTypeFilter] = useState<string>("");
  const [selectedEntity, setSelectedEntity] = useState<GraphEntity | null>(null);

  useEffect(() => {
    if (!selectedProjectId && projects && projects.length > 0) {
      setSelectedProjectId(projects[0].id);
    }
  }, [projects, selectedProjectId, setSelectedProjectId]);

  const { data: graphData, isLoading: graphLoading, isError: graphError } = useGraph(selectedProjectId);

  const entityMap = useMemo(() => {
    const map = new Map<string, GraphEntity>();
    if (graphData?.entities) {
      for (const e of graphData.entities) {
        map.set(e.id, e);
      }
    }
    return map;
  }, [graphData]);

  const entityTypes = useMemo(() => {
    if (!graphData?.entities) return [];
    const types = new Set<string>();
    for (const e of graphData.entities) {
      types.add(e.entity_type);
    }
    return Array.from(types).sort();
  }, [graphData]);

  const filteredEntities = useMemo(() => {
    if (!graphData?.entities) return [];
    let entities = graphData.entities;

    if (typeFilter) {
      entities = entities.filter((e) => e.entity_type === typeFilter);
    }

    if (searchText) {
      const lower = searchText.toLowerCase();
      entities = entities.filter(
        (e) =>
          e.name.toLowerCase().includes(lower) ||
          e.canonical.toLowerCase().includes(lower) ||
          (e.aliases ?? []).some((a) => a.toLowerCase().includes(lower)),
      );
    }

    return entities;
  }, [graphData, searchText, typeFilter]);

  const connectionsForEntity = useMemo(() => {
    if (!selectedEntity || !graphData?.relationships) return [];
    const connections: { name: string; relation: string; direction: string }[] = [];
    for (const rel of graphData.relationships) {
      if (rel.source_id === selectedEntity.id) {
        const target = entityMap.get(rel.target_id);
        if (target) {
          connections.push({ name: target.name, relation: rel.relation, direction: "outgoing" });
        }
      } else if (rel.target_id === selectedEntity.id) {
        const source = entityMap.get(rel.source_id);
        if (source) {
          connections.push({ name: source.name, relation: rel.relation, direction: "incoming" });
        }
      }
    }
    return connections;
  }, [selectedEntity, graphData, entityMap]);

  const isLoading = projectsLoading || (selectedProjectId && graphLoading);

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Entity Browser</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Browse and manage extracted entities.
          </p>
        </div>

        <div className="flex items-center gap-3">
          {selectedProjectId && graphData?.entities && graphData.entities.length > 0 && (
            <button
              onClick={() => navigate("/graph")}
              className="rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm hover:bg-accent hover:text-accent-foreground transition-colors"
            >
              View in Graph
            </button>
          )}
          <label className="text-sm font-medium text-muted-foreground">Project:</label>
          <select
            value={selectedProjectId}
            onChange={(e) => {
              setSelectedProjectId(e.target.value);
              setSelectedEntity(null);
              setSearchText("");
              setTypeFilter("");
            }}
            className="rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          >
            <option value="">Select a project</option>
            {projects?.map((project) => (
              <option key={project.id} value={project.id}>
                {project.name}
              </option>
            ))}
          </select>
        </div>
      </div>

      {!selectedProjectId && (
        <div className="flex items-center justify-center rounded-lg border border-dashed border-border bg-accent/30 h-[500px]">
          <div className="text-center">
            <p className="text-muted-foreground text-sm">
              Select a project to browse its entities.
            </p>
          </div>
        </div>
      )}

      {selectedProjectId && isLoading && (
        <div className="flex items-center justify-center rounded-lg border border-border bg-accent/30 h-[500px]">
          <div className="text-center">
            <div className="inline-block h-6 w-6 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
            <p className="mt-2 text-sm text-muted-foreground">Loading entities...</p>
          </div>
        </div>
      )}

      {selectedProjectId && !isLoading && graphError && (
        <div className="flex items-center justify-center rounded-lg border border-red-300 bg-red-50 dark:border-red-800 dark:bg-red-900/30 h-[500px]">
          <div className="text-center">
            <p className="text-sm text-red-800 dark:text-red-300">
              Failed to load entity data. Please try again.
            </p>
          </div>
        </div>
      )}

      {selectedProjectId &&
        !isLoading &&
        !graphError &&
        graphData &&
        (!graphData.entities || graphData.entities.length === 0) && (
          <div className="flex items-center justify-center rounded-lg border border-dashed border-border bg-accent/30 h-[500px]">
            <div className="text-center">
              <p className="text-muted-foreground text-sm">
                No entities found for this project.
              </p>
              <p className="text-muted-foreground text-xs mt-1">
                Entities are created when memories are enriched.
              </p>
            </div>
          </div>
        )}

      {selectedProjectId &&
        !isLoading &&
        !graphError &&
        graphData &&
        graphData.entities &&
        graphData.entities.length > 0 && (
          <>
            <div className="flex items-center gap-3 mb-4">
              <input
                type="text"
                placeholder="Search entities by name or alias..."
                value={searchText}
                onChange={(e) => setSearchText(e.target.value)}
                className="rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring flex-1 max-w-sm"
              />
              <select
                value={typeFilter}
                onChange={(e) => setTypeFilter(e.target.value)}
                className="rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
              >
                <option value="">All types</option>
                {entityTypes.map((type) => (
                  <option key={type} value={type}>
                    {type}
                  </option>
                ))}
              </select>
              <span className="text-sm text-muted-foreground">
                {filteredEntities.length} of {graphData.entities.length} entities
              </span>
            </div>

            <div className="rounded-lg border border-border overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-muted/50">
                    <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Name</th>
                    <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Type</th>
                    <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Mentions</th>
                    <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Aliases</th>
                    <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">First Seen</th>
                    <th className="text-left px-4 py-2.5 font-medium text-muted-foreground">Last Updated</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {filteredEntities.map((entity) => {
                    const colors = getEntityColor(entity.entity_type);
                    return (
                      <tr
                        key={entity.id}
                        onClick={() => setSelectedEntity(entity)}
                        className="hover:bg-accent/50 cursor-pointer transition-colors"
                      >
                        <td className="px-4 py-2.5 font-medium">{entity.name}</td>
                        <td className="px-4 py-2.5">
                          <span
                            className="inline-block rounded-full px-2 py-0.5 text-xs font-medium capitalize"
                            style={{
                              background: colors.bg,
                              color: colors.text,
                              border: `1px solid ${colors.border}`,
                            }}
                          >
                            {entity.entity_type}
                          </span>
                        </td>
                        <td className="px-4 py-2.5 text-muted-foreground">{entity.mention_count}</td>
                        <td className="px-4 py-2.5">
                          {(entity.aliases ?? []).length > 0 ? (
                            <div className="flex flex-wrap gap-1">
                              {(entity.aliases ?? []).map((alias, i) => (
                                <span
                                  key={i}
                                  className="inline-block rounded-full bg-accent px-2 py-0.5 text-xs text-accent-foreground"
                                >
                                  {alias}
                                </span>
                              ))}
                            </div>
                          ) : (
                            <span className="text-muted-foreground">-</span>
                          )}
                        </td>
                        <td className="px-4 py-2.5 text-muted-foreground text-xs">
                          {formatDate(entity.created_at)}
                        </td>
                        <td className="px-4 py-2.5 text-muted-foreground text-xs">
                          {formatDate(entity.updated_at)}
                        </td>
                      </tr>
                    );
                  })}
                  {filteredEntities.length === 0 && (
                    <tr>
                      <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                        No entities match the current filters.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </>
        )}

      {selectedEntity && (
        <DetailPanel
          entity={selectedEntity}
          connections={connectionsForEntity}
          onClose={() => setSelectedEntity(null)}
        />
      )}
    </div>
  );
}

export default EntityBrowser;
