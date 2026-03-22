import { useState, useCallback, useMemo, useEffect } from "react";
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  Node,
  Edge,
  useNodesState,
  useEdgesState,
  NodeProps,
  Handle,
  Position,
  MarkerType,
} from "reactflow";
import "reactflow/dist/style.css";
import { useMeProjects, useGraph } from "../hooks/useApi";
import { useSelectedProject } from "../context/ProjectContext";
import type { GraphEntity } from "../api/client";

// Color map for entity types
const ENTITY_TYPE_COLORS: Record<string, { bg: string; border: string; text: string }> = {
  person: { bg: "#dbeafe", border: "#3b82f6", text: "#1e40af" },
  organization: { bg: "#dcfce7", border: "#22c55e", text: "#166534" },
  concept: { bg: "#fef3c7", border: "#f59e0b", text: "#92400e" },
  location: { bg: "#fce7f3", border: "#ec4899", text: "#9d174d" },
  event: { bg: "#e0e7ff", border: "#6366f1", text: "#3730a3" },
  technology: { bg: "#f0fdf4", border: "#10b981", text: "#065f46" },
  product: { bg: "#fff1f2", border: "#f43f5e", text: "#9f1239" },
  tool: { bg: "#f5f3ff", border: "#8b5cf6", text: "#5b21b6" },
};

const DEFAULT_COLOR = { bg: "#f3f4f6", border: "#9ca3af", text: "#374151" };

function getEntityColor(entityType: string) {
  return ENTITY_TYPE_COLORS[entityType.toLowerCase()] || DEFAULT_COLOR;
}

// Edge color map for relationship types
const RELATION_COLORS: Record<string, string> = {
  works_for: "#3b82f6",
  knows: "#22c55e",
  part_of: "#f59e0b",
  related_to: "#6366f1",
  uses: "#ec4899",
  created_by: "#10b981",
  located_in: "#f43f5e",
  belongs_to: "#8b5cf6",
};

const DEFAULT_EDGE_COLOR = "#9ca3af";

function getRelationColor(relation: string) {
  return RELATION_COLORS[relation.toLowerCase()] || DEFAULT_EDGE_COLOR;
}

// Custom entity node component
function EntityNode({ data }: NodeProps) {
  const colors = getEntityColor(data.entityType);
  const mentionSize = Math.min(Math.max(data.mentionCount, 1), 10);
  const scale = 0.8 + mentionSize * 0.04;

  return (
    <div
      style={{
        background: colors.bg,
        border: `2px solid ${colors.border}`,
        borderRadius: "8px",
        padding: "8px 12px",
        minWidth: "120px",
        maxWidth: "200px",
        transform: `scale(${scale})`,
        cursor: "pointer",
      }}
    >
      <Handle type="target" position={Position.Top} style={{ background: colors.border }} />
      <div style={{ color: colors.text, fontWeight: 600, fontSize: "13px", marginBottom: "2px" }}>
        {data.label}
      </div>
      <div
        style={{
          color: colors.text,
          opacity: 0.7,
          fontSize: "10px",
          textTransform: "uppercase",
          letterSpacing: "0.05em",
        }}
      >
        {data.entityType}
      </div>
      {data.mentionCount > 0 && (
        <div
          style={{
            position: "absolute",
            top: "-8px",
            right: "-8px",
            background: colors.border,
            color: "#fff",
            borderRadius: "9999px",
            width: "20px",
            height: "20px",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontSize: "10px",
            fontWeight: 700,
          }}
        >
          {data.mentionCount}
        </div>
      )}
      <Handle type="source" position={Position.Bottom} style={{ background: colors.border }} />
    </div>
  );
}

const nodeTypes = { entity: EntityNode };

// Force-directed layout: simple spring-based positions
function computeLayout(
  entities: GraphEntity[],
  relationships: { source_id: string; target_id: string }[],
): Record<string, { x: number; y: number }> {
  if (entities.length === 0) return {};

  // Initialize positions in a circle
  const positions: Record<string, { x: number; y: number }> = {};
  const radius = Math.max(200, entities.length * 40);
  const centerX = 400;
  const centerY = 300;

  entities.forEach((e, i) => {
    const angle = (2 * Math.PI * i) / entities.length;
    positions[e.id] = {
      x: centerX + radius * Math.cos(angle),
      y: centerY + radius * Math.sin(angle),
    };
  });

  // Build adjacency for force computation
  const adjacency: Record<string, Set<string>> = {};
  for (const e of entities) {
    adjacency[e.id] = new Set();
  }
  for (const r of relationships) {
    if (adjacency[r.source_id] && adjacency[r.target_id]) {
      adjacency[r.source_id].add(r.target_id);
      adjacency[r.target_id].add(r.source_id);
    }
  }

  // Simple iterative force-directed placement
  const iterations = 50;
  const repulsionStrength = 5000;
  const attractionStrength = 0.01;
  const idealLength = 200;

  for (let iter = 0; iter < iterations; iter++) {
    const forces: Record<string, { fx: number; fy: number }> = {};
    for (const e of entities) {
      forces[e.id] = { fx: 0, fy: 0 };
    }

    // Repulsion between all pairs
    for (let i = 0; i < entities.length; i++) {
      for (let j = i + 1; j < entities.length; j++) {
        const a = entities[i].id;
        const b = entities[j].id;
        const dx = positions[a].x - positions[b].x;
        const dy = positions[a].y - positions[b].y;
        const dist = Math.max(Math.sqrt(dx * dx + dy * dy), 1);
        const force = repulsionStrength / (dist * dist);
        const fx = (dx / dist) * force;
        const fy = (dy / dist) * force;
        forces[a].fx += fx;
        forces[a].fy += fy;
        forces[b].fx -= fx;
        forces[b].fy -= fy;
      }
    }

    // Attraction along edges
    for (const r of relationships) {
      if (!positions[r.source_id] || !positions[r.target_id]) continue;
      const dx = positions[r.target_id].x - positions[r.source_id].x;
      const dy = positions[r.target_id].y - positions[r.source_id].y;
      const dist = Math.max(Math.sqrt(dx * dx + dy * dy), 1);
      const force = attractionStrength * (dist - idealLength);
      const fx = (dx / dist) * force;
      const fy = (dy / dist) * force;
      forces[r.source_id].fx += fx;
      forces[r.source_id].fy += fy;
      forces[r.target_id].fx -= fx;
      forces[r.target_id].fy -= fy;
    }

    // Apply forces with damping
    const damping = 0.9 - iter * 0.015;
    const clampedDamping = Math.max(damping, 0.1);
    for (const e of entities) {
      positions[e.id].x += forces[e.id].fx * clampedDamping;
      positions[e.id].y += forces[e.id].fy * clampedDamping;
    }
  }

  return positions;
}

interface DetailPanelProps {
  entity: GraphEntity;
  connectedEntities: { name: string; relation: string; direction: string }[];
  onClose: () => void;
}

function DetailPanel({ entity, connectedEntities, onClose }: DetailPanelProps) {
  const colors = getEntityColor(entity.entity_type);

  return (
    <div className="absolute right-0 top-0 h-full w-80 bg-card border-l border-border shadow-lg z-50 overflow-y-auto">
      <div className="p-4">
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

          <div>
            <label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
              Mention Count
            </label>
            <p className="text-sm mt-0.5">{entity.mention_count}</p>
          </div>

          <div className="grid grid-cols-2 gap-2">
            <div>
              <label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                First Seen
              </label>
              <p className="text-xs mt-0.5">
                {new Date(entity.created_at).toLocaleDateString()}
              </p>
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Last Seen
              </label>
              <p className="text-xs mt-0.5">
                {new Date(entity.updated_at).toLocaleDateString()}
              </p>
            </div>
          </div>

          {connectedEntities.length > 0 && (
            <div>
              <label className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                Connected Entities ({connectedEntities.length})
              </label>
              <div className="mt-1 space-y-1">
                {connectedEntities.map((conn, i) => (
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

function GraphVisualization() {
  const projectsQuery = useMeProjects();
  const { data: projects, isLoading: projectsLoading } = projectsQuery;
  const { selectedProjectId, setSelectedProjectId } = useSelectedProject();
  const [selectedEntity, setSelectedEntity] = useState<GraphEntity | null>(null);

  useEffect(() => {
    if (!selectedProjectId && projects && projects.length > 0) {
      setSelectedProjectId(projects[0].id);
    }
  }, [projects, selectedProjectId, setSelectedProjectId]);

  const { data: graphData, isLoading: graphLoading, isError: graphError } = useGraph(selectedProjectId);

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  // Build entity lookup map
  const entityMap = useMemo(() => {
    const map = new Map<string, GraphEntity>();
    if (graphData?.entities) {
      for (const e of graphData.entities) {
        map.set(e.id, e);
      }
    }
    return map;
  }, [graphData]);

  // Compute connected entities for the selected entity
  const connectedEntities = useMemo(() => {
    if (!selectedEntity || !graphData?.relationships) return [];

    const connections: { name: string; relation: string; direction: string }[] = [];
    for (const rel of graphData.relationships) {
      if (rel.source_id === selectedEntity.id) {
        const target = entityMap.get(rel.target_id);
        if (target) {
          connections.push({
            name: target.name,
            relation: rel.relation,
            direction: "outgoing",
          });
        }
      } else if (rel.target_id === selectedEntity.id) {
        const source = entityMap.get(rel.source_id);
        if (source) {
          connections.push({
            name: source.name,
            relation: rel.relation,
            direction: "incoming",
          });
        }
      }
    }
    return connections;
  }, [selectedEntity, graphData, entityMap]);

  // Update nodes/edges when graph data changes
  const updateGraph = useCallback(() => {
    if (!graphData) {
      setNodes([]);
      setEdges([]);
      return;
    }

    const entities = graphData.entities || [];
    const relationships = graphData.relationships || [];

    if (entities.length === 0) {
      setNodes([]);
      setEdges([]);
      return;
    }

    const positions = computeLayout(entities, relationships);

    const newNodes: Node[] = entities.map((entity) => ({
      id: entity.id,
      type: "entity",
      position: positions[entity.id] || { x: 0, y: 0 },
      data: {
        label: entity.name,
        entityType: entity.entity_type,
        mentionCount: entity.mention_count,
      },
    }));

    const newEdges: Edge[] = relationships.map((rel) => ({
      id: rel.id,
      source: rel.source_id,
      target: rel.target_id,
      label: rel.relation,
      type: "default",
      animated: false,
      style: { stroke: getRelationColor(rel.relation), strokeWidth: Math.max(1, Math.min(rel.weight, 4)) },
      labelStyle: { fontSize: 10, fill: getRelationColor(rel.relation) },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: getRelationColor(rel.relation),
        width: 15,
        height: 15,
      },
    }));

    setNodes(newNodes);
    setEdges(newEdges);
  }, [graphData, setNodes, setEdges]);

  // Trigger layout when graphData changes
  useMemo(() => {
    updateGraph();
  }, [updateGraph]);

  const onNodeClick = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      const entity = entityMap.get(node.id);
      if (entity) {
        setSelectedEntity(entity);
      }
    },
    [entityMap],
  );

  const isLoading = projectsLoading || (selectedProjectId && graphLoading);

  return (
    <div className="flex flex-col" style={{ height: "calc(100vh - 3rem)" }}>
      <div className="flex items-center justify-between mb-4 shrink-0">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Graph Visualization</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Explore entity relationships visually.
          </p>
        </div>

        <div className="flex items-center gap-3">
          <label className="text-sm font-medium text-muted-foreground">Project:</label>
          <select
            value={selectedProjectId}
            onChange={(e) => {
              setSelectedProjectId(e.target.value);
              setSelectedEntity(null);
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
        <div className="flex flex-1 min-h-0 items-center justify-center rounded-lg border border-dashed border-border bg-accent/30">
          <div className="text-center">
            <p className="text-muted-foreground text-sm">
              Select a project to view its entity relationship graph.
            </p>
          </div>
        </div>
      )}

      {selectedProjectId && isLoading && (
        <div className="flex flex-1 min-h-0 items-center justify-center rounded-lg border border-border bg-accent/30">
          <div className="text-center">
            <div className="inline-block h-6 w-6 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
            <p className="mt-2 text-sm text-muted-foreground">Loading graph data...</p>
          </div>
        </div>
      )}

      {selectedProjectId && !isLoading && graphError && (
        <div className="flex flex-1 min-h-0 items-center justify-center rounded-lg border border-red-300 bg-red-50 dark:border-red-800 dark:bg-red-900/30">
          <div className="text-center">
            <p className="text-sm text-red-800 dark:text-red-300">
              Failed to load graph data. Please try again.
            </p>
          </div>
        </div>
      )}

      {selectedProjectId &&
        !isLoading &&
        !graphError &&
        graphData &&
        (!graphData.entities || graphData.entities.length === 0) && (
          <div className="flex flex-1 min-h-0 items-center justify-center rounded-lg border border-dashed border-border bg-accent/30">
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
          <div className="relative flex-1 min-h-0 rounded-lg border border-border overflow-hidden">
            <ReactFlow
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onNodeClick={onNodeClick}
              nodeTypes={nodeTypes}
              fitView
              fitViewOptions={{ padding: 0.2 }}
              minZoom={0.1}
              maxZoom={2}
              defaultEdgeOptions={{
                type: "default",
              }}
            >
              <Background color="#e5e7eb" gap={20} />
              <Controls />
              <MiniMap
                nodeStrokeColor={(n) => {
                  const type = n.data?.entityType || "";
                  return getEntityColor(type).border;
                }}
                nodeColor={(n) => {
                  const type = n.data?.entityType || "";
                  return getEntityColor(type).bg;
                }}
                maskColor="rgba(0,0,0,0.1)"
              />
            </ReactFlow>

            {selectedEntity && (
              <DetailPanel
                entity={selectedEntity}
                connectedEntities={connectedEntities}
                onClose={() => setSelectedEntity(null)}
              />
            )}

            {/* Legend */}
            <div className="absolute bottom-4 left-4 z-10 bg-card/90 backdrop-blur rounded-lg border border-border p-3 shadow-sm">
              <div className="text-xs font-medium text-muted-foreground mb-2">Entity Types</div>
              <div className="flex flex-wrap gap-2">
                {Object.entries(ENTITY_TYPE_COLORS).map(([type, colors]) => (
                  <div key={type} className="flex items-center gap-1">
                    <div
                      className="w-3 h-3 rounded-sm"
                      style={{ background: colors.bg, border: `1px solid ${colors.border}` }}
                    />
                    <span className="text-xs text-muted-foreground capitalize">{type}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
    </div>
  );
}

export default GraphVisualization;
