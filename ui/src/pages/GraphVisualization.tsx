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

// Color map for entity types — vibrant, high-contrast for dark backgrounds
const ENTITY_TYPE_COLORS: Record<string, { bg: string; border: string; text: string; glow: string }> = {
  person: { bg: "#1e3a5f", border: "#60a5fa", text: "#bfdbfe", glow: "rgba(96,165,250,0.4)" },
  organization: { bg: "#14532d", border: "#4ade80", text: "#bbf7d0", glow: "rgba(74,222,128,0.4)" },
  concept: { bg: "#451a03", border: "#fbbf24", text: "#fef3c7", glow: "rgba(251,191,36,0.4)" },
  location: { bg: "#4a1942", border: "#f472b6", text: "#fce7f3", glow: "rgba(244,114,182,0.4)" },
  event: { bg: "#312e81", border: "#818cf8", text: "#e0e7ff", glow: "rgba(129,140,248,0.4)" },
  technology: { bg: "#064e3b", border: "#34d399", text: "#d1fae5", glow: "rgba(52,211,153,0.4)" },
  product: { bg: "#4c0519", border: "#fb7185", text: "#ffe4e6", glow: "rgba(251,113,133,0.4)" },
  tool: { bg: "#2e1065", border: "#a78bfa", text: "#ede9fe", glow: "rgba(167,139,250,0.4)" },
};

const DEFAULT_COLOR = { bg: "#1f2937", border: "#6b7280", text: "#d1d5db", glow: "rgba(107,114,128,0.3)" };

function getEntityColor(entityType: string) {
  return ENTITY_TYPE_COLORS[entityType.toLowerCase()] || DEFAULT_COLOR;
}

// Edge color map for relationship types
const RELATION_COLORS: Record<string, string> = {
  works_for: "#60a5fa",
  knows: "#4ade80",
  part_of: "#fbbf24",
  related_to: "#818cf8",
  uses: "#f472b6",
  created_by: "#34d399",
  located_in: "#fb7185",
  belongs_to: "#a78bfa",
};

const DEFAULT_EDGE_COLOR = "#4b5563";

function getRelationColor(relation: string) {
  return RELATION_COLORS[relation.toLowerCase()] || DEFAULT_EDGE_COLOR;
}

// Custom entity node component — neural/sci-fi aesthetic
function EntityNode({ data }: NodeProps) {
  const colors = getEntityColor(data.entityType);
  const mentionSize = Math.min(Math.max(data.mentionCount, 1), 10);
  const scale = 0.85 + mentionSize * 0.03;

  return (
    <div
      style={{
        background: colors.bg,
        border: `1.5px solid ${colors.border}`,
        borderRadius: "6px",
        padding: "6px 10px",
        minWidth: "100px",
        maxWidth: "180px",
        transform: `scale(${scale})`,
        cursor: "pointer",
        boxShadow: `0 0 12px ${colors.glow}, 0 0 4px ${colors.glow}`,
        transition: "box-shadow 0.2s ease",
      }}
    >
      <Handle type="target" position={Position.Top} style={{ background: colors.border, width: 6, height: 6 }} />
      <div style={{ color: colors.text, fontWeight: 600, fontSize: "12px", marginBottom: "1px", lineHeight: 1.3 }}>
        {data.label}
      </div>
      <div
        style={{
          color: colors.text,
          opacity: 0.6,
          fontSize: "9px",
          textTransform: "uppercase",
          letterSpacing: "0.06em",
        }}
      >
        {data.entityType}
      </div>
      {data.mentionCount > 0 && (
        <div
          style={{
            position: "absolute",
            top: "-6px",
            right: "-6px",
            background: colors.border,
            color: colors.bg,
            borderRadius: "9999px",
            width: "18px",
            height: "18px",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontSize: "9px",
            fontWeight: 700,
          }}
        >
          {data.mentionCount}
        </div>
      )}
      <Handle type="source" position={Position.Bottom} style={{ background: colors.border, width: 6, height: 6 }} />
    </div>
  );
}

const nodeTypes = { entity: EntityNode };

// Organic force-directed layout with center gravity and type clustering
function computeLayout(
  entities: GraphEntity[],
  relationships: { source_id: string; target_id: string }[],
): Record<string, { x: number; y: number }> {
  if (entities.length === 0) return {};

  const positions: Record<string, { x: number; y: number }> = {};
  const centerX = 0;
  const centerY = 0;

  // Group entities by type for initial clustering
  const typeGroups: Record<string, GraphEntity[]> = {};
  for (const e of entities) {
    const t = e.entity_type.toLowerCase();
    if (!typeGroups[t]) typeGroups[t] = [];
    typeGroups[t].push(e);
  }

  // Initialize: place type clusters in small arcs near center, with jitter
  const typeKeys = Object.keys(typeGroups);
  const clusterRadius = Math.min(150, 40 + entities.length * 3);
  typeKeys.forEach((type, ti) => {
    const clusterAngle = (2 * Math.PI * ti) / typeKeys.length;
    const cx = centerX + clusterRadius * Math.cos(clusterAngle);
    const cy = centerY + clusterRadius * Math.sin(clusterAngle);
    const group = typeGroups[type];
    group.forEach((e, ei) => {
      const spread = Math.min(80, 20 + group.length * 8);
      const localAngle = (2 * Math.PI * ei) / group.length;
      positions[e.id] = {
        x: cx + spread * Math.cos(localAngle) + (Math.random() - 0.5) * 20,
        y: cy + spread * Math.sin(localAngle) + (Math.random() - 0.5) * 20,
      };
    });
  });

  // Build adjacency
  const adjacency: Record<string, Set<string>> = {};
  for (const e of entities) adjacency[e.id] = new Set();
  for (const r of relationships) {
    if (adjacency[r.source_id] && adjacency[r.target_id]) {
      adjacency[r.source_id].add(r.target_id);
      adjacency[r.target_id].add(r.source_id);
    }
  }

  // Degree map for weighting
  const degree: Record<string, number> = {};
  for (const e of entities) degree[e.id] = adjacency[e.id].size;

  // Force simulation — tighter, more organic
  const iterations = 120;
  const repulsionBase = 2000;
  const attractionStrength = 0.06;
  const idealLength = 120;
  const centerGravity = 0.01;
  const typeAttractionStrength = 0.002;
  const maxDisplacement = 50;

  // Precompute type centers for type-clustering force
  function computeTypeCenters() {
    const centers: Record<string, { x: number; y: number; count: number }> = {};
    for (const e of entities) {
      const t = e.entity_type.toLowerCase();
      if (!centers[t]) centers[t] = { x: 0, y: 0, count: 0 };
      centers[t].x += positions[e.id].x;
      centers[t].y += positions[e.id].y;
      centers[t].count++;
    }
    for (const t of Object.keys(centers)) {
      centers[t].x /= centers[t].count;
      centers[t].y /= centers[t].count;
    }
    return centers;
  }

  for (let iter = 0; iter < iterations; iter++) {
    const temp = 1.0 - iter / iterations; // temperature annealing
    const forces: Record<string, { fx: number; fy: number }> = {};
    for (const e of entities) forces[e.id] = { fx: 0, fy: 0 };

    // Repulsion between all pairs (scaled by temperature)
    const repulsion = repulsionBase * (0.3 + 0.7 * temp);
    for (let i = 0; i < entities.length; i++) {
      for (let j = i + 1; j < entities.length; j++) {
        const a = entities[i].id;
        const b = entities[j].id;
        const dx = positions[a].x - positions[b].x;
        const dy = positions[a].y - positions[b].y;
        const distSq = dx * dx + dy * dy;
        const dist = Math.max(Math.sqrt(distSq), 1);
        const force = repulsion / distSq;
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

    // Center gravity — pulls everything toward origin
    for (const e of entities) {
      const dx = centerX - positions[e.id].x;
      const dy = centerY - positions[e.id].y;
      forces[e.id].fx += dx * centerGravity;
      forces[e.id].fy += dy * centerGravity;
    }

    // Type clustering — gentle pull toward type centroid
    if (iter % 5 === 0 || iter < 10) {
      const typeCenters = computeTypeCenters();
      for (const e of entities) {
        const t = e.entity_type.toLowerCase();
        const tc = typeCenters[t];
        if (tc && tc.count > 1) {
          const dx = tc.x - positions[e.id].x;
          const dy = tc.y - positions[e.id].y;
          forces[e.id].fx += dx * typeAttractionStrength;
          forces[e.id].fy += dy * typeAttractionStrength;
        }
      }
    }

    // Apply forces with temperature-based displacement cap
    const cap = maxDisplacement * temp;
    for (const e of entities) {
      let dx = forces[e.id].fx;
      let dy = forces[e.id].fy;
      const mag = Math.sqrt(dx * dx + dy * dy);
      if (mag > cap && mag > 0) {
        dx = (dx / mag) * cap;
        dy = (dy / mag) * cap;
      }
      positions[e.id].x += dx;
      positions[e.id].y += dy;
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

function LegendPanel() {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <div
      className="absolute top-3 left-3 z-10 rounded-lg shadow-lg"
      style={{ background: "rgba(15,17,23,0.85)", border: "1px solid #1e2030", backdropFilter: "blur(8px)" }}
    >
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="flex items-center gap-2 px-3 py-2 w-full text-left"
        style={{ color: "#9ca3af", fontSize: "11px", fontWeight: 500 }}
      >
        <span style={{ fontSize: "13px" }}>{collapsed ? "+" : "-"}</span>
        Entity Types
      </button>
      {!collapsed && (
        <div className="px-3 pb-2 grid grid-cols-2 gap-x-4 gap-y-1">
          {Object.entries(ENTITY_TYPE_COLORS).map(([type, colors]) => (
            <div key={type} className="flex items-center gap-1.5">
              <div
                className="w-2.5 h-2.5 rounded-sm"
                style={{
                  background: colors.border,
                  boxShadow: `0 0 6px ${colors.glow}`,
                }}
              />
              <span style={{ color: "#9ca3af", fontSize: "10px", textTransform: "capitalize" }}>{type}</span>
            </div>
          ))}
        </div>
      )}
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

    const newEdges: Edge[] = relationships.map((rel) => {
      const color = getRelationColor(rel.relation);
      return {
        id: rel.id,
        source: rel.source_id,
        target: rel.target_id,
        label: rel.relation,
        type: "smoothstep",
        animated: rel.weight >= 3,
        style: {
          stroke: color,
          strokeWidth: Math.max(0.8, Math.min(rel.weight * 0.8, 3)),
          opacity: 0.6,
        },
        labelStyle: { fontSize: 9, fill: color, opacity: 0.8 },
        labelBgStyle: { fill: "#0f1117", fillOpacity: 0.8 },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: color,
          width: 12,
          height: 12,
        },
      };
    });

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
          <div className="relative flex-1 min-h-0 rounded-lg border border-border overflow-hidden" style={{ background: "#0f1117" }}>
            <ReactFlow
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onNodeClick={onNodeClick}
              nodeTypes={nodeTypes}
              fitView
              fitViewOptions={{ padding: 0.3, maxZoom: 1.2 }}
              minZoom={0.05}
              maxZoom={3}
              defaultEdgeOptions={{
                type: "smoothstep",
              }}
            >
              <Background color="#1a1d2e" gap={30} size={1} />
              <Controls
                position="bottom-right"
                style={{ marginBottom: 10, marginRight: 10 }}
              />
              <MiniMap
                position="bottom-left"
                pannable
                zoomable
                nodeStrokeColor={(n) => {
                  const type = n.data?.entityType || "";
                  return getEntityColor(type).border;
                }}
                nodeColor={(n) => {
                  const type = n.data?.entityType || "";
                  return getEntityColor(type).bg;
                }}
                maskColor="rgba(0,0,0,0.3)"
                style={{ background: "#0d0f16", border: "1px solid #1e2030" }}
              />
            </ReactFlow>

            {selectedEntity && (
              <DetailPanel
                entity={selectedEntity}
                connectedEntities={connectedEntities}
                onClose={() => setSelectedEntity(null)}
              />
            )}

            {/* Legend — collapsible, top-left to avoid overlap with controls */}
            <LegendPanel />
          </div>
        )}
    </div>
  );
}

export default GraphVisualization;
