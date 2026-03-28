import { useState, useCallback, useMemo, useEffect, useRef } from "react";
import ForceGraph3D, { ForceGraphMethods } from "react-force-graph-3d";
import * as THREE from "three";
import { useMeProjects, useGraph } from "../hooks/useApi";
import { useSelectedProject } from "../context/ProjectContext";
import type { GraphEntity } from "../api/client";

// Color map for entity types — vibrant for dark/holographic background
const ENTITY_TYPE_COLORS: Record<string, { color: string; emissive: string }> = {
  person: { color: "#60a5fa", emissive: "#2563eb" },
  organization: { color: "#4ade80", emissive: "#16a34a" },
  concept: { color: "#fbbf24", emissive: "#d97706" },
  location: { color: "#f472b6", emissive: "#db2777" },
  event: { color: "#818cf8", emissive: "#4f46e5" },
  technology: { color: "#34d399", emissive: "#059669" },
  product: { color: "#fb7185", emissive: "#e11d48" },
  tool: { color: "#a78bfa", emissive: "#7c3aed" },
};

const DEFAULT_TYPE_COLOR = { color: "#6b7280", emissive: "#374151" };

function getTypeColor(entityType: string) {
  return ENTITY_TYPE_COLORS[entityType.toLowerCase()] || DEFAULT_TYPE_COLOR;
}

// Relationship colors
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

function getRelationColor(relation: string) {
  return RELATION_COLORS[relation.toLowerCase()] || "#4b5563";
}

// Graph data types for 3D force graph
interface GraphNode {
  id: string;
  name: string;
  entityType: string;
  mentionCount: number;
  entity: GraphEntity;
  // d3 adds x, y, z at runtime
  x?: number;
  y?: number;
  z?: number;
}

interface GraphLink {
  source: string;
  target: string;
  relation: string;
  weight: number;
  id: string;
}

interface DetailPanelProps {
  entity: GraphEntity;
  connectedEntities: { name: string; relation: string; direction: string }[];
  onClose: () => void;
}

function DetailPanel({ entity, connectedEntities, onClose }: DetailPanelProps) {
  const colors = getTypeColor(entity.entity_type);

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
          style={{ background: colors.emissive + "33", border: `1px solid ${colors.color}` }}
        >
          <div style={{ color: colors.color }} className="font-semibold text-base">
            {entity.name}
          </div>
          <div
            style={{ color: colors.color, opacity: 0.7 }}
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
                className="w-2.5 h-2.5 rounded-full"
                style={{
                  background: colors.color,
                  boxShadow: `0 0 6px ${colors.color}80`,
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

// Create a text sprite for node labels
function createTextSprite(text: string, color: string): THREE.Sprite {
  const canvas = document.createElement("canvas");
  const ctx = canvas.getContext("2d")!;
  const fontSize = 48;
  ctx.font = `${fontSize}px -apple-system, BlinkMacSystemFont, sans-serif`;
  const metrics = ctx.measureText(text);
  const textWidth = metrics.width;

  canvas.width = textWidth + 20;
  canvas.height = fontSize + 16;

  ctx.font = `${fontSize}px -apple-system, BlinkMacSystemFont, sans-serif`;
  ctx.fillStyle = color;
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(text, canvas.width / 2, canvas.height / 2);

  const texture = new THREE.CanvasTexture(canvas);
  texture.needsUpdate = true;

  const spriteMaterial = new THREE.SpriteMaterial({
    map: texture,
    transparent: true,
    depthWrite: false,
  });
  const sprite = new THREE.Sprite(spriteMaterial);
  sprite.scale.set(canvas.width / 12, canvas.height / 12, 1);

  return sprite;
}

function GraphVisualization() {
  const projectsQuery = useMeProjects();
  const { data: projects, isLoading: projectsLoading } = projectsQuery;
  const { selectedProjectId, setSelectedProjectId } = useSelectedProject();
  const [selectedEntity, setSelectedEntity] = useState<GraphEntity | null>(null);
  const [containerSize, setContainerSize] = useState({ width: 800, height: 600 });
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<ForceGraphMethods | undefined>();

  useEffect(() => {
    if (!selectedProjectId && projects && projects.length > 0) {
      setSelectedProjectId(projects[0].id);
    }
  }, [projects, selectedProjectId, setSelectedProjectId]);

  // Resize observer for container
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setContainerSize({
          width: entry.contentRect.width,
          height: entry.contentRect.height,
        });
      }
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const { data: graphData, isLoading: graphLoading, isError: graphError } = useGraph(selectedProjectId);

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

  // Build 3D graph data
  const graph3dData = useMemo(() => {
    if (!graphData?.entities || graphData.entities.length === 0) {
      return { nodes: [] as GraphNode[], links: [] as GraphLink[] };
    }

    const entityIds = new Set(graphData.entities.map((e) => e.id));

    const nodes: GraphNode[] = graphData.entities.map((e) => ({
      id: e.id,
      name: e.name,
      entityType: e.entity_type,
      mentionCount: e.mention_count,
      entity: e,
    }));

    const links: GraphLink[] = (graphData.relationships || [])
      .filter((r) => entityIds.has(r.source_id) && entityIds.has(r.target_id))
      .map((r) => ({
        source: r.source_id,
        target: r.target_id,
        relation: r.relation,
        weight: r.weight,
        id: r.id,
      }));

    return { nodes, links };
  }, [graphData]);

  // Custom node rendering — glowing sphere with label
  const nodeThreeObject = useCallback((node: GraphNode) => {
    const colors = getTypeColor(node.entityType);
    const size = 3 + Math.min(node.mentionCount, 10) * 0.5;

    const group = new THREE.Group();

    // Core sphere
    const geometry = new THREE.SphereGeometry(size, 20, 20);
    const material = new THREE.MeshPhongMaterial({
      color: new THREE.Color(colors.color),
      emissive: new THREE.Color(colors.emissive),
      emissiveIntensity: 0.6,
      shininess: 80,
      transparent: true,
      opacity: 0.9,
    });
    const sphere = new THREE.Mesh(geometry, material);
    group.add(sphere);

    // Outer glow shell
    const glowGeometry = new THREE.SphereGeometry(size * 1.4, 16, 16);
    const glowMaterial = new THREE.MeshBasicMaterial({
      color: new THREE.Color(colors.color),
      transparent: true,
      opacity: 0.12,
      side: THREE.BackSide,
    });
    const glow = new THREE.Mesh(glowGeometry, glowMaterial);
    group.add(glow);

    // Text label
    const label = createTextSprite(node.name, colors.color);
    label.position.set(0, size + 4, 0);
    group.add(label);

    return group;
  }, []);

  const onNodeClick = useCallback(
    (node: GraphNode) => {
      const entity = entityMap.get(node.id);
      if (entity) {
        setSelectedEntity(entity);
      }
      // Fly camera to clicked node
      if (graphRef.current && node.x !== undefined && node.y !== undefined && node.z !== undefined) {
        const distance = 120;
        const distRatio = 1 + distance / Math.hypot(node.x, node.y, node.z || 1);
        graphRef.current.cameraPosition(
          { x: node.x * distRatio, y: node.y * distRatio, z: node.z! * distRatio },
          { x: node.x, y: node.y, z: node.z! },
          1000,
        );
      }
    },
    [entityMap],
  );

  // Configure forces after mount
  useEffect(() => {
    if (!graphRef.current) return;
    const fg = graphRef.current;

    // Moderate charge — enough to not overlap, not so much they fly apart
    const charge = fg.d3Force("charge") as unknown as { strength?: (v: number) => void } | undefined;
    if (charge?.strength) {
      charge.strength(-40);
    }

    // Keep connected nodes at a readable but close distance
    const link = fg.d3Force("link") as unknown as { distance?: (v: number) => void } | undefined;
    if (link?.distance) {
      link.distance(30);
    }
  }, [graph3dData]);

  const isLoading = projectsLoading || (selectedProjectId && graphLoading);

  return (
    <div className="flex flex-col h-full" style={{ height: "calc(100vh - 3rem)" }}>
      <div className="flex items-center justify-between mb-2 shrink-0">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Graph Visualization</h1>
          <p className="mt-0.5 text-sm text-muted-foreground">
            Explore entity relationships in 3D. Drag to rotate, scroll to zoom.
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
          <div ref={containerRef} className="relative flex-1 min-h-0 rounded-lg border border-border overflow-hidden">
            <ForceGraph3D
              ref={graphRef}
              width={containerSize.width}
              height={containerSize.height}
              graphData={graph3dData}
              backgroundColor="#080a12"
              nodeThreeObject={((node: GraphNode) => nodeThreeObject(node)) as any}
              nodeLabel={((node: GraphNode) =>
                `<div style="background:rgba(0,0,0,0.8);padding:6px 10px;border-radius:6px;border:1px solid ${getTypeColor(node.entityType).color};color:#e5e7eb;font-size:12px;">
                  <b style="color:${getTypeColor(node.entityType).color}">${node.name}</b><br/>
                  <span style="opacity:0.7;font-size:10px;text-transform:uppercase">${node.entityType}</span>
                  ${node.mentionCount > 0 ? `<span style="opacity:0.5;font-size:10px"> · ${node.mentionCount} mentions</span>` : ""}
                </div>`
              ) as any}
              onNodeClick={onNodeClick as any}
              linkColor={((link: GraphLink) => getRelationColor(link.relation)) as any}
              linkWidth={((link: GraphLink) => Math.max(0.3, Math.min(link.weight * 0.4, 2))) as any}
              linkOpacity={0.4}
              linkCurvature={0.15}
              linkDirectionalParticles={((link: GraphLink) => link.weight >= 2 ? 2 : 0) as any}
              linkDirectionalParticleSpeed={0.005}
              linkDirectionalParticleWidth={1.5}
              linkDirectionalParticleColor={((link: GraphLink) => getRelationColor(link.relation)) as any}
              linkDirectionalArrowLength={3}
              linkDirectionalArrowRelPos={1}
              linkDirectionalArrowColor={((link: GraphLink) => getRelationColor(link.relation)) as any}
              linkLabel={((link: GraphLink) =>
                `<span style="background:rgba(0,0,0,0.8);padding:3px 8px;border-radius:4px;color:#9ca3af;font-size:11px;">${link.relation}</span>`
              ) as any}
              warmupTicks={40}
              cooldownTime={3000}
              enableNodeDrag={true}
              showNavInfo={false}
            />

            {selectedEntity && (
              <DetailPanel
                entity={selectedEntity}
                connectedEntities={connectedEntities}
                onClose={() => setSelectedEntity(null)}
              />
            )}

            <LegendPanel />

            {/* Navigation hint */}
            <div
              className="absolute bottom-3 right-3 z-10 rounded-md px-3 py-1.5"
              style={{ background: "rgba(0,0,0,0.75)", border: "1px solid #2a2d3a", backdropFilter: "blur(4px)" }}
            >
              <span style={{ color: "#d1d5db", fontSize: "11px", letterSpacing: "0.02em" }}>
                Drag to rotate · Scroll to zoom · Right-drag to pan · Click node to inspect
              </span>
            </div>
          </div>
        )}
    </div>
  );
}

export default GraphVisualization;
