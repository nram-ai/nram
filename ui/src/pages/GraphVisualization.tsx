import { useState, useCallback, useMemo, useEffect, useRef } from "react";
import ForceGraph3D, { ForceGraphMethods } from "react-force-graph-3d";
import * as THREE from "three";
import {
  useMeProjects,
  useGraph,
  useUpdateProject,
  useSettingDefaults,
} from "../hooks/useApi";
import { useDebounce } from "../hooks/useDebounce";
import { useSelectedProject, useEnsureValidSelectedProject } from "../context/ProjectContext";
import { SliderRow, type SliderSpec } from "../components/LayoutSlider";
import type { GraphEntity, Project, ProjectSettings, MeSettingDefault } from "../api/client";

// Setting keys for the d3-force layout defaults. The effective values come
// from the operator-configured settings store via /me/setting-defaults; never
// hardcode the default values here.
const GRAVITY_KEY = "graph.center_gravity";
const CHARGE_KEY = "graph.charge_strength";
const LINK_DISTANCE_KEY = "graph.link_distance";

// Range (min/max/step) fallbacks used only as a bootstrap while
// /me/setting-defaults is still loading; the live ranges come from the same
// endpoint. These are control bounds, not default values.
const GRAVITY_RANGE_FALLBACK = { min: 0, max: 3, step: 0.05 };
const CHARGE_RANGE_FALLBACK = { min: -100, max: 0, step: 1 };
const LINK_DISTANCE_RANGE_FALLBACK = { min: 5, max: 100, step: 1 };

// Debounce window for persisting slider changes; held longer (DIRTY_GUARD_MS)
// after each drag so React Query refetches from our own writes don't clobber
// in-flight values.
const PERSIST_DEBOUNCE_MS = 300;
const DIRTY_GUARD_MS = 1500;

// Tolerance for treating a slider value as equal to the system default. Slider
// values are discrete, step-aligned floats, so this only absorbs binary
// floating-point noise. Drives both the "custom" badge and the drop-override
// persistence decision so the two never disagree.
const LAYOUT_EPSILON = 1e-9;

// A layout knob counts as a per-project override when its value differs from
// the system default by more than the float tolerance. Single source of truth
// for both the "custom" badge and the drop-override persistence decision, so
// the two never disagree. Returns false until the default has loaded.
function isLayoutOverride(value: number, storeDefault: number | undefined): boolean {
  return storeDefault !== undefined && Math.abs(value - storeDefault) > LAYOUT_EPSILON;
}

// Above this node count the per-frame simulation reheat during a slider drag
// is too expensive (every frame restarts alpha at full, so the sim never
// cools). Below it the live morph is cheap and kept; at or above it the graph
// re-lays-out once on release instead. Adjustable.
const LIVE_LAYOUT_NODE_LIMIT = 1000;

// resolveLayoutValue prefers a per-project override; otherwise it falls back to
// the operator default sourced from the settings store. storeDefault is
// undefined until /me/setting-defaults loads, so the result is likewise
// undefined during that window and callers must defer applying it (see
// defaultsLoaded).
function resolveLayoutValue(
  override: number | undefined,
  storeDefault: number | undefined,
): number | undefined {
  if (typeof override === "number" && Number.isFinite(override)) {
    return override;
  }
  return storeDefault;
}

// resolveRange reads the operator-tunable {min,max,step} from a setting-default
// row, falling back to the supplied bootstrap bounds per field while the
// endpoint is still loading or a constraint is unset.
function resolveRange(
  entry: MeSettingDefault | undefined,
  fallback: { min: number; max: number; step: number },
): { min: number; max: number; step: number } {
  if (!entry) return fallback;
  return {
    min: typeof entry.min === "number" && Number.isFinite(entry.min) ? entry.min : fallback.min,
    max: typeof entry.max === "number" && Number.isFinite(entry.max) ? entry.max : fallback.max,
    step:
      typeof entry.step === "number" && Number.isFinite(entry.step) && entry.step > 0
        ? entry.step
        : fallback.step,
  };
}

// Retoned to live in the cyan-blue luminance band of the neural-network
// backdrop. Categorical hue distinctions preserved; saturation pulled
// inward so the graph view reads as "zooming into the constellation"
// rather than a separate widget.
const ENTITY_TYPE_COLORS: Record<string, { color: string; emissive: string }> = {
  person: { color: "#5C8FDA", emissive: "#3B6CB8" },
  organization: { color: "#4CC8E8", emissive: "#2A9AC2" },
  concept: { color: "#7CCEF8", emissive: "#3FA9DC" },
  location: { color: "#4FCFA0", emissive: "#2F9D75" },
  event: { color: "#EFB868", emissive: "#C28734" },
  technology: { color: "#4CD9C7", emissive: "#2BAA9A" },
  product: { color: "#B580E1", emissive: "#8554B6" },
  tool: { color: "#809DEC", emissive: "#5872C5" },
};

const DEFAULT_TYPE_COLOR = { color: "#5A6884", emissive: "#3A465D" };

function getTypeColor(entityType: string) {
  return ENTITY_TYPE_COLORS[entityType.toLowerCase()] || DEFAULT_TYPE_COLOR;
}

// Edge labels are free-form, canonicalized relations (model.CanonicalRelation:
// lowercase, space-separated) drawn from thousands of distinct phrases, so no
// fixed relation->color table can cover them. Derive a stable, decorative hue
// from the relation string itself: the same relation always renders the same
// color and distinct relations spread across the wheel. Saturation and
// lightness are pinned so edges stay in the muted register of the backdrop
// rather than reading as a rainbow; the exact relation is revealed on hover
// via linkLabel, so no legend entry is implied.
export function getRelationColor(relation: string): string {
  if (!relation) return "#4b5563";
  // FNV-1a (32-bit) over the lowercased relation; map the hash to a hue.
  // Lowercasing mirrors the backend canonicalization so casing never splits
  // a color.
  let h = 0x811c9dc5;
  const s = relation.toLowerCase();
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  const hue = (h >>> 0) % 360;
  return `hsl(${hue}, 62%, 65%)`;
}

// Directional-particle "pulse" tier from relationship weight (stored capped at
// 2.0). Stronger relationships pulse with more, faster particles; below the
// floor they don't pulse at all. Precomputed onto each link so the ForceGraph
// particle accessors stay O(1). Lower band bound is inclusive.
export function getPulse(weight: number): { count: number; speed: number } {
  if (weight < 0.5) return { count: 0, speed: 0 };
  if (weight < 1.0) return { count: 1, speed: 0.003 };
  if (weight < 1.5) return { count: 2, speed: 0.006 };
  return { count: 3, speed: 0.010 };
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
  // Decorative hashed color, computed once when links are built so the three
  // ForceGraph color accessors (line, particles, arrow) read it instead of
  // re-hashing the relation on every link every frame.
  color: string;
  // Directional-particle pulse tier derived from weight (getPulse), precomputed
  // here so the count/speed accessors stay O(1).
  particleCount: number;
  particleSpeed: number;
  weight: number;
  id: string;
}

interface DetailPanelProps {
  entity: GraphEntity;
  connectedEntities: { name: string; relation: string; direction: string }[];
  truncated: boolean;
  onClose: () => void;
}

function DetailPanel({ entity, connectedEntities, truncated, onClose }: DetailPanelProps) {
  const colors = getTypeColor(entity.entity_type);

  return (
    <div className="absolute right-0 top-0 h-full w-full sm:w-80 bg-card border-l border-border shadow-lg z-50 overflow-y-auto">
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
                Connected Entities ({connectedEntities.length}){truncated && " - subset"}
              </label>
              {truncated && (
                <p className="text-xs text-amber-700 dark:text-amber-300 mt-0.5">
                  Graph is truncated; this entity may have additional connections not shown.
                </p>
              )}
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

interface LayoutDrawerProps {
  sliders: SliderSpec[];
  onReset: () => void;
  onClose: () => void;
}

function LayoutDrawer({ sliders, onReset, onClose }: LayoutDrawerProps) {
  const anyOverride = sliders.some((s) => s.isOverride);

  return (
    <div className="absolute right-0 top-0 h-full w-full sm:w-80 bg-card border-l border-border shadow-lg z-40 overflow-y-auto">
      <div className="p-4">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold">Layout</h3>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground transition-colors text-lg px-2"
            aria-label="Close layout panel"
          >
            x
          </button>
        </div>

        <p className="text-xs text-muted-foreground mb-4">
          Per-project tuning of the d3-force layout. Drag to update live; values
          save automatically.
        </p>

        <div className="space-y-5">
          {sliders.map((s) => (
            <SliderRow key={s.label} spec={s} />
          ))}
        </div>

        <div className="mt-6 pt-4 border-t border-border">
          <button
            onClick={onReset}
            disabled={!anyOverride}
            className="w-full px-3 py-1.5 rounded-md text-sm border border-input bg-background hover:bg-accent disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            Reset to system defaults
          </button>
          <p className="mt-2 text-xs text-muted-foreground">
            {anyOverride
              ? "This project has its own layout overrides."
              : "Using system defaults."}
          </p>
        </div>
      </div>
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
  const [containerSize, setContainerSize] = useState<{ width: number; height: number } | null>(null);
  const [layoutDrawerOpen, setLayoutDrawerOpen] = useState(false);
  const observerRef = useRef<ResizeObserver | null>(null);
  const graphRef = useRef<ForceGraphMethods | undefined>();

  // Operator-effective layout defaults + control ranges, sourced from the
  // settings store via the self-tier endpoint (works for non-admin owners too,
  // unlike the admin-only /admin/settings schema). Until this loads the
  // default values are undefined and we defer applying them.
  const settingDefaults = useSettingDefaults();
  const gravityDefault = settingDefaults.byKey[GRAVITY_KEY]?.value;
  const chargeDefault = settingDefaults.byKey[CHARGE_KEY]?.value;
  const linkDistanceDefault = settingDefaults.byKey[LINK_DISTANCE_KEY]?.value;
  const defaultsLoaded =
    typeof gravityDefault === "number" &&
    typeof chargeDefault === "number" &&
    typeof linkDistanceDefault === "number";

  const gravityRange = useMemo(
    () => resolveRange(settingDefaults.byKey[GRAVITY_KEY], GRAVITY_RANGE_FALLBACK),
    [settingDefaults.byKey],
  );
  const chargeRange = useMemo(
    () => resolveRange(settingDefaults.byKey[CHARGE_KEY], CHARGE_RANGE_FALLBACK),
    [settingDefaults.byKey],
  );
  const linkDistanceRange = useMemo(
    () => resolveRange(settingDefaults.byKey[LINK_DISTANCE_KEY], LINK_DISTANCE_RANGE_FALLBACK),
    [settingDefaults.byKey],
  );
  // Repulsion is the UI-positive presentation of charge; flip min/max signs.
  const repulsionRange = useMemo(
    () => ({
      min: -chargeRange.max,
      max: -chargeRange.min,
      step: chargeRange.step,
    }),
    [chargeRange.min, chargeRange.max, chargeRange.step],
  );

  const currentProject: Project | undefined = useMemo(
    () => projects?.find((p) => p.id === selectedProjectId),
    [projects, selectedProjectId],
  );
  const projectSettings: ProjectSettings | undefined = currentProject?.settings;
  // Reserved projects (global, about_me) are managed by nram and reject
  // settings writes (PUT /me/projects/{id} returns 400), so a per-project
  // layout override cannot be saved for them. Hide the Layout control entirely
  // rather than offer a slider whose save silently fails; the graph still
  // renders these projects with the system defaults.
  const isReservedProject = currentProject?.reserved === true;

  const resolvedGravity = resolveLayoutValue(
    projectSettings?.graph_center_gravity,
    gravityDefault,
  );
  const resolvedCharge = resolveLayoutValue(
    projectSettings?.graph_charge_strength,
    chargeDefault,
  );
  const resolvedLinkDistance = resolveLayoutValue(
    projectSettings?.graph_link_distance,
    linkDistanceDefault,
  );

  // Seed at 0 before the store defaults load; the sync effect below replaces
  // these the moment resolved values become defined (and forces are not
  // applied until defaultsLoaded, so the seed is never rendered).
  const [gravity, setGravity] = useState(resolvedGravity ?? 0);
  const [charge, setCharge] = useState(resolvedCharge ?? 0);
  const [linkDistance, setLinkDistance] = useState(resolvedLinkDistance ?? 0);

  // dirtyUntilRef holds off backend->local sync while the user is dragging,
  // so React Query refetches from our own writes don't clobber in-flight
  // values. Project switches always force-sync to avoid carrying the
  // previous project's slider values into a different project's view.
  const dirtyUntilRef = useRef<number>(0);
  const lastSyncedProjectRef = useRef<string | null>(null);
  // userEditedRef latches true the first time the user touches a slider in
  // the current project. The persist effect refuses to fire until this is
  // true, so the stale useDebounce seed (which always starts at the cascade
  // default before projects have loaded) cannot clobber a just-hydrated
  // override with the default value.
  const userEditedRef = useRef(false);
  useEffect(() => {
    // Defer until the store defaults have loaded; otherwise resolved values
    // (for a project without overrides) are undefined and there is nothing
    // meaningful to seed yet.
    if (
      resolvedGravity === undefined ||
      resolvedCharge === undefined ||
      resolvedLinkDistance === undefined
    ) {
      return;
    }
    const projectChanged = lastSyncedProjectRef.current !== selectedProjectId;
    if (!projectChanged && Date.now() < dirtyUntilRef.current) return;
    if (projectChanged) {
      dirtyUntilRef.current = 0;
      lastSyncedProjectRef.current = selectedProjectId;
      userEditedRef.current = false;
    }
    setGravity(resolvedGravity);
    setCharge(resolvedCharge);
    setLinkDistance(resolvedLinkDistance);
  }, [
    selectedProjectId,
    resolvedGravity,
    resolvedCharge,
    resolvedLinkDistance,
  ]);

  const debouncedGravity = useDebounce(gravity, PERSIST_DEBOUNCE_MS);
  const debouncedCharge = useDebounce(charge, PERSIST_DEBOUNCE_MS);
  const debouncedLinkDistance = useDebounce(linkDistance, PERSIST_DEBOUNCE_MS);

  const updateProjectMut = useUpdateProject();

  // Persist when debounced values diverge from the cascade-resolved value
  // (or when an override already exists). Sending undefined for a field
  // drops the override and falls back to the system default. Gated on
  // userEditedRef so that a project hydration (which flips currentProject?.id
  // from undefined to defined while debounced values still hold the useState
  // seed) cannot trigger a write of the stale seed back to the backend.
  useEffect(() => {
    if (!currentProject) return;
    if (!userEditedRef.current) return;
    // Reserved projects reject settings writes; never attempt a save for them
    // (the Layout control is hidden, so this is purely defensive).
    if (isReservedProject) return;
    // The store defaults gate the drop-override comparison below; without them
    // we cannot tell whether the slider equals the default. Persist only once
    // they are known (isLayoutOverride is false while a default is undefined).
    if (!defaultsLoaded) return;
    const fields = [
      { debounced: debouncedGravity, def: gravityDefault, stored: projectSettings?.graph_center_gravity, key: "graph_center_gravity" as const },
      { debounced: debouncedCharge, def: chargeDefault, stored: projectSettings?.graph_charge_strength, key: "graph_charge_strength" as const },
      { debounced: debouncedLinkDistance, def: linkDistanceDefault, stored: projectSettings?.graph_link_distance, key: "graph_link_distance" as const },
    ];

    const targets: Partial<Record<typeof fields[number]["key"], number | undefined>> = {};
    let changed = false;
    for (const f of fields) {
      // Drop the override (target=undefined) when the slider sits at the system
      // default; otherwise store the slider value. Same value-vs-default test as
      // the "custom" badge, so persistence and the badge never disagree.
      const target = isLayoutOverride(f.debounced, f.def) ? f.debounced : undefined;
      targets[f.key] = target;
      if (f.stored !== target) changed = true;
    }
    if (!changed) return;

    const nextSettings: ProjectSettings = { ...(projectSettings ?? {}) };
    for (const f of fields) {
      const target = targets[f.key];
      if (target === undefined) {
        delete nextSettings[f.key];
      } else {
        nextSettings[f.key] = target;
      }
    }

    updateProjectMut.mutate({
      id: currentProject.id,
      data: { settings: nextSettings },
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [debouncedGravity, debouncedCharge, debouncedLinkDistance, currentProject?.id]);

  const markDirty = useCallback(() => {
    dirtyUntilRef.current = Date.now() + DIRTY_GUARD_MS;
    userEditedRef.current = true;
  }, []);
  // rAF-coalesced simulation reheat: a slider drag fires onChange every pixel,
  // so restart alpha at most once per frame. Shared by the live-layout path
  // (small graphs) and the on-release commit path (large graphs).
  const reheatPendingRef = useRef(false);
  const requestReheat = useCallback(() => {
    if (reheatPendingRef.current) return;
    reheatPendingRef.current = true;
    requestAnimationFrame(() => {
      reheatPendingRef.current = false;
      const fg = graphRef.current;
      if (!fg) return;
      const reheat = (fg as unknown as { d3ReheatSimulation?: () => void }).d3ReheatSimulation;
      reheat?.call(fg);
    });
  }, []);
  // Imperatively push the layout knob values into the running d3-force
  // simulation. Cheap O(1) setters; does not restart alpha (that is
  // requestReheat). Used by both the apply effect and the reset handler.
  const applyLayoutForces = useCallback((g: number, c: number, l: number) => {
    const fg = graphRef.current;
    if (!fg) return;
    const chargeForce = fg.d3Force("charge") as unknown as { strength?: (v: number) => void } | undefined;
    chargeForce?.strength?.(c);
    const linkForce = fg.d3Force("link") as unknown as { distance?: (v: number) => void } | undefined;
    linkForce?.distance?.(l);
    const centerForce = fg.d3Force("center") as unknown as { strength?: (v: number) => void } | undefined;
    centerForce?.strength?.(g);
  }, []);
  // Fired when a slider is released (pointer up / key up). On large graphs this
  // is the single re-layout; on small graphs it is a harmless extra reheat
  // after the live morph.
  const handleSliderCommit = useCallback(() => { requestReheat(); }, [requestReheat]);
  const handleGravityChange = useCallback((v: number) => { markDirty(); setGravity(v); }, [markDirty]);
  const handleRepulsionChange = useCallback((v: number) => { markDirty(); setCharge(-v); }, [markDirty]);
  const handleLinkDistanceChange = useCallback((v: number) => { markDirty(); setLinkDistance(v); }, [markDirty]);
  const handleResetLayout = useCallback(() => {
    if (!currentProject) return;
    // Need the store defaults to snap the sliders back to; bail if they have
    // not loaded (the Reset button is disabled without an override anyway).
    if (
      gravityDefault === undefined ||
      chargeDefault === undefined ||
      linkDistanceDefault === undefined
    ) {
      return;
    }
    dirtyUntilRef.current = 0;
    // Drop the edit latch so a still-in-flight debounce from the user's
    // last drag cannot trail the direct reset mutation and resurrect an
    // override at the default value before the refetch lands.
    userEditedRef.current = false;
    const nextSettings: ProjectSettings = { ...(projectSettings ?? {}) };
    delete nextSettings.graph_center_gravity;
    delete nextSettings.graph_charge_strength;
    delete nextSettings.graph_link_distance;
    setGravity(gravityDefault);
    setCharge(chargeDefault);
    setLinkDistance(linkDistanceDefault);
    // Apply the defaults imperatively and reheat now: on large graphs the apply
    // effect does not reheat on its own, and scheduling a reheat off the state
    // update would race the post-paint passive effect and reheat stale
    // strengths. Setting them here first keeps reset coherent at any graph size.
    applyLayoutForces(gravityDefault, chargeDefault, linkDistanceDefault);
    requestReheat();
    updateProjectMut.mutate({
      id: currentProject.id,
      data: { settings: nextSettings },
    });
  }, [
    currentProject,
    projectSettings,
    updateProjectMut,
    gravityDefault,
    chargeDefault,
    linkDistanceDefault,
    applyLayoutForces,
    requestReheat,
  ]);

  // Callback ref, fires when the container div mounts/unmounts
  const containerRef = useCallback((el: HTMLDivElement | null) => {
    // Clean up previous observer
    if (observerRef.current) {
      observerRef.current.disconnect();
      observerRef.current = null;
    }

    if (!el) return;

    const measure = () => {
      setContainerSize({ width: el.offsetWidth, height: el.offsetHeight });
    };

    measure();

    observerRef.current = new ResizeObserver(measure);
    observerRef.current.observe(el);
  }, []);

  useEnsureValidSelectedProject(projects);

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
      .map((r) => {
        const pulse = getPulse(r.weight);
        return {
          source: r.source_id,
          target: r.target_id,
          relation: r.relation,
          color: getRelationColor(r.relation),
          particleCount: pulse.count,
          particleSpeed: pulse.speed,
          weight: r.weight,
          id: r.id,
        };
      });

    return { nodes, links };
  }, [graphData]);

  // Custom node rendering: glowing sphere with label
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

  // Apply forces after mount and on every layout-knob change. The strength
  // setters are always applied (cheap); the reheat (which restarts the
  // simulation alpha) is gated on graph size so large graphs do not churn the
  // simulation on every drag frame. Below LIVE_LAYOUT_NODE_LIMIT the live morph
  // is kept; at or above it the graph re-lays-out only on release/reset.
  useEffect(() => {
    if (!graphRef.current) return;
    // Hold off until the store defaults have loaded so the simulation is never
    // seeded with the placeholder 0s; the sync effect installs the real values
    // and this effect re-runs once defaultsLoaded flips true.
    if (!defaultsLoaded) return;
    applyLayoutForces(gravity, charge, linkDistance);
    if (graph3dData.nodes.length <= LIVE_LAYOUT_NODE_LIMIT) requestReheat();
  }, [
    graph3dData,
    gravity,
    charge,
    linkDistance,
    defaultsLoaded,
    applyLayoutForces,
    requestReheat,
  ]);

  const isLoading = projectsLoading || (selectedProjectId && graphLoading);

  return (
    <div className="flex flex-col h-full" style={{ height: "calc(100vh - 3rem)" }}>
      <div className="flex flex-col gap-2 mb-2 shrink-0 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-xl sm:font-display text-3xl text-foreground">Graph Visualization</h1>
          <p className="mt-0.5 text-sm text-muted-foreground">
            Explore entity relationships in 3D.
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
            className="rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring flex-1 min-w-0 sm:flex-none"
          >
            <option value="">Select a project</option>
            {projects?.map((project) => (
              <option key={project.id} value={project.id}>
                {project.name}
              </option>
            ))}
          </select>
          {!isReservedProject && (
            <button
              onClick={() => setLayoutDrawerOpen((v) => !v)}
              disabled={!selectedProjectId}
              className="rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm hover:bg-accent disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              title="Adjust graph layout forces"
            >
              Layout
            </button>
          )}
        </div>
      </div>

      {selectedProjectId && !isLoading && !graphError && graphData?.truncated &&
        graphData.returned_edges !== undefined && graphData.total_edges !== undefined && (
          <div className="mb-2 shrink-0 rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-2 text-sm text-amber-700 dark:text-amber-300">
            Showing the top {graphData.returned_edges.toLocaleString()} of {graphData.total_edges.toLocaleString()} edges by weight.
          </div>
        )}

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
        <div className="flex flex-1 min-h-0 items-center justify-center rounded-lg border border-destructive/40 bg-destructive/10">
          <div className="text-center">
            <p className="text-sm text-destructive">
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
            {containerSize && <ForceGraph3D
              key={selectedProjectId}
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
              linkColor={((link: GraphLink) => link.color) as any}
              linkWidth={((link: GraphLink) => Math.max(0.3, Math.min(link.weight * 0.4, 2))) as any}
              linkOpacity={0.4}
              linkCurvature={0.15}
              linkDirectionalParticles={((link: GraphLink) => link.particleCount) as any}
              linkDirectionalParticleSpeed={((link: GraphLink) => link.particleSpeed) as any}
              linkDirectionalParticleWidth={1.5}
              linkDirectionalParticleColor={((link: GraphLink) => link.color) as any}
              linkDirectionalArrowLength={3}
              linkDirectionalArrowRelPos={1}
              linkDirectionalArrowColor={((link: GraphLink) => link.color) as any}
              linkLabel={((link: GraphLink) =>
                `<span style="background:rgba(0,0,0,0.8);padding:3px 8px;border-radius:4px;color:#9ca3af;font-size:11px;">${link.relation}</span>`
              ) as any}
              warmupTicks={40}
              cooldownTime={3000}
              enableNodeDrag={true}
              showNavInfo={false}
            />}

            {selectedEntity && (
              <DetailPanel
                entity={selectedEntity}
                connectedEntities={connectedEntities}
                truncated={graphData?.truncated === true}
                onClose={() => setSelectedEntity(null)}
              />
            )}

            {layoutDrawerOpen && currentProject && !isReservedProject && (
              <LayoutDrawer
                sliders={[
                  {
                    label: "Gravity",
                    description: "Pull toward the center. Higher = tighter clumps.",
                    value: gravity,
                    range: gravityRange,
                    onChange: handleGravityChange,
                    onCommit: handleSliderCommit,
                    isOverride: isLayoutOverride(gravity, gravityDefault),
                  },
                  {
                    label: "Repulsion",
                    description: "Force pushing nodes apart. Higher = more spacing.",
                    value: -charge,
                    range: repulsionRange,
                    onChange: handleRepulsionChange,
                    onCommit: handleSliderCommit,
                    isOverride: isLayoutOverride(charge, chargeDefault),
                  },
                  {
                    label: "Link distance",
                    description: "Target edge length for connected nodes.",
                    value: linkDistance,
                    range: linkDistanceRange,
                    onChange: handleLinkDistanceChange,
                    onCommit: handleSliderCommit,
                    isOverride: isLayoutOverride(linkDistance, linkDistanceDefault),
                  },
                ]}
                onReset={handleResetLayout}
                onClose={() => setLayoutDrawerOpen(false)}
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
