import { useState, useCallback, useMemo, useRef, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import {
  useResetSettings,
  useSettings,
  useSettingsSchema,
  useSettingGroups,
  useSetupStatus,
  useUpdateSetting,
  coerceFiniteNumber,
} from "../hooks/useApi";
import { useEnrichmentAvailable } from "../hooks/useEnrichmentAvailable";
import { useDebounce } from "../hooks/useDebounce";
import { SliderRow } from "../components/LayoutSlider";
import type { SettingSchema, SettingGroup } from "../api/client";
import {
  buildCategoryIndex,
  buildFallbackGroup,
  matchesQuery,
  resolveActiveGroup,
  type SettingWithSchema,
} from "./settingsNav";
import Switch from "../components/Switch";
import PhaseBudgetBar, { type PhaseBudgetSegment } from "../components/PhaseBudgetBar";
import { QueryAugmentBackfillBlock } from "../components/QueryAugmentBackfillBlock";
import { MultiVectorBackfillBlock } from "../components/MultiVectorBackfillBlock";
import { GraphMaintenanceBlock } from "../components/GraphMaintenanceBlock";
import { VectorMigrationBlock } from "../components/VectorMigrationBlock";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faCheck, faXmark, faCircleQuestion, faSpinner, faMagnifyingGlass } from "../lib/icons";

// Setting keys are not always literal phase names; e.g. `dreaming.transitive.*`
// drives the `transitive_discovery` phase. The bar needs the phase key to
// look up color and label.
const SETTING_KEY_TO_PHASE: Record<string, string> = {
  "dreaming.entity_dedup.budget_fraction": "entity_dedup",
  "dreaming.embedding_backfill.budget_fraction": "embedding_backfill",
  "dreaming.augmentation_backfill.budget_fraction": "augmentation_backfill",
  "dreaming.multi_vector_backfill.budget_fraction": "multi_vector_backfill",
  "dreaming.paraphrase_dedup.budget_fraction": "paraphrase_dedup",
  "dreaming.transitive.budget_fraction": "transitive_discovery",
  "dreaming.contradiction.budget_fraction": "contradiction_detection",
  "dreaming.consolidation.budget_fraction": "consolidation",
  "dreaming.pruning.budget_fraction": "pruning",
  "dreaming.weight_adjustment.budget_fraction": "weight_adjustment",
  "dreaming.consolidation.audit_budget_fraction": "backfill_audit",
  "dreaming.consolidation.reinforce_budget_fraction": "reinforce",
  "dreaming.consolidation.consolidate_budget_fraction": "consolidate",
};

const PHASE_BUDGET_ORDER = [
  "dreaming.entity_dedup.budget_fraction",
  "dreaming.embedding_backfill.budget_fraction",
  "dreaming.augmentation_backfill.budget_fraction",
  "dreaming.multi_vector_backfill.budget_fraction",
  "dreaming.paraphrase_dedup.budget_fraction",
  "dreaming.transitive.budget_fraction",
  "dreaming.contradiction.budget_fraction",
  "dreaming.consolidation.budget_fraction",
  "dreaming.pruning.budget_fraction",
  "dreaming.weight_adjustment.budget_fraction",
];

const CONSOLIDATION_BUDGET_ORDER = [
  "dreaming.consolidation.audit_budget_fraction",
  "dreaming.consolidation.reinforce_budget_fraction",
  "dreaming.consolidation.consolidate_budget_fraction",
];

const BUDGET_BAR_ORDERS: Record<string, string[]> = {
  dreaming_phase_budget: PHASE_BUDGET_ORDER,
  dreaming_consolidation: CONSOLIDATION_BUDGET_ORDER,
};

function fractionSegments(
  items: SettingWithSchema[],
  order: string[],
): PhaseBudgetSegment[] {
  const byKey = new Map<string, SettingWithSchema>(
    items.map((it) => [it.schema.key, it]),
  );
  const segments: PhaseBudgetSegment[] = [];
  for (const key of order) {
    const item = byKey.get(key);
    if (!item) continue;
    const raw = item.setting?.value ?? item.schema.default_value;
    const v = typeof raw === "number" ? raw : Number(raw);
    if (!Number.isFinite(v) || v <= 0) continue;
    segments.push({
      key: SETTING_KEY_TO_PHASE[key] ?? key,
      value: v,
    });
  }
  return segments;
}

const formatFractionPct = (v: number) => `${(v * 100).toFixed(0)}%`;

// canonicalize returns a structurally-equivalent value with object keys sorted
// recursively, so JSON.stringify produces a stable form regardless of insertion
// order. Used for "is this value different from the schema default?" checks
// where the same logical value may come back from the backend with different
// key ordering than the operator originally typed.
function canonicalize(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(canonicalize);
  if (v !== null && typeof v === "object") {
    const src = v as Record<string, unknown>;
    const sorted: Record<string, unknown> = {};
    for (const k of Object.keys(src).sort()) sorted[k] = canonicalize(src[k]);
    return sorted;
  }
  return v;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// The parent-group taxonomy is owned by the backend and fetched via
// useSettingGroups(); see internal/storage/admin/settings_groups.go. The UI
// renders whatever the server returns, so a new category can never silently
// vanish from this page.


// Prompt-typed schema entries (the per-phase tunable system prompts). Surfaced
// on the dedicated Prompt Templates page; filtered out of the Settings page
// entirely so they cannot be edited in two places.
const PROMPT_KEYS = new Set([
  "enrichment.fact_system_prompt",
  "enrichment.entity_system_prompt",
  "enrichment.ingestion_decision.system_prompt",
  "enrichment.query_augment.system_prompt",
  "dreaming.contradiction_system_prompt",
  "dreaming.synthesis_system_prompt",
  "dreaming.alignment_system_prompt",
  "dreaming.novelty.judge_system_prompt",
]);

// Keys owned by another admin surface, filtered out of the Settings page so
// they cannot be edited in two places. usage.cost_rates is configured on the
// Analytics page at the admin/system tier (its cost-rate editor), which is
// where it belongs.
const EXTERNALLY_MANAGED_KEYS = new Set(["usage.cost_rates"]);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return String(value);
  return JSON.stringify(value, null, 2);
}

function parseValue(raw: string, type: string): unknown {
  switch (type) {
    case "bool":
    case "boolean":
      return raw === "true";
    case "int":
      return parseInt(raw, 10);
    case "float":
    case "number":
      return parseFloat(raw);
    case "json":
      try {
        return JSON.parse(raw);
      } catch {
        return raw;
      }
    default:
      return raw;
  }
}

function isPromptKey(key: string): boolean {
  return PROMPT_KEYS.has(key);
}

// isHiddenFromSettings reports whether a key is edited on a dedicated page
// elsewhere and so must not appear on the Settings page.
function isHiddenFromSettings(key: string): boolean {
  return isPromptKey(key) || EXTERNALLY_MANAGED_KEYS.has(key);
}

// ---------------------------------------------------------------------------
// Status Toast
// ---------------------------------------------------------------------------

function StatusToast({
  message,
  type,
}: {
  message: string;
  type: "success" | "error";
}) {
  return (
    <div
      className={`fixed bottom-4 right-4 z-50 flex items-center gap-2 rounded-md px-4 py-2.5 text-sm font-medium shadow-lg transition-all ${ type === "success" ? "bg-success/10 text-success" : "bg-destructive/10 text-destructive" }`}
    >
      {type === "success" ? (
        <FontAwesomeIcon icon={faCheck} className="h-4 w-4 flex-shrink-0" />
      ) : (
        <FontAwesomeIcon icon={faXmark} className="h-4 w-4 flex-shrink-0" />
      )}
      {message}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Help Tooltip
// ---------------------------------------------------------------------------

function HelpTooltip({ text }: { text: string }) {
  const [open, setOpen] = useState(false);
  if (!text) return null;
  return (
    <span className="relative inline-flex">
      <button
        type="button"
        aria-label="Show help"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
        onBlur={() => {
          if (open) setOpen(false);
        }}
        onKeyDown={(e) => {
          if (e.key === "Escape") setOpen(false);
        }}
        className="inline-flex h-4 w-4 items-center justify-center rounded-full text-muted-foreground hover:text-foreground focus:outline-none focus:ring-2 focus:ring-ring"
      >
        <FontAwesomeIcon icon={faCircleQuestion} className="h-3.5 w-3.5" />
      </button>
      {open && (
        <span
          role="tooltip"
          className="absolute left-1/2 top-full z-20 mt-1 w-64 -translate-x-1/2 rounded-md border border-border bg-popover p-3 text-xs leading-relaxed text-foreground shadow-lg"
        >
          {text}
        </span>
      )}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Value Input
// ---------------------------------------------------------------------------

const INPUT_CLASS =
  "w-full rounded-md border border-input bg-background px-3 py-2 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring";
const TEXTAREA_CLASS = `${INPUT_CLASS} font-mono`;

interface RenderValueInputProps {
  schema: SettingSchema;
  isPrompt: boolean;
  editValue: string;
  setEditValue: (v: string) => void;
  onKeyDown: (e: React.KeyboardEvent) => void;
  inputRef: React.RefObject<HTMLInputElement | HTMLTextAreaElement>;
}

function renderValueInput({
  schema,
  isPrompt,
  editValue,
  setEditValue,
  onKeyDown,
  inputRef,
}: RenderValueInputProps) {
  const onChange = (
    e:
      | React.ChangeEvent<HTMLInputElement>
      | React.ChangeEvent<HTMLTextAreaElement>
      | React.ChangeEvent<HTMLSelectElement>,
  ) => setEditValue(e.target.value);

  if (schema.type === "text" || isPrompt) {
    return (
      <textarea
        ref={inputRef as React.RefObject<HTMLTextAreaElement>}
        value={editValue}
        onChange={onChange}
        onKeyDown={onKeyDown}
        rows={8}
        className={TEXTAREA_CLASS}
      />
    );
  }
  if (schema.type === "json") {
    return (
      <textarea
        ref={inputRef as React.RefObject<HTMLTextAreaElement>}
        value={editValue}
        onChange={onChange}
        onKeyDown={onKeyDown}
        rows={6}
        className={TEXTAREA_CLASS}
      />
    );
  }
  if (schema.type === "int" || schema.type === "float" || schema.type === "number") {
    // Schema-driven range. min/max/step on SettingSchema are pointer-typed
    // on the server (omitted vs. zero distinguishable in JSON), so undefined
    // here means "no constraint": fall back to the legacy heuristic step.
    const fallbackStep = schema.type === "float" ? 0.01 : 1;
    const step =
      typeof schema.step === "number" && Number.isFinite(schema.step) && schema.step > 0
        ? schema.step
        : fallbackStep;
    const min = typeof schema.min === "number" && Number.isFinite(schema.min) ? schema.min : undefined;
    const max = typeof schema.max === "number" && Number.isFinite(schema.max) ? schema.max : undefined;
    return (
      <input
        ref={inputRef as React.RefObject<HTMLInputElement>}
        type="number"
        value={editValue}
        onChange={onChange}
        onKeyDown={onKeyDown}
        step={step}
        min={min}
        max={max}
        className={INPUT_CLASS}
      />
    );
  }
  if (schema.type === "secret") {
    return (
      <input
        ref={inputRef as React.RefObject<HTMLInputElement>}
        type="password"
        value={editValue}
        onChange={onChange}
        onKeyDown={onKeyDown}
        className={INPUT_CLASS}
      />
    );
  }
  if (schema.type === "enum" && schema.enum_values && schema.enum_values.length > 0) {
    return (
      <select
        value={editValue}
        onChange={onChange}
        onKeyDown={onKeyDown}
        className={INPUT_CLASS}
      >
        {schema.enum_values.map((v) => (
          <option key={v} value={v}>
            {v}
          </option>
        ))}
      </select>
    );
  }
  return (
    <input
      ref={inputRef as React.RefObject<HTMLInputElement>}
      type="text"
      value={editValue}
      onChange={onChange}
      onKeyDown={onKeyDown}
      className={INPUT_CLASS}
    />
  );
}

// ---------------------------------------------------------------------------
// Graph layout slider gauge
// ---------------------------------------------------------------------------

// Graph layout d3-force settings that render as a live slider gauge in the
// admin editor, identical to the per-project Layout drawer. signFlip presents
// charge_strength as a positive "Repulsion" value (persisted as the negative
// charge it actually is), removing the "negative means more repulsion"
// ambiguity of a raw number input.
const GRAPH_LAYOUT_SLIDERS: Record<string, { label: string; signFlip: boolean }> = {
  "graph.center_gravity": { label: "Gravity", signFlip: false },
  "graph.charge_strength": { label: "Repulsion", signFlip: true },
  "graph.link_distance": { label: "Link distance", signFlip: false },
};

const SLIDER_SAVE_DEBOUNCE_MS = 300;
const SLIDER_DIRTY_GUARD_MS = 1500;

// GraphLayoutSliderEditor is a live, auto-saving slider matching the graph
// page's Layout drawer. It debounces the persist so a drag does not spam the
// settings PUT, and holds off external resync briefly after a drag so a
// refetch from its own save does not clobber the in-flight value. Exported for
// direct unit testing of the repulsion sign-flip.
export function GraphLayoutSliderEditor({
  schema,
  currentValue,
  label,
  signFlip,
  onSave,
}: {
  schema: SettingSchema;
  currentValue: unknown;
  label: string;
  signFlip: boolean;
  onSave: (key: string, value: unknown) => void;
}) {
  // stored is the signed value as persisted; display is positive for repulsion.
  const stored =
    coerceFiniteNumber(currentValue) ?? coerceFiniteNumber(schema.default_value) ?? 0;
  const toDisplay = (s: number) => (signFlip ? -s : s);
  const toStored = (d: number) => (signFlip ? -d : d);

  const [display, setDisplay] = useState(toDisplay(stored));
  const dirtyUntilRef = useRef(0);
  const userEditedRef = useRef(false);

  // Resync from the persisted value when it changes externally, unless the
  // user is mid-drag (guarded window) so our own save's refetch cannot snap
  // the slider back.
  useEffect(() => {
    if (Date.now() < dirtyUntilRef.current) return;
    setDisplay(toDisplay(stored));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stored, signFlip]);

  const debounced = useDebounce(display, SLIDER_SAVE_DEBOUNCE_MS);
  useEffect(() => {
    // Gated on a real interaction so neither the initial mount nor an external
    // update triggers a redundant PUT of the unchanged value.
    if (!userEditedRef.current) return;
    const target = toStored(debounced);
    if (Math.abs(target - stored) < 1e-9) return;
    onSave(schema.key, target);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [debounced]);

  const min = coerceFiniteNumber(schema.min) ?? 0;
  const max = coerceFiniteNumber(schema.max) ?? 100;
  const stepRaw = coerceFiniteNumber(schema.step);
  const step = stepRaw && stepRaw > 0 ? stepRaw : 1;
  // For repulsion the slider runs 0..100 instead of the stored -100..0.
  const range = signFlip ? { min: -max, max: -min, step } : { min, max, step };

  return (
    <SliderRow
      spec={{
        label,
        description: "",
        value: display,
        range,
        onChange: (v) => {
          dirtyUntilRef.current = Date.now() + SLIDER_DIRTY_GUARD_MS;
          userEditedRef.current = true;
          setDisplay(v);
        },
        isOverride: false,
      }}
    />
  );
}

// ---------------------------------------------------------------------------
// Inline Setting Editor
// ---------------------------------------------------------------------------

function InlineSettingEditor({
  item,
  onSave,
  onReset,
  saving,
  resetting,
}: {
  item: SettingWithSchema;
  onSave: (key: string, value: unknown) => void;
  onReset: (key: string) => void;
  saving: boolean;
  resetting: boolean;
}) {
  const { schema, setting } = item;
  const currentValue = setting?.value ?? schema.default_value;
  const isDefault = setting === null;
  const isPrompt = isPromptKey(schema.key);

  const [editing, setEditing] = useState(false);
  const [editValue, setEditValue] = useState("");
  const [confirmingReset, setConfirmingReset] = useState(false);
  const inputRef = useRef<HTMLInputElement | HTMLTextAreaElement>(null);

  // Reset is only meaningful when the live value differs from the registered
  // default. canonicalize() walks the value sorting object keys recursively
  // so two semantically-equal JSON values compare equal regardless of the
  // order their keys came back from the backend serializer. Arrays preserve
  // their order (semantically significant for cost-rate lists, etc.).
  const differsFromDefault = useMemo(() => {
    try {
      return (
        JSON.stringify(canonicalize(currentValue)) !==
        JSON.stringify(canonicalize(schema.default_value))
      );
    } catch {
      return false;
    }
  }, [currentValue, schema.default_value]);

  const startEdit = useCallback(() => {
    setEditValue(formatValue(currentValue));
    setEditing(true);
  }, [currentValue]);

  useEffect(() => {
    if (editing && inputRef.current) {
      inputRef.current.focus();
    }
  }, [editing]);

  const handleSave = useCallback(() => {
    const parsed = parseValue(editValue, schema.type);
    onSave(schema.key, parsed);
    setEditing(false);
  }, [editValue, schema.key, schema.type, onSave]);

  const handleCancel = useCallback(() => {
    setEditing(false);
  }, []);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter" && !e.shiftKey && schema.type !== "text" && schema.type !== "json") {
        e.preventDefault();
        handleSave();
      }
      if (e.key === "Escape") {
        handleCancel();
      }
    },
    [handleSave, handleCancel, schema.type],
  );

  const requiresRestart = schema.requires_restart === true;
  // Reserve the 2px left border + left padding in both states so a row does
  // not shift horizontally when its value flips between default and modified.
  const rowAccent = `border-l-2 pl-3 ${
    differsFromDefault
      ? "border-accent bg-accent/10 rounded-r"
      : "border-transparent"
  }`;
  const headerRow = (
    <>
      <div className="flex items-center gap-2">
        <span className="font-mono text-sm font-medium text-foreground">
          {schema.key}
        </span>
        <HelpTooltip text={schema.description ?? ""} />
        {isDefault && (
          <span className="text-xs text-muted-foreground">(default)</span>
        )}
        {differsFromDefault && (
          <span className="inline-flex items-center rounded-full bg-accent px-2 py-0.5 text-xs font-medium text-accent-foreground">
            Modified
          </span>
        )}
        {requiresRestart && (
          <span className="inline-flex items-center rounded-full bg-warning/20 px-2 py-0.5 text-xs font-medium text-warning">
            Requires a server restart
          </span>
        )}
      </div>
      <p className="mt-0.5 text-xs text-muted-foreground">
        {schema.description}
      </p>
    </>
  );

  // Bool toggle (no edit mode needed)
  if ((schema.type === "bool" || schema.type === "boolean") && !editing) {
    const boolVal = currentValue === true || currentValue === "true";
    return (
      <div className={`flex items-center justify-between py-3 ${rowAccent}`}>
        <div className="flex-1 min-w-0">{headerRow}</div>
        <Switch
          checked={boolVal}
          disabled={saving}
          onChange={(v) => onSave(schema.key, v)}
        />
      </div>
    );
  }

  // Graph layout d3-force settings: live slider gauge that auto-saves on
  // change, matching the per-project Layout drawer. charge_strength reads as a
  // positive "Repulsion" slider instead of a raw negative number.
  const graphSlider = GRAPH_LAYOUT_SLIDERS[schema.key];
  if (graphSlider) {
    return (
      <div className={`py-3 ${rowAccent}`}>
        <div className="flex items-start justify-between gap-4">
          <div className="flex-1 min-w-0">{headerRow}</div>
          {differsFromDefault && (
            <button
              type="button"
              onClick={() => onReset(schema.key)}
              disabled={resetting}
              className="flex-shrink-0 rounded-md border border-input px-2.5 py-1 text-xs font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
              title="Reset this setting to its registered default"
            >
              Reset
            </button>
          )}
        </div>
        <div className="mt-3 max-w-sm">
          <GraphLayoutSliderEditor
            schema={schema}
            currentValue={currentValue}
            label={graphSlider.label}
            signFlip={graphSlider.signFlip}
            onSave={onSave}
          />
        </div>
      </div>
    );
  }

  // Display mode
  if (!editing) {
    return (
      <div className={`flex items-start justify-between py-3 gap-4 ${rowAccent}`}>
        <div className="flex-1 min-w-0">
          {headerRow}
          {!isDefault && (
            <p className="mt-1 text-xs text-muted-foreground">
              Default:{" "}
              <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
                {formatValue(schema.default_value)}
              </code>
            </p>
          )}
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          <code
            className={`rounded bg-muted px-2 py-1 text-sm ${
              isPrompt || schema.type === "json" || schema.type === "text"
                ? "font-mono text-xs max-w-[200px] truncate"
                : "font-mono"
            }`}
          >
            {schema.type === "secret" && formatValue(currentValue).length > 0
              ? formatValue(currentValue).length > 4
                ? "••••••••" + formatValue(currentValue).slice(-4)
                : "••••••••"
              : formatValue(currentValue).length > 60
                ? formatValue(currentValue).slice(0, 60) + "..."
                : formatValue(currentValue)}
          </code>
          <button
            type="button"
            onClick={startEdit}
            className="rounded-md border border-input px-2.5 py-1 text-xs font-medium text-foreground shadow-sm hover:bg-muted"
          >
            Edit
          </button>
          {differsFromDefault && (
            confirmingReset ? (
              <span className="inline-flex items-center gap-1">
                <button
                  type="button"
                  onClick={() => {
                    // Reset the global row back to its registered default.
                    onReset(schema.key);
                    setConfirmingReset(false);
                  }}
                  disabled={resetting}
                  className="rounded-md bg-destructive px-2.5 py-1 text-xs font-medium text-white shadow-sm hover:bg-destructive/90 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Confirm
                </button>
                <button
                  type="button"
                  onClick={() => setConfirmingReset(false)}
                  className="rounded-md border border-input px-2.5 py-1 text-xs font-medium text-foreground shadow-sm hover:bg-muted"
                >
                  Cancel
                </button>
              </span>
            ) : (
              <button
                type="button"
                onClick={() => setConfirmingReset(true)}
                disabled={resetting}
                className="rounded-md border border-input px-2.5 py-1 text-xs font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
                title="Reset this setting to its registered default"
              >
                Reset
              </button>
            )
          )}
        </div>
      </div>
    );
  }

  // Edit mode. The modified-state rowAccent and "Modified" pill are
  // intentionally not applied here: this is the transient active-edit state,
  // which has its own header and primary border/tint. The marker reappears on
  // the display/toggle rows (via the shared headerRow) once editing ends.
  return (
    <div className="rounded-md border border-primary/30 bg-primary/5 p-3 my-1">
      <div className="mb-2">
        <span className="text-sm font-medium text-foreground">
          {schema.key}
        </span>
        <p className="text-xs text-muted-foreground">{schema.description}</p>
      </div>

      {/* Value input based on type */}
      <div className="mb-2">
        <label className="mb-1 block text-xs font-medium text-muted-foreground">
          Value
        </label>
        {renderValueInput({
          schema,
          isPrompt,
          editValue,
          setEditValue,
          onKeyDown: handleKeyDown,
          inputRef,
        })}
      </div>

      {/* Default reference */}
      <p className="mb-3 text-xs text-muted-foreground">
        Default:{" "}
        <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
          {formatValue(schema.default_value).length > 100
            ? formatValue(schema.default_value).slice(0, 100) + "..."
            : formatValue(schema.default_value)}
        </code>
      </p>

      {/* Actions */}
      <div className="flex gap-2">
        <button
          type="button"
          onClick={handleSave}
          disabled={saving}
          className="rounded-md bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {saving ? (
            <span className="flex items-center gap-1.5">
              <FontAwesomeIcon icon={faSpinner} spin className="h-3.5 w-3.5" />
              Saving...
            </span>
          ) : (
            "Save"
          )}
        </button>
        <button
          type="button"
          onClick={handleCancel}
          className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Parent Group Card
// ---------------------------------------------------------------------------

// Some categories carry an operator action block beneath their setting rows.
// CategoryTrailingBlock returns null for every other category, so both
// ParentGroupCard render paths (flat and sectioned) render it unconditionally;
// the block never depends on whether the group happened to flatten. (A group
// with a single unlabeled subsection takes the flat path, and omitting the
// block there is what once hid GraphMaintenanceBlock under Lifecycle Sweep.)
function CategoryTrailingBlock({ category }: { category: string }) {
  if (category === "enrichment_query_augment") return <QueryAugmentBackfillBlock />;
  if (category === "enrichment_multi_vector") return <MultiVectorBackfillBlock />;
  if (category === "lifecycle") return <GraphMaintenanceBlock />;
  if (category === "qdrant") return <VectorMigrationBlock />;
  return null;
}

function ParentGroupCard({
  group,
  itemsByCategory,
  onSave,
  onReset,
  saving,
  resetting,
}: {
  group: SettingGroup;
  itemsByCategory: Map<string, SettingWithSchema[]>;
  onSave: (key: string, value: unknown) => void;
  onReset: (key: string) => void;
  saving: boolean;
  resetting: boolean;
}) {
  // Empty sub-sections are dropped silently so the card never shows a heading
  // with nothing under it.
  const populated = useMemo(
    () =>
      group.subsections
        .map((sub) => ({ sub, items: itemsByCategory.get(sub.category) ?? [] }))
        .filter((entry) => entry.items.length > 0),
    [group.subsections, itemsByCategory],
  );

  if (populated.length === 0) return null;

  // When a parent has exactly one sub-section and it has no own label, the
  // parent header already conveys the section identity, so render the items
  // flat with no h3.
  const flatten =
    populated.length === 1 && !populated[0].sub.label && !populated[0].sub.description;

  return (
    <div className="rounded-lg border border-border bg-card shadow-sm">
      <div className="border-b border-border px-5 py-4">
        <h2 className="text-lg font-semibold text-foreground">{group.label}</h2>
        {group.description && (
          <p className="mt-1 text-xs text-muted-foreground">{group.description}</p>
        )}
      </div>
      {flatten ? (
        <section className="px-5 py-4">
          <div className="divide-y divide-border">
            {populated[0].items.map((item) => (
              <InlineSettingEditor
                key={item.schema.key}
                item={item}
                onSave={onSave}
                onReset={onReset}
                saving={saving}
                resetting={resetting}
              />
            ))}
          </div>
          <CategoryTrailingBlock category={populated[0].sub.category} />
        </section>
      ) : (
        <div className="divide-y divide-border px-5">
          {populated.map(({ sub, items }) => {
            const order = BUDGET_BAR_ORDERS[sub.category];
            const segments = order ? fractionSegments(items, order) : [];
            const showBar = segments.length > 0;

            return (
              <section key={sub.category} className="py-4">
                {sub.label && (
                  <h3 className="text-sm font-semibold text-foreground">{sub.label}</h3>
                )}
                {sub.description && (
                  <p className="mt-1 text-xs text-muted-foreground">{sub.description}</p>
                )}
                {showBar && (
                  <div className="mt-3">
                    <PhaseBudgetBar
                      segments={segments}
                      total={1}
                      format={formatFractionPct}
                      ariaLabel={`${sub.label ?? sub.category} allocation`}
                    />
                  </div>
                )}
                <div className="mt-2 divide-y divide-border">
                  {items.map((item) => (
                    <InlineSettingEditor
                      key={item.schema.key}
                      item={item}
                      onSave={onSave}
                      onReset={onReset}
                      saving={saving}
                      resetting={resetting}
                    />
                  ))}
                </div>
                <CategoryTrailingBlock category={sub.category} />
              </section>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

function SettingsEditor() {
  const settingsQuery = useSettings();
  const schemaQuery = useSettingsSchema();
  const groupsQuery = useSettingGroups();
  const updateMutation = useUpdateSetting();
  const resetMutation = useResetSettings();
  const { available: enrichmentAvailable } = useEnrichmentAvailable();

  // Active tab lives in the URL (?group=…) so it is deep-linkable and survives
  // a reload, matching the app's useSearchParams convention (MemoryBrowser,
  // Login). Search is ephemeral component state.
  const [searchParams, setSearchParams] = useSearchParams();
  const [search, setSearch] = useState("");

  const [toast, setToast] = useState<{
    message: string;
    type: "success" | "error";
  } | null>(null);
  const toastTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [confirmingResetAll, setConfirmingResetAll] = useState(false);

  const showToast = useCallback(
    (message: string, type: "success" | "error") => {
      if (toastTimer.current) clearTimeout(toastTimer.current);
      setToast({ message, type });
      toastTimer.current = setTimeout(() => setToast(null), 3000);
    },
    [],
  );

  const handleSave = useCallback(
    (key: string, value: unknown) => {
      updateMutation.mutate(
        { key, value },
        {
          onSuccess: () => showToast(`Saved "${key}"`, "success"),
          onError: (err) =>
            showToast(
              `Failed to save "${key}": ${err.message}`,
              "error",
            ),
        },
      );
    },
    [updateMutation, showToast],
  );

  const handleReset = useCallback(
    (key: string) => {
      resetMutation.mutate(
        { key },
        {
          onSuccess: () => showToast(`Reset "${key}" to default`, "success"),
          onError: (err) =>
            showToast(`Failed to reset "${key}": ${err.message}`, "error"),
        },
      );
    },
    [resetMutation, showToast],
  );

  const handleResetAll = useCallback(() => {
    resetMutation.mutate(
      {},
      {
        onSuccess: (resp) =>
          showToast(
            `Restored ${resp.reset} settings to defaults`,
            "success",
          ),
        onError: (err) =>
          showToast(`Failed to reset settings: ${err.message}`, "error"),
      },
    );
    setConfirmingResetAll(false);
  }, [resetMutation, showToast]);

  const isLoading =
    settingsQuery.isLoading || schemaQuery.isLoading || groupsQuery.isLoading;
  const isError =
    settingsQuery.isError || schemaQuery.isError || groupsQuery.isError;

  const schemas = schemaQuery.data?.data ?? [];
  const settings = settingsQuery.data?.data ?? [];
  const serverGroups = groupsQuery.data?.data ?? [];

  // Group settings by their backend category. Prompt keys live on the Prompt
  // Templates page, provider-config keys on the Provider Configuration page,
  // and usage.cost_rates on Analytics, all filtered out so the Settings page
  // does not edit them in two places.
  const itemsByCategory = useMemo(() => {
    const settingsMap = new Map(settings.map((s) => [s.key, s]));
    const out = new Map<string, SettingWithSchema[]>();
    for (const schema of schemas) {
      if (isHiddenFromSettings(schema.key)) continue;
      const cat = schema.category || "other";
      const merged: SettingWithSchema = {
        schema,
        setting: settingsMap.get(schema.key) ?? null,
      };
      const list = out.get(cat);
      if (list) {
        list.push(merged);
      } else {
        out.set(cat, [merged]);
      }
    }
    return out;
  }, [schemas, settings]);

  // The active database backend gates groups that only matter for one
  // storage path (HNSW is SQLite-only, for instance). useSetupStatus already
  // exposes backend and is queried elsewhere in the app, so this reuses the
  // react-query cache. While the lookup is in flight we default to permissive;
  // the alternative is a brief flash where applicable groups disappear.
  const { data: setupStatus } = useSetupStatus();
  const activeBackend = setupStatus?.backend ?? "";

  // The full group set is the server taxonomy plus a synthetic "Other" group
  // for any category the server did not place (should never happen, the
  // backend test enforces total coverage, but it keeps a setting from ever
  // silently vanishing).
  const allGroups = useMemo(() => {
    const fallback = buildFallbackGroup(serverGroups, [...itemsByCategory.keys()]);
    return fallback ? [...serverGroups, fallback] : serverGroups;
  }, [serverGroups, itemsByCategory]);

  // category -> {groupLabel, subLabel}, used so search matches group/section
  // names as well as setting keys and descriptions.
  const categoryIndex = useMemo(() => buildCategoryIndex(allGroups), [allGroups]);

  const visibleGroups = useMemo(
    () =>
      allGroups.filter((g) => {
        if (g.requires_enrichment && !enrichmentAvailable) return false;
        if (
          g.requires_backend &&
          activeBackend &&
          !g.requires_backend.includes(activeBackend)
        ) {
          return false;
        }
        return true;
      }),
    [allGroups, enrichmentAvailable, activeBackend],
  );

  const trimmedSearch = search.trim();
  const searching = trimmedSearch !== "";

  // The active tab, self-healing: falls back to the first visible group when
  // the requested one is hidden by gating or names a stale/unknown group.
  const activeGroup = resolveActiveGroup(visibleGroups, searchParams.get("group"));

  const selectGroup = useCallback(
    (id: string) => {
      const next = new URLSearchParams(searchParams);
      next.set("group", id);
      setSearchParams(next, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  // When searching, filter every category's items to those matching the query
  // and count the survivors in the same pass. Categories with no surviving
  // items are dropped (ParentGroupCard then renders nothing for them).
  const { filteredByCategory, matchCount } = useMemo(() => {
    if (!searching) return { filteredByCategory: itemsByCategory, matchCount: 0 };
    const out = new Map<string, SettingWithSchema[]>();
    let count = 0;
    for (const [cat, list] of itemsByCategory) {
      const ctx = categoryIndex.get(cat);
      const kept = list.filter((it) => matchesQuery(it, ctx, trimmedSearch));
      if (kept.length > 0) {
        out.set(cat, kept);
        count += kept.length;
      }
    }
    return { filteredByCategory: out, matchCount: count };
  }, [searching, itemsByCategory, categoryIndex, trimmedSearch]);

  return (
    <div>
      {/* Page header */}
      <div className="mb-6 space-y-4">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <h1 className="font-display text-3xl text-foreground">Settings</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            System configuration. Changes take effect immediately unless a setting is flagged as requiring a server restart.
          </p>
        </div>
        <div className="flex-shrink-0">
          {confirmingResetAll ? (
            <span className="inline-flex items-center gap-2">
              <span className="text-xs text-muted-foreground">
                Restore every setting to its registered default?
              </span>
              <button
                type="button"
                onClick={handleResetAll}
                disabled={resetMutation.isPending}
                className="rounded-md bg-destructive px-3 py-1.5 text-sm font-medium text-white shadow-sm hover:bg-destructive/90 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Confirm reset
              </button>
              <button
                type="button"
                onClick={() => setConfirmingResetAll(false)}
                className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
              >
                Cancel
              </button>
            </span>
          ) : (
            <button
              type="button"
              onClick={() => setConfirmingResetAll(true)}
              disabled={resetMutation.isPending}
              className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Reset all to defaults
            </button>
          )}
        </div>
      </div>

      {/* Search box: filters across all groups by key/description/section. */}
      {!isLoading && !isError && schemas.length > 0 && (
        <div className="relative">
          <FontAwesomeIcon
            icon={faMagnifyingGlass}
            className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground"
          />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search settings by name or description…"
            aria-label="Search settings"
            className="w-full rounded-md border border-input bg-background py-2 pl-9 pr-9 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          />
          {search && (
            <button
              type="button"
              aria-label="Clear search"
              onClick={() => setSearch("")}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
            >
              <FontAwesomeIcon icon={faXmark} className="h-4 w-4" />
            </button>
          )}
        </div>
      )}

      {/* Tab bar: one pill per visible group. Hidden while searching, since
          search shows matches across every group at once. */}
      {!isLoading && !isError && !searching && visibleGroups.length > 0 && (
        <div role="tablist" aria-label="Setting groups" className="flex flex-wrap gap-2">
          {visibleGroups.map((g) => {
            const active = g.id === activeGroup?.id;
            return (
              <button
                key={g.id}
                type="button"
                role="tab"
                aria-selected={active}
                onClick={() => selectGroup(g.id)}
                className={
                  active
                    ? "rounded-full bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground shadow-sm"
                    : "rounded-full border border-input px-3 py-1.5 text-sm font-medium text-foreground hover:bg-muted"
                }
              >
                {g.label}
              </button>
            );
          })}
        </div>
      )}
      </div>

      {/* Loading state */}
      {isLoading && (
        <div className="flex items-center justify-center py-16">
          <FontAwesomeIcon icon={faSpinner} spin className="h-8 w-8 text-muted-foreground" />
        </div>
      )}

      {/* Error state */}
      {isError && !isLoading && (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4">
          <p className="text-sm text-destructive">
            Failed to load settings. Please try refreshing the page.
          </p>
        </div>
      )}

      {/* Content */}
      {!isLoading && !isError && (
        <div className="space-y-6">
          {/* Empty state */}
          {schemas.length === 0 && (
            <div className="rounded-lg border border-border bg-card p-8 text-center">
              <p className="text-sm text-muted-foreground">
                No settings defined. Settings will appear here once the system
                has been configured.
              </p>
            </div>
          )}

          {searching ? (
            <>
              <p className="text-sm text-muted-foreground" aria-live="polite">
                {matchCount === 0
                  ? `No settings match "${trimmedSearch}".`
                  : `${matchCount} setting${matchCount === 1 ? "" : "s"} match "${trimmedSearch}".`}
              </p>
              {/* Every visible group, filtered to matching items. Groups with
                  no matches render nothing. */}
              {visibleGroups.map((group) => (
                <ParentGroupCard
                  key={group.id}
                  group={group}
                  itemsByCategory={filteredByCategory}
                  onSave={handleSave}
                  onReset={handleReset}
                  saving={updateMutation.isPending}
                  resetting={resetMutation.isPending}
                />
              ))}
            </>
          ) : (
            activeGroup && (
              <div role="tabpanel">
                <ParentGroupCard
                  key={activeGroup.id}
                  group={activeGroup}
                  itemsByCategory={itemsByCategory}
                  onSave={handleSave}
                  onReset={handleReset}
                  saving={updateMutation.isPending}
                  resetting={resetMutation.isPending}
                />
              </div>
            )
          )}
        </div>
      )}

      {/* Toast notification */}
      {toast && <StatusToast message={toast.message} type={toast.type} />}
    </div>
  );
}

export default SettingsEditor;
