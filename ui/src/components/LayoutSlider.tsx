// Shared d3-force layout slider gauge. Used both by the per-project Layout
// drawer on the graph visualization page and by the admin SettingsEditor so
// the two surfaces present the graph_visualization settings identically (and
// charge_strength reads as a positive "Repulsion" slider in both places).

export interface SliderSpec {
  label: string;
  description: string;
  value: number;
  range: { min: number; max: number; step: number };
  onChange: (v: number) => void;
  // Fired when the drag ends (pointer up / key up). Lets a consumer defer an
  // expensive apply (e.g. a graph re-layout) to release instead of every frame.
  onCommit?: () => void;
  isOverride: boolean;
}

export function SliderRow({ spec }: { spec: SliderSpec }) {
  const { label, description, value, range, onChange, onCommit, isOverride } = spec;
  // Match readout precision to slider step so step=1 controls don't show
  // floating-point noise.
  const decimals = range.step >= 1 ? 0 : range.step >= 0.1 ? 1 : 2;

  return (
    <div>
      <div className="flex items-baseline justify-between mb-1">
        <label className="text-sm font-medium">
          {label}
          {isOverride && (
            <span className="ml-2 text-[10px] uppercase tracking-wider text-blue-400">
              custom
            </span>
          )}
        </label>
        <span className="text-xs font-mono text-muted-foreground">
          {value.toFixed(decimals)}
        </span>
      </div>
      <input
        type="range"
        min={range.min}
        max={range.max}
        step={range.step}
        value={value}
        onChange={(e) => onChange(parseFloat(e.target.value))}
        onPointerUp={() => onCommit?.()}
        onKeyUp={() => onCommit?.()}
        className="w-full accent-blue-500"
      />
      <p className="mt-1 text-xs text-muted-foreground">{description}</p>
    </div>
  );
}
