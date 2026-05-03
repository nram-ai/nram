import { PHASE_LABELS, SUB_PHASE_LABELS, phaseColor } from "../lib/dreaming";
import { formatNumber } from "../lib/formatters";

export type PhaseBudgetSegment = {
  key: string;
  value: number;
  cap?: number;
  hasResidual?: boolean;
};

type Variant = "phase" | "sub_phase";

type Props = {
  segments: PhaseBudgetSegment[];
  total: number;
  format?: (n: number) => string;
  variant?: Variant;
  ariaLabel: string;
};

function labelFor(key: string, variant: Variant): string {
  const map = variant === "sub_phase" ? SUB_PHASE_LABELS : PHASE_LABELS;
  return map[key] ?? key;
}

export default function PhaseBudgetBar({
  segments,
  total,
  format = formatNumber,
  variant = "phase",
  ariaLabel,
}: Props) {
  const consumed = segments.reduce((sum, s) => sum + Math.max(0, s.value), 0);
  const denom = total > 0 ? total : Math.max(consumed, 1);
  const slack = Math.max(0, denom - consumed);
  const overshoot = Math.max(0, consumed - denom);

  const barHeight = variant === "sub_phase" ? "h-2.5" : "h-3.5";

  return (
    <div className="w-full" aria-label={ariaLabel} role="img">
      <div
        className={`flex w-full overflow-hidden rounded-full bg-muted/40 ${barHeight}`}
      >
        {segments.map((seg, i) => {
          const w = denom > 0 ? (Math.max(0, seg.value) / denom) * 100 : 0;
          if (w <= 0) return null;
          return (
            <div
              key={`${seg.key}-${i}`}
              className={`h-full ${seg.hasResidual ? "border-t-2 border-warning" : ""}`}
              style={{
                width: `${w}%`,
                background: phaseColor(seg.key),
              }}
              title={`${labelFor(seg.key, variant)} · ${format(seg.value)}${
                seg.cap ? ` / ${format(seg.cap)}` : ""
              }`}
            />
          );
        })}
        {slack > 0 && (
          <div
            className="h-full bg-muted-foreground/15"
            style={{ width: `${(slack / denom) * 100}%` }}
            title={`Unused · ${format(slack)}`}
          />
        )}
      </div>
      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
        {segments
          .filter((s) => s.value > 0)
          .map((seg, i) => (
            <span key={`legend-${seg.key}-${i}`} className="inline-flex items-center gap-1.5">
              <span
                className="inline-block h-2.5 w-2.5 rounded-sm"
                style={{ background: phaseColor(seg.key) }}
              />
              <span className="text-foreground">{labelFor(seg.key, variant)}</span>
              <span className="font-mono">{format(seg.value)}</span>
            </span>
          ))}
        {slack > 0 && (
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-sm bg-muted-foreground/15" />
            <span>Unused</span>
            <span className="font-mono">{format(slack)}</span>
          </span>
        )}
        {overshoot > 0 && (
          <span className="inline-flex items-center gap-1.5 text-destructive">
            <span>+{format(overshoot)} over</span>
          </span>
        )}
      </div>
    </div>
  );
}
