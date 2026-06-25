import { useState } from "react";

// Mirrors internal/service/extraction_llm.go ExtractionFailure.
export interface ExtractionFailure {
  phase: string;
  reason: string;
  error?: string;
  finish_reason?: string;
  prompt_tokens?: number;
  completion_tokens?: number;
  model?: string;
  provider?: string;
  raw_response?: string;
}

// Mirrors internal/enrichment/worker.go partialRecoveryLeg.
export interface PartialRecoveryLeg {
  phase: string;
  reason: string;
  finish_reason?: string;
  prompt_tokens?: number;
  completion_tokens?: number;
  model?: string;
  provider?: string;
  facts_recovered?: number;
  entities_recovered?: number;
  relationships_recovered?: number;
}

export interface PartialRecoveryWarning {
  warnings: PartialRecoveryLeg[];
}

type Parsed =
  | { kind: "string"; value: string }
  | { kind: "failure"; value: ExtractionFailure }
  | { kind: "warnings"; value: PartialRecoveryWarning }
  | { kind: "raw"; value: string };

function parse(raw: string): Parsed {
  if (!raw) return { kind: "raw", value: "" };
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { kind: "raw", value: raw };
  }
  if (typeof parsed === "string") return { kind: "string", value: parsed };
  if (parsed && typeof parsed === "object") {
    const obj = parsed as Record<string, unknown>;
    if (Array.isArray(obj.warnings)) {
      return { kind: "warnings", value: obj as unknown as PartialRecoveryWarning };
    }
    if (typeof obj.phase === "string" && typeof obj.reason === "string") {
      return { kind: "failure", value: obj as unknown as ExtractionFailure };
    }
  }
  return { kind: "raw", value: raw };
}

// isPartialRecoveryError reports whether a serialized last_error is a
// partial-recovery warning payload (parse() classifies it as the "warnings"
// kind). Used to gate the queue's per-row re-extract selection structurally,
// instead of a brittle substring match on the JSON.
export function isPartialRecoveryError(
  value: string | null | undefined,
): boolean {
  if (!value) return false;
  return parse(value).kind === "warnings";
}

function phaseLabel(phase: string): string {
  switch (phase) {
    case "fact_extraction":
      return "fact extraction";
    case "entity_extraction":
      return "entity extraction";
    default:
      return phase.replace(/_/g, " ");
  }
}

function reasonLabel(reason: string): string {
  return reason.replace(/_/g, " ");
}

function legSummary(leg: PartialRecoveryLeg): string {
  const parts: string[] = [phaseLabel(leg.phase)];
  if (leg.facts_recovered != null) parts.push(`${leg.facts_recovered} facts`);
  if (leg.entities_recovered != null)
    parts.push(`${leg.entities_recovered} entities`);
  if (leg.relationships_recovered != null)
    parts.push(`${leg.relationships_recovered} relations`);
  return parts.length > 1
    ? `${parts[0]} (${parts.slice(1).join(", ")})`
    : parts[0];
}

function FailureDiagnostics({ value }: { value: ExtractionFailure }) {
  const rows: Array<[string, string | number]> = [];
  if (value.model) rows.push(["model", value.model]);
  if (value.provider) rows.push(["provider", value.provider]);
  if (value.finish_reason) rows.push(["finish reason", value.finish_reason]);
  if (value.prompt_tokens != null)
    rows.push(["prompt tokens", value.prompt_tokens]);
  if (value.completion_tokens != null)
    rows.push(["completion tokens", value.completion_tokens]);
  return (
    <div className="mt-1 space-y-1">
      {rows.length > 0 && (
        <dl className="grid grid-cols-[max-content_1fr] gap-x-3 gap-y-0.5 text-[11px] text-muted-foreground">
          {rows.map(([k, v]) => (
            <FragmentRow key={k} label={k} value={v} />
          ))}
        </dl>
      )}
      {value.raw_response && (
        <pre className="max-h-40 overflow-auto rounded bg-muted/50 p-2 font-mono text-[11px] text-foreground">
          {value.raw_response}
        </pre>
      )}
    </div>
  );
}

function FragmentRow({
  label,
  value,
}: {
  label: string;
  value: string | number;
}) {
  return (
    <>
      <dt className="font-medium">{label}</dt>
      <dd className="font-mono break-all">{value}</dd>
    </>
  );
}

function WarningsDiagnostics({ value }: { value: PartialRecoveryWarning }) {
  return (
    <div className="mt-1 overflow-x-auto rounded border border-warning/40">
      <table className="w-full text-[11px]">
        <thead>
          <tr className="bg-warning/10 text-left text-muted-foreground">
            <th className="px-2 py-1 font-medium">phase</th>
            <th className="px-2 py-1 font-medium">recovered</th>
            <th className="px-2 py-1 font-medium">finish</th>
            <th className="px-2 py-1 font-medium">model</th>
            <th className="px-2 py-1 font-medium">tokens (p/c)</th>
          </tr>
        </thead>
        <tbody>
          {value.warnings.map((leg, i) => (
            <tr key={i} className="border-t border-warning/30">
              <td className="px-2 py-1">{phaseLabel(leg.phase)}</td>
              <td className="px-2 py-1">
                {[
                  leg.facts_recovered != null
                    ? `${leg.facts_recovered} facts`
                    : null,
                  leg.entities_recovered != null
                    ? `${leg.entities_recovered} entities`
                    : null,
                  leg.relationships_recovered != null
                    ? `${leg.relationships_recovered} relations`
                    : null,
                ]
                  .filter(Boolean)
                  .join(", ") || "-"}
              </td>
              <td className="px-2 py-1 font-mono">
                {leg.finish_reason ?? "-"}
              </td>
              <td className="px-2 py-1 font-mono">{leg.model ?? "-"}</td>
              <td className="px-2 py-1 font-mono">
                {leg.prompt_tokens ?? "-"} / {leg.completion_tokens ?? "-"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function ExtractionErrorView({
  value,
  variant = "cell",
}: {
  value?: string | null;
  variant?: "cell" | "block";
}) {
  const [expanded, setExpanded] = useState(false);
  if (!value) {
    return variant === "cell" ? (
      <span className="text-xs text-muted-foreground">-</span>
    ) : null;
  }

  const parsed = parse(value);

  if (parsed.kind === "string" || parsed.kind === "raw") {
    const text = parsed.kind === "string" ? parsed.value : parsed.value;
    if (variant === "cell") {
      const isLong = text.length > 60;
      return (
        <div className="max-w-xs">
          <p
            className={`text-xs text-destructive ${
              !expanded && isLong ? "line-clamp-2" : ""
            }`}
          >
            {text}
          </p>
          {isLong && (
            <button
              type="button"
              onClick={() => setExpanded((v) => !v)}
              className="mt-0.5 text-xs font-medium text-primary hover:underline"
            >
              {expanded ? "Show less" : "Show more"}
            </button>
          )}
        </div>
      );
    }
    return (
      <div className="rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive">
        {text}
      </div>
    );
  }

  if (parsed.kind === "failure") {
    const f = parsed.value;
    const headline = `${phaseLabel(f.phase)}: ${reasonLabel(f.reason)}`;
    const detail = f.error ?? "";
    const wrapperCls =
      variant === "cell"
        ? "max-w-xs text-xs"
        : "rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm";
    return (
      <div className={wrapperCls}>
        <p className="font-medium text-destructive">{headline}</p>
        {detail && (
          <p
            className={`text-destructive ${
              variant === "cell" && !expanded ? "line-clamp-2" : ""
            }`}
          >
            {detail}
          </p>
        )}
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="mt-0.5 text-xs font-medium text-primary hover:underline"
        >
          {expanded ? "Hide diagnostics" : "Show diagnostics"}
        </button>
        {expanded && <FailureDiagnostics value={f} />}
      </div>
    );
  }

  // warnings
  const w = parsed.value;
  const headline = `Partial recovery: ${w.warnings.map(legSummary).join(", ")}`;
  const wrapperCls =
    variant === "cell"
      ? "max-w-xs text-xs"
      : "rounded-md border border-warning/40 bg-warning/10 p-3 text-sm";
  return (
    <div className={wrapperCls}>
      <p
        className={`font-medium text-warning ${
          variant === "cell" && !expanded ? "line-clamp-2" : ""
        }`}
      >
        {headline}
      </p>
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        className="mt-0.5 text-xs font-medium text-primary hover:underline"
      >
        {expanded ? "Hide diagnostics" : "Show diagnostics"}
      </button>
      {expanded && <WarningsDiagnostics value={w} />}
    </div>
  );
}

export const __test = { parse, legSummary };
