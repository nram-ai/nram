// A small parser for the Prometheus text exposition format, enough to render
// the server's /metrics endpoint in the console. It is deliberately scoped to
// what promhttp emits for nram's custom registry (counters, gauges, and
// histograms with the usual _bucket/_sum/_count companion series); it is not a
// general-purpose OpenMetrics parser.
//
// Format reference: https://prometheus.io/docs/instrumenting/exposition_formats/

export type MetricType =
  | "counter"
  | "gauge"
  | "histogram"
  | "summary"
  | "untyped";

/** The suffix that ties a histogram/summary companion series to its family. */
export type SampleSuffix = "_bucket" | "_sum" | "_count";

export interface MetricSample {
  /** Label set for this series; an empty object when the series has no labels. */
  labels: Record<string, string>;
  /** Parsed value. +Inf/-Inf/NaN are preserved as Infinity/-Infinity/NaN. */
  value: number;
  /** Set for histogram/summary companion series; undefined for the base series. */
  suffix?: SampleSuffix;
}

export interface MetricFamily {
  name: string;
  type: MetricType;
  help: string;
  samples: MetricSample[];
}

/** Unescape a Prometheus HELP string: backslash and newline escapes only. */
function unescapeHelp(s: string): string {
  return s.replace(/\\(.)/g, (_m, c: string) => {
    if (c === "n") return "\n";
    if (c === "\\") return "\\";
    return c;
  });
}

/** Unescape a Prometheus label value: \\, \" and \n are defined escapes. */
function unescapeLabelValue(s: string): string {
  return s.replace(/\\(.)/g, (_m, c: string) => {
    if (c === "n") return "\n";
    if (c === '"') return '"';
    if (c === "\\") return "\\";
    return c;
  });
}

/** Parse a sample value token, honoring the special float spellings. */
function parseValue(token: string): number {
  switch (token) {
    case "+Inf":
    case "Inf":
      return Infinity;
    case "-Inf":
      return -Infinity;
    case "NaN":
      return NaN;
    default:
      return Number(token);
  }
}

// Parse the `{...}` label block. The cursor `start` points at the opening
// brace; returns the parsed labels and the index just past the closing brace.
// Quoted values are scanned with escape awareness so a `}` or `,` inside a
// value does not terminate the block or a label early.
function parseLabels(
  line: string,
  start: number,
): { labels: Record<string, string>; end: number } {
  const labels: Record<string, string> = {};
  let i = start + 1; // skip '{'
  while (i < line.length && line[i] !== "}") {
    // Skip whitespace and stray commas between labels.
    while (i < line.length && (line[i] === " " || line[i] === ",")) i++;
    if (line[i] === "}") break;

    // Label name up to '='.
    const nameStart = i;
    while (i < line.length && line[i] !== "=") {
      i++;
    }
    const name = line.slice(nameStart, i);
    i++; // skip '='

    // Quoted value with escape handling.
    if (line[i] !== '"') {
      // Malformed; bail out to avoid an infinite loop.
      break;
    }
    i++; // skip opening quote
    let raw = "";
    while (i < line.length && line[i] !== '"') {
      if (line[i] === "\\" && i + 1 < line.length) {
        raw += line[i] + line[i + 1];
        i += 2;
        continue;
      }
      raw += line[i];
      i++;
    }
    i++; // skip closing quote

    labels[name.trim()] = unescapeLabelValue(raw);
  }
  // i is at '}' (or end); advance past it.
  return { labels, end: i + 1 };
}

/** A single non-comment metric line: `name{labels} value [timestamp]`. */
function parseSampleLine(line: string): { name: string; sample: MetricSample } | null {
  let i = 0;
  // Metric name: up to '{' or whitespace.
  const nameStart = i;
  while (i < line.length && line[i] !== "{" && line[i] !== " " && line[i] !== "\t") {
    i++;
  }
  const name = line.slice(nameStart, i);
  if (!name) return null;

  let labels: Record<string, string> = {};
  if (line[i] === "{") {
    const parsed = parseLabels(line, i);
    labels = parsed.labels;
    i = parsed.end;
  }

  // Skip whitespace before the value.
  while (i < line.length && (line[i] === " " || line[i] === "\t")) i++;

  // Value token runs to the next whitespace (an optional timestamp follows).
  const valueStart = i;
  while (i < line.length && line[i] !== " " && line[i] !== "\t") {
    i++;
  }
  const valueToken = line.slice(valueStart, i);
  if (!valueToken) return null;

  return { name, sample: { labels, value: parseValue(valueToken) } };
}

// Given a sample's raw metric name and the set of declared families, resolve
// which family it belongs to. A direct name match wins; otherwise a histogram
// or summary family claims its _bucket/_sum/_count companions.
function resolveFamily(
  rawName: string,
  byName: Map<string, MetricFamily>,
): { family: MetricFamily; suffix?: SampleSuffix } | null {
  const direct = byName.get(rawName);
  if (direct) return { family: direct };

  const suffixes: SampleSuffix[] = ["_bucket", "_sum", "_count"];
  for (const suffix of suffixes) {
    if (!rawName.endsWith(suffix)) continue;
    const base = rawName.slice(0, -suffix.length);
    const fam = byName.get(base);
    if (fam && (fam.type === "histogram" || fam.type === "summary")) {
      return { family: fam, suffix };
    }
  }
  return null;
}

/**
 * Parse Prometheus exposition text into metric families, preserving the order
 * in which families first appear. Samples whose family was never declared via
 * a `# TYPE` line are grouped into an `untyped` family of their own name.
 */
export function parsePrometheusText(text: string): MetricFamily[] {
  const order: MetricFamily[] = [];
  const byName = new Map<string, MetricFamily>();

  function ensureFamily(name: string): MetricFamily {
    let fam = byName.get(name);
    if (!fam) {
      fam = { name, type: "untyped", help: "", samples: [] };
      byName.set(name, fam);
      order.push(fam);
    }
    return fam;
  }

  for (const rawLine of text.split("\n")) {
    const line = rawLine.trim();
    if (line === "") continue;

    if (line.startsWith("#")) {
      // `# HELP <name> <help...>` or `# TYPE <name> <type>`.
      const m = /^#\s+(HELP|TYPE)\s+(\S+)\s*(.*)$/.exec(line);
      if (!m) continue;
      const [, kind, name, rest] = m;
      const fam = ensureFamily(name);
      if (kind === "HELP") {
        fam.help = unescapeHelp(rest);
      } else {
        const t = rest.trim();
        if (
          t === "counter" ||
          t === "gauge" ||
          t === "histogram" ||
          t === "summary"
        ) {
          fam.type = t;
        } else {
          fam.type = "untyped";
        }
      }
      continue;
    }

    const parsed = parseSampleLine(line);
    if (!parsed) continue;

    const resolved = resolveFamily(parsed.name, byName);
    if (resolved) {
      resolved.family.samples.push({ ...parsed.sample, suffix: resolved.suffix });
    } else {
      // No TYPE declared: stand it up as its own untyped family.
      ensureFamily(parsed.name).samples.push(parsed.sample);
    }
  }

  return order;
}

// --- Small read helpers used by the renderer ---------------------------------

/** Find a family by exact name. */
export function findFamily(
  families: MetricFamily[],
  name: string,
): MetricFamily | undefined {
  return families.find((f) => f.name === name);
}

/**
 * Sum the values of a family's base samples (those without a histogram/summary
 * suffix). Returns 0 when the family is absent. Useful for collapsing a labeled
 * counter to a single headline total.
 */
export function sumBaseSamples(
  families: MetricFamily[],
  name: string,
): number {
  const fam = findFamily(families, name);
  if (!fam) return 0;
  return fam.samples
    .filter((s) => s.suffix === undefined && Number.isFinite(s.value))
    .reduce((acc, s) => acc + s.value, 0);
}

/** Value of the single companion sample carrying the given suffix. */
export function suffixValue(
  family: MetricFamily,
  suffix: SampleSuffix,
): number | undefined {
  const s = family.samples.find((x) => x.suffix === suffix);
  return s?.value;
}
