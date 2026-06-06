/**
 * @vitest-environment node
 */
import { describe, it, expect } from "vitest";
import {
  parsePrometheusText,
  findFamily,
  sumBaseSamples,
  suffixValue,
} from "./promParser";

// A representative slice of what promhttp emits for nram's custom registry:
// a labeled counter, a no-label counter, a gauge, and a histogram with its
// _bucket/_sum/_count companions. Help text and label values exercise the
// escape handling, and the special float spellings appear too.
const SAMPLE = `# HELP http_requests_total Total number of HTTP requests processed.
# TYPE http_requests_total counter
http_requests_total{method="GET",path="/v1/health",status="200"} 12
http_requests_total{method="POST",path="/v1/projects/{projectID}/memories",status="201"} 3
# HELP http_requests_in_flight Number of HTTP requests currently being processed.
# TYPE http_requests_in_flight gauge
http_requests_in_flight 2
# HELP nram_memories_total Total number of memories stored.
# TYPE nram_memories_total counter
nram_memories_total 47
# HELP nram_embedding_duration_seconds Duration of embedding operations in seconds.
# TYPE nram_embedding_duration_seconds histogram
nram_embedding_duration_seconds_bucket{le="0.005"} 0
nram_embedding_duration_seconds_bucket{le="0.1"} 4
nram_embedding_duration_seconds_bucket{le="+Inf"} 5
nram_embedding_duration_seconds_sum 0.42
nram_embedding_duration_seconds_count 5
# HELP nram_tokens_used_total Total number of tokens consumed.
# TYPE nram_tokens_used_total counter
nram_tokens_used_total{provider="openai",operation="embed"} 1024
nram_tokens_used_total{provider="anthropic",operation="fact_extract"} 2048
`;

describe("parsePrometheusText", () => {
  const families = parsePrometheusText(SAMPLE);

  it("preserves family declaration order", () => {
    expect(families.map((f) => f.name)).toEqual([
      "http_requests_total",
      "http_requests_in_flight",
      "nram_memories_total",
      "nram_embedding_duration_seconds",
      "nram_tokens_used_total",
    ]);
  });

  it("records the declared type and help for each family", () => {
    expect(findFamily(families, "http_requests_total")?.type).toBe("counter");
    expect(findFamily(families, "http_requests_in_flight")?.type).toBe("gauge");
    expect(findFamily(families, "nram_embedding_duration_seconds")?.type).toBe(
      "histogram",
    );
    expect(findFamily(families, "nram_memories_total")?.help).toBe(
      "Total number of memories stored.",
    );
  });

  it("parses labels on a multi-series counter", () => {
    const fam = findFamily(families, "http_requests_total");
    expect(fam?.samples).toHaveLength(2);
    expect(fam?.samples[1].labels).toEqual({
      method: "POST",
      path: "/v1/projects/{projectID}/memories",
      status: "201",
    });
    expect(fam?.samples[1].value).toBe(3);
  });

  it("treats a no-label series as an empty label set", () => {
    const fam = findFamily(families, "nram_memories_total");
    expect(fam?.samples).toHaveLength(1);
    expect(fam?.samples[0].labels).toEqual({});
    expect(fam?.samples[0].value).toBe(47);
  });

  it("groups histogram _bucket/_sum/_count companions under the base family", () => {
    const fam = findFamily(families, "nram_embedding_duration_seconds");
    expect(fam).toBeDefined();
    // 3 buckets + _sum + _count, all under the one base family.
    expect(fam?.samples).toHaveLength(5);
    expect(suffixValue(fam!, "_count")).toBe(5);
    expect(suffixValue(fam!, "_sum")).toBeCloseTo(0.42);
    const buckets = fam!.samples.filter((s) => s.suffix === "_bucket");
    expect(buckets).toHaveLength(3);
    // The +Inf bucket carries "le" as a label string; its value is the
    // cumulative count (5), matching _count for a complete histogram.
    expect(buckets.find((b) => b.labels.le === "+Inf")?.value).toBe(5);
  });

  it("sums base samples across a labeled counter, ignoring companions", () => {
    expect(sumBaseSamples(families, "http_requests_total")).toBe(15);
    expect(sumBaseSamples(families, "nram_tokens_used_total")).toBe(3072);
    // Histogram base has no non-suffixed samples, so its base sum is 0.
    expect(sumBaseSamples(families, "nram_embedding_duration_seconds")).toBe(0);
    // Absent family collapses to 0.
    expect(sumBaseSamples(families, "does_not_exist")).toBe(0);
  });

  it("tolerates blank input", () => {
    expect(parsePrometheusText("")).toEqual([]);
    expect(parsePrometheusText("\n\n  \n")).toEqual([]);
  });

  it("stands up an untyped family for a sample with no TYPE line", () => {
    const fams = parsePrometheusText("orphan_metric{a=\"b\"} 9\n");
    expect(fams).toHaveLength(1);
    expect(fams[0]).toMatchObject({ name: "orphan_metric", type: "untyped" });
    expect(fams[0].samples[0].labels).toEqual({ a: "b" });
    expect(fams[0].samples[0].value).toBe(9);
  });

  it("preserves NaN and signed infinities", () => {
    const fams = parsePrometheusText(
      "# TYPE g gauge\ng{k=\"nan\"} NaN\ng{k=\"pos\"} +Inf\ng{k=\"neg\"} -Inf\n",
    );
    const fam = findFamily(fams, "g")!;
    expect(Number.isNaN(fam.samples[0].value)).toBe(true);
    expect(fam.samples[1].value).toBe(Infinity);
    expect(fam.samples[2].value).toBe(-Infinity);
  });
});
