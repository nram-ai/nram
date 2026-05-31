/**
 * @vitest-environment node
 */
import { describe, it, expect } from "vitest";
import type { SettingGroup, SettingSchema } from "../api/client";
import {
  OTHER_GROUP_ID,
  buildCategoryIndex,
  buildFallbackGroup,
  matchesQuery,
  resolveActiveGroup,
  type SettingWithSchema,
} from "./settingsNav";

const groups: SettingGroup[] = [
  {
    id: "enrichment",
    label: "Enrichment",
    requires_enrichment: true,
    subsections: [
      { category: "enrichment", label: "General" },
      { category: "enrichment_ingestion", label: "Ingestion Decision" },
    ],
  },
  {
    id: "recall",
    label: "Recall & Ranking",
    subsections: [{ category: "ranking", label: "Ranking" }],
  },
  {
    id: "hnsw",
    label: "Vector Index (HNSW)",
    requires_backend: ["sqlite"],
    subsections: [{ category: "hnsw" }],
  },
];

function item(key: string, category: string, description = ""): SettingWithSchema {
  const schema: SettingSchema = {
    key,
    type: "number",
    default_value: 0,
    description,
    category,
  };
  return { schema, setting: null };
}

describe("buildCategoryIndex", () => {
  it("maps each category to its group and sub-section labels", () => {
    const idx = buildCategoryIndex(groups);
    expect(idx.get("enrichment")).toEqual({
      groupLabel: "Enrichment",
      subLabel: "General",
    });
    expect(idx.get("ranking")).toEqual({
      groupLabel: "Recall & Ranking",
      subLabel: "Ranking",
    });
    // sub-section with no label still records the group label.
    expect(idx.get("hnsw")).toEqual({
      groupLabel: "Vector Index (HNSW)",
      subLabel: undefined,
    });
    expect(idx.has("nope")).toBe(false);
  });
});

describe("matchesQuery", () => {
  const idx = buildCategoryIndex(groups);
  const it1 = item("recall.default_limit", "ranking", "Default result count");

  it("returns false for an empty or whitespace query", () => {
    expect(matchesQuery(it1, idx.get("ranking"), "")).toBe(false);
    expect(matchesQuery(it1, idx.get("ranking"), "   ")).toBe(false);
  });

  it("matches on key (case-insensitive)", () => {
    expect(matchesQuery(it1, idx.get("ranking"), "DEFAULT_LIMIT")).toBe(true);
  });

  it("matches on description", () => {
    expect(matchesQuery(it1, idx.get("ranking"), "result count")).toBe(true);
  });

  it("matches on group and sub-section labels", () => {
    expect(matchesQuery(it1, idx.get("ranking"), "recall")).toBe(true); // group label
    expect(matchesQuery(it1, idx.get("ranking"), "ranking")).toBe(true); // sub label
  });

  it("does not match unrelated text", () => {
    expect(matchesQuery(it1, idx.get("ranking"), "zzz")).toBe(false);
  });

  it("tolerates a missing context", () => {
    expect(matchesQuery(it1, undefined, "default_limit")).toBe(true);
    expect(matchesQuery(it1, undefined, "recall & ranking")).toBe(false);
  });
});

describe("resolveActiveGroup", () => {
  it("returns the requested group when it is visible", () => {
    expect(resolveActiveGroup(groups, "recall")?.id).toBe("recall");
  });

  it("falls back to the first visible group when the request is hidden/unknown", () => {
    expect(resolveActiveGroup(groups, "nonexistent")?.id).toBe("enrichment");
    expect(resolveActiveGroup(groups, null)?.id).toBe("enrichment");
    expect(resolveActiveGroup(groups, undefined)?.id).toBe("enrichment");
  });

  it("returns null when no groups are visible", () => {
    expect(resolveActiveGroup([], "recall")).toBeNull();
  });
});

describe("buildFallbackGroup", () => {
  it("returns null when every category is already mapped", () => {
    expect(
      buildFallbackGroup(groups, ["enrichment", "ranking", "hnsw"]),
    ).toBeNull();
  });

  it("buckets unmapped categories into an Other group, sorted", () => {
    const fb = buildFallbackGroup(groups, ["ranking", "usage", "export"]);
    expect(fb).not.toBeNull();
    expect(fb!.id).toBe(OTHER_GROUP_ID);
    expect(fb!.label).toBe("Other");
    // export and usage are unmapped; ranking is already covered. Sorted.
    expect(fb!.subsections.map((s) => s.category)).toEqual(["export", "usage"]);
    // each orphan gets a label so it renders with a heading.
    expect(fb!.subsections.every((s) => s.label === s.category)).toBe(true);
  });
});
