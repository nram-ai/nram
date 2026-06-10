import { describe, it, expect } from "vitest";
import { detectFormat } from "../BulkImport";

describe("detectFormat", () => {
  it("classifies a native nram JSON export as nram, not zep", () => {
    // An nram export is a top-level object with a `memories` array, the same
    // shape that previously matched the Zep heuristic. The `version` plus
    // native fields must take precedence.
    const exportData = {
      version: "1.0",
      exported_at: "2026-06-10T00:00:00Z",
      project: { id: "p1", name: "Demo", slug: "demo" },
      memories: [{ id: "m1", content: "hello", tags: [] }],
      entities: [{ id: "e1", name: "Alice", type: "person" }],
      relationships: [
        { id: "r1", source_id: "e1", target_id: "e2", relation: "knows" },
      ],
      stats: { memory_count: 1, entity_count: 1, relationship_count: 1 },
    };
    const { format, records } = detectFormat(exportData, "nram-export.json");
    expect(format).toBe("nram");
    expect(records).toHaveLength(1);
  });

  it("still classifies a real Zep export as zep", () => {
    const zep = {
      memories: [{ uuid: "1", role: "user", content: "hi" }],
    };
    expect(detectFormat(zep, "zep.json").format).toBe("zep");
  });

  it("classifies a Mem0 array export as mem0", () => {
    const mem0 = [{ id: "1", memory: "user likes tea" }];
    expect(detectFormat(mem0, "mem0.json").format).toBe("mem0");
  });

  it("classifies a generic JSON array as json", () => {
    const generic = [{ content: "a thing", tags: ["x"] }];
    expect(detectFormat(generic, "data.json").format).toBe("json");
  });

  it("classifies a .csv file as csv", () => {
    expect(detectFormat([{ content: "x" }], "data.csv").format).toBe("csv");
  });

  it("does not treat a bare object with a memories array but no version as nram", () => {
    const ambiguous = { memories: [{ content: "x" }] };
    expect(detectFormat(ambiguous, "x.json").format).toBe("zep");
  });
});
