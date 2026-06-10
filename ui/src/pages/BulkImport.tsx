import { useState, useCallback, useRef } from "react";
import { useNavigate } from "react-router-dom";
import {
  useMeProjects,
  useStoreMemory,
  useImportMemories,
} from "../hooks/useApi";
import { useAuth } from "../context/AuthContext";
import type {
  Project,
  StoreMemoryRequest,
  ImportResponse,
} from "../api/client";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faUpload } from "../lib/icons";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type DetectedFormat = "nram" | "mem0" | "zep" | "json" | "csv" | "unknown";

interface ParsedRecord {
  [key: string]: unknown;
}

/**
 * Shape of nram's own JSON export (ExportData). Only the fields the import
 * review surface reads are typed; the full object is forwarded verbatim to the
 * server's nram-format import endpoint, which re-parses it.
 */
interface NRAMExportData {
  version: string;
  project?: { id?: string; name?: string; slug?: string };
  memories?: unknown[];
  entities?: unknown[];
  relationships?: unknown[];
  stats?: {
    memory_count?: number;
    entity_count?: number;
    relationship_count?: number;
  };
}

/**
 * An nram JSON export is a top-level object carrying a `version` string and a
 * `memories` array, corroborated by at least one other native field. This must
 * be checked before the Zep heuristic, which also keys off a `memories` array.
 */
function isNRAMExport(data: unknown): data is NRAMExportData {
  if (typeof data !== "object" || data === null) return false;
  const obj = data as Record<string, unknown>;
  if (typeof obj.version !== "string") return false;
  if (!Array.isArray(obj.memories)) return false;
  return (
    "exported_at" in obj ||
    "entities" in obj ||
    "relationships" in obj ||
    "stats" in obj ||
    "project" in obj
  );
}

interface FieldMapping {
  content: string;
  tags: string;
  metadata: string;
  source: string;
  sourceStatic: string;
}

interface ImportResult {
  total: number;
  success: number;
  failed: number;
  errors: string[];
}

// ---------------------------------------------------------------------------
// CSV Parser
// ---------------------------------------------------------------------------

function parseCSV(text: string): ParsedRecord[] {
  const lines: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < text.length && text[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if ((ch === "\n" || ch === "\r") && !inQuotes) {
      if (current.length > 0 || lines.length > 0) {
        lines.push(current);
        current = "";
      }
      if (ch === "\r" && i + 1 < text.length && text[i + 1] === "\n") {
        i++;
      }
    } else {
      current += ch;
    }
  }
  if (current.length > 0) {
    lines.push(current);
  }

  if (lines.length < 2) return [];

  const splitRow = (row: string): string[] => {
    const fields: string[] = [];
    let field = "";
    let quoted = false;
    for (let i = 0; i < row.length; i++) {
      const ch = row[i];
      if (ch === '"') {
        if (quoted && i + 1 < row.length && row[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          quoted = !quoted;
        }
      } else if (ch === "," && !quoted) {
        fields.push(field.trim());
        field = "";
      } else {
        field += ch;
      }
    }
    fields.push(field.trim());
    return fields;
  };

  const headers = splitRow(lines[0]);
  const records: ParsedRecord[] = [];

  for (let i = 1; i < lines.length; i++) {
    const values = splitRow(lines[i]);
    if (values.length === 0 || (values.length === 1 && values[0] === "")) continue;
    const record: ParsedRecord = {};
    for (let j = 0; j < headers.length; j++) {
      record[headers[j]] = j < values.length ? values[j] : "";
    }
    records.push(record);
  }

  return records;
}

// ---------------------------------------------------------------------------
// Format Detection
// ---------------------------------------------------------------------------

export function detectFormat(
  data: unknown,
  fileName: string,
): { format: DetectedFormat; records: ParsedRecord[] } {
  if (fileName.endsWith(".csv")) {
    return { format: "csv", records: data as ParsedRecord[] };
  }

  if (Array.isArray(data)) {
    if (data.length > 0) {
      const first = data[0];
      if (typeof first === "object" && first !== null) {
        if ("memory" in first || ("text" in first && !("content" in first))) {
          return { format: "mem0", records: data as ParsedRecord[] };
        }
      }
    }
    return { format: "json", records: data as ParsedRecord[] };
  }

  if (typeof data === "object" && data !== null) {
    const obj = data as Record<string, unknown>;
    // nram's own export must win over the Zep heuristic below: both carry a
    // top-level `memories` array, but only the native export sets `version`
    // plus a corroborating native field.
    if (isNRAMExport(data)) {
      return { format: "nram", records: obj.memories as ParsedRecord[] };
    }
    if (Array.isArray(obj.memories)) {
      return { format: "zep", records: obj.memories as ParsedRecord[] };
    }
    if (Array.isArray(obj.messages)) {
      return { format: "zep", records: obj.messages as ParsedRecord[] };
    }
    if (Array.isArray(obj.data)) {
      return { format: "json", records: obj.data as ParsedRecord[] };
    }
    if (Array.isArray(obj.results)) {
      return { format: "json", records: obj.results as ParsedRecord[] };
    }
  }

  return { format: "unknown", records: [] };
}

function getFieldNames(records: ParsedRecord[]): string[] {
  const keys = new Set<string>();
  const limit = Math.min(records.length, 20);
  for (let i = 0; i < limit; i++) {
    for (const key of Object.keys(records[i])) {
      keys.add(key);
    }
  }
  return Array.from(keys).sort();
}

function autoMapFields(
  fields: string[],
  format: DetectedFormat,
): FieldMapping {
  const mapping: FieldMapping = {
    content: "",
    tags: "",
    metadata: "",
    source: "",
    sourceStatic: "",
  };

  const lower = fields.map((f) => f.toLowerCase());

  // Content mapping
  const contentCandidates = ["content", "memory", "text", "message", "body", "value"];
  for (const c of contentCandidates) {
    const idx = lower.indexOf(c);
    if (idx !== -1) {
      mapping.content = fields[idx];
      break;
    }
  }

  // Tags mapping
  const tagCandidates = ["tags", "labels", "categories", "keywords"];
  for (const c of tagCandidates) {
    const idx = lower.indexOf(c);
    if (idx !== -1) {
      mapping.tags = fields[idx];
      break;
    }
  }

  // Metadata mapping
  const metaCandidates = ["metadata", "meta", "properties", "attributes", "extra"];
  for (const c of metaCandidates) {
    const idx = lower.indexOf(c);
    if (idx !== -1) {
      mapping.metadata = fields[idx];
      break;
    }
  }

  // Source mapping
  const sourceCandidates = ["source", "origin", "provider"];
  for (const c of sourceCandidates) {
    const idx = lower.indexOf(c);
    if (idx !== -1) {
      mapping.source = fields[idx];
      break;
    }
  }

  // If no source field found, set a static default based on format
  if (!mapping.source) {
    const formatLabels: Record<DetectedFormat, string> = {
      nram: "nram-import",
      mem0: "mem0-import",
      zep: "zep-import",
      json: "json-import",
      csv: "csv-import",
      unknown: "import",
    };
    mapping.sourceStatic = formatLabels[format];
  }

  return mapping;
}

function formatLabel(format: DetectedFormat): string {
  switch (format) {
    case "nram":
      return "nram Export";
    case "mem0":
      return "Mem0 Export";
    case "zep":
      return "Zep Export";
    case "json":
      return "Generic JSON";
    case "csv":
      return "CSV";
    default:
      return "Unknown";
  }
}

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function truncate(text: string, maxLen = 80): string {
  if (typeof text !== "string") return String(text ?? "");
  if (text.length <= maxLen) return text;
  return text.slice(0, maxLen) + "\u2026";
}

function resolveField(record: ParsedRecord, fieldName: string): unknown {
  if (!fieldName) return undefined;
  return record[fieldName];
}

function toStringArray(val: unknown): string[] | undefined {
  if (!val) return undefined;
  if (Array.isArray(val)) return val.map(String);
  if (typeof val === "string") {
    const trimmed = val.trim();
    if (!trimmed) return undefined;
    // Try JSON parse for "[...]"
    if (trimmed.startsWith("[")) {
      try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) return parsed.map(String);
      } catch {
        // fall through
      }
    }
    return trimmed.split(/[,;|]/).map((s) => s.trim()).filter(Boolean);
  }
  return undefined;
}

function toMetadataKeys(val: unknown): string {
  if (!val) return "-";
  if (typeof val === "object" && val !== null) {
    return Object.keys(val).join(", ") || "-";
  }
  if (typeof val === "string") {
    try {
      const parsed = JSON.parse(val);
      if (typeof parsed === "object" && parsed !== null) {
        return Object.keys(parsed).join(", ") || "-";
      }
    } catch {
      // not json
    }
    return truncate(val, 40);
  }
  return "-";
}

// ---------------------------------------------------------------------------
// Shared step pieces
// ---------------------------------------------------------------------------

const SELECT_CLASS =
  "rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm w-full";

function FileInfoCard({
  file,
  label,
  detail,
  onBack,
}: {
  file: File;
  label: string;
  detail?: string;
  onBack: () => void;
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium">{file.name}</p>
          <p className="text-xs text-muted-foreground">
            {formatFileSize(file.size)} &middot; {label}
            {detail ?? ""}
          </p>
        </div>
        <button
          onClick={onBack}
          className="text-xs text-muted-foreground hover:text-foreground underline"
        >
          Change file
        </button>
      </div>
    </div>
  );
}

function ProjectPicker({
  projects,
  value,
  onChange,
}: {
  projects: Project[];
  value: string;
  onChange: (id: string) => void;
}) {
  return (
    <div>
      <label className="block text-sm font-medium mb-1.5">
        Target Project <span className="text-destructive">*</span>
      </label>
      <select
        className={SELECT_CLASS}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        <option value="">Select a project...</option>
        {projects.map((p) => (
          <option key={p.id} value={p.id}>
            {p.name} ({p.slug})
          </option>
        ))}
      </select>
    </div>
  );
}

function StatCard({
  value,
  label,
  tone,
}: {
  value: number;
  label: string;
  tone?: "success" | "destructive";
}) {
  const toneClass =
    tone === "success"
      ? " text-success"
      : tone === "destructive"
        ? " text-destructive"
        : "";
  return (
    <div className="rounded-md border border-border p-3 text-center">
      <p className={`text-lg font-semibold${toneClass}`}>{value}</p>
      <p className="text-xs text-muted-foreground">{label}</p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Step Components
// ---------------------------------------------------------------------------

const STEPS = ["Upload File", "Map Fields", "Preview", "Import"];
const NRAM_STEPS = ["Upload File", "Review", "Import"];

function StepIndicator({
  current,
  steps,
}: {
  current: number;
  steps: string[];
}) {
  return (
    <div className="flex items-center gap-2 mb-8">
      {steps.map((label, i) => {
        const stepNum = i + 1;
        const isActive = stepNum === current;
        const isDone = stepNum < current;
        return (
          <div key={label} className="flex items-center gap-2">
            {i > 0 && (
              <div
                className={`h-px w-8 ${
                  isDone ? "bg-primary" : "bg-border"
                }`}
              />
            )}
            <div className="flex items-center gap-1.5">
              <div
                className={`flex h-7 w-7 items-center justify-center rounded-full text-xs font-medium ${
                  isActive
                    ? "bg-primary text-primary-foreground"
                    : isDone
                      ? "bg-primary/20 text-primary"
                      : "bg-muted text-muted-foreground"
                }`}
              >
                {isDone ? "\u2713" : stepNum}
              </div>
              <span
                className={`text-sm ${
                  isActive
                    ? "font-medium text-foreground"
                    : "text-muted-foreground"
                }`}
              >
                {label}
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Step 1: Upload
// ---------------------------------------------------------------------------

function UploadStep({
  onFileParsed,
}: {
  onFileParsed: (
    file: File,
    records: ParsedRecord[],
    format: DetectedFormat,
    fields: string[],
    rawData: unknown,
  ) => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const processFile = useCallback(
    async (file: File) => {
      setError(null);
      const name = file.name.toLowerCase();
      if (!name.endsWith(".json") && !name.endsWith(".csv")) {
        setError("Unsupported file type. Please upload a .json or .csv file.");
        return;
      }

      try {
        const text = await file.text();
        let rawData: unknown;
        let records: ParsedRecord[];

        if (name.endsWith(".csv")) {
          records = parseCSV(text);
          rawData = records;
        } else {
          rawData = JSON.parse(text);
          records = [];
        }

        const { format, records: detected } = detectFormat(
          name.endsWith(".csv") ? records : rawData,
          file.name,
        );

        if (name.endsWith(".csv")) {
          // records already set
        } else {
          records = detected;
        }

        if (records.length === 0) {
          setError(
            "No records found in the file. Please check the file format.",
          );
          return;
        }

        const fields = getFieldNames(records);
        onFileParsed(file, records, format, fields, rawData);
      } catch (e) {
        setError(
          `Failed to parse file: ${e instanceof Error ? e.message : "Unknown error"}`,
        );
      }
    },
    [onFileParsed],
  );

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragging(false);
      const file = e.dataTransfer.files[0];
      if (file) processFile(file);
    },
    [processFile],
  );

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) processFile(file);
    },
    [processFile],
  );

  return (
    <div>
      <div
        className={`rounded-lg border-2 border-dashed ${
          dragging ? "border-primary bg-primary/10" : "border-border bg-accent/30 hover:bg-accent/50"
        } transition-colors p-8 text-center cursor-pointer`}
        onDragOver={(e) => {
          e.preventDefault();
          setDragging(true);
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={handleDrop}
        onClick={() => inputRef.current?.click()}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".json,.csv"
          className="hidden"
          onChange={handleChange}
        />
        <div className="flex flex-col items-center gap-3">
          <FontAwesomeIcon icon={faUpload} className="h-10 w-10 text-muted-foreground" />
          <div>
            <p className="text-sm font-medium text-foreground">
              Drop a file here or click to browse
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              Accepts .json (nram export, Mem0, Zep, or generic) and .csv files
            </p>
          </div>
        </div>
      </div>

      {error && (
        <div className="mt-4 rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive">
          {error}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Step 2: Map Fields
// ---------------------------------------------------------------------------

function MapFieldsStep({
  file,
  format,
  fields,
  mapping,
  selectedProject,
  projects,
  onMappingChange,
  onProjectChange,
  onNext,
  onBack,
}: {
  file: File;
  format: DetectedFormat;
  fields: string[];
  mapping: FieldMapping;
  selectedProject: string;
  projects: Project[];
  onMappingChange: (m: FieldMapping) => void;
  onProjectChange: (id: string) => void;
  onNext: () => void;
  onBack: () => void;
}) {
  const selectClass = SELECT_CLASS;
  const inputClass = SELECT_CLASS;

  const update = (key: keyof FieldMapping, val: string) => {
    onMappingChange({ ...mapping, [key]: val });
  };

  const canProceed = mapping.content !== "" && selectedProject !== "";

  return (
    <div className="space-y-6">
      <FileInfoCard file={file} label={formatLabel(format)} onBack={onBack} />

      <ProjectPicker
        projects={projects}
        value={selectedProject}
        onChange={onProjectChange}
      />

      {/* Field mappings */}
      <div className="space-y-4">
        <h3 className="text-sm font-medium">Field Mapping</h3>
        <p className="text-xs text-muted-foreground">
          Map fields from your file to nram memory fields. Available source
          fields: {fields.join(", ")}
        </p>

        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          {/* Content */}
          <div>
            <label className="block text-sm font-medium mb-1.5">
              Content <span className="text-destructive">*</span>
            </label>
            <select
              className={selectClass}
              value={mapping.content}
              onChange={(e) => update("content", e.target.value)}
            >
              <option value="">-- Select field --</option>
              {fields.map((f) => (
                <option key={f} value={f}>
                  {f}
                </option>
              ))}
            </select>
          </div>

          {/* Tags */}
          <div>
            <label className="block text-sm font-medium mb-1.5">Tags</label>
            <select
              className={selectClass}
              value={mapping.tags}
              onChange={(e) => update("tags", e.target.value)}
            >
              <option value="">-- None --</option>
              {fields.map((f) => (
                <option key={f} value={f}>
                  {f}
                </option>
              ))}
            </select>
          </div>

          {/* Metadata */}
          <div>
            <label className="block text-sm font-medium mb-1.5">
              Metadata
            </label>
            <select
              className={selectClass}
              value={mapping.metadata}
              onChange={(e) => update("metadata", e.target.value)}
            >
              <option value="">-- None --</option>
              {fields.map((f) => (
                <option key={f} value={f}>
                  {f}
                </option>
              ))}
            </select>
          </div>

          {/* Source */}
          <div>
            <label className="block text-sm font-medium mb-1.5">Source</label>
            <select
              className={selectClass}
              value={mapping.source}
              onChange={(e) => {
                update("source", e.target.value);
                if (e.target.value) update("sourceStatic", "");
              }}
            >
              <option value="">-- Static value --</option>
              {fields.map((f) => (
                <option key={f} value={f}>
                  {f}
                </option>
              ))}
            </select>
            {!mapping.source && (
              <input
                className={`${inputClass} mt-1.5`}
                type="text"
                placeholder="Static source value (e.g. mem0-import)"
                value={mapping.sourceStatic}
                onChange={(e) => update("sourceStatic", e.target.value)}
              />
            )}
          </div>
        </div>
      </div>

      {/* Actions */}
      <div className="flex justify-between pt-2">
        <button
          onClick={onBack}
          className="border border-input bg-background hover:bg-accent rounded-md px-4 py-2 text-sm font-medium"
        >
          Back
        </button>
        <button
          disabled={!canProceed}
          onClick={onNext}
          className="bg-primary text-primary-foreground hover:bg-primary/90 rounded-md px-4 py-2 text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed"
        >
          Preview
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Step 3: Preview
// ---------------------------------------------------------------------------

function PreviewStep({
  records,
  mapping,
  totalCount,
  onNext,
  onBack,
}: {
  records: ParsedRecord[];
  mapping: FieldMapping;
  totalCount: number;
  onNext: () => void;
  onBack: () => void;
}) {
  const previewRecords = records.slice(0, 10);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          Showing first {Math.min(10, totalCount)} of{" "}
          <span className="font-medium text-foreground">{totalCount}</span>{" "}
          records
        </p>
      </div>

      <div className="overflow-x-auto rounded-lg border border-border">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-muted/50">
              <th className="px-3 py-2 text-left font-medium">#</th>
              <th className="px-3 py-2 text-left font-medium">Content</th>
              <th className="px-3 py-2 text-left font-medium">Tags</th>
              <th className="px-3 py-2 text-left font-medium">Source</th>
              <th className="px-3 py-2 text-left font-medium">Metadata</th>
            </tr>
          </thead>
          <tbody>
            {previewRecords.map((rec, i) => {
              const content = resolveField(rec, mapping.content);
              const tags = toStringArray(resolveField(rec, mapping.tags));
              const source = mapping.source
                ? String(resolveField(rec, mapping.source) ?? "")
                : mapping.sourceStatic;
              const meta = resolveField(rec, mapping.metadata);

              return (
                <tr
                  key={i}
                  className="border-b border-border last:border-b-0 hover:bg-muted/30"
                >
                  <td className="px-3 py-2 text-muted-foreground">{i + 1}</td>
                  <td className="px-3 py-2 max-w-xs">
                    {truncate(String(content ?? ""), 100)}
                  </td>
                  <td className="px-3 py-2">
                    {tags?.length ? (
                      <div className="flex flex-wrap gap-1">
                        {tags.slice(0, 3).map((t) => (
                          <span
                            key={t}
                            className="rounded bg-muted px-1.5 py-0.5 text-xs"
                          >
                            {t}
                          </span>
                        ))}
                        {tags.length > 3 && (
                          <span className="text-xs text-muted-foreground">
                            +{tags.length - 3}
                          </span>
                        )}
                      </div>
                    ) : (
                      <span className="text-muted-foreground">-</span>
                    )}
                  </td>
                  <td className="px-3 py-2 text-xs">{source || "-"}</td>
                  <td className="px-3 py-2 text-xs">{toMetadataKeys(meta)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="flex justify-between pt-2">
        <button
          onClick={onBack}
          className="border border-input bg-background hover:bg-accent rounded-md px-4 py-2 text-sm font-medium"
        >
          Back
        </button>
        <button
          onClick={onNext}
          className="bg-primary text-primary-foreground hover:bg-primary/90 rounded-md px-4 py-2 text-sm font-medium"
        >
          Start Import
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Step 4: Import
// ---------------------------------------------------------------------------

const BATCH_SIZE = 5;

function ImportStep({
  records,
  mapping,
  projectId,
}: {
  records: ParsedRecord[];
  mapping: FieldMapping;
  projectId: string;
}) {
  const navigate = useNavigate();
  const storeMutation = useStoreMemory();
  const [result, setResult] = useState<ImportResult | null>(null);
  const [progress, setProgress] = useState({ done: 0, total: records.length });
  const [importing, setImporting] = useState(false);
  const abortRef = useRef(false);

  const startImport = useCallback(async () => {
    setImporting(true);
    abortRef.current = false;

    const total = records.length;
    let success = 0;
    let failed = 0;
    const errors: string[] = [];

    setProgress({ done: 0, total });

    // Process in batches
    for (let i = 0; i < total; i += BATCH_SIZE) {
      if (abortRef.current) break;

      const batch = records.slice(i, i + BATCH_SIZE);
      const promises = batch.map(async (rec, batchIdx) => {
        const idx = i + batchIdx;
        const contentVal = resolveField(rec, mapping.content);
        const content = String(contentVal ?? "").trim();

        if (!content) {
          failed++;
          errors.push(`Record ${idx + 1}: Empty content, skipped`);
          return;
        }

        const tags = toStringArray(resolveField(rec, mapping.tags));

        const data: StoreMemoryRequest = { content };
        if (tags && tags.length > 0) {
          data.tags = tags;
        }

        try {
          await storeMutation.mutateAsync({ projectId, data });
          success++;
        } catch (e) {
          failed++;
          const msg = e instanceof Error ? e.message : "Unknown error";
          if (errors.length < 50) {
            errors.push(`Record ${idx + 1}: ${msg}`);
          }
        }
      });

      await Promise.all(promises);
      setProgress({ done: Math.min(i + BATCH_SIZE, total), total });
    }

    setResult({ total, success, failed, errors });
    setImporting(false);
  }, [records, mapping, projectId, storeMutation]);

  const pct =
    progress.total > 0
      ? Math.round((progress.done / progress.total) * 100)
      : 0;

  // Not started yet
  if (!importing && !result) {
    return (
      <div className="space-y-6">
        <div className="rounded-lg border border-border bg-card p-6 text-center">
          <p className="text-sm">
            Ready to import{" "}
            <span className="font-semibold">{records.length}</span> records.
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            Records will be imported in batches of {BATCH_SIZE}.
          </p>
          <button
            onClick={startImport}
            className="mt-4 bg-primary text-primary-foreground hover:bg-primary/90 rounded-md px-6 py-2 text-sm font-medium"
          >
            Start Import
          </button>
        </div>
      </div>
    );
  }

  // In progress
  if (importing) {
    return (
      <div className="space-y-6">
        <div className="rounded-lg border border-border bg-card p-6">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm font-medium">Importing...</p>
            <p className="text-sm text-muted-foreground">
              {progress.done} / {progress.total}
            </p>
          </div>
          <div className="h-2 w-full rounded-full bg-muted overflow-hidden">
            <div
              className="h-full rounded-full bg-primary transition-all duration-300"
              style={{ width: `${pct}%` }}
            />
          </div>
          <p className="mt-2 text-xs text-muted-foreground text-center">
            {pct}% complete
          </p>
          <div className="mt-4 text-center">
            <button
              onClick={() => {
                abortRef.current = true;
              }}
              className="border border-input bg-background hover:bg-accent rounded-md px-4 py-2 text-sm font-medium"
            >
              Cancel
            </button>
          </div>
        </div>
      </div>
    );
  }

  // Done
  return (
    <div className="space-y-6">
      <div className="rounded-lg border border-border bg-card p-6">
        <div className="flex items-center gap-3 mb-4">
          <div
            className={`flex h-10 w-10 items-center justify-center rounded-full text-lg ${ result!.failed === 0 ? "bg-success/20 text-success" : "bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300" }`}
          >
            {result!.failed === 0 ? "\u2713" : "!"}
          </div>
          <div>
            <p className="text-sm font-medium">Import Complete</p>
            <p className="text-xs text-muted-foreground">
              {result!.success} succeeded, {result!.failed} failed out of{" "}
              {result!.total} total
            </p>
          </div>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-3 gap-4 mb-4">
          <StatCard value={result!.total} label="Total" />
          <StatCard value={result!.success} label="Imported" tone="success" />
          <StatCard value={result!.failed} label="Failed" tone="destructive" />
        </div>

        {/* Errors */}
        {result!.errors.length > 0 && (
          <div className="rounded-md border border-destructive/40 bg-destructive/10 p-3">
            <p className="text-sm font-medium text-destructive mb-1">
              Errors ({result!.errors.length}
              {result!.errors.length >= 50 ? "+" : ""})
            </p>
            <div className="max-h-40 overflow-y-auto">
              {result!.errors.map((err, i) => (
                <p
                  key={i}
                  className="text-xs text-destructive py-0.5"
                >
                  {err}
                </p>
              ))}
            </div>
          </div>
        )}
      </div>

      <div className="flex justify-end gap-3">
        <button
          onClick={() => navigate("/memories")}
          className="bg-primary text-primary-foreground hover:bg-primary/90 rounded-md px-4 py-2 text-sm font-medium"
        >
          View Imported Memories
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Native (nram) Review + Import
// ---------------------------------------------------------------------------

const STAT_KEY = {
  memories: "memory_count",
  entities: "entity_count",
  relationships: "relationship_count",
} as const;

function countOf(data: NRAMExportData, key: keyof typeof STAT_KEY): number {
  const arr = data[key];
  if (Array.isArray(arr)) return arr.length;
  const stat = data.stats?.[STAT_KEY[key]];
  return typeof stat === "number" ? stat : 0;
}

function NRAMReviewStep({
  file,
  data,
  selectedProject,
  projects,
  onProjectChange,
  onNext,
  onBack,
}: {
  file: File;
  data: NRAMExportData;
  selectedProject: string;
  projects: Project[];
  onProjectChange: (id: string) => void;
  onNext: () => void;
  onBack: () => void;
}) {
  const memCount = countOf(data, "memories");
  const entCount = countOf(data, "entities");
  const relCount = countOf(data, "relationships");
  const canProceed = selectedProject !== "";

  return (
    <div className="space-y-6">
      <FileInfoCard
        file={file}
        label={formatLabel("nram")}
        detail={data.project?.name ? ` from "${data.project.name}"` : undefined}
        onBack={onBack}
      />

      {/* Native exports carry the full graph; the server handles parsing and
          remapping, so no field mapping is needed. */}
      <div className="grid grid-cols-3 gap-4">
        <StatCard value={memCount} label="Memories" />
        <StatCard value={entCount} label="Entities" />
        <StatCard value={relCount} label="Relationships" />
      </div>

      <p className="text-xs text-muted-foreground">
        This is a native nram export. Memories, entities, and relationships are
        imported on the server with relationship endpoints remapped into the
        target project. Duplicate memories are skipped automatically.
      </p>

      <ProjectPicker
        projects={projects}
        value={selectedProject}
        onChange={onProjectChange}
      />

      <div className="flex justify-between pt-2">
        <button
          onClick={onBack}
          className="border border-input bg-background hover:bg-accent rounded-md px-4 py-2 text-sm font-medium"
        >
          Back
        </button>
        <button
          onClick={onNext}
          disabled={!canProceed}
          className="bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed rounded-md px-4 py-2 text-sm font-medium"
        >
          Continue
        </button>
      </div>
    </div>
  );
}

function NRAMImportStep({
  data,
  projectId,
}: {
  data: NRAMExportData;
  projectId: string;
}) {
  const navigate = useNavigate();
  const importMutation = useImportMemories();
  const [result, setResult] = useState<ImportResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const startImport = useCallback(async () => {
    setError(null);
    try {
      const resp = await importMutation.mutateAsync({
        projectId,
        format: "nram",
        data,
      });
      setResult(resp);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unknown error");
    }
  }, [importMutation, projectId, data]);

  const importing = importMutation.isPending;

  // Done
  if (result) {
    const hadErrors = result.errors.length > 0;
    return (
      <div className="space-y-6">
        <div className="rounded-lg border border-border bg-card p-6">
          <div className="flex items-center gap-3 mb-4">
            <div
              className={`flex h-10 w-10 items-center justify-center rounded-full text-lg ${hadErrors ? "bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300" : "bg-success/20 text-success"}`}
            >
              {hadErrors ? "!" : "✓"}
            </div>
            <div>
              <p className="text-sm font-medium">Import Complete</p>
              <p className="text-xs text-muted-foreground">
                {result.imported} memories, {result.entities_imported} entities,{" "}
                {result.relationships_imported} relationships,{" "}
                {result.lineage_imported} lineage links imported
                {result.skipped > 0 ? `; ${result.skipped} skipped` : ""}
              </p>
            </div>
          </div>

          <div className="grid grid-cols-5 gap-3">
            <StatCard value={result.imported} label="Memories" tone="success" />
            <StatCard
              value={result.entities_imported}
              label="Entities"
              tone="success"
            />
            <StatCard
              value={result.relationships_imported}
              label="Relationships"
              tone="success"
            />
            <StatCard
              value={result.lineage_imported}
              label="Lineage"
              tone="success"
            />
            <StatCard value={result.skipped} label="Skipped" />
          </div>

          {hadErrors && (
            <div className="mt-4 rounded-md border border-destructive/40 bg-destructive/10 p-3">
              <p className="text-sm font-medium text-destructive mb-1">
                Errors ({result.errors.length})
              </p>
              <div className="max-h-40 overflow-y-auto">
                {result.errors.map((err, i) => (
                  <p key={i} className="text-xs text-destructive py-0.5">
                    {err.message}
                  </p>
                ))}
              </div>
            </div>
          )}
        </div>

        <div className="flex justify-end gap-3">
          <button
            onClick={() => navigate("/memories")}
            className="bg-primary text-primary-foreground hover:bg-primary/90 rounded-md px-4 py-2 text-sm font-medium"
          >
            View Imported Memories
          </button>
        </div>
      </div>
    );
  }

  // Ready / in progress
  return (
    <div className="space-y-6">
      <div className="rounded-lg border border-border bg-card p-6 text-center">
        <p className="text-sm">
          Ready to import{" "}
          <span className="font-semibold">{countOf(data, "memories")}</span>{" "}
          memories,{" "}
          <span className="font-semibold">{countOf(data, "entities")}</span>{" "}
          entities, and{" "}
          <span className="font-semibold">
            {countOf(data, "relationships")}
          </span>{" "}
          relationships.
        </p>
        <p className="mt-1 text-xs text-muted-foreground">
          The export is sent to the server in a single request.
        </p>
        <button
          onClick={startImport}
          disabled={importing}
          className="mt-4 bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed rounded-md px-6 py-2 text-sm font-medium"
        >
          {importing ? "Importing..." : "Start Import"}
        </button>
        {error && (
          <div className="mt-4 rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive">
            {error}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

function BulkImport() {
  const [step, setStep] = useState(1);
  const [file, setFile] = useState<File | null>(null);
  const [records, setRecords] = useState<ParsedRecord[]>([]);
  const [format, setFormat] = useState<DetectedFormat>("unknown");
  const [fields, setFields] = useState<string[]>([]);
  const [nramData, setNramData] = useState<NRAMExportData | null>(null);
  const [mapping, setMapping] = useState<FieldMapping>({
    content: "",
    tags: "",
    metadata: "",
    source: "",
    sourceStatic: "",
  });
  const [selectedProject, setSelectedProject] = useState("");

  const { canWrite } = useAuth();
  const { data: projects } = useMeProjects();

  const handleFileParsed = useCallback(
    (
      f: File,
      recs: ParsedRecord[],
      fmt: DetectedFormat,
      flds: string[],
      rawData: unknown,
    ) => {
      setFile(f);
      setRecords(recs);
      setFormat(fmt);
      setFields(flds);
      if (fmt === "nram") {
        // Native export: carry the full object; the per-record map/preview
        // flow does not apply, so jump straight to the review step.
        setNramData(rawData as NRAMExportData);
      } else {
        setNramData(null);
        const autoMapped = autoMapFields(flds, fmt);
        setMapping(autoMapped);
      }
      setStep(2);
    },
    [],
  );

  const resetWizard = useCallback(() => {
    setStep(1);
    setFile(null);
    setRecords([]);
    setFormat("unknown");
    setFields([]);
    setNramData(null);
    setMapping({
      content: "",
      tags: "",
      metadata: "",
      source: "",
      sourceStatic: "",
    });
    setSelectedProject("");
  }, []);

  if (!canWrite) {
    return (
      <div className="flex items-center justify-center py-16">
        <div className="w-full max-w-md rounded-lg border bg-card p-8 text-center">
          <h2 className="text-lg font-semibold">Access Denied</h2>
          <p className="mt-2 text-sm text-muted-foreground">
            You don't have permission to import memories.
          </p>
          <a
            href="/"
            className="mt-4 inline-block rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
          >
            Go to Dashboard
          </a>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="mb-6">
        <h1 className="font-display text-3xl text-foreground">Bulk Import</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Import a native nram export, or memories from Mem0, Zep, or custom
          JSON/CSV files.
        </p>
      </div>

      <StepIndicator
        current={step}
        steps={format === "nram" ? NRAM_STEPS : STEPS}
      />

      {step === 1 && <UploadStep onFileParsed={handleFileParsed} />}

      {/* Native nram export: Upload -> Review -> Import (server-side). */}
      {format === "nram" && nramData ? (
        <>
          {step === 2 && file && (
            <NRAMReviewStep
              file={file}
              data={nramData}
              selectedProject={selectedProject}
              projects={projects ?? []}
              onProjectChange={setSelectedProject}
              onNext={() => setStep(3)}
              onBack={resetWizard}
            />
          )}

          {step === 3 && (
            <NRAMImportStep data={nramData} projectId={selectedProject} />
          )}
        </>
      ) : (
        <>
          {step === 2 && file && (
            <MapFieldsStep
              file={file}
              format={format}
              fields={fields}
              mapping={mapping}
              selectedProject={selectedProject}
              projects={projects ?? []}
              onMappingChange={setMapping}
              onProjectChange={setSelectedProject}
              onNext={() => setStep(3)}
              onBack={resetWizard}
            />
          )}

          {step === 3 && (
            <PreviewStep
              records={records}
              mapping={mapping}
              totalCount={records.length}
              onNext={() => setStep(4)}
              onBack={() => setStep(2)}
            />
          )}

          {step === 4 && (
            <ImportStep
              records={records}
              mapping={mapping}
              projectId={selectedProject}
            />
          )}
        </>
      )}
    </div>
  );
}

export default BulkImport;
