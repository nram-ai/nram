import { useState, useCallback, useRef, useEffect } from "react";
import {
  useSettings,
  useSettingsSchema,
  useUpdateSetting,
  useTestExtractionPrompt,
} from "../hooks/useApi";
import type { Setting, SettingSchema } from "../api/client";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const FACT_PROMPT_KEYS = [
  "enrichment.extraction_prompt",
  "enrichment.fact_extraction_prompt",
  "enrichment.fact_prompt",
];

const ENTITY_PROMPT_KEYS = [
  "enrichment.entity_prompt",
  "enrichment.entity_extraction_prompt",
];

const DEFAULT_FACT_PROMPT = `You are a memory extraction system. Given the following text, extract discrete, standalone facts that would be useful to remember about the user or context in future conversations.

Rules:
- Each fact must be self-contained (understandable without the original text)
- Prefer specific over vague ("lives in Denver" not "lives somewhere in Colorado")
- Include temporal context when relevant ("as of March 2026")
- Assign confidence 0.0-1.0 based on how explicitly the fact was stated vs inferred
- Skip pleasantries, filler, and procedural content

Respond ONLY as a JSON array, no markdown fences, no preamble:
[{"fact": "...", "confidence": 0.95}, ...]`;

const DEFAULT_ENTITY_PROMPT = `You are an entity and relationship extraction system. Given the following text, extract entities (people, organizations, technologies, places, concepts) and the relationships between them.

Rules:
- Each entity needs a name, a type, and optionally key properties
- Each relationship needs a source entity, target entity, relationship label, and temporal qualifier
- Temporal qualifiers: "current" (default), "as of <date>", "previously", "no longer"
- Normalize entity names
- Include relationship directionality

Respond ONLY as JSON, no markdown fences, no preamble:
{
  "entities": [{"name": "...", "type": "person|org|tech|place|concept", "properties": {}}],
  "relationships": [{"source": "...", "target": "...", "relation": "...", "temporal": "current"}]
}`;

interface DreamingPromptSpec {
  key: string;
  title: string;
}

// Title is UI-only; the prompt body default and the per-key description are
// resolved from the admin settings schema at render time so the editor
// cannot drift from the runtime cascade in service.GetDefault.
const DREAMING_PROMPTS: DreamingPromptSpec[] = [
  { key: "dreaming.contradiction_prompt", title: "Contradiction Detection Prompt" },
  { key: "dreaming.synthesis_prompt", title: "Memory Synthesis Prompt" },
  { key: "dreaming.alignment_prompt", title: "Alignment Scoring Prompt" },
  { key: "dreaming.novelty.judge_prompt", title: "Novelty Judge Prompt" },
];

const SAMPLE_INPUT_PLACEHOLDER = `Enter sample text to test extraction against, for example:

"John Smith works at Acme Corp as a senior engineer. He joined in January 2025 and primarily works with Python and Go. The company is headquartered in San Francisco and recently expanded to Austin, Texas."`;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface PromptData {
  key: string;
  currentValue: string;
  defaultValue: string;
  scope: string;
  isModified: boolean;
  description: string;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function resolvePromptData(
  keys: string[],
  schemas: SettingSchema[],
  settingsMap: Map<string, Setting>,
  fallbackDefault: string,
): PromptData {
  // Find the first matching schema key.
  for (const key of keys) {
    const schema = schemas.find((s) => s.key === key);
    if (schema) {
      const setting = settingsMap.get(key);
      const defaultVal =
        typeof schema.default_value === "string"
          ? schema.default_value
          : fallbackDefault;
      const currentVal = setting
        ? typeof setting.value === "string"
          ? setting.value
          : JSON.stringify(setting.value)
        : defaultVal;
      return {
        key,
        currentValue: currentVal,
        defaultValue: defaultVal,
        scope: setting?.scope ?? "global",
        isModified: setting !== null && setting !== undefined,
        description: schema.description,
      };
    }
  }

  let description = "System prompt";
  if (keys[0].includes("fact")) {
    description = "System prompt for extracting structured facts from memory content";
  } else if (keys[0].includes("entity")) {
    description = "System prompt for extracting entities and relationships from memory content";
  }

  return {
    key: keys[0],
    currentValue: fallbackDefault,
    defaultValue: fallbackDefault,
    scope: "global",
    isModified: false,
    description,
  };
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
      className={`fixed bottom-4 right-4 z-50 flex items-center gap-2 rounded-md px-4 py-2.5 text-sm font-medium shadow-lg transition-all ${
        type === "success"
          ? "bg-green-50 text-green-800 dark:bg-green-900/80 dark:text-green-200"
          : "bg-red-50 text-red-800 dark:bg-red-900/80 dark:text-red-200"
      }`}
    >
      {type === "success" ? (
        <svg
          className="h-4 w-4 flex-shrink-0"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M5 13l4 4L19 7"
          />
        </svg>
      ) : (
        <svg
          className="h-4 w-4 flex-shrink-0"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M6 18L18 6M6 6l12 12"
          />
        </svg>
      )}
      {message}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Spinner
// ---------------------------------------------------------------------------

function Spinner({ className = "h-4 w-4" }: { className?: string }) {
  return (
    <svg className={`animate-spin ${className}`} fill="none" viewBox="0 0 24 24">
      <circle
        className="opacity-25"
        cx="12"
        cy="12"
        r="10"
        stroke="currentColor"
        strokeWidth="4"
      />
      <path
        className="opacity-75"
        fill="currentColor"
        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
      />
    </svg>
  );
}

// ---------------------------------------------------------------------------
// Line-numbered Textarea
// ---------------------------------------------------------------------------

function LineNumberedTextarea({
  value,
  onChange,
  rows,
  placeholder,
  readOnly,
}: {
  value: string;
  onChange: (value: string) => void;
  rows: number;
  placeholder?: string;
  readOnly?: boolean;
}) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const lineNumbersRef = useRef<HTMLDivElement>(null);
  const lines = value.split("\n");
  const lineCount = Math.max(lines.length, rows);

  const handleScroll = useCallback(() => {
    if (textareaRef.current && lineNumbersRef.current) {
      lineNumbersRef.current.scrollTop = textareaRef.current.scrollTop;
    }
  }, []);

  return (
    <div className="relative flex rounded-md border border-input bg-background shadow-sm focus-within:ring-2 focus-within:ring-ring">
      <div
        ref={lineNumbersRef}
        className="select-none overflow-hidden border-r border-input bg-muted/50 px-2 py-2 text-right font-mono text-xs leading-[1.625rem] text-muted-foreground"
        style={{ minWidth: "3rem" }}
      >
        {Array.from({ length: lineCount }, (_, i) => (
          <div key={i}>{i + 1}</div>
        ))}
      </div>
      <textarea
        ref={textareaRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onScroll={handleScroll}
        rows={rows}
        readOnly={readOnly}
        placeholder={placeholder}
        className="w-full resize-y bg-transparent px-3 py-2 font-mono text-sm leading-[1.625rem] focus:outline-none"
        spellCheck={false}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Prompt Editor Card
// ---------------------------------------------------------------------------

function PromptEditorCard({
  title,
  description,
  promptData,
  onSave,
  saving,
  onTest,
  testing,
  testResult,
  sampleInput,
  onSampleInputChange,
}: {
  title: string;
  description: string;
  promptData: PromptData;
  onSave: (key: string, value: string, scope: string) => void;
  saving: boolean;
  onTest: () => void;
  testing: boolean;
  testResult: {
    output: string;
    parsed: unknown;
    error?: string;
    latency_ms: number;
  } | null;
  sampleInput: string;
  onSampleInputChange: (value: string) => void;
}) {
  const [editValue, setEditValue] = useState(promptData.currentValue);
  const [editScope, setEditScope] = useState(promptData.scope);
  const [showDefault, setShowDefault] = useState(false);

  // Sync editValue when promptData changes (e.g. after save).
  useEffect(() => {
    setEditValue(promptData.currentValue);
    setEditScope(promptData.scope);
  }, [promptData.currentValue, promptData.scope]);

  const hasChanges = editValue !== promptData.currentValue;
  const isCustomized = editValue !== promptData.defaultValue;

  return (
    <div className="rounded-lg border border-border bg-card shadow-sm">
      {/* Header */}
      <div className="border-b border-border px-5 py-4">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 className="text-lg font-semibold text-foreground">{title}</h2>
            <p className="mt-1 text-xs text-muted-foreground">{description}</p>
          </div>
          <div className="flex items-center gap-2">
            {isCustomized && (
              <span className="inline-flex items-center rounded-full bg-blue-100 px-2 py-0.5 text-xs font-medium text-blue-800 dark:bg-blue-900 dark:text-blue-300">
                Customized
              </span>
            )}
            {!isCustomized && (
              <span className="inline-flex items-center rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-600 dark:bg-gray-800 dark:text-gray-400">
                Default
              </span>
            )}
            <span className="text-xs text-muted-foreground">
              Key: <code className="font-mono">{promptData.key}</code>
            </span>
          </div>
        </div>
      </div>

      {/* Prompt Editor */}
      <div className="px-5 py-4 space-y-4">
        {/* Scope selector */}
        <div className="flex items-center gap-3">
          <label className="text-xs font-medium text-muted-foreground">
            Scope:
          </label>
          <select
            value={editScope}
            onChange={(e) => setEditScope(e.target.value)}
            className="rounded-md border border-input bg-background px-2 py-1 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          >
            <option value="global">Global</option>
            <option value="project">Project</option>
          </select>
        </div>

        {/* Textarea with line numbers */}
        <LineNumberedTextarea
          value={editValue}
          onChange={setEditValue}
          rows={14}
          placeholder="Enter extraction prompt..."
        />

        {/* Action buttons */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => onSave(promptData.key, editValue, editScope)}
              disabled={saving || !hasChanges}
              className="rounded-md bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {saving ? (
                <span className="flex items-center gap-1.5">
                  <Spinner className="h-3.5 w-3.5" />
                  Saving...
                </span>
              ) : (
                "Save Prompt"
              )}
            </button>
            {hasChanges && (
              <button
                type="button"
                onClick={() => setEditValue(promptData.currentValue)}
                className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
              >
                Discard Changes
              </button>
            )}
            {isCustomized && (
              <button
                type="button"
                onClick={() => setEditValue(promptData.defaultValue)}
                className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-muted-foreground shadow-sm hover:bg-muted"
              >
                Reset to Default
              </button>
            )}
          </div>
          <button
            type="button"
            onClick={() => setShowDefault(!showDefault)}
            className="text-xs text-muted-foreground hover:text-foreground underline"
          >
            {showDefault ? "Hide default prompt" : "Show default prompt"}
          </button>
        </div>

        {/* Default prompt comparison */}
        {showDefault && (
          <div className="rounded-md border border-border bg-muted/30 p-4">
            <h4 className="mb-2 text-xs font-medium text-muted-foreground uppercase tracking-wider">
              Default Prompt
            </h4>
            <pre className="whitespace-pre-wrap font-mono text-xs text-foreground leading-relaxed">
              {promptData.defaultValue}
            </pre>
          </div>
        )}
      </div>

      {/* Test Section */}
      <div className="border-t border-border px-5 py-4 space-y-3">
        <h3 className="text-sm font-medium text-foreground">
          Test with Sample Input
        </h3>
        <p className="text-xs text-muted-foreground">
          Enter sample memory content below and run the prompt through the
          configured LLM provider to see parsed output.
        </p>

        <textarea
          value={sampleInput}
          onChange={(e) => onSampleInputChange(e.target.value)}
          rows={4}
          placeholder={SAMPLE_INPUT_PLACEHOLDER}
          className="w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          spellCheck={false}
        />

        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={onTest}
            disabled={testing || !sampleInput.trim()}
            className="rounded-md bg-orange-600 px-4 py-1.5 text-sm font-medium text-white shadow-sm hover:bg-orange-700 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {testing ? (
              <span className="flex items-center gap-1.5">
                <Spinner className="h-3.5 w-3.5" />
                Running...
              </span>
            ) : (
              "Test Extraction"
            )}
          </button>
          {testResult && (
            <span className="text-xs text-muted-foreground">
              Completed in {testResult.latency_ms}ms
            </span>
          )}
        </div>

        {/* Test Results */}
        {testResult && (
          <div className="space-y-3">
            {testResult.error && (
              <div className="rounded-md border border-yellow-300 bg-yellow-50 px-3 py-2 dark:border-yellow-800 dark:bg-yellow-900/30">
                <p className="text-xs text-yellow-800 dark:text-yellow-300">
                  {testResult.error}
                </p>
              </div>
            )}

            {/* Parsed Output */}
            {testResult.parsed != null && (
              <div>
                <h4 className="mb-1.5 text-xs font-medium text-muted-foreground uppercase tracking-wider">
                  Parsed Output
                </h4>
                <pre className="max-h-64 overflow-auto rounded-md bg-muted p-3 font-mono text-xs text-foreground leading-relaxed">
                  {JSON.stringify(testResult.parsed, null, 2)}
                </pre>
              </div>
            )}

            {/* Raw Output */}
            <details className="group">
              <summary className="cursor-pointer text-xs font-medium text-muted-foreground hover:text-foreground">
                Raw LLM Output
              </summary>
              <pre className="mt-1.5 max-h-48 overflow-auto rounded-md bg-muted p-3 font-mono text-xs text-foreground leading-relaxed">
                {testResult.output}
              </pre>
            </details>
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Simple Prompt Editor Card (no test section)
// ---------------------------------------------------------------------------

function SimplePromptEditorCard({
  title,
  description,
  promptData,
  onSave,
  saving,
}: {
  title: string;
  description: string;
  promptData: PromptData;
  onSave: (key: string, value: string, scope: string) => void;
  saving: boolean;
}) {
  const [editValue, setEditValue] = useState(promptData.currentValue);
  const [editScope, setEditScope] = useState(promptData.scope);
  const [showDefault, setShowDefault] = useState(false);

  useEffect(() => {
    setEditValue(promptData.currentValue);
    setEditScope(promptData.scope);
  }, [promptData.currentValue, promptData.scope]);

  const hasChanges = editValue !== promptData.currentValue;
  const isCustomized = editValue !== promptData.defaultValue;

  return (
    <div className="rounded-lg border border-border bg-card shadow-sm">
      <div className="border-b border-border px-5 py-4">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h3 className="text-base font-semibold text-foreground">{title}</h3>
            <p className="mt-1 text-xs text-muted-foreground">{description}</p>
          </div>
          <div className="flex items-center gap-2">
            {isCustomized && (
              <span className="inline-flex items-center rounded-full bg-blue-100 px-2 py-0.5 text-xs font-medium text-blue-800 dark:bg-blue-900 dark:text-blue-300">
                Customized
              </span>
            )}
            {!isCustomized && (
              <span className="inline-flex items-center rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-600 dark:bg-gray-800 dark:text-gray-400">
                Default
              </span>
            )}
            <span className="text-xs text-muted-foreground">
              Key: <code className="font-mono">{promptData.key}</code>
            </span>
          </div>
        </div>
      </div>

      <div className="px-5 py-4 space-y-4">
        <div className="flex items-center gap-3">
          <label className="text-xs font-medium text-muted-foreground">
            Scope:
          </label>
          <select
            value={editScope}
            onChange={(e) => setEditScope(e.target.value)}
            className="rounded-md border border-input bg-background px-2 py-1 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-ring"
          >
            <option value="global">Global</option>
          </select>
        </div>

        <LineNumberedTextarea
          value={editValue}
          onChange={setEditValue}
          rows={10}
          placeholder="Enter prompt..."
        />

        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => onSave(promptData.key, editValue, editScope)}
              disabled={saving || !hasChanges}
              className="rounded-md bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {saving ? (
                <span className="flex items-center gap-1.5">
                  <Spinner className="h-3.5 w-3.5" />
                  Saving...
                </span>
              ) : (
                "Save Prompt"
              )}
            </button>
            {hasChanges && (
              <button
                type="button"
                onClick={() => setEditValue(promptData.currentValue)}
                className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
              >
                Discard Changes
              </button>
            )}
            {isCustomized && (
              <button
                type="button"
                onClick={() => setEditValue(promptData.defaultValue)}
                className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-muted-foreground shadow-sm hover:bg-muted"
              >
                Reset to Default
              </button>
            )}
          </div>
          <button
            type="button"
            onClick={() => setShowDefault(!showDefault)}
            className="text-xs text-muted-foreground hover:text-foreground underline"
          >
            {showDefault ? "Hide default prompt" : "Show default prompt"}
          </button>
        </div>

        {showDefault && (
          <div className="rounded-md border border-border bg-muted/30 p-4">
            <h4 className="mb-2 text-xs font-medium text-muted-foreground uppercase tracking-wider">
              Default Prompt
            </h4>
            <pre className="whitespace-pre-wrap font-mono text-xs text-foreground leading-relaxed">
              {promptData.defaultValue}
            </pre>
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export default function ExtractionPromptEditor() {
  const settingsQuery = useSettings();
  const schemaQuery = useSettingsSchema();
  const updateMutation = useUpdateSetting();
  const testMutation = useTestExtractionPrompt();

  const [toast, setToast] = useState<{
    message: string;
    type: "success" | "error";
  } | null>(null);
  const toastTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const [factSampleInput, setFactSampleInput] = useState("");
  const [entitySampleInput, setEntitySampleInput] = useState("");
  const [factTestResult, setFactTestResult] = useState<{
    output: string;
    parsed: unknown;
    error?: string;
    latency_ms: number;
  } | null>(null);
  const [entityTestResult, setEntityTestResult] = useState<{
    output: string;
    parsed: unknown;
    error?: string;
    latency_ms: number;
  } | null>(null);
  const [testingFact, setTestingFact] = useState(false);
  const [testingEntity, setTestingEntity] = useState(false);

  // Track the current prompt values for testing (updated when textarea changes).
  const factPromptRef = useRef("");
  const entityPromptRef = useRef("");

  const showToast = useCallback(
    (message: string, type: "success" | "error") => {
      if (toastTimer.current) clearTimeout(toastTimer.current);
      setToast({ message, type });
      toastTimer.current = setTimeout(() => setToast(null), 3000);
    },
    [],
  );

  const handleSave = useCallback(
    (key: string, value: string, scope: string) => {
      updateMutation.mutate(
        { key, value, scope },
        {
          onSuccess: () => showToast(`Saved "${key}"`, "success"),
          onError: (err) =>
            showToast(`Failed to save "${key}": ${err.message}`, "error"),
        },
      );
    },
    [updateMutation, showToast],
  );

  const isLoading = settingsQuery.isLoading || schemaQuery.isLoading;
  const isError = settingsQuery.isError || schemaQuery.isError;

  // Build merged data.
  const schemas = schemaQuery.data?.data ?? [];
  const settings = settingsQuery.data?.data ?? [];
  const settingsMap = new Map(settings.map((s) => [s.key, s]));

  // Resolve prompt data.
  const factPromptData = resolvePromptData(
    FACT_PROMPT_KEYS,
    schemas,
    settingsMap,
    DEFAULT_FACT_PROMPT,
  );
  const entityPromptData = resolvePromptData(
    ENTITY_PROMPT_KEYS,
    schemas,
    settingsMap,
    DEFAULT_ENTITY_PROMPT,
  );

  const dreamingPrompts = DREAMING_PROMPTS.map((spec) => ({
    spec,
    data: resolvePromptData([spec.key], schemas, settingsMap, ""),
  }));

  // Keep refs updated for test calls.
  if (factPromptData) {
    factPromptRef.current = factPromptData.currentValue;
  }
  if (entityPromptData) {
    entityPromptRef.current = entityPromptData.currentValue;
  }

  const handleTestFact = useCallback(() => {
    if (!factSampleInput.trim()) return;
    setTestingFact(true);
    setFactTestResult(null);
    testMutation.mutate(
      {
        type: "fact",
        prompt: factPromptRef.current,
        sampleInput: factSampleInput,
      },
      {
        onSuccess: (data) => {
          setFactTestResult(data);
          setTestingFact(false);
        },
        onError: (err) => {
          setFactTestResult({
            output: "",
            parsed: null,
            error: err.message,
            latency_ms: 0,
          });
          setTestingFact(false);
        },
      },
    );
  }, [factSampleInput, testMutation]);

  const handleTestEntity = useCallback(() => {
    if (!entitySampleInput.trim()) return;
    setTestingEntity(true);
    setEntityTestResult(null);
    testMutation.mutate(
      {
        type: "entity",
        prompt: entityPromptRef.current,
        sampleInput: entitySampleInput,
      },
      {
        onSuccess: (data) => {
          setEntityTestResult(data);
          setTestingEntity(false);
        },
        onError: (err) => {
          setEntityTestResult({
            output: "",
            parsed: null,
            error: err.message,
            latency_ms: 0,
          });
          setTestingEntity(false);
        },
      },
    );
  }, [entitySampleInput, testMutation]);

  return (
    <div>
      {/* Page header */}
      <div className="mb-6">
        <h1 className="text-2xl font-semibold tracking-tight">
          Prompt Editor
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Edit the system prompts used by the enrichment and dreaming pipelines.
          Extraction prompts control how facts and entities are extracted from
          memories. Dreaming prompts control how the system consolidates
          knowledge during background processing.
        </p>
      </div>

      {/* Loading state */}
      {isLoading && (
        <div className="flex items-center justify-center py-16">
          <Spinner className="h-8 w-8 text-muted-foreground" />
        </div>
      )}

      {/* Error state */}
      {isError && !isLoading && (
        <div className="rounded-lg border border-red-300 bg-red-50 p-4 dark:border-red-800 dark:bg-red-900/30">
          <p className="text-sm text-red-800 dark:text-red-300">
            Failed to load settings. Please try refreshing the page.
          </p>
        </div>
      )}

      {/* Content */}
      {!isLoading && !isError && (
        <div className="space-y-8">
          {/* Fact Extraction Prompt */}
          {factPromptData && (
            <PromptEditorCard
              title="Fact Extraction Prompt"
              description="This prompt instructs the LLM to extract discrete, standalone facts from memory content. Facts are stored as separate memories with confidence scores."
              promptData={factPromptData}
              onSave={handleSave}
              saving={updateMutation.isPending}
              onTest={handleTestFact}
              testing={testingFact}
              testResult={factTestResult}
              sampleInput={factSampleInput}
              onSampleInputChange={setFactSampleInput}
            />
          )}

          {/* Entity Extraction Prompt */}
          {entityPromptData && (
            <PromptEditorCard
              title="Entity Extraction Prompt"
              description="This prompt instructs the LLM to identify entities (people, organizations, technologies, places) and their relationships from memory content. Results populate the knowledge graph."
              promptData={entityPromptData}
              onSave={handleSave}
              saving={updateMutation.isPending}
              onTest={handleTestEntity}
              testing={testingEntity}
              testResult={entityTestResult}
              sampleInput={entitySampleInput}
              onSampleInputChange={setEntitySampleInput}
            />
          )}

          {/* Dreaming Prompts Section */}
          <div className="border-t border-border pt-8">
            <div className="mb-6">
              <h2 className="text-xl font-semibold tracking-tight">
                Dreaming Prompts
              </h2>
              <p className="mt-1 text-sm text-muted-foreground">
                These prompts are used by the dreaming system during background
                memory consolidation. They control how the LLM detects
                contradictions, synthesizes related memories, scores alignment
                between new evidence and existing knowledge, and audits the
                novelty of each synthesis. Prompts use %s placeholders for
                content injection.
              </p>
            </div>

            <div className="space-y-6">
              {dreamingPrompts.map(({ spec, data }) => (
                <SimplePromptEditorCard
                  key={spec.key}
                  title={spec.title}
                  description={data.description}
                  promptData={data}
                  onSave={handleSave}
                  saving={updateMutation.isPending}
                />
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Toast notification */}
      {toast && <StatusToast message={toast.message} type={toast.type} />}
    </div>
  );
}
