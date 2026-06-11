import { useState, useCallback, useRef, useEffect, type ReactNode } from "react";
import {
  useSettings,
  useSettingsSchema,
  useUpdateSetting,
  useTestExtractionPrompt,
} from "../hooks/useApi";
import type { ExtractionTestResult, Setting, SettingSchema } from "../api/client";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faCheck, faXmark, faSpinner } from "../lib/icons";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Canonical prompt keys. Both fact and entity prompts now have schema entries
// with backend-registered defaults, so the editor reads description and
// default value straight from useSettingsSchema, so no UI fallback is needed.
const FACT_PROMPT_KEY = "enrichment.fact_prompt";
const ENTITY_PROMPT_KEY = "enrichment.entity_prompt";
const AUGMENT_PROMPT_KEY = "enrichment.query_augment.prompt";
const INGESTION_PROMPT_KEY = "enrichment.ingestion_decision.prompt";

// System-instruction companion keys (the static, cacheable half of each phase's
// prompt). Each is edited alongside its dynamic data template below.
const FACT_SYSTEM_PROMPT_KEY = "enrichment.fact_system_prompt";
const ENTITY_SYSTEM_PROMPT_KEY = "enrichment.entity_system_prompt";
const AUGMENT_SYSTEM_PROMPT_KEY = "enrichment.query_augment.system_prompt";
const INGESTION_SYSTEM_PROMPT_KEY = "enrichment.ingestion_decision.system_prompt";

interface SimplePromptSpec {
  key: string;
  systemKey: string;
  title: string;
}

// Title is UI-only; the prompt body default and the per-key description are
// resolved from the admin settings schema at render time so the editor
// cannot drift from the runtime cascade in service.GetDefault. systemKey is the
// static instruction half, rendered as a paired card above the dynamic half.
const DREAMING_PROMPTS: SimplePromptSpec[] = [
  { key: "dreaming.contradiction_prompt", systemKey: "dreaming.contradiction_system_prompt", title: "Contradiction Detection" },
  { key: "dreaming.synthesis_prompt", systemKey: "dreaming.synthesis_system_prompt", title: "Memory Synthesis" },
  { key: "dreaming.alignment_prompt", systemKey: "dreaming.alignment_system_prompt", title: "Alignment Scoring" },
  { key: "dreaming.novelty.judge_prompt", systemKey: "dreaming.novelty.judge_system_prompt", title: "Novelty Judge" },
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
// Spinner
// ---------------------------------------------------------------------------

function Spinner({ className = "h-4 w-4" }: { className?: string }) {
  return <FontAwesomeIcon icon={faSpinner} spin className={className} />;
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
  onSave: (key: string, value: string) => void;
  saving: boolean;
  onTest: () => void;
  testing: boolean;
  testResult: ExtractionTestResult | null;
  sampleInput: string;
  onSampleInputChange: (value: string) => void;
}) {
  const [editValue, setEditValue] = useState(promptData.currentValue);
  const [showDefault, setShowDefault] = useState(false);

  // Sync editValue when promptData changes (e.g. after save).
  useEffect(() => {
    setEditValue(promptData.currentValue);
  }, [promptData.currentValue]);

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
              <span className="inline-flex items-center rounded-full bg-info/20 px-2 py-0.5 text-xs font-medium text-info">
                Customized
              </span>
            )}
            {!isCustomized && (
              <span className="inline-flex items-center rounded-full bg-muted px-2 py-0.5 text-xs font-medium text-muted-foreground">
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
              onClick={() => onSave(promptData.key, editValue)}
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
              {testResult.model ? ` · model: ${testResult.model}` : ""}
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
  onSave: (key: string, value: string) => void;
  saving: boolean;
}) {
  const [editValue, setEditValue] = useState(promptData.currentValue);
  const [showDefault, setShowDefault] = useState(false);

  useEffect(() => {
    setEditValue(promptData.currentValue);
  }, [promptData.currentValue]);

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
              <span className="inline-flex items-center rounded-full bg-info/20 px-2 py-0.5 text-xs font-medium text-info">
                Customized
              </span>
            )}
            {!isCustomized && (
              <span className="inline-flex items-center rounded-full bg-muted px-2 py-0.5 text-xs font-medium text-muted-foreground">
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
              onClick={() => onSave(promptData.key, editValue)}
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
// Prompt Pair Group
// ---------------------------------------------------------------------------

// PromptPairGroup wraps a phase's two halves (system instructions + dynamic
// data template) in a single labeled container so it reads as one unit rather
// than two unrelated stacked cards. The muted container lets the inner cards
// (bg-card) stand out as the two parts of the same prompt.
function PromptPairGroup({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="rounded-xl border border-border bg-muted/30 p-3 sm:p-4">
      <h3 className="mb-3 px-1 text-sm font-semibold text-foreground">{title}</h3>
      <div className="space-y-3">{children}</div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export default function PromptTemplates() {
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
  const [augmentSampleInput, setAugmentSampleInput] = useState("");
  const [ingestionSampleInput, setIngestionSampleInput] = useState("");
  const [factTestResult, setFactTestResult] =
    useState<ExtractionTestResult | null>(null);
  const [entityTestResult, setEntityTestResult] =
    useState<ExtractionTestResult | null>(null);
  const [augmentTestResult, setAugmentTestResult] =
    useState<ExtractionTestResult | null>(null);
  const [ingestionTestResult, setIngestionTestResult] =
    useState<ExtractionTestResult | null>(null);
  const [testingFact, setTestingFact] = useState(false);
  const [testingEntity, setTestingEntity] = useState(false);
  const [testingAugment, setTestingAugment] = useState(false);
  const [testingIngestion, setTestingIngestion] = useState(false);

  // Track the current saved prompt values for testing. A test reconstructs the
  // combined prompt (system + blank line + dynamic) so it exercises exactly what
  // the runtime sends; the dynamic half alone is missing the instructions.
  const factPromptRef = useRef("");
  const entityPromptRef = useRef("");
  const augmentPromptRef = useRef("");
  const ingestionPromptRef = useRef("");
  const factSystemPromptRef = useRef("");
  const entitySystemPromptRef = useRef("");
  const augmentSystemPromptRef = useRef("");
  const ingestionSystemPromptRef = useRef("");

  const showToast = useCallback(
    (message: string, type: "success" | "error") => {
      if (toastTimer.current) clearTimeout(toastTimer.current);
      setToast({ message, type });
      toastTimer.current = setTimeout(() => setToast(null), 3000);
    },
    [],
  );

  const handleSave = useCallback(
    (key: string, value: string) => {
      updateMutation.mutate(
        { key, value },
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

  // Resolve prompt data. Defaults come from the registered schema; the
  // empty-string fallback is only reached if the schema entry is missing,
  // which would itself be a registration bug surfaced at server boot.
  const factPromptData = resolvePromptData(
    [FACT_PROMPT_KEY],
    schemas,
    settingsMap,
    "",
  );
  const entityPromptData = resolvePromptData(
    [ENTITY_PROMPT_KEY],
    schemas,
    settingsMap,
    "",
  );
  const augmentPromptData = resolvePromptData(
    [AUGMENT_PROMPT_KEY],
    schemas,
    settingsMap,
    "",
  );
  const ingestionPromptData = resolvePromptData(
    [INGESTION_PROMPT_KEY],
    schemas,
    settingsMap,
    "",
  );

  // System-instruction halves (static, cacheable). Rendered as a paired card
  // above each dynamic data template.
  const factSystemPromptData = resolvePromptData([FACT_SYSTEM_PROMPT_KEY], schemas, settingsMap, "");
  const entitySystemPromptData = resolvePromptData([ENTITY_SYSTEM_PROMPT_KEY], schemas, settingsMap, "");
  const augmentSystemPromptData = resolvePromptData([AUGMENT_SYSTEM_PROMPT_KEY], schemas, settingsMap, "");
  const ingestionSystemPromptData = resolvePromptData([INGESTION_SYSTEM_PROMPT_KEY], schemas, settingsMap, "");

  const dreamingPrompts = DREAMING_PROMPTS.map((spec) => ({
    spec,
    data: resolvePromptData([spec.key], schemas, settingsMap, ""),
    systemData: resolvePromptData([spec.systemKey], schemas, settingsMap, ""),
  }));

  // Keep refs updated for test calls.
  if (factPromptData) {
    factPromptRef.current = factPromptData.currentValue;
  }
  if (entityPromptData) {
    entityPromptRef.current = entityPromptData.currentValue;
  }
  if (augmentPromptData) {
    augmentPromptRef.current = augmentPromptData.currentValue;
  }
  if (ingestionPromptData) {
    ingestionPromptRef.current = ingestionPromptData.currentValue;
  }
  factSystemPromptRef.current = factSystemPromptData.currentValue;
  entitySystemPromptRef.current = entitySystemPromptData.currentValue;
  augmentSystemPromptRef.current = augmentSystemPromptData.currentValue;
  ingestionSystemPromptRef.current = ingestionSystemPromptData.currentValue;

  const handleTestFact = useCallback(() => {
    if (!factSampleInput.trim()) return;
    setTestingFact(true);
    setFactTestResult(null);
    testMutation.mutate(
      {
        type: "fact",
        prompt: factPromptRef.current,
        systemPrompt: factSystemPromptRef.current,
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

  const handleTestAugment = useCallback(() => {
    if (!augmentSampleInput.trim()) return;
    setTestingAugment(true);
    setAugmentTestResult(null);
    testMutation.mutate(
      {
        type: "augment",
        prompt: augmentPromptRef.current,
        systemPrompt: augmentSystemPromptRef.current,
        sampleInput: augmentSampleInput,
        // Server defaults to 4 when count is omitted, matching
        // SettingQueryAugmentCount's runtime default. Operators tuning the
        // count separately do so through the Settings page.
      },
      {
        onSuccess: (data) => {
          setAugmentTestResult(data);
          setTestingAugment(false);
        },
        onError: (err) => {
          setAugmentTestResult({
            output: "",
            parsed: null,
            error: err.message,
            latency_ms: 0,
          });
          setTestingAugment(false);
        },
      },
    );
  }, [augmentSampleInput, testMutation]);

  const handleTestIngestion = useCallback(() => {
    if (!ingestionSampleInput.trim()) return;
    setTestingIngestion(true);
    setIngestionTestResult(null);
    testMutation.mutate(
      {
        type: "ingestion",
        prompt: ingestionPromptRef.current,
        systemPrompt: ingestionSystemPromptRef.current,
        sampleInput: ingestionSampleInput,
      },
      {
        onSuccess: (data) => {
          setIngestionTestResult(data);
          setTestingIngestion(false);
        },
        onError: (err) => {
          setIngestionTestResult({
            output: "",
            parsed: null,
            error: err.message,
            latency_ms: 0,
          });
          setTestingIngestion(false);
        },
      },
    );
  }, [ingestionSampleInput, testMutation]);

  const handleTestEntity = useCallback(() => {
    if (!entitySampleInput.trim()) return;
    setTestingEntity(true);
    setEntityTestResult(null);
    testMutation.mutate(
      {
        type: "entity",
        prompt: entityPromptRef.current,
        systemPrompt: entitySystemPromptRef.current,
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
        <h1 className="font-display text-3xl text-foreground">
          Prompt Templates
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          The system prompts the model uses for enrichment and dreaming.
          Enrichment prompts decide how facts and entities are pulled out of
          new memories. Dreaming prompts decide how the model consolidates
          knowledge in the background.
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
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4">
          <p className="text-sm text-destructive">
            Failed to load settings. Please try refreshing the page.
          </p>
        </div>
      )}

      {/* Content */}
      {!isLoading && !isError && (
        <div className="space-y-8">
          {/* Each phase is split into a static System Instructions card (the
              cacheable prefix) and a Data Template card (carrying the
              placeholders, with the live test harness). */}

          {/* Fact Extraction Prompt */}
          {factPromptData && (
            <PromptPairGroup title="Fact Extraction">
              <SimplePromptEditorCard
                title="System Instructions"
                description={factSystemPromptData.description}
                promptData={factSystemPromptData}
                onSave={handleSave}
                saving={updateMutation.isPending}
              />
              <PromptEditorCard
                title="Data Template"
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
            </PromptPairGroup>
          )}

          {/* Entity Extraction Prompt */}
          {entityPromptData && (
            <PromptPairGroup title="Entity Extraction">
              <SimplePromptEditorCard
                title="System Instructions"
                description={entitySystemPromptData.description}
                promptData={entitySystemPromptData}
                onSave={handleSave}
                saving={updateMutation.isPending}
              />
              <PromptEditorCard
                title="Data Template"
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
            </PromptPairGroup>
          )}

          {/* Query Augmentation Prompt. Template uses named placeholders
              {content} and {N}, not %s, so operators can safely include
              literal '%' in the body. */}
          {augmentPromptData && (
            <PromptPairGroup title="Query Augmentation">
              <SimplePromptEditorCard
                title="System Instructions"
                description={augmentSystemPromptData.description}
                promptData={augmentSystemPromptData}
                onSave={handleSave}
                saving={updateMutation.isPending}
              />
              <PromptEditorCard
                title="Data Template"
                description="Generates short paraphrased queries the augmentation phase prepends to memory content before embedding, so a single vector captures the fact and the ways someone would ask about it. Off by default; enable in Settings then use Backfill to re-embed pre-flag memories."
                promptData={augmentPromptData}
                onSave={handleSave}
                saving={updateMutation.isPending}
                onTest={handleTestAugment}
                testing={testingAugment}
                testResult={augmentTestResult}
                sampleInput={augmentSampleInput}
                onSampleInputChange={setAugmentSampleInput}
              />
            </PromptPairGroup>
          )}

          {/* Enrichment Prompts Section */}
          {ingestionPromptData && (
            <div className="border-t border-border pt-8">
              <div className="mb-6">
                <h2 className="text-xl font-semibold tracking-tight">
                  Enrichment Prompts
                </h2>
                <p className="mt-1 text-sm text-muted-foreground">
                  Additional prompts used by the enrichment pipeline beyond
                  fact and entity extraction. The ingestion-decision prompt
                  drives the ADD/UPDATE/DELETE/NONE judgment on near-duplicate
                  matches at write time. The test runs the prompt with an empty
                  candidate list against the ingestion-decision provider slot
                  (configured under Provider Configuration), so it exercises the
                  prompt and the model that slot resolves to.
                </p>
              </div>

              <PromptPairGroup title="Ingestion Decision">
                <SimplePromptEditorCard
                  title="System Instructions"
                  description={ingestionSystemPromptData.description}
                  promptData={ingestionSystemPromptData}
                  onSave={handleSave}
                  saving={updateMutation.isPending}
                />
                <PromptEditorCard
                  title="Data Template"
                  description={ingestionPromptData.description}
                  promptData={ingestionPromptData}
                  onSave={handleSave}
                  saving={updateMutation.isPending}
                  onTest={handleTestIngestion}
                  testing={testingIngestion}
                  testResult={ingestionTestResult}
                  sampleInput={ingestionSampleInput}
                  onSampleInputChange={setIngestionSampleInput}
                />
              </PromptPairGroup>
            </div>
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
              {dreamingPrompts.map(({ spec, data, systemData }) => (
                <PromptPairGroup key={spec.key} title={spec.title}>
                  <SimplePromptEditorCard
                    title="System Instructions"
                    description={systemData.description}
                    promptData={systemData}
                    onSave={handleSave}
                    saving={updateMutation.isPending}
                  />
                  <SimplePromptEditorCard
                    title="Data Template"
                    description={data.description}
                    promptData={data}
                    onSave={handleSave}
                    saving={updateMutation.isPending}
                  />
                </PromptPairGroup>
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
