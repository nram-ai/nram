import { useState, useCallback } from "react";
import { useProviderSlots, useTestProviderSlot } from "../hooks/useApi";
import SectionTabs, { type SectionTabTone } from "../components/SectionTabs";
import { useSectionTabParam } from "../hooks/useSectionTabParam";
import type { IconDefinition } from "@fortawesome/fontawesome-svg-core";
import type {
  ProviderSlot,
  TestProviderResult,
  UpdateProviderSlotResult,
} from "../api/client";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faSpinner,
  faCubes,
  faListCheck,
  faDiagramProject,
  faWandMagicSparkles,
  faScaleBalanced,
  faComments,
  faArrowDownWideShort,
} from "../lib/icons";
import {
  providerDisplayName,
  PROVIDER_BADGE_COLORS,
  DEFAULT_BADGE_COLOR,
  maskUrl,
} from "../components/providerSlots/config";
import {
  ProviderSlotEditor,
  StatusDot,
  TestResultDisplay,
} from "../components/providerSlots/ProviderSlotEditor";

// ---------------------------------------------------------------------------
// Provider Slot Card
// ---------------------------------------------------------------------------

function ProviderSlotCard({
  slot,
  disabled,
}: {
  slot: ProviderSlot;
  disabled: boolean;
}) {
  const [editing, setEditing] = useState(false);
  const [testResult, setTestResult] = useState<TestProviderResult | null>(null);
  // Set after a confirmed embedding-model swap so the operator knows re-embedding
  // is in flight. Lives here (not in the editor) so the banner survives after the
  // editor collapses on save.
  const [cascadeResult, setCascadeResult] = useState<UpdateProviderSlotResult | null>(null);

  const testMutation = useTestProviderSlot();

  const label = slot.label || slot.slot;
  const description = slot.description || "";
  const isEmbedding = slot.slot === "embedding";
  const badgeCls = PROVIDER_BADGE_COLORS[slot.type] || DEFAULT_BADGE_COLOR;
  const showModelMax =
    slot.context_window != null &&
    slot.context_window > 0 &&
    slot.context_window_max != null &&
    slot.context_window_max > slot.context_window;

  const handleTest = useCallback(() => {
    setTestResult(null);
    testMutation.mutate(
      {
        slot: slot.slot,
        config: {
          type: slot.type,
          url: slot.url,
          model: slot.model,
        },
      },
      {
        onSuccess: (result) => setTestResult(result),
        onError: () =>
          setTestResult({
            success: false,
            latency_ms: 0,
            message: "Request failed",
          }),
      },
    );
  }, [slot, testMutation]);

  // Shared success path for both the initial save and the confirmed swap: a
  // cascade response carries entity_reembed_queued + row counts; either way the
  // editor closes.
  const handleEditorSaved = useCallback((result: UpdateProviderSlotResult) => {
    if (result.entity_reembed_queued) {
      setCascadeResult(result);
    }
    setEditing(false);
  }, []);

  return (
    <div
      className={`rounded-lg border border-border bg-card shadow-sm ${
        disabled ? "pointer-events-none opacity-50" : ""
      }`}
    >
      {/* Header */}
      <div className="flex items-center justify-between border-b border-border px-5 py-4">
        <div className="flex items-center gap-3">
          <StatusDot configured={slot.configured} healthy={slot.status === "ok"} />
          <div>
            <h3 className="text-sm font-semibold text-foreground">{label}</h3>
            <p className="text-xs text-muted-foreground">{description}</p>
          </div>
        </div>
        {slot.configured && !editing && (
          <span
            className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${badgeCls}`}
          >
            {providerDisplayName(slot.type)}
          </span>
        )}
      </div>

      {/* Body */}
      <div className="px-5 py-4">
        {/* Cascade success banner: shown after a confirmed model switch
            so the operator knows re-embedding is in flight. The memory
            queue drains in the background; the entity loop runs in a
            detached goroutine on the server. */}
        {cascadeResult && (
          <div className="mb-4 rounded-md border border-info/40 bg-info/10 p-3">
            <p className="text-sm font-medium text-info">
              Embedding model switched: {cascadeResult.old_model} →{" "}
              {cascadeResult.new_model}
            </p>
            <p className="mt-1 text-xs text-info">
              {cascadeResult.memory_jobs_enqueued ?? 0} memory re-embed jobs
              queued, {cascadeResult.entities_affected ?? 0} entities queued
              for re-embed in the background. Recall is degraded until the
              workers drain (~5-15 min for typical corpora).
            </p>
            <button
              type="button"
              onClick={() => setCascadeResult(null)}
              className="mt-2 text-xs font-medium text-info hover:text-info"
            >
              Dismiss
            </button>
          </div>
        )}

        {editing || !slot.configured ? (
          <ProviderSlotEditor
            slot={slot}
            onSaved={handleEditorSaved}
            onCancel={slot.configured ? () => setEditing(false) : null}
          />
        ) : (
          <div className="space-y-3">
            {/* Info rows */}
            <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
              <div>
                <span className="text-muted-foreground">URL</span>
                <p className="font-mono text-xs text-foreground">
                  {maskUrl(slot.url)}
                </p>
              </div>
              <div>
                <span className="text-muted-foreground">Model</span>
                <p className="font-medium text-foreground">{slot.model}</p>
              </div>
              {isEmbedding && slot.dimensions != null && (
                <div>
                  <span className="text-muted-foreground">Dimensions</span>
                  <p className="font-medium text-foreground">
                    {slot.dimensions}
                  </p>
                </div>
              )}
              <div>
                <span
                  className="text-muted-foreground"
                  title={
                    (isEmbedding
                      ? "Maximum input length the model can encode in a single request. Memory content longer than this is silently truncated by the provider; small windows (e.g. 2048) are a frequent cause of degraded recall on long memories."
                      : "Maximum input + output tokens the model can process in a single request. Caps the prompt the enrichment pipeline can build (recall hits + memory content) plus the model's response.") +
                    " For Ollama slots this is the lesser of the model's GGUF max and Ollama's configured num_ctx; the model max is shown beneath it when num_ctx is the binding constraint."
                  }
                >
                  Context
                </span>
                <p className="font-medium text-foreground">
                  {slot.context_window != null && slot.context_window > 0 ? (
                    <>
                      {slot.context_window.toLocaleString()}{" "}
                      <span className="text-xs font-normal text-muted-foreground">
                        tokens
                      </span>
                    </>
                  ) : (
                    <span className="text-xs font-normal text-muted-foreground">
                      see provider docs
                    </span>
                  )}
                </p>
                {showModelMax && (
                  <p className="text-xs font-normal text-muted-foreground">
                    model max {slot.context_window_max!.toLocaleString()}
                  </p>
                )}
              </div>
              <div>
                <span className="text-muted-foreground">Status</span>
                <p className="text-xs text-foreground">
                  {slot.status ?? "unknown"}
                </p>
              </div>
              {(slot.custom_header_keys?.length ?? 0) > 0 && (
                <div className="col-span-2">
                  <span className="text-muted-foreground">Custom headers</span>
                  <p className="font-mono text-xs text-foreground">
                    {slot.custom_header_keys!.join(", ")}
                  </p>
                </div>
              )}
              {slot.rerank_method && (
                <div>
                  <span className="text-muted-foreground">Detected method</span>
                  <p className="text-xs text-foreground">
                    {slot.rerank_method === "cross_encoder"
                      ? "cross-encoder (/v1/rerank)"
                      : slot.rerank_method === "judge"
                        ? "LLM judge"
                        : slot.rerank_method}
                  </p>
                </div>
              )}
            </div>

            {/* Health info */}
            <div className="flex items-center gap-4 text-xs text-muted-foreground">
              {slot.latency_ms != null && (
                <span>Latency: {slot.latency_ms}ms</span>
              )}
            </div>

            {/* Actions */}
            <div className="flex gap-2 pt-1">
              <button
                type="button"
                onClick={handleTest}
                disabled={testMutation.isPending}
                className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {testMutation.isPending ? (
                  <span className="flex items-center gap-1.5">
                    <FontAwesomeIcon icon={faSpinner} spin className="h-3.5 w-3.5" />
                    Testing...
                  </span>
                ) : (
                  "Test Connection"
                )}
              </button>
              <button
                type="button"
                onClick={() => {
                  setEditing(true);
                  setTestResult(null);
                }}
                className="rounded-md border border-input px-3 py-1.5 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
              >
                Change Provider
              </button>
            </div>

            {/* Test result */}
            {testResult && <TestResultDisplay result={testResult} />}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

// slotTone maps a slot's configured/health state to a SectionTabs icon tone,
// mirroring StatusDot so the tab strip surfaces each slot's health at a glance.
function slotTone(slot: ProviderSlot): SectionTabTone {
  if (!slot.configured) return "muted";
  return slot.status === "ok" ? "ok" : "error";
}

// Per-slot tab icon, keyed by the canonical slot id (internal/provider/slots.go).
const SLOT_ICONS: Record<string, IconDefinition> = {
  embedding: faCubes,
  fact: faListCheck,
  entity: faDiagramProject,
  query_augment: faWandMagicSparkles,
  ingestion_decision: faScaleBalanced,
  ask: faComments,
  reranker: faArrowDownWideShort,
};

function ProviderConfiguration() {
  const slotsQuery = useProviderSlots();

  const isLoading = slotsQuery.isLoading;
  const isError = slotsQuery.isError;

  // The backend returns the ordered canonical slot list with labels and live
  // status; render it directly (empty until the query resolves).
  const slots: ProviderSlot[] = slotsQuery.data ?? [];

  // Active slot synced to ?slot=, self-healing to the first slot when the param
  // is missing or names a stale/unknown slot.
  const { active: activeSlotId, select: selectSlot } = useSectionTabParam(
    "slot",
    slots.map((s) => s.slot),
  );
  const activeSlot = slots.find((s) => s.slot === activeSlotId);

  return (
    <div className="relative">
      {/* Page header */}
      <div className="mb-6">
        <h1 className="font-display text-3xl text-foreground">
          Provider Configuration
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Configure LLM and embedding providers for vector search and enrichment.
        </p>
      </div>

      {/* Loading state */}
      {isLoading && (
        <div className="flex items-center justify-center py-16">
          <FontAwesomeIcon icon={faSpinner} spin className="h-8 w-8 text-muted-foreground" />
        </div>
      )}

      {/* Error state */}
      {isError && !isLoading && (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4">
          <p className="text-sm text-destructive">
            Failed to load provider configuration. Please try refreshing the
            page.
          </p>
        </div>
      )}

      {/* Content: a tab per slot, with only the active slot's card rendered. */}
      {!isLoading && !isError && activeSlot && (
        <div className="space-y-6">
          <SectionTabs
            ariaLabel="Provider slots"
            active={activeSlot.slot}
            onChange={selectSlot}
            items={slots.map((s) => ({
              id: s.slot,
              label: s.label,
              icon: SLOT_ICONS[s.slot],
              tone: slotTone(s),
            }))}
          />
          <ProviderSlotCard
            key={activeSlot.slot}
            slot={activeSlot}
            disabled={false}
          />
        </div>
      )}
    </div>
  );
}

export default ProviderConfiguration;
