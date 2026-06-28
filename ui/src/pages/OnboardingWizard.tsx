import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  useSetupStatus,
  useProviderSlots,
  useUpdateOnboarding,
  useSettings,
  useUpdateSetting,
} from "../hooks/useApi";
import { useMeCapabilities } from "../hooks/useEnrichmentAvailable";
import { useAuth } from "../context/AuthContext";
import type { ProviderSlot } from "../api/client";
import { ProviderSlotEditor } from "../components/providerSlots/ProviderSlotEditor";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faCircleInfo,
  faTriangleExclamation,
  faCircleCheck,
  faChevronDown,
  faChevronRight,
} from "../lib/icons";
import { CopyButton } from "../components/CopyButton";

// The non-provider steps that always follow the provider slots, in order. The
// provider steps are derived at runtime from the live canonical slot list
// (provider.Slots, served via GET /admin/providers with a `required` flag and
// the render order): required slots become one step each, optional slots are
// grouped into the "optional" step. The persisted cursor (PUT
// /admin/setup/onboarding) is a slot name or one of these sentinels.
const TAIL_STEPS = ["optional", "settings", "finish"] as const;

// Local onboarding rationale per required slot, keyed by slot name. Richer than
// the backend slot description; a required slot with no entry here falls back to
// its description, so adding a required slot needs no edit to keep working.
const REQUIRED_WHY: Record<string, string> = {
  embedding:
    "Turns every memory into a vector so recall finds things by meaning, not just keyword match. Without it, memories are stored as raw text and semantic search is unavailable. This is the one provider almost every feature depends on.",
  fact: "Reads each new memory and extracts the durable facts worth keeping. Powers enrichment, near-duplicate handling, and higher-quality recall.",
  entity:
    "Identifies the people, projects, and concepts in your memories and the relationships between them, building the knowledge graph.",
};

// The four high-level feature switches surfaced during onboarding, with their
// known defaults and provider dependency. Sub-toggles stay on the Settings page.
type ToggleDep = "enrichment" | "ask" | "reranker";
const FEATURE_TOGGLES: {
  key: string;
  label: string;
  blurb: string;
  defaultOn: boolean;
  dep: ToggleDep;
}[] = [
  {
    key: "enrichment.enabled",
    label: "Enrichment pipeline",
    blurb: "Run the background pipeline that extracts facts and entities from new memories.",
    defaultOn: true,
    dep: "enrichment",
  },
  {
    key: "dreaming.enabled",
    label: "Dreaming",
    blurb: "Periodically consolidate, dedup, and synthesize memories in the background (the dreaming cycle).",
    defaultOn: true,
    dep: "enrichment",
  },
  {
    key: "ask.enabled",
    label: "Ask synthesis",
    blurb: "Enable the ask tool, which synthesizes a single cited answer over recalled memories. Spends model tokens per call.",
    defaultOn: false,
    dep: "ask",
  },
  {
    key: "ranking.rerank.enabled",
    label: "Reranking",
    blurb: "Re-score recall candidates with the reranker for sharper top results.",
    defaultOn: false,
    dep: "reranker",
  },
];

const DEP_HINT: Record<ToggleDep, string> = {
  enrichment: "Configure the embedding, fact, and entity providers to enable.",
  ask: "Configure the Ask provider to enable.",
  reranker: "Configure the Reranker provider to enable.",
};

function StepHeader({
  index,
  total,
  title,
  subtitle,
}: {
  index: number;
  total: number;
  title: string;
  subtitle: string;
}) {
  return (
    <div className="mb-6">
      <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        Step {index + 1} of {total}
      </p>
      <h1 className="mt-1 font-display text-3xl text-foreground">{title}</h1>
      <p className="mt-2 text-sm text-muted-foreground">{subtitle}</p>
    </div>
  );
}

function NavRow({
  onBack,
  onSkip,
  skipLabel,
  primary,
}: {
  onBack: (() => void) | null;
  onSkip?: (() => void) | null;
  skipLabel?: string;
  primary?: { label: string; onClick: () => void; disabled?: boolean } | null;
}) {
  return (
    <div className="mt-6 flex items-center justify-between border-t border-border pt-4">
      <div>
        {onBack && (
          <button
            type="button"
            onClick={onBack}
            className="rounded-md border border-input px-4 py-2 text-sm font-medium text-foreground shadow-sm hover:bg-muted"
          >
            Back
          </button>
        )}
      </div>
      <div className="flex items-center gap-2">
        {onSkip && (
          <button
            type="button"
            onClick={onSkip}
            className="rounded-md px-4 py-2 text-sm font-medium text-muted-foreground hover:text-foreground"
          >
            {skipLabel ?? "Skip"}
          </button>
        )}
        {primary && (
          <button
            type="button"
            onClick={primary.onClick}
            disabled={primary.disabled}
            className="rounded-md bg-primary px-5 py-2 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {primary.label}
          </button>
        )}
      </div>
    </div>
  );
}

function CodeBlock({ code, label }: { code: string; label?: string }) {
  const cls =
    "inline-flex items-center gap-1.5 rounded-md border border-border bg-card px-3 py-1.5 text-xs font-medium text-foreground transition-colors hover:bg-accent focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2";
  return (
    <div className="rounded-lg border border-border bg-muted/50">
      <div className="flex items-center justify-between border-b border-border px-4 py-2">
        {label && <span className="text-xs font-medium text-muted-foreground">{label}</span>}
        <CopyButton text={code} withIcon className={cls} />
      </div>
      <pre className="overflow-x-auto p-4 text-sm leading-relaxed text-foreground">
        <code>{code}</code>
      </pre>
    </div>
  );
}

function ConfiguredBadge({ slot }: { slot: ProviderSlot | undefined }) {
  if (!slot?.configured) return null;
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full bg-success/10 px-2.5 py-0.5 text-xs font-medium text-success">
      <FontAwesomeIcon icon={faCircleCheck} className="h-3.5 w-3.5" />
      Configured: {slot.model}
    </span>
  );
}

function OnboardingWizard() {
  const navigate = useNavigate();
  const auth = useAuth();
  const { data: status, isLoading: statusLoading } = useSetupStatus();
  const slotsQuery = useProviderSlots();
  const { data: caps } = useMeCapabilities();
  const settingsQuery = useSettings();
  const updateOnboarding = useUpdateOnboarding();
  const updateSetting = useUpdateSetting();

  const slots = useMemo<ProviderSlot[]>(() => slotsQuery.data ?? [], [slotsQuery.data]);
  const slotByName = (name: string) => slots.find((s) => s.slot === name);

  // Provider steps are the required slots (one each), in canonical order; the
  // optional slots are grouped into the "optional" tail step. Both partitions
  // come straight from the live list, so the wizard tracks provider.Slots.
  const requiredSlots = useMemo(() => slots.filter((s) => s.required), [slots]);
  const optionalSlots = useMemo(() => slots.filter((s) => !s.required), [slots]);
  const stepKeys = useMemo<string[]>(
    () => [...requiredSlots.map((s) => s.slot), ...TAIL_STEPS],
    [requiredSlots],
  );

  const [stepIndex, setStepIndex] = useState(0);
  const initialized = useRef(false);
  const [expandedOptional, setExpandedOptional] = useState<string | null>(null);

  // Restore the cursor once both the persisted step and the live slot state are
  // available: jump to the furthest of the persisted step and the first required
  // slot still unconfigured, so configured slots are not re-asked while the
  // cursor still drives the later non-derivable steps. firstUnconfigured indexes
  // requiredSlots, which are stepKeys[0..n-1] in the same order.
  useEffect(() => {
    if (initialized.current) return;
    if (statusLoading || slotsQuery.isLoading || !status) return;

    const firstUnconfigured = requiredSlots.findIndex((s) => !s.configured);
    const requiredIdx = firstUnconfigured === -1 ? stepKeys.indexOf("optional") : firstUnconfigured;
    const persistedIdx = stepKeys.indexOf(status.onboarding_step);
    setStepIndex(Math.max(0, requiredIdx, persistedIdx));
    initialized.current = true;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [statusLoading, slotsQuery.isLoading, status]);

  // Guard: only administrators run onboarding; bounce everyone else (and anyone
  // who has already finished) to the dashboard. Setup must be complete first.
  if (!statusLoading && status) {
    if (!status.setup_complete) {
      navigate("/setup", { replace: true });
      return null;
    }
    if (status.onboarding_complete || !auth.isAdmin) {
      navigate("/", { replace: true });
      return null;
    }
  }

  if (statusLoading || slotsQuery.isLoading || !status) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center">
        <div className="text-sm text-muted-foreground">Loading setup...</div>
      </div>
    );
  }

  const stepKey = stepKeys[stepIndex];

  const goTo = (index: number) => {
    const clamped = Math.min(Math.max(index, 0), stepKeys.length - 1);
    setStepIndex(clamped);
    updateOnboarding.mutate({ step: stepKeys[clamped] });
  };
  const next = () => goTo(stepIndex + 1);
  // Back does not persist the cursor: the saved step is a high-water mark (the
  // furthest reached), so restore lands you forward, not back where you stepped.
  const back = stepIndex > 0 ? () => setStepIndex(stepIndex - 1) : null;

  const finish = () => {
    updateOnboarding.mutate(
      { step: "finish", complete: true },
      { onSuccess: () => navigate("/", { replace: true }) },
    );
  };

  // --- Required provider step -------------------------------------------------
  function renderRequiredStep(slotName: string) {
    const slot = slotByName(slotName);
    const why = REQUIRED_WHY[slotName] ?? slot?.description ?? "";
    return (
      <>
        <StepHeader
          index={stepIndex}
          total={stepKeys.length}
          title={`${slot?.label ?? slotName} provider`}
          subtitle={slot?.description ?? ""}
        />
        <div className="mb-4 flex items-start gap-3 rounded-lg border border-info/40 bg-info/10 px-4 py-3">
          <FontAwesomeIcon icon={faCircleInfo} className="mt-0.5 h-5 w-5 shrink-0 text-info" />
          <p className="text-sm text-info">{why}</p>
        </div>
        <ConfiguredBadge slot={slot} />
        <div className="mt-4 rounded-lg border border-border bg-card p-5">
          {slot ? (
            <ProviderSlotEditor
              key={slot.slot}
              slot={slot}
              showTest
              requireTest
              onSaved={() => next()}
              onCancel={null}
            />
          ) : (
            <p className="text-sm text-muted-foreground">Loading slot…</p>
          )}
        </div>
        <NavRow
          onBack={back}
          onSkip={next}
          skipLabel="Skip for now"
          primary={slot?.configured ? { label: "Continue", onClick: next } : null}
        />
      </>
    );
  }

  // --- Optional providers step ------------------------------------------------
  function renderOptionalStep() {
    return (
      <>
        <StepHeader
          index={stepIndex}
          total={stepKeys.length}
          title="Optional providers"
          subtitle="These sharpen recall and unlock extra features. All are optional and can be configured later from Settings → Providers. Skip the whole step if you want."
        />
        <div className="space-y-3">
          {optionalSlots.map((slot) => {
            const slotName = slot.slot;
            const open = expandedOptional === slotName;
            return (
              <div key={slotName} className="rounded-lg border border-border bg-card">
                <button
                  type="button"
                  onClick={() => setExpandedOptional(open ? null : slotName)}
                  className="flex w-full items-center justify-between gap-3 px-5 py-4 text-left"
                >
                  <div className="flex items-start gap-3">
                    <FontAwesomeIcon
                      icon={open ? faChevronDown : faChevronRight}
                      className="mt-1 h-3.5 w-3.5 shrink-0 text-muted-foreground"
                    />
                    <div>
                      <p className="text-sm font-semibold text-foreground">
                        {slot.label}
                      </p>
                      <p className="mt-0.5 text-xs text-muted-foreground">{slot.description}</p>
                    </div>
                  </div>
                  {slot?.configured ? (
                    <span className="shrink-0 text-xs font-medium text-success">Configured</span>
                  ) : (
                    <span className="shrink-0 text-xs text-muted-foreground">Not set</span>
                  )}
                </button>
                {open && (
                  <div className="border-t border-border px-5 py-4">
                    <ProviderSlotEditor
                      slot={slot}
                      showTest
                      requireTest
                      onSaved={() => setExpandedOptional(null)}
                      onCancel={() => setExpandedOptional(null)}
                    />
                  </div>
                )}
              </div>
            );
          })}
        </div>
        <NavRow
          onBack={back}
          onSkip={next}
          skipLabel="Skip optional providers"
          primary={{ label: "Continue", onClick: next }}
        />
      </>
    );
  }

  // --- Feature toggles step ---------------------------------------------------
  function renderSettingsStep() {
    const settings = settingsQuery.data?.data ?? [];
    const settingBool = (key: string, dflt: boolean): boolean => {
      const s = settings.find((x) => x.key === key);
      return s ? s.value === true : dflt;
    };
    const depMet = (dep: ToggleDep): boolean => {
      if (dep === "enrichment") return caps?.enrichment_available === true;
      if (dep === "ask") return slotByName("ask")?.configured === true;
      return slotByName("reranker")?.configured === true;
    };

    return (
      <>
        <StepHeader
          index={stepIndex}
          total={stepKeys.length}
          title="High-level features"
          subtitle="Turn the major background features on or off. Each one and its finer-grained knobs stay available later under Settings."
        />
        <div className="space-y-3">
          {FEATURE_TOGGLES.map((t) => {
            const enabled = depMet(t.dep);
            const checked = enabled && settingBool(t.key, t.defaultOn);
            const saving =
              updateSetting.isPending && updateSetting.variables?.key === t.key;
            return (
              <div
                key={t.key}
                className={`rounded-lg border border-border bg-card p-4 ${
                  enabled ? "" : "opacity-60"
                }`}
              >
                <label className="flex items-start justify-between gap-4">
                  <div>
                    <p className="text-sm font-semibold text-foreground">{t.label}</p>
                    <p className="mt-0.5 text-xs text-muted-foreground">{t.blurb}</p>
                    {!enabled && (
                      <p className="mt-1 text-xs font-medium text-warning">
                        {DEP_HINT[t.dep]}
                      </p>
                    )}
                  </div>
                  <input
                    type="checkbox"
                    checked={checked}
                    disabled={!enabled || saving}
                    onChange={(e) =>
                      updateSetting.mutate({ key: t.key, value: e.target.checked })
                    }
                    className="mt-1 h-5 w-5 shrink-0 rounded border-input text-primary focus:ring-2 focus:ring-ring disabled:opacity-50"
                  />
                </label>
              </div>
            );
          })}
        </div>
        <NavRow
          onBack={back}
          primary={{ label: "Continue", onClick: next }}
        />
      </>
    );
  }

  // --- Finish step ------------------------------------------------------------
  function renderFinishStep() {
    const origin =
      typeof window !== "undefined" ? window.location.origin : "http://localhost:8674";
    const claudeCodeCmd = `claude mcp add --transport http nram ${origin}/mcp`;
    const curlStore = `curl -X POST ${origin}/v1/memories \\
  -H "Authorization: Bearer $NRAM_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"content": "The user prefers dark mode.", "tags": ["preferences", "ui"]}'`;
    const backendLabel = status?.backend === "sqlite" ? "SQLite" : "Postgres";

    return (
      <>
        <StepHeader
          index={stepIndex}
          total={stepKeys.length}
          title="You're all set"
          subtitle="Connect an MCP client and start storing memories. You can revisit any of this under Settings."
        />

        <div className="space-y-6">
          <div className="space-y-3">
            <h2 className="text-lg font-semibold text-foreground">Connect an MCP client</h2>
            <p className="text-sm text-muted-foreground">
              nram supports OAuth auto-discovery, so most clients connect with just the
              server URL, no API key required.
            </p>
            <CodeBlock code={claudeCodeCmd} label="Claude Code" />
            <div className="rounded-lg border border-border bg-card p-4 space-y-2">
              <p className="text-sm font-medium text-foreground">
                Claude Desktop / Claude.ai / Cursor
              </p>
              <p className="text-sm text-muted-foreground">
                Add a custom connector with this URL:
              </p>
              <code className="block rounded-md bg-muted px-3 py-2 text-sm font-mono text-foreground">
                {origin}/mcp
              </code>
            </div>
            <p className="text-xs text-muted-foreground">
              More options on the{" "}
              <a href="/mcp-config" className="text-primary hover:underline">
                MCP Config
              </a>{" "}
              page.
            </p>
          </div>

          <div className="space-y-3">
            <h2 className="text-lg font-semibold text-foreground">Direct API access</h2>
            <p className="text-sm text-muted-foreground">
              For tools without OAuth, use the API key shown when you created your
              account, or mint a new one on the{" "}
              <a href="/account" className="text-primary hover:underline">
                API Keys
              </a>{" "}
              page, then set <code className="rounded bg-muted px-1 py-0.5">NRAM_API_KEY</code>.
            </p>
            <CodeBlock code={curlStore} label="Store a memory" />
          </div>

          <div className="flex items-start gap-3 rounded-lg border border-info/40 bg-info/10 px-4 py-3">
            <FontAwesomeIcon icon={faCircleInfo} className="mt-0.5 h-5 w-5 shrink-0 text-info" />
            <p className="text-sm text-info">
              Running on {backendLabel}. The full feature set, vector search, hybrid
              recall, enrichment, dreaming, and the knowledge graph, is active.
            </p>
          </div>

          {caps?.enrichment_available === false && (
            <div className="flex items-start gap-3 rounded-lg border border-warning/40 bg-warning/10 px-4 py-3">
              <FontAwesomeIcon
                icon={faTriangleExclamation}
                className="mt-0.5 h-5 w-5 shrink-0 text-warning"
              />
              <p className="text-sm text-warning">
                Required providers are not all configured, so memories are stored as raw
                text only. You can finish now and configure them anytime under Settings →
                Providers.
              </p>
            </div>
          )}
        </div>

        <NavRow
          onBack={back}
          primary={{
            label: updateOnboarding.isPending ? "Finishing…" : "Finish & go to dashboard",
            onClick: finish,
            disabled: updateOnboarding.isPending,
          }}
        />
      </>
    );
  }

  function renderStep(key: string) {
    if (key === "optional") return renderOptionalStep();
    if (key === "settings") return renderSettingsStep();
    if (key === "finish") return renderFinishStep();
    // Anything else is a required provider slot's step.
    return renderRequiredStep(key);
  }

  return (
    <div className="app-shell min-h-screen overflow-y-auto p-6">
      <div className="mx-auto max-w-2xl py-8">
        {/* Progress bar */}
        <div className="mb-8 flex gap-1.5">
          {stepKeys.map((k, i) => (
            <div
              key={k}
              className={`h-1.5 flex-1 rounded-full ${
                i <= stepIndex ? "bg-primary" : "bg-border"
              }`}
            />
          ))}
        </div>
        {renderStep(stepKey)}
      </div>
    </div>
  );
}

export default OnboardingWizard;
