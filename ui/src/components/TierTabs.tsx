import { useTierAccess, type Tier } from "../context/AuthContext";

const LABELS: Record<Tier, string> = {
  self: "Mine",
  org: "Org",
  system: "System",
};

export function TierTabs({
  current,
  onChange,
  ariaLabel,
}: {
  current: Tier;
  onChange: (tier: Tier) => void;
  ariaLabel: string;
}) {
  const { availableTiers } = useTierAccess();
  if (availableTiers.length <= 1) return null;
  return (
    <div
      className="inline-flex rounded-md border bg-card p-0.5"
      role="tablist"
      aria-label={ariaLabel}
    >
      {availableTiers.map((t) => (
        <button
          key={t}
          type="button"
          role="tab"
          aria-selected={current === t}
          onClick={() => onChange(t)}
          className={`rounded px-3 py-1 text-xs font-medium ${
            current === t
              ? "bg-primary text-primary-foreground"
              : "text-muted-foreground hover:bg-muted"
          }`}
        >
          {LABELS[t]}
        </button>
      ))}
    </div>
  );
}
