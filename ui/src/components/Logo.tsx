// Minimal placeholder while the new transparent brand SVG is being made.
// Renders a plain sans brand mark only — no serif treatment, no image — so
// it can be dropped into the sidebar header without competing with the
// serif page titles below it. Swap in <img src="/brand.svg"> once the
// transparent asset lands; the Logo callers do not need to change.

type LogoSize = "sm" | "md" | "lg";

type Props = {
  size?: LogoSize;
  // Kept for call-site compatibility; has no effect until the brand mark
  // is reintroduced.
  showWordmark?: boolean;
  className?: string;
};

const WORDMARK_PX: Record<LogoSize, string> = {
  sm: "0.95rem",
  md: "1.1rem",
  lg: "1.25rem",
};

export function Logo({ size = "md", className }: Props) {
  return (
    <span
      className={`font-sans font-semibold tracking-tight text-foreground${
        className ? ` ${className}` : ""
      }`}
      style={{ fontSize: WORDMARK_PX[size], lineHeight: 1 }}
    >
      nram
    </span>
  );
}
