// Brand mark for the console: the vector tile (/brand.svg) beside the
// "Neural Ram" wordmark, matching the nram.ai site nav. The SVG has a dark
// field baked in (the white "n" only reads on dark), so the rounded tile
// renders correctly on both the light and dark console themes.

type LogoSize = "sm" | "md" | "lg";

type Props = {
  size?: LogoSize;
  // When false, renders the tile only (no wordmark). Defaults to true.
  showWordmark?: boolean;
  className?: string;
};

const WORDMARK_PX: Record<LogoSize, string> = {
  sm: "0.95rem",
  md: "1.1rem",
  lg: "1.25rem",
};

const TILE_PX: Record<LogoSize, number> = {
  sm: 24,
  md: 28,
  lg: 32,
};

export function Logo({ size = "md", showWordmark = true, className }: Props) {
  const tile = TILE_PX[size];
  return (
    <span
      className={`inline-flex items-center gap-2${className ? ` ${className}` : ""}`}
    >
      <img
        src="/brand.svg"
        alt="Neural Ram"
        className="rounded-md"
        width={tile}
        height={tile}
        style={{ width: tile, height: tile }}
      />
      {showWordmark && (
        <span
          className="font-sans font-semibold tracking-tight text-foreground"
          style={{ fontSize: WORDMARK_PX[size], lineHeight: 1 }}
        >
          Neural Ram
        </span>
      )}
    </span>
  );
}
