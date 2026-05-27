type Props = {
  className?: string;
  // Defaults to a card-shaped skeleton block.
  variant?: "block" | "line";
};

// Cyan-sweep skeleton that replaces animate-pulse. Animation is defined in
// index.css under .skeleton-shimmer (1.4s ease-in-out infinite). Honors
// prefers-reduced-motion via the global CSS rule.
export function Shimmer({ className, variant = "block" }: Props) {
  const base =
    variant === "line"
      ? "h-3 rounded-sm skeleton-shimmer"
      : "rounded-lg skeleton-shimmer";
  return <div className={`${base}${className ? ` ${className}` : ""}`} />;
}
