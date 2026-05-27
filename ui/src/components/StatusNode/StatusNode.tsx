import type { ReactNode } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faCircleCheck,
  faCircleInfo,
  faCirclePause,
  faCircleXmark,
  faSpinner,
  faTriangleExclamation,
} from "../../lib/icons";
import styles from "./StatusNode.module.css";

export type StatusKind =
  | "success"
  | "info"
  | "warning"
  | "error"
  | "pending"
  | "paused";

type Props = {
  kind: StatusKind;
  label?: ReactNode;
  // events-per-second observed recently. Higher values pulse faster.
  rate?: number;
  // Hide the leading icon when the label already carries iconography.
  noIcon?: boolean;
  className?: string;
};

const COLOR_BY_KIND: Record<StatusKind, string> = {
  success: "hsl(var(--success))",
  info: "hsl(var(--info))",
  warning: "hsl(var(--warning))",
  error: "hsl(var(--destructive))",
  pending: "hsl(var(--primary))",
  paused: "hsl(var(--muted-foreground))",
};

const ICON_BY_KIND = {
  success: faCircleCheck,
  info: faCircleInfo,
  warning: faTriangleExclamation,
  error: faCircleXmark,
  pending: faSpinner,
  paused: faCirclePause,
} as const;

function rateClass(rate: number | undefined, kind: StatusKind): string {
  if (kind === "paused") return styles.still;
  if (kind === "error") return styles.stutter;
  if (rate === undefined) return "";
  if (rate >= 2) return styles.fast;
  if (rate >= 0.4) return styles.medium;
  return "";
}

export function StatusNode({ kind, label, rate, noIcon, className }: Props) {
  const color = COLOR_BY_KIND[kind];
  const tempo = rateClass(rate, kind);
  const icon = ICON_BY_KIND[kind];
  const spinIcon = kind === "pending";

  return (
    <span
      className={`${styles.root}${className ? ` ${className}` : ""}`}
      role="status"
    >
      <span
        className={`${styles.dot} ${tempo}`}
        style={{ backgroundColor: color, color }}
        aria-hidden
      />
      {!noIcon && (
        <FontAwesomeIcon
          icon={icon}
          spin={spinIcon}
          className={styles.icon}
          style={{ color }}
        />
      )}
      {label !== undefined && <span className={styles.label}>{label}</span>}
    </span>
  );
}
