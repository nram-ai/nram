import type { ReactNode } from "react";
import type { IconDefinition } from "@fortawesome/fontawesome-svg-core";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";

type Props = {
  icon?: IconDefinition;
  title: ReactNode;
  body?: ReactNode;
  action?: ReactNode;
  className?: string;
};

export function EmptyState({ icon, title, body, action, className }: Props) {
  return (
    <div
      className={`flex flex-col items-center justify-center gap-3 py-12 text-center${
        className ? ` ${className}` : ""
      }`}
    >
      {icon && (
        <FontAwesomeIcon
          icon={icon}
          className="h-10 w-10 text-primary/40"
        />
      )}
      <h2 className="font-display text-2xl text-foreground">{title}</h2>
      {body && (
        <p className="max-w-sm text-sm text-muted-foreground">{body}</p>
      )}
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}
