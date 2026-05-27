import type { ReactNode } from "react";
import type { IconDefinition } from "@fortawesome/fontawesome-svg-core";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";

type Props = {
  icon?: IconDefinition;
  title: ReactNode;
  subtitle?: ReactNode;
  actions?: ReactNode;
  className?: string;
};

export function PageHeader({ icon, title, subtitle, actions, className }: Props) {
  return (
    <div className={`mb-6 flex items-start justify-between gap-4${className ? ` ${className}` : ""}`}>
      <div className="flex items-start gap-3">
        {icon && (
          <FontAwesomeIcon
            icon={icon}
            className="mt-1.5 h-7 w-7 text-primary/70"
          />
        )}
        <div>
          <h1 className="font-display text-3xl leading-tight text-foreground">{title}</h1>
          {subtitle && (
            <p className="mt-1 text-sm text-muted-foreground">{subtitle}</p>
          )}
        </div>
      </div>
      {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
    </div>
  );
}
