import type { ReactNode } from "react";
import { useAuth } from "../context/AuthContext";

interface RequireRoleProps {
  minRole: string;
  children: ReactNode;
  fallback?: ReactNode;
}

function AccessDenied() {
  return (
    <div className="flex items-center justify-center py-16">
      <div className="w-full max-w-md rounded-lg border bg-card p-8 text-center">
        <h2 className="text-lg font-semibold">Access Denied</h2>
        <p className="mt-2 text-sm text-muted-foreground">
          You don't have permission to view this page.
        </p>
        <a
          href="/"
          className="mt-4 inline-block rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
        >
          Go to Dashboard
        </a>
      </div>
    </div>
  );
}

export default function RequireRole({ minRole, children, fallback }: RequireRoleProps) {
  const { hasMinRole } = useAuth();

  if (!hasMinRole(minRole)) {
    return <>{fallback ?? <AccessDenied />}</>;
  }

  return <>{children}</>;
}
