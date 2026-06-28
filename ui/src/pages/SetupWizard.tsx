import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useSetupStatus, useCompleteSetup } from "../hooks/useApi";
import type { SetupResponse } from "../api/client";
import { useAuth } from "../context/AuthContext";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faTriangleExclamation, faCircleCheck } from "../lib/icons";
import { CopyButton } from "../components/CopyButton";

// Shared styling for the wizard's copy buttons (icon + label card style).
const WIZARD_COPY_CLASS =
  "inline-flex items-center gap-1.5 rounded-md border border-border bg-card px-3 py-1.5 text-xs font-medium text-foreground transition-colors hover:bg-accent focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2";

function CompletionScreen({ result }: { result: SetupResponse }) {
  const navigate = useNavigate();

  return (
    <div className="app-shell flex min-h-[80vh] items-center justify-center p-6">
      <div className="mx-auto w-full max-w-xl space-y-6">
        <div className="text-center">
          <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-full bg-success/20">
            <FontAwesomeIcon icon={faCircleCheck} className="h-8 w-8 text-success" />
          </div>
          <h1 className="mt-4 font-display text-3xl text-foreground">Administrator created</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Save your API key below, then continue to the guided setup to configure
            providers and turn on the features you want.
          </p>
        </div>

        {/* API Key: one-time reveal. Shown here because it is never returned again;
            the rest of setup (providers, features, MCP connect) follows in the
            guided wizard. */}
        <div className="rounded-lg border-2 border-warning/40 bg-warning/10 p-5">
          <div className="flex items-start gap-3">
            <FontAwesomeIcon icon={faTriangleExclamation} className="mt-0.5 h-5 w-5 shrink-0 text-warning" />
            <div className="flex-1">
              <p className="text-sm font-semibold text-warning">
                API Key (fallback): save this now, it will not be shown again
              </p>
              <p className="mt-1 text-xs text-warning">
                Use this for tools that don&apos;t support OAuth, or for direct API access.
              </p>
              <div className="mt-3 flex items-center gap-2">
                <code className="flex-1 rounded-md border border-warning/40 bg-white px-3 py-2 text-sm font-mono break-all dark:bg-warning/15">
                  {result.api_key}
                </code>
                <CopyButton text={result.api_key} label="Copy Key" withIcon className={WIZARD_COPY_CLASS} />
              </div>
            </div>
          </div>
        </div>

        <div className="flex justify-center pt-2">
          <button
            type="button"
            onClick={() => navigate("/onboarding")}
            className="rounded-lg bg-primary px-6 py-2.5 text-sm font-medium text-primary-foreground shadow-sm transition-colors hover:bg-primary/90 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
          >
            Continue to guided setup
          </button>
        </div>
      </div>
    </div>
  );
}

function SetupWizard() {
  const navigate = useNavigate();
  const auth = useAuth();
  const { data: status, isLoading: statusLoading } = useSetupStatus();
  const completeMutation = useCompleteSetup();

  const [email, setEmail] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [validationErrors, setValidationErrors] = useState<string[]>([]);
  const [setupResult, setSetupResult] = useState<SetupResponse | null>(null);

  // Redirect if setup already complete
  if (status?.setup_complete && !setupResult) {
    navigate("/", { replace: true });
    return null;
  }

  if (statusLoading) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center">
        <div className="text-sm text-muted-foreground">Checking setup status...</div>
      </div>
    );
  }

  // Show completion screen after successful setup
  if (setupResult) {
    return <CompletionScreen result={setupResult} />;
  }

  function validate(): string[] {
    const errors: string[] = [];
    if (!email.trim()) {
      errors.push("Email is required.");
    } else if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim())) {
      errors.push("Please enter a valid email address.");
    }
    if (password.length < 8) {
      errors.push("Password must be at least 8 characters.");
    }
    if (password !== confirmPassword) {
      errors.push("Passwords do not match.");
    }
    return errors;
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const errors = validate();
    if (errors.length > 0) {
      setValidationErrors(errors);
      return;
    }
    setValidationErrors([]);
    completeMutation.mutate(
      { email: email.trim(), password },
      {
        onSuccess: (data) => {
          if (data.token) {
            const userInfo = {
              id: data.user.id,
              email: data.user.email,
              display_name: data.user.display_name,
              role: "administrator",
              org_id: data.user.org_id ?? "",
            };
            auth.login(data.token, userInfo);
          }
          setSetupResult(data);
        },
      },
    );
  }

  const serverError = completeMutation.error
    ? String(
        (completeMutation.error as { body?: { error?: string } })?.body?.error ??
          completeMutation.error.message,
      )
    : null;

  return (
    <div className="app-shell flex min-h-[80vh] items-center justify-center p-6">
      <div className="w-full max-w-md">
        <div className="text-center">
          <h1 className="font-display text-4xl text-foreground">Welcome to Neural Ram</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Create your administrator account to get started.
          </p>
        </div>

        <form onSubmit={handleSubmit} className="surface-elevated mt-8 space-y-5 rounded-lg p-6 shadow-lg shadow-black/10">
          {/* Errors */}
          {(validationErrors.length > 0 || serverError) && (
            <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4">
              <ul className="space-y-1 text-sm text-destructive">
                {validationErrors.map((err) => (
                  <li key={err}>{err}</li>
                ))}
                {serverError && <li>{serverError}</li>}
              </ul>
            </div>
          )}

          {/* Email */}
          <div>
            <label htmlFor="setup-email" className="block text-sm font-medium text-foreground">
              Email <span className="text-destructive">*</span>
            </label>
            <input
              id="setup-email"
              type="email"
              required
              autoComplete="email"
              autoFocus
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="admin@example.com"
              className="mt-1.5 block w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-muted-foreground shadow-sm transition-colors focus:border-ring focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
            />
          </div>

          {/* Display Name */}
          <div>
            <label htmlFor="setup-display-name" className="block text-sm font-medium text-foreground">
              Display Name <span className="text-muted-foreground font-normal">(optional)</span>
            </label>
            <input
              id="setup-display-name"
              type="text"
              autoComplete="name"
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              placeholder="Admin"
              className="mt-1.5 block w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-muted-foreground shadow-sm transition-colors focus:border-ring focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
            />
          </div>

          {/* Password */}
          <div>
            <label htmlFor="setup-password" className="block text-sm font-medium text-foreground">
              Password <span className="text-destructive">*</span>
            </label>
            <input
              id="setup-password"
              type="password"
              required
              autoComplete="new-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Minimum 8 characters"
              className="mt-1.5 block w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-muted-foreground shadow-sm transition-colors focus:border-ring focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
            />
          </div>

          {/* Confirm Password */}
          <div>
            <label htmlFor="setup-confirm-password" className="block text-sm font-medium text-foreground">
              Confirm Password <span className="text-destructive">*</span>
            </label>
            <input
              id="setup-confirm-password"
              type="password"
              required
              autoComplete="new-password"
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              placeholder="Re-enter your password"
              className="mt-1.5 block w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-muted-foreground shadow-sm transition-colors focus:border-ring focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
            />
          </div>

          {/* Submit */}
          <button
            type="submit"
            disabled={completeMutation.isPending}
            className="w-full rounded-lg bg-primary px-4 py-2.5 text-sm font-medium text-primary-foreground shadow-sm transition-colors hover:bg-primary/90 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {completeMutation.isPending ? "Creating Administrator..." : "Create Administrator"}
          </button>

          <p className="text-xs text-center text-muted-foreground leading-relaxed">
            This is the only account with local credentials by default. Add users and configure SSO for your organization after setup.
          </p>
        </form>
      </div>
    </div>
  );
}

export default SetupWizard;
