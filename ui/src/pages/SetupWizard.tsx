import { useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { useSetupStatus, useCompleteSetup } from "../hooks/useApi";
import type { SetupResponse } from "../api/client";
import { useAuth } from "../context/AuthContext";

function CopyButton({ text, label }: { text: string; label?: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [text]);

  return (
    <button
      type="button"
      onClick={handleCopy}
      className="inline-flex items-center gap-1.5 rounded-md border border-border bg-card px-3 py-1.5 text-xs font-medium text-foreground transition-colors hover:bg-accent focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
    >
      {copied ? (
        <>
          <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
          </svg>
          Copied
        </>
      ) : (
        <>
          <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M15.666 3.888A2.25 2.25 0 0013.5 2.25h-3c-1.03 0-1.9.693-2.166 1.638m7.332 0c.055.194.084.4.084.612v0a.75.75 0 01-.75.75H9.75a.75.75 0 01-.75-.75v0c0-.212.03-.418.084-.612m7.332 0c.646.049 1.288.11 1.927.184 1.1.128 1.907 1.077 1.907 2.185V19.5a2.25 2.25 0 01-2.25 2.25H6.75A2.25 2.25 0 014.5 19.5V6.257c0-1.108.806-2.057 1.907-2.185a48.208 48.208 0 011.927-.184" />
          </svg>
          {label ?? "Copy"}
        </>
      )}
    </button>
  );
}

function CodeBlock({ code, label }: { code: string; label?: string }) {
  return (
    <div className="rounded-lg border border-border bg-muted/50">
      <div className="flex items-center justify-between border-b border-border px-4 py-2">
        {label && <span className="text-xs font-medium text-muted-foreground">{label}</span>}
        <CopyButton text={code} />
      </div>
      <pre className="overflow-x-auto p-4 text-sm leading-relaxed text-foreground">
        <code>{code}</code>
      </pre>
    </div>
  );
}

function BackendBanners({ backend }: { backend: string }) {
  const backendLabel = backend === "sqlite" ? "SQLite" : "Postgres";
  const backendBlurb =
    backend === "sqlite"
      ? "SQLite supports the full feature set — vector search (pure-Go HNSW), hybrid recall (FTS5), enrichment, dreaming, knowledge graph, and every MCP tool. Upgrade to Postgres only if you need multiple nram instances against one database with cross-instance event propagation."
      : "Vector search via pgvector, hybrid recall, multi-instance event propagation via LISTEN/NOTIFY, and the full feature set are active.";

  return (
    <div className="space-y-3">
      <div className="rounded-lg border border-blue-300 bg-blue-50 px-4 py-3 dark:border-blue-700 dark:bg-blue-950/30">
        <div className="flex items-start gap-3">
          <svg className="mt-0.5 h-5 w-5 shrink-0 text-blue-600 dark:text-blue-400" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z" />
          </svg>
          <div>
            <p className="text-sm font-medium text-blue-800 dark:text-blue-200">Running on {backendLabel}</p>
            <p className="mt-1 text-sm text-blue-700 dark:text-blue-300">{backendBlurb}</p>
            <a href="/database" className="mt-1 inline-block text-sm font-medium text-blue-800 underline hover:no-underline dark:text-blue-200">
              Settings &rarr; Database
            </a>
          </div>
        </div>
      </div>
      <div className="rounded-lg border border-amber-300 bg-amber-50 px-4 py-3 dark:border-amber-700 dark:bg-amber-950/30">
        <div className="flex items-start gap-3">
          <svg className="mt-0.5 h-5 w-5 shrink-0 text-amber-600 dark:text-amber-400" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
          </svg>
          <div>
            <p className="text-sm font-medium text-amber-800 dark:text-amber-200">LLM providers not configured</p>
            <p className="mt-1 text-sm text-amber-700 dark:text-amber-300">
              Memories are stored as raw text only. Configure an <strong>embedding</strong> provider for semantic recall and a <strong>fact</strong> + <strong>entity</strong> provider for enrichment, dreaming, and the knowledge graph. Provider changes hot-reload — no restart.
            </p>
            <a href="/providers" className="mt-1 inline-block text-sm font-medium text-amber-800 underline hover:no-underline dark:text-amber-200">
              Settings &rarr; Providers
            </a>
          </div>
        </div>
      </div>
    </div>
  );
}

function CompletionScreen({
  result,
  backend,
}: {
  result: SetupResponse;
  backend: string;
}) {
  const navigate = useNavigate();
  const origin = typeof window !== "undefined" ? window.location.origin : "http://localhost:8674";

  const claudeCodeCmd = `claude mcp add --transport http nram ${origin}/mcp`;

  const curlStore = `curl -X POST ${origin}/v1/memories \\
  -H "Authorization: Bearer ${result.api_key}" \\
  -H "Content-Type: application/json" \\
  -d '{
    "content": "The user prefers dark mode.",
    "tags": ["preferences", "ui"]
  }'`;

  const curlRecall = `curl "${origin}/v1/memories/recall?q=user+preferences" \\
  -H "Authorization: Bearer ${result.api_key}"`;

  return (
    <div className="mx-auto max-w-2xl space-y-8">
      <div className="text-center">
        <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-full bg-green-100 dark:bg-green-900/30">
          <svg className="h-8 w-8 text-green-600 dark:text-green-400" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
          </svg>
        </div>
        <h1 className="mt-4 text-2xl font-semibold tracking-tight">Setup Complete</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          Your nram instance is ready. Review the information below to get started.
        </p>
      </div>

      {/* MCP Connection */}
      <div className="space-y-3">
        <h2 className="text-lg font-semibold">Connect an MCP Client</h2>
        <p className="text-sm text-muted-foreground">
          nram supports OAuth auto-discovery. Most MCP clients connect with just the server URL &mdash; no API key needed.
        </p>
        <CodeBlock code={claudeCodeCmd} label="Claude Code" />
        <div className="rounded-lg border border-border bg-card p-4 space-y-2">
          <p className="text-sm font-medium">Claude Desktop / Claude.ai</p>
          <p className="text-sm text-muted-foreground">Settings &rarr; Connectors &rarr; Add URL:</p>
          <code className="block rounded-md bg-muted px-3 py-2 text-sm font-mono">{origin}/mcp</code>
        </div>
        <div className="rounded-lg border border-border bg-card p-4 space-y-2">
          <p className="text-sm font-medium">Cursor</p>
          <p className="text-sm text-muted-foreground">Settings &rarr; MCP &rarr; Add &rarr; URL type:</p>
          <code className="block rounded-md bg-muted px-3 py-2 text-sm font-mono">{origin}/mcp</code>
        </div>
        <p className="text-xs text-muted-foreground">
          For more options including ChatGPT and API key fallback, visit the <a href="/mcp-config" className="text-primary hover:underline">MCP Config</a> page.
        </p>
      </div>

      {/* API Key */}
      <div className="rounded-lg border-2 border-amber-400 bg-amber-50 p-5 dark:border-amber-600 dark:bg-amber-950/30">
        <div className="flex items-start gap-3">
          <svg className="mt-0.5 h-5 w-5 shrink-0 text-amber-600 dark:text-amber-400" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
          </svg>
          <div className="flex-1">
            <p className="text-sm font-semibold text-amber-800 dark:text-amber-200">
              API Key (fallback) &mdash; save this now, it will not be shown again
            </p>
            <p className="mt-1 text-xs text-amber-700 dark:text-amber-300">
              Use this for tools that don&apos;t support OAuth, or for direct API access.
            </p>
            <div className="mt-3 flex items-center gap-2">
              <code className="flex-1 rounded-md border border-amber-300 bg-white px-3 py-2 text-sm font-mono break-all dark:border-amber-700 dark:bg-amber-950/50">
                {result.api_key}
              </code>
              <CopyButton text={result.api_key} label="Copy Key" />
            </div>
          </div>
        </div>
      </div>

      {/* curl examples */}
      <div className="space-y-4">
        <h2 className="text-lg font-semibold">Quick Start</h2>
        <CodeBlock code={curlStore} label="Store a memory" />
        <CodeBlock code={curlRecall} label="Recall memories" />
      </div>

      {/* Backend banners */}
      <BackendBanners backend={backend} />

      {/* Go to Dashboard */}
      <div className="flex justify-center pt-2 pb-8">
        <button
          type="button"
          onClick={() => navigate("/")}
          className="rounded-lg bg-primary px-6 py-2.5 text-sm font-medium text-primary-foreground shadow-sm transition-colors hover:bg-primary/90 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2"
        >
          Go to Dashboard
        </button>
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
    return <CompletionScreen result={setupResult} backend={status?.backend ?? "sqlite"} />;
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
    <div className="flex min-h-[80vh] items-center justify-center">
      <div className="w-full max-w-md">
        <div className="text-center">
          <h1 className="text-2xl font-semibold tracking-tight">Welcome to nram</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            Create your administrator account to get started.
          </p>
        </div>

        <form onSubmit={handleSubmit} className="mt-8 space-y-5">
          {/* Errors */}
          {(validationErrors.length > 0 || serverError) && (
            <div className="rounded-lg border border-red-300 bg-red-50 p-4 dark:border-red-700 dark:bg-red-950/30">
              <ul className="space-y-1 text-sm text-red-700 dark:text-red-300">
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
              Email <span className="text-red-500">*</span>
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
              Password <span className="text-red-500">*</span>
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
              Confirm Password <span className="text-red-500">*</span>
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
