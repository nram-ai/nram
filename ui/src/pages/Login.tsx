import { useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { authAPI } from "../api/client";
import type { APIError } from "../api/client";
import { useAuth } from "../context/AuthContext";

type Step = "email" | "password";

function Login() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const auth = useAuth();

  const [step, setStep] = useState<Step>("email");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function handleEmailSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = email.trim();
    if (!trimmed) {
      setError("Email is required.");
      return;
    }
    setError(null);
    setLoading(true);
    try {
      const result = await authAPI.lookup({ email: trimmed });
      if (result.method === "local") {
        setStep("password");
      } else if (result.method === "idp" && result.idp_id) {
        // Redirect to the external IdP for SSO authentication.
        const redirect = searchParams.get("redirect") ?? "/";
        window.location.href = `/auth/idp/login?idp_id=${encodeURIComponent(result.idp_id)}&redirect=${encodeURIComponent(redirect)}`;
      } else if (result.method === "idp") {
        setError(
          "Your organization uses external authentication but no identity provider is configured. Contact your administrator.",
        );
      } else {
        setError("User not found. Contact your administrator.");
      }
    } catch (err) {
      const apiErr = err as APIError;
      const body = apiErr.body as { error?: string } | undefined;
      setError(body?.error ?? apiErr.message ?? "Failed to look up user.");
    } finally {
      setLoading(false);
    }
  }

  async function handlePasswordSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!password) {
      setError("Password is required.");
      return;
    }
    setError(null);
    setLoading(true);
    try {
      const result = await authAPI.login({ email: email.trim(), password });
      auth.login(result.token, result.user);
      // Set a short-lived session cookie for the OAuth authorize redirect flow
      document.cookie = `nram_session=${result.token}; path=/; max-age=300; SameSite=Lax`;
      const redirect = searchParams.get("redirect");
      if (redirect) {
        window.location.href = redirect;
      } else {
        navigate("/", { replace: true });
      }
    } catch (err) {
      const apiErr = err as APIError;
      const body = apiErr.body as { error?: string } | undefined;
      setError(body?.error ?? apiErr.message ?? "Invalid credentials.");
    } finally {
      setLoading(false);
    }
  }

  function handleBack() {
    setStep("email");
    setPassword("");
    setError(null);
  }

  const inputClass =
    "mt-1.5 block w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-muted-foreground shadow-sm transition-colors focus:border-ring focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2";
  const buttonClass =
    "w-full rounded-lg bg-primary px-4 py-2.5 text-sm font-medium text-primary-foreground shadow-sm transition-colors hover:bg-primary/90 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50";

  return (
    <div className="flex min-h-screen items-center justify-center">
      <div className="w-full max-w-md">
        <div className="text-center">
          <h1 className="text-2xl font-semibold tracking-tight">Sign in to nram</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            {step === "email"
              ? "Enter your email address to continue."
              : "Enter your password to sign in."}
          </p>
        </div>

        {error && (
          <div className="mt-6 rounded-lg border border-red-300 bg-red-50 p-4 dark:border-red-700 dark:bg-red-950/30">
            <p className="text-sm text-red-700 dark:text-red-300">{error}</p>
          </div>
        )}

        {step === "email" ? (
          <form onSubmit={handleEmailSubmit} className="mt-8 space-y-5">
            <div>
              <label htmlFor="login-email" className="block text-sm font-medium text-foreground">
                Email
              </label>
              <input
                id="login-email"
                type="email"
                required
                autoComplete="email"
                autoFocus
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="you@example.com"
                className={inputClass}
              />
            </div>
            <button type="submit" disabled={loading} className={buttonClass}>
              {loading ? "Checking..." : "Continue"}
            </button>
          </form>
        ) : (
          <form onSubmit={handlePasswordSubmit} className="mt-8 space-y-5">
            <div>
              <label className="block text-sm font-medium text-foreground">Email</label>
              <p className="mt-1.5 text-sm text-muted-foreground">{email.trim()}</p>
            </div>
            <div>
              <label htmlFor="login-password" className="block text-sm font-medium text-foreground">
                Password
              </label>
              <input
                id="login-password"
                type="password"
                required
                autoComplete="current-password"
                autoFocus
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Enter your password"
                className={inputClass}
              />
            </div>
            <button type="submit" disabled={loading} className={buttonClass}>
              {loading ? "Signing in..." : "Sign in"}
            </button>
            <div className="text-center">
              <button
                type="button"
                onClick={handleBack}
                className="text-sm text-muted-foreground transition-colors hover:text-foreground"
              >
                Back
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

export default Login;
