import { createContext, useContext, useState, useCallback, type ReactNode } from "react";
import { useNavigate } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface UserInfo {
  id: string;
  email: string;
  display_name: string;
  role: string;
  org_id: string;
}

// ---------------------------------------------------------------------------
// Role hierarchy
// ---------------------------------------------------------------------------

export const ROLE_LEVELS: Record<string, number> = {
  readonly: 10,
  service: 20,
  member: 20,
  org_owner: 30,
  administrator: 40,
};

function roleLevel(role: string): number {
  return ROLE_LEVELS[role] ?? 0;
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

export interface AuthContextValue {
  user: UserInfo | null;
  isAdmin: boolean;
  isOrgOwner: boolean;
  canWrite: boolean;
  hasMinRole: (role: string) => boolean;
  login: (token: string, user: UserInfo) => void;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

// ---------------------------------------------------------------------------
// JWT fallback decoder
// ---------------------------------------------------------------------------

function decodeUserFromJWT(token: string): UserInfo | null {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const payload = JSON.parse(atob(parts[1]));
    return {
      id: payload.sub ?? "",
      email: payload.email ?? "",
      display_name: payload.display_name ?? "",
      role: payload.role ?? "readonly",
      org_id: payload.org_id ?? "",
    };
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Initial user from localStorage
// ---------------------------------------------------------------------------

// Check for nram_session cookie set by the IdP callback and bootstrap
// localStorage auth from it. The cookie is short-lived and non-HttpOnly
// so the SPA can read it exactly once.
function bootstrapFromIdPCookie(): void {
  const match = document.cookie.match(/(?:^|;\s*)nram_session=([^;]+)/);
  if (!match) return;
  const token = match[1];
  if (!token) return;

  // Move to localStorage.
  localStorage.setItem("nram_token", token);
  const user = decodeUserFromJWT(token);
  if (user) {
    localStorage.setItem("nram_user", JSON.stringify(user));
  }

  // Clear the cookie.
  document.cookie = "nram_session=; path=/; max-age=0";
}

function loadInitialUser(): UserInfo | null {
  bootstrapFromIdPCookie();

  const raw = localStorage.getItem("nram_user");
  if (raw) {
    try {
      return JSON.parse(raw) as UserInfo;
    } catch {
      // fall through to JWT fallback
    }
  }
  const token = localStorage.getItem("nram_token");
  if (token) {
    return decodeUserFromJWT(token);
  }
  return null;
}

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

export function AuthProvider({ children }: { children: ReactNode }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [user, setUser] = useState<UserInfo | null>(loadInitialUser);

  const login = useCallback((token: string, userInfo: UserInfo) => {
    localStorage.setItem("nram_token", token);
    localStorage.setItem("nram_user", JSON.stringify(userInfo));
    setUser(userInfo);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem("nram_token");
    localStorage.removeItem("nram_user");
    queryClient.clear();
    setUser(null);
    navigate("/login");
  }, [navigate, queryClient]);

  const hasMinRole = useCallback(
    (minRole: string): boolean => {
      if (!user) return false;
      return roleLevel(user.role) >= roleLevel(minRole);
    },
    [user],
  );

  const role = user?.role ?? "";
  const isAdmin = role === "administrator";
  const isOrgOwner = isAdmin || role === "org_owner";
  const canWrite = !!user && role !== "readonly";

  const value: AuthContextValue = {
    user,
    isAdmin,
    isOrgOwner,
    canWrite,
    hasMinRole,
    login,
    logout,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return ctx;
}
