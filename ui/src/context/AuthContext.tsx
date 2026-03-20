import { createContext, useContext, useState, useCallback, type ReactNode } from "react";
import { useNavigate } from "react-router-dom";

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

function loadInitialUser(): UserInfo | null {
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
  const [user, setUser] = useState<UserInfo | null>(loadInitialUser);

  const login = useCallback((token: string, userInfo: UserInfo) => {
    localStorage.setItem("nram_token", token);
    localStorage.setItem("nram_user", JSON.stringify(userInfo));
    setUser(userInfo);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem("nram_token");
    localStorage.removeItem("nram_user");
    setUser(null);
    navigate("/login");
  }, [navigate]);

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
