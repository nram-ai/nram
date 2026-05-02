import { createContext, useContext, useEffect, useState, useCallback, type ReactNode } from "react";
import { useAuth } from "./AuthContext";
import { meAPI, type Theme } from "../api/client";

export type { Theme };

const STORAGE_KEY = "nram_theme";

function readPersistedTheme(): Theme | null {
  try {
    const v = localStorage.getItem(STORAGE_KEY);
    return v === "light" || v === "dark" ? v : null;
  } catch {
    return null;
  }
}

function readPrefersDark(): boolean {
  try {
    return window.matchMedia("(prefers-color-scheme: dark)").matches;
  } catch {
    return true;
  }
}

function applyThemeClass(theme: Theme) {
  document.documentElement.classList.toggle("dark", theme === "dark");
}

function loadInitialTheme(): Theme {
  return readPersistedTheme() ?? (readPrefersDark() ? "dark" : "light");
}

export interface ThemeContextValue {
  theme: Theme;
  setTheme: (t: Theme) => void;
  toggleTheme: () => void;
}

const ThemeContext = createContext<ThemeContextValue | null>(null);

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<Theme>(loadInitialTheme);
  const { user } = useAuth();

  useEffect(() => {
    applyThemeClass(theme);
  }, [theme]);

  // Follow OS preference only while the user has not pinned a choice.
  useEffect(() => {
    if (readPersistedTheme() !== null) return;
    let mq: MediaQueryList;
    try {
      mq = window.matchMedia("(prefers-color-scheme: dark)");
    } catch {
      return;
    }
    const handler = (e: MediaQueryListEvent) => {
      if (readPersistedTheme() !== null) return;
      setThemeState(e.matches ? "dark" : "light");
    };
    if (typeof mq.addEventListener === "function") {
      mq.addEventListener("change", handler);
      return () => mq.removeEventListener("change", handler);
    }
    mq.addListener(handler);
    return () => mq.removeListener(handler);
  }, []);

  // Reconcile from server-stored theme once per logged-in user. The dep is
  // user.id so this fires on login/user-swap, not on unrelated user-object
  // mutations.
  useEffect(() => {
    if (!user) return;
    const serverTheme = user.theme;
    if (serverTheme !== "light" && serverTheme !== "dark") return;
    if (serverTheme === theme && readPersistedTheme() === serverTheme) return;
    try {
      localStorage.setItem(STORAGE_KEY, serverTheme);
    } catch {
      // private-mode browsers throw; the in-memory state still applies.
    }
    setThemeState(serverTheme);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user?.id]);

  const setTheme = useCallback(
    (t: Theme) => {
      if (t === theme && readPersistedTheme() === t) return;
      setThemeState(t);
      try {
        localStorage.setItem(STORAGE_KEY, t);
      } catch {
        // private-mode browsers throw; the in-memory state still applies.
      }
      meAPI.updateProfile({ theme: t }).catch((err) => {
        console.warn("theme sync to server failed:", err);
      });
    },
    [theme],
  );

  const toggleTheme = useCallback(() => {
    setTheme(theme === "dark" ? "light" : "dark");
  }, [theme, setTheme]);

  const value: ThemeContextValue = { theme, setTheme, toggleTheme };

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme(): ThemeContextValue {
  const ctx = useContext(ThemeContext);
  if (!ctx) {
    throw new Error("useTheme must be used within a ThemeProvider");
  }
  return ctx;
}
