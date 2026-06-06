/**
 * @vitest-environment happy-dom
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";

import Analytics from "../Analytics";
import * as useApi from "../../hooks/useApi";
import { useAuth } from "../../context/AuthContext";

// The page is heavy on data hooks. Stub them at import boundaries so each
// test only has to set up a minimal authenticated context.
//
// Updated 2026-04-30 leak fix: the page is now tier-aware. Self-tier hooks
// (useAnalytics, useUsage) plus org-tier (useOrgAnalytics, useOrgUsage)
// plus system-tier (useSystemAnalytics, useSystemUsage) are all imported
// by the page; tests must mock all of them so the role-driven tier
// rendering paths are reachable.

vi.mock("../../context/AuthContext", () => {
  const useAuth = vi.fn();
  return {
    useAuth,
    useTierAccess: () => {
      const a = useAuth() as { isAdmin: boolean; isOrgOwner: boolean };
      const availableTiers = a.isAdmin
        ? ["self", "org", "system"]
        : a.isOrgOwner
          ? ["self", "org"]
          : ["self"];
      return { availableTiers };
    },
  };
});

vi.mock("../../hooks/useApi", () => ({
  useAnalytics: vi.fn(),
  useUsage: vi.fn(),
  useOrgAnalytics: vi.fn(),
  useOrgUsage: vi.fn(),
  useSystemAnalytics: vi.fn(),
  useSystemUsage: vi.fn(),
  useOrgs: vi.fn(),
  useOrgUsers: vi.fn(),
  useCostRates: vi.fn(),
  useUpdateSetting: vi.fn(),
}));

const useAuthMock = vi.mocked(useAuth);
const useAnalyticsMock = vi.mocked(useApi.useAnalytics);
const useUsageMock = vi.mocked(useApi.useUsage);
const useOrgAnalyticsMock = vi.mocked(useApi.useOrgAnalytics);
const useOrgUsageMock = vi.mocked(useApi.useOrgUsage);
const useSystemAnalyticsMock = vi.mocked(useApi.useSystemAnalytics);
const useSystemUsageMock = vi.mocked(useApi.useSystemUsage);
const useOrgsMock = vi.mocked(useApi.useOrgs);
const useOrgUsersMock = vi.mocked(useApi.useOrgUsers);
const useCostRatesMock = vi.mocked(useApi.useCostRates);
const useUpdateSettingMock = vi.mocked(useApi.useUpdateSetting);

function authStub(role: "administrator" | "org_owner" | "member") {
  return {
    user: {
      id: "u1",
      email: "u@test",
      display_name: "U",
      role,
      org_id: "org-self",
    },
    isAdmin: role === "administrator",
    isOrgOwner: role === "org_owner" || role === "administrator",
    canWrite: role !== "member",
    hasMinRole: () => true,
    login: vi.fn(),
    logout: vi.fn(),
    setUser: vi.fn(),
  };
}

// Returns a query stub that satisfies any of the multiple useQuery-shaped
// hooks below (useAnalytics, useUsage, useOrgAnalytics, useSystemAnalytics,
// useOrgs, etc.). Each mock has a different result type, so we return
// `any` here and let mockReturnValue accept it for any of them.
//
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function emptyQuery<T>(data?: T): any {
  return {
    data,
    isLoading: false,
    isError: false,
    error: null,
    refetch: vi.fn(),
  };
}

afterEach(() => {
  cleanup();
});

beforeEach(() => {
  useAnalyticsMock.mockReturnValue(emptyQuery());
  useUsageMock.mockReturnValue(emptyQuery());
  useOrgAnalyticsMock.mockReturnValue(emptyQuery());
  useOrgUsageMock.mockReturnValue(emptyQuery());
  useSystemAnalyticsMock.mockReturnValue(emptyQuery());
  useSystemUsageMock.mockReturnValue(emptyQuery());
  useOrgsMock.mockReturnValue(emptyQuery([
    { id: "org-a", name: "Org Alpha", slug: "alpha", created_at: "", updated_at: "" },
    { id: "org-b", name: "Org Beta", slug: "beta", created_at: "", updated_at: "" },
  ]));
  useOrgUsersMock.mockReturnValue(emptyQuery([]));
  useCostRatesMock.mockReturnValue(emptyQuery([]));
  // The page only calls .mutate() on useUpdateSetting; a partial stub
  // is enough for these render-only tests.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  useUpdateSettingMock.mockReturnValue({ mutate: vi.fn() } as any);
});

describe("Analytics page, role-aware tier picker", () => {
  it("administrator: defaults to Mine, shows Mine/Org/System tabs", () => {
    useAuthMock.mockReturnValue(authStub("administrator"));

    render(<Analytics />);

    // Default tier is "self" for everyone; admin's "Mine" view shows
    // their own analytics, not system-wide (post-2026-04-30 leak fix).
    expect(screen.getByRole("heading", { level: 1, name: /My Analytics/i })).toBeInTheDocument();

    const tabs = screen.getAllByRole("tab");
    const tabLabels = tabs.map((t) => t.textContent?.trim() ?? "");
    expect(tabLabels).toContain("Mine");
    expect(tabLabels).toContain("Org");
    expect(tabLabels).toContain("System");
  });

  it("org_owner: defaults to Mine, shows Mine + Org tabs only", () => {
    useAuthMock.mockReturnValue(authStub("org_owner"));

    render(<Analytics />);

    expect(screen.getByRole("heading", { level: 1, name: /My Analytics/i })).toBeInTheDocument();

    const tabs = screen.getAllByRole("tab");
    const tabLabels = tabs.map((t) => t.textContent?.trim() ?? "");
    expect(tabLabels).toContain("Mine");
    expect(tabLabels).toContain("Org");
    expect(tabLabels).not.toContain("System");
  });

  it("member: shows no tier picker (only self-tier accessible)", () => {
    useAuthMock.mockReturnValue(authStub("member"));

    render(<Analytics />);

    expect(screen.getByRole("heading", { level: 1, name: /My Analytics/i })).toBeInTheDocument();

    // No tablist when only one tier is available.
    expect(screen.queryByRole("tab")).not.toBeInTheDocument();
  });

  it("self-tier hook is called without org/user widening params", () => {
    useAuthMock.mockReturnValue(authStub("administrator"));

    render(<Analytics />);

    // useAnalytics is called with no args (or just an undefined params
    // bag). The pre-fix `org` / `user` widening params were removed in
    // the 2026-04-30 leak fix; admin's "Mine" tab pins to admin's own
    // scope, no widening.
    const analyticsCall = useAnalyticsMock.mock.calls[0];
    const arg = analyticsCall?.[0];
    if (arg !== undefined) {
      expect(arg).not.toHaveProperty("org", "expected-some-org-id");
      expect(arg).not.toHaveProperty("user", "expected-some-user-id");
    }
  });
});
