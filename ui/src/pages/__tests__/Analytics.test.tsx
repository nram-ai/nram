/**
 * @vitest-environment happy-dom
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";

import Analytics from "../Analytics";
import * as useApi from "../../hooks/useApi";
import { useAuth } from "../../context/AuthContext";

// The page is heavy on data hooks. Stub them at import boundaries so each
// test only has to set up a minimal authenticated context.

vi.mock("../../context/AuthContext", () => ({
  useAuth: vi.fn(),
}));

vi.mock("../../hooks/useApi", () => ({
  useAnalytics: vi.fn(),
  useUsage: vi.fn(),
  useOrgs: vi.fn(),
  useOrgUsers: vi.fn(),
}));

const useAuthMock = vi.mocked(useAuth);
const useAnalyticsMock = vi.mocked(useApi.useAnalytics);
const useUsageMock = vi.mocked(useApi.useUsage);
const useOrgsMock = vi.mocked(useApi.useOrgs);
const useOrgUsersMock = vi.mocked(useApi.useOrgUsers);

// The UsageControls card labels carry this class combination. Cost rate
// editor labels and tooltip labels elsewhere on the page use different
// styling, so this filter narrows the assertions to just the filter row.
const FILTER_LABEL_CLASS = "block text-xs font-medium text-muted-foreground";

function filterControlLabels(container: HTMLElement): string[] {
  return Array.from(container.querySelectorAll(`label.${FILTER_LABEL_CLASS.split(" ").join(".")}`))
    .map((l) => l.textContent?.trim() ?? "");
}

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
  };
}

function emptyQuery<T>(data?: T) {
  return {
    data,
    isLoading: false,
    isError: false,
    error: null,
    refetch: vi.fn(),
  } as unknown as ReturnType<typeof useApi.useAnalytics> &
    ReturnType<typeof useApi.useUsage> &
    ReturnType<typeof useApi.useOrgs> &
    ReturnType<typeof useApi.useOrgUsers>;
}

beforeEach(() => {
  useAnalyticsMock.mockReturnValue(emptyQuery());
  useUsageMock.mockReturnValue(emptyQuery());
  useOrgsMock.mockReturnValue(emptyQuery([
    { id: "org-a", name: "Org Alpha", slug: "alpha", created_at: "", updated_at: "" },
    { id: "org-b", name: "Org Beta", slug: "beta", created_at: "", updated_at: "" },
  ]));
  useOrgUsersMock.mockReturnValue(emptyQuery([]));
});

describe("Analytics page — role-aware rendering", () => {
  it("renders the global title and Organization filter for administrators", () => {
    useAuthMock.mockReturnValue(authStub("administrator"));

    const { container } = render(<Analytics />);

    expect(screen.getByRole("heading", { name: /System Analytics/i })).toBeInTheDocument();
    // The filter labels live inside the UsageControls card and use a
    // distinctive class. Cost rate editor labels sit further down the page
    // and are excluded by the class match.
    const filterLabels = filterControlLabels(container);
    expect(filterLabels).toContain("Organization");
    expect(filterLabels).not.toContain("User");
  });

  it("renders the org title and User filter for org_owner (no Organization filter)", () => {
    useAuthMock.mockReturnValue(authStub("org_owner"));
    useOrgUsersMock.mockReturnValue(emptyQuery([
      { id: "u-a", email: "a@test", display_name: "Alice", role: "member", org_id: "org-self", disabled_at: null, settings: {}, created_at: "", updated_at: "" },
    ]));

    const { container } = render(<Analytics />);

    expect(screen.getByRole("heading", { name: /Organization Analytics/i })).toBeInTheDocument();
    // The org filter <select> should be absent. (We can't query by label
    // text alone because "Organization" also appears in the page heading
    // for org_owner, which would yield a false positive.)
    const filterLabels = filterControlLabels(container);
    expect(filterLabels).not.toContain("Organization");
    expect(filterLabels).toContain("User");
  });

  it("renders the user title with no Org/User filter for member", () => {
    useAuthMock.mockReturnValue(authStub("member"));

    const { container } = render(<Analytics />);

    expect(screen.getByRole("heading", { name: /Your Analytics/i })).toBeInTheDocument();
    const filterLabels = filterControlLabels(container);
    expect(filterLabels).not.toContain("Organization");
    expect(filterLabels).not.toContain("User");
  });

  it("threads org and user params into useAnalytics and useUsage", () => {
    useAuthMock.mockReturnValue(authStub("administrator"));

    render(<Analytics />);

    // Initial call has no org or user.
    const analyticsArg = useAnalyticsMock.mock.calls[0][0];
    expect(analyticsArg).toMatchObject({ org: undefined, user: undefined });

    const usageArg = useUsageMock.mock.calls[0][0];
    expect(usageArg).toMatchObject({ org: undefined, user: undefined });
  });
});
