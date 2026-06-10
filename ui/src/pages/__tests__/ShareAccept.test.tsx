/**
 * @vitest-environment happy-dom
 *
 * The recipient-facing share accept page must surface the per-share connector
 * URL (/mcp/{share_id}) the backend returns, so the recipient adds that exact
 * URL as a distinct MCP connector.
 */
import { describe, it, expect, vi, afterEach } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";
import { MemoryRouter } from "react-router-dom";

import ShareAccept from "../ShareAccept";

// vi.hoisted so the literal is available inside the hoisted vi.mock factory.
const { payload } = vi.hoisted(() => ({
  payload: {
    owner_name: "Alice",
    share_name: "Q3 architecture review",
    expires_at: "January 2, 2027 at 12:00 UTC",
    grants: [{ project_name: "Alpha", project_slug: "alpha", permission: "Read only" }],
    mcp_server_url: "https://development.nram.ai/mcp/11111111-1111-1111-1111-111111111111",
    share_token: "nram_s_examplesecret",
  },
}));

vi.mock("../../api/client", () => ({
  shareAcceptAPI: { get: vi.fn().mockResolvedValue(payload) },
}));

afterEach(() => cleanup());

describe("ShareAccept page", () => {
  it("renders the per-share MCP server URL returned by the backend", async () => {
    render(
      <MemoryRouter initialEntries={["/share/accept?token=nram_s_examplesecret"]}>
        <ShareAccept />
      </MemoryRouter>,
    );
    expect(await screen.findByText(payload.mcp_server_url)).toBeInTheDocument();
  });
});
