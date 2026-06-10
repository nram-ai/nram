/**
 * @vitest-environment happy-dom
 *
 * After creating a share, the owner-facing create panel must show this share's
 * per-share connector URL (/mcp/{share_id}) with a copy control, so a
 * bearer-direct recipient has a URL to add (previously only the token and magic
 * link were offered).
 */
import { describe, it, expect, vi, afterEach } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";

import { CreateSharePanel } from "../Shares";

const SHARE_ID = "abc12345-1111-2222-3333-444455556666";
const projects = [{ id: "proj-1", name: "Alpha", slug: "alpha" }];
const createMutateAsync = vi.fn().mockResolvedValue({
  share: { id: SHARE_ID, name: "Q3 share", is_one_shot: false },
  secret: "nram_s_thesecret",
});

const noopMutation = () => ({ mutate: vi.fn(), mutateAsync: vi.fn(), isPending: false });

vi.mock("../../hooks/useApi", () => ({
  useMeProjects: () => ({ data: projects, isLoading: false }),
  useCreateMeShare: () => ({ mutateAsync: createMutateAsync, isPending: false }),
  useMeShares: () => ({ data: [], isLoading: false }),
  useMeShareDetail: () => ({ data: null }),
  useUpdateMeShareGrants: () => noopMutation(),
  useRevokeMeShare: () => noopMutation(),
  useRevokeMeOAuthClient: () => noopMutation(),
}));

afterEach(() => cleanup());

describe("CreateSharePanel created result", () => {
  it("renders the per-share /mcp/{id} URL with a copy control", async () => {
    render(<CreateSharePanel open={true} onClose={() => {}} />);

    fireEvent.change(screen.getByPlaceholderText(/architecture review/i), {
      target: { value: "Q3 share" },
    });

    // Add a project grant via the "+ Add project" selector.
    const addSelect = screen.getByText("+ Add project").closest("select");
    if (!addSelect) throw new Error("add-project select not found");
    fireEvent.change(addSelect, { target: { value: "proj-1" } });

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    const url = await screen.findByText((t) => t.includes("/mcp/" + SHARE_ID));
    expect(url).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /copy url/i })).toBeInTheDocument();
  });
});
