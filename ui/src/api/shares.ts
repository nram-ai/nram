// Share-token typed client. Capability-bearer credentials let owners delegate
// scoped access to curated projects without the recipient having an nram
// account. The raw secret (nram_s_<hex>) is returned exactly once on create
// and is never recoverable afterward — callers must capture it on the
// response.
//
// All endpoints live under /v1/me/shares because share creation is a self-
// service operation on the owner's identity; the recipient never authenticates
// against these endpoints.

import { request } from "./client";

export type SharePermission = "read" | "read_store" | "read_store_modify";

export interface ShareGrantInput {
  project_id: string;
  permission: SharePermission;
}

export interface ShareGrant {
  share_token_id: string;
  project_id: string;
  permission: SharePermission;
}

export interface ShareBinding {
  id: string;
  client_id: string;
  name: string;
  created_at: string;
}

export interface ShareToken {
  id: string;
  name: string;
  description?: string;
  token_prefix: string;
  is_one_shot: boolean;
  expires_at: string;
  consumed_at?: string;
  created_at: string;
  last_used_at?: string;
  use_count: number;
  revoked_at?: string;
  grants: ShareGrant[];
  bindings?: ShareBinding[];
}

export interface CreateShareRequest {
  name: string;
  description?: string;
  is_one_shot?: boolean;
  expires_at: string;
  grants: ShareGrantInput[];
}

export interface ShareCreatedResponse {
  share: ShareToken;
  secret: string;
}

export const sharesAPI = {
  list: () =>
    request<{ shares: ShareToken[] }>("GET", "/me/shares").then(
      (r: { shares: ShareToken[] }) => r.shares ?? [],
    ),
  create: (data: CreateShareRequest) =>
    request<ShareCreatedResponse>("POST", "/me/shares", data),
  get: (id: string) => request<ShareToken>("GET", `/me/shares/${id}`),
  updateGrants: (id: string, grants: ShareGrantInput[]) =>
    request<void>("PATCH", `/me/shares/${id}`, { grants }),
  revoke: (id: string) => request<void>("DELETE", `/me/shares/${id}`),
};
