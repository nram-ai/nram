import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";

const BASE = "http://localhost/v1";

const handlers = [
  // --- Auth ---
  http.post(`${BASE}/auth/login`, () => {
    return HttpResponse.json({
      token: "test-jwt",
      user: {
        id: "u1",
        email: "admin@test.com",
        display_name: "Admin",
        role: "administrator",
        org_id: "org1",
      },
    });
  }),

  http.post(`${BASE}/auth/lookup`, () => {
    return HttpResponse.json({ method: "local" });
  }),

  // --- Health ---
  http.get(`${BASE}/health`, () => {
    return HttpResponse.json({
      status: "ok",
      version: "1.0.0",
      backend: "sqlite",
      database: { status: "ok", latency_ms: 1 },
      providers: {
        embedding: {
          status: "ok",
          provider: "ollama",
          model: "nomic-embed-text",
          latency_ms: 5,
        },
        fact_extraction: {
          status: "ok",
          provider: "ollama",
          model: "llama3",
          latency_ms: 10,
        },
        entity_extraction: {
          status: "ok",
          provider: "ollama",
          model: "llama3",
          latency_ms: 10,
        },
      },
      enrichment_queue: null,
      uptime_seconds: 3600,
    });
  }),

  // --- Admin: Setup ---
  http.get(`${BASE}/admin/setup/status`, () => {
    return HttpResponse.json({ setup_complete: true, backend: "sqlite" });
  }),

  http.post(`${BASE}/admin/setup`, () => {
    return HttpResponse.json({
      user: {
        id: "u1",
        email: "admin@test.com",
        display_name: "Admin",
        role: "administrator",
        org_id: "org1",
        disabled_at: null,
        settings: {},
        created_at: "2026-01-01T00:00:00Z",
        updated_at: "2026-01-01T00:00:00Z",
      },
      api_key: "nram_k_test",
      token: "test-jwt",
      message: "Setup complete",
    });
  }),

  // --- Dashboard / Activity / Analytics / Usage ---
  http.get(`${BASE}/dashboard`, () => {
    return HttpResponse.json({
      total_memories: 100,
      total_projects: 5,
      total_users: 3,
      total_entities: 50,
      total_organizations: 2,
      memories_by_project: [
        { project_id: "p1", project_name: "Test", count: 100 },
      ],
      enrichment_queue: { pending: 0, processing: 0, failed: 0 },
    });
  }),

  http.get(`${BASE}/activity`, () => {
    return HttpResponse.json({
      events: [
        {
          id: "e1",
          type: "memory.stored",
          summary: "Memory stored",
          timestamp: "2026-01-01T00:00:00Z",
        },
      ],
    });
  }),

  http.get(`${BASE}/analytics`, () => {
    return HttpResponse.json({
      memory_counts: { total: 100, active: 90, deleted: 10, enriched: 50 },
      most_recalled: [],
      least_recalled: [],
      dead_weight: [],
      enrichment_stats: {
        total_processed: 50,
        success_rate: 95.0,
        failure_rate: 5.0,
        avg_latency_ms: 200,
      },
    });
  }),

  http.get(`${BASE}/usage`, () => {
    return HttpResponse.json({
      groups: [
        {
          key: "store",
          tokens_input: 1000,
          tokens_output: 500,
          call_count: 10,
        },
      ],
      totals: { tokens_input: 1000, tokens_output: 500, call_count: 10 },
    });
  }),

  // --- Admin: Orgs ---
  http.get(`${BASE}/admin/orgs`, () => {
    return HttpResponse.json({
      data: [
        {
          id: "org1",
          namespace_id: "ns1",
          name: "Test Org",
          slug: "test-org",
          settings: {},
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        },
      ],
    });
  }),

  http.get(`${BASE}/admin/orgs/:id`, () => {
    return HttpResponse.json({
      id: "org1",
      namespace_id: "ns1",
      name: "Test Org",
      slug: "test-org",
      settings: {},
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.post(`${BASE}/admin/orgs`, async ({ request }) => {
    const body = (await request.json()) as Record<string, unknown>;
    return HttpResponse.json({
      ...body,
      id: "org-new",
      namespace_id: "ns-new",
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.put(`${BASE}/admin/orgs/:id`, async ({ request, params }) => {
    const body = (await request.json()) as Record<string, unknown>;
    return HttpResponse.json({
      ...body,
      id: params.id,
      namespace_id: "ns1",
      name: "Updated",
      slug: "updated",
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.delete(`${BASE}/admin/orgs/:id`, () => {
    return new HttpResponse(null, { status: 204 });
  }),

  // --- Admin: Users ---
  http.get(`${BASE}/admin/users`, () => {
    return HttpResponse.json({
      data: [
        {
          id: "u1",
          email: "admin@test.com",
          display_name: "Admin",
          role: "administrator",
          org_id: "org1",
          disabled_at: null,
          settings: {},
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        },
      ],
    });
  }),

  http.get(`${BASE}/admin/users/:id`, () => {
    return HttpResponse.json({
      id: "u1",
      email: "admin@test.com",
      display_name: "Admin",
      role: "administrator",
      org_id: "org1",
      disabled_at: null,
      settings: {},
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.post(`${BASE}/admin/users`, () => {
    return HttpResponse.json({
      id: "u1",
      email: "admin@test.com",
      display_name: "Admin",
      role: "administrator",
      org_id: "org1",
      disabled_at: null,
      settings: {},
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.put(`${BASE}/admin/users/:id`, () => {
    return HttpResponse.json({
      id: "u1",
      email: "admin@test.com",
      display_name: "Admin",
      role: "administrator",
      org_id: "org1",
      disabled_at: null,
      settings: {},
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.delete(`${BASE}/admin/users/:id`, () => {
    return new HttpResponse(null, { status: 204 });
  }),

  http.post(`${BASE}/admin/users/:userId/api-keys`, () => {
    return HttpResponse.json({
      id: "key1",
      key: "nram_k_test123",
      label: "test-key",
      prefix: "nram_k_te",
      scopes: [],
      expires_at: null,
      created_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.delete(`${BASE}/admin/users/:userId/api-keys/:keyId`, () => {
    return new HttpResponse(null, { status: 204 });
  }),

  // --- Admin: Projects ---
  http.get(`${BASE}/admin/projects`, () => {
    return HttpResponse.json({
      data: [
        {
          id: "p1",
          namespace_id: "ns-p1",
          owner_namespace_id: "ns1",
          name: "Test Project",
          slug: "test-project",
          path: "/org/user/test-project",
          description: "A test",
          default_tags: [],
          settings: {
            dedup_threshold: 0.9,
            enrichment_enabled: true,
            ranking_weights: {
              recency: 0.3,
              relevance: 0.5,
              importance: 0.2,
            },
          },
          memory_count: 42,
          entity_count: 10,
          owner: { id: "u1", email: "admin@test.com" },
          organization: { id: "org1", name: "Test Org" },
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        },
      ],
    });
  }),

  http.get(`${BASE}/admin/projects/:id`, () => {
    return HttpResponse.json({
      id: "p1",
      namespace_id: "ns-p1",
      owner_namespace_id: "ns1",
      name: "Test Project",
      slug: "test-project",
      description: "A test",
      default_tags: [],
      settings: {
        dedup_threshold: 0.9,
        enrichment_enabled: true,
        ranking_weights: {
          recency: 0.3,
          relevance: 0.5,
          importance: 0.2,
        },
      },
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.post(`${BASE}/admin/projects`, () => {
    return HttpResponse.json({
      id: "p1",
      namespace_id: "ns-p1",
      owner_namespace_id: "ns1",
      name: "Test Project",
      slug: "test-project",
      description: "A test",
      default_tags: [],
      settings: {
        dedup_threshold: 0.9,
        enrichment_enabled: true,
        ranking_weights: {
          recency: 0.3,
          relevance: 0.5,
          importance: 0.2,
        },
      },
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.put(`${BASE}/admin/projects/:id`, () => {
    return HttpResponse.json({
      id: "p1",
      namespace_id: "ns-p1",
      owner_namespace_id: "ns1",
      name: "Test Project",
      slug: "test-project",
      description: "A test",
      default_tags: [],
      settings: {
        dedup_threshold: 0.9,
        enrichment_enabled: true,
        ranking_weights: {
          recency: 0.3,
          relevance: 0.5,
          importance: 0.2,
        },
      },
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.delete(`${BASE}/admin/projects/:id`, () => {
    return new HttpResponse(null, { status: 204 });
  }),

  // --- Admin: Providers ---
  http.get(`${BASE}/admin/providers`, () => {
    return HttpResponse.json({
      embedding: {
        configured: true,
        type: "ollama",
        url: "http://localhost:11434",
        model: "nomic-embed-text",
        dimensions: 768,
      },
      fact: {
        configured: true,
        type: "ollama",
        url: "http://localhost:11434",
        model: "llama3",
      },
      entity: {
        configured: true,
        type: "ollama",
        url: "http://localhost:11434",
        model: "llama3",
      },
    });
  }),

  http.put(`${BASE}/admin/providers/:slot`, () => {
    return HttpResponse.json({ status: "ok" });
  }),

  http.post(`${BASE}/admin/providers/test`, () => {
    return HttpResponse.json({
      success: true,
      message: "Connection successful",
      latency_ms: 42,
    });
  }),

  http.get(`${BASE}/admin/providers/ollama/models`, () => {
    return HttpResponse.json([
      {
        name: "llama3",
        size: 4700000000,
        modified_at: "2026-01-01T00:00:00Z",
      },
    ]);
  }),

  http.post(`${BASE}/admin/providers/ollama/pull`, () => {
    return HttpResponse.json({ status: "accepted", model: "llama3" });
  }),

  // --- Admin: Settings ---
  http.get(`${BASE}/admin/settings`, ({ request }) => {
    const url = new URL(request.url);
    if (url.searchParams.get("schema") === "true") {
      return HttpResponse.json({
        data: [
          {
            key: "enrichment.enabled",
            type: "boolean",
            default_value: true,
            description: "Enable enrichment",
            category: "enrichment",
          },
        ],
      });
    }
    return HttpResponse.json({
      data: [
        {
          key: "enrichment.enabled",
          value: true,
          scope: "global",
          updated_at: "2026-01-01T00:00:00Z",
        },
      ],
    });
  }),

  http.put(`${BASE}/admin/settings`, () => {
    return HttpResponse.json({ status: "ok" });
  }),

  // --- Admin: Webhooks ---
  http.get(`${BASE}/admin/webhooks`, () => {
    return HttpResponse.json({
      data: [
        {
          id: "wh1",
          url: "https://example.com/hook",
          events: ["memory.stored"],
          scope: "global",
          active: true,
          last_fired: null,
          last_status: null,
          failure_count: 0,
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        },
      ],
    });
  }),

  http.post(`${BASE}/admin/webhooks`, () => {
    return HttpResponse.json({
      id: "wh1",
      url: "https://example.com/hook",
      events: ["memory.stored"],
      scope: "global",
      active: true,
      last_fired: null,
      last_status: null,
      failure_count: 0,
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.put(`${BASE}/admin/webhooks/:id`, () => {
    return HttpResponse.json({
      id: "wh1",
      url: "https://example.com/hook",
      events: ["memory.stored"],
      scope: "global",
      active: true,
      last_fired: null,
      last_status: null,
      failure_count: 0,
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.delete(`${BASE}/admin/webhooks/:id`, () => {
    return new HttpResponse(null, { status: 204 });
  }),

  http.post(`${BASE}/admin/webhooks/:id/test`, () => {
    return HttpResponse.json({
      success: true,
      status_code: 200,
      message: "OK",
      latency_ms: 100,
    });
  }),

  // --- Admin: Database ---
  http.get(`${BASE}/admin/database`, () => {
    return HttpResponse.json({
      backend: "sqlite",
      version: "3.40.0",
      sqlite: { file_path: "/data/nram.db", file_size_bytes: 1048576 },
      data_counts: {
        memories: 100,
        entities: 50,
        projects: 5,
        users: 3,
        organizations: 2,
      },
    });
  }),

  http.post(`${BASE}/admin/database/test`, () => {
    return HttpResponse.json({
      success: true,
      message: "Connected",
      pgvector_installed: true,
      latency_ms: 15,
    });
  }),

  http.post(`${BASE}/admin/database/migrate`, () => {
    return HttpResponse.json({
      status: "completed",
      message: "Migration complete",
    });
  }),

  // --- Enrichment ---
  http.get(`${BASE}/enrichment`, () => {
    return HttpResponse.json({
      counts: { pending: 2, processing: 1, completed: 100, failed: 3 },
      items: [
        {
          id: "eq1",
          memory_id: "m1",
          status: "pending",
          attempts: 0,
          created_at: "2026-01-01T00:00:00Z",
        },
      ],
      paused: false,
    });
  }),

  http.post(`${BASE}/enrichment/retry`, () => {
    return HttpResponse.json({ retried: 3 });
  }),

  http.post(`${BASE}/enrichment/pause`, async ({ request }) => {
    const body = (await request.json()) as { paused: boolean };
    return HttpResponse.json({ paused: body.paused });
  }),

  http.post(`${BASE}/enrichment/test-prompt`, () => {
    return HttpResponse.json({
      output: "test output",
      parsed: { facts: [] },
      latency_ms: 150,
    });
  }),

  // --- Graph ---
  http.get(`${BASE}/graph`, () => {
    return HttpResponse.json({
      entities: [
        {
          id: "ent1",
          name: "Test Entity",
          canonical: "test_entity",
          entity_type: "concept",
          mention_count: 5,
          aliases: [],
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        },
      ],
      relationships: [
        {
          id: "rel1",
          source_id: "ent1",
          target_id: "ent2",
          relation: "related_to",
          weight: 0.8,
        },
      ],
    });
  }),

  // --- Namespaces ---
  http.get(`${BASE}/namespaces/tree`, () => {
    return HttpResponse.json({
      tree: [
        {
          id: "ns-root",
          name: "Root",
          slug: "root",
          kind: "root",
          path: "/",
          depth: 0,
          children: [],
        },
      ],
    });
  }),

  // --- Admin: OAuth Clients ---
  http.get(`${BASE}/admin/oauth/clients`, () => {
    return HttpResponse.json([
      {
        id: "oc1",
        name: "Test Client",
        client_id: "client_abc",
        type: "manual",
        client_type: "confidential",
        redirect_uris: ["http://localhost/callback"],
        created_at: "2026-01-01T00:00:00Z",
      },
    ]);
  }),

  http.post(`${BASE}/admin/oauth/clients`, () => {
    return HttpResponse.json({
      id: "oc-new",
      name: "New Client",
      client_id: "client_xyz",
      type: "manual",
      client_type: "confidential",
      redirect_uris: [],
      created_at: "2026-01-01T00:00:00Z",
      client_secret: "secret_123",
    });
  }),

  http.delete(`${BASE}/admin/oauth/clients/:id`, () => {
    return new HttpResponse(null, { status: 204 });
  }),

  // --- Admin: IdP ---
  http.get(`${BASE}/admin/oauth/idp`, () => {
    return HttpResponse.json([
      {
        id: "idp1",
        org_id: "org1",
        provider_type: "oidc",
        client_id: "client_idp",
        issuer_url: "https://idp.example.com",
        allowed_domains: ["example.com"],
        auto_provision: true,
        created_at: "2026-01-01T00:00:00Z",
        updated_at: "2026-01-01T00:00:00Z",
      },
    ]);
  }),

  http.post(`${BASE}/admin/oauth/idp`, () => {
    return HttpResponse.json({
      id: "idp1",
      org_id: "org1",
      provider_type: "oidc",
      client_id: "client_idp",
      issuer_url: "https://idp.example.com",
      allowed_domains: ["example.com"],
      auto_provision: true,
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.delete(`${BASE}/admin/oauth/idp/:id`, () => {
    return new HttpResponse(null, { status: 204 });
  }),

  // --- Memory API (project-scoped) ---
  http.post(`${BASE}/projects/:projectId/memories`, () => {
    return HttpResponse.json({
      id: "m1",
      project_id: "p1",
      project_slug: "test-project",
      path: "/test-project",
      content: "test content",
      tags: ["test"],
      enriched: false,
      enrichment_queued: false,
      latency_ms: 5,
    });
  }),

  http.get(`${BASE}/projects/:projectId/memories/export`, () => {
    return HttpResponse.json({
      version: "1.0",
      exported_at: "2026-01-01T00:00:00Z",
      project: { id: "p1", name: "Test Project", slug: "test-project" },
      memories: [],
      stats: { memory_count: 0, entity_count: 0, relationship_count: 0 },
    });
  }),

  http.post(`${BASE}/projects/:projectId/memories/recall`, () => {
    return HttpResponse.json({
      memories: [
        {
          id: "m1",
          project_id: "p1",
          project_slug: "test-project",
          path: "/test-project",
          content: "recalled content",
          tags: [],
          source: null,
          score: 0.95,
          similarity: 0.95,
          metadata: {},
          created_at: "2026-01-01T00:00:00Z",
        },
      ],
      graph: {
        entities: [{ id: "e1", name: "Test", type: "concept" }],
        relationships: [
          {
            source_id: "e1",
            target_id: "e2",
            relation: "related",
            weight: 0.5,
          },
        ],
      },
      total_searched: 100,
      latency_ms: 25,
    });
  }),

  http.post(`${BASE}/projects/:projectId/memories/forget`, () => {
    return HttpResponse.json({ deleted: 5, latency_ms: 10 });
  }),

  http.post(`${BASE}/projects/:projectId/memories/enrich`, () => {
    return HttpResponse.json({ queued: 3, skipped: 0, latency_ms: 5 });
  }),

  http.get(`${BASE}/projects/:projectId/memories/:memoryId`, () => {
    return HttpResponse.json({
      id: "m1",
      content: "test content",
      source: null,
      tags: [],
      enriched: false,
      metadata: {},
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
  }),

  http.put(`${BASE}/projects/:projectId/memories/:memoryId`, () => {
    return HttpResponse.json({
      id: "m1",
      project_id: "p1",
      content: "updated content",
      tags: ["updated"],
      previous_content: "test content",
      re_embedded: true,
      latency_ms: 10,
    });
  }),

  http.delete(`${BASE}/projects/:projectId/memories/:memoryId`, () => {
    return HttpResponse.json({ deleted: 1, latency_ms: 2 });
  }),

  http.get(`${BASE}/projects/:projectId/memories`, () => {
    return HttpResponse.json({
      data: [
        {
          id: "m1",
          content: "test content",
          source: null,
          tags: [],
          confidence: 1,
          importance: 0.5,
          access_count: 0,
          enriched: false,
          metadata: {},
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        },
      ],
      pagination: { total: 1, limit: 50, offset: 0 },
    });
  }),
];

export const server = setupServer(...handlers);
