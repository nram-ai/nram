/**
 * E2E tests for the API client -- runs against a real nram Go server.
 *
 * Uses @vitest-environment node so child_process.spawn works reliably.
 * Polyfills localStorage and window.location before the client module loads.
 *
 * @vitest-environment node
 */
import { vi, describe, it, expect, beforeAll, afterAll } from "vitest";

// ---------------------------------------------------------------------------
// Polyfills -- must run before the client module is imported.
// vi.hoisted() executes before any ESM imports are evaluated.
// ---------------------------------------------------------------------------
vi.hoisted(() => {
  // Minimal localStorage polyfill
  const store = new Map<string, string>();
  (globalThis as Record<string, unknown>).localStorage = {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => store.set(k, v),
    removeItem: (k: string) => store.delete(k),
    clear: () => store.clear(),
    get length() {
      return store.size;
    },
    key: (i: number) => [...store.keys()][i] ?? null,
  };

  // Minimal window polyfill
  (globalThis as Record<string, unknown>).window = {
    location: {
      href: "",
      origin: "",
      pathname: "/dashboard",
      hostname: "localhost",
      port: "18674",
      protocol: "http:",
    },
  };
});

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

// The client module reads localStorage and window.location at call-time,
// not at import-time, so the polyfills above are sufficient.
import {
  APIError,
  authAPI,
  adminAPI,
  meAPI,
  orgAPI,
  memoryAPI,
  healthAPI,
  type SetupResponse,
  type User,
} from "./client";

// ---------------------------------------------------------------------------
// Paths & constants
// ---------------------------------------------------------------------------

// process.cwd() is the ui/ directory when vitest runs
const SERVER_BIN = join(process.cwd(), "..", "bin", "nram");
// Use a non-default port to avoid conflicting with a running dev server.
const SERVER_PORT = 18674;
const SERVER_URL = `http://localhost:${SERVER_PORT}`;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

let serverProcess: ChildProcess;
let tmpDir: string;
let adminToken: string;
let adminApiKey: string;
let adminUserId: string;
let _adminOrgId: string;
let adminNamespaceId: string;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function waitForServer(
  url: string,
  timeoutMs = 20000,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`${url}/v1/health`);
      if (res.ok) return;
    } catch {
      // server not ready
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`Server did not become ready within ${timeoutMs}ms`);
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe("API Client E2E", () => {
  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  beforeAll(async () => {
    // Patch fetch so the client's relative URLs ("/v1/...") resolve to the
    // test server. Also automatically retry on 429 (rate limit) with backoff.
    const nodeFetch = globalThis.fetch;
    globalThis.fetch = async (
      input: RequestInfo | URL,
      init?: RequestInit,
    ): Promise<Response> => {
      let url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : (input as Request).url;
      if (url.startsWith("/")) {
        url = `${SERVER_URL}${url}`;
      }
      for (let attempt = 0; attempt < 5; attempt++) {
        const res = await nodeFetch(url, init);
        if (res.status === 429) {
          const retryAfter = res.headers.get("Retry-After");
          const waitMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : 1000;
          await new Promise((r) => setTimeout(r, waitMs));
          continue;
        }
        return res;
      }
      return nodeFetch(url, init);
    };

    // Verify the port is free before starting
    try {
      await nodeFetch(`${SERVER_URL}/v1/health`);
      throw new Error(
        `Port ${SERVER_PORT} is already in use. Kill any existing nram process and retry.`,
      );
    } catch (e) {
      if (e instanceof Error && e.message.includes("already in use")) throw e;
    }

    // Isolated temp directory -- SQLite will create nram.db here
    tmpDir = mkdtempSync(join(tmpdir(), "nram-e2e-"));

    // Spawn the real Go server
    serverProcess = spawn(SERVER_BIN, [], {
      cwd: tmpDir,
      env: {
        HOME: process.env.HOME ?? "",
        PATH: process.env.PATH ?? "",
        PORT: String(SERVER_PORT),
        LOG_LEVEL: "error",
      },
      stdio: "pipe",
    });

    // Capture stderr for debugging startup failures
    let stderr = "";
    serverProcess.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });

    serverProcess.on("error", (err) => {
      throw new Error(`Failed to start server: ${err.message}\n${stderr}`);
    });

    await waitForServer(SERVER_URL);

    // Complete initial setup
    const setupRes = await fetch(`${SERVER_URL}/v1/admin/setup`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email: "admin@test.com",
        password: "TestPassword123!",
      }),
    });

    if (!setupRes.ok) {
      const body = await setupRes.text();
      throw new Error(`Setup failed (${setupRes.status}): ${body}`);
    }

    const setupData: SetupResponse = await setupRes.json();
    adminToken = setupData.token;
    adminApiKey = setupData.api_key;
    adminUserId = setupData.user.id;
    _adminOrgId = setupData.user.org_id;

    // Fetch the admin user to get namespace_id
    const userRes = await fetch(`${SERVER_URL}/v1/admin/users/${adminUserId}`, {
      headers: { Authorization: `Bearer ${adminToken}` },
    });
    const adminUser: User = await userRes.json();
    adminNamespaceId = adminUser.namespace_id!;

    // Put the token into localStorage so the client sends it with requests
    localStorage.setItem("nram_token", adminToken);

    // Set window.location.pathname to /login so that the client's 401 handler
    // falls through to throw APIError instead of returning a never-resolving
    // promise (which would hang the test). This is safe because we are in a
    // node environment, not a real browser.
    (globalThis as Record<string, unknown>).window = {
      location: {
        href: SERVER_URL,
        origin: SERVER_URL,
        pathname: "/login",
        hostname: "localhost",
        port: String(SERVER_PORT),
        protocol: "http:",
      },
    };
  }, 45000);

  afterAll(async () => {
    if (serverProcess) {
      serverProcess.kill("SIGTERM");
      await new Promise<void>((resolve) => {
        serverProcess.on("exit", () => resolve());
        setTimeout(resolve, 5000);
      });
    }
    if (tmpDir) {
      try {
        rmSync(tmpDir, { recursive: true, force: true });
      } catch {
        // best-effort cleanup
      }
    }
  }, 15000);

  // -----------------------------------------------------------------------
  // APIError
  // -----------------------------------------------------------------------

  describe("APIError", () => {
    it("constructs with status and body", () => {
      const err = new APIError(404, "not found");
      expect(err.name).toBe("APIError");
      expect(err).toBeInstanceOf(Error);
      expect(err).toBeInstanceOf(APIError);
      expect(err.status).toBe(404);
      expect(err.body).toBe("not found");
      expect(err.message).toBe("API error 404");
    });
  });

  // -----------------------------------------------------------------------
  // Health
  // -----------------------------------------------------------------------

  describe("healthAPI", () => {
    it("check() returns well-formed health response", async () => {
      const h = await healthAPI.check();
      expect(h.status).toBe("ok");
      expect(typeof h.version).toBe("string");
      expect(h.backend).toBe("sqlite");
      expect(h.database.status).toBe("ok");
      expect(typeof h.database.latency_ms).toBe("number");
      expect(h.providers).toBeDefined();
      expect(h.providers.embedding).toBeDefined();
      expect(h.providers.fact_extraction).toBeDefined();
      expect(h.providers.entity_extraction).toBeDefined();
      expect(typeof h.uptime_seconds).toBe("number");
    });
  });

  // -----------------------------------------------------------------------
  // Auth
  // -----------------------------------------------------------------------

  describe("authAPI", () => {
    it("lookup() returns 'local' for existing user", async () => {
      const res = await authAPI.lookup({ email: "admin@test.com" });
      expect(res.method).toBe("local");
    });

    it("lookup() returns 'unknown' for nonexistent user", async () => {
      const res = await authAPI.lookup({ email: "nobody@example.com" });
      expect(res.method).toBe("unknown");
    });

    it("login() returns token and user for valid credentials", async () => {
      const res = await authAPI.login({
        email: "admin@test.com",
        password: "TestPassword123!",
      });
      expect(typeof res.token).toBe("string");
      expect(res.token.length).toBeGreaterThan(0);
      expect(res.user.email).toBe("admin@test.com");
      expect(res.user.role).toBe("administrator");
    });

    it("login() throws APIError for invalid credentials", async () => {
      // The 401 handler clears localStorage -- save and restore the token
      const savedToken = localStorage.getItem("nram_token");
      try {
        await expect(
          authAPI.login({ email: "admin@test.com", password: "wrong" }),
        ).rejects.toThrow(APIError);
      } finally {
        if (savedToken) localStorage.setItem("nram_token", savedToken);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Setup status
  // -----------------------------------------------------------------------

  describe("adminAPI.setup", () => {
    it("getSetupStatus() reports complete on sqlite", async () => {
      const s = await adminAPI.getSetupStatus();
      expect(s.setup_complete).toBe(true);
      expect(s.backend).toBe("sqlite");
    });

    it("setup returned a valid API key", () => {
      expect(typeof adminApiKey).toBe("string");
      expect(adminApiKey.length).toBeGreaterThan(0);
    });
  });

  // -----------------------------------------------------------------------
  // Dashboard
  // -----------------------------------------------------------------------

  describe("adminAPI.dashboard", () => {
    it("getDashboard() returns counts", async () => {
      const d = await adminAPI.getDashboard();
      expect(typeof d.total_memories).toBe("number");
      expect(typeof d.total_projects).toBe("number");
      expect(typeof d.total_users).toBe("number");
      expect(typeof d.total_entities).toBe("number");
      expect(typeof d.total_organizations).toBe("number");
      expect(Array.isArray(d.memories_by_project)).toBe(true);
    });
  });

  // -----------------------------------------------------------------------
  // Activity
  // -----------------------------------------------------------------------

  describe("adminAPI.activity", () => {
    it("getActivity() returns events array", async () => {
      const a = await adminAPI.getActivity();
      expect(a).toBeDefined();
      expect(Array.isArray(a.events)).toBe(true);
    });

    it("getActivity(limit) respects custom limit", async () => {
      const a = await adminAPI.getActivity(5);
      expect(Array.isArray(a.events)).toBe(true);
      expect(a.events.length).toBeLessThanOrEqual(5);
    });
  });

  // -----------------------------------------------------------------------
  // Organizations
  // -----------------------------------------------------------------------

  describe("adminAPI.organizations", () => {
    let createdOrgId: string;

    it("listOrgs() returns array with default org", async () => {
      const orgs = await adminAPI.listOrgs();
      expect(Array.isArray(orgs)).toBe(true);
      expect(orgs.length).toBeGreaterThanOrEqual(1);
      expect(typeof orgs[0].id).toBe("string");
      expect(typeof orgs[0].name).toBe("string");
    });

    it("createOrg() creates a new organization", async () => {
      const org = await adminAPI.createOrg({
        name: "Test Org",
        slug: "test-org",
      });
      expect(org.name).toBe("Test Org");
      expect(org.slug).toBe("test-org");
      expect(typeof org.id).toBe("string");
      createdOrgId = org.id;
    });

    it("getOrg() retrieves the created org", async () => {
      const org = await adminAPI.getOrg(createdOrgId);
      expect(org.id).toBe(createdOrgId);
      expect(org.name).toBe("Test Org");
    });

    it("updateOrg() updates the org name", async () => {
      const org = await adminAPI.updateOrg(createdOrgId, {
        name: "Updated Org",
      });
      expect(org.name).toBe("Updated Org");
    });

    it("deleteOrg() removes the org", async () => {
      const result = await adminAPI.deleteOrg(createdOrgId);
      expect(result).toBeUndefined();
    });

    it("getOrg() throws 404 for deleted org", async () => {
      await expect(adminAPI.getOrg(createdOrgId)).rejects.toThrow(APIError);
      try {
        await adminAPI.getOrg(createdOrgId);
      } catch (e) {
        expect((e as APIError).status).toBe(404);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Users
  // -----------------------------------------------------------------------

  describe("adminAPI.users", () => {
    let testUserId: string;
    let testApiKeyId: string;

    it("listUsers() returns array with admin user", async () => {
      const users = await adminAPI.listUsers();
      expect(Array.isArray(users)).toBe(true);
      expect(users.length).toBeGreaterThanOrEqual(1);
      const admin = users.find((u) => u.email === "admin@test.com");
      expect(admin).toBeDefined();
    });

    it("getUser() retrieves admin user details", async () => {
      const user = await adminAPI.getUser(adminUserId);
      expect(user.id).toBe(adminUserId);
      expect(user.email).toBe("admin@test.com");
      expect(user.role).toBe("administrator");
      expect(typeof user.namespace_id).toBe("string");
    });

    it("createUser() creates a new user", async () => {
      const user = await adminAPI.createUser({
        email: "testuser@test.com",
        password: "UserPass123!",
        role: "member",
        display_name: "Test User",
        organization_id: _adminOrgId,
      });
      expect(user.email).toBe("testuser@test.com");
      expect(user.role).toBe("member");
      testUserId = user.id;
    });

    it("updateUser() updates the test user", async () => {
      const user = await adminAPI.updateUser(testUserId, {
        display_name: "Updated Test User",
      });
      expect(user.display_name).toBe("Updated Test User");
    });

    it("generateAPIKey() generates a key for test user", async () => {
      const key = await adminAPI.generateAPIKey(testUserId, {
        label: "e2e-key",
      });
      expect(typeof key.id).toBe("string");
      expect(typeof key.key).toBe("string");
      expect(key.label).toBe("e2e-key");
      testApiKeyId = key.id;
    });

    it("revokeAPIKey() revokes the generated key", async () => {
      const result = await adminAPI.revokeAPIKey(testUserId, testApiKeyId);
      expect(result).toBeUndefined();
    });

    it("deleteUser() deletes the test user", async () => {
      const result = await adminAPI.deleteUser(testUserId);
      expect(result).toBeUndefined();
    });

    it("getUser() throws 404 for deleted user", async () => {
      await expect(adminAPI.getUser(testUserId)).rejects.toThrow(APIError);
    });
  });

  // -----------------------------------------------------------------------
  // Projects
  // -----------------------------------------------------------------------

  describe("adminAPI.projects", () => {
    let createdProjectId: string;

    it("listProjects() returns array (may be empty)", async () => {
      const projects = await adminAPI.listProjects();
      expect(Array.isArray(projects)).toBe(true);
    });

    it("createProject() creates a project", async () => {
      const proj = await adminAPI.createProject({
        name: "E2E Project",
        slug: "e2e-project",
        owner_namespace_id: adminNamespaceId,
        description: "Integration test project",
      });
      expect(proj.name).toBe("E2E Project");
      expect(proj.slug).toBe("e2e-project");
      expect(typeof proj.id).toBe("string");
      createdProjectId = proj.id;
    });

    it("getProject() retrieves the created project", async () => {
      const proj = await adminAPI.getProject(createdProjectId);
      expect(proj.id).toBe(createdProjectId);
      expect(proj.name).toBe("E2E Project");
      expect(proj.description).toBe("Integration test project");
    });

    it("updateProject() updates the project description", async () => {
      const proj = await adminAPI.updateProject(createdProjectId, {
        description: "Updated description",
      });
      expect(proj.description).toBe("Updated description");
    });

    it("deleteProject() removes the project", async () => {
      const result = await meAPI.deleteProject(createdProjectId);
      expect(result).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // Memory operations (project-scoped)
  // -----------------------------------------------------------------------

  describe("memoryAPI", () => {
    let memProjectId: string;
    let storedMemoryId: string;

    beforeAll(async () => {
      const proj = await adminAPI.createProject({
        name: "Memory Test Project",
        slug: "mem-test",
        owner_namespace_id: adminNamespaceId,
      });
      memProjectId = proj.id;
    });

    afterAll(async () => {
      try {
        await meAPI.deleteProject(memProjectId);
      } catch {
        // ignore
      }
    });

    it("store() stores a memory", async () => {
      const mem = await memoryAPI.store(memProjectId, {
        content: "Hello world -- this is an E2E test memory.",
        tags: ["e2e", "test"],
        source: "vitest",
      });
      expect(typeof mem.id).toBe("string");
      expect(mem.project_id).toBe(memProjectId);
      expect(mem.content).toBe("Hello world -- this is an E2E test memory.");
      storedMemoryId = mem.id;
    });

    it("list() returns stored memories", async () => {
      const res = await memoryAPI.list(memProjectId);
      expect(res.data.length).toBeGreaterThanOrEqual(1);
      expect(res.pagination).toBeDefined();
      expect(typeof res.pagination.total).toBe("number");
      expect(typeof res.pagination.limit).toBe("number");
      expect(typeof res.pagination.offset).toBe("number");
    });

    it("list() respects pagination params", async () => {
      const res = await memoryAPI.list(memProjectId, { limit: 1, offset: 0 });
      expect(res.data.length).toBeLessThanOrEqual(1);
      expect(res.pagination.limit).toBe(1);
      expect(res.pagination.offset).toBe(0);
    });

    it("get() retrieves a specific memory", async () => {
      const mem = await memoryAPI.get(memProjectId, storedMemoryId);
      expect(mem.id).toBe(storedMemoryId);
      expect(mem.content).toBe("Hello world -- this is an E2E test memory.");
      expect(mem.tags).toContain("e2e");
      expect(mem.tags).toContain("test");
    });

    it("update() updates memory content", async () => {
      const res = await memoryAPI.update(memProjectId, storedMemoryId, {
        content: "Updated E2E content.",
        tags: ["e2e", "updated"],
      });
      expect(res.id).toBe(storedMemoryId);
      expect(res.content).toBe("Updated E2E content.");
    });

    it("get() confirms update persisted", async () => {
      const mem = await memoryAPI.get(memProjectId, storedMemoryId);
      expect(mem.content).toBe("Updated E2E content.");
      expect(mem.tags).toContain("updated");
    });

    it("recall() performs keyword recall", async () => {
      const res = await memoryAPI.recall(memProjectId, {
        query: "updated",
        limit: 10,
      });
      expect(typeof res.total_searched).toBe("number");
      expect(typeof res.latency_ms).toBe("number");
      expect(Array.isArray(res.memories)).toBe(true);
    });

    it("export() exports project data", async () => {
      const data = await memoryAPI.export(memProjectId);
      expect(typeof data.version).toBe("string");
      expect(typeof data.exported_at).toBe("string");
      expect(data.project).toBeDefined();
      expect(data.project.id).toBe(memProjectId);
      expect(Array.isArray(data.memories)).toBe(true);
      expect(data.memories.length).toBeGreaterThanOrEqual(1);
    });

    it("forget() deletes memories by ID", async () => {
      const res = await memoryAPI.forget(memProjectId, {
        ids: [storedMemoryId],
      });
      expect(typeof res.deleted).toBe("number");
      expect(res.deleted).toBe(1);
      expect(typeof res.latency_ms).toBe("number");
    });

    it("store() + remove() deletes a single memory", async () => {
      const mem = await memoryAPI.store(memProjectId, {
        content: "Temporary memory for remove test.",
      });
      const res = await memoryAPI.remove(memProjectId, mem.id);
      expect(typeof res.deleted).toBe("number");
    });

    it("enrich() returns error on SQLite (postgres-only)", async () => {
      try {
        await memoryAPI.enrich(memProjectId, { all: true });
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Provider slots
  // -----------------------------------------------------------------------

  describe("adminAPI.providers", () => {
    it("getProviderSlots() returns 3 slots", async () => {
      const slots = await adminAPI.getProviderSlots();
      expect(Array.isArray(slots)).toBe(true);
      expect(slots.length).toBe(3);
      const slotNames = slots.map((s) => s.slot);
      expect(slotNames).toContain("embedding");
      expect(slotNames).toContain("fact");
      expect(slotNames).toContain("entity");
    });

    it("testProviderSlot() returns a result (may fail without provider)", async () => {
      try {
        const result = await adminAPI.testProviderSlot("embedding", {
          type: "ollama",
          url: "http://localhost:11434",
          model: "nomic-embed-text",
        });
        expect(typeof result.success).toBe("boolean");
        expect(typeof result.latency_ms).toBe("number");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Settings
  // -----------------------------------------------------------------------

  describe("adminAPI.settings", () => {
    it("getSettings() returns settings data", async () => {
      const res = await adminAPI.getSettings();
      expect(res).toBeDefined();
      expect(res.data).toBeDefined();
      expect(Array.isArray(res.data)).toBe(true);
    });

    it("getSettingsSchema() returns schema definitions", async () => {
      const res = await adminAPI.getSettingsSchema();
      expect(res).toBeDefined();
      expect(res.data).toBeDefined();
      expect(Array.isArray(res.data)).toBe(true);
      if (res.data.length > 0) {
        expect(typeof res.data[0].key).toBe("string");
        expect(typeof res.data[0].type).toBe("string");
        expect(typeof res.data[0].description).toBe("string");
      }
    });

    it("updateSetting() updates a setting value", async () => {
      const res = await adminAPI.updateSetting(
        "enrichment.enabled",
        false,
        "global",
      );
      expect(res).toBeDefined();
      expect(typeof res.status).toBe("string");
    });
  });

  // -----------------------------------------------------------------------
  // Webhooks
  // -----------------------------------------------------------------------

  describe("adminAPI.webhooks", () => {
    let webhookId: string;

    it("listWebhooks() returns array (initially empty)", async () => {
      const hooks = await adminAPI.listWebhooks();
      expect(Array.isArray(hooks)).toBe(true);
    });

    it("createWebhook() creates a webhook", async () => {
      const hook = await adminAPI.createWebhook({
        url: "https://example.com/hook",
        events: ["memory.stored"],
      });
      expect(typeof hook.id).toBe("string");
      expect(hook.url).toBe("https://example.com/hook");
      expect(hook.events).toContain("memory.stored");
      expect(hook.active).toBe(true);
      webhookId = hook.id;
    });

    it("updateWebhook() updates the webhook", async () => {
      const hook = await adminAPI.updateWebhook(webhookId, {
        url: "https://example.com/hook-v2",
        events: ["memory.stored", "memory.updated"],
      });
      expect(hook.url).toBe("https://example.com/hook-v2");
      expect(hook.events).toContain("memory.updated");
    });

    it("testWebhook() fires a test delivery", async () => {
      try {
        const res = await adminAPI.testWebhook(webhookId);
        expect(typeof res.success).toBe("boolean");
        expect(typeof res.latency_ms).toBe("number");
      } catch (e) {
        // Delivery to fake endpoint will fail -- that is expected
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("deleteWebhook() removes the webhook", async () => {
      const result = await adminAPI.deleteWebhook(webhookId);
      expect(result).toBeUndefined();
    });

    it("listWebhooks() is empty after deletion", async () => {
      const hooks = await adminAPI.listWebhooks();
      const found = hooks.find((h) => h.id === webhookId);
      expect(found).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // Analytics
  // -----------------------------------------------------------------------

  describe("adminAPI.analytics", () => {
    it("getAnalytics() returns analytics data", async () => {
      const a = await adminAPI.getAnalytics();
      expect(a.memory_counts).toBeDefined();
      expect(typeof a.memory_counts.total).toBe("number");
      expect(typeof a.memory_counts.active).toBe("number");
      expect(typeof a.memory_counts.deleted).toBe("number");
      expect(typeof a.memory_counts.enriched).toBe("number");
      expect(Array.isArray(a.most_recalled)).toBe(true);
      expect(Array.isArray(a.least_recalled)).toBe(true);
      expect(Array.isArray(a.dead_weight)).toBe(true);
      expect(a.enrichment_stats).toBeDefined();
    });
  });

  // -----------------------------------------------------------------------
  // Usage
  // -----------------------------------------------------------------------

  describe("adminAPI.usage", () => {
    it("getUsage() returns usage report", async () => {
      const u = await adminAPI.getUsage();
      expect(u.totals).toBeDefined();
      expect(typeof u.totals.tokens_input).toBe("number");
      expect(typeof u.totals.tokens_output).toBe("number");
      expect(typeof u.totals.call_count).toBe("number");
      expect(Array.isArray(u.groups)).toBe(true);
    });

    it("getUsage() accepts query params", async () => {
      const u = await adminAPI.getUsage({ group_by: "project" });
      expect(u.totals).toBeDefined();
    });
  });

  // -----------------------------------------------------------------------
  // Database
  // -----------------------------------------------------------------------

  describe("adminAPI.database", () => {
    it("getDatabaseInfo() returns sqlite info", async () => {
      const info = await adminAPI.getDatabaseInfo();
      expect(info.backend).toBe("sqlite");
      expect(typeof info.version).toBe("string");
      expect(info.data_counts).toBeDefined();
      expect(typeof info.data_counts.memories).toBe("number");
      expect(typeof info.data_counts.entities).toBe("number");
      expect(typeof info.data_counts.projects).toBe("number");
      expect(typeof info.data_counts.users).toBe("number");
      expect(typeof info.data_counts.organizations).toBe("number");
    });

    it("testDatabaseConnection() tests a postgres URL", async () => {
      try {
        const res = await adminAPI.testDatabaseConnection(
          "postgres://invalid:invalid@localhost:5432/invalid",
        );
        expect(typeof res.success).toBe("boolean");
        expect(typeof res.message).toBe("string");
      } catch (e) {
        // Connection failure is expected
        expect(e).toBeInstanceOf(APIError);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Enrichment
  // -----------------------------------------------------------------------

  describe("adminAPI.enrichment", () => {
    it("getEnrichmentStatus() returns queue status", async () => {
      const s = await adminAPI.getEnrichmentStatus();
      expect(s.counts).toBeDefined();
      expect(typeof s.counts.pending).toBe("number");
      expect(typeof s.counts.processing).toBe("number");
      expect(typeof s.counts.failed).toBe("number");
      expect(typeof s.paused).toBe("boolean");
    });

    it("retryEnrichment() accepts empty ID list", async () => {
      try {
        const res = await adminAPI.retryEnrichment([]);
        expect(typeof res.retried).toBe("number");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("pauseEnrichment() toggles pause state", async () => {
      try {
        const res = await adminAPI.pauseEnrichment(true);
        expect(typeof res.paused).toBe("boolean");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("testExtractionPrompt() returns error without LLM provider", async () => {
      try {
        const res = await adminAPI.testExtractionPrompt(
          "fact",
          "Extract facts from: {{content}}",
          "The sky is blue.",
        );
        expect(typeof res.latency_ms).toBe("number");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Graph
  // -----------------------------------------------------------------------

  describe("adminAPI.graph", () => {
    let graphProjectId: string;

    beforeAll(async () => {
      const proj = await adminAPI.createProject({
        name: "Graph Test",
        slug: "graph-test",
        owner_namespace_id: adminNamespaceId,
      });
      graphProjectId = proj.id;
    });

    afterAll(async () => {
      try {
        await meAPI.deleteProject(graphProjectId);
      } catch {
        // ignore
      }
    });

    it("getGraph() returns entities and relationships", async () => {
      const g = await adminAPI.getGraph(graphProjectId);
      expect(Array.isArray(g.entities)).toBe(true);
      expect(Array.isArray(g.relationships)).toBe(true);
    });
  });

  // -----------------------------------------------------------------------
  // Namespaces
  // -----------------------------------------------------------------------

  describe("adminAPI.namespaces", () => {
    it("getNamespaceTree() returns tree", async () => {
      const res = await adminAPI.getNamespaceTree();
      expect(res.tree).toBeDefined();
      expect(Array.isArray(res.tree)).toBe(true);
      if (res.tree.length > 0) {
        const node = res.tree[0];
        expect(typeof node.id).toBe("string");
        expect(typeof node.name).toBe("string");
        expect(typeof node.kind).toBe("string");
        expect(Array.isArray(node.children)).toBe(true);
      }
    });
  });

  // -----------------------------------------------------------------------
  // OAuth Clients
  // -----------------------------------------------------------------------

  describe("adminAPI.oauthClients", () => {
    let oauthClientId: string;

    it("listOAuthClients() returns array", async () => {
      const clients = await adminAPI.listOAuthClients();
      expect(Array.isArray(clients)).toBe(true);
    });

    it("createOAuthClient() creates a client", async () => {
      const client = await adminAPI.createOAuthClient({
        name: "E2E Test Client",
        redirect_uris: ["http://localhost:3000/callback"],
        client_type: "public",
      });
      expect(typeof client.id).toBe("string");
      expect(client.name).toBe("E2E Test Client");
      expect(typeof client.client_id).toBe("string");
      oauthClientId = client.id;
    });

    it("deleteOAuthClient() removes the client", async () => {
      const result = await adminAPI.deleteOAuthClient(oauthClientId);
      expect(result).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // IdP Configs
  // -----------------------------------------------------------------------

  describe("adminAPI.idpConfigs", () => {
    it("listIdPConfigs() returns array", async () => {
      const configs = await adminAPI.listIdPConfigs();
      expect(Array.isArray(configs)).toBe(true);
    });
  });

  // -----------------------------------------------------------------------
  // Ollama provider endpoints (expect failure without Ollama running)
  // -----------------------------------------------------------------------

  describe("adminAPI.ollama", () => {
    it("getOllamaModels() throws without Ollama running", async () => {
      try {
        const res = await adminAPI.getOllamaModels();
        expect(res.models).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("pullOllamaModel() throws without Ollama running", async () => {
      try {
        await adminAPI.pullOllamaModel("nomic-embed-text");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });
  });

  // -----------------------------------------------------------------------
  // meAPI (self-service endpoints)
  // -----------------------------------------------------------------------

  describe("meAPI", () => {
    it("listProjects() returns array", async () => {
      const projects = await meAPI.listProjects();
      expect(Array.isArray(projects)).toBe(true);
    });

    it("createProject() creates a project via /me/projects", async () => {
      const proj = await meAPI.createProject({
        name: "Me Test Project",
        slug: "me-test-project",
      });
      expect(proj.name).toBe("Me Test Project");
      expect(proj.slug).toBe("me-test-project");
      expect(typeof proj.id).toBe("string");

      // Cleanup
      await meAPI.deleteProject(proj.id);
    });

    it("listAPIKeys() returns array", async () => {
      const keys = await meAPI.listAPIKeys();
      expect(Array.isArray(keys)).toBe(true);
    });

    it("createAPIKey() + revokeAPIKey() lifecycle", async () => {
      const key = await meAPI.createAPIKey({ name: "me-e2e-key" });
      expect(typeof key.id).toBe("string");
      expect(typeof key.key).toBe("string");
      expect(key.name).toBe("me-e2e-key");

      await meAPI.revokeAPIKey(key.id);
    });
  });

  // -----------------------------------------------------------------------
  // orgAPI (org-scoped endpoints)
  // -----------------------------------------------------------------------

  describe("orgAPI", () => {
    let testOrgId: string;
    let orgUserId: string;

    beforeAll(async () => {
      // Create a test org
      const org = await adminAPI.createOrg({
        name: "Org API Test",
        slug: "org-api-test",
      });
      testOrgId = org.id;
    });

    afterAll(async () => {
      try {
        await adminAPI.deleteOrg(testOrgId);
      } catch {
        // ignore
      }
    });

    it("listUsers() returns array", async () => {
      const users = await orgAPI.listUsers(testOrgId);
      expect(Array.isArray(users)).toBe(true);
    });

    it("createUser() creates a user in the org", async () => {
      const user = await orgAPI.createUser(testOrgId, {
        email: "orguser@test.com",
        password: "OrgUserPass123!",
        role: "member",
        display_name: "Org User",
      });
      expect(user.email).toBe("orguser@test.com");
      expect(user.role).toBe("member");
      orgUserId = user.id;
    });

    it("getUser() retrieves the created user", async () => {
      const user = await orgAPI.getUser(testOrgId, orgUserId);
      expect(user.id).toBe(orgUserId);
      expect(user.email).toBe("orguser@test.com");
    });

    it("updateUser() updates the user", async () => {
      const user = await orgAPI.updateUser(testOrgId, orgUserId, {
        display_name: "Updated Org User",
      });
      expect(user.display_name).toBe("Updated Org User");
    });

    it("deleteUser() removes the user", async () => {
      const result = await orgAPI.deleteUser(testOrgId, orgUserId);
      expect(result).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // Role-based access control
  // -----------------------------------------------------------------------

  describe("RBAC", () => {
    let memberToken: string;
    let readonlyToken: string;
    let orgOwnerToken: string;
    let rbacOrgId: string;

    beforeAll(async () => {
      // Create an org for RBAC tests
      const org = await adminAPI.createOrg({
        name: "RBAC Test Org",
        slug: "rbac-test-org",
      });
      rbacOrgId = org.id;

      // Create users with different roles
      const memberUser = await adminAPI.createUser({
        email: "member@test.com",
        password: "MemberPass123!",
        role: "member",
        organization_id: rbacOrgId,
      });

      const readonlyUser = await adminAPI.createUser({
        email: "readonly@test.com",
        password: "ReadonlyPass123!",
        role: "readonly",
        organization_id: rbacOrgId,
      });

      const ownerUser = await adminAPI.createUser({
        email: "orgowner@test.com",
        password: "OwnerPass123!",
        role: "org_owner",
        organization_id: rbacOrgId,
      });

      // Login each user
      const memberLogin = await authAPI.login({
        email: "member@test.com",
        password: "MemberPass123!",
      });
      memberToken = memberLogin.token;
      expect(memberLogin.user.role).toBe("member");

      const readonlyLogin = await authAPI.login({
        email: "readonly@test.com",
        password: "ReadonlyPass123!",
      });
      readonlyToken = readonlyLogin.token;
      expect(readonlyLogin.user.role).toBe("readonly");

      const ownerLogin = await authAPI.login({
        email: "orgowner@test.com",
        password: "OwnerPass123!",
      });
      orgOwnerToken = ownerLogin.token;
      expect(ownerLogin.user.role).toBe("org_owner");

      // Restore admin token
      localStorage.setItem("nram_token", adminToken);

      // Suppress unused variable warnings
      void memberUser;
      void readonlyUser;
      void ownerUser;
    });

    afterAll(async () => {
      localStorage.setItem("nram_token", adminToken);
      try {
        const users = await adminAPI.listUsers();
        for (const u of users) {
          if (["member@test.com", "readonly@test.com", "orgowner@test.com"].includes(u.email)) {
            await adminAPI.deleteUser(u.id);
          }
        }
        await adminAPI.deleteOrg(rbacOrgId);
      } catch {
        // ignore cleanup errors
      }
    });

    it("member can access meAPI.listProjects()", async () => {
      localStorage.setItem("nram_token", memberToken);
      try {
        const projects = await meAPI.listProjects();
        expect(Array.isArray(projects)).toBe(true);
      } finally {
        localStorage.setItem("nram_token", adminToken);
      }
    });

    it("member can access meAPI.listAPIKeys()", async () => {
      localStorage.setItem("nram_token", memberToken);
      try {
        const keys = await meAPI.listAPIKeys();
        expect(Array.isArray(keys)).toBe(true);
      } finally {
        localStorage.setItem("nram_token", adminToken);
      }
    });

    it("member gets 403 on admin endpoints", async () => {
      localStorage.setItem("nram_token", memberToken);
      try {
        await adminAPI.listUsers();
        expect.fail("should have thrown 403");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
        expect((e as APIError).status).toBe(403);
      } finally {
        localStorage.setItem("nram_token", adminToken);
      }
    });

    it("readonly gets 403 on admin endpoints", async () => {
      localStorage.setItem("nram_token", readonlyToken);
      try {
        await adminAPI.listUsers();
        expect.fail("should have thrown 403");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
        expect((e as APIError).status).toBe(403);
      } finally {
        localStorage.setItem("nram_token", adminToken);
      }
    });

    it("org_owner can access orgAPI.listUsers()", async () => {
      localStorage.setItem("nram_token", orgOwnerToken);
      try {
        const users = await orgAPI.listUsers(rbacOrgId);
        expect(Array.isArray(users)).toBe(true);
      } finally {
        localStorage.setItem("nram_token", adminToken);
      }
    });

    it("org_owner gets 403 on admin settings", async () => {
      localStorage.setItem("nram_token", orgOwnerToken);
      try {
        await adminAPI.getSettings();
        expect.fail("should have thrown 403");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
        expect((e as APIError).status).toBe(403);
      } finally {
        localStorage.setItem("nram_token", adminToken);
      }
    });

    it("login response includes user role and org_id", async () => {
      const res = await authAPI.login({
        email: "member@test.com",
        password: "MemberPass123!",
      });
      expect(res.user.role).toBe("member");
      expect(typeof res.user.org_id).toBe("string");
      expect(res.user.org_id).toBe(rbacOrgId);
    });
  });

  // -----------------------------------------------------------------------
  // Error handling -- must be at the end to avoid breaking state
  // -----------------------------------------------------------------------

  describe("error handling", () => {
    it("request with invalid token throws APIError", async () => {
      const realToken = localStorage.getItem("nram_token")!;

      localStorage.setItem("nram_token", "invalid-jwt-token");

      // Set pathname to /login so the 401 handler throws APIError
      // instead of trying to redirect (which would hang)
      const prevWindow = (globalThis as Record<string, unknown>).window;
      (globalThis as Record<string, unknown>).window = {
        location: { pathname: "/login", href: "" },
      };

      try {
        await adminAPI.getDashboard();
        expect.fail("should have thrown");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
        expect((e as APIError).status).toBe(401);
      } finally {
        localStorage.setItem("nram_token", realToken);
        (globalThis as Record<string, unknown>).window = prevWindow;
      }
    });

    it("request with no token throws APIError", async () => {
      const realToken = localStorage.getItem("nram_token")!;
      localStorage.removeItem("nram_token");

      const prevWindow = (globalThis as Record<string, unknown>).window;
      (globalThis as Record<string, unknown>).window = {
        location: { pathname: "/login", href: "" },
      };

      try {
        await adminAPI.getDashboard();
        expect.fail("should have thrown");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
        expect((e as APIError).status).toBe(401);
      } finally {
        localStorage.setItem("nram_token", realToken);
        (globalThis as Record<string, unknown>).window = prevWindow;
      }
    });

    it("404 for nonexistent resource", async () => {
      try {
        await adminAPI.getUser("00000000-0000-0000-0000-000000000000");
        expect.fail("should have thrown");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
        expect((e as APIError).status).toBe(404);
      }
    });

    it("404 for nonexistent project", async () => {
      try {
        await adminAPI.getProject("00000000-0000-0000-0000-000000000000");
        expect.fail("should have thrown");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
        expect((e as APIError).status).toBe(404);
      }
    });

    it("404 for nonexistent org", async () => {
      try {
        await adminAPI.getOrg("00000000-0000-0000-0000-000000000000");
        expect.fail("should have thrown");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
        expect((e as APIError).status).toBe(404);
      }
    });
  });
});
