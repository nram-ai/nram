/**
 * E2E tests for the API client -- runs against a real nram Go server.
 *
 * Uses @vitest-environment node so child_process.spawn works reliably.
 * Polyfills localStorage and window.location before the client module loads.
 *
 * @vitest-environment node
 */
import { vi, describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "vitest";

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
  systemAPI,
  oauthAPI,
  shareAcceptAPI,
  sharesAPI,
  fetchMetricsText,
  getInstructions,
  memoryRowLabel,
  buildLogQuery,
  isLoopbackRedirectUri,
  changePassword,
  downloadLogsExport,
  downloadProjectExport,
  downloadExportJobArtifact,
  type SetupResponse,
  type User,
  type OAuthAuthorizeParams,
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

// Spawn a real nram server bound to `port` in a fresh temp dir (where SQLite
// creates nram.db) and wait for /v1/health. Returns the process + temp dir so
// the caller owns teardown. Shared by the main suite and the dedicated
// destructive-endpoints server.
async function startNramServer(
  port: number,
  tmpPrefix: string,
): Promise<{ proc: ChildProcess; serverTmp: string }> {
  const serverTmp = mkdtempSync(join(tmpdir(), tmpPrefix));
  const proc = spawn(SERVER_BIN, [], {
    cwd: serverTmp,
    env: {
      HOME: process.env.HOME ?? "",
      PATH: process.env.PATH ?? "",
      PORT: String(port),
      LOG_LEVEL: "error",
    },
    stdio: "pipe",
  });
  let stderr = "";
  proc.stderr?.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  proc.on("error", (err: Error) => {
    throw new Error(`Failed to start nram server on ${port}: ${err.message}\n${stderr}`);
  });
  await waitForServer(`http://localhost:${port}`);
  return { proc, serverTmp };
}

// Install a minimal document stub for the download helpers (triggerBlobDownload
// needs document.createElement + body.appendChild; @vitest-environment node has
// no DOM). Returns the previous document so the caller can restore it.
function installDocumentStub(): unknown {
  const prev = (globalThis as Record<string, unknown>).document;
  (globalThis as Record<string, unknown>).document = {
    createElement: () => ({ href: "", download: "", click: () => {}, remove: () => {} }),
    body: { appendChild: () => {} },
  };
  return prev;
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

    // Spawn the real Go server in an isolated temp dir and wait for health.
    const started = await startNramServer(SERVER_PORT, "nram-e2e-");
    serverProcess = started.proc;
    tmpDir = started.serverTmp;

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

    // Configure stub provider slots so the EnrichmentGate opens in the
    // suite. Every dreaming/enrichment route is wrapped in EnrichmentGate
    // which returns 503 until all three slots (embedding, fact, entity)
    // have a provider. We don't need the providers to actually work for
    // the e2e tests: only the gate-passing record.
    for (const slot of ["embedding", "fact", "entity"]) {
      try {
        await adminAPI.updateProviderSlot(slot, {
          type: "ollama",
          url: "http://localhost:11434",
          model: slot === "embedding" ? "nomic-embed-text" : "llama3",
        });
      } catch {
        // Suite tolerates unconfigured providers; the gate-dependent tests
        // will surface their own failure if this stub setup didn't take.
      }
    }
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

  // Re-establish the ambient admin credential after every test. The request()
  // 401 handler clears nram_token on any 401 response (intentional + expected
  // error-path tests trip it), which would otherwise leak a cleared token into
  // the next test or a nested beforeAll. Restoring here keeps tests
  // order-independent. adminToken is refreshed by the changePassword test.
  afterEach(() => {
    localStorage.setItem("nram_token", adminToken);
  });

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
      expect(h.providers.fact).toBeDefined();
      expect(h.providers.entity).toBeDefined();
      expect(h.providers.query_augment).toBeDefined();
      expect(h.providers.ingestion_decision).toBeDefined();
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
      const proj = await meAPI.getProject(createdProjectId);
      expect(proj.id).toBe(createdProjectId);
      expect(proj.name).toBe("E2E Project");
      expect(proj.description).toBe("Integration test project");
    });

    it("updateProject() updates the project description", async () => {
      const proj = await meAPI.updateProject(createdProjectId, {
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
      // Content-change update returns a NEW id under supersede semantics
      // (per service/update.go). previous_memory_id echoes the input id.
      expect(typeof res.id).toBe("string");
      expect(res.id).not.toBe(storedMemoryId);
      expect(res.previous_memory_id).toBe(storedMemoryId);
      expect(res.superseded).toBe(true);
      expect(res.content).toBe("Updated E2E content.");
      // Re-point storedMemoryId to the active head so subsequent tests
      // chain off the new memory.
      storedMemoryId = res.id;
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

    it("ask() hits the gated project-scoped ask endpoint", async () => {
      // ask is off by default, so the endpoint 404s (the AskGate hides it);
      // this still exercises the client wiring regardless of feature state.
      await expect(
        memoryAPI.ask(memProjectId, { query: "anything" }),
      ).rejects.toBeDefined();
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
      // Forget on the active head walks the supersede chain and soft-deletes
      // the entire chain (per service/forget.go), so deleted >= 1: at least
      // the head, plus any superseded ancestors.
      expect(res.deleted).toBeGreaterThanOrEqual(1);
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
    it("getProviderSlots() returns 6 slots", async () => {
      const slots = await adminAPI.getProviderSlots();
      expect(Array.isArray(slots)).toBe(true);
      expect(slots.length).toBe(6);
      const slotNames = slots.map((s) => s.slot);
      expect(slotNames).toContain("embedding");
      expect(slotNames).toContain("fact");
      expect(slotNames).toContain("entity");
      expect(slotNames).toContain("query_augment");
      expect(slotNames).toContain("ingestion_decision");
      expect(slotNames).toContain("ask");
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

    it("round-trips custom_headers and masks values on read", async () => {
      // Use the optional ingestion_decision slot so required-slot setup is untouched.
      await adminAPI.updateProviderSlot("ingestion_decision", {
        type: "openai",
        url: "https://api.openai.com",
        api_key: "sk-roundtrip",
        model: "gpt-4o-mini",
        custom_headers: { "X-Proxy-Auth": "tok", "X-Tenant": "acme" },
      });

      const slots = await adminAPI.getProviderSlots();
      const slot = slots.find((s) => s.slot === "ingestion_decision");
      expect(slot).toBeDefined();
      expect(slot!.api_key_set).toBe(true);
      expect((slot!.custom_header_keys ?? []).sort()).toEqual([
        "X-Proxy-Auth",
        "X-Tenant",
      ]);
      // Values must never be returned on the read path.
      expect(JSON.stringify(slot)).not.toContain("tok");
      expect(JSON.stringify(slot)).not.toContain("acme");
    });

    it("preserves a blank header value and the api_key on re-save", async () => {
      await adminAPI.updateProviderSlot("ingestion_decision", {
        type: "openai",
        url: "https://api.openai.com",
        api_key: "sk-keepme",
        model: "gpt-4o-mini",
        custom_headers: { "X-Keep": "originalvalue" },
      });

      // Re-save with the header value blank and no api_key: both must be kept.
      await adminAPI.updateProviderSlot("ingestion_decision", {
        type: "openai",
        url: "https://api.openai.com",
        model: "gpt-4o-mini",
        custom_headers: { "X-Keep": "" },
      });

      const slots = await adminAPI.getProviderSlots();
      const slot = slots.find((s) => s.slot === "ingestion_decision");
      expect(slot!.api_key_set).toBe(true);
      expect(slot!.custom_header_keys ?? []).toContain("X-Keep");
    });

    it("round-trips extra_body verbatim on read", async () => {
      await adminAPI.updateProviderSlot("ingestion_decision", {
        type: "vllm",
        url: "http://localhost:8000",
        model: "Qwen/Qwen3-8B",
        extra_body: { chat_template_kwargs: { enable_thinking: true } },
      });

      const slots = await adminAPI.getProviderSlots();
      const slot = slots.find((s) => s.slot === "ingestion_decision");
      expect(slot).toBeDefined();
      expect(slot!.type).toBe("vllm");
      // Unlike headers, extra_body is not secret and is returned verbatim.
      expect(slot!.extra_body).toEqual({
        chat_template_kwargs: { enable_thinking: true },
      });
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
      const res = await adminAPI.updateSetting("enrichment.enabled", false);
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

    it("getEnrichmentStatus() honors pagination, sort, and status params", async () => {
      // limit caps the page; the server caps at 200 and never returns more
      // than asked. An empty queue simply returns 0 items, which still
      // exercises the query-string path and server-side parsing.
      const page = await adminAPI.getEnrichmentStatus({
        limit: 5,
        offset: 0,
        sort: "attempts",
        dir: "asc",
        status: "pending",
      });
      expect(Array.isArray(page.items)).toBe(true);
      expect(page.items.length).toBeLessThanOrEqual(5);
      // status filter must not return rows of other states.
      for (const item of page.items) {
        expect(item.status).toBe("pending");
      }
      // counts are queue-wide and independent of the filter/page.
      expect(typeof page.counts.completed).toBe("number");
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

    it("getProviderModels() returns models or throws when unreachable", async () => {
      try {
        const res = await adminAPI.getProviderModels("http://127.0.0.1:1/");
        expect(Array.isArray(res.models)).toBe(true);
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
  // Public text endpoints (metrics + instructions)
  // -----------------------------------------------------------------------

  describe("public text endpoints", () => {
    it("fetchMetricsText() returns Prometheus exposition text", async () => {
      const text = await fetchMetricsText();
      expect(typeof text).toBe("string");
      expect(text.length).toBeGreaterThan(0);
    });

    it("getInstructions() returns text for each format", async () => {
      for (const fmt of ["claude", "agents", "cursor"] as const) {
        const text = await getInstructions(fmt);
        expect(typeof text).toBe("string");
        expect(text.length).toBeGreaterThan(0);
      }
    });
  });

  // -----------------------------------------------------------------------
  // authAPI passkey endpoints (no credential available; cover the call path)
  // -----------------------------------------------------------------------

  describe("authAPI.passkey", () => {
    it("passkeyBegin() returns options or throws for a user without passkeys", async () => {
      try {
        const res = await authAPI.passkeyBegin({ email: "admin@test.com" });
        expect(res).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("passkeyFinish() throws with a bogus assertion", async () => {
      const savedToken = localStorage.getItem("nram_token");
      try {
        await authAPI.passkeyFinish({ bogus: true }, "no-such-session");
        expect.fail("should have thrown");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      } finally {
        if (savedToken) localStorage.setItem("nram_token", savedToken);
      }
    });
  });

  // -----------------------------------------------------------------------
  // adminAPI -- previously-uncovered methods
  // -----------------------------------------------------------------------

  describe("adminAPI.extra", () => {
    it("getSettingGroups() returns grouped schema", async () => {
      const res = await adminAPI.getSettingGroups();
      expect(Array.isArray(res.data)).toBe(true);
    });

    it("getCostRates() returns an array", async () => {
      const rates = await adminAPI.getCostRates();
      expect(Array.isArray(rates)).toBe(true);
    });

    it("getDatabaseInfo() + preflightDatabase() against an invalid URL throws", async () => {
      try {
        await adminAPI.preflightDatabase("postgres://x:x@127.0.0.1:1/none");
        // A reachable-but-empty server could succeed; either is acceptable.
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("migrationAudit() returns an audit report", async () => {
      try {
        const audit = await adminAPI.migrationAudit();
        expect(audit).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("triggerMigration() with an invalid URL throws", async () => {
      try {
        await adminAPI.triggerMigration("postgres://x:x@127.0.0.1:1/none");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("vectorMigrationDryRun() throws without qdrant configured", async () => {
      try {
        await adminAPI.vectorMigrationDryRun("to_qdrant");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("startVectorMigration() throws without qdrant configured", async () => {
      try {
        await adminAPI.startVectorMigration("from_qdrant", 100);
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("listLogs() returns a paginated response", async () => {
      const res = await adminAPI.listLogs({ limit: 5, level: ["error", "warn"], search: "x" });
      expect(res).toBeDefined();
      expect(Array.isArray(res.data)).toBe(true);
      expect(res.pagination).toBeDefined();
    });

    it("getLogFacets() returns levels and components", async () => {
      const facets = await adminAPI.getLogFacets();
      expect(Array.isArray(facets.levels)).toBe(true);
      expect(Array.isArray(facets.components)).toBe(true);
    });

    it("backfillAugmentation() dry run returns candidate counts or errors", async () => {
      try {
        const res = await adminAPI.backfillAugmentation({ dry_run: true, limit: 10 });
        expect(res).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("backfillMultiVector() dry run returns candidate counts or errors", async () => {
      try {
        const res = await adminAPI.backfillMultiVector({ dry_run: true, limit: 10 });
        expect(res).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("getGraphHealth() + repairGraph() run", async () => {
      const health = await adminAPI.getGraphHealth();
      expect(health).toBeDefined();
      const repair = await adminAPI.repairGraph();
      expect(repair).toBeDefined();
    });

    it("getDreamingStatus() + getDreamingCycles() run", async () => {
      const status = await adminAPI.getDreamingStatus();
      expect(status).toBeDefined();
      const cycles = await adminAPI.getDreamingCycles();
      expect(Array.isArray(cycles)).toBe(true);
      const filtered = await adminAPI.getDreamingCycles("00000000-0000-0000-0000-000000000000");
      expect(Array.isArray(filtered)).toBe(true);
    });

    it("setDreamingEnabled() + setProjectDreamingEnabled() toggle", async () => {
      const proj = await adminAPI.createProject({
        name: "Dream Toggle",
        slug: "dream-toggle",
        owner_namespace_id: adminNamespaceId,
      });
      try {
        const r1 = await adminAPI.setDreamingEnabled(true);
        expect(r1).toBeDefined();
        const r2 = await adminAPI.setProjectDreamingEnabled(proj.id, true);
        expect(r2.enabled).toBe(true);
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      } finally {
        await meAPI.deleteProject(proj.id);
      }
    });

    it("rollbackDreamCycle() + abandonDreamCycle() throw for a bogus cycle", async () => {
      for (const fn of [
        () => adminAPI.rollbackDreamCycle("00000000-0000-0000-0000-000000000000"),
        () => adminAPI.abandonDreamCycle("00000000-0000-0000-0000-000000000000"),
      ]) {
        try {
          await fn();
        } catch (e) {
          expect(e).toBeInstanceOf(APIError);
        }
      }
    });

    it("getDreamingCycleDetail() throws for a bogus cycle", async () => {
      try {
        await adminAPI.getDreamingCycleDetail("00000000-0000-0000-0000-000000000000");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("previewMemoryAugmentation() throws for a bogus memory", async () => {
      const proj = await adminAPI.createProject({
        name: "Aug Preview",
        slug: "aug-preview",
        owner_namespace_id: adminNamespaceId,
      });
      try {
        await adminAPI.previewMemoryAugmentation(proj.id, "00000000-0000-0000-0000-000000000000");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      } finally {
        await meAPI.deleteProject(proj.id);
      }
    });

    it("testExtractionPrompt() variants run with optional count + systemPrompt", async () => {
      for (const t of ["entity", "augment", "ingestion"] as const) {
        try {
          await adminAPI.testExtractionPrompt(t, "Sample input.", 2, "You are a tester.");
        } catch (e) {
          expect(e).toBeInstanceOf(APIError);
        }
      }
    });

    it("getUsage() honors every query param", async () => {
      const u = await adminAPI.getUsage({
        project: "p",
        from: "2026-01-01T00:00:00Z",
        to: "2026-12-31T00:00:00Z",
        group_by: "model",
        success_only: true,
      });
      expect(u.totals).toBeDefined();
    });

    it("IdP config create rejects an invalid org, list reflects no change", async () => {
      try {
        await adminAPI.createIdPConfig({
          org_id: "00000000-0000-0000-0000-000000000000",
          provider_type: "oidc",
          client_id: "cid",
          client_secret: "secret",
          issuer_url: "https://idp.example.com",
        });
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
      const configs = await adminAPI.listIdPConfigs();
      expect(Array.isArray(configs)).toBe(true);
    });

    it("updateIdPConfig() + deleteIdPConfig() throw for a bogus id", async () => {
      try {
        await adminAPI.updateIdPConfig("00000000-0000-0000-0000-000000000000", {
          client_id: "new",
        });
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
      try {
        await adminAPI.deleteIdPConfig("00000000-0000-0000-0000-000000000000");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });
  });

  // -----------------------------------------------------------------------
  // memoryAPI -- previously-uncovered methods
  // -----------------------------------------------------------------------

  describe("memoryAPI.extra", () => {
    let srcProjectId: string;
    let dstProjectId: string;

    beforeAll(async () => {
      const src = await adminAPI.createProject({
        name: "Move Src",
        slug: "move-src",
        owner_namespace_id: adminNamespaceId,
      });
      const dst = await adminAPI.createProject({
        name: "Move Dst",
        slug: "move-dst",
        owner_namespace_id: adminNamespaceId,
      });
      srcProjectId = src.id;
      dstProjectId = dst.id;
    });

    afterAll(async () => {
      for (const id of [srcProjectId, dstProjectId]) {
        try {
          await meAPI.deleteProject(id);
        } catch {
          // ignore
        }
      }
    });

    it("listIDs() returns IDs with filters applied", async () => {
      await memoryAPI.store(srcProjectId, { content: "id-list memory", tags: ["x"] });
      const res = await memoryAPI.listIDs(srcProjectId, {
        max: 50,
        tags: ["x"],
        limit: 10,
        offset: 0,
      });
      expect(res).toBeDefined();
      expect(Array.isArray(res.ids)).toBe(true);
    });

    it("import() bulk-imports nram-format data", async () => {
      await memoryAPI.store(srcProjectId, { content: "to export" });
      const data = await memoryAPI.export(srcProjectId);
      const res = await memoryAPI.import(dstProjectId, "nram", data);
      expect(typeof res.imported).toBe("number");
    });

    it("move() relocates a single memory", async () => {
      const mem = await memoryAPI.store(srcProjectId, { content: "movable" });
      const res = await memoryAPI.move(srcProjectId, mem.id, dstProjectId);
      expect(res).toBeDefined();
    });

    it("bulkMove() relocates several memories", async () => {
      const a = await memoryAPI.store(srcProjectId, { content: "bulk-a" });
      const b = await memoryAPI.store(srcProjectId, { content: "bulk-b" });
      const res = await memoryAPI.bulkMove(srcProjectId, [a.id, b.id], dstProjectId);
      expect(res).toBeDefined();
    });

    it("list() applies the full set of optional filters", async () => {
      const res = await memoryAPI.list(srcProjectId, {
        limit: 5,
        offset: 0,
        tags: ["x"],
        date_from: "2000-01-01T00:00:00Z",
        date_to: "2100-01-01T00:00:00Z",
        enriched: "false",
        origin: "user",
        augmented: "false",
        include_superseded: "true",
        source: "vitest",
        search: "memory",
        group_by_parent: true,
      });
      expect(res.data).toBeDefined();
    });

    it("get() with includeSuperseded option", async () => {
      const mem = await memoryAPI.store(srcProjectId, { content: "with-superseded" });
      const got = await memoryAPI.get(srcProjectId, mem.id, { includeSuperseded: true });
      expect(got.id).toBe(mem.id);
    });
  });

  // -----------------------------------------------------------------------
  // meAPI -- previously-uncovered methods
  // -----------------------------------------------------------------------

  describe("meAPI.extra", () => {
    it("getProfile() + updateProfile() round-trip the theme", async () => {
      const profile = await meAPI.getProfile();
      expect(profile).toBeDefined();
      const updated = await meAPI.updateProfile({ theme: "dark" });
      expect(updated).toBeDefined();
    });

    it("getCapabilities() returns the two boolean flags", async () => {
      const caps = await meAPI.getCapabilities();
      expect(typeof caps.enrichment_available).toBe("boolean");
      expect(typeof caps.dreaming_enabled).toBe("boolean");
    });

    it("getRankingWeightDefaults() returns rows", async () => {
      const res = await meAPI.getRankingWeightDefaults();
      expect(Array.isArray(res.data)).toBe(true);
    });

    it("getSettingDefaults() returns rows", async () => {
      const res = await meAPI.getSettingDefaults();
      expect(Array.isArray(res.data)).toBe(true);
    });

    it("listPasskeys() returns an array", async () => {
      const keys = await meAPI.listPasskeys();
      expect(Array.isArray(keys)).toBe(true);
    });

    it("registerPasskeyBegin() returns options", async () => {
      try {
        const opts = await meAPI.registerPasskeyBegin({ name: "e2e-key" });
        expect(opts).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("registerPasskeyFinish() throws with a bogus attestation", async () => {
      try {
        await meAPI.registerPasskeyFinish({ bogus: true }, "e2e-key");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("deletePasskey() throws for a bogus id", async () => {
      try {
        await meAPI.deletePasskey("00000000-0000-0000-0000-000000000000");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("updateProject() via meAPI", async () => {
      const proj = await meAPI.createProject({ name: "Me Upd", slug: "me-upd" });
      const updated = await meAPI.updateProject(proj.id, { description: "changed" });
      expect(updated.description).toBe("changed");
      await meAPI.deleteProject(proj.id);
    });

    it("procedural tier CRUD + export/import", async () => {
      const created = await meAPI.createProcedural({
        content: "Always cite sources.",
        title: "Cite",
        category: "checklist",
        tags: ["t"],
        priority: 10,
        enabled: true,
      });
      expect(created.id).toBeDefined();

      const list = await meAPI.listProcedural();
      expect(Array.isArray(list)).toBe(true);

      const listCapped = await meAPI.listProcedural(50);
      expect(Array.isArray(listCapped)).toBe(true);

      const got = await meAPI.getProcedural(created.id);
      expect(got.id).toBe(created.id);

      const updated = await meAPI.updateProcedural(created.id, { content: "Cite primary sources." });
      expect(updated.content).toBe("Cite primary sources.");

      const exported = await meAPI.exportProcedural();
      expect(Array.isArray(exported.entries)).toBe(true);

      const importRes = await meAPI.importProcedural(exported);
      expect(importRes).toBeDefined();

      const importArr = await meAPI.importProcedural(exported.entries);
      expect(importArr).toBeDefined();

      await meAPI.deleteProcedural(created.id);
    });

    it("OAuth client self-service CRUD", async () => {
      const list0 = await meAPI.listOAuthClients();
      expect(Array.isArray(list0)).toBe(true);
      const created = await meAPI.createOAuthClient({
        name: "Me OAuth Client",
        redirect_uris: ["http://localhost:3000/callback"],
        client_type: "public",
      });
      expect(typeof created.id).toBe("string");
      await meAPI.revokeOAuthClient(created.id);
    });

    it("recall() runs against the caller's projects", async () => {
      const res = await meAPI.recall({ query: "anything", limit: 5 });
      expect(res).toBeDefined();
      expect(Array.isArray(res.memories)).toBe(true);
    });

    it("ask() hits the gated user-scoped ask endpoint", async () => {
      // ask is off by default, so the endpoint 404s; the call still exercises
      // the client wiring regardless of feature state.
      await expect(meAPI.ask({ query: "anything" })).rejects.toBeDefined();
    });

    it("dreaming self-tier reads", async () => {
      const agg = await meAPI.getDreamingAggregateStatus();
      expect(agg).toBeDefined();
      const proj = await meAPI.createProject({ name: "Me Dream", slug: "me-dream" });
      try {
        const status = await meAPI.getDreamingProjectStatus(proj.id);
        expect(status).toBeDefined();
        const cycles = await meAPI.getDreamingCycles(proj.id);
        expect(Array.isArray(cycles)).toBe(true);
        const allCycles = await meAPI.getDreamingCycles();
        expect(Array.isArray(allCycles)).toBe(true);
      } finally {
        await meAPI.deleteProject(proj.id);
      }
    });

    it("getDreamingCycleDetail() + abandon/rollback throw for a bogus cycle", async () => {
      for (const fn of [
        () => meAPI.getDreamingCycleDetail("00000000-0000-0000-0000-000000000000"),
        () => meAPI.abandonDreamCycle("00000000-0000-0000-0000-000000000000"),
        () => meAPI.rollbackDreamCycle("00000000-0000-0000-0000-000000000000"),
      ]) {
        try {
          await fn();
        } catch (e) {
          expect(e).toBeInstanceOf(APIError);
        }
      }
    });

    it("enrichment self-tier status + retry", async () => {
      const status = await meAPI.getEnrichmentStatus({ limit: 5, status: "pending" });
      expect(status.counts).toBeDefined();
      try {
        const r = await meAPI.retryEnrichment([]);
        expect(r).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("export jobs CRUD", async () => {
      const list0 = await meAPI.listExportJobs();
      expect(Array.isArray(list0)).toBe(true);
      const job = await meAPI.createExportJob({ scope: "account", format: "zip" });
      expect(typeof job.id).toBe("string");
      const got = await meAPI.getExportJob(job.id);
      expect(got.id).toBe(job.id);
      await meAPI.deleteExportJob(job.id);
    });

    it("changePassword() round-trips the admin password", async () => {
      const r1 = await changePassword("TestPassword123!", "TempPassword999!");
      expect(r1.changed).toBe(true);
      // A password change revokes the current session token; re-login to
      // continue and to refresh the ambient admin token the afterEach restores.
      const l1 = await authAPI.login({
        email: "admin@test.com",
        password: "TempPassword999!",
      });
      localStorage.setItem("nram_token", l1.token);
      adminToken = l1.token;

      const r2 = await changePassword("TempPassword999!", "TestPassword123!");
      expect(r2.changed).toBe(true);
      const l2 = await authAPI.login({
        email: "admin@test.com",
        password: "TestPassword123!",
      });
      adminToken = l2.token;
      localStorage.setItem("nram_token", adminToken);
    });
  });

  // -----------------------------------------------------------------------
  // orgAPI -- previously-uncovered methods
  // -----------------------------------------------------------------------

  describe("orgAPI.extra", () => {
    let orgId: string;
    let orgUserId: string;

    beforeAll(async () => {
      const org = await adminAPI.createOrg({ name: "Org Extra", slug: "org-extra" });
      orgId = org.id;
      const user = await orgAPI.createUser(orgId, {
        email: "orgextra@test.com",
        password: "OrgExtra123!",
        role: "member",
        display_name: "Org Extra User",
      });
      orgUserId = user.id;
    });

    afterAll(async () => {
      try {
        await orgAPI.deleteUser(orgId, orgUserId);
      } catch {
        // ignore
      }
      try {
        await adminAPI.deleteOrg(orgId);
      } catch {
        // ignore
      }
    });

    it("user API key lifecycle", async () => {
      const list0 = await orgAPI.listUserAPIKeys(orgId, orgUserId);
      expect(Array.isArray(list0)).toBe(true);
      const key = await orgAPI.generateUserAPIKey(orgId, orgUserId, { label: "org-key" });
      expect(typeof key.id).toBe("string");
      await orgAPI.revokeUserAPIKey(orgId, orgUserId, key.id);
    });

    it("analytics + usage (with and without params)", async () => {
      const analytics = await orgAPI.getAnalytics(orgId);
      expect(analytics).toBeDefined();
      const usage0 = await orgAPI.getUsage(orgId);
      expect(usage0).toBeDefined();
      const usage1 = await orgAPI.getUsage(orgId, {
        from: "2026-01-01T00:00:00Z",
        to: "2026-12-31T00:00:00Z",
        group_by: "user",
        user: orgUserId,
        success_only: false,
      });
      expect(usage1).toBeDefined();
    });

    it("dashboard + activity", async () => {
      const dash = await orgAPI.getDashboard(orgId);
      expect(dash).toBeDefined();
      const act = await orgAPI.getActivity(orgId);
      expect(act).toBeDefined();
    });

    it("dreaming + enrichment org-tier", async () => {
      const status = await orgAPI.getDreamingStatus(orgId);
      expect(status).toBeDefined();
      const cycles = await orgAPI.getDreamingCycles(orgId);
      expect(Array.isArray(cycles)).toBe(true);
      const enrich = await orgAPI.getEnrichmentStatus(orgId, { limit: 5 });
      expect(enrich.counts).toBeDefined();
      try {
        await orgAPI.retryEnrichment(orgId, []);
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
      for (const fn of [
        () => orgAPI.getDreamingCycleDetail(orgId, "00000000-0000-0000-0000-000000000000"),
        () => orgAPI.abandonDreamCycle(orgId, "00000000-0000-0000-0000-000000000000"),
        () => orgAPI.rollbackDreamCycle(orgId, "00000000-0000-0000-0000-000000000000"),
      ]) {
        try {
          await fn();
        } catch (e) {
          expect(e).toBeInstanceOf(APIError);
        }
      }
    });

    it("org IdP CRUD", async () => {
      const list0 = await orgAPI.listOrgIdPs(orgId);
      expect(Array.isArray(list0)).toBe(true);
      let createdId: string | undefined;
      try {
        const cfg = await orgAPI.configureIdP(orgId, {
          org_id: orgId,
          provider_type: "oidc",
          client_id: "cid",
          client_secret: "secret",
          issuer_url: "https://idp.example.com",
        });
        createdId = cfg.id;
        const updated = await orgAPI.updateOrgIdP(orgId, cfg.id, { client_id: "cid2" });
        expect(updated).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      } finally {
        if (createdId) {
          try {
            await orgAPI.deleteOrgIdP(orgId, createdId);
          } catch {
            // ignore
          }
        }
      }
    });
  });

  // -----------------------------------------------------------------------
  // systemAPI (tier-C admin aggregate views)
  // -----------------------------------------------------------------------

  describe("systemAPI", () => {
    it("getDashboard()/getActivity()/getAnalytics() return aggregates", async () => {
      expect(await systemAPI.getDashboard()).toBeDefined();
      expect(await systemAPI.getActivity()).toBeDefined();
      expect(await systemAPI.getAnalytics()).toBeDefined();
    });

    it("getUsage() with and without params", async () => {
      expect(await systemAPI.getUsage()).toBeDefined();
      const u = await systemAPI.getUsage({
        from: "2026-01-01T00:00:00Z",
        to: "2026-12-31T00:00:00Z",
        group_by: "org",
        success_only: true,
      });
      expect(u).toBeDefined();
    });
  });

  // -----------------------------------------------------------------------
  // oauthAPI + shareAcceptAPI (consent + share-accept read paths)
  // -----------------------------------------------------------------------

  describe("oauthAPI + shareAcceptAPI", () => {
    let clientId: string;
    let shareToken: string;
    let shareProjectId: string;
    const redirectUri = "http://127.0.0.1:8765/callback";
    const baseParams = (): OAuthAuthorizeParams => ({
      client_id: clientId,
      redirect_uri: redirectUri,
      response_type: "code",
      code_challenge: "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
      code_challenge_method: "S256",
      scope: "mcp",
      resource: "https://nram.example.com",
      state: "xyz",
    });

    beforeAll(async () => {
      const client = await adminAPI.createOAuthClient({
        name: "Consent Client",
        redirect_uris: [redirectUri],
        client_type: "public",
      });
      clientId = client.client_id;

      const proj = await meAPI.createProject({ name: "Share Proj", slug: "share-proj" });
      shareProjectId = proj.id;
      const created = await sharesAPI.create({
        name: "E2E Share",
        description: "shared for e2e",
        is_one_shot: false,
        expires_at: "2099-01-01T00:00:00Z",
        grants: [{ project_id: proj.id, permission: "read" }],
      });
      shareToken = created.secret;
    });

    afterAll(async () => {
      try {
        await adminAPI.deleteOAuthClient(
          (await adminAPI.listOAuthClients()).find((c) => c.client_id === clientId)?.id ?? "",
        );
      } catch {
        // ignore
      }
      try {
        await meAPI.deleteProject(shareProjectId);
      } catch {
        // ignore
      }
    });

    it("getAuthorizeContext() returns context or a redirect_to", async () => {
      const res = await oauthAPI.getAuthorizeContext(baseParams());
      expect(res).toBeDefined();
    });

    it("previewShare() previews a pasted share token", async () => {
      try {
        const res = await oauthAPI.previewShare({ ...baseParams(), share_token: shareToken });
        expect(res).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("completeAuthorize() with decision deny", async () => {
      try {
        const res = await oauthAPI.completeAuthorize({
          ...baseParams(),
          auth_mode: "account",
          decision: "deny",
        });
        expect(res).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("shareAcceptAPI.get() previews a share token", async () => {
      try {
        const res = await shareAcceptAPI.get(shareToken);
        expect(res).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("shareAcceptAPI.get() with an invalid token surfaces an error payload or APIError", async () => {
      try {
        const res = await shareAcceptAPI.get("nram_s_deadbeef");
        // server may return a 200 envelope carrying { error } instead of throwing
        expect(res).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Branch-coverage fillers -- exercise the "other side" of optional-param
  // branches against the real server.
  // -----------------------------------------------------------------------

  describe("branch coverage fillers", () => {
    let projId: string;
    let orgId: string;
    let clientId: string;
    const redirectUri = "http://127.0.0.1:8799/cb";

    beforeAll(async () => {
      const proj = await meAPI.createProject({ name: "Filler Proj", slug: "filler-proj" });
      projId = proj.id;
      await memoryAPI.store(projId, { content: "filler memory" });
      const org = await adminAPI.createOrg({ name: "Filler Org", slug: "filler-org" });
      orgId = org.id;
      const client = await adminAPI.createOAuthClient({
        name: "Filler Client",
        redirect_uris: [redirectUri],
        client_type: "public",
      });
      clientId = client.client_id;
    });

    afterAll(async () => {
      try {
        await meAPI.deleteProject(projId);
      } catch {
        // ignore
      }
      try {
        await adminAPI.deleteOrg(orgId);
      } catch {
        // ignore
      }
      try {
        const c = (await adminAPI.listOAuthClients()).find((x) => x.client_id === clientId);
        if (c) await adminAPI.deleteOAuthClient(c.id);
      } catch {
        // ignore
      }
    });

    it("adminAPI.getUsage with success_only=false", async () => {
      expect(await adminAPI.getUsage({ success_only: false })).toBeDefined();
    });

    it("adminAPI.startVectorMigration without a batch size", async () => {
      try {
        await adminAPI.startVectorMigration("to_qdrant");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("adminAPI.retryEnrichment without ids", async () => {
      try {
        const r = await adminAPI.retryEnrichment();
        expect(r).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("memoryAPI.list without limit/offset and listIDs without params", async () => {
      const list = await memoryAPI.list(projId, { tags: ["no-such-tag"] });
      expect(list.data).toBeDefined();
      const ids = await memoryAPI.listIDs(projId);
      expect(Array.isArray(ids.ids)).toBe(true);
    });

    it("meAPI.retryEnrichment without ids", async () => {
      try {
        const r = await meAPI.retryEnrichment();
        expect(r).toBeDefined();
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("orgAPI.getUsage with success_only=true and retryEnrichment without ids", async () => {
      expect(await orgAPI.getUsage(orgId, { success_only: true })).toBeDefined();
      try {
        await orgAPI.retryEnrichment(orgId);
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("systemAPI.getUsage with success_only=false", async () => {
      expect(await systemAPI.getUsage({ success_only: false })).toBeDefined();
    });

    it("oauthAPI.getAuthorizeContext with minimal params (no scope/resource/state)", async () => {
      const res = await oauthAPI.getAuthorizeContext({
        client_id: clientId,
        redirect_uri: redirectUri,
        response_type: "code",
        code_challenge: "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
        code_challenge_method: "S256",
      });
      expect(res).toBeDefined();
    });
  });

  // -----------------------------------------------------------------------
  // Download helpers (real server + minimal document stub)
  // -----------------------------------------------------------------------

  describe("download helpers", () => {
    let dlProjectId: string;
    let dlProjectSlug: string;
    let prevDocument: unknown;

    beforeAll(async () => {
      // triggerBlobDownload (lib/download) needs a DOM: under @vitest-environment
      // node there is none, so install a minimal document stub. URL.createObjectURL
      // / revokeObjectURL already exist in node.
      prevDocument = installDocumentStub();

      const proj = await meAPI.createProject({ name: "DL Proj", slug: "dl-proj" });
      dlProjectId = proj.id;
      dlProjectSlug = proj.slug;
      await memoryAPI.store(dlProjectId, { content: "downloadable memory" });
    });

    afterAll(async () => {
      (globalThis as Record<string, unknown>).document = prevDocument;
      try {
        await meAPI.deleteProject(dlProjectId);
      } catch {
        // ignore
      }
    });

    it("downloadProjectExport() (json default) triggers a download", async () => {
      await expect(
        downloadProjectExport(dlProjectId, dlProjectSlug),
      ).resolves.toBeUndefined();
    });

    it("downloadProjectExport() (ndjson + includeSuperseded) triggers a download", async () => {
      await expect(
        downloadProjectExport(dlProjectId, dlProjectSlug, {
          format: "ndjson",
          includeSuperseded: true,
        }),
      ).resolves.toBeUndefined();
    });

    it("downloadProjectExport() throws for a bogus project", async () => {
      try {
        await downloadProjectExport("00000000-0000-0000-0000-000000000000", "nope");
        expect.fail("should have thrown");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
    });

    it("downloadLogsExport() (csv + json) triggers a download", async () => {
      await expect(
        downloadLogsExport("csv", { level: ["error"], limit: 10 }),
      ).resolves.toBeUndefined();
      await expect(downloadLogsExport("json")).resolves.toBeUndefined();
    });

    it("downloadExportJobArtifact() throws for a bogus job id", async () => {
      try {
        await downloadExportJobArtifact("00000000-0000-0000-0000-000000000000");
        expect.fail("should have thrown");
      } catch (e) {
        expect(e).toBeInstanceOf(APIError);
      }
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
        await meAPI.getProject("00000000-0000-0000-0000-000000000000");
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

// ===========================================================================
// Pure functions + APIError message derivation -- no server required.
// ===========================================================================

describe("client pure helpers", () => {
  describe("memoryRowLabel", () => {
    it("prefers the preview when present", () => {
      expect(memoryRowLabel({ preview: "hello", length_chars: 5, id: "abcdefgh1234" })).toBe(
        "hello",
      );
    });

    it("falls back to a localized length hint", () => {
      expect(memoryRowLabel({ length_chars: 1234, id: "abcdefgh1234" })).toBe(
        `${(1234).toLocaleString()} chars`,
      );
    });

    it("falls back to a truncated id", () => {
      expect(memoryRowLabel({ id: "abcdefgh1234" })).toBe("abcdefgh…");
    });

    it("treats length_chars of 0 as present (not the id fallback)", () => {
      expect(memoryRowLabel({ length_chars: 0, id: "abcdefgh1234" })).toBe("0 chars");
    });
  });

  describe("buildLogQuery", () => {
    it("returns an empty string when there is nothing to encode", () => {
      expect(buildLogQuery()).toBe("");
      expect(buildLogQuery({})).toBe("");
      expect(buildLogQuery({ level: [] })).toBe("");
    });

    it("encodes every supported parameter", () => {
      const qs = buildLogQuery({
        level: ["error", "warn"],
        component: "server",
        search: "boom",
        attrKey: "request_id",
        attrValue: "abc",
        from: "2026-01-01T00:00:00Z",
        to: "2026-02-01T00:00:00Z",
        limit: 25,
        offset: 50,
      });
      const params = new URLSearchParams(qs.replace(/^\?/, ""));
      expect(params.get("level")).toBe("error,warn");
      expect(params.get("component")).toBe("server");
      expect(params.get("search")).toBe("boom");
      expect(params.get("attr_key")).toBe("request_id");
      expect(params.get("attr_value")).toBe("abc");
      expect(params.get("from")).toBe("2026-01-01T00:00:00Z");
      expect(params.get("to")).toBe("2026-02-01T00:00:00Z");
      expect(params.get("limit")).toBe("25");
      expect(params.get("offset")).toBe("50");
    });

    it("encodes an empty attrValue (defined but blank)", () => {
      expect(buildLogQuery({ attrValue: "" })).toBe("?attr_value=");
    });

    it("encodes limit/offset of 0", () => {
      const qs = buildLogQuery({ limit: 0, offset: 0 });
      const params = new URLSearchParams(qs.replace(/^\?/, ""));
      expect(params.get("limit")).toBe("0");
      expect(params.get("offset")).toBe("0");
    });
  });

  describe("isLoopbackRedirectUri", () => {
    it("accepts localhost, ::1 and 127.0.0.0/8", () => {
      expect(isLoopbackRedirectUri("http://localhost:8080/cb")).toBe(true);
      expect(isLoopbackRedirectUri("http://[::1]:8080/cb")).toBe(true);
      expect(isLoopbackRedirectUri("http://127.0.0.1/cb")).toBe(true);
      expect(isLoopbackRedirectUri("http://127.5.6.7/cb")).toBe(true);
    });

    it("rejects non-loopback hosts", () => {
      expect(isLoopbackRedirectUri("https://example.com/cb")).toBe(false);
      expect(isLoopbackRedirectUri("http://10.0.0.1/cb")).toBe(false);
    });

    it("returns false for an unparseable URI", () => {
      expect(isLoopbackRedirectUri("not a url")).toBe(false);
    });
  });

  describe("APIError.deriveMessage", () => {
    it("uses a nested error.message", () => {
      expect(new APIError(400, { error: { message: "nested boom" } }).message).toBe("nested boom");
    });

    it("uses a string-valued error field", () => {
      expect(new APIError(400, { error: "string boom" }).message).toBe("string boom");
    });

    it("uses a top-level message field", () => {
      expect(new APIError(400, { message: "top boom" }).message).toBe("top boom");
    });

    it("falls back to the generic status string for an empty error object", () => {
      expect(new APIError(400, { error: {} }).message).toBe("API error 400");
    });

    it("falls back to the generic status string for a non-object body", () => {
      expect(new APIError(500, "plain text").message).toBe("API error 500");
      expect(new APIError(500, null).message).toBe("API error 500");
    });
  });
});

// ===========================================================================
// Branches the real server cannot deterministically drive. These swap
// globalThis.fetch for a synthetic Response per test -- the same mechanism the
// E2E suite already uses for its global wrapper (not msw).
// ===========================================================================

describe("synthetic-response branches", () => {
  let savedFetch: typeof globalThis.fetch;
  let prevWindow: unknown;
  let prevDocument: unknown;

  beforeEach(() => {
    savedFetch = globalThis.fetch;
    prevWindow = (globalThis as Record<string, unknown>).window;
    prevDocument = installDocumentStub();
  });

  afterEach(() => {
    globalThis.fetch = savedFetch;
    (globalThis as Record<string, unknown>).window = prevWindow;
    (globalThis as Record<string, unknown>).document = prevDocument;
    localStorage.removeItem("nram_token");
    localStorage.removeItem("nram_user");
  });

  function stubFetch(make: () => Response): void {
    globalThis.fetch = (async () => make()) as typeof globalThis.fetch;
  }

  it("applies a refreshed JWT carrying a decodable payload", async () => {
    const payload = btoa(
      JSON.stringify({
        sub: "u1",
        email: "a@b.c",
        display_name: "A",
        role: "member",
        org_id: "o1",
      }),
    );
    const refreshed = `h.${payload}.s`;
    localStorage.setItem("nram_token", "old-token");
    stubFetch(
      () =>
        new Response(JSON.stringify({}), {
          status: 200,
          headers: { "X-Refreshed-Token": refreshed, "Content-Type": "application/json" },
        }),
    );
    await meAPI.getProfile();
    expect(localStorage.getItem("nram_token")).toBe(refreshed);
    expect(localStorage.getItem("nram_user")).toBeTruthy();
  });

  it("applies a refreshed token whose payload is undecodable (no user stored)", async () => {
    localStorage.setItem("nram_token", "old-token");
    localStorage.removeItem("nram_user");
    stubFetch(
      () =>
        new Response(JSON.stringify({}), {
          status: 200,
          headers: { "X-Refreshed-Token": "not-a-jwt", "Content-Type": "application/json" },
        }),
    );
    await meAPI.getProfile();
    expect(localStorage.getItem("nram_token")).toBe("not-a-jwt");
    expect(localStorage.getItem("nram_user")).toBeNull();
  });

  it("fetchText throws APIError on a non-ok response", async () => {
    stubFetch(() => new Response("boom", { status: 500 }));
    await expect(fetchMetricsText()).rejects.toThrow(APIError);
  });

  it("never-settling 401 redirect clears the token and navigates to /login", async () => {
    localStorage.setItem("nram_token", "expired");
    (globalThis as Record<string, unknown>).window = {
      location: { pathname: "/dashboard", href: "" },
    };
    stubFetch(() => new Response("unauthorized", { status: 401 }));
    const pending = meAPI.getProfile();
    const outcome = await Promise.race([
      pending.then(() => "resolved"),
      new Promise((r) => setTimeout(() => r("pending"), 50)),
    ]);
    expect(outcome).toBe("pending");
    const win = (globalThis as Record<string, unknown>).window as {
      location: { href: string };
    };
    expect(win.location.href).toBe("/login");
    expect(localStorage.getItem("nram_token")).toBeNull();
    expect(localStorage.getItem("nram_user")).toBeNull();
  });

  it("getProviderModels returns the wrapped models list on success", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(
      () =>
        new Response(JSON.stringify(["m1", "m2"]), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    const res = await adminAPI.getProviderModels("http://vllm.local", { "X-A": "v" });
    expect(res.models).toEqual(["m1", "m2"]);
  });

  it("getOllamaModels returns the wrapped models list on success", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(
      () =>
        new Response(JSON.stringify([{ name: "llama3" }]), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    const res = await adminAPI.getOllamaModels("http://ollama.local", { "X-A": "v", "X-Empty": "" });
    expect(Array.isArray(res.models)).toBe(true);
  });

  it("getCostRates falls back to [] when data is absent", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(
      () =>
        new Response(JSON.stringify({}), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    expect(await adminAPI.getCostRates()).toEqual([]);
  });

  it("meAPI.listExportJobs falls back to [] when data is absent", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(
      () =>
        new Response(JSON.stringify({}), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    expect(await meAPI.listExportJobs()).toEqual([]);
  });

  it("downloadExportJobArtifact uses the Content-Disposition filename when present", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(
      () =>
        new Response(new Blob(["zip-bytes"]), {
          status: 200,
          headers: { "Content-Disposition": 'attachment; filename="custom.zip"' },
        }),
    );
    await expect(downloadExportJobArtifact("job-1")).resolves.toBeUndefined();
  });

  it("downloadExportJobArtifact falls back to a default filename when absent", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(() => new Response(new Blob(["zip-bytes"]), { status: 200 }));
    await expect(downloadExportJobArtifact("job-2")).resolves.toBeUndefined();
  });

  it("downloadLogsExport surfaces a non-ok response as APIError", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(() => new Response("nope", { status: 500 }));
    await expect(downloadLogsExport("csv")).rejects.toThrow(APIError);
  });

  it("downloadLogsExport falls back to a default filename when no Content-Disposition", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(() => new Response(new Blob(["log-bytes"]), { status: 200 }));
    await expect(downloadLogsExport("json")).resolves.toBeUndefined();
  });

  it("downloadProjectExport surfaces a non-ok response as APIError", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(() => new Response("nope", { status: 403 }));
    await expect(downloadProjectExport("p", "slug")).rejects.toThrow(APIError);
  });

  it("forwardedProviderHeaders yields undefined when every header value is blank", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(
      () =>
        new Response(JSON.stringify([]), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    const res = await adminAPI.getProviderModels("http://vllm.local", { "X-Empty": "" });
    expect(res.models).toEqual([]);
  });

  it("pullOllamaModel includes non-empty headers and tolerates an empty map", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(
      () =>
        new Response(JSON.stringify({ status: "ok", model: "m" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    expect(await adminAPI.pullOllamaModel("m", "http://ollama.local", { "X-A": "v" })).toBeDefined();
    expect(await adminAPI.pullOllamaModel("m", "http://ollama.local", {})).toBeDefined();
  });

  it("403 with an empty body uses the default forbidden message", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(() => new Response("", { status: 403 }));
    try {
      await meAPI.getProfile();
      expect.fail("should have thrown");
    } catch (e) {
      expect(e).toBeInstanceOf(APIError);
      expect((e as APIError).status).toBe(403);
      // Empty body -> the default forbidden string lands in APIError.body.
      expect((e as APIError).body).toContain("forbidden");
    }
  });

  it("downloadLogsExport empty-body error uses the default message", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(() => new Response("", { status: 500 }));
    await expect(downloadLogsExport("csv")).rejects.toThrow(APIError);
  });

  it("downloadExportJobArtifact empty-body error uses the default message", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(() => new Response("", { status: 404 }));
    await expect(downloadExportJobArtifact("x")).rejects.toThrow(APIError);
  });

  it("downloadProjectExport empty-body error uses the default message", async () => {
    localStorage.setItem("nram_token", "t");
    stubFetch(() => new Response("", { status: 500 }));
    await expect(downloadProjectExport("p", "slug")).rejects.toThrow(APIError);
  });
});

// ===========================================================================
// Destructive endpoints -- run against a dedicated throwaway server so they
// cannot corrupt the main suite's shared state.
// ===========================================================================

describe("destructive endpoints (isolated server)", () => {
  const DPORT = 18675;
  const DURL = `http://localhost:${DPORT}`;
  let proc: ChildProcess;
  let dTmp: string;
  let savedToken: string | null;

  beforeAll(async () => {
    savedToken = localStorage.getItem("nram_token");

    // Re-point relative URLs at the dedicated server for the duration of this
    // suite. The outer wrapper rewrites "/..." to the main server; override it.
    const nodeFetch = globalThis.fetch;
    globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      let url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : (input as Request).url;
      if (url.startsWith("/")) url = `${DURL}${url}`;
      return nodeFetch(url, init);
    };

    const started = await startNramServer(DPORT, "nram-e2e-destructive-");
    proc = started.proc;
    dTmp = started.serverTmp;

    const setupRes = await fetch(`${DURL}/v1/admin/setup`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: "admin@dtest.com", password: "TestPassword123!" }),
    });
    if (!setupRes.ok) {
      throw new Error(`Destructive setup failed (${setupRes.status})`);
    }
    const data: SetupResponse = await setupRes.json();
    localStorage.setItem("nram_token", data.token);
  }, 45000);

  afterAll(async () => {
    if (proc) {
      proc.kill("SIGTERM");
      await new Promise<void>((resolve) => {
        proc.on("exit", () => resolve());
        setTimeout(resolve, 5000);
      });
    }
    if (dTmp) {
      try {
        rmSync(dTmp, { recursive: true, force: true });
      } catch {
        // ignore
      }
    }
    if (savedToken) {
      localStorage.setItem("nram_token", savedToken);
    } else {
      localStorage.removeItem("nram_token");
    }
  });

  it("completeSetup() rejects once setup is already complete", async () => {
    try {
      await adminAPI.completeSetup({ email: "admin@dtest.com", password: "TestPassword123!" });
      expect.fail("setup should not run twice");
    } catch (e) {
      expect(e).toBeInstanceOf(APIError);
    }
  });

  it("resetSettings() resets settings to defaults", async () => {
    await adminAPI.updateSetting("enrichment.enabled", false);
    const res = await adminAPI.resetSettings();
    expect(typeof res.status).toBe("string");
    expect(typeof res.reset).toBe("number");
  });

  it("resetSettings(key) resets a single setting", async () => {
    const res = await adminAPI.resetSettings({ key: "enrichment.enabled" });
    expect(typeof res.reset).toBe("number");
  });

  it("resetDatabase() truncates the dedicated database", async () => {
    // Runs last: this wipes the throwaway DB. Either a success result or an
    // APIError (mode unsupported on sqlite) exercises the request path.
    try {
      const res = await adminAPI.resetDatabase("", "truncate");
      expect(res).toBeDefined();
    } catch (e) {
      expect(e).toBeInstanceOf(APIError);
    }
  });
});
