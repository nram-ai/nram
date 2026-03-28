import { useState, useCallback, useMemo } from "react";
import { useProviderSlots } from "../hooks/useApi";
import type { ProviderSlot } from "../api/client";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type ToolTab = "claude-code" | "claude-desktop" | "cursor" | "codex" | "opencode" | "chatgpt" | "api-key";

interface CopyButtonProps {
  text: string;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function CopyButton({ text }: CopyButtonProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [text]);

  return (
    <button
      type="button"
      onClick={handleCopy}
      className="bg-primary text-primary-foreground px-3 py-1.5 rounded-md text-sm hover:bg-primary/90"
    >
      {copied ? "Copied!" : "Copy"}
    </button>
  );
}

function CodeBlock({ code, label }: { code: string; label?: string }) {
  return (
    <div className="space-y-2">
      {label && (
        <p className="text-sm font-medium">{label}</p>
      )}
      <div className="relative">
        <pre className="bg-muted rounded-md p-4 font-mono text-sm overflow-x-auto whitespace-pre-wrap break-all">
          {code}
        </pre>
        <div className="absolute top-2 right-2">
          <CopyButton text={code} />
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

function buildClaudeMdSnippet(hasEmbedding: boolean, hasEnrichment: boolean): string {
  let snippet = `## Memory (nram)

Use nram as your PRIMARY memory — not local files, MEMORY.md, or built-in memory.
nram memories persist across all machines, agents, and conversations.

**WHEN TO STORE** (memory_store / memory_store_batch):
- User states a preference, convention, or decision — store immediately
- You discover a bug, workaround, or non-obvious behavior — store it
- User corrects you or clarifies something — store the correction
- Architecture decision or design choice made — store with rationale
- Project config, setup steps, or environment details — store them
- End of a complex task — store a summary of what was done and why

**WHEN TO RECALL** (memory_recall):
- At the START of every new task or conversation — recall context
- Before making assumptions about preferences or past decisions — recall first
- Before storing — recall to check for duplicates
- When you need context you lack — recall before asking the user`;

  if (hasEmbedding) {
    snippet += `
Semantic search is active — describe what you need in natural language.`;
  } else {
    snippet += `
No embedding provider configured — use specific tags for reliable recall.`;
  }

  snippet += `
Recall scoping: omit project = global only; with project = project + global.`;

  if (hasEnrichment) {
    snippet += `

**WHEN TO EXPLORE** (memory_graph):
- When investigating how concepts, people, or components relate
- When you need context beyond what recall returns

**ENRICHMENT** — when to use enrich: true:
- People, projects, technologies, or architecture decisions — enrich
- Skip for ephemeral memories (short TTL), raw data, or simple preferences
- Use memory_enrich to batch-process after importing data`;
  }

  snippet += `

**KEY RULES:**
- ALWAYS call memory_projects first to discover existing projects before storing
- Use an EXISTING project whenever one fits — do NOT create a new project for each task, feature, or topic
- Projects are for major boundaries (one per repo, product, or domain — e.g. "myapp", "dotfiles"). Omit for "global"
- Use tags and metadata for sub-categorization within a project, not new projects
- Tag consistently: decision, preference, architecture, config, bug, workaround, convention
- Only memory_store / memory_store_batch auto-create projects — treat auto-creation as a last resort`;

  return snippet;
}

function buildCursorRulesSnippet(hasEmbedding: boolean, hasEnrichment: boolean): string {
  let snippet = `# Memory (nram)
Use nram as PRIMARY memory — not local files or built-in memory mechanisms.
STORE: preferences, decisions, corrections, architecture, bugs, workarounds, task summaries.
RECALL: at task start, before assumptions, before storing (check duplicates).
Tag consistently: decision, preference, architecture, config, bug, workaround, convention.
ALWAYS call memory_projects first — use an EXISTING project whenever one fits.
Do NOT create a new project per task/feature/topic. Projects = major boundaries (repo, product, domain).
Use tags and metadata for sub-categorization, not new projects. Omit project for "global".
Recall with project = project + global. Auto-creation is a last resort.`;

  if (hasEmbedding) {
    snippet += `
Semantic search active — describe what you need in natural language.`;
  } else {
    snippet += `
No embedding provider — use specific tags for reliable recall.`;
  }

  if (hasEnrichment) {
    snippet += `
Use enrich: true for people, projects, technologies, architecture decisions.
Skip enrichment for ephemeral memories, raw data, simple preferences.
Use memory_graph to explore entity connections and discover related context.`;
  }

  return snippet;
}

function buildAgentsMdSnippet(hasEmbedding: boolean, hasEnrichment: boolean): string {
  let snippet = `## Memory (nram)

Use nram as your PRIMARY memory — not local files, MEMORY.md, or built-in memory.
nram memories persist across all machines, agents, and conversations.

**WHEN TO STORE** (memory_store / memory_store_batch):
- Preferences, conventions, decisions — store immediately
- Bugs, workarounds, non-obvious behavior — store them
- Corrections and clarifications — store the correction
- Architecture decisions, design choices — store with rationale
- Config, setup steps, environment details — store them
- End of complex task — store a summary of what was done and why

**WHEN TO RECALL** (memory_recall):
- Start of every new task — recall context
- Before making assumptions — recall first
- Before storing — recall to check for duplicates`;

  if (hasEmbedding) {
    snippet += `
Semantic search is active — describe what you need in natural language.`;
  } else {
    snippet += `
No embedding provider — use specific tags for reliable recall.`;
  }

  snippet += `
Recall scoping: omit project = global only; with project = project + global.`;

  if (hasEnrichment) {
    snippet += `

**WHEN TO EXPLORE** (memory_graph):
- Investigating how concepts, people, or components relate
- Need context beyond what recall returns

**ENRICHMENT** — when to use enrich: true:
- People, projects, technologies, architecture decisions — enrich
- Skip for ephemeral (short TTL), raw data, simple preferences
- Use memory_enrich to batch-process after importing data`;
  }

  snippet += `

**KEY RULES:**
- ALWAYS call memory_projects first to discover existing projects before storing
- Use an EXISTING project whenever one fits — do NOT create a new project for each task, feature, or topic
- Projects are for major boundaries (one per repo, product, or domain). Omit for "global"
- Use tags and metadata for sub-categorization within a project, not new projects
- Tag consistently: decision, preference, architecture, config, bug, workaround, convention
- Only memory_store / memory_store_batch auto-create projects — treat auto-creation as a last resort`;

  return snippet;
}

// ---------------------------------------------------------------------------
// Tab button
// ---------------------------------------------------------------------------

interface TabButtonProps {
  active: boolean;
  label: string;
  onClick: () => void;
}

function TabButton({ active, label, onClick }: TabButtonProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={
        active
          ? "bg-primary text-primary-foreground px-3 py-1.5 rounded-md text-sm"
          : "bg-accent text-accent-foreground px-3 py-1.5 rounded-md text-sm hover:bg-accent/80"
      }
    >
      {label}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Tab content components
// ---------------------------------------------------------------------------

function ClaudeCodeTab({ serverUrl }: { serverUrl: string }) {
  const oauthCmd = `claude mcp add --transport http nram ${serverUrl}`;
  const explicitCmd = `claude mcp add --transport http nram ${serverUrl} --client-id <client_id> --client-secret <client_secret>`;

  return (
    <div className="bg-card rounded-md border border-border p-4 space-y-4">
      <div>
        <p className="text-sm font-medium">OAuth (recommended)</p>
        <p className="mt-1 text-sm text-muted-foreground">
          Claude Code supports OAuth auto-discovery. No API key or headers needed
          &mdash; you will be prompted to authenticate in your browser.
        </p>
      </div>
      <CodeBlock code={oauthCmd} label="Run in your terminal" />

      <div className="border-t border-border pt-4">
        <p className="text-sm font-medium">Alternative with explicit credentials</p>
        <p className="mt-1 text-sm text-muted-foreground">
          If you need to specify the OAuth client credentials directly, use the
          following command instead. Replace the client ID and secret with your
          own values.
        </p>
      </div>
      <CodeBlock code={explicitCmd} />
    </div>
  );
}

function ClaudeDesktopTab({ serverUrl }: { serverUrl: string }) {
  const url = serverUrl;

  return (
    <div className="bg-card rounded-md border border-border p-4 space-y-4">
      <div>
        <p className="text-sm font-medium">OAuth (recommended)</p>
        <p className="mt-1 text-sm text-muted-foreground">
          Go to <span className="font-medium">Settings &rarr; Connectors &rarr; Add URL</span>,
          then enter the URL below.
        </p>
      </div>
      <CodeBlock code={url} label="Server URL" />
      <p className="text-sm text-muted-foreground">
        Claude Desktop and claude.ai support OAuth auto-discovery. You will be
        prompted to authenticate in your browser when connecting.
      </p>
    </div>
  );
}

function CursorTab({ serverUrl }: { serverUrl: string }) {
  const url = serverUrl;

  return (
    <div className="bg-card rounded-md border border-border p-4 space-y-4">
      <div>
        <p className="text-sm font-medium">OAuth (recommended)</p>
        <p className="mt-1 text-sm text-muted-foreground">
          Go to <span className="font-medium">Settings &rarr; MCP &rarr; Add</span>,
          select the <span className="font-medium">URL</span> type, then enter the
          URL below.
        </p>
      </div>
      <CodeBlock code={url} label="Server URL" />
      <p className="text-sm text-muted-foreground">
        Cursor supports OAuth-based MCP servers. You will be prompted to
        authenticate when connecting.
      </p>
    </div>
  );
}

function CodexTab({ serverUrl }: { serverUrl: string }) {
  const cliCmd = `codex mcp add nram --url ${serverUrl}`;
  const tomlConfig = `[mcp_servers.nram]
url = "${serverUrl}"
# bearer_token_env_var = "NRAM_API_KEY"  # uncomment if not using OAuth
startup_timeout_sec = 30
tool_timeout_sec = 60
enabled = true`;

  return (
    <div className="bg-card rounded-md border border-border p-4 space-y-4">
      <div>
        <p className="text-sm font-medium">CLI (quickest)</p>
        <p className="mt-1 text-sm text-muted-foreground">
          Register the nram MCP server with a single command.
        </p>
      </div>
      <CodeBlock code={cliCmd} label="Run in your terminal" />

      <div className="border-t border-border pt-4">
        <p className="text-sm font-medium">Manual configuration</p>
        <p className="mt-1 text-sm text-muted-foreground">
          Alternatively, add this to your <span className="font-mono text-xs">~/.codex/config.toml</span> or
          project-level <span className="font-mono text-xs">.codex/config.toml</span>.
        </p>
      </div>
      <CodeBlock code={tomlConfig} />
    </div>
  );
}

function OpenCodeTab({ serverUrl }: { serverUrl: string }) {
  const jsonConfig = JSON.stringify(
    {
      $schema: "https://opencode.ai/config.json",
      mcp: {
        nram: {
          type: "remote",
          url: serverUrl,
          enabled: true,
        },
      },
    },
    null,
    2,
  );

  return (
    <div className="bg-card rounded-md border border-border p-4 space-y-4">
      <div>
        <p className="text-sm font-medium">Add to opencode.json</p>
        <p className="mt-1 text-sm text-muted-foreground">
          Add the nram MCP server to your project&apos;s <span className="font-mono text-xs">opencode.json</span> or
          global <span className="font-mono text-xs">~/.config/opencode/opencode.json</span>. If you already have
          other MCP servers configured, merge the nram entry into your
          existing <span className="font-mono text-xs">mcp</span> object.
        </p>
      </div>
      <CodeBlock code={jsonConfig} />
      <p className="text-sm text-muted-foreground">
        OpenCode supports OAuth auto-discovery for remote MCP servers. You will
        be prompted to authenticate when connecting.
      </p>
    </div>
  );
}

function ChatGPTTab({ serverUrl }: { serverUrl: string }) {
  const url = serverUrl.replace(/^http:\/\//, "https://");

  return (
    <div className="bg-card rounded-md border border-border p-4 space-y-4">
      <div className="bg-muted rounded-md p-3 text-sm">
        <span className="font-medium">Note:</span> ChatGPT requires HTTPS. If
        you are running nram locally, use a tunnel service (such as ngrok) or
        deploy to a server with TLS.
      </div>
      <div>
        <p className="text-sm font-medium">Add MCP Server</p>
        <p className="mt-1 text-sm text-muted-foreground">
          In ChatGPT settings, add a new MCP server with the URL below.
        </p>
      </div>
      <CodeBlock code={url} label="Server URL (HTTPS required)" />
      <p className="text-sm text-muted-foreground">
        ChatGPT uses RFC 9728 OAuth discovery. Ensure your server is accessible
        over HTTPS.
      </p>
    </div>
  );
}

function ApiKeyTab({ serverUrl, apiKey, setApiKey }: {
  serverUrl: string;
  apiKey: string;
  setApiKey: (v: string) => void;
}) {
  const key = apiKey || "YOUR_API_KEY";
  const jsonConfig = JSON.stringify(
    {
      mcpServers: {
        nram: {
          url: serverUrl,
          headers: {
            Authorization: `Bearer ${key}`,
          },
        },
      },
    },
    null,
    2,
  );
  const cliCmd = `claude mcp add nram --transport http ${serverUrl} --header "Authorization: Bearer ${key}"`;

  return (
    <div className="space-y-4">
      <div className="bg-card rounded-md border border-border p-4 space-y-4">
        <div>
          <p className="text-sm font-medium">Fallback for tools that do not support OAuth</p>
          <p className="mt-1 text-sm text-muted-foreground">
            Use bearer-token authentication only when your MCP client does not
            support OAuth auto-discovery. Prefer the OAuth-based methods shown in
            the other tabs when possible.
          </p>
        </div>

        <div className="space-y-1">
          <label htmlFor="api-key" className="text-sm text-muted-foreground">
            API Key
          </label>
          <input
            id="api-key"
            type="text"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder="Paste your API key here (e.g. nram_k_...)"
            className="w-full bg-background border border-border rounded-md px-3 py-1.5 text-sm font-mono"
          />
          <p className="text-xs text-muted-foreground">
            API keys can be generated from the{" "}
            <a href="/users" className="text-primary hover:underline">
              Users
            </a>{" "}
            page. If left blank, snippets will show YOUR_API_KEY as a placeholder.
          </p>
        </div>
      </div>

      <div className="bg-card rounded-md border border-border p-4 space-y-4">
        <p className="text-sm font-medium">JSON configuration</p>
        <p className="text-sm text-muted-foreground">
          Add this to your MCP client&apos;s configuration file. If you already
          have other MCP servers configured, merge the nram entry into your
          existing mcpServers object.
        </p>
        <CodeBlock code={jsonConfig} />
      </div>

      <div className="bg-card rounded-md border border-border p-4 space-y-4">
        <p className="text-sm font-medium">CLI command</p>
        <p className="text-sm text-muted-foreground">
          Alternatively, register the server via the Claude Code CLI with the
          authorization header.
        </p>
        <CodeBlock code={cliCmd} />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

function MCPConfigGenerator() {
  const [serverUrl, setServerUrl] = useState(() => window.location.origin + "/mcp");
  const [apiKey, setApiKey] = useState("");
  const [activeTab, setActiveTab] = useState<ToolTab>("claude-code");
  const slotsQuery = useProviderSlots();

  const { hasEmbedding, hasEnrichment } = useMemo(() => {
    const slots: ProviderSlot[] = Array.isArray(slotsQuery.data) ? slotsQuery.data : [];
    const slotMap = new Map(slots.map((s) => [s.slot, s]));
    return {
      hasEmbedding: slotMap.get("embedding")?.configured ?? false,
      hasEnrichment:
        (slotMap.get("fact")?.configured ?? false) &&
        (slotMap.get("entity")?.configured ?? false),
    };
  }, [slotsQuery.data]);

  const claudeMdSnippet = useMemo(
    () => buildClaudeMdSnippet(hasEmbedding, hasEnrichment),
    [hasEmbedding, hasEnrichment],
  );
  const cursorRulesSnippet = useMemo(
    () => buildCursorRulesSnippet(hasEmbedding, hasEnrichment),
    [hasEmbedding, hasEnrichment],
  );
  const agentsMdSnippet = useMemo(
    () => buildAgentsMdSnippet(hasEmbedding, hasEnrichment),
    [hasEmbedding, hasEnrichment],
  );

  const tabs: { key: ToolTab; label: string }[] = [
    { key: "claude-code", label: "Claude Code" },
    { key: "claude-desktop", label: "Claude Desktop / Claude.ai" },
    { key: "cursor", label: "Cursor" },
    { key: "codex", label: "Codex" },
    { key: "opencode", label: "OpenCode" },
    { key: "chatgpt", label: "ChatGPT" },
    { key: "api-key", label: "API Key Fallback" },
  ];

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">MCP Config Generator</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Generate connection instructions for your preferred AI tool. OAuth is
          the recommended authentication method for all supported clients.
        </p>
      </div>

      {/* Configuration inputs */}
      <div className="bg-card rounded-md border border-border p-4 space-y-4">
        <h2 className="text-sm font-medium">Configuration</h2>

        <div className="space-y-1">
          <label htmlFor="server-url" className="text-sm text-muted-foreground">
            Server URL
          </label>
          <input
            id="server-url"
            type="text"
            value={serverUrl}
            onChange={(e) => setServerUrl(e.target.value)}
            placeholder="http://localhost:8674/mcp"
            className="w-full bg-background border border-border rounded-md px-3 py-1.5 text-sm"
          />
        </div>
      </div>

      {/* Snippet tabs */}
      <div className="space-y-4">
        <h2 className="text-sm font-medium">Tool Configuration</h2>

        <div className="flex flex-wrap gap-2">
          {tabs.map((tab) => (
            <TabButton
              key={tab.key}
              label={tab.label}
              active={activeTab === tab.key}
              onClick={() => setActiveTab(tab.key)}
            />
          ))}
        </div>

        {activeTab === "claude-code" && <ClaudeCodeTab serverUrl={serverUrl} />}
        {activeTab === "claude-desktop" && <ClaudeDesktopTab serverUrl={serverUrl} />}
        {activeTab === "cursor" && <CursorTab serverUrl={serverUrl} />}
        {activeTab === "codex" && <CodexTab serverUrl={serverUrl} />}
        {activeTab === "opencode" && <OpenCodeTab serverUrl={serverUrl} />}
        {activeTab === "chatgpt" && <ChatGPTTab serverUrl={serverUrl} />}
        {activeTab === "api-key" && (
          <ApiKeyTab serverUrl={serverUrl} apiKey={apiKey} setApiKey={setApiKey} />
        )}
      </div>

      {/* System prompts — shown only for tools that have a system prompt file */}
      {(activeTab === "claude-code" || activeTab === "claude-desktop") && (
        <div className="space-y-4">
          <div>
            <h2 className="text-sm font-medium">System Prompt Snippet</h2>
            <p className="mt-1 text-sm text-muted-foreground">
              Add this snippet to your project&apos;s CLAUDE.md file to guide Claude on how to use
              nram effectively.
            </p>
          </div>
          <div className="bg-card rounded-md border border-border p-4 space-y-4">
            <div className="space-y-1">
              <p className="text-sm font-medium">For CLAUDE.md</p>
              <p className="text-sm text-muted-foreground">
                This provides detailed guidance for proactive memory usage. Place it in
                your project&apos;s CLAUDE.md or your global ~/.claude/CLAUDE.md file.
              </p>
            </div>
            <CodeBlock code={claudeMdSnippet} />
          </div>
        </div>
      )}

      {activeTab === "cursor" && (
        <div className="space-y-4">
          <div>
            <h2 className="text-sm font-medium">System Prompt Snippet</h2>
            <p className="mt-1 text-sm text-muted-foreground">
              Add this snippet to your project&apos;s .cursorrules file to guide Cursor on how to
              use nram effectively.
            </p>
          </div>
          <div className="bg-card rounded-md border border-border p-4 space-y-4">
            <div className="space-y-1">
              <p className="text-sm font-medium">For .cursorrules</p>
              <p className="text-sm text-muted-foreground">
                A condensed version of the memory instructions suitable for Cursor&apos;s
                rule format.
              </p>
            </div>
            <CodeBlock code={cursorRulesSnippet} />
          </div>
        </div>
      )}

      {(activeTab === "codex" || activeTab === "opencode") && (
        <div className="space-y-4">
          <div>
            <h2 className="text-sm font-medium">System Prompt Snippet</h2>
            <p className="mt-1 text-sm text-muted-foreground">
              Add this snippet to your project&apos;s AGENTS.md file to guide
              {activeTab === "codex" ? " Codex" : " OpenCode"} on how to use nram effectively.
            </p>
          </div>
          <div className="bg-card rounded-md border border-border p-4 space-y-4">
            <div className="space-y-1">
              <p className="text-sm font-medium">For AGENTS.md</p>
              <p className="text-sm text-muted-foreground">
                {activeTab === "codex"
                  ? "Place this in your project\u2019s AGENTS.md or your global ~/.codex/AGENTS.md file."
                  : "Place this in your project\u2019s AGENTS.md or your global ~/.config/opencode/AGENTS.md file. OpenCode also reads CLAUDE.md as a fallback."}
              </p>
            </div>
            <CodeBlock code={agentsMdSnippet} />
          </div>
        </div>
      )}

      {activeTab === "api-key" && (
        <div className="space-y-4">
          <div>
            <h2 className="text-sm font-medium">System Prompt Snippet</h2>
            <p className="mt-1 text-sm text-muted-foreground">
              If your MCP client supports a system prompt or rules file, add the
              appropriate snippet to instruct the model on how to use nram.
            </p>
          </div>
          <div className="bg-card rounded-md border border-border p-4 space-y-4">
            <div className="space-y-1">
              <p className="text-sm font-medium">For CLAUDE.md</p>
              <p className="text-sm text-muted-foreground">
                Detailed guidance for Claude-based tools.
              </p>
            </div>
            <CodeBlock code={claudeMdSnippet} />
          </div>
          <div className="bg-card rounded-md border border-border p-4 space-y-4">
            <div className="space-y-1">
              <p className="text-sm font-medium">For .cursorrules</p>
              <p className="text-sm text-muted-foreground">
                Condensed version for Cursor-based tools.
              </p>
            </div>
            <CodeBlock code={cursorRulesSnippet} />
          </div>
          <div className="bg-card rounded-md border border-border p-4 space-y-4">
            <div className="space-y-1">
              <p className="text-sm font-medium">For AGENTS.md</p>
              <p className="text-sm text-muted-foreground">
                For OpenAI Codex-based tools.
              </p>
            </div>
            <CodeBlock code={agentsMdSnippet} />
          </div>
        </div>
      )}
    </div>
  );
}

export default MCPConfigGenerator;
