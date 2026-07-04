import { useEffect, useState, type ReactNode } from "react";
import { CopyButton } from "../components/CopyButton";
import { getInstructions } from "../api/client";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type ToolTab = "claude-code" | "claude-desktop" | "cursor" | "cursor-cli" | "codex" | "opencode" | "chatgpt" | "api-key";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Primary-styled copy button used throughout the MCP config page.
const MCP_COPY_CLASS =
  "bg-primary text-primary-foreground px-3 py-1.5 rounded-md text-sm hover:bg-primary/90";

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
          <CopyButton text={code} copiedLabel="Copied!" className={MCP_COPY_CLASS} />
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// The instructions/rules snippets are served by the backend at GET
// /instructions and fetched at runtime (see the main component). The server is
// the single source of truth so the page and external callers read one copy;
// nothing here hardcodes the wording.

type Snippets = { claude: string; condensed: string; agents: string };

// A single labeled snippet card within a System Prompt Snippet section: which
// fetched flavor to show and the prose that frames it. filePrefix, when set, is
// prepended to the copyable block so the user can save the result verbatim as a
// complete file (e.g. a Cursor rule needs a small header above the body).
type SnippetCardSpec = {
  label: string;
  description: ReactNode;
  snippet: keyof Snippets;
  filePrefix?: string;
};

// Header that makes a Cursor rule load automatically in every session. We ship
// it as part of the copyable file so users never have to hand-write it; the
// body follows after the blank line.
const CURSOR_RULE_HEADER = "---\nalwaysApply: true\n---\n\n";

// Per-tab content for the System Prompt Snippet section: the placement guidance
// under the shared heading, plus one card per snippet the client needs.
type SystemPromptSpec = {
  description: ReactNode;
  cards: SnippetCardSpec[];
};

// SnippetBlock renders a fetched instructions snippet, or a loading/error
// placeholder while the fetch is in flight or has failed.
function SnippetBlock({ code, error }: { code: string | undefined; error: string | null }) {
  if (error) {
    return <p className="text-sm text-destructive">Failed to load snippet: {error}</p>;
  }
  if (code === undefined) {
    return <p className="text-sm text-muted-foreground">Loading…</p>;
  }
  return <CodeBlock code={code} />;
}

// A single labeled snippet card: a heading row (label + description) above the
// fetched snippet. Used by SystemPromptSection; most tabs render one, the API
// key tab renders several.
function SnippetCard({ label, description, code, error, filePrefix }: {
  label: string;
  description: ReactNode;
  code: string | undefined;
  error: string | null;
  filePrefix?: string;
}) {
  const shown = code === undefined ? undefined : (filePrefix ?? "") + code;
  return (
    <div className="bg-card rounded-md border border-border p-4 space-y-4">
      <div className="space-y-1">
        <p className="text-sm font-medium">{label}</p>
        <p className="text-sm text-muted-foreground">{description}</p>
      </div>
      <SnippetBlock code={shown} error={error} />
    </div>
  );
}

// SystemPromptSection renders the "System Prompt Snippet" block for a tab: a
// shared heading plus placement guidance, then one card per snippet the client
// needs. The per-tab content lives in SYSTEM_PROMPT_SECTIONS so each client is
// data, not copy-pasted markup.
function SystemPromptSection({ spec, snippets, error }: {
  spec: SystemPromptSpec;
  snippets: Snippets | null;
  error: string | null;
}) {
  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-sm font-medium">System Prompt Snippet</h2>
        <p className="mt-1 text-sm text-muted-foreground">{spec.description}</p>
      </div>
      {spec.cards.map((card) => (
        <SnippetCard
          key={card.label}
          label={card.label}
          description={card.description}
          code={snippets?.[card.snippet]}
          error={error}
          filePrefix={card.filePrefix}
        />
      ))}
    </div>
  );
}

// HostedWebHttpsNote explains why the hosted web clients (ChatGPT, claude.ai, and
// the Claude desktop/mobile apps) need a public HTTPS URL. The wording is kept in
// sync with the same callout in README.md so the page and the README read as one
// message. Only the hosted-web tabs render this; local clients (Claude Code, Cursor,
// Cursor CLI, Codex, OpenCode) reach nram directly and do not need it.
function HostedWebHttpsNote() {
  return (
    <div className="bg-muted rounded-md p-3 text-sm space-y-2">
      <p>
        <span className="font-medium">Hosted web tools need a public HTTPS URL.</span>{" "}
        ChatGPT, Claude on the web (claude.ai), and the Claude desktop and mobile apps
        reach your server from the vendor&apos;s cloud, not from your machine, so{" "}
        <span className="font-mono text-xs">http://localhost</span> will not work. They
        require a real, publicly resolvable hostname served over HTTPS with a valid (not
        self-signed) TLS certificate.
      </p>
      <p>
        nram serves plain HTTP and does not terminate TLS itself, so put it behind a
        reverse proxy that handles TLS (Caddy, nginx, Traefik) or expose it through a
        tunnel (Cloudflare Tunnel, ngrok, Tailscale Funnel), then point the connector at
        your public <span className="font-mono text-xs">https://your-host/mcp</span> URL.
      </p>
    </div>
  );
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
          Claude Code supports OAuth auto-discovery. No API key or headers needed;
          you will be prompted to authenticate in your browser.
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
  return (
    <div className="bg-card rounded-md border border-border p-4 space-y-4">
      <HostedWebHttpsNote />
      <div>
        <p className="text-sm font-medium">OAuth (recommended)</p>
        <p className="mt-1 text-sm text-muted-foreground">
          Go to <span className="font-medium">Customize &rarr; Connectors</span>, click{" "}
          <span className="font-medium">Add custom connector</span>, then enter the URL
          below. You can optionally open <span className="font-medium">Advanced settings</span>{" "}
          to supply an OAuth Client ID and Secret; otherwise leave them blank and finish
          with <span className="font-medium">Add</span>.
        </p>
      </div>
      <CodeBlock code={serverUrl} label="Server URL" />
      <p className="text-sm text-muted-foreground">
        Claude Desktop and claude.ai support OAuth auto-discovery, so you will be
        prompted to authenticate in your browser when connecting. Free-plan accounts are
        limited to one custom connector.
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

function CursorCliTab({ serverUrl }: { serverUrl: string }) {
  const jsonConfig = JSON.stringify(
    {
      mcpServers: {
        nram: {
          url: serverUrl,
        },
      },
    },
    null,
    2,
  );

  return (
    <div className="bg-card rounded-md border border-border p-4 space-y-4">
      <div>
        <p className="text-sm font-medium">Add to ~/.cursor/mcp.json</p>
        <p className="mt-1 text-sm text-muted-foreground">
          Add the nram MCP server to your global <span className="font-mono text-xs">~/.cursor/mcp.json</span> or
          project-level <span className="font-mono text-xs">.cursor/mcp.json</span>. If you already have
          other MCP servers configured, merge the nram entry into your
          existing <span className="font-mono text-xs">mcpServers</span> object.
        </p>
      </div>
      <CodeBlock code={jsonConfig} />
      <p className="text-sm text-muted-foreground">
        Authenticate with <span className="font-mono text-xs">cursor-agent mcp login nram</span>, then
        confirm the server and its tools are visible with{" "}
        <span className="font-mono text-xs">cursor-agent mcp list</span>. If your server is not
        OAuth-capable, add a static token instead by giving the entry a{" "}
        <span className="font-mono text-xs">headers</span> object with{" "}
        <span className="font-mono text-xs">{'"Authorization": "Bearer <your-key>"'}</span> (or a{" "}
        <span className="font-mono text-xs">{"${env:NRAM_API_KEY}"}</span> reference).
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
      <HostedWebHttpsNote />
      <div>
        <p className="text-sm font-medium">1. Enable Developer mode</p>
        <p className="mt-1 text-sm text-muted-foreground">
          Custom MCP servers that are not registered ChatGPT apps are only available
          through Developer mode, which you must turn on first. Go to{" "}
          <span className="font-medium">Settings &rarr; Apps &amp; Connectors &rarr; Advanced settings</span>,
          enable <span className="font-medium">Developer mode</span>, and accept the
          warning about running third-party code. Developer mode is available on the
          Plus, Pro, Business/Team, Enterprise, and Edu plans on the web; it is not
          available on Free.
        </p>
      </div>
      <div className="border-t border-border pt-4">
        <p className="text-sm font-medium">2. Add a custom connector</p>
        <p className="mt-1 text-sm text-muted-foreground">
          With Developer mode on, add a custom connector pointing at the HTTPS URL
          below and authenticate via the OAuth flow when prompted.
        </p>
      </div>
      <CodeBlock code={url} label="Server URL (HTTPS required)" />
      <p className="text-sm text-muted-foreground">
        ChatGPT uses RFC 9728 OAuth discovery, so an OAuth-capable connection
        negotiates a token automatically once the server is reachable over HTTPS.
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
// System Prompt Snippet content
// ---------------------------------------------------------------------------

// Per-tab placement guidance for the System Prompt Snippet section. Each client
// reads the instructions from a different place (a rules file vs. a preferences
// UI), so the prose and the snippet flavor vary while the markup is shared. The
// Record is keyed by ToolTab, so adding a tab forces adding its guidance here.
const SYSTEM_PROMPT_SECTIONS: Record<ToolTab, SystemPromptSpec> = {
  "claude-code": {
    description:
      "Add this snippet to your project's CLAUDE.md file to guide Claude on how to use nram effectively.",
    cards: [
      {
        label: "For CLAUDE.md",
        description:
          "This provides detailed guidance for proactive memory usage. Place it in your project's CLAUDE.md or your global ~/.claude/CLAUDE.md file.",
        snippet: "claude",
      },
    ],
  },
  "claude-desktop": {
    description:
      "Claude Desktop and Claude.ai do not read a CLAUDE.md file. Paste these instructions into your Claude preferences so they apply to your chats.",
    cards: [
      {
        label: "For Claude personal preferences",
        description: (
          <>
            Open <span className="font-medium">Settings &rarr; Profile</span> and paste this into{" "}
            <span className="font-medium">&ldquo;What preferences should Claude consider in responses?&rdquo;</span>{" "}
            to apply it to every conversation, or add it to a specific{" "}
            <span className="font-medium">Project&apos;s instructions</span> to scope it to one project.
          </>
        ),
        snippet: "claude",
      },
    ],
  },
  cursor: {
    description:
      "Save this as a Cursor rule so Cursor knows how to use nram in every chat.",
    cards: [
      {
        label: "Save as a Cursor rule file",
        description: (
          <>
            In your project folder, create a file at{" "}
            <span className="font-mono text-xs">.cursor/rules/nram-memory.mdc</span> and paste in
            everything below, exactly as shown. Create the{" "}
            <span className="font-mono text-xs">.cursor/rules</span> folder first if it does not
            exist. Cursor then applies it automatically in every chat.
          </>
        ),
        snippet: "claude",
        filePrefix: CURSOR_RULE_HEADER,
      },
    ],
  },
  "cursor-cli": {
    description:
      "Save this as a Cursor rule so the Cursor CLI uses nram in every session.",
    cards: [
      {
        label: "Save as a Cursor rule file",
        description: (
          <>
            In your home folder, create a file at{" "}
            <span className="font-mono text-xs">~/.cursor/rules/nram-memory.mdc</span> and paste in
            everything below, exactly as shown. Create the{" "}
            <span className="font-mono text-xs">~/.cursor/rules</span> folder first if it does not
            exist. The Cursor CLI then applies it automatically in every session.
          </>
        ),
        snippet: "claude",
        filePrefix: CURSOR_RULE_HEADER,
      },
    ],
  },
  codex: {
    description:
      "Add this snippet to your project's AGENTS.md file to guide Codex on how to use nram effectively.",
    cards: [
      {
        label: "For AGENTS.md",
        description:
          "Place this in your project's AGENTS.md or your global ~/.codex/AGENTS.md file.",
        snippet: "agents",
      },
    ],
  },
  opencode: {
    description:
      "Add this snippet to your project's AGENTS.md file to guide OpenCode on how to use nram effectively.",
    cards: [
      {
        label: "For AGENTS.md",
        description:
          "Place this in your project's AGENTS.md or your global ~/.config/opencode/AGENTS.md file. OpenCode also reads CLAUDE.md as a fallback.",
        snippet: "agents",
      },
    ],
  },
  chatgpt: {
    description:
      "ChatGPT does not read a rules file. Paste these instructions into your ChatGPT personalization settings so they apply to your chats.",
    cards: [
      {
        label: "For ChatGPT custom instructions",
        description: (
          <>
            Open{" "}
            <span className="font-medium">Settings &rarr; Personalization &rarr; Custom instructions</span>{" "}
            and paste this in; it fits within the 1,500-character limit. To scope it to one project
            instead, add it to a <span className="font-medium">Project&apos;s instructions</span>.
          </>
        ),
        snippet: "condensed",
      },
    ],
  },
  "api-key": {
    description:
      "If your MCP client supports a system prompt or rules file, add the appropriate snippet to instruct the model on how to use nram.",
    cards: [
      { label: "For CLAUDE.md", description: "Detailed guidance for Claude-based tools.", snippet: "claude" },
      { label: "Condensed (length-limited tools)", description: "Shorter version for clients that cap the field length, like ChatGPT Custom instructions.", snippet: "condensed" },
      { label: "For AGENTS.md", description: "For OpenAI Codex-based tools.", snippet: "agents" },
    ],
  },
};

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

function MCPConfigGenerator() {
  const [serverUrl, setServerUrl] = useState(() => window.location.origin + "/mcp");
  const [apiKey, setApiKey] = useState("");
  const [activeTab, setActiveTab] = useState<ToolTab>("claude-code");

  // Snippets come from the backend (GET /instructions), the single source of
  // truth. They are paste-in text for client-side prompt files and do not
  // condition on live server state; the MCP server's connection-time
  // instructions deliver any provider-conditional guidance separately.
  const [snippets, setSnippets] = useState<Snippets | null>(null);
  const [snippetError, setSnippetError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    // claude and agents return byte-identical bodies server-side, so fetch
    // claude once and alias it; condensed is the only other distinct flavor
    // (the length-limited copy used by ChatGPT's Custom instructions).
    Promise.all([getInstructions("claude"), getInstructions("condensed")])
      .then(([claude, condensed]) => {
        if (!cancelled) setSnippets({ claude, condensed, agents: claude });
      })
      .catch((err) => {
        if (!cancelled) {
          setSnippetError(err instanceof Error ? err.message : "Failed to load instructions");
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const tabs: { key: ToolTab; label: string }[] = [
    { key: "claude-code", label: "Claude Code" },
    { key: "claude-desktop", label: "Claude Desktop / Claude.ai" },
    { key: "cursor", label: "Cursor IDE" },
    { key: "cursor-cli", label: "Cursor CLI" },
    { key: "codex", label: "Codex" },
    { key: "opencode", label: "OpenCode" },
    { key: "chatgpt", label: "ChatGPT" },
    { key: "api-key", label: "API Key Fallback" },
  ];

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h1 className="font-display text-3xl text-foreground">MCP Config Generator</h1>
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
        {activeTab === "cursor-cli" && <CursorCliTab serverUrl={serverUrl} />}
        {activeTab === "codex" && <CodexTab serverUrl={serverUrl} />}
        {activeTab === "opencode" && <OpenCodeTab serverUrl={serverUrl} />}
        {activeTab === "chatgpt" && <ChatGPTTab serverUrl={serverUrl} />}
        {activeTab === "api-key" && (
          <ApiKeyTab serverUrl={serverUrl} apiKey={apiKey} setApiKey={setApiKey} />
        )}
      </div>

      {/* System prompts: per-tab placement guidance, driven by
          SYSTEM_PROMPT_SECTIONS so each client is data, not copy-pasted markup. */}
      <SystemPromptSection
        spec={SYSTEM_PROMPT_SECTIONS[activeTab]}
        snippets={snippets}
        error={snippetError}
      />
    </div>
  );
}

export default MCPConfigGenerator;
