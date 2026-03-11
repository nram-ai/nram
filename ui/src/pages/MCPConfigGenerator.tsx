import { useState, useCallback } from "react";
import { useProjects } from "../hooks/useApi";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type ToolTab = "claude-code" | "claude-desktop" | "cursor";

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
// Snippet generators
// ---------------------------------------------------------------------------

function generateClaudeCodeSnippet(serverUrl: string, apiKey: string): string {
  const key = apiKey || "YOUR_API_KEY";
  return `claude mcp add nram --transport http ${serverUrl}/mcp --header "Authorization: Bearer ${key}"`;
}

function generateClaudeDesktopSnippet(serverUrl: string, apiKey: string): string {
  const key = apiKey || "YOUR_API_KEY";
  const config = {
    mcpServers: {
      nram: {
        url: `${serverUrl}/mcp`,
        headers: {
          Authorization: `Bearer ${key}`,
        },
      },
    },
  };
  return JSON.stringify(config, null, 2);
}

function generateCursorSnippet(serverUrl: string, apiKey: string): string {
  const key = apiKey || "YOUR_API_KEY";
  const config = {
    mcpServers: {
      nram: {
        url: `${serverUrl}/mcp`,
        headers: {
          Authorization: `Bearer ${key}`,
        },
      },
    },
  };
  return JSON.stringify(config, null, 2);
}

const CLAUDE_MD_SNIPPET = `## Memory (nram)

You have access to persistent memory via nram. Use it proactively:

**When to store:** After completing a task, store key decisions, architecture
choices, configuration details, and anything you'd want to remember next session.
Store facts, not conversations. Be specific: "API uses JWT RS256 with 1h expiry"
not "we discussed auth."

**When to recall:** At the start of every task, recall memories relevant to the
current project and topic. Before making architecture decisions, check if prior
decisions exist. Before asking the user something, check if the answer is in memory.

**Tags:** Use consistent tags for your domain: architecture, config, decision,
preference, bug, workaround, dependency, deployment.`;

const CURSORRULES_SNIPPET = `# Memory
Use nram memory tools at the start of each task to recall prior context.
After completing work, store key decisions and technical details as memories.
Tag memories consistently: architecture, config, decision, preference.`;

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
// Main component
// ---------------------------------------------------------------------------

function MCPConfigGenerator() {
  const [serverUrl, setServerUrl] = useState(() => window.location.origin);
  const [apiKey, setApiKey] = useState("");
  const [selectedProjectId, setSelectedProjectId] = useState("");
  const [activeTab, setActiveTab] = useState<ToolTab>("claude-code");

  const { data: projects } = useProjects();

  const selectedProject = projects?.find((p) => p.id === selectedProjectId);

  const tabs: { key: ToolTab; label: string }[] = [
    { key: "claude-code", label: "Claude Code" },
    { key: "claude-desktop", label: "Claude Desktop" },
    { key: "cursor", label: "Cursor" },
  ];

  const snippetMap: Record<ToolTab, { code: string; filename: string; description: string }> = {
    "claude-code": {
      code: generateClaudeCodeSnippet(serverUrl, apiKey),
      filename: "Terminal command",
      description: "Run this command in your terminal to register nram as an MCP server in Claude Code.",
    },
    "claude-desktop": {
      code: generateClaudeDesktopSnippet(serverUrl, apiKey),
      filename: "claude_desktop_config.json",
      description:
        "Add this to your Claude Desktop configuration file (claude_desktop_config.json). If you already have other MCP servers configured, merge the nram entry into your existing mcpServers object.",
    },
    cursor: {
      code: generateCursorSnippet(serverUrl, apiKey),
      filename: ".cursor/mcp.json",
      description:
        "Add this to the .cursor/mcp.json file in your project root. If you already have other MCP servers configured, merge the nram entry into your existing mcpServers object.",
    },
  };

  const activeSnippet = snippetMap[activeTab];

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">MCP Config Generator</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Generate copy-pasteable MCP server configuration snippets for your preferred AI tool.
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
            placeholder="http://localhost:8674"
            className="w-full bg-background border border-border rounded-md px-3 py-1.5 text-sm"
          />
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
            placeholder="Paste your API key here"
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

        <div className="space-y-1">
          <label htmlFor="project" className="text-sm text-muted-foreground">
            Project (optional)
          </label>
          <select
            id="project"
            value={selectedProjectId}
            onChange={(e) => setSelectedProjectId(e.target.value)}
            className="w-full bg-background border border-border rounded-md px-3 py-1.5 text-sm"
          >
            <option value="">All projects (no filter)</option>
            {projects?.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name} ({p.slug})
              </option>
            ))}
          </select>
          <p className="text-xs text-muted-foreground">
            Selecting a project is for reference only. The MCP server handles project scoping
            via the memory tools&apos; project parameter.
          </p>
        </div>

        {selectedProject && (
          <div className="bg-muted rounded-md p-3 text-sm">
            <span className="font-medium">Selected project:</span>{" "}
            <span className="font-mono">{selectedProject.slug}</span>
            {selectedProject.path && (
              <>
                {" "}&mdash; path: <span className="font-mono">{selectedProject.path}</span>
              </>
            )}
          </div>
        )}
      </div>

      {/* Snippet tabs */}
      <div className="space-y-4">
        <h2 className="text-sm font-medium">Tool Configuration Snippets</h2>

        <div className="flex gap-2">
          {tabs.map((tab) => (
            <TabButton
              key={tab.key}
              label={tab.label}
              active={activeTab === tab.key}
              onClick={() => setActiveTab(tab.key)}
            />
          ))}
        </div>

        <div className="bg-card rounded-md border border-border p-4 space-y-3">
          <div className="flex items-center justify-between">
            <p className="text-sm font-medium font-mono">{activeSnippet.filename}</p>
          </div>
          <p className="text-sm text-muted-foreground">{activeSnippet.description}</p>
          <CodeBlock code={activeSnippet.code} />
        </div>
      </div>

      {/* System prompts */}
      <div className="space-y-4">
        <div>
          <h2 className="text-sm font-medium">System Prompt Snippets</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Add these snippets to your project&apos;s system prompt configuration to guide your AI
            tool on how to use nram effectively.
          </p>
        </div>

        <div className="bg-card rounded-md border border-border p-4 space-y-4">
          <div className="space-y-1">
            <p className="text-sm font-medium">For CLAUDE.md</p>
            <p className="text-sm text-muted-foreground">
              Add this to your project&apos;s CLAUDE.md file to instruct Claude on when and how to
              use memory. This provides detailed guidance for proactive memory usage.
            </p>
          </div>
          <CodeBlock code={CLAUDE_MD_SNIPPET} />
        </div>

        <div className="bg-card rounded-md border border-border p-4 space-y-4">
          <div className="space-y-1">
            <p className="text-sm font-medium">For .cursorrules</p>
            <p className="text-sm text-muted-foreground">
              Add this to your project&apos;s .cursorrules file for Cursor. This is a condensed
              version of the memory instructions suitable for Cursor&apos;s rule format.
            </p>
          </div>
          <CodeBlock code={CURSORRULES_SNIPPET} />
        </div>
      </div>
    </div>
  );
}

export default MCPConfigGenerator;
