import React from "react";
import { Routes, Route, NavLink, Navigate, useLocation, useNavigate } from "react-router-dom";
import { useSetupStatus } from "./hooks/useApi";
import Dashboard from "./pages/Dashboard";
import SetupWizard from "./pages/SetupWizard";
import Login from "./pages/Login";
import MemoryBrowser from "./pages/MemoryBrowser";
import ProjectManagement from "./pages/ProjectManagement";
import OrganizationManagement from "./pages/OrganizationManagement";
import UserManagement from "./pages/UserManagement";
import ProviderConfiguration from "./pages/ProviderConfiguration";
import SettingsEditor from "./pages/SettingsEditor";
import DatabaseManagement from "./pages/DatabaseManagement";
import EnrichmentMonitor from "./pages/EnrichmentMonitor";
import GraphVisualization from "./pages/GraphVisualization";
import EntityBrowser from "./pages/EntityBrowser";
import Analytics from "./pages/Analytics";
import BulkImport from "./pages/BulkImport";
import WebhookManagement from "./pages/WebhookManagement";
import OAuthClients from "./pages/OAuthClients";
import MCPConfigGenerator from "./pages/MCPConfigGenerator";
import ExtractionPromptEditor from "./pages/ExtractionPromptEditor";

// ---------------------------------------------------------------------------
// Error Boundary
// ---------------------------------------------------------------------------

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

class ErrorBoundary extends React.Component<
  { children: React.ReactNode },
  ErrorBoundaryState
> {
  constructor(props: { children: React.ReactNode }) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex flex-col items-center justify-center py-16 gap-4">
          <div className="rounded-lg border border-red-300 bg-red-50 p-6 max-w-lg dark:border-red-800 dark:bg-red-900/30">
            <h2 className="text-lg font-semibold text-red-800 dark:text-red-200 mb-2">
              Something went wrong
            </h2>
            <p className="text-sm text-red-700 dark:text-red-300 mb-4">
              {this.state.error?.message || "An unexpected error occurred."}
            </p>
            <button
              type="button"
              onClick={() => {
                this.setState({ hasError: false, error: null });
                window.location.href = "/";
              }}
              className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90"
            >
              Go to Dashboard
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface NavItem {
  path: string;
  label: string;
  section: string;
}

const navItems: NavItem[] = [
  { path: "/", label: "Dashboard", section: "Overview" },
  { path: "/memories", label: "Memory Browser", section: "Data" },
  { path: "/entities", label: "Entity Browser", section: "Data" },
  { path: "/graph", label: "Graph Visualization", section: "Data" },
  { path: "/projects", label: "Projects", section: "Management" },
  { path: "/organizations", label: "Organizations", section: "Management" },
  { path: "/users", label: "Users", section: "Management" },
  { path: "/providers", label: "Providers", section: "Configuration" },
  { path: "/settings", label: "Settings", section: "Configuration" },
  { path: "/extraction-prompts", label: "Extraction Prompts", section: "Configuration" },
  { path: "/webhooks", label: "Webhooks", section: "Configuration" },
  { path: "/oauth", label: "OAuth Clients", section: "Configuration" },
  { path: "/mcp-config", label: "MCP Config", section: "Configuration" },
  { path: "/database", label: "Database", section: "System" },
  { path: "/enrichment", label: "Enrichment Queue", section: "System" },
  { path: "/analytics", label: "Analytics", section: "System" },
  { path: "/import", label: "Bulk Import", section: "System" },
];

function groupBySection(items: NavItem[]): Record<string, NavItem[]> {
  const groups: Record<string, NavItem[]> = {};
  for (const item of items) {
    if (!groups[item.section]) {
      groups[item.section] = [];
    }
    groups[item.section].push(item);
  }
  return groups;
}

function SetupGuard({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  const { data: status, isLoading } = useSetupStatus();

  // While loading, show a minimal loading indicator
  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="text-sm text-muted-foreground">Loading...</div>
      </div>
    );
  }

  // If setup is not complete and we are not already on /setup, redirect.
  // But if a token exists (just completed setup), skip this redirect — the
  // status query may not have refreshed yet.
  const hasToken = !!localStorage.getItem("nram_token");
  if (status && !status.setup_complete && !hasToken && location.pathname !== "/setup") {
    return <Navigate to="/setup" replace />;
  }

  return <>{children}</>;
}

function AuthGuard({ children }: { children: React.ReactNode }) {
  const token = localStorage.getItem("nram_token");
  if (!token) {
    return <Navigate to="/login" replace />;
  }
  return <>{children}</>;
}

function AppLayout() {
  const { data: setupStatus } = useSetupStatus();
  const navigate = useNavigate();

  const isSQLite = setupStatus?.backend === "sqlite";

  const filteredItems = navItems.filter((item) => {
    if (isSQLite && (item.path === "/enrichment" || item.path === "/extraction-prompts")) {
      return false;
    }
    return true;
  });

  const sections = groupBySection(filteredItems);

  function handleLogout() {
    localStorage.removeItem("nram_token");
    navigate("/login");
  }

  return (
    <div className="flex h-screen">
      <aside className="w-60 shrink-0 border-r border-border bg-card overflow-y-auto flex flex-col">
        <div className="px-4 py-5">
          <h1 className="text-lg font-semibold tracking-tight">nram</h1>
          <p className="text-xs text-muted-foreground">Admin Console</p>
        </div>
        <nav className="px-2 pb-4 flex-1">
          {Object.entries(sections).map(([section, items]) => (
            <div key={section} className="mb-4">
              <h2 className="px-2 mb-1 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                {section}
              </h2>
              <ul className="space-y-0.5">
                {items.map((item) => (
                  <li key={item.path}>
                    <NavLink
                      to={item.path}
                      end={item.path === "/"}
                      className={({ isActive }) =>
                        `block rounded-md px-2 py-1.5 text-sm transition-colors ${
                          isActive
                            ? "bg-accent text-accent-foreground font-medium"
                            : "text-muted-foreground hover:bg-accent hover:text-accent-foreground"
                        }`
                      }
                    >
                      {item.label}
                      {isSQLite && item.path === "/providers" && (
                        <span className="ml-1.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground/60">
                          SQLite
                        </span>
                      )}
                    </NavLink>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </nav>
        <div className="border-t border-border px-2 py-3">
          <button
            type="button"
            onClick={handleLogout}
            className="block w-full rounded-md px-2 py-1.5 text-left text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            Logout
          </button>
        </div>
      </aside>
      <main className="flex-1 overflow-y-auto">
        <div className="p-6">
          <ErrorBoundary>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/memories" element={<MemoryBrowser />} />
              <Route path="/projects" element={<ProjectManagement />} />
              <Route path="/organizations" element={<OrganizationManagement />} />
              <Route path="/users" element={<UserManagement />} />
              <Route path="/providers" element={<ProviderConfiguration />} />
              <Route path="/settings" element={<SettingsEditor isSQLite={isSQLite} />} />
              <Route path="/extraction-prompts" element={<ExtractionPromptEditor isSQLite={isSQLite} />} />
              <Route path="/database" element={<DatabaseManagement />} />
              <Route path="/enrichment" element={<EnrichmentMonitor />} />
              <Route path="/graph" element={<GraphVisualization />} />
              <Route path="/entities" element={<EntityBrowser />} />
              <Route path="/analytics" element={<Analytics />} />
              <Route path="/import" element={<BulkImport />} />
              <Route path="/webhooks" element={<WebhookManagement />} />
              <Route path="/oauth" element={<OAuthClients />} />
              <Route path="/mcp-config" element={<MCPConfigGenerator />} />
            </Routes>
          </ErrorBoundary>
        </div>
      </main>
    </div>
  );
}

function App() {
  return (
    <SetupGuard>
      <Routes>
        <Route path="/setup" element={<SetupWizard />} />
        <Route path="/login" element={<Login />} />
        <Route path="/*" element={<AuthGuard><AppLayout /></AuthGuard>} />
      </Routes>
    </SetupGuard>
  );
}

export default App;
