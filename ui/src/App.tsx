import React, { useState, useEffect } from "react";
import { Routes, Route, NavLink, Navigate, useLocation } from "react-router-dom";
import { useSetupStatus } from "./hooks/useApi";
import { AuthProvider, useAuth } from "./context/AuthContext";
import { ProjectProvider } from "./context/ProjectContext";
import RequireRole from "./components/RequireRole";
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
import IdPConfiguration from "./pages/IdPConfiguration";
import MCPConfigGenerator from "./pages/MCPConfigGenerator";
import ExtractionPromptEditor from "./pages/ExtractionPromptEditor";
import MyAccount from "./pages/MyAccount";

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
  minRole?: string;
  writeOnly?: boolean;
}

const navItems: NavItem[] = [
  { path: "/", label: "Dashboard", section: "Overview" },
  { path: "/memories", label: "Memory Browser", section: "Data" },
  { path: "/entities", label: "Entity Browser", section: "Data" },
  { path: "/graph", label: "Graph Visualization", section: "Data" },
  { path: "/projects", label: "Projects", section: "Management" },
  { path: "/organizations", label: "Organizations", section: "Management", minRole: "administrator" },
  { path: "/users", label: "Users", section: "Management", minRole: "org_owner" },
  { path: "/providers", label: "Providers", section: "Configuration", minRole: "administrator" },
  { path: "/settings", label: "Settings", section: "Configuration", minRole: "administrator" },
  { path: "/extraction-prompts", label: "Extraction Prompts", section: "Configuration", minRole: "administrator" },
  { path: "/webhooks", label: "Webhooks", section: "Configuration", minRole: "administrator" },
  { path: "/oauth", label: "OAuth Clients", section: "Configuration", minRole: "administrator" },
  { path: "/idp", label: "Identity Providers", section: "Configuration", minRole: "org_owner" },
  { path: "/mcp-config", label: "MCP Config", section: "Configuration" },
  { path: "/database", label: "Database", section: "System", minRole: "administrator" },
  { path: "/enrichment", label: "Enrichment Queue", section: "System", minRole: "administrator" },
  { path: "/analytics", label: "Analytics", section: "System" },
  { path: "/import", label: "Bulk Import", section: "System", writeOnly: true },
  { path: "/account", label: "My Account", section: "Account" },
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

  // If setup is not complete and we are not already on /setup, clear any
  // stale auth state and redirect to the setup wizard.
  if (status && !status.setup_complete && location.pathname !== "/setup") {
    localStorage.removeItem("nram_token");
    localStorage.removeItem("nram_user");
    return <Navigate to="/setup" replace />;
  }

  return <>{children}</>;
}

function AuthGuard({ children }: { children: React.ReactNode }) {
  const { user } = useAuth();
  if (!user) {
    return <Navigate to="/login" replace />;
  }
  return <>{children}</>;
}

function AppLayout() {
  const auth = useAuth();
  const location = useLocation();
  const [sidebarOpen, setSidebarOpen] = useState(false);

  // Close sidebar on route change (mobile)
  useEffect(() => {
    setSidebarOpen(false);
  }, [location.pathname]);

  const filteredItems = navItems.filter((item) => {
    if (item.minRole && !auth.hasMinRole(item.minRole)) {
      return false;
    }
    if (item.writeOnly && !auth.canWrite) {
      return false;
    }
    return true;
  });

  const sections = groupBySection(filteredItems);

  function handleLogout() {
    auth.logout();
  }

  return (
    <div className="flex h-screen">
      {/* Mobile header bar */}
      <div className="fixed top-0 left-0 right-0 z-40 flex items-center border-b border-border bg-card px-4 py-3 md:hidden">
        <button
          type="button"
          onClick={() => setSidebarOpen(true)}
          className="mr-3 rounded-md p-1.5 text-muted-foreground hover:bg-accent hover:text-accent-foreground"
          aria-label="Open navigation"
        >
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <line x1="3" y1="6" x2="21" y2="6" />
            <line x1="3" y1="12" x2="21" y2="12" />
            <line x1="3" y1="18" x2="21" y2="18" />
          </svg>
        </button>
        <h1 className="text-lg font-semibold tracking-tight">nram</h1>
      </div>

      {/* Backdrop overlay (mobile) */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/50 md:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside
        className={`fixed inset-y-0 left-0 z-50 w-60 border-r border-border bg-card overflow-y-auto flex flex-col transform transition-transform duration-200 ease-in-out md:static md:translate-x-0 md:shrink-0 ${
          sidebarOpen ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        <div className="flex items-center justify-between px-4 py-5">
          <div>
            <h1 className="text-lg font-semibold tracking-tight">nram</h1>
            <p className="text-xs text-muted-foreground">
              {auth.isAdmin ? "Admin Console" : "Console"}
            </p>
          </div>
          <button
            type="button"
            onClick={() => setSidebarOpen(false)}
            className="rounded-md p-1.5 text-muted-foreground hover:bg-accent hover:text-accent-foreground md:hidden"
            aria-label="Close navigation"
          >
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
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
                        `block rounded-md px-2 py-2.5 md:py-1.5 text-sm transition-colors ${
                          isActive
                            ? "bg-accent text-accent-foreground font-medium"
                            : "text-muted-foreground hover:bg-accent hover:text-accent-foreground"
                        }`
                      }
                    >
                      {item.label}
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
            className="block w-full rounded-md px-2 py-2.5 md:py-1.5 text-left text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            Logout
          </button>
        </div>
      </aside>
      <main className="flex-1 overflow-y-auto pt-14 md:pt-0">
        <div className="p-4 sm:p-6">
          <ErrorBoundary>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/memories" element={<MemoryBrowser />} />
              <Route path="/projects" element={<ProjectManagement />} />
              <Route path="/organizations" element={<RequireRole minRole="administrator"><OrganizationManagement /></RequireRole>} />
              <Route path="/users" element={<RequireRole minRole="org_owner"><UserManagement /></RequireRole>} />
              <Route path="/providers" element={<RequireRole minRole="administrator"><ProviderConfiguration /></RequireRole>} />
              <Route path="/settings" element={<RequireRole minRole="administrator"><SettingsEditor /></RequireRole>} />
              <Route path="/extraction-prompts" element={<RequireRole minRole="administrator"><ExtractionPromptEditor /></RequireRole>} />
              <Route path="/database" element={<RequireRole minRole="administrator"><DatabaseManagement /></RequireRole>} />
              <Route path="/enrichment" element={<RequireRole minRole="administrator"><EnrichmentMonitor /></RequireRole>} />
              <Route path="/graph" element={<GraphVisualization />} />
              <Route path="/entities" element={<EntityBrowser />} />
              <Route path="/analytics" element={<Analytics />} />
              <Route path="/import" element={<BulkImport />} />
              <Route path="/webhooks" element={<RequireRole minRole="administrator"><WebhookManagement /></RequireRole>} />
              <Route path="/oauth" element={<RequireRole minRole="administrator"><OAuthClients /></RequireRole>} />
              <Route path="/idp" element={<RequireRole minRole="org_owner"><IdPConfiguration /></RequireRole>} />
              <Route path="/mcp-config" element={<MCPConfigGenerator />} />
              <Route path="/account" element={<MyAccount />} />
            </Routes>
          </ErrorBoundary>
        </div>
      </main>
    </div>
  );
}

function App() {
  return (
    <AuthProvider>
      <ProjectProvider>
        <SetupGuard>
          <Routes>
            <Route path="/setup" element={<SetupWizard />} />
            <Route path="/login" element={<Login />} />
            <Route path="/*" element={<AuthGuard><AppLayout /></AuthGuard>} />
          </Routes>
        </SetupGuard>
      </ProjectProvider>
    </AuthProvider>
  );
}

export default App;
