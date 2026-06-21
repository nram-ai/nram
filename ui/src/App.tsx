import React, { Suspense, useState, useEffect } from "react";
import { Routes, Route, NavLink, Navigate, useLocation } from "react-router-dom";
import type { IconDefinition } from "@fortawesome/fontawesome-svg-core";
import { useSetupStatus, useHealth } from "./hooks/useApi";
import { useEnrichmentAvailable, useMeCapabilities } from "./hooks/useEnrichmentAvailable";
import { AuthProvider, useAuth } from "./context/AuthContext";
import { ProjectProvider } from "./context/ProjectContext";
import { ThemeProvider } from "./context/ThemeContext";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faBars,
  faXmark,
  faSun,
  faMoon,
  faRightFromBracket,
  faSpinner,
  faGauge,
  faBrain,
  faCubes,
  faDiagramProject,
  faFolderTree,
  faBuilding,
  faUsers,
  faPlug,
  faSliders,
  faMessage,
  faComments,
  faSatelliteDish,
  faKey,
  faFingerprint,
  faShareNodes,
  faPuzzlePiece,
  faBookOpen,
  faDatabase,
  faListCheck,
  faScroll,
  faCloudMoon,
  faChartLine,
  faBolt,
  faFileLines,
  faFileImport,
  faUserAstronaut,
  faStar,
  faTableCellsLarge,
  faPeopleGroup,
  faGears,
  faServer,
  faUser,
} from "./lib/icons";
import { formatCommit } from "./lib/formatters";
import { useTheme } from "./context/ThemeContext";
import RequireRole from "./components/RequireRole";
import { NeuralNetwork } from "./components/NeuralNetwork/NeuralNetwork";
import { Logo } from "./components/Logo";
import { EmptyState } from "./components/EmptyState";
import Dashboard from "./pages/Dashboard";
// Dashboard, Login, and SetupWizard stay eager: they are the only pages an
// unauthenticated/cold-start user can land on, and a Suspense flash on the
// auth path is worse than the few KB they add to the entry chunk.
import Login from "./pages/Login";
import SetupWizard from "./pages/SetupWizard";

// Pre-auth routes served by the SPA fallback when the OAuth client
// redirects the browser to /authorize or a recipient opens a share
// magic-link at /share/accept. Lazy-loaded so they do not weigh on the
// admin-UI entry chunk.
const Authorize = React.lazy(() => import("./pages/Authorize"));
const ShareAccept = React.lazy(() => import("./pages/ShareAccept"));

const MemoryBrowser = React.lazy(() => import("./pages/MemoryBrowser"));
const Ask = React.lazy(() => import("./pages/Ask"));
const ProceduralMemory = React.lazy(() => import("./pages/ProceduralMemory"));
const ProjectManagement = React.lazy(() => import("./pages/ProjectManagement"));
const OrganizationManagement = React.lazy(() => import("./pages/OrganizationManagement"));
const UserManagement = React.lazy(() => import("./pages/UserManagement"));
const ProviderConfiguration = React.lazy(() => import("./pages/ProviderConfiguration"));
const SettingsEditor = React.lazy(() => import("./pages/SettingsEditor"));
const DatabaseManagement = React.lazy(() => import("./pages/DatabaseManagement"));
const EnrichmentMonitor = React.lazy(() => import("./pages/EnrichmentMonitor"));
const GraphVisualization = React.lazy(() => import("./pages/GraphVisualization"));
const EntityBrowser = React.lazy(() => import("./pages/EntityBrowser"));
const Analytics = React.lazy(() => import("./pages/Analytics"));
const BulkImport = React.lazy(() => import("./pages/BulkImport"));
const WebhookManagement = React.lazy(() => import("./pages/WebhookManagement"));
const OAuthClients = React.lazy(() => import("./pages/OAuthClients"));
const IdPConfiguration = React.lazy(() => import("./pages/IdPConfiguration"));
const MCPConfigGenerator = React.lazy(() => import("./pages/MCPConfigGenerator"));
const Shares = React.lazy(() => import("./pages/Shares"));
const PromptTemplates = React.lazy(() => import("./pages/PromptTemplates"));
const DreamingMonitor = React.lazy(() => import("./pages/DreamingMonitor"));
const Metrics = React.lazy(() => import("./pages/Metrics"));
const Logs = React.lazy(() => import("./pages/Logs"));
const MyAccount = React.lazy(() => import("./pages/MyAccount"));

function RouteFallback({ fullScreen = false }: { fullScreen?: boolean }) {
  const sizing = fullScreen ? "h-screen" : "h-full min-h-[40vh]";
  return (
    <div className={`flex ${sizing} items-center justify-center text-sm text-muted-foreground`}>
      <FontAwesomeIcon icon={faSpinner} spin className="h-5 w-5 text-primary/70" />
    </div>
  );
}

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
        <EmptyState
          icon={faSatelliteDish}
          title="Lost in the network."
          body={this.state.error?.message || "An unexpected error occurred."}
          action={
            <button
              type="button"
              onClick={() => {
                this.setState({ hasError: false, error: null });
                window.location.href = "/";
              }}
              className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground shadow-sm transition-colors hover:bg-primary/90 active:scale-[0.98]"
            >
              Go to Dashboard
            </button>
          }
        />
      );
    }
    return this.props.children;
  }
}

// ---------------------------------------------------------------------------
// Types and nav model
// ---------------------------------------------------------------------------

interface NavItem {
  path: string;
  label: string;
  section: string;
  icon: IconDefinition;
  minRole?: string;
  writeOnly?: boolean;
  requiresEnrichment?: boolean;
  // requiresAsk hides the entry unless the ask feature flag (ask.enabled) is on.
  requiresAsk?: boolean;
  // External links to a server-served (non-SPA) route, opened in a new tab.
  external?: boolean;
}

const navItems: NavItem[] = [
  { path: "/", label: "Dashboard", section: "Overview", icon: faGauge },
  { path: "/memories", label: "Memory Browser", section: "Data", icon: faBrain },
  { path: "/ask", label: "Ask", section: "Data", icon: faComments, requiresAsk: true },
  { path: "/procedural", label: "Procedural Memory", section: "Data", icon: faScroll },
  { path: "/entities", label: "Entity Browser", section: "Data", icon: faCubes },
  { path: "/graph", label: "Graph Visualization", section: "Data", icon: faDiagramProject },
  { path: "/projects", label: "Projects", section: "Management", icon: faFolderTree },
  { path: "/organizations", label: "Organizations", section: "Management", icon: faBuilding, minRole: "administrator" },
  { path: "/users", label: "Users", section: "Management", icon: faUsers, minRole: "org_owner" },
  { path: "/providers", label: "Providers", section: "Configuration", icon: faPlug, minRole: "administrator" },
  { path: "/settings", label: "Settings", section: "Configuration", icon: faSliders, minRole: "administrator" },
  { path: "/prompt-templates", label: "Prompt Templates", section: "Configuration", icon: faMessage, minRole: "administrator", requiresEnrichment: true },
  { path: "/webhooks", label: "Webhooks", section: "Configuration", icon: faSatelliteDish, minRole: "administrator" },
  { path: "/oauth", label: "OAuth Clients", section: "Configuration", icon: faKey },
  { path: "/idp", label: "Identity Providers", section: "Configuration", icon: faFingerprint, minRole: "org_owner" },
  { path: "/shares", label: "Shares", section: "Configuration", icon: faShareNodes },
  { path: "/mcp-config", label: "MCP Config", section: "Configuration", icon: faPuzzlePiece },
  { path: "/docs", label: "API Docs", section: "Configuration", icon: faBookOpen, external: true },
  { path: "/database", label: "Database", section: "System", icon: faDatabase, minRole: "administrator" },
  { path: "/enrichment", label: "Enrichment Queue", section: "System", icon: faListCheck, requiresEnrichment: true },
  { path: "/dreaming", label: "Dreaming", section: "System", icon: faCloudMoon, requiresEnrichment: true },
  { path: "/analytics", label: "Analytics", section: "System", icon: faChartLine },
  { path: "/observability", label: "Metrics", section: "System", icon: faBolt, minRole: "administrator" },
  { path: "/logs", label: "Logs", section: "System", icon: faFileLines, minRole: "administrator" },
  { path: "/import", label: "Bulk Import", section: "System", icon: faFileImport, writeOnly: true },
  { path: "/account", label: "My Account", section: "Account", icon: faUserAstronaut },
];

const SECTION_ICONS: Record<string, IconDefinition> = {
  Overview: faStar,
  Data: faTableCellsLarge,
  Management: faPeopleGroup,
  Configuration: faGears,
  System: faServer,
  Account: faUser,
};

// Routes where the neural-network backdrop fades back so foreground
// data-visualizations own the visual budget.
const DIM_BACKDROP_ROUTES = ["/graph", "/entities", "/analytics", "/observability"];

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

  if (isLoading) {
    return <RouteFallback fullScreen />;
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
  const { theme, toggleTheme } = useTheme();
  const location = useLocation();
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const { available: enrichmentAvailable } = useEnrichmentAvailable();
  const { data: capabilities } = useMeCapabilities();
  const askEnabled = capabilities?.ask_enabled === true;
  const { data: health } = useHealth();
  const buildCommit = health ? formatCommit(health.build) : null;

  // Close sidebar on route change (mobile)
  useEffect(() => {
    setSidebarOpen(false);
  }, [location.pathname]);

  // Drop the neural-network backdrop opacity on data-heavy routes so the
  // foreground visualization is what reads.
  useEffect(() => {
    const dim = DIM_BACKDROP_ROUTES.some((r) => location.pathname.startsWith(r));
    if (dim) {
      document.documentElement.style.setProperty("--network-opacity", "0.06");
    } else {
      // Clear inline override so the value falls back to the :root/.dark
      // declaration in index.css.
      document.documentElement.style.removeProperty("--network-opacity");
    }
  }, [location.pathname]);

  const filteredItems = navItems.filter((item) => {
    if (item.minRole && !auth.hasMinRole(item.minRole)) {
      return false;
    }
    if (item.writeOnly && !auth.canWrite) {
      return false;
    }
    if (item.requiresEnrichment && !enrichmentAvailable) {
      return false;
    }
    if (item.requiresAsk && !askEnabled) {
      return false;
    }
    return true;
  });

  const sections = groupBySection(filteredItems);

  function handleLogout() {
    auth.logout();
  }

  return (
    <div className="app-shell flex h-screen">
      {/* Mobile header bar */}
      <div className="surface-elevated fixed top-0 left-0 right-0 z-40 flex items-center px-4 py-3 md:hidden">
        <button
          type="button"
          onClick={() => setSidebarOpen(true)}
          className="mr-3 rounded-md p-1.5 text-muted-foreground hover:bg-accent hover:text-accent-foreground"
          aria-label="Open navigation"
        >
          <FontAwesomeIcon icon={faBars} className="h-6 w-6" />
        </button>
        <Logo size="sm" />
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
        className={`surface-elevated fixed inset-y-0 left-0 z-50 w-60 overflow-y-auto flex flex-col transform transition-transform duration-200 ease-in-out md:static md:translate-x-0 md:shrink-0 ${
          sidebarOpen ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        <div className="flex items-center justify-between px-4 py-5">
          <div>
            <Logo size="sm" />
            <p className="mt-1 text-xs text-muted-foreground">
              Console
            </p>
          </div>
          <button
            type="button"
            onClick={() => setSidebarOpen(false)}
            className="rounded-md p-1.5 text-muted-foreground hover:bg-accent hover:text-accent-foreground md:hidden"
            aria-label="Close navigation"
          >
            <FontAwesomeIcon icon={faXmark} className="h-5 w-5" />
          </button>
        </div>
        <nav className="px-2 pb-4 flex-1">
          {Object.entries(sections).map(([section, items]) => (
            <div key={section} className="mb-4">
              <h2 className="px-2 mb-1 flex items-center gap-1.5 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                {SECTION_ICONS[section] && (
                  <FontAwesomeIcon
                    icon={SECTION_ICONS[section]}
                    className="h-3 w-3 opacity-60"
                  />
                )}
                {section}
              </h2>
              <ul className="space-y-0.5">
                {items.map((item) => {
                  const content = (
                    <>
                      <FontAwesomeIcon
                        icon={item.icon}
                        className="h-4 w-4 opacity-80"
                      />
                      <span>{item.label}</span>
                    </>
                  );
                  return (
                    <li key={item.path}>
                      {item.external ? (
                        // Server-served (non-SPA) route: a full-page anchor
                        // opened in a new tab rather than a client-side NavLink.
                        <a
                          href={item.path}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="relative flex items-center gap-2.5 rounded-md px-2 py-2.5 md:py-1.5 text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
                        >
                          {content}
                        </a>
                      ) : (
                        <NavLink
                          to={item.path}
                          end={item.path === "/"}
                          className={({ isActive }) =>
                            `relative flex items-center gap-2.5 rounded-md px-2 py-2.5 md:py-1.5 text-sm transition-colors ${
                              isActive
                                ? "bg-accent text-accent-foreground font-medium shadow-[0_0_24px_-12px_hsl(var(--ring))] before:absolute before:left-0 before:top-1.5 before:bottom-1.5 before:w-0.5 before:rounded-r before:bg-primary"
                                : "text-muted-foreground hover:bg-accent hover:text-accent-foreground"
                            }`
                          }
                        >
                          {content}
                        </NavLink>
                      )}
                    </li>
                  );
                })}
              </ul>
            </div>
          ))}
        </nav>
        <div className="border-t border-border/60 px-2 py-3 space-y-1">
          <button
            type="button"
            onClick={toggleTheme}
            className="flex w-full items-center gap-2 rounded-md px-2 py-2.5 md:py-1.5 text-left text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
            aria-label={theme === "dark" ? "Switch to light mode" : "Switch to dark mode"}
          >
            <FontAwesomeIcon
              icon={theme === "dark" ? faSun : faMoon}
              className="h-4 w-4"
            />
            <span>{theme === "dark" ? "Light mode" : "Dark mode"}</span>
          </button>
          <button
            type="button"
            onClick={handleLogout}
            className="flex w-full items-center gap-2 rounded-md px-2 py-2.5 md:py-1.5 text-left text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            <FontAwesomeIcon icon={faRightFromBracket} className="h-4 w-4" />
            <span>Logout</span>
          </button>
          {health && (
            <p
              className="px-2 pt-1 font-mono text-[11px] leading-tight text-muted-foreground/70"
              title={
                health.build.time
                  ? `Built ${health.build.time} · ${health.build.go}`
                  : health.build.go
              }
            >
              v{health.version}
              {buildCommit && ` · ${buildCommit}`}
            </p>
          )}
        </div>
      </aside>
      <main className="flex-1 overflow-y-auto pt-14 md:pt-0">
        <div className="p-4 sm:p-6">
          <ErrorBoundary>
            <Suspense fallback={<RouteFallback />}>
              <div key={location.pathname} className="route-enter">
                <Routes>
                  <Route path="/" element={<Dashboard />} />
                  <Route path="/memories" element={<MemoryBrowser />} />
                <Route path="/ask" element={<Ask />} />
                  <Route path="/procedural" element={<ProceduralMemory />} />
                  <Route path="/projects" element={<ProjectManagement />} />
                  <Route path="/organizations" element={<RequireRole minRole="administrator"><OrganizationManagement /></RequireRole>} />
                  <Route path="/users" element={<RequireRole minRole="org_owner"><UserManagement /></RequireRole>} />
                  <Route path="/providers" element={<RequireRole minRole="administrator"><ProviderConfiguration /></RequireRole>} />
                  <Route path="/settings" element={<RequireRole minRole="administrator"><SettingsEditor /></RequireRole>} />
                  <Route path="/prompt-templates" element={<RequireRole minRole="administrator"><PromptTemplates /></RequireRole>} />
                  <Route path="/extraction-prompts" element={<Navigate to="/prompt-templates" replace />} />
                  <Route path="/database" element={<RequireRole minRole="administrator"><DatabaseManagement /></RequireRole>} />
                  <Route path="/enrichment" element={<EnrichmentMonitor />} />
                  <Route path="/dreaming" element={<DreamingMonitor />} />
                  <Route path="/graph" element={<GraphVisualization />} />
                  <Route path="/entities" element={<EntityBrowser />} />
                  <Route path="/analytics" element={<Analytics />} />
                  <Route path="/observability" element={<RequireRole minRole="administrator"><Metrics /></RequireRole>} />
                  <Route path="/logs" element={<RequireRole minRole="administrator"><Logs /></RequireRole>} />
                  <Route path="/import" element={<BulkImport />} />
                  <Route path="/webhooks" element={<RequireRole minRole="administrator"><WebhookManagement /></RequireRole>} />
                  <Route path="/oauth" element={<OAuthClients />} />
                  <Route path="/idp" element={<RequireRole minRole="org_owner"><IdPConfiguration /></RequireRole>} />
                  <Route path="/mcp-config" element={<MCPConfigGenerator />} />
                <Route path="/shares" element={<Shares />} />
                  <Route path="/account" element={<MyAccount />} />
                </Routes>
              </div>
            </Suspense>
          </ErrorBoundary>
        </div>
      </main>
    </div>
  );
}

function App() {
  return (
    <AuthProvider>
      <ThemeProvider>
        <ProjectProvider>
          <NeuralNetwork />
          <SetupGuard>
            <Suspense fallback={<RouteFallback />}>
              <Routes>
                <Route path="/setup" element={<SetupWizard />} />
                <Route path="/login" element={<Login />} />
                {/*
                  /authorize and /share/accept must work for unauthenticated
                  callers: the OAuth client redirects an external browser
                  to /authorize, and share recipients open /share/accept
                  magic-links before they ever sign in. Both pages tolerate
                  a logged-in session but never require one.
                */}
                <Route path="/authorize" element={<Authorize />} />
                <Route path="/share/accept" element={<ShareAccept />} />
                <Route path="/*" element={<AuthGuard><AppLayout /></AuthGuard>} />
              </Routes>
            </Suspense>
          </SetupGuard>
        </ProjectProvider>
      </ThemeProvider>
    </AuthProvider>
  );
}

export default App;
