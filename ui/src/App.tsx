import { Routes, Route, NavLink } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import SetupWizard from "./pages/SetupWizard";
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

interface NavItem {
  path: string;
  label: string;
  section: string;
}

const navItems: NavItem[] = [
  { path: "/", label: "Dashboard", section: "Overview" },
  { path: "/setup", label: "Setup Wizard", section: "Overview" },
  { path: "/memories", label: "Memory Browser", section: "Data" },
  { path: "/entities", label: "Entity Browser", section: "Data" },
  { path: "/graph", label: "Graph Visualization", section: "Data" },
  { path: "/projects", label: "Projects", section: "Management" },
  { path: "/organizations", label: "Organizations", section: "Management" },
  { path: "/users", label: "Users", section: "Management" },
  { path: "/providers", label: "Providers", section: "Configuration" },
  { path: "/settings", label: "Settings", section: "Configuration" },
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

function App() {
  const sections = groupBySection(navItems);

  return (
    <div className="flex h-screen">
      <aside className="w-60 shrink-0 border-r border-border bg-card overflow-y-auto">
        <div className="px-4 py-5">
          <h1 className="text-lg font-semibold tracking-tight">nram</h1>
          <p className="text-xs text-muted-foreground">Admin Console</p>
        </div>
        <nav className="px-2 pb-4">
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
                    </NavLink>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </nav>
      </aside>
      <main className="flex-1 overflow-y-auto">
        <div className="p-6">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/setup" element={<SetupWizard />} />
            <Route path="/memories" element={<MemoryBrowser />} />
            <Route path="/projects" element={<ProjectManagement />} />
            <Route path="/organizations" element={<OrganizationManagement />} />
            <Route path="/users" element={<UserManagement />} />
            <Route path="/providers" element={<ProviderConfiguration />} />
            <Route path="/settings" element={<SettingsEditor />} />
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
        </div>
      </main>
    </div>
  );
}

export default App;
