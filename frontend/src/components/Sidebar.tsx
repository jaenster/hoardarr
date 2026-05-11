import { NavLink, useLocation } from "react-router-dom";
import {
  Activity as ActivityIcon,
  History as HistoryIcon,
  Settings as SettingsIcon,
  Cpu as SystemIcon,
  type LucideIcon,
} from "lucide-react";

type BadgeTone = "warn" | "err";

type Badge = {
  count: number;
  tone: BadgeTone;
};

type SubNavItem = {
  label: string;
  href: string;
};

type NavItem = {
  to: string;
  label: string;
  icon: LucideIcon;
  badge?: Badge;
  subNav?: SubNavItem[];
};

// Sub-nav reflects the panels actually rendered on the Settings page.
// Aspirational sections (Post-Processing, Bandwidth, Connect, UI) come
// back when their backing features land.
const settingsSubNav: SubNavItem[] = [
  { label: "Usenet Servers", href: "#servers" },
  { label: "Categories", href: "#categories" },
  { label: "Paths", href: "#paths" },
  { label: "General", href: "#general" },
  { label: "Authentication", href: "#authentication" },
  { label: "SABnzbd Compatibility", href: "#sab-compat" },
];

const navItems: NavItem[] = [
  { to: "/activity", label: "Activity", icon: ActivityIcon },
  { to: "/history", label: "History", icon: HistoryIcon },
  {
    to: "/settings",
    label: "Settings",
    icon: SettingsIcon,
    subNav: settingsSubNav,
  },
  { to: "/system", label: "System", icon: SystemIcon },
];

function renderBadge(badge: Badge | undefined) {
  if (!badge || badge.count <= 0) return null;
  const toneClass = badge.tone === "err" ? "is-err" : "is-warn";
  return <span className={`sidebar-badge ${toneClass}`}>{badge.count}</span>;
}

export default function Sidebar() {
  const location = useLocation();

  return (
    <aside className="sidebar">
      <nav className="sidebar-nav" aria-label="Primary">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive =
            location.pathname === item.to ||
            location.pathname.startsWith(item.to + "/");
          return (
            <div key={item.to} className="sidebar-nav-group">
              <NavLink
                to={item.to}
                className={({ isActive: navActive }) =>
                  "nav-item" + (navActive ? " is-active" : "")
                }
                end={item.to === "/"}
              >
                <span className="nav-icon" aria-hidden="true">
                  <Icon size={16} strokeWidth={2} />
                </span>
                <span className="nav-label">{item.label}</span>
                {renderBadge(item.badge)}
              </NavLink>

              {isActive && item.subNav && item.subNav.length > 0 && (
                <div className="sidebar-subnav" role="list">
                  {item.subNav.map((sub) => (
                    <a
                      key={sub.href}
                      href={sub.href}
                      className="sidebar-subnav-item"
                      role="listitem"
                    >
                      {sub.label}
                    </a>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </nav>

      <div className="sidebar-footer">
        <div className="sidebar-status">
          <span className="status-dot" aria-hidden="true" />
          <span className="status-text">Idle</span>
        </div>
        <span className="sidebar-version">v0.0.1-dev</span>
      </div>
    </aside>
  );
}
