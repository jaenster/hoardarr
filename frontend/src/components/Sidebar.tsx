import { useEffect, useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import {
  Activity as ActivityIcon,
  History as HistoryIcon,
  Settings as SettingsIcon,
  Cpu as SystemIcon,
  type LucideIcon,
} from "lucide-react";
import { api } from "../api/client";
import type { Job, SystemStatus } from "../api/types";

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
  { label: "Bandwidth", href: "#bandwidth" },
  { label: "General", href: "#general" },
  { label: "Authentication", href: "#authentication" },
  { label: "SABnzbd Compatibility", href: "#sab-compat" },
  { label: "Connect (Webhooks)", href: "#connect" },
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

// formatVersion strips a leading "v" from the server-reported version
// before re-prefixing it, so a Go-module pseudo-version
// ("v0.1.5-0.20260514...") doesn't render as "vv0.1.5-...".
function formatVersion(v?: string): string {
  if (!v) return "dev";
  return "v" + v.replace(/^v/, "");
}

// formatRate is a short bytes/s formatter used in the footer; the
// Activity page has its own copy but the sidebar is in the pre-auth
// bundle and we don't want to drag the queue page in here just for
// this. Tiny duplicate, big-perf win.
function formatRate(bps: number): string {
  if (bps < 1024) return `${bps} B/s`;
  const units = ["KB/s", "MB/s", "GB/s"];
  let v = bps / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`;
}

// sidebarStatus picks the right label + dot color for the footer pill
// based on the queue snapshot. "Idle" when no jobs are downloading,
// throughput rate when at least one is, "Paused" when all are paused.
function sidebarStatus(jobs: Job[], bytesPerSec: number): {
  label: string;
  tone: "ok" | "info" | "warn" | "neutral";
} {
  if (!jobs || jobs.length === 0) return { label: "Idle", tone: "ok" };
  const downloading = jobs.filter((j) => j.state === "downloading").length;
  if (downloading > 0) {
    return {
      label: bytesPerSec > 0 ? formatRate(bytesPerSec) : "Downloading",
      tone: "info",
    };
  }
  const paused = jobs.filter((j) => j.state === "paused").length;
  if (paused > 0 && paused === jobs.length) return { label: "Paused", tone: "warn" };
  return { label: "Idle", tone: "ok" };
}

export default function Sidebar() {
  const location = useLocation();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [throughput, setThroughput] = useState<number>(0);
  const [status, setStatus] = useState<SystemStatus | null>(null);

  // Poll the queue + throughput at a slow cadence rather than opening
  // a second SSE stream (Activity already owns one). 5s is plenty for
  // a footer pill that just answers "is anything happening".
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const [q, t] = await Promise.all([
          api.listQueue(),
          api.throughput().catch(() => null),
        ]);
        if (cancelled) return;
        setJobs(q.jobs ?? []);
        if (t) setThroughput(t.current_bytes_per_sec ?? 0);
      } catch {
        /* footer is decorative — ignore */
      }
    };
    void tick();
    const id = setInterval(() => void tick(), 5000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // System status (for the version label) only needs to be fetched once.
  useEffect(() => {
    let cancelled = false;
    void api.systemStatus().then((s) => {
      if (!cancelled) setStatus(s);
    }).catch(() => {
      /* ignore */
    });
    return () => {
      cancelled = true;
    };
  }, []);

  const footer = sidebarStatus(jobs, throughput);

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
        <div className="sidebar-status" title={`hoardarr ${status?.version ?? ""}`}>
          <span
            className={`status-dot status-dot-${footer.tone}`}
            aria-hidden="true"
          />
          <span className="status-text">{footer.label}</span>
        </div>
        <span className="sidebar-version">{formatVersion(status?.version)}</span>
      </div>
    </aside>
  );
}
