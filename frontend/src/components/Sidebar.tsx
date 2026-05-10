import { NavLink } from "react-router-dom";

const navItems = [
  { to: "/activity", label: "Activity", icon: "▶" },
  { to: "/history", label: "History", icon: "⟳" },
  { to: "/settings", label: "Settings", icon: "⚙" },
  { to: "/system", label: "System", icon: "⌬" },
];

export default function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar-brand">
        <span className="brand-mark">⬢</span>
        <span className="brand-name">hoardarr</span>
      </div>
      <nav className="sidebar-nav">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            className={({ isActive }) =>
              "nav-item" + (isActive ? " active" : "")
            }
          >
            <span className="nav-icon">{item.icon}</span>
            <span className="nav-label">{item.label}</span>
          </NavLink>
        ))}
      </nav>
      <div className="sidebar-footer">
        <span className="status-dot" />
        <span className="status-text">Idle</span>
      </div>
    </aside>
  );
}
