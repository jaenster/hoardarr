import { useState } from "react";
import { Boxes, Heart, LogOut, Search, UserCircle2 } from "lucide-react";
import { api } from "../api/client";

export default function TopBar() {
  const [menuOpen, setMenuOpen] = useState(false);

  const logout = async () => {
    try {
      await api.logout();
    } finally {
      // Reload to bring AuthGate back to the login screen.
      window.location.reload();
    }
  };

  return (
    <header className="topbar" role="banner">
      <div className="topbar-brand" aria-hidden="true">
        <span className="topbar-brand-mark">
          <Boxes size={22} strokeWidth={2} />
        </span>
      </div>

      <div className="topbar-search">
        <Search size={14} className="topbar-search-icon" aria-hidden="true" />
        <input
          type="search"
          placeholder="Search"
          aria-label="Search"
          spellCheck={false}
          autoComplete="off"
        />
      </div>

      <div className="topbar-spacer" />

      <div className="topbar-right">
        <button
          type="button"
          className="topbar-icon-btn is-donate"
          aria-label="Donate"
          tabIndex={0}
        >
          <Heart size={18} strokeWidth={2} />
        </button>
        <div className="topbar-account">
          <button
            type="button"
            className="topbar-icon-btn"
            aria-label="Account"
            aria-expanded={menuOpen}
            onClick={() => setMenuOpen((v) => !v)}
            tabIndex={0}
          >
            <UserCircle2 size={20} strokeWidth={2} />
          </button>
          {menuOpen && (
            <div className="topbar-account-menu" role="menu">
              <button
                type="button"
                className="topbar-account-item"
                role="menuitem"
                onClick={() => void logout()}
              >
                <LogOut size={14} aria-hidden="true" />
                <span>Sign out</span>
              </button>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}
