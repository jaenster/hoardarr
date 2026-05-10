import { Boxes, Heart, Search, UserCircle2 } from "lucide-react";

export default function TopBar() {
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
        <button
          type="button"
          className="topbar-icon-btn"
          aria-label="Account"
          tabIndex={0}
        >
          <UserCircle2 size={20} strokeWidth={2} />
        </button>
      </div>
    </header>
  );
}
