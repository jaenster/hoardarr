import type { ReactNode } from "react";

// EntityCard is one tile in a CardGrid: a large title and an optional
// status badge (or other compact meta) underneath. The whole card is
// the click target — opens the section's edit modal.
//
// Deliberately minimal: no buttons, no inline edit, no expand state.
// All actions (test, delete) live inside the modal so the grid stays
// scannable.
type Props = {
  title: string;
  badge?: ReactNode;
  onClick: () => void;
  ariaLabel?: string;
};

export default function EntityCard({ title, badge, onClick, ariaLabel }: Props) {
  return (
    <button
      type="button"
      className="entity-card"
      onClick={onClick}
      aria-label={ariaLabel ?? `Edit ${title}`}
    >
      <span className="entity-card-title">{title}</span>
      {badge ? <span className="entity-card-badge">{badge}</span> : null}
    </button>
  );
}
