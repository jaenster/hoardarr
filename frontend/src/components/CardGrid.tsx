import type { ReactNode } from "react";
import { Plus } from "lucide-react";

// CardGrid renders a Sonarr-style 2-column grid of entity cards with a
// "+" add-card at the end. Used for Settings sections that manage a
// small set of named entities (Usenet servers, webhook subscriptions,
// notification providers, categories). Each card click opens a modal
// editor; the "+" card opens the same editor for a fresh entity.
//
// Children are EntityCard nodes — keeping the shape minimal so each
// section can choose its own status badges + click handler without
// the grid having to know.
type Props = {
  children: ReactNode;
  onAdd: () => void;
  addLabel?: string;
};

export default function CardGrid({ children, onAdd, addLabel }: Props) {
  return (
    <div className="card-grid">
      {children}
      <button
        type="button"
        className="card-grid-add"
        onClick={onAdd}
        aria-label={addLabel ?? "Add"}
      >
        <Plus size={28} />
      </button>
    </div>
  );
}
