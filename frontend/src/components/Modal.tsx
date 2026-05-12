import { useEffect, useRef } from "react";
import type { ReactNode } from "react";
import { X } from "lucide-react";

// Modal is the centered overlay used by Settings card-grid sections
// to host edit/add forms. Backdrop click + Esc close it; the focus
// is moved to the dialog on open so screen readers + Tab cycling work
// without ceremony.
//
// Footer + body are explicit children so each section can place a
// destructive Delete button on the left while Save/Cancel sit right.
type Props = {
  title: string;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
};

export default function Modal({ title, onClose, children, footer }: Props) {
  const dialogRef = useRef<HTMLDivElement | null>(null);

  // Esc closes. Captured on document so focus inside form inputs
  // doesn't swallow it.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    // Body scroll lock — prevents the underlying Settings page from
    // jittering when the dialog grows.
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    // Move focus into the dialog for screen readers + Tab.
    dialogRef.current?.focus();
    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = prev;
    };
  }, [onClose]);

  return (
    <div
      className="modal-backdrop"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        tabIndex={-1}
        className="modal-dialog"
      >
        <div className="modal-header">
          <h2>{title}</h2>
          <button
            type="button"
            className="icon-btn"
            onClick={onClose}
            aria-label="Close"
          >
            <X size={16} />
          </button>
        </div>
        <div className="modal-body">{children}</div>
        {footer ? <div className="modal-footer">{footer}</div> : null}
      </div>
    </div>
  );
}
