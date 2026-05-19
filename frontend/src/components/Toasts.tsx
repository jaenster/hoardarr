import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { CheckCircle2, AlertTriangle, AlertCircle, Info, X } from "lucide-react";

// Toasts are short, low-friction notifications fired in response to
// operator actions: "Server saved", "Test connection failed",
// "Webhook deleted". They live for ~4s then auto-dismiss, can be
// manually closed, and stack bottom-right (out of content's way).
//
// Usage:
//   const toast = useToasts();
//   toast.success("Server saved");
//   toast.error("Test failed", { message: err.message });
//
// One ToastProvider sits inside AppShell so every page can call
// useToasts() without importing anything else.

export type ToastTone = "success" | "error" | "warning" | "info";

export type Toast = {
  id: number;
  tone: ToastTone;
  title: string;
  message?: string;
  ttlMs: number;
};

type ToastOpts = {
  message?: string;
  ttlMs?: number;
};

type ToastApi = {
  success: (title: string, opts?: ToastOpts) => void;
  error: (title: string, opts?: ToastOpts) => void;
  warning: (title: string, opts?: ToastOpts) => void;
  info: (title: string, opts?: ToastOpts) => void;
  dismiss: (id: number) => void;
};

const ToastContext = createContext<ToastApi | null>(null);

const DEFAULT_TTL = 4000;

export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<Toast[]>([]);
  const nextId = useRef(1);

  const dismiss = useCallback((id: number) => {
    setItems((cur) => cur.filter((t) => t.id !== id));
  }, []);

  const push = useCallback((tone: ToastTone, title: string, opts?: ToastOpts) => {
    const id = nextId.current++;
    const ttl = opts?.ttlMs ?? DEFAULT_TTL;
    setItems((cur) => [...cur, { id, tone, title, message: opts?.message, ttlMs: ttl }]);
    if (ttl > 0) {
      window.setTimeout(() => dismiss(id), ttl);
    }
  }, [dismiss]);

  const api = useMemo<ToastApi>(() => ({
    success: (t, o) => push("success", t, o),
    error: (t, o) => push("error", t, { ttlMs: 8000, ...o }), // errors stick longer
    warning: (t, o) => push("warning", t, o),
    info: (t, o) => push("info", t, o),
    dismiss,
  }), [push, dismiss]);

  return (
    <ToastContext.Provider value={api}>
      {children}
      <ToastViewport items={items} dismiss={dismiss} />
    </ToastContext.Provider>
  );
}

export function useToasts(): ToastApi {
  const ctx = useContext(ToastContext);
  if (!ctx) {
    throw new Error("useToasts() must be inside <ToastProvider>");
  }
  return ctx;
}

function ToastViewport({ items, dismiss }: { items: Toast[]; dismiss: (id: number) => void }) {
  if (items.length === 0) return null;
  return (
    <div className="toast-viewport" role="region" aria-label="Notifications" aria-live="polite">
      {items.map((t) => (
        <ToastCard key={t.id} toast={t} onDismiss={() => dismiss(t.id)} />
      ))}
    </div>
  );
}

function ToastCard({ toast, onDismiss }: { toast: Toast; onDismiss: () => void }) {
  useEffect(() => {
    // No-op effect; ttl auto-dismiss is set up in push() to keep the
    // timer alive across re-renders.
  }, []);
  return (
    <div className={`toast toast-${toast.tone}`} role="status">
      <span className="toast-icon" aria-hidden="true">
        {toast.tone === "success" && <CheckCircle2 size={16} />}
        {toast.tone === "error" && <AlertCircle size={16} />}
        {toast.tone === "warning" && <AlertTriangle size={16} />}
        {toast.tone === "info" && <Info size={16} />}
      </span>
      <div className="toast-body">
        <div className="toast-title">{toast.title}</div>
        {toast.message && <div className="toast-msg">{toast.message}</div>}
      </div>
      <button
        className="toast-dismiss"
        onClick={onDismiss}
        aria-label="Dismiss notification"
      >
        <X size={14} />
      </button>
    </div>
  );
}
