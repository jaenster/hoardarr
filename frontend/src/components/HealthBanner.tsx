import { useEffect, useState } from "react";
import { AlertTriangle, AlertCircle, RefreshCw, X } from "lucide-react";
import { api } from "../api/client";
import type { HealthIssue } from "../api/types";

// HealthBanner polls /api/v1/system/health on a 30s cadence and
// renders a stacked banner above the main app content. Issues sort
// errors-first (already done server-side), red for errors, amber for
// warnings, with an optional docs link.
//
// Each issue can be dismissed in the current session — sometimes the
// operator is mid-fixing and doesn't want the alarm-red banner in
// every screenshot. Dismissal is keyed by Source so a re-occurring
// issue (e.g. server toggled off then on then off) re-appears on the
// next change.

const DISMISS_KEY = "hoardarr.health.dismissed";

function loadDismissed(): Set<string> {
  try {
    const raw = sessionStorage.getItem(DISMISS_KEY);
    if (!raw) return new Set();
    return new Set(JSON.parse(raw) as string[]);
  } catch {
    return new Set();
  }
}

function saveDismissed(s: Set<string>) {
  try {
    sessionStorage.setItem(DISMISS_KEY, JSON.stringify([...s]));
  } catch {
    /* ignore */
  }
}

export default function HealthBanner() {
  const [issues, setIssues] = useState<HealthIssue[]>([]);
  const [dismissed, setDismissed] = useState<Set<string>>(() => loadDismissed());
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const resp = await api.systemHealth();
        if (!cancelled) setIssues(resp.issues ?? []);
      } catch {
        /* network blips are silently absorbed; the banner is decorative */
      }
    };
    void tick();
    const t = setInterval(() => void tick(), 30_000);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, []);

  const visible = issues.filter((i) => !dismissed.has(i.source));
  if (visible.length === 0) return null;

  const dismiss = (source: string) => {
    const next = new Set(dismissed);
    next.add(source);
    setDismissed(next);
    saveDismissed(next);
  };

  const refresh = async () => {
    setRefreshing(true);
    try {
      const resp = await api.refreshSystemHealth();
      setIssues(resp.issues ?? []);
    } catch {
      /* ignore */
    } finally {
      setRefreshing(false);
    }
  };

  return (
    <div className="health-banner">
      {visible.map((i) => (
        <div
          key={i.source}
          className={`health-issue health-${i.severity}`}
          role="alert"
        >
          {i.severity === "error" ? (
            <AlertCircle size={16} aria-hidden="true" />
          ) : (
            <AlertTriangle size={16} aria-hidden="true" />
          )}
          <span className="health-msg">{i.message}</span>
          {i.docs_url && (
            <a
              className="health-docs"
              href={i.docs_url}
              target="_blank"
              rel="noopener noreferrer"
            >
              docs
            </a>
          )}
          <button
            className="health-refresh"
            onClick={() => void refresh()}
            disabled={refreshing}
            title="Re-run checks"
            aria-label="Re-run health checks"
          >
            <RefreshCw size={14} />
          </button>
          <button
            className="health-dismiss"
            onClick={() => dismiss(i.source)}
            title="Dismiss for this session"
            aria-label="Dismiss"
          >
            <X size={14} />
          </button>
        </div>
      ))}
    </div>
  );
}
