import { useEffect, useRef, useState } from "react";
import { Pause, Play, RefreshCw } from "lucide-react";
import Page from "../components/Page";
import Panel from "../components/Panel";
import Button from "../components/Button";
import StatusBadge from "../components/StatusBadge";
import { api, logStreamURL } from "../api/client";
import type { LogEntry, SystemStatus, Throughput } from "../api/types";

export default function System() {
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [throughput, setThroughput] = useState<Throughput | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = async () => {
    setLoading(true);
    try {
      const [s, t] = await Promise.all([api.systemStatus(), api.throughput()]);
      setStatus(s);
      setThroughput(t);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
    // Poll once every 5s. Cheap (one DB count + N pool snapshots
    // + a small rolling-window snapshot for the sparkline).
    const id = setInterval(() => void refresh(), 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <Page
      title="System"
      subtitle="Build, runtime and diagnostics"
      actions={
        <Button
          variant="ghost"
          icon={<RefreshCw size={14} />}
          onClick={() => void refresh()}
          disabled={loading}
        >
          Refresh
        </Button>
      }
    >
      <Panel
        title="Status"
        meta={
          <StatusBadge tone={status ? "ok" : "warn"} dot>
            {status ? "running" : "checking"}
          </StatusBadge>
        }
      >
        {error ? (
          <p className="text-err">{error}</p>
        ) : status ? (
          <dl className="kv">
            <dt>Service</dt>
            <dd>
              <code className="inline-code">{status.service}</code>
            </dd>
            <dt>Version</dt>
            <dd>
              <code className="inline-code">{status.version}</code>
            </dd>
            <dt>Started</dt>
            <dd className="muted">{formatTime(status.started_at)}</dd>
            <dt>Uptime</dt>
            <dd className="muted">{formatUptime(status.uptime_ms)}</dd>
            <dt>Queue</dt>
            <dd className="muted">
              {status.queue.active} active / {status.queue.total} total
            </dd>
            <dt>Throughput</dt>
            <dd>
              <Sparkline data={throughput?.series ?? []} />
              <span className="muted" style={{ marginLeft: "0.5rem" }}>
                {throughput
                  ? `${formatBytes(throughput.current_bytes_per_sec)}/s now • ${formatBytes(throughput.total_bytes)} last 5m`
                  : "—"}
              </span>
            </dd>
          </dl>
        ) : null}
      </Panel>

      <Panel
        title="Usenet pools"
        meta={
          status ? (
            <StatusBadge tone="neutral">{status.pools.length} servers</StatusBadge>
          ) : null
        }
        flush
      >
        {status && status.pools.length > 0 ? (
          <table className="table">
            <thead>
              <tr>
                <th>Server</th>
                <th>Endpoint</th>
                <th>Conns</th>
                <th>Type</th>
                <th>Used</th>
                <th>State</th>
              </tr>
            </thead>
            <tbody>
              {status.pools.map((p) => (
                <tr key={p.server_id}>
                  <td>
                    {p.server_name}
                    {p.backup ? <span className="muted"> (backup)</span> : null}
                  </td>
                  <td className="muted">
                    {p.host}:{p.port}
                  </td>
                  <td className="muted">
                    {p.in_use}/{p.max_conns} in use, {p.idle} idle
                  </td>
                  <td>
                    <StatusBadge
                      tone={p.billing_mode === "metered" ? "warn" : "ok"}
                      dot
                    >
                      {p.billing_mode === "metered" ? "metered" : "flat"}
                    </StatusBadge>
                  </td>
                  <td className="muted">
                    {p.billing_mode === "metered" ? (
                      <UsageBar used={p.used_bytes} quota={p.quota_bytes} />
                    ) : (
                      formatBytes(p.used_bytes)
                    )}
                  </td>
                  <td>
                    <StatusBadge tone={p.enabled ? "ok" : "neutral"} dot>
                      {p.enabled ? "enabled" : "disabled"}
                    </StatusBadge>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty-state">
            <p className="empty-title">No Usenet servers configured</p>
            <p className="muted">Add one under Settings → Servers.</p>
          </div>
        )}
      </Panel>

      <LogsPanel />
    </Page>
  );
}

function Sparkline({ data, height = 22, width = 140 }: { data: number[]; height?: number; width?: number }) {
  if (data.length === 0) {
    return <svg className="sparkline" width={width} height={height} aria-hidden="true" />;
  }
  const max = Math.max(...data, 1);
  const step = width / Math.max(data.length - 1, 1);
  const pts = data
    .map((v, i) => {
      const x = i * step;
      const y = height - (v / max) * (height - 2) - 1;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <svg className="sparkline" width={width} height={height} aria-label="throughput last 5 minutes">
      <polyline points={pts} fill="none" stroke="var(--accent)" strokeWidth={1.5} />
    </svg>
  );
}

function LogsPanel() {
  const [entries, setEntries] = useState<LogEntry[]>([]);
  const [paused, setPaused] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const esRef = useRef<EventSource | null>(null);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;

  useEffect(() => {
    let cancelled = false;
    api
      .logSnapshot()
      .then((s) => {
        if (!cancelled) setEntries(s.entries ?? []);
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });

    const es = new EventSource(logStreamURL(), { withCredentials: true });
    esRef.current = es;
    es.addEventListener("log", (ev) => {
      if (pausedRef.current) return;
      try {
        const entry = JSON.parse((ev as MessageEvent).data) as LogEntry;
        setEntries((cur) => {
          const next = [...cur, entry];
          // Cap at 500 in-memory entries to keep the DOM happy.
          return next.length > 500 ? next.slice(next.length - 500) : next;
        });
      } catch {
        // ignore
      }
    });
    es.onerror = () => {
      // Browser auto-reconnects; we just surface a soft hint.
    };
    return () => {
      cancelled = true;
      es.close();
      esRef.current = null;
    };
  }, []);

  return (
    <Panel
      title="Logs"
      meta={
        <button
          type="button"
          className="icon-btn"
          aria-label={paused ? "Resume log stream" : "Pause log stream"}
          onClick={() => setPaused((v) => !v)}
        >
          {paused ? <Play size={14} /> : <Pause size={14} />}
        </button>
      }
      flush
    >
      {error && <p className="text-err">{error}</p>}
      <div className="log-viewer">
        {entries.length === 0 ? (
          <p className="muted">Waiting for log records…</p>
        ) : (
          entries.slice(-300).map((e, i) => (
            <div key={`${e.time}-${i}`} className={`log-line log-${e.level.toLowerCase()}`}>
              <span className="log-time">
                {new Date(e.time).toLocaleTimeString()}
              </span>
              <span className="log-level">{e.level}</span>
              <span className="log-msg">{e.message}</span>
              {e.attrs &&
                Object.entries(e.attrs).map(([k, v]) => (
                  <span key={k} className="log-attr">
                    {k}=
                    <span className="log-attr-value">{v}</span>
                  </span>
                ))}
            </div>
          ))
        )}
      </div>
    </Panel>
  );
}

function UsageBar({ used, quota }: { used: number; quota: number }) {
  if (!quota) {
    // No quota set → show just the used count.
    return <>{formatBytes(used)}</>;
  }
  const pct = Math.min(100, Math.round((used * 100) / quota));
  const tone = pct >= 90 ? "is-err" : pct >= 75 ? "is-warn" : "";
  return (
    <span className="usage-bar">
      <span className="usage-bar-text">
        {formatBytes(used)} / {formatBytes(quota)} ({pct}%)
      </span>
      <span className={`usage-bar-track`}>
        <span
          className={`usage-bar-fill ${tone}`}
          style={{ width: `${pct}%` }}
        />
      </span>
    </span>
  );
}

function formatBytes(n: number): string {
  if (!n) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 && i > 0 ? 1 : 0)} ${units[i]}`;
}

function formatTime(s: string): string {
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return s;
  return d.toLocaleString();
}

function formatUptime(ms: number): string {
  if (ms < 0) ms = 0;
  const s = Math.floor(ms / 1000);
  const days = Math.floor(s / 86400);
  const hours = Math.floor((s % 86400) / 3600);
  const mins = Math.floor((s % 3600) / 60);
  const secs = s % 60;
  if (days > 0) return `${days}d ${hours}h ${mins}m`;
  if (hours > 0) return `${hours}h ${mins}m`;
  if (mins > 0) return `${mins}m ${secs}s`;
  return `${secs}s`;
}
