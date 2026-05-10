import { useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import Page from "../components/Page";
import Panel from "../components/Panel";
import Button from "../components/Button";
import StatusBadge from "../components/StatusBadge";
import { api } from "../api/client";
import type { SystemStatus } from "../api/types";

export default function System() {
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = async () => {
    setLoading(true);
    try {
      setStatus(await api.systemStatus());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
    // Poll once every 5s. Cheap (one DB count + N pool snapshots).
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
                <th>State</th>
              </tr>
            </thead>
            <tbody>
              {status.pools.map((p) => (
                <tr key={p.server_id}>
                  <td>{p.server_name}</td>
                  <td className="muted">
                    {p.host}:{p.port}
                  </td>
                  <td className="muted">
                    {p.in_use}/{p.max_conns} in use, {p.idle} idle
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
    </Page>
  );
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
