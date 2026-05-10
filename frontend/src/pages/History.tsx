import { useEffect, useState } from "react";
import { Filter, History as HistoryIcon, RefreshCw } from "lucide-react";
import Page from "../components/Page";
import Panel from "../components/Panel";
import Button from "../components/Button";
import StatusBadge from "../components/StatusBadge";
import { api } from "../api/client";
import type { Job, JobState } from "../api/types";

type Filter = "" | "completed" | "failed" | "aborted";

const tone: Record<string, "ok" | "err" | "warn" | "neutral"> = {
  completed: "ok",
  failed: "err",
  aborted: "warn",
};

export default function History() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<Filter>("");

  const refresh = async (f: Filter = filter) => {
    setLoading(true);
    try {
      const opts = f
        ? { state: f as Exclude<Filter, "">, limit: 200 }
        : { limit: 200 };
      const r = await api.listHistory(opts);
      setJobs(r.jobs ?? []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh("");
  }, []);

  return (
    <Page
      title="History"
      subtitle="Completed, failed, and aborted jobs"
      actions={
        <>
          <FilterPicker
            value={filter}
            onChange={(v) => {
              setFilter(v);
              void refresh(v);
            }}
          />
          <Button
            variant="secondary"
            icon={<RefreshCw size={14} />}
            onClick={() => void refresh()}
            disabled={loading}
          >
            Refresh
          </Button>
        </>
      }
    >
      <Panel
        title="Recent activity"
        meta={<StatusBadge tone="neutral">{jobs.length} records</StatusBadge>}
        flush
      >
        {error ? (
          <div className="empty-state">
            <p className="empty-title text-err">{error}</p>
          </div>
        ) : jobs.length === 0 && !loading ? (
          <div className="empty-state">
            <HistoryIcon size={28} className="empty-icon" aria-hidden="true" />
            <p className="empty-title">No history yet</p>
            <p className="muted">Completed jobs appear here.</p>
          </div>
        ) : (
          <table className="table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Category</th>
                <th>State</th>
                <th>Finished</th>
                <th>Size</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((j) => (
                <tr key={j.id}>
                  <td>
                    <span className="queue-row-name">{j.name}</span>
                    {j.error ? (
                      <span className="muted" title={j.error}>
                        {" — "}
                        {truncate(j.error, 80)}
                      </span>
                    ) : null}
                  </td>
                  <td>
                    <span className="queue-row-cat">{j.category || "—"}</span>
                  </td>
                  <td>
                    <StatusBadge tone={tone[j.state] ?? "neutral"}>
                      {labelFor(j.state)}
                    </StatusBadge>
                  </td>
                  <td className="muted">{formatTime(j.finished_at)}</td>
                  <td className="muted">{formatBytes(j.total_bytes)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Panel>
    </Page>
  );
}

function FilterPicker({
  value,
  onChange,
}: {
  value: Filter;
  onChange: (v: Filter) => void;
}) {
  return (
    <div className="filter-picker">
      <Filter size={14} aria-hidden="true" />
      <select
        value={value}
        onChange={(e) => onChange(e.target.value as Filter)}
        aria-label="Filter by state"
      >
        <option value="">All</option>
        <option value="completed">Completed</option>
        <option value="failed">Failed</option>
        <option value="aborted">Aborted</option>
      </select>
    </div>
  );
}

function labelFor(s: JobState): string {
  return s.charAt(0).toUpperCase() + s.slice(1).replace(/_/g, " ");
}

function truncate(s: string, n: number): string {
  return s.length <= n ? s : s.slice(0, n - 1) + "…";
}

function formatTime(s?: string): string {
  if (!s) return "—";
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return s;
  return d.toLocaleString();
}

function formatBytes(n: number): string {
  if (!n) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`;
}
