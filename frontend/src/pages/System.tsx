import { useEffect, useRef, useState } from "react";
import { Database, Gauge, Pause, Play, PlayCircle, RefreshCw, Send } from "lucide-react";
import Page from "../components/Page";
import Panel from "../components/Panel";
import Button from "../components/Button";
import SpeedChart from "../components/SpeedChart";
import StatusBadge from "../components/StatusBadge";
import { useToasts } from "../components/Toasts";
import { api, logStreamURL } from "../api/client";
import type {
  BackupFile,
  Command,
  DiskEntry,
  LogEntry,
  LogFile,
  ScheduledTask,
  SpeedHistory,
  SpeedHistoryRange,
  SystemStatus,
  Throughput,
} from "../api/types";

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
      <SpeedPanel throughput={throughput} />

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
              {status.commit && <span className="muted" style={{ marginLeft: "0.5rem" }}>commit {status.commit.slice(0, 8)}</span>}
              {status.build_date && <span className="muted" style={{ marginLeft: "0.5rem" }}>built {status.build_date}</span>}
            </dd>
            {status.runtime_version && (
              <>
                <dt>Runtime</dt>
                <dd className="muted">
                  {status.runtime_version}
                  {status.os && status.arch && <> • {status.os}/{status.arch}</>}
                  {status.is_docker && <> • docker</>}
                </dd>
              </>
            )}
            {(status.database_type || status.migration_version !== undefined) && (
              <>
                <dt>Database</dt>
                <dd className="muted">
                  {status.database_type ?? "sqlite"} • schema v{status.migration_version ?? "?"}
                </dd>
              </>
            )}
            <dt>Started</dt>
            <dd className="muted">{formatTime(status.started_at)}</dd>
            <dt>Uptime</dt>
            <dd className="muted">{formatUptime(status.uptime_ms)}</dd>
            <dt>Queue</dt>
            <dd className="muted">
              {status.queue.active} active / {status.queue.total} total
            </dd>
            <dt>Throughput</dt>
            <dd className="muted">
              {throughput
                ? `${formatBytes(throughput.current_bytes_per_sec)}/s now • peak ${formatBytes(throughput.peak_alltime_bytes_per_sec)}/s`
                : "—"}
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

      <DiskSpacePanel />

      <TasksPanel />

      <CommandsPanel />

      <LogsPanel />

      <BackupsPanel />

      <LogFilesPanel />
    </Page>
  );
}

// SpeedPanel renders the full-width throughput chart, range picker,
// peak stats, and a quick throttle slider that PUTs the global cap
// without a trip to Settings. The chart polls /system/speed-history
// at a cadence proportional to the chosen range (5m views poll fast,
// 7d views poll once a minute — no point thrashing the DB).
function SpeedPanel({ throughput }: { throughput: Throughput | null }) {
  const [range, setRange] = useState<SpeedHistoryRange>("5m");
  const [history, setHistory] = useState<SpeedHistory | null>(null);
  const [draftCap, setDraftCap] = useState<string>("");
  const [savingCap, setSavingCap] = useState(false);
  const toast = useToasts();

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const h = await api.speedHistory(range);
        if (!cancelled) setHistory(h);
      } catch {
        /* ignore — chart simply doesn't update */
      }
    };
    void load();
    const interval = range === "5m" || range === "1h" ? 5_000 : 30_000;
    const id = setInterval(() => void load(), interval);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [range]);

  // Seed the cap input from the live throughput response so the
  // operator sees the current value without needing to load Settings.
  useEffect(() => {
    if (throughput && draftCap === "") {
      const mb = throughput.global_cap_bytes_per_sec / (1024 * 1024);
      setDraftCap(mb > 0 ? String(round1(mb)) : "0");
    }
  }, [throughput]);

  const submitCap = async (e: React.FormEvent) => {
    e.preventDefault();
    const mb = Number(draftCap);
    if (!Number.isFinite(mb) || mb < 0) {
      toast.error("Speed must be a non-negative number (MB/s)");
      return;
    }
    setSavingCap(true);
    try {
      const bytes = Math.round(mb * 1024 * 1024);
      await api.setBandwidth({ global_bytes_per_sec: bytes });
      toast.success(mb > 0 ? `Throttle set to ${mb} MB/s` : "Throttle disabled");
    } catch (err) {
      toast.error("Could not save cap", { message: err instanceof Error ? err.message : String(err) });
    } finally {
      setSavingCap(false);
    }
  };

  const cap = history?.global_cap_bytes_per_sec ?? throughput?.global_cap_bytes_per_sec ?? 0;
  const peakAllTime = history?.peak_alltime_bytes_per_sec ?? throughput?.peak_alltime_bytes_per_sec ?? 0;
  const peakWindow = history?.peak_window_bytes_per_sec ?? throughput?.peak_window_bytes_per_sec ?? 0;
  const samples = history?.samples ?? [];
  const resolution = history?.resolution_seconds ?? 1;

  return (
    <Panel
      title="Download speed"
      meta={
        <StatusBadge tone={cap > 0 ? "warn" : "ok"} dot>
          <Gauge size={12} />
          {cap > 0 ? `${formatBytes(cap)}/s cap` : "uncapped"}
        </StatusBadge>
      }
      actions={
        <div className="speed-ranges">
          {(["5m", "1h", "6h", "24h", "7d"] as SpeedHistoryRange[]).map((r) => (
            <button
              key={r}
              type="button"
              className={"speed-range-btn" + (r === range ? " is-active" : "")}
              onClick={() => setRange(r)}
            >
              {r}
            </button>
          ))}
        </div>
      }
    >
      <SpeedChart
        samples={samples}
        capBytesPerSec={cap}
        peakAllTimeBytesPerSec={peakAllTime}
        peakWindowBytesPerSec={peakWindow}
        resolutionSeconds={resolution}
      />
      <div className="speed-footer">
        <div className="speed-stats">
          <span><strong>Now</strong> {formatBytes(throughput?.current_bytes_per_sec ?? 0)}/s</span>
          <span><strong>Peak (window)</strong> {formatBytes(peakWindow)}/s</span>
          <span><strong>Peak (all-time)</strong> {formatBytes(peakAllTime)}/s</span>
        </div>
        <form className="speed-throttle" onSubmit={submitCap}>
          <label>
            <span>Throttle MB/s</span>
            <input
              type="number"
              min={0}
              step={0.5}
              value={draftCap}
              onChange={(e) => setDraftCap(e.target.value)}
              placeholder="0"
            />
          </label>
          <Button variant="primary" type="submit" disabled={savingCap}>
            {savingCap ? "Saving…" : "Apply"}
          </Button>
        </form>
      </div>
    </Panel>
  );
}

function round1(n: number): number {
  return Math.round(n * 10) / 10;
}

function BackupsPanel() {
  const [backups, setBackups] = useState<BackupFile[]>([]);
  const [busy, setBusy] = useState(false);
  const toast = useToasts();

  const reload = async () => {
    try {
      const r = await api.listBackups();
      setBackups(r.backups ?? []);
    } catch {
      /* ignore */
    }
  };

  useEffect(() => {
    void reload();
    const t = setInterval(() => void reload(), 60_000);
    return () => clearInterval(t);
  }, []);

  const runNow = async () => {
    setBusy(true);
    try {
      const r = await api.runBackup();
      setBackups(r.backups ?? []);
      toast.success("Backup completed");
    } catch (e) {
      toast.error("Backup failed", { message: e instanceof Error ? e.message : String(e) });
    } finally {
      setBusy(false);
    }
  };

  return (
    <Panel
      title="Backups"
      meta={backups.length > 0 && <StatusBadge tone="neutral">{backups.length} files</StatusBadge>}
      actions={
        <Button
          variant="secondary"
          icon={<Database size={14} />}
          onClick={() => void runNow()}
          disabled={busy}
        >
          Back up now
        </Button>
      }
    >
      {backups.length === 0 ? (
        <p className="muted">No backups yet. Weekly auto-backup runs in the scheduler; click "Back up now" to take one immediately.</p>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>File</th>
              <th>Size</th>
              <th>Created</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {backups.map((b) => (
              <tr key={b.name}>
                <td><code className="inline-code">{b.name}</code></td>
                <td className="muted">{formatBytes(b.size_bytes)}</td>
                <td className="muted">{new Date(b.created_at).toLocaleString()}</td>
                <td>
                  <a
                    className="link"
                    href={api.backupURL(b.name)}
                    target="_blank"
                    rel="noopener noreferrer"
                    download={b.name}
                  >
                    Download
                  </a>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

function CommandsPanel() {
  const [commands, setCommands] = useState<Command[]>([]);
  const [names, setNames] = useState<string[]>([]);
  const [selected, setSelected] = useState<string>("");
  const [submitting, setSubmitting] = useState(false);
  const toast = useToasts();

  const reload = async () => {
    try {
      const resp = await api.listCommands(20);
      setCommands(resp.commands ?? []);
    } catch {
      /* ignore */
    }
  };

  useEffect(() => {
    void reload();
    void api.commandNames().then((r) => {
      setNames(r.names ?? []);
      if (r.names && r.names.length > 0) setSelected(r.names[0]);
    });
    const t = setInterval(() => void reload(), 3000);
    return () => clearInterval(t);
  }, []);

  const submit = async () => {
    if (!selected) return;
    setSubmitting(true);
    try {
      await api.submitCommand(selected);
      toast.success(`Queued command: ${selected}`);
      await reload();
    } catch (e) {
      toast.error("Could not queue command", { message: e instanceof Error ? e.message : String(e) });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Panel
      title="Commands"
      meta={commands.length > 0 && <StatusBadge tone="neutral">{commands.length} recent</StatusBadge>}
    >
      <div className="commands-trigger">
        <select
          className="select"
          value={selected}
          onChange={(e) => setSelected(e.target.value)}
        >
          {names.map((n) => (
            <option key={n} value={n}>{n}</option>
          ))}
        </select>
        <Button
          variant="primary"
          icon={<Send size={14} />}
          onClick={() => void submit()}
          disabled={submitting || !selected}
        >
          Run
        </Button>
      </div>
      {commands.length === 0 ? (
        <p className="muted">No commands run yet.</p>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Trigger</th>
              <th>Status</th>
              <th>Duration</th>
              <th>Queued</th>
            </tr>
          </thead>
          <tbody>
            {commands.map((c) => (
              <tr key={c.id}>
                <td>
                  {c.name}
                  {c.error && (
                    <div className="text-err" title={c.error}>{c.error}</div>
                  )}
                </td>
                <td className="muted">{c.trigger}</td>
                <td>{commandTone(c)}</td>
                <td className="muted">{c.duration_ms ? `${c.duration_ms} ms` : "—"}</td>
                <td className="muted">{new Date(c.queued_at).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

function commandTone(c: Command) {
  if (c.status === "queued") return <StatusBadge tone="neutral">queued</StatusBadge>;
  if (c.status === "running") return <StatusBadge tone="info" dot>running</StatusBadge>;
  if (c.result === "failed") return <StatusBadge tone="err" dot>failed</StatusBadge>;
  return <StatusBadge tone="ok" dot>success</StatusBadge>;
}

function LogFilesPanel() {
  const [files, setFiles] = useState<LogFile[]>([]);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const reload = async () => {
      try {
        const resp = await api.logFiles();
        if (!cancelled) {
          setFiles(resp.files ?? []);
          setErr(null);
        }
      } catch (e) {
        if (!cancelled) setErr(e instanceof Error ? e.message : String(e));
      }
    };
    void reload();
    const t = setInterval(() => void reload(), 30_000);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, []);

  return (
    <Panel
      title="Log files"
      meta={files.length > 0 && <StatusBadge tone="neutral">{files.length} files</StatusBadge>}
    >
      {err && <p className="text-err">{err}</p>}
      {!err && files.length === 0 && <p className="muted">No log files yet.</p>}
      {files.length > 0 && (
        <table className="table">
          <thead>
            <tr>
              <th>File</th>
              <th>Size</th>
              <th>Updated</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {files.map((f) => (
              <tr key={f.name}>
                <td>
                  <code className="inline-code">{f.name}</code>
                  {f.active && (
                    <StatusBadge tone="ok" dot>
                      live
                    </StatusBadge>
                  )}
                </td>
                <td className="muted">{formatBytes(f.size_bytes)}</td>
                <td className="muted">{new Date(f.updated_at).toLocaleString()}</td>
                <td>
                  <a
                    className="link"
                    href={api.logFileURL(f.name)}
                    target="_blank"
                    rel="noopener noreferrer"
                    download={f.name}
                  >
                    Download
                  </a>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

function DiskSpacePanel() {
  const [entries, setEntries] = useState<DiskEntry[]>([]);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const resp = await api.diskspace();
        if (!cancelled) setEntries(resp.entries ?? []);
      } catch {
        /* ignore */
      }
    };
    void tick();
    const t = setInterval(() => void tick(), 30_000);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, []);

  return (
    <Panel title="Disk space">
      {entries.length === 0 ? (
        <p className="muted">Loading…</p>
      ) : (
        <ul className="disk-list">
          {entries.map((e) => (
            <li key={e.path} className="disk-row">
              <div className="disk-head">
                <span className="disk-label">{e.label}</span>
                <code className="muted disk-path">{e.path}</code>
              </div>
              {!e.reachable ? (
                <p className="text-err">{e.error || "unreachable"}</p>
              ) : (
                <>
                  <div
                    className={
                      "disk-bar " +
                      (e.free_bytes < 1 << 30
                        ? "disk-bar-crit"
                        : e.free_bytes < 5 * (1 << 30)
                          ? "disk-bar-warn"
                          : "")
                    }
                  >
                    <div
                      className="disk-bar-fill"
                      style={{ width: `${pct(e.used_bytes, e.total_bytes)}%` }}
                    />
                  </div>
                  <div className="disk-meta muted">
                    {formatBytes(e.free_bytes)} free of {formatBytes(e.total_bytes)}
                  </div>
                </>
              )}
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}

function pct(used: number, total: number): number {
  if (total <= 0) return 0;
  return Math.min(100, Math.max(0, Math.round((used / total) * 100)));
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

// TasksPanel lists every recurring + oneshot scheduled task with
// last/next-run timestamps. The Run-Now button pulls next_run_at to
// now so the scheduler's normal dispatch picks the task up on its
// next tick — keeps the claim model intact and avoids blocking the
// HTTP request on long-running tasks.
function TasksPanel() {
  const [tasks, setTasks] = useState<ScheduledTask[]>([]);
  const [busy, setBusy] = useState<Set<number>>(() => new Set());
  const toast = useToasts();

  const reload = async () => {
    try {
      const resp = await api.systemTasks();
      setTasks(resp.tasks ?? []);
    } catch {
      /* ignore */
    }
  };

  useEffect(() => {
    void reload();
    const t = setInterval(() => void reload(), 5000);
    return () => clearInterval(t);
  }, []);

  const runNow = async (id: number) => {
    setBusy((s) => new Set(s).add(id));
    try {
      const resp = await api.runTaskNow(id);
      setTasks((cur) => cur.map((t) => (t.id === id ? resp.task : t)));
      toast.success(`Task scheduled to run`);
    } catch (e) {
      toast.error("Could not run task", { message: e instanceof Error ? e.message : String(e) });
    } finally {
      setBusy((s) => {
        const n = new Set(s);
        n.delete(id);
        return n;
      });
    }
  };

  return (
    <Panel title="Scheduled tasks" meta={<StatusBadge tone="neutral">{tasks.length} tasks</StatusBadge>}>
      {tasks.length === 0 ? (
        <p className="muted">No scheduled tasks registered.</p>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Cadence</th>
              <th>Last run</th>
              <th>Next run</th>
              <th>Status</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {tasks.map((t) => (
              <tr key={t.id}>
                <td>{t.name}</td>
                <td className="muted">
                  {t.kind === "oneshot" ? "one-shot" : formatUptime(t.cadence_seconds * 1000)}
                </td>
                <td className="muted">{t.last_run_at ? new Date(t.last_run_at).toLocaleString() : "never"}</td>
                <td className="muted">{new Date(t.next_run_at).toLocaleString()}</td>
                <td>
                  {t.status === "running" ? (
                    <StatusBadge tone="info" dot>running</StatusBadge>
                  ) : t.last_error ? (
                    <StatusBadge tone="err" dot>{`failed (${t.consecutive_failures}×)`}</StatusBadge>
                  ) : !t.enabled ? (
                    <StatusBadge tone="neutral">disabled</StatusBadge>
                  ) : (
                    <StatusBadge tone="ok" dot>idle</StatusBadge>
                  )}
                </td>
                <td>
                  <Button
                    variant="ghost"
                    icon={<PlayCircle size={14} />}
                    onClick={() => void runNow(t.id)}
                    disabled={busy.has(t.id) || t.status === "running"}
                    title="Pull next run forward to now"
                  >
                    Run now
                  </Button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}
