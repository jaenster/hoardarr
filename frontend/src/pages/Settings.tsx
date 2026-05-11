import { useEffect, useState } from "react";
import {
  Server as ServerIcon,
  FolderTree,
  HardDrive,
  Plus,
  Trash2,
  Settings2,
  KeyRound,
  Plug,
  Copy,
  Eye,
  EyeOff,
  Webhook,
  Send,
  Gauge,
  Pencil,
  PlugZap,
  X,
} from "lucide-react";
import Page from "../components/Page";
import Panel from "../components/Panel";
import Button from "../components/Button";
import StatusBadge from "../components/StatusBadge";
import { api, ApiError, type TestServerResult } from "../api/client";
import type {
  BandwidthConfig,
  Category,
  General,
  Paths,
  Server,
  Subscription,
  User,
} from "../api/types";

export default function Settings() {
  return (
    <Page
      title="Settings"
      subtitle="Servers, categories, paths, and integration endpoints"
    >
      <section id="servers"><ServersSection /></section>
      <section id="categories"><CategoriesSection /></section>
      <section id="paths"><PathsSection /></section>
      <section id="bandwidth"><BandwidthSection /></section>
      <section id="general"><GeneralSection /></section>
      <section id="authentication"><AuthSection /></section>
      <section id="sab-compat"><SABSection /></section>
      <section id="connect"><WebhooksSection /></section>
    </Page>
  );
}

// --- Servers --------------------------------------------------------

function ServersSection() {
  const [servers, setServers] = useState<Server[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState<Server | null>(null);
  const [probeBusy, setProbeBusy] = useState<number | null>(null);
  const [probeResult, setProbeResult] = useState<{ id: number; result: TestServerResult } | null>(null);

  const refresh = async () => {
    setLoading(true);
    try {
      const r = await api.listServers();
      setServers(r.servers ?? []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  const remove = async (id: number) => {
    if (!confirm("Remove this server?")) return;
    try {
      await api.removeServer(id);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const testExisting = async (id: number) => {
    setProbeBusy(id);
    setProbeResult(null);
    try {
      const result = await api.testExistingServer(id);
      setProbeResult({ id, result });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setProbeBusy(null);
    }
  };

  return (
    <Panel
      title="Usenet Servers"
      meta={
        <StatusBadge tone="neutral">
          <ServerIcon size={12} />
          {servers.length} configured
        </StatusBadge>
      }
    >
      {error && <p className="text-err">{error}</p>}

      {!loading && servers.length === 0 ? (
        <p className="muted">No servers yet — add one below to start downloading.</p>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Endpoint</th>
              <th>TLS</th>
              <th>Conns</th>
              <th>Priority</th>
              <th>Type</th>
              <th>Used</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {servers.map((s) => (
              <tr key={s.id}>
                <td>{s.name}</td>
                <td className="muted">
                  {s.host}:{s.port}
                </td>
                <td>
                  <StatusBadge tone={s.tls ? "ok" : "warn"} dot>
                    {s.tls ? "TLS" : "plain"}
                  </StatusBadge>
                </td>
                <td className="muted">{s.max_conns}</td>
                <td className="muted">
                  {s.priority}
                  {s.backup ? " (backup)" : ""}
                </td>
                <td>
                  <StatusBadge tone={s.billing_mode === "metered" ? "warn" : "ok"} dot>
                    {s.billing_mode === "metered" ? "metered" : "flat"}
                  </StatusBadge>
                </td>
                <td className="muted">
                  {s.billing_mode === "metered"
                    ? `${formatBytesShort(s.used_bytes)} / ${
                        s.quota_bytes ? formatBytesShort(s.quota_bytes) : "∞"
                      }`
                    : formatBytesShort(s.used_bytes)}
                </td>
                <td className="queue-row-actions">
                  <button
                    type="button"
                    className="icon-btn"
                    aria-label="Test connection"
                    title="Test connection"
                    disabled={probeBusy === s.id}
                    onClick={() => void testExisting(s.id)}
                  >
                    <PlugZap size={14} />
                  </button>
                  <button
                    type="button"
                    className="icon-btn"
                    aria-label="Edit server"
                    title="Edit"
                    onClick={() => setEditing(s)}
                  >
                    <Pencil size={14} />
                  </button>
                  <button
                    type="button"
                    className="icon-btn icon-btn-danger"
                    aria-label="Remove server"
                    onClick={() => void remove(s.id)}
                  >
                    <Trash2 size={14} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {probeResult && (
        <ProbeBanner
          result={probeResult.result}
          onDismiss={() => setProbeResult(null)}
        />
      )}

      {editing && (
        <EditServerForm
          server={editing}
          onClose={() => setEditing(null)}
          onSaved={async () => {
            setEditing(null);
            await refresh();
          }}
        />
      )}

      <AddServerForm onAdded={() => void refresh()} />
    </Panel>
  );
}

// ProbeBanner renders the result of a connection probe. The hint-list
// shows each handshake step as ok/fail so the operator can pinpoint
// where the failure is (dial vs auth vs MODE READER vs DATE).
function ProbeBanner({
  result,
  onDismiss,
}: {
  result: TestServerResult;
  onDismiss: () => void;
}) {
  const steps: { label: string; ok: boolean }[] = [
    { label: "Dial", ok: result.dial },
    { label: "Greeting", ok: result.greeted },
    { label: "Auth", ok: result.auth },
    { label: "MODE READER", ok: result.mode_reader },
    { label: "DATE", ok: result.date },
  ];
  return (
    <div className={`probe-banner ${result.ok ? "probe-ok" : "probe-fail"}`}>
      <div className="probe-summary">
        <strong>{result.ok ? "Connection OK" : "Connection failed"}</strong>
        <span className="muted"> · {result.elapsed_ms} ms</span>
        <button
          type="button"
          className="icon-btn"
          aria-label="Dismiss"
          onClick={onDismiss}
        >
          <X size={14} />
        </button>
      </div>
      <ul className="probe-steps">
        {steps.map((s) => (
          <li key={s.label} className={s.ok ? "probe-step-ok" : "probe-step-fail"}>
            <span className="probe-step-dot" /> {s.label}
          </li>
        ))}
      </ul>
      {result.server_date && (
        <p className="muted">Server time: {result.server_date}</p>
      )}
      {result.err && <p className="text-err">{result.err}</p>}
    </div>
  );
}

// EditServerForm is the same shape as AddServerForm but pre-populated
// and submitting via PATCH. We don't surface the existing password —
// leaving the field blank means "don't change it"; typing a new value
// replaces it.
function EditServerForm({
  server,
  onClose,
  onSaved,
}: {
  server: Server;
  onClose: () => void;
  onSaved: () => void | Promise<void>;
}) {
  const [host, setHost] = useState(server.host);
  const [port, setPort] = useState(server.port);
  const [tls, setTls] = useState(server.tls);
  const [username, setUsername] = useState(server.username ?? "");
  const [password, setPassword] = useState("");
  const [maxConns, setMaxConns] = useState(server.max_conns);
  const [priority, setPriority] = useState(server.priority);
  const [backup, setBackup] = useState(server.backup);
  const [billingMode, setBillingMode] = useState<"flat" | "metered">(
    server.billing_mode === "metered" ? "metered" : "flat",
  );
  const [quotaGB, setQuotaGB] = useState(
    server.quota_bytes ? Math.round(server.quota_bytes / 1024 / 1024 / 1024) : 0,
  );
  const [bandwidthMBPerSec, setBandwidthMBPerSec] = useState(
    server.bandwidth_bytes_per_sec
      ? Math.round((server.bandwidth_bytes_per_sec / 1024 / 1024) * 10) / 10
      : 0,
  );
  const [submitting, setSubmitting] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [probe, setProbe] = useState<TestServerResult | null>(null);
  const [probing, setProbing] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setErr(null);
    try {
      await api.patchServer(server.id, {
        host: host.trim(),
        port,
        tls,
        username: username.trim(),
        // Empty password = don't change. The backend only mutates when
        // the field is present, so we omit it entirely when blank.
        ...(password ? { password } : {}),
        max_conns: maxConns,
        priority,
        backup,
        billing_mode: billingMode,
        quota_bytes:
          billingMode === "metered" && quotaGB > 0
            ? Math.round(quotaGB * 1024 * 1024 * 1024)
            : 0,
        bandwidth_bytes_per_sec:
          bandwidthMBPerSec > 0
            ? Math.round(bandwidthMBPerSec * 1024 * 1024)
            : 0,
      });
      await onSaved();
    } catch (e) {
      if (e instanceof ApiError) {
        const body = e.body as { error?: string } | null;
        setErr(body?.error ?? e.message);
      } else {
        setErr(e instanceof Error ? e.message : String(e));
      }
    } finally {
      setSubmitting(false);
    }
  };

  const runTest = async () => {
    setProbing(true);
    setProbe(null);
    try {
      const result = await api.testServer({
        host: host.trim(),
        port,
        tls,
        username: username.trim() || undefined,
        password: password || undefined,
      });
      setProbe(result);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setProbing(false);
    }
  };

  return (
    <form className="settings-form" onSubmit={submit}>
      <div className="settings-form-header">
        <h3>Edit {server.name}</h3>
        <button
          type="button"
          className="icon-btn"
          aria-label="Close edit"
          onClick={onClose}
        >
          <X size={14} />
        </button>
      </div>
      <div className="settings-row">
        <label className="settings-field">
          <span>Host</span>
          <input value={host} onChange={(e) => setHost(e.target.value)} />
        </label>
        <label className="settings-field settings-field-narrow">
          <span>Port</span>
          <input
            type="number"
            value={port}
            onChange={(e) => setPort(Number(e.target.value))}
          />
        </label>
        <label className="settings-checkbox">
          <input
            type="checkbox"
            checked={tls}
            onChange={(e) => setTls(e.target.checked)}
          />
          <span>TLS</span>
        </label>
      </div>
      <div className="settings-row">
        <label className="settings-field">
          <span>Username</span>
          <input
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoComplete="off"
          />
        </label>
        <label className="settings-field">
          <span>Password</span>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="leave blank to keep current"
            autoComplete="new-password"
          />
        </label>
        <label className="settings-field settings-field-narrow">
          <span>Max conns</span>
          <input
            type="number"
            value={maxConns}
            onChange={(e) => setMaxConns(Number(e.target.value))}
          />
        </label>
        <label className="settings-field settings-field-narrow">
          <span>Priority</span>
          <input
            type="number"
            value={priority}
            onChange={(e) => setPriority(Number(e.target.value))}
          />
        </label>
        <label className="settings-checkbox">
          <input
            type="checkbox"
            checked={backup}
            onChange={(e) => setBackup(e.target.checked)}
          />
          <span>Backup</span>
        </label>
      </div>
      <div className="settings-row">
        <label className="settings-field">
          <span>Billing</span>
          <select
            value={billingMode}
            onChange={(e) => setBillingMode(e.target.value as "flat" | "metered")}
          >
            <option value="flat">Flat (unlimited)</option>
            <option value="metered">Metered (block / pay-per-byte)</option>
          </select>
        </label>
        {billingMode === "metered" && (
          <label className="settings-field settings-field-narrow">
            <span>Quota (GB)</span>
            <input
              type="number"
              min={0}
              step={1}
              value={quotaGB}
              onChange={(e) => setQuotaGB(Number(e.target.value))}
              placeholder="0 = unlimited"
            />
          </label>
        )}
        <label className="settings-field settings-field-narrow">
          <span>Speed cap (MB/s)</span>
          <input
            type="number"
            min={0}
            step={0.5}
            value={bandwidthMBPerSec}
            onChange={(e) => setBandwidthMBPerSec(Number(e.target.value))}
            placeholder="0 = no cap"
          />
        </label>
      </div>
      {err && <p className="text-err">{err}</p>}
      {probe && <ProbeBanner result={probe} onDismiss={() => setProbe(null)} />}
      <div className="settings-form-actions">
        <Button
          variant="ghost"
          type="button"
          icon={<PlugZap size={14} />}
          disabled={probing || !host.trim() || port <= 0}
          onClick={() => void runTest()}
        >
          {probing ? "Testing…" : "Test connection"}
        </Button>
        <Button variant="primary" type="submit" disabled={submitting}>
          {submitting ? "Saving…" : "Save changes"}
        </Button>
      </div>
    </form>
  );
}

function AddServerForm({ onAdded }: { onAdded: () => void }) {
  const [name, setName] = useState("");
  const [host, setHost] = useState("");
  const [port, setPort] = useState(563);
  const [tls, setTls] = useState(true);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [maxConns, setMaxConns] = useState(8);
  const [priority, setPriority] = useState(0);
  const [backup, setBackup] = useState(false);
  const [billingMode, setBillingMode] = useState<"flat" | "metered">("flat");
  const [quotaGB, setQuotaGB] = useState(0); // operator-friendly: GB; converted to bytes on submit
  const [bandwidthMBPerSec, setBandwidthMBPerSec] = useState(0); // 0 = no per-server cap
  const [submitting, setSubmitting] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setErr(null);
    try {
      await api.addServer({
        name: name.trim(),
        host: host.trim(),
        port,
        tls,
        username: username.trim() || undefined,
        password: password || undefined,
        max_conns: maxConns,
        priority,
        backup,
        billing_mode: billingMode,
        quota_bytes:
          billingMode === "metered" && quotaGB > 0
            ? Math.round(quotaGB * 1024 * 1024 * 1024)
            : 0,
        bandwidth_bytes_per_sec:
          bandwidthMBPerSec > 0
            ? Math.round(bandwidthMBPerSec * 1024 * 1024)
            : 0,
      });
      setName("");
      setHost("");
      setUsername("");
      setPassword("");
      setBackup(false);
      setBillingMode("flat");
      setQuotaGB(0);
      setBandwidthMBPerSec(0);
      onAdded();
    } catch (e) {
      if (e instanceof ApiError) {
        const body = e.body as { error?: string } | null;
        setErr(body?.error ?? e.message);
      } else {
        setErr(e instanceof Error ? e.message : String(e));
      }
    } finally {
      setSubmitting(false);
    }
  };

  const valid =
    name.trim().length > 0 &&
    host.trim().length > 0 &&
    port > 0 &&
    port <= 65535 &&
    maxConns > 0;

  return (
    <form className="settings-form" onSubmit={submit}>
      <h3>Add server</h3>
      <div className="settings-row">
        <label className="settings-field">
          <span>Name</span>
          <input value={name} onChange={(e) => setName(e.target.value)} />
        </label>
        <label className="settings-field">
          <span>Host</span>
          <input value={host} onChange={(e) => setHost(e.target.value)} />
        </label>
        <label className="settings-field settings-field-narrow">
          <span>Port</span>
          <input
            type="number"
            value={port}
            onChange={(e) => setPort(Number(e.target.value))}
          />
        </label>
        <label className="settings-checkbox">
          <input
            type="checkbox"
            checked={tls}
            onChange={(e) => setTls(e.target.checked)}
          />
          <span>TLS</span>
        </label>
      </div>
      <div className="settings-row">
        <label className="settings-field">
          <span>Username</span>
          <input
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoComplete="off"
          />
        </label>
        <label className="settings-field">
          <span>Password</span>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="new-password"
          />
        </label>
        <label className="settings-field settings-field-narrow">
          <span>Max conns</span>
          <input
            type="number"
            value={maxConns}
            onChange={(e) => setMaxConns(Number(e.target.value))}
          />
        </label>
        <label className="settings-field settings-field-narrow">
          <span>Priority</span>
          <input
            type="number"
            value={priority}
            onChange={(e) => setPriority(Number(e.target.value))}
          />
        </label>
        <label className="settings-checkbox">
          <input
            type="checkbox"
            checked={backup}
            onChange={(e) => setBackup(e.target.checked)}
          />
          <span>Backup</span>
        </label>
      </div>
      <div className="settings-row">
        <label className="settings-field">
          <span>Billing</span>
          <select
            value={billingMode}
            onChange={(e) => setBillingMode(e.target.value as "flat" | "metered")}
          >
            <option value="flat">Flat (unlimited)</option>
            <option value="metered">Metered (block / pay-per-byte)</option>
          </select>
        </label>
        {billingMode === "metered" && (
          <label className="settings-field settings-field-narrow">
            <span>Quota (GB)</span>
            <input
              type="number"
              min={0}
              step={1}
              value={quotaGB}
              onChange={(e) => setQuotaGB(Number(e.target.value))}
              placeholder="0 = unlimited"
            />
          </label>
        )}
        <label className="settings-field settings-field-narrow">
          <span>Speed cap (MB/s)</span>
          <input
            type="number"
            min={0}
            step={0.5}
            value={bandwidthMBPerSec}
            onChange={(e) => setBandwidthMBPerSec(Number(e.target.value))}
            placeholder="0 = no cap"
          />
        </label>
      </div>
      {err && <p className="text-err">{err}</p>}
      <Button
        variant="primary"
        type="submit"
        icon={<Plus size={14} />}
        disabled={!valid || submitting}
      >
        {submitting ? "Adding…" : "Add server"}
      </Button>
    </form>
  );
}

// --- shared helpers --------------------------------------------------

function formatBytesShort(n: number): string {
  if (n <= 0) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 && i > 0 ? 1 : 0)} ${units[i]}`;
}

// --- Categories -----------------------------------------------------

function CategoriesSection() {
  const [cats, setCats] = useState<Category[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [dir, setDir] = useState("");
  const [priority, setPriority] = useState(0);

  const refresh = async () => {
    try {
      const r = await api.listCategories();
      setCats(r.categories ?? []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  const add = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await api.upsertCategory({ name: name.trim(), dir: dir.trim(), priority });
      setName("");
      setDir("");
      setPriority(0);
      await refresh();
    } catch (e) {
      if (e instanceof ApiError) {
        const body = e.body as { error?: string } | null;
        setError(body?.error ?? e.message);
      } else {
        setError(e instanceof Error ? e.message : String(e));
      }
    }
  };

  const remove = async (n: string) => {
    if (n === "*") return;
    if (!confirm(`Remove category "${n}"?`)) return;
    try {
      await api.removeCategory(n);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <Panel
      title="Categories"
      meta={
        <StatusBadge tone="neutral">
          <FolderTree size={12} />
          {cats.length} configured
        </StatusBadge>
      }
    >
      {error && <p className="text-err">{error}</p>}

      <table className="table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Subdirectory</th>
            <th>Priority</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {cats.map((c) => (
            <tr key={c.name}>
              <td>
                {c.name === "*" ? (
                  <>
                    <span className="queue-row-name">{c.name}</span>{" "}
                    <span className="muted">(default)</span>
                  </>
                ) : (
                  c.name
                )}
              </td>
              <td className="muted">{c.dir || "—"}</td>
              <td className="muted">{c.priority}</td>
              <td className="queue-row-actions">
                {c.name !== "*" && (
                  <button
                    type="button"
                    className="icon-btn icon-btn-danger"
                    aria-label="Remove category"
                    onClick={() => void remove(c.name)}
                  >
                    <Trash2 size={14} />
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <form className="settings-form" onSubmit={add}>
        <h3>Add category</h3>
        <div className="settings-row">
          <label className="settings-field">
            <span>Name</span>
            <input value={name} onChange={(e) => setName(e.target.value)} />
          </label>
          <label className="settings-field">
            <span>Subdirectory</span>
            <input
              value={dir}
              onChange={(e) => setDir(e.target.value)}
              placeholder="e.g. movies"
            />
          </label>
          <label className="settings-field settings-field-narrow">
            <span>Priority</span>
            <input
              type="number"
              value={priority}
              onChange={(e) => setPriority(Number(e.target.value))}
            />
          </label>
        </div>
        <Button
          variant="primary"
          type="submit"
          icon={<Plus size={14} />}
          disabled={name.trim().length === 0}
        >
          Add category
        </Button>
      </form>
    </Panel>
  );
}

// --- Paths ----------------------------------------------------------

function PathsSection() {
  const [paths, setPaths] = useState<Paths | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    api
      .paths()
      .then((p) => {
        if (!cancelled) setPaths(p);
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <Panel
      title="Paths"
      meta={
        <StatusBadge tone="neutral">
          <HardDrive size={12} />
          read-only
        </StatusBadge>
      }
    >
      {error && <p className="text-err">{error}</p>}
      {paths ? (
        <>
          <dl className="kv">
            <dt>Data dir</dt>
            <dd>
              <code className="inline-code">{paths.data_dir}</code>
            </dd>
            <dt>Incomplete</dt>
            <dd>
              <code className="inline-code">{paths.incomplete_dir}</code>
            </dd>
            <dt>Complete</dt>
            <dd>
              <code className="inline-code">{paths.complete_dir}</code>
            </dd>
          </dl>
          <p className="muted">
            Paths live in <code className="inline-code">config.toml</code>. Edit
            and restart hoardarr to change them — runtime mutation is unsafe
            while jobs hold open files.
          </p>
        </>
      ) : null}
    </Panel>
  );
}

// --- General --------------------------------------------------------

function GeneralSection() {
  const [gen, setGen] = useState<General | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [revealKey, setRevealKey] = useState(false);

  // URL base is live-editable; we keep a draft separate from the
  // committed value so the user can type freely and save explicitly.
  const [urlBaseDraft, setUrlBaseDraft] = useState("");
  const [urlBaseSaving, setUrlBaseSaving] = useState(false);
  const [urlBaseSavedAt, setUrlBaseSavedAt] = useState<number | null>(null);
  const [urlBaseErr, setUrlBaseErr] = useState<string | null>(null);

  const refresh = async () => {
    try {
      const g = await api.general();
      setGen(g);
      setUrlBaseDraft(g.url_base);
      setErr(null);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  const saveURLBase = async (e: React.FormEvent) => {
    e.preventDefault();
    setUrlBaseSaving(true);
    setUrlBaseErr(null);
    try {
      // Normalise: ensure leading slash, strip trailing slashes.
      let next = urlBaseDraft.trim();
      if (next && !next.startsWith("/")) next = "/" + next;
      next = next.replace(/\/+$/, "");
      await api.setGeneral({ url_base: next });
      setUrlBaseSavedAt(Date.now());
      // Note: the *current* page is still on the old base. Refresh
      // recommended for the SPA to pick up the new prefix.
      await refresh();
    } catch (e) {
      if (e instanceof ApiError) {
        const body = e.body as { error?: string } | null;
        setUrlBaseErr(body?.error ?? e.message);
      } else {
        setUrlBaseErr(e instanceof Error ? e.message : String(e));
      }
    } finally {
      setUrlBaseSaving(false);
    }
  };

  const dirty = gen != null && urlBaseDraft !== gen.url_base;

  return (
    <Panel
      title="General"
      meta={<StatusBadge tone="neutral"><Settings2 size={12} />runtime</StatusBadge>}
    >
      {err && <p className="text-err">{err}</p>}
      {gen ? (
        <>
          <dl className="kv">
            <dt>Listen</dt>
            <dd>
              <code className="inline-code">{gen.listen}</code>
            </dd>
            <dt>API key</dt>
            <dd className="kv-key-row">
              <code className="inline-code kv-key-value">
                {revealKey ? gen.api_key : "•".repeat(gen.api_key.length || 32)}
              </code>
              <button
                type="button"
                className="icon-btn"
                aria-label={revealKey ? "Hide API key" : "Reveal API key"}
                onClick={() => setRevealKey((v) => !v)}
              >
                {revealKey ? <EyeOff size={14} /> : <Eye size={14} />}
              </button>
              <button
                type="button"
                className="icon-btn"
                aria-label="Copy API key"
                onClick={() => void navigator.clipboard.writeText(gen.api_key)}
              >
                <Copy size={14} />
              </button>
            </dd>
            <dt>Log level</dt>
            <dd>
              <code className="inline-code">{gen.log_level}</code>
            </dd>
          </dl>

          <form className="settings-form" onSubmit={saveURLBase}>
            <h3>URL base</h3>
            <p className="muted" style={{ marginTop: 0 }}>
              Mount path behind a reverse proxy (e.g.{" "}
              <code className="inline-code">/hoardarr</code>). Leave empty
              when hoardarr is served at the root. After saving, refresh the
              page so the embedded frontend picks up the new prefix.
            </p>
            <div className="settings-row">
              <label className="settings-field">
                <span>Path</span>
                <input
                  value={urlBaseDraft}
                  onChange={(e) => setUrlBaseDraft(e.target.value)}
                  placeholder="(empty = root)"
                  spellCheck={false}
                />
              </label>
            </div>
            {urlBaseErr && <p className="text-err">{urlBaseErr}</p>}
            {urlBaseSavedAt && !dirty && !urlBaseErr && (
              <p className="muted">
                Saved. Reload the page to apply (or visit{" "}
                <code className="inline-code">{gen.url_base || "/"}</code> directly).
              </p>
            )}
            <Button
              variant="primary"
              type="submit"
              disabled={!dirty || urlBaseSaving}
            >
              {urlBaseSaving ? "Saving…" : "Save URL base"}
            </Button>
          </form>

          <p className="muted">
            Listen address, API key, and log level live in{" "}
            <code className="inline-code">config.toml</code> and require a
            restart to change.
          </p>
        </>
      ) : null}
    </Panel>
  );
}

// --- Authentication -------------------------------------------------

function AuthSection() {
  const [user, setUser] = useState<User | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    api
      .whoami()
      .then((w) => {
        if (cancelled) return;
        if (w.state === "authenticated") setUser(w.user);
      })
      .catch((e) => {
        if (!cancelled) setErr(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <Panel
      title="Authentication"
      meta={
        <StatusBadge tone="ok"><KeyRound size={12} />cookie session</StatusBadge>
      }
    >
      {err && <p className="text-err">{err}</p>}
      {user ? (
        <>
          <dl className="kv">
            <dt>Signed in as</dt>
            <dd>
              <code className="inline-code">{user.username}</code>
            </dd>
            <dt>Role</dt>
            <dd>
              <code className="inline-code">{user.role}</code>
            </dd>
          </dl>
          <ChangePasswordForm />
        </>
      ) : null}
    </Panel>
  );
}

function ChangePasswordForm() {
  const [oldPassword, setOldPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [done, setDone] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErr(null);
    setDone(false);
    if (newPassword.length < 8) {
      setErr("New password must be at least 8 characters.");
      return;
    }
    if (newPassword !== confirm) {
      setErr("New passwords don't match.");
      return;
    }
    setSubmitting(true);
    try {
      await api.changePassword(oldPassword, newPassword);
      setOldPassword("");
      setNewPassword("");
      setConfirm("");
      setDone(true);
    } catch (e) {
      if (e instanceof ApiError) {
        const body = e.body as { error?: string } | null;
        setErr(body?.error ?? e.message);
      } else {
        setErr(e instanceof Error ? e.message : String(e));
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form className="settings-form" onSubmit={submit}>
      <h3>Change password</h3>
      <div className="settings-row">
        <label className="settings-field">
          <span>Current password</span>
          <input
            type="password"
            value={oldPassword}
            onChange={(e) => setOldPassword(e.target.value)}
            autoComplete="current-password"
          />
        </label>
        <label className="settings-field">
          <span>New password (8+)</span>
          <input
            type="password"
            value={newPassword}
            onChange={(e) => setNewPassword(e.target.value)}
            autoComplete="new-password"
          />
        </label>
        <label className="settings-field">
          <span>Confirm new</span>
          <input
            type="password"
            value={confirm}
            onChange={(e) => setConfirm(e.target.value)}
            autoComplete="new-password"
          />
        </label>
      </div>
      {err && <p className="text-err">{err}</p>}
      {done && <p className="muted">Password updated.</p>}
      <Button
        variant="primary"
        type="submit"
        disabled={!oldPassword || !newPassword || !confirm || submitting}
      >
        {submitting ? "Saving…" : "Change password"}
      </Button>
    </form>
  );
}

// --- SAB compat -----------------------------------------------------

function SABSection() {
  const [gen, setGen] = useState<General | null>(null);

  useEffect(() => {
    let cancelled = false;
    api.general().then((g) => {
      if (!cancelled) setGen(g);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <Panel
      title="SABnzbd Compatibility"
      meta={
        <StatusBadge tone="ok"><Plug size={12} />enabled</StatusBadge>
      }
    >
      <p className="muted">
        Configure any *arr client (Sonarr, Radarr, Lidarr, Readarr, Prowlarr)
        with these values to use hoardarr as a drop-in SABnzbd download client.
      </p>
      {gen ? (
        <dl className="kv">
          <dt>Host:Port</dt>
          <dd className="kv-key-row">
            <code className="inline-code kv-key-value">{gen.sab_base}</code>
            <button
              type="button"
              className="icon-btn"
              aria-label="Copy SAB URL"
              onClick={() => void navigator.clipboard.writeText(gen.sab_base)}
            >
              <Copy size={14} />
            </button>
          </dd>
          <dt>API key</dt>
          <dd>
            <span className="muted">— see General panel above</span>
          </dd>
          <dt>Version reported</dt>
          <dd>
            <code className="inline-code">3.7.2</code>{" "}
            <span className="muted">(lies to *arr so it accepts us)</span>
          </dd>
        </dl>
      ) : null}
    </Panel>
  );
}

// --- Connect (webhooks) ---------------------------------------------

const KNOWN_TOPICS = [
  "download.job.created",
  "download.job.download_complete",
  "download.job.download_failed",
  "download.job.completed",
  "download.job.failed",
  "verify.ok",
  "verify.repair_needed",
  "verify.failed",
  "repair.ok",
  "repair.failed",
  "deliver.complete",
  "deliver.failed",
  "extract.complete",
  "extract.failed",
];

function WebhooksSection() {
  const [subs, setSubs] = useState<Subscription[]>([]);
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    try {
      const r = await api.listSubscriptions();
      setSubs(r.subscriptions ?? []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  const remove = async (id: number) => {
    if (!confirm("Remove this webhook?")) return;
    try {
      await api.removeSubscription(id);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const test = async (id: number) => {
    try {
      await api.testSubscription(id);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <Panel
      title="Connect (Webhooks)"
      meta={
        <StatusBadge tone="neutral">
          <Webhook size={12} />
          {subs.length} configured
        </StatusBadge>
      }
    >
      <p className="muted">
        POST job events to any URL — wire hoardarr into Discord / Slack /
        Notifiarr / your own automation. The body is the bus envelope as JSON.
        If you set a secret the body is HMAC-SHA256 signed in the{" "}
        <code className="inline-code">X-Hoardarr-Signature</code> header.
      </p>

      {error && <p className="text-err">{error}</p>}

      {subs.length > 0 ? (
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>URL</th>
              <th>Topics</th>
              <th>Signed</th>
              <th>Last delivery</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {subs.map((s) => (
              <tr key={s.id}>
                <td>{s.name}</td>
                <td>
                  <StatusBadge tone="neutral">{s.kind}</StatusBadge>
                </td>
                <td className="muted kv-key-value">{s.url}</td>
                <td className="muted">{s.topics.length} topics</td>
                <td>
                  <StatusBadge tone={s.has_secret ? "ok" : "neutral"} dot>
                    {s.has_secret ? "HMAC" : "none"}
                  </StatusBadge>
                </td>
                <td className="muted">
                  {s.last_error ? (
                    <span className="text-err" title={s.last_error}>
                      failed
                    </span>
                  ) : s.last_success_at ? (
                    <>ok @ {new Date(s.last_success_at).toLocaleTimeString()}</>
                  ) : (
                    "—"
                  )}
                </td>
                <td className="queue-row-actions">
                  <button
                    type="button"
                    className="icon-btn"
                    aria-label="Send test event"
                    title="Send test event"
                    onClick={() => void test(s.id)}
                  >
                    <Send size={14} />
                  </button>
                  <button
                    type="button"
                    className="icon-btn icon-btn-danger"
                    aria-label="Remove webhook"
                    onClick={() => void remove(s.id)}
                  >
                    <Trash2 size={14} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="muted">No webhooks configured yet.</p>
      )}

      <AddWebhookForm onAdded={() => void refresh()} />
    </Panel>
  );
}

function AddWebhookForm({ onAdded }: { onAdded: () => void }) {
  const [name, setName] = useState("");
  const [url, setUrl] = useState("");
  const [secret, setSecret] = useState("");
  const [kind, setKind] = useState<"webhook" | "discord" | "slack">("webhook");
  const [picked, setPicked] = useState<string[]>(["deliver.complete"]);
  const [submitting, setSubmitting] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const toggle = (t: string) => {
    setPicked((cur) =>
      cur.includes(t) ? cur.filter((x) => x !== t) : [...cur, t],
    );
  };

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setErr(null);
    try {
      await api.addSubscription({
        name: name.trim(),
        url: url.trim(),
        topics: picked,
        secret: secret || undefined,
        kind,
      });
      setName("");
      setUrl("");
      setSecret("");
      setKind("webhook");
      setPicked(["deliver.complete"]);
      onAdded();
    } catch (e) {
      if (e instanceof ApiError) {
        const body = e.body as { error?: string } | null;
        setErr(body?.error ?? e.message);
      } else {
        setErr(e instanceof Error ? e.message : String(e));
      }
    } finally {
      setSubmitting(false);
    }
  };

  const valid = name.trim() && url.trim() && picked.length > 0;

  return (
    <form className="settings-form" onSubmit={submit}>
      <h3>Add webhook</h3>
      <div className="settings-row">
        <label className="settings-field settings-field-narrow">
          <span>Type</span>
          <select
            value={kind}
            onChange={(e) =>
              setKind(e.target.value as "webhook" | "discord" | "slack")
            }
          >
            <option value="webhook">Generic webhook</option>
            <option value="discord">Discord</option>
            <option value="slack">Slack</option>
          </select>
        </label>
        <label className="settings-field">
          <span>Name</span>
          <input value={name} onChange={(e) => setName(e.target.value)} />
        </label>
        <label className="settings-field">
          <span>URL</span>
          <input
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder={
              kind === "discord"
                ? "https://discord.com/api/webhooks/…"
                : kind === "slack"
                  ? "https://hooks.slack.com/services/…"
                  : "https://example.com/hook"
            }
          />
        </label>
        {kind === "webhook" && (
          <label className="settings-field">
            <span>Secret (optional)</span>
            <input
              type="password"
              value={secret}
              onChange={(e) => setSecret(e.target.value)}
              autoComplete="off"
            />
          </label>
        )}
      </div>
      <div className="settings-row settings-topics">
        {KNOWN_TOPICS.map((t) => (
          <label key={t} className="settings-checkbox">
            <input
              type="checkbox"
              checked={picked.includes(t)}
              onChange={() => toggle(t)}
            />
            <span>{t}</span>
          </label>
        ))}
      </div>
      {err && <p className="text-err">{err}</p>}
      <Button
        variant="primary"
        type="submit"
        icon={<Plus size={14} />}
        disabled={!valid || submitting}
      >
        {submitting ? "Adding…" : "Add webhook"}
      </Button>
    </form>
  );
}

// --- Bandwidth ------------------------------------------------------

function BandwidthSection() {
  const [config, setConfig] = useState<BandwidthConfig | null>(null);
  const [draft, setDraft] = useState<string>("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    try {
      const c = await api.bandwidth();
      setConfig(c);
      setDraft(c.global_bytes_per_sec > 0 ? formatMB(c.global_bytes_per_sec) : "0");
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    const mb = Number(draft);
    if (!Number.isFinite(mb) || mb < 0) {
      setError("Speed must be a non-negative number (MB/s).");
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      const bytes = Math.round(mb * 1024 * 1024);
      await api.setBandwidth({ global_bytes_per_sec: bytes });
      await refresh();
    } catch (e) {
      if (e instanceof ApiError) {
        const body = e.body as { error?: string } | null;
        setError(body?.error ?? e.message);
      } else {
        setError(e instanceof Error ? e.message : String(e));
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Panel
      title="Bandwidth"
      meta={
        <StatusBadge tone="neutral">
          <Gauge size={12} />
          {config && config.global_bytes_per_sec > 0
            ? `${formatMB(config.global_bytes_per_sec)} MB/s cap`
            : "no global cap"}
        </StatusBadge>
      }
    >
      <p className="muted">
        Global cap throttles total download throughput across every server.
        Set to 0 for unlimited. Per-server caps (set under each server in the
        list above) apply in addition: the effective rate is the lower of the
        two when both are set.
      </p>
      {error && <p className="text-err">{error}</p>}
      <form className="settings-form" onSubmit={submit}>
        <h3>Global cap</h3>
        <div className="settings-row">
          <label className="settings-field settings-field-narrow">
            <span>MB / second</span>
            <input
              type="number"
              min={0}
              step={0.5}
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder="0 = unlimited"
            />
          </label>
        </div>
        <Button variant="primary" type="submit" disabled={submitting}>
          {submitting ? "Saving…" : "Save"}
        </Button>
      </form>
    </Panel>
  );
}

function formatMB(bytes: number): string {
  return (bytes / (1024 * 1024)).toFixed(1).replace(/\.0$/, "");
}
