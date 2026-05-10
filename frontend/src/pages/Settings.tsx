import { useEffect, useState } from "react";
import {
  Server as ServerIcon,
  FolderTree,
  HardDrive,
  Plus,
  Trash2,
} from "lucide-react";
import Page from "../components/Page";
import Panel from "../components/Panel";
import Button from "../components/Button";
import StatusBadge from "../components/StatusBadge";
import { api, ApiError } from "../api/client";
import type { Category, Paths, Server } from "../api/types";

export default function Settings() {
  return (
    <Page
      title="Settings"
      subtitle="Configure servers, categories and paths"
    >
      <ServersSection />
      <CategoriesSection />
      <PathsSection />
    </Page>
  );
}

// --- Servers --------------------------------------------------------

function ServersSection() {
  const [servers, setServers] = useState<Server[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

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
                <td className="muted">{s.priority}</td>
                <td className="queue-row-actions">
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

      <AddServerForm onAdded={() => void refresh()} />
    </Panel>
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
      });
      setName("");
      setHost("");
      setUsername("");
      setPassword("");
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
