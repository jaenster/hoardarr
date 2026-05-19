import { lazy, Suspense, useEffect, useState } from "react";
import { KeyRound, UserPlus } from "lucide-react";
import { api, ApiError, type WhoamiState } from "../api/client";
import Button from "./Button";

// LazyApp is the entire authenticated UI — router, pages, charts,
// SSE client, the lot. Vite splits it into its own chunk so a
// logged-out visitor never downloads it.
const LazyApp = lazy(() => import("../App"));

// AuthGate is the front door. It probes /api/v1/auth/whoami once on
// mount, then renders one of:
//
//   - <LazyApp/>                     (state: authenticated)
//   - <SetupAdminForm/>              (state: needs_setup, first run)
//   - <LoginForm/>                   (state: needs_login)
//
// Submitting either form re-probes whoami so the app mounts without
// a page refresh.
export default function AuthGate() {
  const [state, setState] = useState<WhoamiState | "checking" | "error">("checking");
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    setState("checking");
    setError(null);
    try {
      const w = await api.whoami();
      setState(w);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setState("error");
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  if (state === "checking") {
    return <div className="auth-gate"><p className="muted">Checking…</p></div>;
  }
  if (state === "error") {
    return (
      <div className="auth-gate">
        <div className="auth-form">
          <h1>Can't reach server</h1>
          <p className="text-err">{error}</p>
          <Button variant="primary" onClick={() => void refresh()}>Retry</Button>
        </div>
      </div>
    );
  }
  if (state.state === "authenticated") {
    return (
      <Suspense fallback={<div className="auth-gate"><p className="muted">Loading…</p></div>}>
        <LazyApp />
      </Suspense>
    );
  }
  if (state.state === "needs_setup") {
    return <SetupAdminForm onDone={refresh} />;
  }
  return <LoginForm onDone={refresh} />;
}

function SetupAdminForm({ onDone }: { onDone: () => void }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const valid =
    username.trim().length > 0 &&
    password.length >= 8 &&
    password === confirm;

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!valid) return;
    setSubmitting(true);
    setErr(null);
    try {
      await api.setupAdmin(username.trim(), password);
      onDone();
    } catch (e) {
      if (e instanceof ApiError) {
        const body = e.body as { error?: string } | null;
        setErr(body?.error ?? e.message);
      } else {
        setErr(e instanceof Error ? e.message : String(e));
      }
      setSubmitting(false);
    }
  };

  return (
    <div className="auth-gate">
      <form className="auth-form" onSubmit={submit}>
        <div className="auth-icon" aria-hidden="true">
          <UserPlus size={22} />
        </div>
        <h1>Welcome to hoardarr</h1>
        <p className="muted">
          No users exist yet. Create the first admin account.
        </p>
        <label className="auth-field">
          <span>Username</span>
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoComplete="username"
            autoFocus
            spellCheck={false}
          />
        </label>
        <label className="auth-field">
          <span>Password (8+ chars)</span>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="new-password"
          />
        </label>
        <label className="auth-field">
          <span>Confirm password</span>
          <input
            type="password"
            value={confirm}
            onChange={(e) => setConfirm(e.target.value)}
            autoComplete="new-password"
          />
        </label>
        {err && <p className="text-err">{err}</p>}
        {confirm && password !== confirm && (
          <p className="text-err">Passwords don't match.</p>
        )}
        <Button variant="primary" type="submit" disabled={!valid || submitting}>
          {submitting ? "Creating…" : "Create admin"}
        </Button>
      </form>
    </div>
  );
}

function LoginForm({ onDone }: { onDone: () => void }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const valid = username.trim().length > 0 && password.length > 0;

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!valid) return;
    setSubmitting(true);
    setErr(null);
    try {
      await api.login(username.trim(), password);
      onDone();
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) {
        setErr("Invalid username or password.");
      } else if (e instanceof Error) {
        setErr(e.message);
      } else {
        setErr(String(e));
      }
      setSubmitting(false);
    }
  };

  return (
    <div className="auth-gate">
      <form className="auth-form" onSubmit={submit}>
        <div className="auth-icon" aria-hidden="true">
          <KeyRound size={22} />
        </div>
        <h1>Sign in</h1>
        <label className="auth-field">
          <span>Username</span>
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoComplete="username"
            autoFocus
            spellCheck={false}
          />
        </label>
        <label className="auth-field">
          <span>Password</span>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="current-password"
          />
        </label>
        {err && <p className="text-err">{err}</p>}
        <Button variant="primary" type="submit" disabled={!valid || submitting}>
          {submitting ? "Signing in…" : "Sign in"}
        </Button>
      </form>
    </div>
  );
}
