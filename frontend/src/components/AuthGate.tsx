import { useEffect, useState, type ReactNode } from "react";
import { KeyRound } from "lucide-react";
import { api, getApiKey, setApiKey } from "../api/client";
import Button from "./Button";

// AuthGate blocks rendering of children until a valid API key is
// stored in localStorage.
//
// On first render: probes /api/v1/whoami with the stored key (if any).
// If unset or rejected, shows a small splash form. Submitting a key
// re-probes; on 200 it persists and the gate lifts.
//
// The key is shown in the daemon's first-run log and in config.toml
// under [auth] api_key. The user pastes it once.
export default function AuthGate({ children }: { children: ReactNode }) {
  type Phase = "checking" | "ok" | "needs-key" | "submitting";
  const [phase, setPhase] = useState<Phase>("checking");
  const [input, setInput] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    if (!getApiKey()) {
      setPhase("needs-key");
      return;
    }
    api
      .whoami()
      .then(() => {
        if (!cancelled) setPhase("ok");
      })
      .catch(() => {
        if (!cancelled) setPhase("needs-key");
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setPhase("submitting");
    setError(null);
    setApiKey(input.trim());
    try {
      await api.whoami();
      setPhase("ok");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setPhase("needs-key");
    }
  };

  if (phase === "ok") return <>{children}</>;
  if (phase === "checking") {
    return <div className="auth-gate"><p className="muted">Checking…</p></div>;
  }

  return (
    <div className="auth-gate">
      <form className="auth-form" onSubmit={onSubmit}>
        <div className="auth-icon" aria-hidden="true">
          <KeyRound size={22} />
        </div>
        <h1>API key required</h1>
        <p className="muted">
          Paste the value from <code className="inline-code">[auth] api_key</code> in
          your <code className="inline-code">config.toml</code> (or the
          <code className="inline-code"> hoardarr</code> daemon's first-run log).
        </p>
        <input
          type="password"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="32 hex characters"
          autoComplete="off"
          autoFocus
          spellCheck={false}
        />
        {error && <p className="text-err">{error}</p>}
        <Button
          variant="primary"
          type="submit"
          disabled={phase === "submitting" || input.trim().length === 0}
        >
          {phase === "submitting" ? "Verifying…" : "Continue"}
        </Button>
      </form>
    </div>
  );
}
