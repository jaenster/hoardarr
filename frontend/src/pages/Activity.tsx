import { useEffect, useState } from "react";

type Health = { status: string; service: string };

export default function Activity() {
  const [health, setHealth] = useState<Health | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/v1/health")
      .then((r) => r.json())
      .then(setHealth)
      .catch((e) => setError(String(e)));
  }, []);

  return (
    <div className="page">
      <header className="page-header">
        <h1>Activity</h1>
        <div className="page-actions">
          <button className="btn btn-secondary" disabled>
            Pause
          </button>
          <button className="btn btn-primary" disabled>
            Add NZB
          </button>
        </div>
      </header>

      <section className="panel">
        <div className="panel-header">
          <h2>Queue</h2>
          <span className="muted">0 items</span>
        </div>
        <div className="empty-state">
          <p>Queue is empty.</p>
          <p className="muted">
            The download pipeline isn't built yet — this is the scaffold.
          </p>
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2>Backend</h2>
          {health && <span className="badge ok">{health.status}</span>}
          {error && <span className="badge err">error</span>}
        </div>
        <pre className="code-block">
          {error
            ? error
            : health
              ? JSON.stringify(health, null, 2)
              : "loading..."}
        </pre>
      </section>
    </div>
  );
}
