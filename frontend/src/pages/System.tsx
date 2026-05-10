export default function System() {
  return (
    <div className="page">
      <header className="page-header">
        <h1>System</h1>
      </header>
      <section className="panel">
        <div className="panel-header">
          <h2>Status</h2>
        </div>
        <dl className="kv">
          <dt>Version</dt>
          <dd>0.0.1-dev</dd>
          <dt>Uptime</dt>
          <dd className="muted">tbd</dd>
        </dl>
      </section>
    </div>
  );
}
