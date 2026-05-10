export default function Settings() {
  return (
    <div className="page">
      <header className="page-header">
        <h1>Settings</h1>
      </header>
      <section className="panel">
        <div className="panel-header">
          <h2>Usenet servers</h2>
        </div>
        <div className="empty-state">
          <p>No servers configured.</p>
          <p className="muted">Server configuration UI lands with the NNTP layer.</p>
        </div>
      </section>
    </div>
  );
}
