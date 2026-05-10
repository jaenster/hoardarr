import { RefreshCw } from "lucide-react";
import Page from "../components/Page";
import Panel from "../components/Panel";
import Button from "../components/Button";
import StatusBadge from "../components/StatusBadge";

export default function System() {
  return (
    <Page
      title="System"
      subtitle="Build, runtime and diagnostics"
      actions={
        <Button variant="ghost" icon={<RefreshCw size={14} />} disabled>
          Refresh
        </Button>
      }
    >
      <Panel
        title="Status"
        meta={
          <StatusBadge tone="ok" dot>
            running
          </StatusBadge>
        }
      >
        <dl className="kv">
          <dt>Version</dt>
          <dd>
            <code className="inline-code">0.0.1-dev</code>
          </dd>
          <dt>Uptime</dt>
          <dd className="muted">tbd</dd>
          <dt>Build</dt>
          <dd className="muted">tbd</dd>
        </dl>
      </Panel>

      <Panel title="Logs" flush>
        <div className="empty-state">
          <p className="empty-title">No logs surfaced</p>
          <p className="muted">Log streaming arrives with the runtime layer.</p>
        </div>
      </Panel>
    </Page>
  );
}
