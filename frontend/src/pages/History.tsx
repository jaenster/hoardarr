import { Filter, Trash2, History as HistoryIcon } from "lucide-react";
import Page from "../components/Page";
import Panel from "../components/Panel";
import Button from "../components/Button";
import StatusBadge from "../components/StatusBadge";

export default function History() {
  return (
    <Page
      title="History"
      subtitle="Completed and failed downloads"
      actions={
        <>
          <Button variant="ghost" icon={<Filter size={14} />} disabled>
            Filter
          </Button>
          <Button variant="secondary" icon={<Trash2 size={14} />} disabled>
            Clear
          </Button>
        </>
      }
    >
      <Panel
        title="Recent activity"
        meta={<StatusBadge tone="neutral">0 records</StatusBadge>}
        flush
      >
        <div className="empty-state">
          <HistoryIcon size={28} className="empty-icon" aria-hidden="true" />
          <p className="empty-title">No history yet</p>
          <p className="muted">
            Once jobs complete, they will appear here with timing and status.
          </p>
        </div>
      </Panel>
    </Page>
  );
}
