import { useRef, useState } from "react";
import { Inbox, Pause, Plus, RefreshCw } from "lucide-react";
import Page from "../components/Page";
import Panel from "../components/Panel";
import StatusBadge from "../components/StatusBadge";
import Button from "../components/Button";
import QueueList from "../components/QueueList";
import { api, ApiError } from "../api/client";
import { useQueue } from "../hooks/useQueue";

export default function Activity() {
  const { jobs, activity, error, loading, refresh, applyReorder } = useQueue();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);

  const onPause = async (id: number) => {
    try {
      await api.pauseJob(id);
    } catch (e) {
      setUploadError(e instanceof Error ? e.message : String(e));
    }
  };
  const onResume = async (id: number) => {
    try {
      await api.resumeJob(id);
    } catch (e) {
      setUploadError(e instanceof Error ? e.message : String(e));
    }
  };
  const onRemove = async (id: number) => {
    try {
      await api.removeJob(id);
    } catch (e) {
      setUploadError(e instanceof Error ? e.message : String(e));
    }
  };
  const onReorder = async (orderedIds: number[]) => {
    applyReorder(orderedIds); // optimistic local reorder
    try {
      await api.reorderQueue(orderedIds);
    } catch (e) {
      setUploadError(e instanceof Error ? e.message : String(e));
      await refresh(); // rollback to server truth on failure
    }
  };

  const onAddNZB = () => fileInputRef.current?.click();

  const onFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    e.target.value = "";
    if (!f) return;
    setUploading(true);
    setUploadError(null);
    try {
      await api.uploadNZB(f);
      await refresh();
    } catch (err) {
      if (err instanceof ApiError) {
        setUploadError(`${err.status}: ${(err.body as { error?: string })?.error ?? err.message}`);
      } else {
        setUploadError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      setUploading(false);
    }
  };

  const queueLabel =
    jobs == null ? "—" : jobs.length === 1 ? "1 item" : `${jobs.length} items`;

  return (
    <Page
      title="Activity"
      subtitle="Live view of the download pipeline"
      actions={
        <>
          <Button
            variant="ghost"
            icon={<RefreshCw size={14} />}
            onClick={() => void refresh()}
            aria-label="Refresh"
            disabled={loading}
          >
            Refresh
          </Button>
          <Button variant="secondary" icon={<Pause size={14} />} disabled>
            Pause all
          </Button>
          <Button
            variant="primary"
            icon={<Plus size={14} />}
            onClick={onAddNZB}
            disabled={uploading}
          >
            {uploading ? "Uploading…" : "Add NZB"}
          </Button>
          <input
            ref={fileInputRef}
            type="file"
            accept=".nzb,application/x-nzb"
            onChange={onFile}
            style={{ display: "none" }}
          />
        </>
      }
    >
      {(error || uploadError) && (
        <Panel
          title="Error"
          meta={<StatusBadge tone="err" dot>fail</StatusBadge>}
        >
          <p className="text-err">{error ?? uploadError}</p>
        </Panel>
      )}

      <Panel
        title="Queue"
        meta={<StatusBadge tone="neutral">{queueLabel}</StatusBadge>}
        flush={jobs != null && jobs.length > 0}
      >
        {jobs == null && (
          <div className="empty-state">
            <p className="muted">Loading…</p>
          </div>
        )}
        {jobs != null && jobs.length === 0 && (
          <div className="empty-state">
            <Inbox size={28} className="empty-icon" aria-hidden="true" />
            <p className="empty-title">Queue is empty</p>
            <p className="muted">
              Drop an NZB via <strong>Add NZB</strong> to get started.
            </p>
          </div>
        )}
        {jobs != null && jobs.length > 0 && (
          <QueueList
            jobs={jobs}
            activity={activity}
            onPause={onPause}
            onResume={onResume}
            onRemove={onRemove}
            onReorder={onReorder}
          />
        )}
      </Panel>
    </Page>
  );
}
