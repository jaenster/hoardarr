import { useCallback, useEffect, useRef, useState } from "react";
import { Inbox, Pause, Plus, RefreshCw, Upload } from "lucide-react";
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
  // Drag-counter: dragenter/leave fire for every child element. Track
  // a depth count so we only hide the overlay when ALL nested
  // dragleave events have fired.
  const [dragDepth, setDragDepth] = useState(0);

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

  // uploadFiles processes a FileList from either the file picker or
  // a drag-and-drop event. Files are uploaded sequentially so the
  // operator gets one queue entry per NZB in order, and a single
  // failure doesn't stop the rest.
  const uploadFiles = useCallback(
    async (files: FileList | File[]) => {
      const list = Array.from(files).filter(
        (f) => f.name.toLowerCase().endsWith(".nzb") || f.type === "application/x-nzb",
      );
      if (list.length === 0) return;
      setUploading(true);
      setUploadError(null);
      try {
        for (const f of list) {
          try {
            await api.uploadNZB(f);
          } catch (err) {
            if (err instanceof ApiError) {
              setUploadError(
                `${err.status}: ${(err.body as { error?: string })?.error ?? err.message}`,
              );
            } else {
              setUploadError(err instanceof Error ? err.message : String(err));
            }
          }
        }
        await refresh();
      } finally {
        setUploading(false);
      }
    },
    [refresh],
  );

  const onFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    e.target.value = "";
    if (files && files.length > 0) {
      await uploadFiles(files);
    }
  };

  // Page-wide drag-and-drop handlers. We intentionally listen on the
  // whole Activity page (not just a small drop zone) so dragging an
  // NZB anywhere on the route works.
  useEffect(() => {
    const onWinDragOver = (e: DragEvent) => {
      // Suppress the browser default (opens the file) for any drag
      // that contains files. Required to make the drop work.
      if (e.dataTransfer?.types?.includes("Files")) {
        e.preventDefault();
      }
    };
    const onWinDrop = (e: DragEvent) => {
      // Catch any file drop outside the explicit handler — without
      // this, dropping near a sidebar would still open the file.
      if (e.dataTransfer?.types?.includes("Files")) {
        e.preventDefault();
      }
    };
    window.addEventListener("dragover", onWinDragOver);
    window.addEventListener("drop", onWinDrop);
    return () => {
      window.removeEventListener("dragover", onWinDragOver);
      window.removeEventListener("drop", onWinDrop);
    };
  }, []);

  const handleDragEnter = (e: React.DragEvent) => {
    if (!e.dataTransfer.types.includes("Files")) return;
    e.preventDefault();
    setDragDepth((d) => d + 1);
  };
  const handleDragLeave = (e: React.DragEvent) => {
    if (!e.dataTransfer.types.includes("Files")) return;
    e.preventDefault();
    setDragDepth((d) => Math.max(0, d - 1));
  };
  const handleDragOver = (e: React.DragEvent) => {
    if (!e.dataTransfer.types.includes("Files")) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  };
  const handleDrop = async (e: React.DragEvent) => {
    if (!e.dataTransfer.types.includes("Files")) return;
    e.preventDefault();
    setDragDepth(0);
    if (e.dataTransfer.files.length > 0) {
      await uploadFiles(e.dataTransfer.files);
    }
  };

  const queueLabel =
    jobs == null ? "—" : jobs.length === 1 ? "1 item" : `${jobs.length} items`;
  const dragActive = dragDepth > 0;

  return (
    <div
      className={"activity-dropzone" + (dragActive ? " is-drag-over" : "")}
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={(e) => void handleDrop(e)}
    >
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
              multiple
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
                Drop an NZB anywhere on this page, or click <strong>Add NZB</strong>.
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

      {dragActive && (
        <div className="drag-overlay" aria-hidden="true">
          <div className="drag-overlay-card">
            <Upload size={32} />
            <p>Drop NZB here</p>
          </div>
        </div>
      )}
    </div>
  );
}
