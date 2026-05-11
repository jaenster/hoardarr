import { useState } from "react";
import { GripVertical, Pause, Play, Trash2 } from "lucide-react";
import { Link } from "react-router-dom";
import type { Job, JobState } from "../api/types";
import type { JobActivity } from "../hooks/useQueue";
import StatusBadge from "./StatusBadge";

type Tone = "ok" | "warn" | "err" | "info" | "neutral";

const stateTone: Record<JobState, Tone> = {
  queued: "neutral",
  downloading: "info",
  paused: "warn",
  download_complete: "ok",
  verifying: "info",
  repairing: "warn",
  unpacking: "info",
  completed: "ok",
  failed: "err",
  aborted: "neutral",
};

const stateLabel: Record<JobState, string> = {
  queued: "Queued",
  downloading: "Downloading",
  paused: "Paused",
  download_complete: "Downloaded",
  verifying: "Verifying",
  repairing: "Repairing",
  unpacking: "Unpacking",
  completed: "Completed",
  failed: "Failed",
  aborted: "Aborted",
};

export type QueueAction = (jobID: number) => void | Promise<void>;
export type QueueReorder = (orderedIds: number[]) => void | Promise<void>;

type Props = {
  jobs: Job[];
  activity?: Record<number, JobActivity>;
  bytesPerSec?: number;
  onPause: QueueAction;
  onResume: QueueAction;
  onRemove: QueueAction;
  onReorder?: QueueReorder;
};

export default function QueueList({
  jobs,
  activity,
  bytesPerSec,
  onPause,
  onResume,
  onRemove,
  onReorder,
}: Props) {
  // Share the global rate evenly across active downloading jobs for
  // per-row ETA. Crude but matches the user's mental model: "speed
  // = total rate, split between what's running".
  const activeCount = jobs.filter((j) => j.state === "downloading").length;
  const perJobRate =
    bytesPerSec && activeCount > 0 ? bytesPerSec / Math.max(1, activeCount) : 0;
  // Live drag state — index of the row being dragged and the drop
  // target. Used to render a "drop here" placeholder line.
  const [dragIdx, setDragIdx] = useState<number | null>(null);
  const [overIdx, setOverIdx] = useState<number | null>(null);

  const isReorderable = !!onReorder;

  const handleDrop = async () => {
    if (dragIdx == null || overIdx == null || dragIdx === overIdx) {
      setDragIdx(null);
      setOverIdx(null);
      return;
    }
    const next = jobs.slice();
    const [moved] = next.splice(dragIdx, 1);
    next.splice(overIdx, 0, moved);
    setDragIdx(null);
    setOverIdx(null);
    if (onReorder) {
      await onReorder(next.map((j) => j.id));
    }
  };

  return (
    <ul className="queue-list" role="list">
      {jobs.map((j, i) => (
        <QueueRow
          key={j.id}
          job={j}
          activity={activity?.[j.id]}
          bytesPerSec={perJobRate}
          index={i}
          isDragging={dragIdx === i}
          isDropTarget={overIdx === i && dragIdx !== null && dragIdx !== i}
          reorderable={isReorderable}
          onDragStart={() => setDragIdx(i)}
          onDragOver={() => setOverIdx(i)}
          onDragEnd={() => {
            setDragIdx(null);
            setOverIdx(null);
          }}
          onDrop={() => void handleDrop()}
          onPause={onPause}
          onResume={onResume}
          onRemove={onRemove}
        />
      ))}
    </ul>
  );
}

function QueueRow({
  job,
  activity,
  bytesPerSec,
  index,
  isDragging,
  isDropTarget,
  reorderable,
  onDragStart,
  onDragOver,
  onDragEnd,
  onDrop,
  onPause,
  onResume,
  onRemove,
}: {
  job: Job;
  activity?: JobActivity;
  bytesPerSec?: number;
  index: number;
  isDragging: boolean;
  isDropTarget: boolean;
  reorderable: boolean;
  onDragStart: () => void;
  onDragOver: () => void;
  onDragEnd: () => void;
  onDrop: () => void;
  onPause: QueueAction;
  onResume: QueueAction;
  onRemove: QueueAction;
}) {
  const pct = job.total_bytes > 0 ? (job.done_bytes / job.total_bytes) * 100 : 0;
  const isPaused = job.state === "paused";
  const isTerminal =
    job.state === "completed" || job.state === "failed" || job.state === "aborted";
  // Terminal jobs aren't draggable — reordering them is meaningless.
  const draggable = reorderable && !isTerminal;

  return (
    <li
      className={
        "queue-row" +
        (isDragging ? " is-dragging" : "") +
        (isDropTarget ? " is-drop-target" : "")
      }
      draggable={draggable}
      onDragStart={(e) => {
        if (!draggable) return;
        e.dataTransfer.effectAllowed = "move";
        e.dataTransfer.setData("text/plain", String(job.id));
        onDragStart();
      }}
      onDragOver={(e) => {
        if (!reorderable) return;
        e.preventDefault();
        e.dataTransfer.dropEffect = "move";
        onDragOver();
      }}
      onDragEnd={onDragEnd}
      onDrop={(e) => {
        if (!reorderable) return;
        e.preventDefault();
        onDrop();
      }}
      data-index={index}
    >
      <div className="queue-row-head">
        <div className="queue-row-title">
          {draggable && (
            <span className="queue-row-grip" aria-hidden="true" title="Drag to reorder">
              <GripVertical size={14} />
            </span>
          )}
          <Link to={`/jobs/${job.id}`} className="queue-row-name queue-row-name-link">{job.name}</Link>
          {job.category && (
            <span className="queue-row-cat muted">{job.category}</span>
          )}
          {job.source && (
            <span className="queue-row-source muted" title={job.source}>
              from {shortSource(job.source)}
            </span>
          )}
        </div>
        <div className="queue-row-actions">
          <StatusBadge tone={stateTone[job.state]}>{stateLabel[job.state]}</StatusBadge>
          {!isTerminal && !isPaused && (
            <button
              type="button"
              className="icon-btn"
              aria-label="Pause"
              onClick={() => void onPause(job.id)}
            >
              <Pause size={14} />
            </button>
          )}
          {isPaused && (
            <button
              type="button"
              className="icon-btn"
              aria-label="Resume"
              onClick={() => void onResume(job.id)}
            >
              <Play size={14} />
            </button>
          )}
          <button
            type="button"
            className="icon-btn icon-btn-danger"
            aria-label="Remove"
            onClick={() => void onRemove(job.id)}
          >
            <Trash2 size={14} />
          </button>
        </div>
      </div>
      <div className="progress" role="progressbar" aria-valuenow={pct}>
        <div
          className={"progress-fill " + (isPaused ? "is-paused" : "")}
          style={{ width: pct + "%" }}
        />
        <span className="progress-label">
          {formatBytes(job.done_bytes)} / {formatBytes(job.total_bytes)}{" "}
          ({pct.toFixed(1)}%)
          {job.state === "downloading" && bytesPerSec && bytesPerSec > 0 && (
            <>
              {" · "}
              <span className="muted">
                {formatBytes(bytesPerSec)}/s · ETA{" "}
                {formatETA((job.total_bytes - job.done_bytes) / bytesPerSec)}
              </span>
            </>
          )}
        </span>
      </div>
      {activity?.currentMessageID && job.state === "downloading" && (
        <p className="muted queue-row-activity" title={activity.currentMessageID}>
          Fetching <code className="inline-code">&lt;{truncateMsgID(activity.currentMessageID)}&gt;</code>
          {activity.attempt && activity.attempt > 1 && (
            <span className="muted"> · attempt {activity.attempt}</span>
          )}
        </p>
      )}
      {job.failed_bytes > 0 && (
        <p className="muted queue-row-warn">
          {formatBytes(job.failed_bytes)} marked missing or failed
        </p>
      )}
    </li>
  );
}

function truncateMsgID(s: string): string {
  if (s.length <= 56) return s;
  return s.slice(0, 30) + "…" + s.slice(-22);
}

// shortSource collapses a full HTTP User-Agent down to the headline
// product name + version. "Sonarr/4.0.5.1710 (linux 6.1)" → "Sonarr/4.0".
// "Mozilla/5.0 … Chrome/120" → "browser".
function shortSource(ua: string): string {
  const t = ua.trim();
  // Common *arr UA patterns: "Sonarr/4.0.5.1710" etc.
  const arrMatch = t.match(/^(Sonarr|Radarr|Lidarr|Readarr|Prowlarr|Bazarr)\/(\d+(?:\.\d+)?)/i);
  if (arrMatch) return `${arrMatch[1]}/${arrMatch[2]}`;
  // Generic <Product>/<version> tokens take the first one.
  const first = t.split(/\s+/)[0];
  if (first.includes("/") && first.length < 40) return first;
  if (/Mozilla|Chrome|Safari|Firefox/i.test(t)) return "browser";
  return first.length > 24 ? first.slice(0, 24) + "…" : first;
}

function formatETA(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  const mm = m % 60;
  return `${h}h ${mm}m`;
}

function formatBytes(n: number): string {
  if (n < 1024) return n + " B";
  const units = ["KB", "MB", "GB", "TB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return v.toFixed(v < 10 ? 2 : 1) + " " + units[i];
}
