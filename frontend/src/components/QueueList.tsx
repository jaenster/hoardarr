import { Pause, Play, Trash2 } from "lucide-react";
import type { Job, JobState } from "../api/types";
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

type Props = {
  jobs: Job[];
  onPause: QueueAction;
  onResume: QueueAction;
  onRemove: QueueAction;
};

export default function QueueList({ jobs, onPause, onResume, onRemove }: Props) {
  return (
    <ul className="queue-list" role="list">
      {jobs.map((j) => (
        <QueueRow
          key={j.id}
          job={j}
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
  onPause,
  onResume,
  onRemove,
}: {
  job: Job;
  onPause: QueueAction;
  onResume: QueueAction;
  onRemove: QueueAction;
}) {
  const pct = job.total_bytes > 0 ? (job.done_bytes / job.total_bytes) * 100 : 0;
  const isPaused = job.state === "paused";
  const isTerminal =
    job.state === "completed" || job.state === "failed" || job.state === "aborted";

  return (
    <li className="queue-row">
      <div className="queue-row-head">
        <div className="queue-row-title">
          <span className="queue-row-name">{job.name}</span>
          {job.category && (
            <span className="queue-row-cat muted">{job.category}</span>
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
        </span>
      </div>
      {job.failed_bytes > 0 && (
        <p className="muted queue-row-warn">
          {formatBytes(job.failed_bytes)} marked missing or failed
        </p>
      )}
    </li>
  );
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
