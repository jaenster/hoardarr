import { useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import {
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  RefreshCw,
  Activity,
  Wrench,
  Hammer,
  Truck,
  Box,
  FileText,
  ShieldCheck,
  Layers,
} from "lucide-react";
import Page from "../components/Page";
import Panel from "../components/Panel";
import Button from "../components/Button";
import StatusBadge from "../components/StatusBadge";
import { api } from "../api/client";
import type {
  EventEnvelope,
  FileState,
  Job,
  JobFile,
  JobSegment,
  SegmentState,
} from "../api/types";

// Per-job timeline. Polls /api/v1/queue/{id}/events on a tick so live
// jobs grow in front of the operator. For terminal jobs it's a static
// audit trail of everything the bus delivered for this job, ordered
// by occurred_at.

export default function JobDetail() {
  const params = useParams<{ id: string }>();
  const id = Number(params.id);
  const navigate = useNavigate();

  const [job, setJob] = useState<Job | null>(null);
  const [events, setEvents] = useState<EventEnvelope[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = async () => {
    setLoading(true);
    try {
      // Fetch the job by id directly — the queue list endpoint strips
      // files for perf, so we need the dedicated detail endpoint to
      // render the per-file breakdown. Falls back to null on 404 so
      // a removed-mid-poll job doesn't crash the page.
      const [jobResp, evResp] = await Promise.all([
        api.getJob(id).catch(() => ({ job: null as Job | null })),
        api.jobEvents(id),
      ]);
      setJob(jobResp.job);
      setEvents(evResp.events ?? []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!Number.isFinite(id)) return;
    void refresh();
    // Poll every 3s for live updates. The events list grows monotonically;
    // we just re-fetch the whole thing — at ≤ a few hundred events per
    // job, payload is tiny.
    const t = setInterval(() => void refresh(), 3000);
    return () => clearInterval(t);
  }, [id]);

  if (!Number.isFinite(id)) {
    return (
      <Page title="Job" subtitle="Invalid id">
        <Panel title="Not found">
          <p className="muted">The job id in the URL isn't a number.</p>
        </Panel>
      </Page>
    );
  }

  return (
    <Page
      title={job ? job.name : `Job #${id}`}
      subtitle={
        job
          ? `${job.category || "*"} • ${job.state}${job.source ? " • from " + job.source : ""}`
          : loading
            ? "Loading…"
            : "Not in queue or history"
      }
      actions={
        <>
          <Button
            variant="ghost"
            icon={<ChevronLeft size={14} />}
            onClick={() => navigate(-1)}
          >
            Back
          </Button>
          <Button
            variant="secondary"
            icon={<RefreshCw size={14} />}
            onClick={() => void refresh()}
            disabled={loading}
          >
            Refresh
          </Button>
        </>
      }
    >
      {error && <p className="text-err">{error}</p>}

      {job && job.files && job.files.length > 0 && (
        <FilesPanel job={job} />
      )}

      <Panel
        title="Timeline"
        meta={<StatusBadge tone="neutral">{events.length} events</StatusBadge>}
      >
        {events.length === 0 && !loading ? (
          <p className="muted">No events recorded for this job yet.</p>
        ) : (
          <TimelineGroups events={events} />
        )}
      </Panel>
    </Page>
  );
}

// TimelineGroups renders the event list as collapsed groups of
// adjacent same-topic events. For a job with thousands of
// `download.segment.completed` events, we collapse them into a single
// "1234× download.segment.completed" row that expands on click —
// turning a 1000-line DOM into a 10-line one. Click an individual
// event inside the group to reveal its JSON payload.
function TimelineGroups({ events }: { events: EventEnvelope[] }) {
  const groups = useMemo(() => groupEvents(events), [events]);
  return (
    <ol className="timeline">
      {groups.map((g) => (
        <TimelineGroup key={g.firstID} group={g} />
      ))}
    </ol>
  );
}

type EventGroup = {
  firstID: string;
  topic: string;
  events: EventEnvelope[];
  firstAt: string;
  lastAt: string;
};

function groupEvents(events: EventEnvelope[]): EventGroup[] {
  const out: EventGroup[] = [];
  for (const e of events) {
    const tail = out[out.length - 1];
    if (tail && tail.topic === e.Topic) {
      tail.events.push(e);
      tail.lastAt = e.OccurredAt;
      continue;
    }
    out.push({
      firstID: e.ID,
      topic: e.Topic,
      events: [e],
      firstAt: e.OccurredAt,
      lastAt: e.OccurredAt,
    });
  }
  return out;
}

function TimelineGroup({ group }: { group: EventGroup }) {
  const [open, setOpen] = useState(false);
  const single = group.events.length === 1;
  // Single-event groups behave like leaf items — clicking expands the
  // payload directly. Multi-event groups expand to show the per-event
  // list (and each event in there is its own click-to-expand).
  if (single) {
    return (
      <li className="timeline-item">
        <span className={`timeline-marker ${toneFor(group.topic)}`}>
          {iconFor(group.topic)}
        </span>
        <div className="timeline-body">
          <button
            className="timeline-head timeline-head-btn"
            onClick={() => setOpen((v) => !v)}
            aria-expanded={open}
          >
            <ChevronRight
              size={12}
              className={"timeline-chev " + (open ? "is-open" : "")}
              aria-hidden="true"
            />
            <code className="inline-code">{group.topic}</code>
            <span className="muted timeline-time">
              {new Date(group.firstAt).toLocaleString()}
            </span>
          </button>
          {open && (
            <pre className="timeline-payload">
              {prettyPayload(group.events[0].Payload)}
            </pre>
          )}
        </div>
      </li>
    );
  }
  return (
    <li className="timeline-item">
      <span className={`timeline-marker ${toneFor(group.topic)}`}>
        {iconFor(group.topic)}
      </span>
      <div className="timeline-body">
        <button
          className="timeline-head timeline-head-btn"
          onClick={() => setOpen((v) => !v)}
          aria-expanded={open}
        >
          <ChevronRight
            size={12}
            className={"timeline-chev " + (open ? "is-open" : "")}
            aria-hidden="true"
          />
          <code className="inline-code">{group.topic}</code>
          <span className="timeline-count">{group.events.length}×</span>
          <span className="muted timeline-time">
            {new Date(group.firstAt).toLocaleString()}
            {group.firstAt !== group.lastAt && (
              <> → {new Date(group.lastAt).toLocaleString()}</>
            )}
          </span>
        </button>
        {open && (
          <ol className="timeline-sub">
            {group.events.map((e) => (
              <TimelineSubItem key={e.ID} event={e} />
            ))}
          </ol>
        )}
      </div>
    </li>
  );
}

function TimelineSubItem({ event }: { event: EventEnvelope }) {
  const [open, setOpen] = useState(false);
  return (
    <li className="timeline-sub-item">
      <button
        className="timeline-sub-head"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        <ChevronRight
          size={11}
          className={"timeline-chev " + (open ? "is-open" : "")}
          aria-hidden="true"
        />
        <span className="muted timeline-time">
          {new Date(event.OccurredAt).toLocaleString()}
        </span>
      </button>
      {open && (
        <pre className="timeline-payload">{prettyPayload(event.Payload)}</pre>
      )}
    </li>
  );
}

// Expandable file/segment explorer. Default view groups by file with a
// per-file summary row (state, size, progress, segment counts). Click
// a file to reveal its segments — the operator can spot exactly which
// articles 430'd, are still pending, or hit retry budget.
//
// "Show problems only" filters to files with any segment in a non-done
// terminal state (missing/failed) or any non-resolved segment when the
// file's state is non-terminal. Useful for tracking down stuck jobs
// when most files completed cleanly.
function FilesPanel({ job }: { job: Job }) {
  const [problemsOnly, setProblemsOnly] = useState(false);
  const [expanded, setExpanded] = useState<Set<number>>(() => new Set());

  const files = problemsOnly
    ? job.files.filter((f) => hasProblems(f))
    : job.files;

  const problemCount = useMemo(
    () => job.files.filter(hasProblems).length,
    [job.files],
  );

  const toggle = (id: number) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  return (
    <Panel
      title="Files"
      meta={
        <>
          <StatusBadge tone="neutral">
            <Layers size={12} />
            {job.files.length} files
          </StatusBadge>
          {problemCount > 0 && (
            <StatusBadge tone="warn" dot>
              {problemCount} with problems
            </StatusBadge>
          )}
        </>
      }
      actions={
        problemCount > 0 ? (
          <Button
            variant={problemsOnly ? "primary" : "ghost"}
            onClick={() => setProblemsOnly((v) => !v)}
          >
            {problemsOnly ? "Show all" : "Problems only"}
          </Button>
        ) : null
      }
    >
      {files.length === 0 ? (
        <p className="muted">
          {problemsOnly
            ? "No files with unresolved segments — everything healthy."
            : "No files."}
        </p>
      ) : (
        <ul className="file-tree" role="tree">
          {files.map((f) => (
            <FileTreeRow
              key={f.id}
              file={f}
              expanded={expanded.has(f.id)}
              onToggle={() => toggle(f.id)}
            />
          ))}
        </ul>
      )}
    </Panel>
  );
}

function FileTreeRow({
  file,
  expanded,
  onToggle,
}: {
  file: JobFile;
  expanded: boolean;
  onToggle: () => void;
}) {
  const pct =
    file.segment_count > 0
      ? Math.round((file.segments_done / file.segment_count) * 100)
      : 0;
  const segs = file.segments;
  const hasSegmentData = Array.isArray(segs);
  const counts = hasSegmentData ? countSegmentStates(segs!) : null;
  const problems =
    counts && counts.missing + counts.failed + counts.inflight + counts.pending > 0
      ? counts
      : null;

  return (
    <li role="treeitem" aria-expanded={expanded} className="file-tree-item">
      <button
        type="button"
        className="file-tree-row"
        onClick={onToggle}
        disabled={!hasSegmentData}
      >
        <span className="file-tree-chevron" aria-hidden="true">
          {hasSegmentData ? (
            expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />
          ) : null}
        </span>
        {file.is_par2 ? (
          <ShieldCheck size={14} aria-hidden="true" className="file-icon" />
        ) : (
          <FileText size={14} aria-hidden="true" className="file-icon" />
        )}
        <span className="file-tree-name" title={file.filename}>
          {file.filename}
        </span>
        <span className="muted file-tree-meta">
          {fileKindLabel(file)} · {formatBytes(file.size_bytes)}
        </span>
        <span className="muted file-tree-segs">
          {file.segments_done} / {file.segment_count}
        </span>
        <span className="file-tree-progress" aria-hidden="true">
          <span
            className="file-tree-progress-fill"
            style={{ width: pct + "%" }}
          />
        </span>
        <StatusBadge tone={fileStateTone(file.state)} dot>
          {fileStateLabel(file.state)}
        </StatusBadge>
      </button>
      {expanded && hasSegmentData && (
        <div className="file-tree-children">
          {problems && (
            <p className="muted file-tree-summary">
              {problems.done > 0 && <>{problems.done} done · </>}
              {problems.pending > 0 && <>{problems.pending} pending · </>}
              {problems.inflight > 0 && <>{problems.inflight} inflight · </>}
              {problems.missing > 0 && (
                <span className="text-err">{problems.missing} missing · </span>
              )}
              {problems.failed > 0 && (
                <span className="text-err">{problems.failed} failed</span>
              )}
            </p>
          )}
          <table className="table file-tree-segments">
            <thead>
              <tr>
                <th>#</th>
                <th>Message-ID</th>
                <th>Bytes</th>
                <th>Attempts</th>
                <th>State</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody>
              {segs!.map((s) => (
                <tr
                  key={s.id}
                  className={segmentRowClass(s.state)}
                >
                  <td className="muted file-seg-idx">{s.seq_index}</td>
                  <td className="file-seg-msgid" title={s.message_id}>
                    <code className="inline-code">{s.message_id}</code>
                  </td>
                  <td className="muted">{formatBytes(s.bytes)}</td>
                  <td className="muted">{s.attempts}</td>
                  <td>
                    <StatusBadge tone={segStateTone(s.state)} dot>
                      {s.state}
                    </StatusBadge>
                  </td>
                  <td className="text-err file-seg-err" title={s.last_error || ""}>
                    {s.last_error || ""}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </li>
  );
}

function countSegmentStates(segs: JobSegment[]) {
  let done = 0,
    pending = 0,
    inflight = 0,
    missing = 0,
    failed = 0;
  for (const s of segs) {
    switch (s.state) {
      case "done": done++; break;
      case "pending": pending++; break;
      case "inflight": inflight++; break;
      case "missing": missing++; break;
      case "failed": failed++; break;
    }
  }
  return { done, pending, inflight, missing, failed };
}

function hasProblems(f: JobFile): boolean {
  if (Array.isArray(f.segments)) {
    return f.segments.some(
      (s) => s.state === "missing" || s.state === "failed",
    );
  }
  // Fall back to header counts when segments aren't hydrated.
  return f.state === "failed" ||
    (f.state !== "complete" && f.segments_done < f.segment_count);
}

function fileKindLabel(f: JobFile): string {
  if (f.is_recovery_vol) return "PAR2 vol";
  if (f.is_par2) return "PAR2";
  return "data";
}

function segStateTone(
  s: SegmentState,
): "ok" | "warn" | "err" | "info" | "neutral" {
  switch (s) {
    case "done": return "ok";
    case "pending": return "neutral";
    case "inflight": return "info";
    case "missing": return "err";
    case "failed": return "err";
  }
}

function segmentRowClass(s: SegmentState): string {
  switch (s) {
    case "missing":
    case "failed":
      return "file-seg-row-bad";
    default:
      return "";
  }
}

function fileStateTone(s: FileState): "ok" | "warn" | "err" | "info" | "neutral" {
  switch (s) {
    case "complete":
      return "ok";
    case "failed":
      return "err";
    case "downloading":
      return "info";
    default:
      return "neutral";
  }
}

function fileStateLabel(s: FileState): string {
  switch (s) {
    case "pending":
      return "Pending";
    case "downloading":
      return "Downloading";
    case "complete":
      return "Complete";
    case "failed":
      return "Failed";
    default:
      return String(s);
  }
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

function toneFor(topic: string): string {
  if (topic.endsWith(".failed") || topic.endsWith(".repair_needed")) return "is-err";
  if (topic.endsWith(".ok") || topic.endsWith(".complete") || topic.endsWith(".completed")) return "is-ok";
  return "is-info";
}

function iconFor(topic: string) {
  if (topic.startsWith("download.")) return <Activity size={12} />;
  if (topic.startsWith("verify.")) return <Hammer size={12} />;
  if (topic.startsWith("repair.")) return <Wrench size={12} />;
  if (topic.startsWith("deliver.")) return <Truck size={12} />;
  if (topic.startsWith("extract.")) return <Box size={12} />;
  return <Activity size={12} />;
}

function prettyPayload(p: unknown): string {
  try {
    return JSON.stringify(p, null, 2);
  } catch {
    return String(p);
  }
}
