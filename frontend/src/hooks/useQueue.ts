import { useCallback, useEffect, useRef, useState } from "react";
import { api, streamURL } from "../api/client";
import type { EventEnvelope, Job, PoolStatus } from "../api/types";

// useQueue holds the current queue snapshot and applies live SSE
// updates.
//
// Strategy:
//   1. On mount: GET /api/v1/queue once.
//   2. Open EventSource on /api/v1/queue/stream.
//   3. Segment-level events (completed/missing/failed) patch the
//      affected job in place — done_bytes / failed_bytes tick on the
//      sub-second cadence the orchestrator produces them at, with no
//      network roundtrip per tick.
//   4. Job-level events (created/state transitions/removed) schedule
//      a debounced refresh, because those require fields we don't
//      derive from segment payloads (state, started_at, files…).
//
// In-place patching is essential for "smooth tick" UX — a refresh per
// segment would be ~hundreds of /queue/list calls per second on a fast
// job.
// JobActivity holds short-lived per-job hints surfaced to the UI:
// what message-id is currently being fetched, last dispatch time, etc.
// Lives only in memory — recomputed from the SSE stream on every page
// load.
export type JobActivity = {
  currentMessageID?: string;
  attempt?: number;
  at?: string; // ISO timestamp of the dispatch
};

export function useQueue() {
  const [jobs, setJobs] = useState<Job[] | null>(null);
  const [activity, setActivity] = useState<Record<number, JobActivity>>({});
  // Current overall throughput in bytes/sec. Used by QueueRow to render
  // an ETA next to the progress bar.
  const [bytesPerSec, setBytesPerSec] = useState(0);
  // Per-server pool connection accounting. Surfaces "8/8 conns" in
  // the Activity header so operators can see whether they're capped
  // by their provider's connection limit.
  const [pools, setPools] = useState<PoolStatus[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const debounceTimer = useRef<number | null>(null);

  const refresh = useCallback(async () => {
    try {
      const { jobs } = await api.listQueue();
      setJobs(jobs ?? []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  const scheduleRefresh = useCallback(() => {
    if (debounceTimer.current != null) {
      window.clearTimeout(debounceTimer.current);
    }
    debounceTimer.current = window.setTimeout(() => {
      void refresh();
    }, 250);
  }, [refresh]);

  // applyReorder mutates local state to match the given id order.
  // Called by the Activity page to give immediate visual feedback on
  // drag-drop without waiting for the POST + SSE refresh round trip.
  // The server will overwrite this on the next refresh anyway, so a
  // stale order from a missed update self-corrects.
  const applyReorder = useCallback((orderedIds: number[]) => {
    setJobs((current) => {
      if (!current) return current;
      const byId = new Map(current.map((j) => [j.id, j]));
      const reordered: Job[] = [];
      for (const id of orderedIds) {
        const j = byId.get(id);
        if (j) {
          reordered.push(j);
          byId.delete(id);
        }
      }
      // Anything the client didn't reorder (terminal jobs, races)
      // keeps its old relative order at the end.
      reordered.push(...byId.values());
      return reordered;
    });
  }, []);

  const patchJobBytes = useCallback(
    (jobId: number, deltaDone: number, deltaFailed: number) => {
      setJobs((current) => {
        if (!current) return current;
        let mutated = false;
        const next = current.map((j) => {
          if (j.id !== jobId) return j;
          mutated = true;
          const nextDone = j.done_bytes + deltaDone;
          // Clamp to total_bytes so the bar never overshoots while we
          // wait for the next authoritative refresh.
          const clampedDone =
            j.total_bytes > 0 ? Math.min(j.total_bytes, nextDone) : nextDone;
          return {
            ...j,
            done_bytes: clampedDone,
            failed_bytes: j.failed_bytes + deltaFailed,
          };
        });
        return mutated ? next : current;
      });
    },
    [],
  );

  useEffect(() => {
    void refresh();

    // Poll throughput once per second so the ETA stays responsive
    // without hammering the server. Window is 300s rolling; this
    // is a cheap snapshot read.
    let stopped = false;
    const pollThroughput = async () => {
      try {
        const tp = await api.throughput();
        if (!stopped) {
          // Prefer the 10-second rolling average so the displayed
          // speed doesn't jitter with every fetched article. Falls
          // back to current-second value while the window fills up.
          const rate = tp.avg10s_bytes_per_sec || tp.current_bytes_per_sec;
          setBytesPerSec(rate);
        }
      } catch {
        /* non-fatal — ETA just won't render */
      }
    };
    const pollPools = async () => {
      try {
        const st = await api.systemStatus();
        if (!stopped) setPools(st.pools ?? []);
      } catch {
        /* non-fatal */
      }
    };
    void pollThroughput();
    void pollPools();
    const tpTimer = window.setInterval(() => void pollThroughput(), 1000);
    // Pool stats change much more slowly than speed — every 2s is plenty.
    const poolTimer = window.setInterval(() => void pollPools(), 2000);

    const es = new EventSource(streamURL());

    // Segment-completed → tick done_bytes immediately. Payload carries
    // {job_id, segment_id, bytes, at}.
    es.addEventListener("download.segment.completed", (ev) => {
      const env = parseEnvelope(ev);
      const payload = env?.Payload as { job_id?: number; bytes?: number } | undefined;
      if (payload?.job_id && typeof payload.bytes === "number") {
        patchJobBytes(payload.job_id, payload.bytes, 0);
      }
    });

    // Segment-dispatched → "currently fetching X" UI hint. Payload
    // carries {job_id, segment_id, message_id, attempt, at}.
    es.addEventListener("download.segment.dispatched", (ev) => {
      const env = parseEnvelope(ev);
      const payload = env?.Payload as
        | { job_id?: number; message_id?: string; attempt?: number; at?: string }
        | undefined;
      if (!payload?.job_id || !payload.message_id) return;
      setActivity((current) => ({
        ...current,
        [payload.job_id!]: {
          currentMessageID: payload.message_id,
          attempt: payload.attempt,
          at: payload.at,
        },
      }));
    });
    // Segment-missing / failed → tick failed_bytes (best-effort; the
    // event payload doesn't carry a byte count for missing, so we
    // leave failed_bytes alone and rely on the next refresh for the
    // exact figure).
    es.addEventListener("download.segment.failed", () => scheduleRefresh());
    es.addEventListener("download.segment.missing", () => scheduleRefresh());

    // Anything that mutates job state (or the job set itself) → full
    // refresh. These are infrequent, so the cost is fine.
    [
      "download.job.created",
      "download.job.started",
      "download.job.paused",
      "download.job.resumed",
      "download.job.removed",
      "download.job.download_complete",
      "download.job.download_failed",
      "download.job.completed",
      "download.job.failed",
      "download.file.completed",
      "verify.started",
      "verify.ok",
      "verify.repair_needed",
      "verify.failed",
      "repair.started",
      "repair.ok",
      "repair.failed",
      "extract.started",
      "extract.complete",
      "extract.failed",
      "deliver.started",
      "deliver.complete",
      "deliver.skipped",
      "deliver.failed",
    ].forEach((topic) => es.addEventListener(topic, () => scheduleRefresh()));

    es.onerror = () => {
      // EventSource auto-reconnects; we surface a non-fatal warning.
      // If the connection genuinely fails (auth, network), the next
      // refresh will show the error.
    };

    return () => {
      stopped = true;
      window.clearInterval(tpTimer);
      window.clearInterval(poolTimer);
      if (debounceTimer.current != null) {
        window.clearTimeout(debounceTimer.current);
      }
      es.close();
    };
  }, [refresh, scheduleRefresh, patchJobBytes]);

  return { jobs, activity, bytesPerSec, pools, error, loading, refresh, applyReorder };
}

// parseEnvelope decodes the SSE data payload. The server emits the
// full event.Envelope as JSON; we lift the Payload field out.
function parseEnvelope(ev: MessageEvent): EventEnvelope | null {
  if (typeof ev.data !== "string") return null;
  try {
    return JSON.parse(ev.data) as EventEnvelope;
  } catch {
    return null;
  }
}
