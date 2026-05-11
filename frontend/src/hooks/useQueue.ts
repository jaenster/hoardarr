import { useCallback, useEffect, useRef, useState } from "react";
import { api, streamURL } from "../api/client";
import type { EventEnvelope, Job } from "../api/types";

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
export function useQueue() {
  const [jobs, setJobs] = useState<Job[] | null>(null);
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
      if (debounceTimer.current != null) {
        window.clearTimeout(debounceTimer.current);
      }
      es.close();
    };
  }, [refresh, scheduleRefresh, patchJobBytes]);

  return { jobs, error, loading, refresh, applyReorder };
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
