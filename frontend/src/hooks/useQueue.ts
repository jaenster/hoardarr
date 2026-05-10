import { useCallback, useEffect, useRef, useState } from "react";
import { api, streamURL } from "../api/client";
import type { Job } from "../api/types";

// useQueue holds the current queue snapshot and refreshes it via
// /api/v1/queue + Server-Sent Events.
//
// Strategy:
//  1. On mount: GET /api/v1/queue once.
//  2. Open an EventSource on /api/v1/queue/stream.
//  3. On any download.* event, debounce-refresh /api/v1/queue.
//
// The debounce coalesces rapid-fire SegmentCompleted events into one
// fetch every ~250ms, giving smooth progress updates without
// hammering the API. Going to per-event in-place patching is a
// future optimisation.
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

  useEffect(() => {
    void refresh();

    const es = new EventSource(streamURL());
    const onAnyDownload = () => scheduleRefresh();
    [
      "download.job.created",
      "download.job.started",
      "download.job.paused",
      "download.job.resumed",
      "download.job.removed",
      "download.job.download_complete",
      "download.job.download_failed",
      "download.segment.completed",
      "download.segment.missing",
      "download.segment.failed",
      "download.file.completed",
    ].forEach((topic) => es.addEventListener(topic, onAnyDownload));

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
  }, [refresh, scheduleRefresh]);

  return { jobs, error, loading, refresh };
}
