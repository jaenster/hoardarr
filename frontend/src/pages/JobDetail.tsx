import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { ChevronLeft, RefreshCw, Activity, Wrench, Hammer, Truck, Box } from "lucide-react";
import Page from "../components/Page";
import Panel from "../components/Panel";
import Button from "../components/Button";
import StatusBadge from "../components/StatusBadge";
import { api } from "../api/client";
import type { EventEnvelope, Job } from "../api/types";

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
      const [queue, evResp] = await Promise.all([
        api.listQueue(true),
        api.jobEvents(id),
      ]);
      const j = (queue.jobs ?? []).find((x) => x.id === id) ?? null;
      setJob(j);
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
          ? `${job.category || "*"} • ${job.state}`
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

      <Panel
        title="Timeline"
        meta={<StatusBadge tone="neutral">{events.length} events</StatusBadge>}
      >
        {events.length === 0 && !loading ? (
          <p className="muted">No events recorded for this job yet.</p>
        ) : (
          <ol className="timeline">
            {events.map((e) => (
              <li key={e.ID} className="timeline-item">
                <span className={`timeline-marker ${toneFor(e.Topic)}`}>
                  {iconFor(e.Topic)}
                </span>
                <div className="timeline-body">
                  <div className="timeline-head">
                    <code className="inline-code">{e.Topic}</code>
                    <span className="muted timeline-time">
                      {new Date(e.OccurredAt).toLocaleString()}
                    </span>
                  </div>
                  <pre className="timeline-payload">
                    {prettyPayload(e.Payload)}
                  </pre>
                </div>
              </li>
            ))}
          </ol>
        )}
      </Panel>
    </Page>
  );
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
