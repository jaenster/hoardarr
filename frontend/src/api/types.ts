// Wire shapes for /api/v1 — mirror internal/api/rest/dto.go.
//
// Keep these in sync with the Go DTOs. JobState is the same enum the
// domain layer emits; use its values (lowercase strings) directly.

export type JobState =
  | "queued"
  | "downloading"
  | "paused"
  | "download_complete"
  | "verifying"
  | "repairing"
  | "unpacking"
  | "completed"
  | "failed"
  | "aborted";

export type FileState = "pending" | "downloading" | "complete" | "failed";

export type Job = {
  id: number;
  nzb_hash: string;
  name: string;
  category: string;
  priority: number;
  state: JobState;
  total_bytes: number;
  done_bytes: number;
  failed_bytes: number;
  added_at: string;
  started_at?: string;
  finished_at?: string;
  error?: string;
  files: JobFile[];
};

export type JobFile = {
  id: number;
  filename: string;
  size_bytes: number;
  state: FileState;
  segment_count: number;
  segments_done: number;
  is_par2: boolean;
};

export type Server = {
  id: number;
  name: string;
  host: string;
  port: number;
  tls: boolean;
  username?: string;
  max_conns: number;
  priority: number;
  enabled: boolean;
  added_at: string;
  updated_at: string;
};

export type Category = {
  name: string;
  dir: string;
  priority: number;
};

// Envelope shape for SSE messages — matches event.Envelope in Go.
export type EventEnvelope = {
  ID: string;
  Topic: string;
  AggregateID: string;
  OccurredAt: string;
  Payload: unknown;
  Attempts: number;
};

export type User = {
  id: number;
  username: string;
  role: string;
};
