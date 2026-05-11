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
  | "aborted"
  | "waiting_for_server";

export type FileState = "pending" | "downloading" | "complete" | "failed";

export type Job = {
  id: number;
  nzb_hash: string;
  name: string;
  category: string;
  priority: number;
  state: JobState;
  source?: string;
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

export type BillingMode = "flat" | "metered";

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
  backup: boolean;
  billing_mode: BillingMode;
  quota_bytes: number;
  used_bytes: number;
  bandwidth_bytes_per_sec: number;
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

export type SystemStatus = {
  service: string;
  version: string;
  started_at: string;
  uptime_ms: number;
  queue: {
    active: number;
    total: number;
  };
  pools: PoolStatus[];
};

export type PoolStatus = {
  server_id: number;
  server_name: string;
  host: string;
  port: number;
  max_conns: number;
  in_use: number;
  idle: number;
  enabled: boolean;
  backup: boolean;
  billing_mode: BillingMode;
  quota_bytes: number;
  used_bytes: number;
};

export type Paths = {
  data_dir: string;
  incomplete_dir: string;
  complete_dir: string;
  runtime_mutable: boolean;
  requires_restart: boolean;
};

export type General = {
  listen: string;
  api_key: string;
  log_level: string;
  sab_base: string;
  url_base: string;
  max_concurrent_jobs: number;
};

export type BandwidthConfig = {
  global_bytes_per_sec: number;
};

export type LogEntry = {
  time: string;
  level: string;
  message: string;
  attrs?: Record<string, string>;
};

export type Throughput = {
  window_seconds: number;
  series: number[];
  total_bytes: number;
  current_bytes_per_sec: number;
  avg10s_bytes_per_sec: number;
  avg60s_bytes_per_sec: number;
};

export type SubscriptionKind = "webhook" | "discord" | "slack";

export type Subscription = {
  id: number;
  name: string;
  kind: SubscriptionKind;
  url: string;
  topics: string[];
  has_secret: boolean;
  enabled: boolean;
  last_success_at?: string;
  last_error_at?: string;
  last_error?: string;
  created_at: string;
  updated_at: string;
};
