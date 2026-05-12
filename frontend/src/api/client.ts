// Tiny fetch wrapper for /api/v1.
//
// Auth: requests are authenticated by the session cookie that the
// server issues on /auth/login (HTTP-only, sent automatically by the
// browser via `credentials: "include"`). The X-Api-Key path remains
// in the server middleware for *arr clients but the web UI no longer
// uses it.
//
// URL base: Vite bakes a sentinel into `import.meta.env.BASE_URL`,
// which the Go server replaces with the runtime URLBase before
// serving any JS file. Every request goes through `withBase` so the
// SPA works whether mounted at "/" or "/hoardarr".

import type {
  BandwidthConfig,
  Category,
  EventEnvelope,
  General,
  Job,
  LogEntry,
  Paths,
  Server,
  Subscription,
  SystemStatus,
  Throughput,
  User,
} from "./types";

export class ApiError extends Error {
  constructor(public status: number, public body: unknown, msg: string) {
    super(msg);
  }
}

// urlBase is the resolved runtime prefix (no trailing slash). Empty
// when hoardarr is mounted at root.
export const urlBase = (import.meta.env.BASE_URL ?? "/").replace(/\/+$/, "");

// withBase prefixes an absolute "/api/..." or "/auth/..." path with
// the runtime URL base. Idempotent for already-prefixed paths.
export function withBase(path: string): string {
  if (!urlBase) return path;
  if (path.startsWith(urlBase + "/") || path === urlBase) return path;
  return urlBase + path;
}

async function req<T>(
  method: string,
  path: string,
  body?: BodyInit | null,
  extraHeaders: Record<string, string> = {},
): Promise<T> {
  const headers: Record<string, string> = { ...extraHeaders };
  // JSON encoding for plain object bodies happens at call sites that
  // need it; FormData / file bodies pass through unmodified.
  const res = await fetch(withBase(path), {
    method,
    headers,
    body,
    credentials: "include",
  });
  const text = await res.text();
  let parsed: unknown = null;
  if (text) {
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = text;
    }
  }
  if (!res.ok) {
    let msg = `${method} ${path}: ${res.status}`;
    if (parsed && typeof parsed === "object" && "error" in parsed) {
      msg += ` — ${(parsed as { error: string }).error}`;
    }
    throw new ApiError(res.status, parsed, msg);
  }
  return parsed as T;
}

function jsonReq<T>(method: string, path: string, body: unknown): Promise<T> {
  return req(method, path, JSON.stringify(body), {
    "Content-Type": "application/json",
  });
}

export type WhoamiState =
  | { state: "needs_setup" }
  | { state: "needs_login" }
  | { state: "authenticated"; user: User };

export const api = {
  // --- auth -----------------------------------------------------
  whoami(): Promise<WhoamiState> {
    return req("GET", "/api/v1/auth/whoami");
  },
  setupAdmin(username: string, password: string): Promise<{ id: number }> {
    return jsonReq("POST", "/api/v1/auth/setup", { username, password });
  },
  login(username: string, password: string): Promise<{ ok: true }> {
    return jsonReq("POST", "/api/v1/auth/login", { username, password });
  },
  logout(): Promise<void> {
    return req("POST", "/api/v1/auth/logout");
  },
  changePassword(oldPassword: string, newPassword: string): Promise<void> {
    return jsonReq("POST", "/api/v1/auth/change-password", {
      old_password: oldPassword,
      new_password: newPassword,
    });
  },

  // --- public -----------------------------------------------------
  health(): Promise<{ status: string; service: string }> {
    return req("GET", "/api/v1/health");
  },

  // --- queue ------------------------------------------------------
  listQueue(includeAll = false): Promise<{ jobs: Job[] | null }> {
    const qs = includeAll ? "?include=all" : "";
    return req("GET", "/api/v1/queue" + qs);
  },
  // getJob returns a job with files + segments populated. Used by the
  // detail page; the list endpoint strips files for perf reasons so
  // we can't reuse its payload to render the file panel.
  getJob(id: number): Promise<{ job: Job }> {
    return req("GET", `/api/v1/queue/${id}`);
  },
  uploadNZB(file: File, category?: string): Promise<{ job_id: number; duplicate?: boolean }> {
    const fd = new FormData();
    fd.append("nzb", file);
    if (category) fd.append("category", category);
    return req("POST", "/api/v1/queue/nzb", fd);
  },
  pauseJob(id: number): Promise<void> {
    return req("POST", `/api/v1/queue/${id}/pause`);
  },
  resumeJob(id: number): Promise<void> {
    return req("POST", `/api/v1/queue/${id}/resume`);
  },
  removeJob(id: number): Promise<void> {
    return req("DELETE", `/api/v1/queue/${id}`);
  },
  reorderQueue(ids: number[]): Promise<void> {
    return jsonReq("POST", "/api/v1/queue/reorder", { ids });
  },

  // --- per-job event timeline -----------------------------------
  jobEvents(id: number): Promise<{ events: EventEnvelope[] }> {
    return req("GET", `/api/v1/queue/${id}/events`);
  },

  // --- history ----------------------------------------------------
  listHistory(opts: HistoryOpts = {}): Promise<{ jobs: Job[] | null }> {
    const qs = new URLSearchParams();
    if (opts.state) qs.set("state", opts.state);
    if (opts.category) qs.set("category", opts.category);
    if (opts.since) qs.set("since", opts.since);
    if (opts.limit) qs.set("limit", String(opts.limit));
    const tail = qs.toString();
    return req("GET", "/api/v1/history" + (tail ? "?" + tail : ""));
  },

  // --- servers + categories --------------------------------------
  listServers(): Promise<{ servers: Server[] | null }> {
    return req("GET", "/api/v1/servers");
  },
  addServer(body: AddServerBody): Promise<{ id: number }> {
    return jsonReq("POST", "/api/v1/servers", body);
  },
  patchServer(id: number, body: PatchServerBody): Promise<void> {
    return jsonReq("PATCH", `/api/v1/servers/${id}`, body);
  },
  removeServer(id: number): Promise<void> {
    return req("DELETE", `/api/v1/servers/${id}`);
  },
  testServer(body: TestServerBody): Promise<TestServerResult> {
    return jsonReq("POST", "/api/v1/servers/test", body);
  },
  testExistingServer(id: number): Promise<TestServerResult> {
    return req("POST", `/api/v1/servers/${id}/test`);
  },
  enableServer(id: number, enabled: boolean): Promise<void> {
    return req("POST", `/api/v1/servers/${id}/${enabled ? "enable" : "disable"}`);
  },
  listCategories(): Promise<{ categories: Category[] | null }> {
    return req("GET", "/api/v1/categories");
  },
  upsertCategory(body: Category): Promise<Category> {
    return jsonReq("POST", "/api/v1/categories", body);
  },
  removeCategory(name: string): Promise<void> {
    return req("DELETE", `/api/v1/categories/${encodeURIComponent(name)}`);
  },

  // --- system + config -------------------------------------------
  systemStatus(): Promise<SystemStatus> {
    return req("GET", "/api/v1/system/status");
  },
  paths(): Promise<Paths> {
    return req("GET", "/api/v1/config/paths");
  },
  general(): Promise<General> {
    return req("GET", "/api/v1/config/general");
  },
  setGeneral(body: {
    url_base?: string;
    max_concurrent_jobs?: number;
    fail_hopeless_ratio?: number;
  }): Promise<void> {
    return jsonReq("PUT", "/api/v1/config/general", body);
  },
  bandwidth(): Promise<BandwidthConfig> {
    return req("GET", "/api/v1/config/bandwidth");
  },
  setBandwidth(body: BandwidthConfig): Promise<BandwidthConfig> {
    return jsonReq("PUT", "/api/v1/config/bandwidth", body);
  },
  throughput(): Promise<Throughput> {
    return req("GET", "/api/v1/system/throughput");
  },
  logSnapshot(): Promise<{ entries: LogEntry[] }> {
    return req("GET", "/api/v1/system/logs");
  },

  // --- subscriptions / webhooks ----------------------------------
  listSubscriptions(): Promise<{ subscriptions: Subscription[] }> {
    return req("GET", "/api/v1/subscriptions");
  },
  addSubscription(body: AddSubscriptionBody): Promise<{ id: number }> {
    return jsonReq("POST", "/api/v1/subscriptions", body);
  },
  patchSubscription(id: number, body: PatchSubscriptionBody): Promise<void> {
    return jsonReq("PATCH", `/api/v1/subscriptions/${id}`, body);
  },
  removeSubscription(id: number): Promise<void> {
    return req("DELETE", `/api/v1/subscriptions/${id}`);
  },
  testSubscription(id: number): Promise<void> {
    return req("POST", `/api/v1/subscriptions/${id}/test`);
  },
  enableSubscription(id: number, enabled: boolean): Promise<void> {
    return req("POST", `/api/v1/subscriptions/${id}/${enabled ? "enable" : "disable"}`);
  },
};

export type AddSubscriptionBody = {
  name: string;
  url: string;
  topics: string[];
  secret?: string;
  kind?: "webhook" | "discord" | "slack";
};

// PatchSubscriptionBody — name is immutable, every other field optional.
// Omit a key to leave it; pass empty-string secret to clear it.
export type PatchSubscriptionBody = {
  url?: string;
  topics?: string[];
  secret?: string;
  enabled?: boolean;
};

export type HistoryOpts = {
  state?: "completed" | "failed" | "aborted";
  category?: string;
  since?: string;
  limit?: number;
};

export type AddServerBody = {
  name: string;
  host: string;
  port: number;
  tls?: boolean;
  username?: string;
  password?: string;
  max_conns?: number;
  priority?: number;
  backup?: boolean;
  billing_mode?: "flat" | "metered";
  quota_bytes?: number;
  bandwidth_bytes_per_sec?: number;
};

// PatchServerBody mirrors AddServerBody but every field is optional.
// Omitted fields are left unchanged. Pass null on a string field to
// clear it (e.g. removing a username on a public server).
export type PatchServerBody = Partial<Omit<AddServerBody, "name">>;

export type TestServerBody = {
  host: string;
  port: number;
  tls?: boolean;
  username?: string;
  password?: string;
};

export type TestServerResult = {
  ok: boolean;
  dial: boolean;
  greeted: boolean;
  auth: boolean;
  mode_reader: boolean;
  date: boolean;
  server_date?: string;
  err?: string;
  elapsed_ms: number;
};

// streamURL returns the URL for the SSE endpoint. The session cookie
// is sent automatically; no apikey query param needed.
//
// Path note: /api/v1/events (not /queue/stream) — adblock filter
// lists often match any URL containing "stream" and silently kill
// the EventSource before it leaves the browser. The server still
// mounts the old /queue/stream path as an alias for back-compat.
export function streamURL(): string {
  return withBase("/api/v1/events");
}

// logStreamURL is the SSE endpoint that pushes new log entries.
// Uses /tail (not /stream) for the same adblock reason as streamURL —
// the server mounts both for back-compat.
export function logStreamURL(): string {
  return withBase("/api/v1/system/logs/tail");
}
