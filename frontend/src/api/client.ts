// Tiny fetch wrapper for /api/v1.
//
// Auth: requests are authenticated by the session cookie that the
// server issues on /auth/login (HTTP-only, sent automatically by the
// browser via `credentials: "include"`). The X-Api-Key path remains
// in the server middleware for *arr clients but the web UI no longer
// uses it.

import type { Category, Job, Server, User } from "./types";

export class ApiError extends Error {
  constructor(public status: number, public body: unknown, msg: string) {
    super(msg);
  }
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
  const res = await fetch(path, {
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

  // --- public -----------------------------------------------------
  health(): Promise<{ status: string; service: string }> {
    return req("GET", "/api/v1/health");
  },

  // --- queue ------------------------------------------------------
  listQueue(includeAll = false): Promise<{ jobs: Job[] | null }> {
    const qs = includeAll ? "?include=all" : "";
    return req("GET", "/api/v1/queue" + qs);
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

  // --- servers + categories --------------------------------------
  listServers(): Promise<{ servers: Server[] | null }> {
    return req("GET", "/api/v1/servers");
  },
  listCategories(): Promise<{ categories: Category[] | null }> {
    return req("GET", "/api/v1/categories");
  },
};

// streamURL returns the URL for the SSE endpoint. The session cookie
// is sent automatically; no apikey query param needed.
export function streamURL(): string {
  return "/api/v1/queue/stream";
}
