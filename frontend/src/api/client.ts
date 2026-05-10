// Tiny fetch wrapper for /api/v1. Reads the API key from localStorage
// and attaches it as X-Api-Key. SSE uses a separate URL builder because
// EventSource can't set headers — for SSE we pass the key as a query
// param.

import type { Category, Job, Server } from "./types";

const KEY_STORAGE = "hoardarr.apiKey";

export function getApiKey(): string | null {
  return localStorage.getItem(KEY_STORAGE);
}

export function setApiKey(k: string): void {
  localStorage.setItem(KEY_STORAGE, k);
}

export function clearApiKey(): void {
  localStorage.removeItem(KEY_STORAGE);
}

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
  const key = getApiKey();
  const headers: Record<string, string> = { ...extraHeaders };
  if (key) headers["X-Api-Key"] = key;
  const res = await fetch(path, { method, headers, body });
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

export const api = {
  health(): Promise<{ status: string; service: string }> {
    return req("GET", "/api/v1/health");
  },
  whoami(): Promise<{ service: string; version: string; authenticated: boolean }> {
    return req("GET", "/api/v1/whoami");
  },
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
  listServers(): Promise<{ servers: Server[] | null }> {
    return req("GET", "/api/v1/servers");
  },
  listCategories(): Promise<{ categories: Category[] | null }> {
    return req("GET", "/api/v1/categories");
  },
};

// streamURL returns the URL for the SSE endpoint with the api key
// embedded as a query parameter (EventSource can't set headers).
export function streamURL(): string {
  const key = getApiKey();
  return `/api/v1/queue/stream${key ? "?apikey=" + encodeURIComponent(key) : ""}`;
}
