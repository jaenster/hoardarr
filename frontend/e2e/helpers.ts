// Helpers shared by every spec. The Hoardarr class brings a fresh
// hoardarr process up per spec, exposes its baseURL + apiKey, and
// proxies a few common operations (seed NZB on testserver, wait for
// a job to complete, etc).
//
// Each spec file should do:
//
//   test.beforeAll(async () => { await app.start(); });
//   test.afterAll(async () => { await app.stop(); });
//
// then in tests:
//
//   await page.goto(app.baseURL);

import { ChildProcess, spawn } from "node:child_process";
import * as fs from "node:fs";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";

type State = {
  hoardarrBin: string;
  testserverNNTP: string; // host:port for hoardarr's server config
  testserverHTTP: string; // http://host:port for control plane
  artifactDir: string;
};

function loadState(): State {
  const f = process.env.HOARDARR_E2E_STATE;
  if (!f) throw new Error("HOARDARR_E2E_STATE not set; globalSetup didn't run?");
  return JSON.parse(fs.readFileSync(f, "utf8")) as State;
}

export class HoardarrInstance {
  readonly state = loadState();
  proc?: ChildProcess;
  dataDir = "";
  listen = "";
  apiKey = "";

  get baseURL(): string {
    if (!this.listen) throw new Error("HoardarrInstance not started");
    return `http://${this.listen}`;
  }

  get testserverHTTP(): string {
    return this.state.testserverHTTP;
  }

  // testserverHost/Port — what hoardarr should be told to dial for NNTP.
  get testserverNNTP(): { host: string; port: number } {
    const [host, portStr] = this.state.testserverNNTP.split(":");
    return { host, port: parseInt(portStr, 10) };
  }

  async start() {
    this.dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "hoardarr-e2e-"));
    const port = await freePort();
    this.listen = `127.0.0.1:${port}`;
    // Pin a known API key so we don't have to fish it out of the
    // config file the binary writes on first run.
    this.apiKey = "e2e" + Math.random().toString(16).slice(2, 18).padEnd(29, "0");
    const cfgPath = path.join(this.dataDir, "config.toml");
    this.proc = spawn(
      this.state.hoardarrBin,
      ["serve", "--config", cfgPath],
      {
        env: {
          ...process.env,
          HOARDARR_LISTEN: this.listen,
          HOARDARR_DATA_DIR: this.dataDir,
          HOARDARR_API_KEY: this.apiKey,
        },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    // Surface logs to the playwright console so test failures are
    // easier to diagnose. We tee both streams.
    this.proc.stdout?.on("data", (b) => process.stdout.write(`[hoardarr] ${b}`));
    this.proc.stderr?.on("data", (b) => process.stderr.write(`[hoardarr] ${b}`));

    await this.waitListening();
    // Reset testserver state before this instance starts running tests.
    await fetch(`${this.state.testserverHTTP}/reset`, { method: "POST" });
  }

  async stop() {
    if (this.proc && !this.proc.killed) {
      this.proc.kill("SIGTERM");
      await new Promise<void>((resolve) => {
        let done = false;
        const onDone = () => {
          if (!done) {
            done = true;
            resolve();
          }
        };
        this.proc!.once("exit", onDone);
        setTimeout(() => {
          this.proc?.kill("SIGKILL");
          onDone();
        }, 3000);
      });
    }
    if (this.dataDir) {
      fs.rmSync(this.dataDir, { recursive: true, force: true });
    }
  }

  private async waitListening() {
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
      try {
        const res = await fetch(`${this.baseURL}/api/v1/health`);
        if (res.ok) return;
      } catch {
        /* not up yet */
      }
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error(`hoardarr did not start listening on ${this.listen}`);
  }

  // --- testserver helpers ---------------------------------------

  // seedNZB synthesizes articles on the fake server and returns the
  // NZB body bytes. Pass the bytes as `nzb` form-file to /api/v1/queue/nzb.
  async seedNZB(spec: {
    jobName: string;
    files: Array<{
      filename: string;
      segments: Array<{ msgID: string; sizeBytes: number }>;
    }>;
  }): Promise<Buffer> {
    const body = {
      job_name: spec.jobName,
      files: spec.files.map((f) => ({
        filename: f.filename,
        segments: f.segments.map((s) => ({
          msg_id: s.msgID,
          size_bytes: s.sizeBytes,
        })),
      })),
    };
    const res = await fetch(`${this.state.testserverHTTP}/seed-nzb`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`seed-nzb status=${res.status}: ${await res.text()}`);
    }
    return Buffer.from(await res.arrayBuffer());
  }

  async setTestserverOptions(opts: {
    bytesPerSec?: number;
    latencyMs?: number;
    missingFraction?: number;
  }) {
    const body: Record<string, number> = {};
    if (opts.bytesPerSec !== undefined) body.bytes_per_sec = opts.bytesPerSec;
    if (opts.latencyMs !== undefined) body.latency_ms = opts.latencyMs;
    if (opts.missingFraction !== undefined) body.missing_fraction = opts.missingFraction;
    const res = await fetch(`${this.state.testserverHTTP}/options`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`options: ${res.status}`);
  }
}

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      if (addr && typeof addr === "object") {
        const port = addr.port;
        srv.close(() => resolve(port));
      } else {
        reject(new Error("unexpected address"));
      }
    });
    srv.on("error", reject);
  });
}
