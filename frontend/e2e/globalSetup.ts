// Global setup runs once before any spec.
//
// Builds two Go binaries — hoardarr (with embedded frontend) and the
// fake testserver-nntpd — then spawns one testserver-nntpd that all
// specs share. Per-spec hoardarr instances are spawned in their own
// beforeAll hook so DB state is isolated; the testserver state is
// reset between specs via its HTTP control plane.
//
// Outputs:
//   playwright-state.json — paths + testserver address. Read by
//                           helpers.ts in each spec.

import { spawn, spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, "..", "..");
const ARTIFACT_DIR = path.join(os.tmpdir(), "hoardarr-e2e");
const HOARDARR_BIN = path.join(ARTIFACT_DIR, "hoardarr");
const TESTSERVER_BIN = path.join(ARTIFACT_DIR, "testserver-nntpd");
const STATE_FILE = path.join(ARTIFACT_DIR, "state.json");
const TESTSERVER_PID_FILE = path.join(ARTIFACT_DIR, "testserver.pid");
const TESTSERVER_ADDR_FILE = path.join(ARTIFACT_DIR, "testserver-addr.json");

export default async function globalSetup() {
  fs.mkdirSync(ARTIFACT_DIR, { recursive: true });

  // Build the frontend embed bundle so hoardarr -tags embed can pick
  // it up. The Go build below depends on assets being present.
  log("building frontend bundle");
  run("npm", ["run", "build"], { cwd: path.join(REPO_ROOT, "frontend") });

  log("building hoardarr binary");
  run("go", ["build", "-tags", "embed", "-o", HOARDARR_BIN, "./cmd/hoardarr"], {
    cwd: REPO_ROOT,
  });

  log("building testserver-nntpd binary");
  run("go", ["build", "-o", TESTSERVER_BIN, "./cmd/testserver-nntpd"], {
    cwd: REPO_ROOT,
  });

  // Spawn testserver-nntpd. It writes its addresses to a file once
  // ready; we poll until the file exists.
  if (fs.existsSync(TESTSERVER_ADDR_FILE)) {
    fs.unlinkSync(TESTSERVER_ADDR_FILE);
  }
  log("starting testserver-nntpd");
  const child = spawn(
    TESTSERVER_BIN,
    ["-addr-file", TESTSERVER_ADDR_FILE],
    {
      stdio: ["ignore", "inherit", "inherit"],
      detached: true,
    },
  );
  child.unref();
  fs.writeFileSync(TESTSERVER_PID_FILE, String(child.pid));

  const addr = await waitForFile(TESTSERVER_ADDR_FILE, 5000);
  const parsed = JSON.parse(addr) as { nntp: string; http: string };
  log(`testserver-nntpd nntp=${parsed.nntp} http=${parsed.http}`);

  const state = {
    hoardarrBin: HOARDARR_BIN,
    testserverNNTP: parsed.nntp,
    testserverHTTP: "http://" + parsed.http,
    testserverPidFile: TESTSERVER_PID_FILE,
    artifactDir: ARTIFACT_DIR,
  };
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
  process.env.HOARDARR_E2E_STATE = STATE_FILE;
}

function run(cmd: string, args: string[], opts: { cwd: string }) {
  const res = spawnSync(cmd, args, { ...opts, stdio: "inherit" });
  if (res.status !== 0) {
    throw new Error(`${cmd} ${args.join(" ")} failed (status=${res.status})`);
  }
}

async function waitForFile(p: string, timeoutMs: number): Promise<string> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const body = fs.readFileSync(p, "utf8");
      if (body.length > 0) return body;
    } catch {
      /* not yet */
    }
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`timeout waiting for ${p}`);
}

function log(msg: string) {
  // eslint-disable-next-line no-console
  console.log(`[globalSetup] ${msg}`);
}
