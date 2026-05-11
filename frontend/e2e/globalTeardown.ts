import * as fs from "node:fs";

export default async function globalTeardown() {
  const stateFile = process.env.HOARDARR_E2E_STATE;
  if (!stateFile || !fs.existsSync(stateFile)) return;
  const state = JSON.parse(fs.readFileSync(stateFile, "utf8")) as {
    testserverPidFile?: string;
  };
  if (state.testserverPidFile && fs.existsSync(state.testserverPidFile)) {
    const pid = parseInt(fs.readFileSync(state.testserverPidFile, "utf8"), 10);
    if (Number.isFinite(pid)) {
      try {
        process.kill(pid, "SIGTERM");
      } catch {
        /* already gone */
      }
    }
  }
}
