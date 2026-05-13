import { test } from "@playwright/test";
import * as fs from "node:fs";
import * as path from "node:path";
import { HoardarrInstance } from "../helpers";

// Capture README screenshots. NOT part of the normal CI suite — gated
// on HOARDARR_SCREENSHOTS=1 so a developer can refresh the images
// without surprising contributors who just want playwright to run.
//
//   make build
//   HOARDARR_SCREENSHOTS=1 npx playwright test screenshots
//
// Resulting PNGs land in docs/img/. Commit them and uncomment the
// matching lines in README.md.

const ENABLED = process.env.HOARDARR_SCREENSHOTS === "1";

const app = new HoardarrInstance();
const USERNAME = "admin";
const PASSWORD = "hunter22hunter22";
// Playwright runs with cwd = frontend/; docs/ sits at the repo root.
const SHOT_DIR = path.resolve(process.cwd(), "../docs/img");

test.beforeAll(async () => {
  if (!ENABLED) test.skip();
  await app.start();
  if (!fs.existsSync(SHOT_DIR)) fs.mkdirSync(SHOT_DIR, { recursive: true });
});
test.afterAll(async () => {
  if (!ENABLED) return;
  await app.stop();
});

// Don't reuse a session across the per-test fresh-login pattern; this
// is a single capture session, so logging in once at the start is
// what we want.
test.beforeEach(async ({ context }) => {
  if (!ENABLED) test.skip();
  await app.loginViaAPI(context, USERNAME, PASSWORD);
});

test.use({ viewport: { width: 1440, height: 900 } });

test("capture README screenshots", async ({ page, context }) => {
  // Throttle the fake NNTP so the job sits mid-flight long enough for
  // the Activity shot to show a partially-filled progress bar.
  await app.setTestserverOptions({
    bytesPerSec: 1024 * 16, // 16 KiB/s
    latencyMs: 50,
    missingFraction: 0,
  });

  // Wire two servers so the Servers card grid has more than one tile.
  const cookies = await context.cookies();
  const cookie = cookies.find((c) => c.name === "hoardarr_session");
  const cookieHeader = cookie ? `${cookie.name}=${cookie.value}` : "";
  const { host, port } = app.testserverNNTP;
  for (const [name, prio] of [["Primary", 0], ["Backup", 10]] as const) {
    await fetch(`${app.baseURL}/api/v1/servers`, {
      method: "POST",
      headers: { "content-type": "application/json", cookie: cookieHeader },
      body: JSON.stringify({
        name, host, port, tls: false, max_conns: 8, priority: prio,
      }),
    });
  }

  // Drop a fat-ish fixture so progress is visible at 16 KiB/s.
  const { nzb } = await app.seedFixture({
    name: "Release.Name.2026.1080p.WEB-DL.x264-DEMO",
    fileCount: 1,
    fileSize: 1024 * 1024, // 1 MiB
    articleSize: 32 * 1024,
    par2SliceSize: 32 * 1024,
    recoverySlices: 4,
  });
  const fd = new FormData();
  fd.set(
    "nzb",
    new Blob([nzb], { type: "application/x-nzb" }),
    "Release.Name.2026.1080p.WEB-DL.x264-DEMO.nzb",
  );
  const upRes = await fetch(`${app.baseURL}/api/v1/queue/nzb`, {
    method: "POST",
    headers: { cookie: cookieHeader },
    body: fd,
  });
  const upBody = (await upRes.json()) as { job_id: number };

  // ---- Activity (mid-flight) ----
  await page.goto(app.baseURL + "/activity");
  // Give the SSE a moment to push the first progress event.
  await page.waitForTimeout(2000);
  await page.screenshot({
    path: path.join(SHOT_DIR, "activity.png"),
    fullPage: false,
  });

  // ---- Job detail (file explorer) ----
  await page.goto(app.baseURL + `/jobs/${upBody.job_id}`);
  await page.waitForTimeout(1500);
  await page.screenshot({
    path: path.join(SHOT_DIR, "job-detail.png"),
    fullPage: false,
  });

  // ---- Settings → Servers ----
  await page.goto(app.baseURL + "/settings");
  await page.waitForTimeout(1000);
  // Scroll to the Servers section if there's a sidebar anchor.
  const serversHeading = page.getByRole("heading", { name: /Servers/i }).first();
  if (await serversHeading.count() > 0) {
    await serversHeading.scrollIntoViewIfNeeded();
  }
  await page.screenshot({
    path: path.join(SHOT_DIR, "settings-servers.png"),
    fullPage: false,
  });

  // ---- Let the job finish, then History ----
  // Speed it back up so the rest finishes within the test timeout.
  await app.setTestserverOptions({ bytesPerSec: 0, latencyMs: 0 });
  await page.waitForTimeout(8000); // pipeline cleanup
  await page.goto(app.baseURL + "/history");
  await page.waitForTimeout(1500);
  await page.screenshot({
    path: path.join(SHOT_DIR, "history.png"),
    fullPage: false,
  });
});
