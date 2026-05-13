import { expect, test } from "@playwright/test";
import { HoardarrInstance } from "../helpers";

// Realistic multi-file release with PAR2 + intentionally missing
// articles. Drives the whole download → verify → repair → deliver
// pipeline from a real browser session.
//
// The Go e2e (TestE2E_RealisticReleaseWithRepair) covers the wire
// behaviour; this spec verifies the UI surfaces the right names,
// the file-list panel populates, and the job lands in History.

const app = new HoardarrInstance();
const USERNAME = "admin";
const PASSWORD = "hunter22";

test.beforeAll(async () => {
  await app.start();
});
test.afterAll(async () => {
  await app.stop();
});

test.beforeEach(async ({ context }) => {
  await app.loginViaAPI(context, USERNAME, PASSWORD);
});

// Pipeline is download → verify → repair → re-verify → deliver. Each
// step is fast on the synthetic fixture but they add up, and when this
// spec runs after others, the testserver-nntpd shared instance has
// fielded enough connections that the first articles arrive ~1s slower
// than in isolation. The default 60 s spec timeout starts to bite.
test.setTimeout(120_000);

test("fixture with PAR2 repair: download + repair + delivery via UI", async ({ page, context }) => {
  // Drop ~10% of articles. With 4 recovery slices covering 12 data
  // slices, repair is comfortably possible.
  await app.setTestserverOptions({ missingFraction: 0.10 });

  // Add a usenet server pointing at the fake (hot-wire picks it up).
  const cookies = await context.cookies();
  const cookie = cookies.find((c) => c.name === "hoardarr_session");
  const cookieHeader = cookie ? `${cookie.name}=${cookie.value}` : "";
  const { host, port } = app.testserverNNTP;
  const sres = await fetch(`${app.baseURL}/api/v1/servers`, {
    method: "POST",
    headers: { "content-type": "application/json", cookie: cookieHeader },
    body: JSON.stringify({ name: "fake", host, port, tls: false, max_conns: 4 }),
  });
  expect(sres.ok).toBeTruthy();

  // Generate a realistic release on the fake: 3 data files × 96 KiB,
  // sliced at 32 KiB → 9 data slices, 4 recovery slices (~45%
  // redundancy). NZB ends up referencing ~17 articles total.
  const { nzb, files, articleCount } = await app.seedFixture({
    name: "fixture-e2e",
    fileCount: 3,
    fileSize: 96 * 1024,
    articleSize: 32 * 1024,
    par2SliceSize: 32 * 1024,
    recoverySlices: 4,
  });
  expect(articleCount).toBeGreaterThan(10);
  expect(files.length).toBe(3 + 1 + 4); // data + index + recovery

  // Upload via API. (UI upload is covered by spec 002; this spec
  // focuses on the multi-file + repair flow on the receiving end.)
  const fd = new FormData();
  fd.set("nzb", new Blob([nzb], { type: "application/x-nzb" }), "fixture-e2e.nzb");
  const upRes = await fetch(`${app.baseURL}/api/v1/queue/nzb`, {
    method: "POST",
    headers: { cookie: cookieHeader },
    body: fd,
  });
  expect(upRes.ok).toBeTruthy();
  const upBody = (await upRes.json()) as { job_id: number };

  // Open the job detail page by id. The release may already have
  // transitioned to History (small fixtures finish before Playwright
  // can poll Activity), so don't depend on the queue row being there.
  await page.goto(app.baseURL + "/jobs/" + upBody.job_id);
  await expect(page.getByRole("heading", { name: "Files" })).toBeVisible({ timeout: 10_000 });
  // The file explorer (#116) renders one .file-tree-item per file —
  // data + index .par2 + recovery vols all show.
  const fileRows = page.locator(".file-tree-item");
  await expect.poll(async () => await fileRows.count(), { timeout: 10_000 }).toBeGreaterThanOrEqual(
    files.length,
  );

  // Poll history API for terminal state — the job should reach
  // "completed" once verify + repair + deliver finish.
  await expect.poll(
    async () => {
      const r = await fetch(`${app.baseURL}/api/v1/history`, {
        headers: { cookie: cookieHeader },
      });
      const body = (await r.json()) as { jobs: { id: number; state: string }[] | null };
      return (body.jobs ?? []).find((j) => j.id === upBody.job_id)?.state;
    },
    { timeout: 60_000 },
  ).toMatch(/completed/);
});
