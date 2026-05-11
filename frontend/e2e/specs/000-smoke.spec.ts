import { expect, test } from "@playwright/test";
import { HoardarrInstance } from "../helpers";

const app = new HoardarrInstance();

test.beforeAll(async () => {
  await app.start();
});
test.afterAll(async () => {
  await app.stop();
});

test("hoardarr instance is reachable and reports needs_setup", async ({ page }) => {
  const res = await page.request.get(`${app.baseURL}/api/v1/health`);
  expect(res.status()).toBe(200);

  const whoami = await page.request.get(`${app.baseURL}/api/v1/auth/whoami`);
  expect(whoami.status()).toBe(200);
  const body = await whoami.json();
  expect(body.state).toBe("needs_setup");
});

test("testserver-nntpd seed endpoint returns an NZB body", async () => {
  const nzb = await app.seedNZB({
    jobName: "smoke",
    files: [
      {
        filename: "smoke.bin",
        segments: [{ msgID: "smoke@h", sizeBytes: 1024 }],
      },
    ],
  });
  expect(nzb.length).toBeGreaterThan(0);
  expect(nzb.toString()).toContain("<nzb");
  expect(nzb.toString()).toContain("smoke@h");
});
