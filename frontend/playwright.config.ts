import { defineConfig } from "@playwright/test";

// Playwright runs against a real hoardarr binary (frontend embedded
// via -tags embed). Each spec file boots its own hoardarr instance
// in beforeAll so DB state is isolated; a shared testserver-nntpd
// is brought up once in globalSetup and torn down in globalTeardown.
//
// baseURL is set per-test via the HoardarrInstance helper since each
// instance binds a different port. Specs use the `app` fixture.

export default defineConfig({
  testDir: "./e2e/specs",
  // One worker so specs that share the testserver-nntpd address don't
  // step on each other. Each spec already spawns its own hoardarr.
  workers: 1,
  fullyParallel: false,
  retries: 0,
  reporter: [["list"]],
  timeout: 60_000,
  expect: { timeout: 10_000 },
  globalSetup: "./e2e/globalSetup.ts",
  globalTeardown: "./e2e/globalTeardown.ts",
  use: {
    headless: true,
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
    ignoreHTTPSErrors: true,
  },
  projects: [
    { name: "chromium", use: { browserName: "chromium" } },
  ],
});
