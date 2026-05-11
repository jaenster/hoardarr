import { expect, test } from "@playwright/test";
import { HoardarrInstance } from "../helpers";

// First-run experience: hoardarr boots empty, the UI shows the
// "create admin" screen, after submit the user lands on the
// authenticated UI. Then add a usenet server pointing at the fake
// NNTP and verify Test connection succeeds end-to-end.

const app = new HoardarrInstance();

test.beforeAll(async () => {
  await app.start();
});
test.afterAll(async () => {
  await app.stop();
});

test("first-run setup", async ({ page }) => {
  await page.goto(app.baseURL);

  await expect(page.getByRole("heading", { name: "Welcome to hoardarr" })).toBeVisible();
  await page.getByLabel("Username").fill("admin");
  await page.getByLabel("Password (8+ chars)").fill("hunter22");
  await page.getByLabel("Confirm password").fill("hunter22");
  await page.getByRole("button", { name: "Create admin" }).click();

  // Top-level Activity heading appears once we're authenticated.
  await expect(page.getByRole("heading", { name: "Activity" })).toBeVisible();
});

test("add usenet server + test connection succeeds against fake", async ({ page }) => {
  await page.goto(app.baseURL);

  // Settings is in the sidebar.
  await page.getByRole("link", { name: "Settings" }).click();
  await expect(page.getByRole("heading", { name: "Usenet Servers" })).toBeVisible();

  const { host, port } = app.testserverNNTP;

  await page.getByRole("textbox", { name: "Name" }).first().fill("fake");
  await page.getByRole("textbox", { name: "Host" }).first().fill(host);
  // Port is type=number; fill triggers the right event.
  await page.getByRole("spinbutton", { name: "Port" }).first().fill(String(port));
  // The default TLS toggle is "on"; the fake speaks plain TCP, so flip it.
  const tlsToggle = page.getByRole("checkbox", { name: "TLS" }).first();
  if (await tlsToggle.isChecked()) await tlsToggle.click();

  // Username / password are optional on the fake (auth disabled by
  // default) — leave blank.

  await page.getByRole("button", { name: "Add server" }).click();

  // New row appears in the servers table.
  const row = page.getByRole("row", { name: /fake/ });
  await expect(row).toBeVisible();

  // Click the per-row Test connection button. The probe runs against
  // the saved creds and lights up each step green on success.
  await row.getByLabel("Test connection").click();

  await expect(page.locator(".probe-banner.probe-ok")).toBeVisible();
  await expect(page.locator(".probe-step-ok", { hasText: "Dial" })).toBeVisible();
  await expect(page.locator(".probe-step-ok", { hasText: "MODE READER" })).toBeVisible();
  await expect(page.locator(".probe-step-ok", { hasText: "DATE" })).toBeVisible();
});
