import { expect, test } from "@playwright/test";
import { HoardarrInstance } from "../helpers";

// First-run experience: hoardarr boots empty, the UI shows the
// "create admin" screen, after submit the user lands on the
// authenticated UI. Then a separate test (with admin already
// created via the API) drives the "add usenet server + test
// connection" UI flow against the fake NNTP.

const app = new HoardarrInstance();
const USERNAME = "admin";
const PASSWORD = "hunter22";

test.beforeAll(async () => {
  await app.start();
});
test.afterAll(async () => {
  await app.stop();
});

test("first-run setup form creates the admin and lands on Activity", async ({ page }) => {
  // The setup form only renders when no admin exists. This test runs
  // first; subsequent tests setupAdmin via the API in beforeEach.
  await page.goto(app.baseURL);

  await expect(page.getByRole("heading", { name: "Welcome to hoardarr" })).toBeVisible();
  await page.getByLabel("Username").fill(USERNAME);
  await page.getByLabel("Password (8+ chars)").fill(PASSWORD);
  await page.getByLabel("Confirm password").fill(PASSWORD);
  await page.getByRole("button", { name: "Create admin" }).click();

  await expect(page.getByRole("heading", { name: "Activity" })).toBeVisible();
});

test("add usenet server + Test connection succeeds against the fake", async ({ page, context }) => {
  // Admin was created by the previous test (we share an instance via
  // beforeAll). Just log in via API so this test's browser context
  // sees the session cookie before page.goto.
  await app.loginViaAPI(context, USERNAME, PASSWORD);

  await page.goto(app.baseURL + "/settings");
  await expect(page.getByRole("heading", { name: "Usenet Servers" })).toBeVisible();

  const { host, port } = app.testserverNNTP;

  // The Servers section is a Sonarr-style card grid (#129). The
  // "+" tile opens a modal with the add-server form.
  await page.getByRole("button", { name: "Add server" }).first().click();

  // Scope to the open modal so selectors don't ambiguously match
  // categories / webhooks / etc.
  const dialog = page.getByRole("dialog");
  await expect(dialog).toBeVisible();

  await dialog.getByRole("textbox", { name: "Name", exact: true }).fill("fake");
  await dialog.getByRole("textbox", { name: "Host", exact: true }).fill(host);
  await dialog.getByRole("spinbutton", { name: "Port", exact: true }).fill(String(port));

  // Fake speaks plain TCP, so flip TLS off.
  const tls = dialog.getByRole("checkbox", { name: "TLS" });
  if (await tls.isChecked()) await tls.click();

  // Test connection from inside the modal — fastest signal that the
  // host/port reach the fake. The probe banner appears in the modal
  // body on success.
  await dialog.getByRole("button", { name: "Test" }).click();
  await expect(dialog.locator(".probe-banner.probe-ok")).toBeVisible();
  await expect(dialog.locator(".probe-step-ok", { hasText: "Dial" })).toBeVisible();
  await expect(dialog.locator(".probe-step-ok", { hasText: "MODE READER" })).toBeVisible();
  await expect(dialog.locator(".probe-step-ok", { hasText: "DATE" })).toBeVisible();

  // Submit. Modal closes and a card with the new server's name
  // appears in the grid.
  await dialog.getByRole("button", { name: "Add server" }).click();
  await expect(dialog).toBeHidden();
  await expect(
    page.locator("section#servers").getByRole("button", { name: /Edit fake/ }),
  ).toBeVisible();
});
