import { chromium, expect } from "@playwright/test";
import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const base = process.env.DASHBOARD_URL ?? "http://127.0.0.1:13000";
const directory = process.env.REVIEW_OUTPUT_DIR
  ? new URL(`file://${process.env.REVIEW_OUTPUT_DIR.replace(/\/$/, "")}/`)
  : new URL("../../docs/screenshots/phase20v/", import.meta.url);
await mkdir(directory, { recursive: true });
const browser = await chromium.launch({ channel: "chrome", headless: true });
const page = await browser.newPage({
  viewport: { width: 1440, height: 900 },
  permissions: ["clipboard-read", "clipboard-write"],
});
const errors = [];
const expectedUnavailableErrors = [];
const failedResponses = [];
let errorFixtureActive = false;
page.on("pageerror", (error) => errors.push(error.message));
page.on("console", (message) => {
  if (message.type() !== "error") return;
  if (errorFixtureActive && /503/.test(message.text()))
    expectedUnavailableErrors.push(message.text());
  else errors.push(message.text());
});
page.on("response", (response) => {
  if (response.status() >= 400)
    failedResponses.push({
      url: response.url(),
      status: response.status(),
      intentional_error_fixture: errorFixtureActive,
    });
});
const routes = [
  ["overview", "/", "Overview"],
  ["history", "/history/", "Infrastructure History"],
  ["rpki", "/rpki/", "RPKI States"],
  ["ris", "/ris/", "RIS Events"],
  ["dns", "/dns/", "DNS Measurements"],
  ["experiments", "/experiments/", "Experiments"],
  ["operations", "/operations/", "Pipeline & Operations"],
];
const measurements = [];
for (const [name, path, title] of routes) {
  const started = performance.now();
  await page.goto(base + path, { waitUntil: "domcontentloaded" });
  await expect(
    page.getByRole("heading", { name: title, exact: true }),
  ).toBeVisible();
  await expect(page.locator("footer")).toContainText(
    "Verified evidence export",
  );
  if (name === "history")
    await expect(
      page.getByRole("button", { name: "k8s-owned-v1", exact: true }),
    ).toBeVisible();
  if (name === "history") {
    await page.getByRole("tab", { name: "Adjacent diffs" }).click();
    await expect(
      page.getByRole("button", { name: /^diff-/ }).first(),
    ).toBeVisible();
    await page
      .getByRole("button", { name: /^diff-/ })
      .first()
      .click();
    await expect(page.getByRole("dialog")).toContainText("k8s-owned-v1");
    await expect(page.getByRole("dialog")).toContainText("k8s-owned-v2");
    await page.keyboard.press("Escape");
    await page.getByRole("tab", { name: "Versions", exact: true }).click();
    await expect(
      page.getByRole("button", { name: "k8s-owned-v1", exact: true }),
    ).toBeVisible();
  }
  if (name === "operations") {
    await page.getByRole("button", { name: "Check API now" }).click();
    await expect(page.getByRole("status")).toContainText("LIVE READY");
  }
  await page.evaluate(() => document.fonts.ready);
  await page.screenshot({
    path: fileURLToPath(new URL(`${name}-desktop.png`, directory)),
    fullPage: true,
  });
  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth > innerWidth,
  );
  measurements.push({
    route: path,
    viewport: [1440, 900],
    ready_seconds: (performance.now() - started) / 1000,
    overflow,
  });
  if (name === "overview" || name === "ris") {
    await page.getByLabel("RIS time window").click();
    await page.getByRole("option", { name: "First 5 minutes" }).click();
    await expect(page.getByLabel("RIS time window")).toContainText(
      "First 5 minutes",
    );
  }
  if (name === "rpki") {
    await page.getByLabel("RPKI observation day").click();
    await page.getByRole("option", { name: "2026-09-01", exact: true }).click();
    await expect(
      page.getByText("No previous selected day", { exact: true }),
    ).toHaveCount(2);
  }
  if (name === "experiments") {
    await page.getByRole("tab", { name: "Matched processing" }).click();
    await expect(
      page.getByRole("heading", { name: "Matched normalized-core trials" }),
    ).toBeVisible();
    await page.getByRole("tab", { name: "Negative findings" }).click();
    await expect(page.getByText("2,207,468", { exact: false })).toBeVisible();
  }
}
for (const [name, width, height] of [
  ["tablet", 768, 1024],
  ["mobile", 390, 844],
]) {
  await page.setViewportSize({ width, height });
  await page.goto(base, { waitUntil: "domcontentloaded" });
  await expect(page.locator("footer")).toBeVisible();
  await page.screenshot({
    path: fileURLToPath(new URL(`overview-${name}.png`, directory)),
    fullPage: true,
  });
  measurements.push({
    route: "/",
    viewport: [width, height],
    overflow: await page.evaluate(
      () => document.documentElement.scrollWidth > innerWidth,
    ),
  });
}
await page.getByRole("button", { name: "Toggle Sidebar" }).click();
await page
  .getByRole("dialog")
  .getByRole("link", { name: "DNS Measurements" })
  .click();
await expect(
  page.getByRole("heading", { name: "DNS Measurements", exact: true }),
).toBeVisible();
await expect(page.getByRole("dialog")).toHaveCount(0);
await page.setViewportSize({ width: 1440, height: 900 });
await page.goto(base + "/ris/", { waitUntil: "domcontentloaded" });
await page.getByRole("tab", { name: "Owned occurrence identities" }).click();
await expect(
  page.getByRole("heading", { name: "Owned routing occurrence details" }),
).toBeVisible();
await page
  .getByRole("button", { name: /announcement.*192/ })
  .first()
  .click();
await expect(page.getByRole("dialog")).toBeVisible();
await page.getByRole("button", { name: "Copy safe identifier" }).click();
await expect(
  page.getByRole("button", { name: "Copied", exact: true }),
).toBeVisible();
await page.screenshot({
  path: fileURLToPath(new URL("evidence-detail.png", directory)),
});
await page.getByRole("button", { name: "Close", exact: true }).click();
await page.getByLabel("Search records", { exact: true }).fill("does-not-exist");
await expect(
  page.getByText("No matching evidence.", { exact: false }),
).toBeVisible();
await page.screenshot({
  path: fileURLToPath(new URL("empty-filter.png", directory)),
});
errorFixtureActive = true;
await page.route("**/research/v1/catalog.json", (route) =>
  route.fulfill({ status: 503, body: "unavailable" }),
);
await page.goto(base + "/dns/", { waitUntil: "domcontentloaded" });
await expect(
  page.getByRole("heading", { name: "Evidence unavailable" }),
).toBeVisible();
await page.screenshot({
  path: fileURLToPath(new URL("evidence-error.png", directory)),
});
await writeFile(
  process.env.REVIEW_RESULT_PATH ??
    new URL("../../.cache/phase20v/browser-review.json", import.meta.url),
  JSON.stringify(
    {
      base,
      captured_at: new Date().toISOString(),
      measurements,
      page_errors: errors,
      failed_responses: failedResponses,
      expected_error_state_console_messages: expectedUnavailableErrors.length,
      real_configuration_versions: 2,
      real_adjacent_diff: 1,
      real_api_probe: "READY",
      mobile_navigation: "PASS",
      safe_identifier_copy: "PASS",
      error_and_empty_states: "PASS",
    },
    null,
    2,
  ),
);
console.log(
  JSON.stringify({ measurements, page_errors: errors, failedResponses }),
);
await browser.close();
if (errors.length > 0) process.exitCode = 1;
