import { chromium, expect } from "@playwright/test";
import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const base = process.env.DASHBOARD_URL ?? "http://127.0.0.1:13000";
const directory = new URL("../../docs/screenshots/phase20v/", import.meta.url);
await mkdir(directory, { recursive: true });
const browser = await chromium.launch({ channel: "chrome", headless: true });
const page = await browser.newPage({
  viewport: { width: 1440, height: 900 },
  permissions: ["clipboard-read", "clipboard-write"],
});
const errors = [];
page.on("pageerror", (error) => errors.push(error.message));
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
  new URL("../../.cache/phase20v/browser-review.json", import.meta.url),
  JSON.stringify(
    {
      base,
      captured_at: new Date().toISOString(),
      measurements,
      page_errors: errors,
      mobile_navigation: "PASS",
      safe_identifier_copy: "PASS",
      error_and_empty_states: "PASS",
    },
    null,
    2,
  ),
);
console.log(JSON.stringify({ measurements, page_errors: errors }));
await browser.close();
