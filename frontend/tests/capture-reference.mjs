import { chromium } from "@playwright/test";
import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const directory = new URL("../../docs/screenshots/phase20v/", import.meta.url);
await mkdir(directory, { recursive: true });
const browser = await chromium.launch({ channel: "chrome", headless: true });
const page = await browser.newPage();
await page.goto("https://ui.shadcn.com/view/new-york-v4/dashboard-01", {
  waitUntil: "domcontentloaded",
});
await page.getByRole("heading", { name: "Documents", exact: true }).waitFor();
await page.locator(".recharts-wrapper").first().waitFor();
await page.evaluate(() => document.fonts.ready);
for (const [name, width, height] of [
  ["reference-desktop", 1440, 900],
  ["reference-mobile", 390, 844],
]) {
  await page.setViewportSize({ width, height });
  await page.screenshot({
    path: fileURLToPath(new URL(`${name}.png`, directory)),
  });
}
await writeFile(
  new URL("../../.cache/phase20v/reference-capture.json", import.meta.url),
  JSON.stringify(
    {
      url: page.url(),
      captured_at: new Date().toISOString(),
      desktop: [1440, 900],
      mobile: [390, 844],
    },
    null,
    2,
  ),
);
await browser.close();
