import { test, expect } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";

test.beforeEach(async ({ page }) => {
  await page.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url());
    let items: Record<string, unknown>[] = [];
    if (
      url.pathname.endsWith("snapshots") &&
      (!url.searchParams.get("asset_id") ||
        url.searchParams.get("asset_id") === "k8s-owned-router")
    )
      items = [1, 2].map((version) => ({
        snapshot_id: `k8s-owned-v${version}`,
        asset_id: "k8s-owned-router",
        version,
        event_time: "2026-01-01T00:00:00Z",
        ingest_time: "2026-01-01T00:00:00Z",
        raw_hash: "a".repeat(64),
        normalized_hash: "b".repeat(64),
        normalization_version: "text-v1",
      }));
    await route.fulfill({
      json: { items, count: items.length, has_more: false, next_cursor: null },
    });
  });
  await page.route("**/readyz", (route) =>
    route.fulfill({ json: { status: "ok" } }),
  );
});

const routes: [[string, string], ...[string, string][]] = [
  ["/", "Overview"],
  ["/history/", "Infrastructure History"],
  ["/rpki/", "RPKI States"],
  ["/ris/", "RIS Events"],
  ["/dns/", "DNS Measurements"],
  ["/experiments/", "Experiments"],
  ["/operations/", "Pipeline & Operations"],
];
for (const [path, title] of routes) {
  test(`${title} navigation, evidence and accessibility`, async ({ page }) => {
    await page.goto(path);
    await expect(
      page.getByRole("heading", { name: title, exact: true }),
    ).toBeVisible();
    await expect(page.locator("footer")).toContainText(
      "Verified evidence export",
    );
    const result = await new AxeBuilder({ page })
      .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
      .analyze();
    expect(result.violations).toEqual([]);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth,
      ),
    ).toBe(false);
  });
}
test("mobile sidebar changes route and closes sheet", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await page.getByRole("button", { name: "Toggle Sidebar" }).click();
  await page
    .getByRole("dialog")
    .getByRole("link", { name: "DNS Measurements" })
    .click();
  await expect(
    page.getByRole("heading", { name: "DNS Measurements", exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth > innerWidth,
    ),
  ).toBe(false);
});
test("tablet overview retains bounded layout", async ({ page }) => {
  await page.setViewportSize({ width: 768, height: 1024 });
  await page.goto("/");
  await expect(page.locator("footer")).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth > innerWidth,
    ),
  ).toBe(false);
});
test("complete table filtering, sorting and pagination", async ({ page }) => {
  await page.goto("/rpki/");
  await expect(
    page.getByRole("heading", { name: "Complete daily observations" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Record", exact: true }).click();
  await page
    .getByRole("button", { name: "Next page of Complete daily observations" })
    .click();
  await expect(page.getByText(/Page 2 of 2/)).toBeVisible();
  await page.getByLabel("Search records", { exact: true }).fill("2026-09-14");
  await expect(
    page.getByRole("button", { name: "2026-09-14", exact: true }),
  ).toBeVisible();
  await page
    .getByLabel("Search records", { exact: true })
    .fill("absent-entity");
  await expect(page.getByText(/No matching evidence/)).toBeVisible();
});
test("source day and source-minute range filters work", async ({ page }) => {
  await page.goto("/rpki/");
  await page.getByLabel("RPKI observation day").click();
  await page.getByRole("option", { name: "2026-09-01", exact: true }).click();
  await expect(
    page.getByText("No previous selected day", { exact: true }),
  ).toHaveCount(2);
  await page.goto("/ris/");
  await page.getByLabel("RIS time window").click();
  await page.getByRole("option", { name: "First 5 minutes" }).click();
  await expect(page.getByLabel("RIS time window")).toContainText(
    "First 5 minutes",
  );
  await page.locator(".recharts-wrapper").first().hover();
  await expect(page.locator(".recharts-tooltip-wrapper").first()).toBeVisible();
});
test("configuration search applies server-side asset and date bounds", async ({
  page,
}) => {
  await page.goto("/history/");
  await expect(
    page.getByRole("button", { name: "k8s-owned-v1", exact: true }),
  ).toBeVisible();
  await page
    .getByLabel("Asset ID (exact match)")
    .fill("unobserved-owned-asset");
  await page.getByLabel("From UTC date").fill("2026-01-01");
  const request = page.waitForRequest(
    (req) =>
      req.url().includes("asset_id=unobserved-owned-asset") &&
      req.url().includes("start="),
  );
  await page.getByRole("button", { name: "Apply filters" }).click();
  await request;
  await expect(page.getByText(/No matching evidence/)).toBeVisible();
});
test("server cursor pagination is honored", async ({ page }) => {
  await page.unroute("**/api/v1/**");
  await page.route("**/api/v1/**", (route) => {
    const second = new URL(route.request().url()).searchParams.has("cursor");
    return route.fulfill({
      json: {
        items: [
          {
            snapshot_id: second ? "owned-last" : "owned-first",
            asset_id: "k8s-owned-router",
            version: second ? 11 : 1,
          },
        ],
        count: 1,
        has_more: !second,
        next_cursor: second ? null : "owned-cursor",
      },
    });
  });
  await page.goto("/history/");
  await page.getByRole("button", { name: "Next server page" }).click();
  await expect(
    page.getByRole("button", { name: "owned-last", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Previous server page" }).click();
  await expect(
    page.getByRole("button", { name: "owned-first", exact: true }),
  ).toBeVisible();
});
test("owned source identity detail and keyboard close", async ({
  page,
  context,
}) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await page.goto("/ris/");
  await page.getByRole("tab", { name: "Owned occurrence identities" }).click();
  await page
    .getByRole("button", { name: /announcement.*192/ })
    .first()
    .click();
  await expect(page.getByRole("dialog")).toContainText(
    "PROJECT-OWNED DEMO FIXTURE",
  );
  await page.getByRole("button", { name: "Copy safe identifier" }).click();
  await expect(
    page.getByRole("button", { name: "Copied", exact: true }),
  ).toBeVisible();
  expect(
    (await page.evaluate(() => navigator.clipboard.readText())).length,
  ).toBe(64);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
});
test("DNS qualifiers and unexecuted H3 remain explicit", async ({ page }) => {
  await page.goto("/dns/");
  await expect(
    page.getByText(
      /Differing observed addresses do not prove authoritative DNS edits/,
    ),
  ).toBeVisible();
  await expect(page.getByText(/ENTITY COVERAGE UNKNOWN/).first()).toBeVisible();
  expect(await page.locator("main").innerText()).not.toMatch(
    /4,091 DNS configuration changes|676 DNS deletions/,
  );
  await page.goto("/experiments/");
  await expect(
    page.getByText(/RIS H3 physical 1× replay remains NOT_RUN/),
  ).toBeVisible();
  await page.getByRole("tab", { name: "Negative findings" }).click();
  await expect(
    page.getByText("2,207,468 additional bytes", { exact: true }),
  ).toBeVisible();
});
test("missing and corrupt evidence fail closed and retry", async ({ page }) => {
  await page.route("**/research/v1/catalog.json", (route) =>
    route.fulfill({ status: 503, body: "missing" }),
  );
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Evidence unavailable" }),
  ).toBeVisible();
  await expect(page.getByText("4,725,156", { exact: true })).toHaveCount(0);
  await page.unroute("**/research/v1/catalog.json");
  await page.getByRole("button", { name: "Retry evidence" }).click();
  await expect(page.getByText("4,725,156", { exact: true })).toBeVisible();
  await page.route("**/research/v1/catalog.json", (route) =>
    route.fulfill({ json: { schema_version: "dashboard-1" } }),
  );
  await page.reload();
  await expect(page.locator("main").getByRole("alert")).toContainText(
    "checksum mismatch",
  );
});
test("fresh API health and API failure do not display previous success", async ({
  page,
}) => {
  await page.goto("/operations/");
  await page.getByRole("button", { name: "Check API now" }).click();
  await expect(page.getByRole("status")).toContainText("LIVE READY");
  await page.unroute("**/readyz");
  await page.route("**/readyz", (route) =>
    route.fulfill({ status: 503, body: "down" }),
  );
  await page.getByRole("button", { name: "Check API now" }).click();
  await expect(page.getByRole("status")).toContainText("LIVE UNAVAILABLE");
  await page.goto("/history/");
  await page.unroute("**/api/v1/**");
  await page.route("**/api/v1/**", (route) =>
    route.fulfill({ status: 503, body: "down" }),
  );
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(page.locator("main").getByRole("alert")).toContainText(
    "metadata unavailable",
  );
});
test("navigation and console remain clean", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  await page.goto("/");
  for (const [, title] of routes.slice(1)) {
    await page.getByRole("link", { name: title, exact: true }).click();
    await expect(
      page.getByRole("heading", { name: title, exact: true }),
    ).toBeVisible();
    await expect(page.locator("footer")).toBeVisible();
  }
  expect(errors).toEqual([]);
});

test("health becomes stale at the declared freshness boundary", async ({
  page,
}) => {
  await page.clock.install();
  await page.goto("/operations/");
  await page.getByRole("button", { name: "Check API now" }).click();
  await expect(page.getByRole("status")).toContainText("LIVE READY");
  await page.clock.fastForward(61000);
  await expect(page.getByRole("status")).toContainText("STALE previous probe");
});
