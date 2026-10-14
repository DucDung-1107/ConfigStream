import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  testMatch: "*.spec.ts",
  fullyParallel: false,
  workers: 1,
  timeout: 30000,
  use: {
    baseURL: process.env.DASHBOARD_URL ?? "http://127.0.0.1:13000",
    browserName: "chromium",
    channel: process.env.CI ? undefined : "chrome",
    viewport: { width: 1440, height: 900 },
    trace: "retain-on-failure",
  },
  webServer: process.env.DASHBOARD_URL
    ? undefined
    : {
        command: "node tests/serve.mjs",
        url: "http://127.0.0.1:13000",
        reuseExistingServer: !process.env.CI,
      },
  reporter: [
    ["list"],
    ["json", { outputFile: "test-results/interaction-results.json" }],
  ],
});
