import { defineConfig } from "@playwright/test";

// Runs against the Expo WEB build served on 127.0.0.1:19006, the real oncall-api on 8088 and the mock GitHub on 8099.
// `make oncall-e2e` starts all of that; see oncall/e2e/run.sh.
export default defineConfig({
  testDir: "./e2e",
  timeout: 300_000,
  expect: { timeout: 20_000 },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [["list"], ["json", { outputFile: "test-results/results.json" }]],
  globalSetup: "./e2e/global-setup.ts",
  use: {
    baseURL: process.env.E2E_WEB ?? "http://127.0.0.1:19006",
    viewport: { width: 390, height: 844 },
    deviceScaleFactor: 2,
    trace: "retain-on-failure",
    video: process.env.E2E_VIDEO ? "on" : "off",
  },
  projects: [{ name: "chromium", use: { browserName: "chromium" } }],
});
