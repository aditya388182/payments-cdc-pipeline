import { expect, test, type Page } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import path from "node:path";
import { api, API, mintToken, readState, REPO, SHOTS, sleep, STATE_FILE, type E2EState } from "./helpers";

interface Audit { entries: { action: string; target: string; result: string; actor: string }[] }
interface DlqPage { records: { id: string }[]; open_total: number }

const shot = (page: Page, name: string): Promise<Buffer> => page.screenshot({ path: `${SHOTS}/${name}.png`, animations: "disabled" });

async function signIn(page: Page, githubLogin: string): Promise<void> {
  await page.goto("/");
  await page.getByTestId("sign-in").click();
  await page.locator(`#user-${githubLogin}`).click(); // the mock GitHub page
  await expect(page.getByTestId("connection-banner")).toBeVisible();
}

async function openTab(page: Page, name: "live" | "alerts" | "dlq" | "audit" | "settings"): Promise<void> {
  await page.getByTestId(`tab-${name}`).click();
}

/** The DLQ list is oldest-first and paged (25 per page): like a user would, press "Load more" until the record shows up. */
async function revealDlqRow(page: Page, id: string): Promise<void> {
  const row = page.getByTestId(`dlq-row-${id}`);
  for (let i = 0; i < 40; i++) {
    if (await row.isVisible()) return;
    const more = page.getByTestId("dlq-more");
    if (!(await more.isVisible())) await sleep(500);
    else await more.click();
    await sleep(300);
  }
  await expect(row).toBeVisible();
}

/**
 * The DLQ scenario, staged after the load generator has stopped and the pipeline has drained (so the generator cannot
 * update or delete the injected row): a transaction for an unknown merchant is inserted in Postgres, the pipeline's
 * validator dead-letters it, and only then is the merchant added to the reference table. The pipeline's merchant cache
 * refreshes every 120 s, so the replay test waits for that (DECISIONS D-010).
 */
async function stageDlqScenario(admin: string): Promise<void> {
  const py = path.join(REPO, "venv/bin/python");
  await expect
    .poll(async () => (await api<{ caught_up: boolean; lag_total: number }>("GET", "/v1/snapshot", admin)).body.caught_up, { timeout: 420_000, intervals: [5_000] })
    .toBe(true);
  await sleep(15_000); // a full empty batch after the last record, so the pipeline's merchant cache is not mid-refresh
  const merchant = `MERCH_E2E_${Date.now().toString(36).toUpperCase().slice(-5)}`;
  const transactionId = (execFileSync(py, ["oncall/tools/dlq_demo.py", "inject", merchant], { cwd: REPO }).toString().trim().split(" ")[0]) ?? "";
  let dlqId = "";
  for (let i = 0; i < 40 && !dlqId; i++) {
    let cursor = "";
    do {
      const q = new URLSearchParams({ reason: "UNKNOWN_MERCHANT", limit: "100", ...(cursor ? { cursor } : {}) });
      const page = (await api<DlqPage & { next_cursor: string }>("GET", `/v1/dlq?${q.toString()}`, admin)).body;
      const hit = (page.records as { id: string; transaction_id: string }[]).find((r) => r.transaction_id === transactionId);
      dlqId = hit?.id ?? "";
      cursor = dlqId ? "" : page.next_cursor;
    } while (cursor);
    if (!dlqId) await sleep(5_000);
  }
  expect(dlqId, "the injected record never reached the DLQ").not.toBe("");
  execFileSync(py, ["oncall/tools/dlq_demo.py", "fix", merchant], { cwd: REPO });
  const state: E2EState = { transactionId, merchant, dlqId, fixedAtMs: Date.now(), cacheWaitMs: 130_000 };
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

test.describe.serial("on-call console end to end (mock GitHub, real API and pipeline)", () => {
  test("login screen, GitHub sign-in as an admin, live screen with charts", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByTestId("sign-in")).toBeVisible();
    await shot(page, "01-login");

    await page.getByTestId("sign-in").click();
    await expect(page.locator("#user-admin-user")).toBeVisible();
    await page.screenshot({ path: `${SHOTS}/02-mock-github.png` });
    await page.locator("#user-admin-user").click();

    await expect(page.getByTestId("connection-banner")).toHaveAttribute("aria-label", "Connected, live");
    await expect(page.getByTestId("parity-badge")).toBeVisible();
    await expect(page.getByTestId("connector-status")).toHaveAttribute("aria-label", "RUNNING");
    // wait for a minute of data so the charts have a line
    await expect.poll(async () => Number(((await page.getByTestId("snapshot-seq").textContent()) ?? "").replace(/\D/g, "")), { timeout: 60_000 }).toBeGreaterThan(0);
    await sleep(25_000);
    await expect(page.getByTestId("chart-throughput")).toHaveAttribute("aria-label", /events in now/);
    await shot(page, "03-live");
  });

  test("a lag alert fires, shows up on the Alerts screen and can be acknowledged", async ({ page }) => {
    await signIn(page, "admin-user");
    await openTab(page, "alerts");
    await expect(page.getByTestId("alerts-firing")).toBeVisible({ timeout: 120_000 });
    await expect(page.getByTestId("alerts-firing").getByText(/consumer lag high/i)).toBeVisible({ timeout: 120_000 });
    await shot(page, "04-alerts-firing");
    const ack = page.getByTestId("alerts-firing").getByRole("button", { name: "Acknowledge" }).first();
    await ack.click();
    await expect(page.getByTestId("alerts-acknowledged").getByText(/acknowledged by admin-user/)).toBeVisible();
    await shot(page, "05-alerts-acknowledged");
  });

  test("stop the load, then the DLQ list shows the dead-lettered record and its detail", async ({ page }) => {
    const admin = mintToken("admin", "admin-user");
    const stopped = await api("POST", "/v1/generator", admin, { action: "stop" }, { "Idempotency-Key": `e2e-stop-${Date.now()}` });
    expect([200, 409]).toContain(stopped.status);
    await stageDlqScenario(admin); // waits for the pipeline to drain, injects the bad record, adds the merchant

    const s = readState();
    await signIn(page, "admin-user");
    await openTab(page, "dlq");
    await page.getByTestId("filter-UNKNOWN_MERCHANT").click();
    await revealDlqRow(page, s.dlqId);
    await shot(page, "06-dlq-list");

    await page.getByTestId(`dlq-row-${s.dlqId}`).click();
    await expect(page.getByTestId("detail-reason")).toBeVisible();
    await expect(page.getByTestId("detail-payload")).toContainText(s.merchant);
    await expect(page.getByTestId("detail-error")).toContainText("UNKNOWN_MERCHANT");
    await shot(page, "07-dlq-detail");
  });

  test("a viewer sees no Replay button and no Audit tab, and the API refuses the replay with 403", async ({ page }) => {
    const s = readState();
    await signIn(page, "viewer-user");
    await expect(page.getByTestId("tab-audit")).toHaveCount(0);
    await openTab(page, "dlq");
    await page.getByTestId("filter-UNKNOWN_MERCHANT").click();
    await revealDlqRow(page, s.dlqId);
    await page.getByTestId(`dlq-row-${s.dlqId}`).click();
    await expect(page.getByTestId("detail-reason")).toBeVisible();
    await expect(page.getByTestId("replay-button")).toHaveCount(0);
    await expect(page.getByTestId("replay-admin-only")).toBeVisible();
    await shot(page, "08-viewer-no-replay");

    const token = await page.evaluate(() => (JSON.parse(sessionStorage.getItem("oncall.tokens.v1") ?? "{}") as { accessToken?: string }).accessToken ?? "");
    expect(token.length).toBeGreaterThan(20);
    const direct = await page.request.post(`${API}/v1/dlq/${s.dlqId}/replay`, { headers: { Authorization: `Bearer ${token}`, "Idempotency-Key": "viewer-attempt-0001" } });
    expect(direct.status()).toBe(403);
    expect(((await direct.json()) as { error: { code: string } }).error.code).toBe("forbidden");
    const audit = await page.request.get(`${API}/v1/audit`, { headers: { Authorization: `Bearer ${token}` } });
    expect(audit.status()).toBe(403);
  });

  test("admin replays the record: confirm, APPLIED, it leaves the queue, an audit row appears, a second replay is ALREADY_APPLIED", async ({ page }) => {
    const s = readState();
    const wait = s.fixedAtMs + s.cacheWaitMs - Date.now();
    if (wait > 0) await sleep(wait); // the pipeline's merchant cache must have refreshed (D-010)

    await signIn(page, "admin-user");
    await openTab(page, "dlq");
    await page.getByTestId("filter-UNKNOWN_MERCHANT").click();
    await revealDlqRow(page, s.dlqId);
    await page.getByTestId(`dlq-row-${s.dlqId}`).click();
    await expect(page.getByTestId("replay-button")).toBeVisible();
    await page.getByTestId("replay-button").click();
    await expect(page.getByTestId("confirm-sheet")).toBeVisible();
    await shot(page, "09-replay-confirm");
    await page.getByTestId("confirm-ok").click();
    await expect(page.getByTestId("replay-status")).toHaveAttribute("aria-label", "APPLIED", { timeout: 120_000 });
    await page.getByTestId("replay-result").scrollIntoViewIfNeeded();
    await shot(page, "10-replay-applied");

    // it left the queue
    const admin = mintToken("admin", "admin-user");
    const open = await api<DlqPage>("GET", "/v1/dlq?reason=UNKNOWN_MERCHANT&limit=100", admin);
    expect(open.body.records.map((r) => r.id)).not.toContain(s.dlqId);
    await page.goBack();
    await expect(page.getByTestId(`dlq-row-${s.dlqId}`)).toHaveCount(0);
    await shot(page, "11-dlq-after-replay");

    // ...and the audit log has the replay under the admin's name
    await openTab(page, "audit");
    await expect(page.getByTestId("audit-list").getByText("dlq.replay").first()).toBeVisible();
    await shot(page, "12-audit");
    const audit = await api<Audit>("GET", "/v1/audit?limit=100", admin);
    const rows = audit.body.entries.filter((e) => e.action === "dlq.replay" && e.target === `dlq:${s.dlqId}`);
    expect(rows.map((r) => r.result)).toContain("APPLIED");
    expect(rows[0]?.actor).toBe("admin-user");

    // replaying the same record again is a no-op: ALREADY_APPLIED
    await page.goto(`/dlq/${s.dlqId}`);
    await expect(page.getByTestId("replay-button")).toBeVisible();
    await page.getByTestId("replay-button").click();
    await page.getByTestId("confirm-ok").click();
    await expect(page.getByTestId("replay-status")).toHaveAttribute("aria-label", "ALREADY_APPLIED", { timeout: 60_000 });
    await page.getByTestId("replay-result").scrollIntoViewIfNeeded();
    await shot(page, "13-replay-already-applied");
    const again = await api<Audit>("GET", "/v1/audit?limit=100", admin);
    expect(again.body.entries.filter((e) => e.action === "dlq.replay" && e.target === `dlq:${s.dlqId}`).length).toBe(rows.length + 1);
  });

  test("after the drain the live screen is back in sync; settings shows the user and role; sign out returns to login", async ({ page }) => {
    await signIn(page, "admin-user");
    // the load stopped long ago and the replayed record is in the lake: counts agree (diff 0) and the stream is caught up
    await expect(page.getByTestId("parity-badge")).toHaveAttribute("aria-label", "In sync", { timeout: 180_000 });
    await expect(page.getByTestId("tile-lag")).toHaveAttribute("aria-label", /caught up/, { timeout: 180_000 });
    await expect(page.getByText(/diff 0\b/)).toBeVisible({ timeout: 180_000 });
    await shot(page, "17-live-in-sync");
    await openTab(page, "alerts");
    await expect(page.getByTestId("alerts-acknowledged").or(page.getByTestId("alerts-resolved")).first()).toBeVisible();
    await shot(page, "14-alerts-after");
    await openTab(page, "settings");
    await expect(page.getByTestId("settings-user")).toHaveText("admin-user");
    await expect(page.getByTestId("settings-role")).toHaveAttribute("aria-label", "ADMIN");
    await shot(page, "15-settings");
    await page.getByTestId("sign-out").click();
    await expect(page.getByTestId("sign-in")).toBeVisible();
    const stored = await page.evaluate(() => sessionStorage.getItem("oncall.tokens.v1"));
    expect(stored).toBeNull();
  });

  test("a GitHub account that is not allow-listed is refused with a clear message", async ({ page }) => {
    await page.goto("/");
    await page.getByTestId("sign-in").click();
    await page.locator("#user-outsider").click();
    await expect(page.getByText("This GitHub account is not allowed to use the console.")).toBeVisible();
    await shot(page, "16-not-allowed");
  });
});
