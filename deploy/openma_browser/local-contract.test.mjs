/** Explicit local fake-VM contract: actual Chromium, no operator profile/keys.
 * Run with Node >=24 and TANK_BROWSER_TEST_PYTHON pointing to Python Playwright.
 * This proves the SandboxPort boundary, not microVM isolation.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";
import { createSandboxBrowserHarness } from "../../vendor/openma/apps/agent/src/harness/sandbox-browser.ts";

const run = promisify(execFile);
const here = dirname(fileURLToPath(import.meta.url));

test("actual Playwright through fake VM exec: actions, PNG, persistence, isolation", { timeout: 90_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "tank-browser-contract-"));
  const server = createServer((_req, res) => {
    res.setHeader("content-type", "text/html");
    res.end(`<html><body><h1>Tank browser fixture</h1><input id="field"><button onclick='document.querySelector("#result").innerText=document.querySelector("#field").value'>Apply</button><p id="result"></p></body></html>`);
  });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const url = `http://127.0.0.1:${server.address().port}/`;
  const options = {
    python: process.env.TANK_BROWSER_TEST_PYTHON ?? "python3",
    worker: resolve(here, "browser-worker.py"),
    stateRoot: root,
  };
  const commands = [];
  // No inherited auth, X profile, or application secrets in the fake VM.
  const env = { PATH: "/usr/bin:/bin", HOME: root, PLAYWRIGHT_BROWSERS_PATH: process.env.PLAYWRIGHT_BROWSERS_PATH };
  const sandbox = { async exec(command) {
    commands.push(command);
    const { stdout } = await run("/bin/sh", ["-c", command], { env, maxBuffer: 32 * 1024 * 1024, timeout: 90_000 });
    return stdout;
  } };
  const sessions = [];
  try {
    const session = await createSandboxBrowserHarness(sandbox, "one", options).launch();
    sessions.push(session);
    assert.equal(commands.length, 0);
    const page = await session.page();
    assert.equal((await page.goto(url)).status(), 200);
    assert.equal(page.url(), url);
    await page.locator("#field").first().fill("real browser value");
    await page.locator("button").first().click();
    assert.equal(await page.locator("#result").first().innerText(), "real browser value");
    await page.locator("#field").first().press("Enter");
    assert.match(await page.content(), /Tank browser fixture/);
    assert.equal(await page.evaluate("document.querySelectorAll('button').length"), 1);
    assert.equal(await page.evaluate(arg => arg.value + 1, { value: 41 }), 42);
    assert.equal((await page.screenshot({ type: "png", fullPage: true })).subarray(0, 8).toString("hex"), "89504e470d0a1a0a");
    await page.evaluate("localStorage.setItem('session', 'one'); document.cookie='session=one; Max-Age=3600; Path=/'");
    // Another toolset build still sees the same live page, without relaunch.
    const continuation = await createSandboxBrowserHarness(sandbox, "one", options).launch();
    sessions.push(continuation);
    const continuedPage = await continuation.page();
    assert.equal(await continuedPage.locator("#result").first().innerText(), "real browser value");
    const other = await createSandboxBrowserHarness(sandbox, "two", options).launch();
    sessions.push(other);
    const isolatedPage = await other.page();
    await isolatedPage.goto(url);
    assert.equal(await isolatedPage.evaluate("localStorage.getItem('session')"), null);
    assert.equal(await isolatedPage.evaluate("document.cookie"), "");
    await session.close();
    assert.equal(session.isOpen(), false);
    const reopened = await createSandboxBrowserHarness(sandbox, "one", options).launch();
    sessions.push(reopened);
    const reopenedPage = await reopened.page();
    await reopenedPage.goto(url);
    assert.equal(await reopenedPage.evaluate("localStorage.getItem('session')"), "one");
    assert.match(await reopenedPage.evaluate("document.cookie"), /session=one/);
    await assert.rejects(reopenedPage.locator("#missing").first().click({ timeout: 100 }), /Timeout/);
    await reopened.close();
    await other.close();
    // Emulate workspace checkpoint/restore into a replacement fake VM.
    await cp(root, root + "-restored", { recursive: true });
    const restored = await createSandboxBrowserHarness(sandbox, "one", { ...options, stateRoot: root + "-restored" }).launch();
    sessions.push(restored);
    const restoredPage = await restored.page();
    await restoredPage.goto(url);
    assert.equal(await restoredPage.evaluate("localStorage.getItem('session')"), "one");
    assert.match(await restoredPage.evaluate("document.cookie"), /session=one/);
    assert.ok(commands.length > 20);
  } finally {
    for (const session of sessions) await session.close().catch(() => {});
    // Daemons are owned by the fake VM fixture and must not outlive the test.
    await run("/usr/bin/pkill", ["-f", `${options.worker} --state-dir ${root}`], { env }).catch(() => {});
    await new Promise(resolve => server.close(resolve));
    await rm(root, { recursive: true, force: true });
    await rm(root + "-restored", { recursive: true, force: true });
  }
});
