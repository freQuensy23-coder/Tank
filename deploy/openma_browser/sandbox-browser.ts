/** Tank's browser transport: every Playwright operation executes in SandboxPort.
 * No host Playwright import, CDP endpoint, browser profile, or network proxy.
 */
import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import type { SandboxPort } from "@open-managed-agents/sandbox";
import type { BrowserHarness, BrowserPage, BrowserSessionOpts } from "@open-managed-agents/browser-harness";

export interface SandboxBrowserOptions {
  /** Test seams. Production uses only the defaults baked into the VM image. */
  python?: string;
  worker?: string;
  stateRoot?: string;
}

function quote(value: string): string {
  return "'" + value.replaceAll("'", "'\\''") + "'";
}

const REPLY_PREFIX = "__TANK_BROWSER_RPC_V1__ ";
const MAX_REPLY_CHARS = 32 * 1024 * 1024;

/** SandboxPort.exec combines stdout and stderr. Frame the worker reply so
 * native runtime diagnostics cannot corrupt otherwise successful RPC output.
 */
function readReply(raw: string): string {
  if (raw.length > MAX_REPLY_CHARS * 2) throw new Error("Sandbox browser transport output exceeds size limit");
  const lines = raw.split(/\r?\n/);
  const replies = lines.filter(line => line.startsWith(REPLY_PREFIX));
  const failed = lines.some(line => !line.startsWith(REPLY_PREFIX) && /^\[(?:exit(?:\s|\])|error:)/.test(line));
  if (failed) throw new Error("Sandbox browser transport reported a failed command");
  if (replies.length !== 1) throw new Error("Sandbox browser worker must return exactly one protocol reply");
  const reply = replies[0]!.slice(REPLY_PREFIX.length);
  if (reply.length > MAX_REPLY_CHARS) throw new Error("Sandbox browser reply exceeds size limit");
  return reply;
}

export function createSandboxBrowserHarness(
  sandbox: Pick<SandboxPort, "exec">,
  sessionId: string,
  options: SandboxBrowserOptions = {},
): BrowserHarness {
  if (!sessionId) throw new Error("Sandbox browser requires a session identity");
  const identity = createHash("sha256").update(sessionId).digest("hex").slice(0, 32);
  const stateDir = `${options.stateRoot ?? "/workspace/.tank-browser"}/${identity}`;
  const command = [options.python ?? "python3", options.worker ?? "/opt/tank/browser-worker.py", "--state-dir", stateDir].map(quote).join(" ");
  return {
    async launch(opts?: BrowserSessionOpts) {
      let opened = false;
      let openedAt = 0;
      let currentUrl = "about:blank";
      async function rpc(op: string, args: Record<string, unknown> = {}): Promise<any> {
        const request = Buffer.from(JSON.stringify({ version: 1, op, args })).toString("base64");
        const raw = await sandbox.exec(`${command} --request ${quote(request)}`, 90_000);
        const reply = readReply(raw);
        let response: { version?: number; ok?: boolean; error?: string; result?: unknown; url?: string };
        try { response = JSON.parse(reply); }
        catch { throw new Error("Sandbox browser worker returned an invalid response; verify the sandbox image and execution status"); }
        if (!response || typeof response !== "object" || Array.isArray(response)
          || response.version !== 1 || typeof response.ok !== "boolean"
          || (response.url !== undefined && typeof response.url !== "string")
          || (response.ok ? !("result" in response) : typeof response.error !== "string")) {
          throw new Error("Sandbox browser protocol mismatch");
        }
        if (!response.ok) throw new Error(response.error ?? "Sandbox browser operation failed");
        if (response.url !== undefined) currentUrl = response.url;
        return response.result;
      }
      const page: BrowserPage = {
        async goto(url, opts) {
          const result = await rpc("goto", { url, waitUntil: opts?.waitUntil, timeout: opts?.timeout });
          return result.status === null ? null : { status: () => result.status };
        },
        url: () => currentUrl,
        async screenshot(opts) { return Buffer.from(await rpc("screenshot", opts), "base64"); },
        locator(selector: string) {
          const locator = {
            first: () => locator,
            click: (opts?: { timeout?: number }) => rpc("click", { selector, ...opts }),
            fill: (text: string, opts?: { timeout?: number }) => rpc("fill", { selector, text, ...opts }),
            press: (key: string) => rpc("press", { selector, key }),
            innerText: (opts?: { timeout?: number }) => rpc("inner_text", { selector, ...opts }),
          };
          return locator;
        },
        evaluate: (expr, arg) => rpc("evaluate", {
          expression: typeof expr === "string" ? expr : `(${expr.toString()})`,
          ...(arg === undefined ? {} : { arg }),
        }),
        content: () => rpc("content"),
      };
      return {
        async page() {
          if (!opened) { await rpc("open"); opened = true; openedAt = Date.now(); }
          return page;
        },
        isOpen: () => opened,
        async close() {
          if (!opened) return;
          await rpc("close");
          opened = false;
          const seconds = Math.floor((Date.now() - openedAt) / 1000);
          if (seconds > 0 && opts?.hook) {
            try { await opts.hook.onClose(seconds); }
            catch { /* Billing failure must not turn a successful browser close into a failed tool. */ }
          }
        },
      };
    },
  };
}
