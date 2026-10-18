import { describe, expect, it } from "vitest";
import { Buffer } from "node:buffer";
import { buildBrowserTools, disposeBrowserTools } from "../src/index";
import { createSandboxBrowserHarness } from "../../../apps/agent/src/harness/sandbox-browser";

function fakeSandbox() {
  const calls: Array<{ command: string; op: string; args: any }> = [];
  return {
    calls,
    async exec(command: string) {
      const payload = command.match(/--request '([^']+)'$/)![1]!;
      const request = JSON.parse(Buffer.from(payload, "base64").toString());
      calls.push({ command, ...request });
      const result = request.op === "goto" ? { status: 200 }
        : request.op === "inner_text" ? "Fixture text"
        : request.op === "evaluate" ? { answer: 42 }
        : request.op === "screenshot" ? Buffer.from([137, 80, 78, 71]).toString("base64") : null;
      return JSON.stringify({ version: 1, ok: true, result, url: "https://fixture.invalid/final" });
    },
  };
}

describe("Tank SandboxPort browser transport", () => {
  it("implements every actual pinned browser tool and screenshot model output", async () => {
    const sandbox = fakeSandbox();
    const tools = buildBrowserTools(createSandboxBrowserHarness(sandbox, "session-a"));
    expect(Object.keys(tools).sort()).toEqual(["browser_click", "browser_close", "browser_eval", "browser_get_text", "browser_navigate", "browser_screenshot", "browser_type"]);
    expect(sandbox.calls).toHaveLength(0);
    expect(await tools.browser_navigate.execute({ url: "https://fixture.invalid" })).toBe("Loaded https://fixture.invalid/final (HTTP 200)");
    expect(await tools.browser_click.execute({ selector: "button" })).toBe("Clicked: button");
    const text = "' ; $(touch /tmp/should-never-exist) `id`\n";
    expect(await tools.browser_type.execute({ selector: "input", text, submit: true })).toContain("submitted");
    expect(await tools.browser_get_text.execute({ max_chars: 7 })).toContain("Fixture\n\n...[truncated");
    expect(await tools.browser_eval.execute({ expression: "({answer: 42})" })).toContain('"answer": 42');
    const shot = await tools.browser_screenshot.execute({ full_page: true });
    expect(shot.source).toEqual({ type: "base64", media_type: "image/png", data: "iVBORw==" });
    expect(tools.browser_screenshot.toModelOutput({ output: shot })).toEqual({ type: "content", value: [{ type: "file-data", data: "iVBORw==", mediaType: "image/png" }] });
    expect(await tools.browser_close.execute({})).toBe("Browser session closed.");
    expect(sandbox.calls.map(c => c.op)).toEqual(["open", "goto", "click", "fill", "press", "inner_text", "evaluate", "screenshot", "close"]);
    expect(sandbox.calls.find(c => c.op === "fill")?.args.text).toBe(text);
    expect(sandbox.calls.every(c => !c.command.includes(text))).toBe(true);
    expect(sandbox.calls.every(c => c.command.startsWith("'python3' '/opt/tank/browser-worker.py'"))).toBe(true);
  });

  it("separates identities and reuses the same session profile across harness builds", async () => {
    const sandbox = fakeSandbox();
    await (await createSandboxBrowserHarness(sandbox, "session-a").launch()).page();
    await (await createSandboxBrowserHarness(sandbox, "session-b").launch()).page();
    await (await createSandboxBrowserHarness(sandbox, "session-a").launch()).page();
    const state = (command: string) => command.match(/--state-dir' '([^']+)'/)![1];
    expect(state(sandbox.calls[0]!.command)).toBe(state(sandbox.calls[2]!.command));
    expect(state(sandbox.calls[0]!.command)).not.toBe(state(sandbox.calls[1]!.command));
  });

  it("fails clearly on transport or browser errors instead of opening a host browser", async () => {
    for (const output of ["[error: VM unavailable]", '{"version":1,"ok":false,"error":"Chromium unavailable"}']) {
      const tools = buildBrowserTools(createSandboxBrowserHarness({ exec: async () => output }, "a"));
      expect(await tools.browser_navigate.execute({ url: "https://fixture.invalid" })).toMatch(/^Navigate error:/);
    }
  });

  it("flushes an opened browser before workspace checkpoint, including implicit end of turn", async () => {
    const sandbox = fakeSandbox();
    const tools = buildBrowserTools(createSandboxBrowserHarness(sandbox, "a"));
    await tools.browser_navigate.execute({ url: "https://fixture.invalid" });
    await disposeBrowserTools(tools);
    await disposeBrowserTools(tools);
    expect(sandbox.calls.map(c => c.op)).toEqual(["open", "goto", "close"]);
    const unused = buildBrowserTools(createSandboxBrowserHarness(sandbox, "b"));
    await disposeBrowserTools(unused);
    expect(sandbox.calls).toHaveLength(3);
  });

  it("owns browser cleanup through the actual aggregate agent ToolSet", async () => {
    const { buildTools, disposeTools } = await import("../../../apps/agent/src/harness/tools");
    const sandbox = fakeSandbox();
    const tools = await buildTools({ tools: [{ type: "agent_toolset_20260401", default_config: { enabled: false }, configs: [{ name: "browser", enabled: true }] }] } as any, sandbox as any, {
      browser: createSandboxBrowserHarness(sandbox, "aggregate"),
    });
    await tools.browser_navigate.execute({ url: "https://fixture.invalid" });
    await disposeTools(tools);
    expect(sandbox.calls.map(c => c.op)).toEqual(["open", "goto", "close"]);
  });

  it("disposes confirmed toolsets after execution failures", async () => {
    const { NodeManagedConfirmedToolExecutor } = await import("../../../packages/control-plane-node/src/lib/node-managed-confirmed-tool-executor");
    let disposed = false;
    const executor = new NodeManagedConfirmedToolExecutor({
      buildExecutableTools: async () => ({ browser_click: { execute: async () => { throw new Error("click failed"); } } }),
      disposeTools: async () => { disposed = true; },
    });
    const result = await executor.execute({ toolUse: { name: "browser_click", input: {}, id: "tool" }, abortSignal: new AbortController().signal } as any);
    expect(result.isError).toBe(true);
    expect(disposed).toBe(true);
  });
});
