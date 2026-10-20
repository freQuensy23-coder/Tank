"""Distinct post-retained file-I/O overlay; skills and native lifecycle inputs stay frozen."""

import hashlib
import json
from pathlib import Path

TARGET = "packages/control-plane-node/src/lib/tank-retained/boxlite-provider.ts"
ORIGINAL_SHA256 = "ecbbe30d29a7e2c48ff918681d4816a0af57ecda2ece093bcf023f101763fca9"


def apply(root):
    asset = Path(__file__).resolve().parent
    root = Path(root).resolve()
    target = root / TARGET
    if target.is_symlink() or not target.resolve().is_relative_to(root):
        raise ValueError("Unsafe retained provider source path")
    for name in ("guest-file-io.ts", "guest-file-worker.ts"):
        if target.with_name(name).exists() or target.with_name(name).is_symlink():
            raise ValueError("File-I/O overlay asset already exists")
    manifest = json.loads((asset / "manifest.json").read_text())
    for name, expected in manifest["assets_sha256"].items():
        if hashlib.sha256((asset / name).read_bytes()).hexdigest() != expected:
            raise ValueError("File-I/O overlay frozen asset hash mismatch")
    source = target.read_text()
    if hashlib.sha256(target.read_bytes()).hexdigest() != ORIGINAL_SHA256:
        raise ValueError("Retained provider file-I/O source hash mismatch")
    source = source.replace(
        "import { readFile, writeFile } from 'node:fs/promises';",
        "import { copyGuestFileIn, copyGuestFileOut } from './guest-file-io.js';",
        1,
    )
    anchor = "  async exec(command: string, args: string[], env?: Record<string, string>, options?: { cwd?: string; user?: string; timeoutSecs?: number }) {\n"
    replacement = """  async exec(command: string, args: string[], env?: Record<string, string>, options?: { cwd?: string; user?: string; timeoutSecs?: number }) {
    return this.run(command, args, env, options, async process => {
      const completion = Promise.all([text(process.stdout), text(process.stderr), process.exited]);
      void completion.catch(() => undefined);
      const writer = process.stdin.getWriter();
      try { await writer.close().catch(() => undefined); } finally { writer.releaseLock(); }
      const [stdout, stderr, result] = await completion;
      return { stdout, stderr, exitCode: result.code ?? 1 };
    });
  }
  private async run<T>(command: string, args: string[], env: Record<string, string> | undefined,
    options: { cwd?: string; user?: string; timeoutSecs?: number } | undefined,
    exchange: (process: Process) => Promise<T>): Promise<T> {
"""
    if source.count(anchor) != 1:
        raise ValueError("Retained exec method anchor mismatch")
    source = source.replace(anchor, replacement, 1)
    start = source.index("        // Drain output while closing stdin; either native handshake may stall.")
    end = source.index("\n      };\n      try { return await Promise.race", start)
    source = (
        source[:start]
        + """        const result = await exchange(process);
        if (timedOut || performance.now() >= deadline) throw expire();
        return result;"""
        + source[end:]
    )
    old = "      try { return await Promise.race([execute(), expired]); }\n      finally { if (timer) clearTimeout(timer); }"
    new = """      try { return await Promise.race([execute(), expired]); }
      catch (error) {
        // A channel/input/parser failure can leave a guest write in progress.
        // Confirmed guest errno results are returned by exchange and raised only
        // after this scope; transport failures require the same safe stop.
        this.unsafe.add(runtime); killExactProcess(); throw error;
      }
      finally { if (timer) clearTimeout(timer); }"""
    if source.count(old) != 1:
        raise ValueError("Retained exec cleanup anchor mismatch")
    source = source.replace(old, new, 1)
    old = """  async copyIn(host: string, guest: string): Promise<void> {
    const bytes = await readFile(host);
    await this.use(runtime => runtime.writeFileBytes(guest, bytes));
  }
  async copyOut(guest: string, host: string): Promise<void> {
    const bytes = await this.use(runtime => runtime.readFileBytes(guest));
    await writeFile(host, bytes);
  }"""
    new = """  async copyIn(host: string, guest: string): Promise<void> {
    await copyGuestFileIn((args, exchange) => this.run('python3', args, undefined, { timeoutSecs: 120 }, exchange), host, guest);
  }
  async copyOut(guest: string, host: string): Promise<void> {
    await copyGuestFileOut((args, exchange) => this.run('python3', args, undefined, { timeoutSecs: 120 }, exchange), guest, host);
  }"""
    if source.count(old) != 1:
        raise ValueError("Retained file method anchor mismatch")
    source = source.replace(old, new, 1)
    target.write_text(source)
    target.with_name("guest-file-io.ts").write_bytes((asset / "guest-file-io.ts").read_bytes())
    worker = (asset / "guest-file-io.py").read_text()
    target.with_name("guest-file-worker.ts").write_text(
        "export const GUEST_FILE_WORKER = " + json.dumps(worker) + ";\n"
    )

    result = {
        "stage": "guest-streamed-file-io",
        "status": "OFFLINE_TESTED_NATIVE_NOT_QUALIFIED",
        "source_sha256": {TARGET: ORIGINAL_SHA256},
        "patched_file_hashes": {TARGET: hashlib.sha256(target.read_bytes()).hexdigest()},
        "added_file_hashes": {
            str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (target.with_name("guest-file-io.ts"), target.with_name("guest-file-worker.ts"))
        },
        "asset_manifest_sha256": hashlib.sha256((asset / "manifest.json").read_bytes()).hexdigest(),
        "file_operation_budget_seconds": 120,
        "browser_budgets_unchanged": True,
        "native_vm_proof": "NOT_RUN",
    }
    (root / "tank-file-io-overlay.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("root")
    apply(parser.parse_args().root)
