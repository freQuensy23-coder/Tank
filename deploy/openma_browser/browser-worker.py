#!/usr/bin/env python3
"""VM-resident Playwright RPC. Unix socket only; never binds a TCP/CDP port.

The command-line client starts one detached daemon per session state directory.
The daemon retains the actual Page across SandboxPort.exec invocations; closing
the context flushes the session's persistent Chromium profile to the VM disk.
"""

import argparse
import asyncio
import base64
import fcntl
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

MAX_MESSAGE = 32 * 1024 * 1024


def paths(state):
    digest = hashlib.sha256(str(state).encode()).hexdigest()[:32]
    root = Path("/tmp") / ("tank-browser-" + digest)
    root.mkdir(mode=0o700, exist_ok=True)
    os.chmod(root, 0o700)
    return root / "rpc.sock", root / "start.lock"


async def serve(state):
    from playwright.async_api import async_playwright

    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(state, 0o700)
    sock_path, _ = paths(state)
    sock_path.unlink(missing_ok=True)
    lock = asyncio.Lock()
    context = None
    page = None
    async with async_playwright() as playwright:

        async def operation(request):
            nonlocal context, page
            if request.get("version") != 1:
                raise ValueError("Unsupported browser protocol version")
            op, args = request["op"], request.get("args", {})
            if op == "close":
                if context is not None:
                    await context.close()
                context = page = None
                return None
            if op not in {
                "open",
                "goto",
                "screenshot",
                "click",
                "fill",
                "press",
                "inner_text",
                "evaluate",
                "content",
            }:
                raise ValueError("Unsupported browser operation")
            if context is None:
                context = await playwright.chromium.launch_persistent_context(
                    str(state / "profile"),
                    headless=True,
                    channel="chromium",
                    accept_downloads=True,
                    downloads_path=str(state / "downloads"),
                    viewport={"width": 1280, "height": 900},
                )
                context.set_default_timeout(30_000)
            if page is None or page.is_closed():
                page = context.pages[0] if context.pages else await context.new_page()
            if op == "open":
                return None
            if op == "goto":
                response = await page.goto(
                    args["url"],
                    wait_until=args.get("waitUntil") or "domcontentloaded",
                    timeout=args.get("timeout") or 30_000,
                )
                return {"status": response.status if response else None}
            if op == "screenshot":
                data = await page.screenshot(
                    full_page=args.get("fullPage", False), type=args.get("type", "png")
                )
                return base64.b64encode(data).decode()
            if op == "content":
                return await page.content()
            if op == "evaluate":
                return await page.evaluate(args["expression"], args.get("arg"))
            locator = page.locator(args["selector"]).first
            timeout = args.get("timeout") or 30_000
            if op == "click":
                return await locator.click(timeout=timeout)
            if op == "fill":
                return await locator.fill(args["text"], timeout=timeout)
            if op == "press":
                return await locator.press(args["key"], timeout=timeout)
            return await locator.inner_text(timeout=timeout)

        async def handle(reader, writer):
            try:
                raw = await asyncio.wait_for(reader.readline(), 10)
                if len(raw) > MAX_MESSAGE:
                    raise ValueError("Browser request exceeds size limit")
                async with lock:
                    try:
                        result = await operation(json.loads(raw))
                        response = {
                            "version": 1,
                            "ok": True,
                            "result": result,
                            "url": page.url if page else "about:blank",
                        }
                    except Exception as error:  # noqa: BLE001 - RPC translates browser errors into tool results.
                        response = {"version": 1, "ok": False, "error": str(error)[:4000]}
                encoded = json.dumps(response, ensure_ascii=True, allow_nan=False).encode() + b"\n"
                if len(encoded) > MAX_MESSAGE:
                    encoded = b'{"version":1,"ok":false,"error":"Browser response exceeds size limit"}\n'
                writer.write(encoded)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_unix_server(handle, str(sock_path), limit=MAX_MESSAGE + 1)
        os.chmod(sock_path, 0o600)
        async with server:
            await server.serve_forever()


def connect(sock_path):
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(75)
    try:
        client.connect(str(sock_path))
    except Exception:
        client.close()
        raise
    return client


def request(state, encoded):
    data = base64.b64decode(encoded, validate=True)
    if len(data) > MAX_MESSAGE:
        raise ValueError("Browser request exceeds size limit")
    json.loads(data)  # Validate before starting a daemon.
    sock_path, lock_path = paths(state)
    # Lock startup only: concurrent tool calls must never launch two Chromium
    # owners for one persistent profile. The daemon serializes actual actions.
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            client = connect(sock_path)
        except (FileNotFoundError, ConnectionRefusedError):
            subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "--state-dir", str(state), "--serve"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
            deadline = time.monotonic() + 15
            while True:
                try:
                    client = connect(sock_path)
                    break
                except (FileNotFoundError, ConnectionRefusedError):
                    if time.monotonic() > deadline:
                        raise RuntimeError(
                            "Browser worker did not start; verify Python Playwright in the sandbox image"
                        )
                    time.sleep(0.05)
    with client:
        client.sendall(data + b"\n")
        with client.makefile("rb") as stream:
            result = stream.readline(MAX_MESSAGE + 1)
        if not result or len(result) > MAX_MESSAGE:
            raise ValueError("Invalid browser worker response length")
        sys.stdout.write(result.decode())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--serve", action="store_true")
    mode.add_argument("--request")
    args = parser.parse_args()
    state = Path(args.state_dir).resolve()
    if args.serve:
        asyncio.run(serve(state))
    else:
        try:
            request(state, args.request)
        except Exception as error:  # noqa: BLE001 - CLI protocol must emit a structured transport error.
            print(json.dumps({"version": 1, "ok": False, "error": str(error)[:4000]}))


if __name__ == "__main__":
    main()
