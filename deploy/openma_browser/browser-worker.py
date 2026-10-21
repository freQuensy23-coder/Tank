#!/usr/bin/env python3
"""VM-resident Playwright RPC. Unix socket only; never binds a TCP/CDP port.

The command-line client starts one detached daemon per session state directory.
The daemon retains the actual Page across SandboxPort.exec invocations; closing
the context flushes the session's persistent Chromium profile to the VM disk.
"""

import argparse
import asyncio
import base64
import ctypes
import fcntl
import hashlib
import json
import math
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

MAX_MESSAGE = 32 * 1024 * 1024
REQUEST_TIMEOUT = 75.0
RESPONSE_GRACE = 5.0
REPLY_PREFIX = "__TANK_BROWSER_RPC_V1__ "


class BrowserLaunchFailure(Exception):
    """A failed native launch requires recycling its supervised process tree."""


def paths(state):
    digest = hashlib.sha256(str(state).encode()).hexdigest()[:32]
    root = Path("/tmp") / ("tank-browser-" + digest)
    root.mkdir(mode=0o700, exist_ok=True)
    os.chmod(root, 0o700)
    return root / "rpc.sock", root / "start.lock"


def supervise_descendants():
    if sys.platform == "linux":
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:
            code = ctypes.get_errno()
            raise OSError(code, os.strerror(code))
        handle = os.pidfd_open(os.getpid())
        try:
            signal.pidfd_send_signal(handle, 0)
        finally:
            os.close(handle)


def terminate_owned_descendants(startup_pid=None):
    """Freeze/kill/reap only this Linux worker's supervised descendant tree.

    Stable pidfds avoid signaling a reused PID. Freeze parents before walking
    their children. Subreaper adoption catches double-forked helpers and any
    child fork that races SIGSTOP delivery; repeat until no live child remains.
    """
    handles = []

    def stop_children(parent):
        try:
            children = Path(f"/proc/{parent}/task/{parent}/children").read_text().split()
        except FileNotFoundError:
            return
        for child in children:
            pid = int(child)
            if parent == os.getpid() and startup_pid is not None and pid != startup_pid:
                continue
            try:
                handle = os.pidfd_open(pid)
            except ProcessLookupError:
                continue
            try:
                fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
                if int(fields[1]) != parent:
                    os.close(handle)
                    continue
                signal.pidfd_send_signal(handle, signal.SIGSTOP)
                handles.append(handle)
                stop_children(pid)
            except (FileNotFoundError, ProcessLookupError):
                os.close(handle)

    first = True
    while True:
        stop_children(os.getpid())
        if not handles:
            if first and startup_pid is not None:
                startup_pid = None
                first = False
                continue
            break
        for handle in reversed(handles):
            try:
                signal.pidfd_send_signal(handle, signal.SIGKILL)
            except ProcessLookupError:
                pass
            finally:
                os.close(handle)
        handles.clear()
        while True:
            try:
                reaped, _ = os.waitpid(-1, os.WNOHANG)
                if not reaped:
                    break
            except ChildProcessError:
                break
        if first:
            startup_pid = None
            first = False
        time.sleep(0.001)


def request_deadline(request):
    deadline = request.get("deadline")
    if isinstance(deadline, bool) or not isinstance(deadline, (float, int)) or not math.isfinite(deadline):
        raise ValueError("Browser request requires a finite monotonic deadline")
    if deadline > time.monotonic() + REQUEST_TIMEOUT:
        raise ValueError("Browser request exceeds its allowed deadline")
    return deadline


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

        async def operation(request, deadline):
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
                try:
                    context = await playwright.chromium.launch_persistent_context(
                        str(state / "profile"),
                        timeout=max(1, int(remaining(deadline, "browser launch") * 1000)),
                        headless=True,
                        accept_downloads=True,
                        downloads_path=str(state / "downloads"),
                        viewport={"width": 1280, "height": 900},
                    )
                except Exception as error:
                    raise BrowserLaunchFailure(str(error)) from error
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
            fatal = acquired = False
            task = None
            try:
                raw = await asyncio.wait_for(reader.readline(), 10)
                if len(raw) > MAX_MESSAGE:
                    raise ValueError("Browser request exceeds size limit")
                request = json.loads(raw)
                deadline = request_deadline(request)
                try:
                    await asyncio.wait_for(lock.acquire(), remaining(deadline, "action queue"))
                except TimeoutError as error:
                    raise TimeoutError("Browser request exceeded its budget during action queue") from error
                acquired = True
                seconds = remaining(deadline, "browser action")
                task = asyncio.create_task(operation(request, deadline))
                done, _ = await asyncio.wait({task}, timeout=seconds)
                if not done:
                    fatal = True
                    task.cancel()
                    raise TimeoutError(
                        "Browser request exceeded its budget during browser action; worker recycled"
                    )
                result = task.result()
                response = {
                    "version": 1,
                    "ok": True,
                    "result": result,
                    "url": page.url if page else "about:blank",
                }
            except Exception as error:
                if isinstance(error, BrowserLaunchFailure):
                    fatal = True
                response = {"version": 1, "ok": False, "error": str(error)[:4000]}
            finally:
                if fatal:
                    try:
                        terminate_owned_descendants()
                    except Exception as cleanup_error:
                        response = {
                            "version": 1,
                            "ok": False,
                            "error": f"Browser worker cleanup failed: {cleanup_error}"[:4000],
                        }
                if acquired and not fatal:
                    lock.release()
            try:
                encoded = json.dumps(response, ensure_ascii=True, allow_nan=False).encode() + b"\n"
                if len(encoded) > MAX_MESSAGE:
                    encoded = b'{"version":1,"ok":false,"error":"Browser response exceeds size limit"}\n'
                writer.write(encoded)
                await writer.drain()
            finally:
                writer.close()
                if fatal:
                    os._exit(0)
                await writer.wait_closed()

        server = await asyncio.start_unix_server(handle, str(sock_path), limit=MAX_MESSAGE + 1)
        os.chmod(sock_path, 0o600)
        async with server:
            await server.serve_forever()


def remaining(deadline, phase):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError(f"Browser request exceeded its {REQUEST_TIMEOUT:g}s budget during {phase}")
    return seconds


def connect(sock_path, deadline):
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.settimeout(remaining(deadline, "connect"))
        client.connect(str(sock_path))
    except Exception:
        client.close()
        raise
    return client


def request(state, encoded):
    deadline = time.monotonic() + REQUEST_TIMEOUT
    data = base64.b64decode(encoded, validate=True)
    if len(data) > MAX_MESSAGE:
        raise ValueError("Browser request exceeds size limit")
    request_data = json.loads(data)
    if not isinstance(request_data, dict):
        raise TypeError("Browser request must be an object")
    request_data["deadline"] = deadline
    data = json.dumps(request_data, ensure_ascii=True, allow_nan=False).encode()
    if len(data) > MAX_MESSAGE:
        raise ValueError("Browser request exceeds size limit")
    sock_path, lock_path = paths(state)
    with lock_path.open("a") as lock:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(min(0.05, remaining(deadline, "startup lock")))
        try:
            client = connect(sock_path, deadline)
        except (FileNotFoundError, ConnectionRefusedError):
            process = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "--state-dir", str(state), "--serve"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
            try:
                while True:
                    try:
                        client = connect(sock_path, deadline)
                        break
                    except (FileNotFoundError, ConnectionRefusedError):
                        exit_code = process.poll()
                        if exit_code is not None:
                            raise RuntimeError(
                                f"Browser worker exited during startup (exit {exit_code}); "
                                "verify Python Playwright in the sandbox image"
                            ) from None
                        time.sleep(min(0.05, remaining(deadline, "worker startup")))
            except BaseException:
                startup_pid = getattr(process, "pid", None)
                if startup_pid is not None:
                    terminate_owned_descendants(startup_pid)
                raise
    with client:
        client.settimeout(remaining(deadline, "request write"))
        client.sendall(data + b"\n")
        chunks, size = [], 0
        response_deadline = deadline + RESPONSE_GRACE
        while True:
            seconds = response_deadline - time.monotonic()
            if seconds <= 0:
                raise TimeoutError("Browser worker cleanup response exceeded its bounded grace")
            client.settimeout(seconds)
            try:
                chunk = client.recv(min(65_536, MAX_MESSAGE + 1 - size))
            except TimeoutError as error:
                raise TimeoutError("Browser worker cleanup response exceeded its bounded grace") from error
            if not chunk:
                raise ValueError("Browser worker closed without a complete response")
            chunks.append(chunk)
            size += len(chunk)
            if size > MAX_MESSAGE:
                raise ValueError("Browser worker response exceeds size limit")
            if b"\n" in chunk:
                break
        result = b"".join(chunks)
        if not result.endswith(b"\n") or result.count(b"\n") != 1:
            raise ValueError("Invalid browser worker response framing")
        sys.stdout.write(REPLY_PREFIX + result.decode())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--serve", action="store_true")
    mode.add_argument("--request")
    args = parser.parse_args()
    state = Path(args.state_dir).resolve()
    supervise_descendants()
    if args.serve:
        asyncio.run(serve(state))
    else:
        try:
            request(state, args.request)
        except Exception as error:
            print(REPLY_PREFIX + json.dumps({"version": 1, "ok": False, "error": str(error)[:4000]}))


if __name__ == "__main__":
    main()
