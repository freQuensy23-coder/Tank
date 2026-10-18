"""Explicit live Codex probe; never prints credentials or mutates source auth.

Run only with operator authorization. Uses the standard Codex auth file and an
isolated mode-0600 copy. OAuth refresh is blocked, including after provider 401.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import secrets
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx

from tank.codex_gateway import RESPONSES_URL, TOKEN_URL, CodexGateway, GatewayError

ROOT = Path(__file__).resolve().parents[2]
ARTIFACT = ROOT / "data/live-integration-2026-10-18/codex-auth"
PI_PACKAGE = Path("/tmp/tank-gateway-pi-audit/node_modules/@earendil-works/pi-ai")


class RestrictedLiveTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.inner = httpx.AsyncHTTPTransport(retries=0)
        self.calls = []
        self.refresh_blocked = 0
        self.failures = []

    async def handle_async_request(self, request):
        if str(request.url) == TOKEN_URL:
            self.refresh_blocked += 1
            raise GatewayError("live_probe_refresh_blocked")
        if str(request.url) != RESPONSES_URL:
            raise GatewayError("live_probe_destination_refused")
        try:
            response = await self.inner.handle_async_request(request)
        except Exception as error:
            text = str(error).lower()
            category = next((label for marker, label in (
                ("certificate_verify_failed", "tls_certificate_verification_failed"),
                ("nodename nor servname", "dns_resolution_failed"),
                ("name or service not known", "dns_resolution_failed"),
                ("connection refused", "connection_refused"),
                ("timed out", "connection_timeout"),
                ("permission denied", "connection_permission_denied"),
            ) if marker in text), "transport_failure")
            self.failures.append({"class": type(error).__name__, "category": category})
            raise
        self.calls.append({"endpoint": RESPONSES_URL, "status": response.status_code,
                           "sse_content_type_advertised": "text/event-stream" in response.headers.get("content-type", ""),
                           "content_type": response.headers.get("content-type", "")[:256]})
        return response

    async def aclose(self):
        await self.inner.aclose()


async def main(*, nonstream=False):
    logging.basicConfig(level=logging.CRITICAL)
    source = (Path(os.environ["CODEX_HOME"]).expanduser() if os.environ.get("CODEX_HOME")
              else Path.home() / ".codex") / "auth.json"
    report = {"checked_at": datetime.now(UTC).isoformat(), "source_auth_path": str(source),
              "model": "gpt-6.1-sol", "pi_version": "1.1.0", "refresh_exercised": False,
              "source_auth_unchanged": False, "status": "FAIL"}
    original = source.read_bytes()
    original_digest = hashlib.sha256(original).digest()
    ARTIFACT.mkdir(parents=True, exist_ok=True, mode=0o700)
    ARTIFACT.chmod(0o700)
    copy = ARTIFACT / "auth.json"
    descriptor = os.open(copy, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        os.fchmod(handle.fileno(), 0o600)
        handle.write(original)
        handle.flush()
        os.fsync(handle.fileno())
    internal_key = secrets.token_urlsafe(32)
    settings = SimpleNamespace(codex_gateway_key=internal_key, codex_auth_path=copy,
                               codex_gateway_host="127.0.0.1", codex_gateway_port=0)
    transport = RestrictedLiveTransport()
    gateway = CodexGateway(settings, upstream_transport=transport)
    try:
        credentials = gateway._load()
        report["access_token_present"] = bool(credentials.access)
        report["account_id_present"] = bool(credentials.account)
        report["proactive_refresh_needed"] = credentials.needs_refresh()
        if credentials.needs_refresh():
            report["failure_code"] = "proactive_refresh_needed_no_live_rotation_performed"
            return report
        if json.loads((PI_PACKAGE / "package.json").read_text())["version"] != "1.1.0":
            report["failure_code"] = "pinned_pi_package_unavailable"
            return report
        await gateway.start()
        if nonstream:
            async with httpx.AsyncClient(trust_env=False, timeout=60) as client:
                response = await client.post(f"http://127.0.0.1:{gateway.bound_port}/v1/responses",
                    headers={"Authorization": "Bearer " + internal_key},
                    json={"model": "gpt-6.1-sol", "stream": False, "input": "Reply exactly TANK_LIVE_NONSTREAM_OK.",
                          "reasoning": {"effort": "low"}})
            value = response.json()
            text = "".join(part.get("text", "") for item in value.get("output", [])
                           for part in item.get("content", []) if part.get("type") == "output_text").strip()
            report["nonstream"] = {"http_status": response.status_code,
                "local_content_type": response.headers.get("content-type", ""),
                "expected_text_verified": text == "TANK_LIVE_NONSTREAM_OK",
                "terminal_status": value.get("status"),
                "usage": {key: value.get("usage", {}).get(key, 0) for key in ("input_tokens", "output_tokens")}}
            report["status"] = "PASS" if response.status_code == 200 and text == "TANK_LIVE_NONSTREAM_OK" else "FAIL"
            return report
        process = await asyncio.create_subprocess_exec(
            "node", str(Path(__file__).with_suffix(".mjs")), str(PI_PACKAGE),
            f"http://127.0.0.1:{gateway.bound_port}/v1", stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(internal_key.encode()), 90)
        except TimeoutError:
            process.kill()
            await process.communicate()
            report["failure_code"] = "live_pi_probe_timeout"
            return report
        # Node emits only a controlled JSON summary; its stderr is never persisted.
        del stderr
        try:
            outcome = json.loads(stdout)
        except ValueError:
            report["failure_code"] = "live_pi_probe_invalid_output"
            return report
        report["probe"] = outcome
        report["status"] = "PASS" if outcome.get("status") == "PASS" and process.returncode == 0 else "FAIL"
        return report
    except GatewayError as error:
        report["failure_code"] = error.code
        return report
    except Exception:  # noqa: BLE001 -- credential-safe probe boundary; never print exception payloads.
        report["failure_code"] = "live_probe_local_failure"
        return report
    finally:
        await gateway.close()
        report["provider_requests"] = transport.calls
        report["transport_failures"] = transport.failures
        report["refresh_attempts_blocked"] = transport.refresh_blocked
        report["source_auth_unchanged"] = hashlib.sha256(source.read_bytes()).digest() == original_digest
        # The disposable copy is unnecessary after the probe; do not retain tokens.
        copy.unlink(missing_ok=True)
        report["temporary_auth_removed"] = not copy.exists()
        output = ARTIFACT / ("nonstream-probe-report.json" if nonstream else "probe-report.json")
        output.write_text(json.dumps(report, indent=2) + "\n")
        output.chmod(0o600)
        print(json.dumps(report))


if __name__ == "__main__":
    result = asyncio.run(main(nonstream="--nonstream" in sys.argv))
    raise SystemExit(0 if result["status"] == "PASS" else 1)
