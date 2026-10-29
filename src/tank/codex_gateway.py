"""Internal Responses bridge; OAuth credentials never enter OpenMA.

The provider URLs are intentionally constants, not deployment configuration.
Tests replace the HTTP transport, never the destination or real auth store.
"""

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, quote_plus

import httpx
from aiohttp import web

RESPONSES_URL = "https://chatgpt.com/backend-api/codex/responses"
TOKEN_URL = "https://auth.openai.com/oauth/token"
CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
_MAX_AUTH_BYTES = 1024 * 1024
_RESPONSE_HEADERS = {
    "content-type",
    "retry-after",
    "retry-after-ms",
    "x-ms-retry-after-ms",
    "x-request-id",
    "x-ratelimit-limit-requests",
    "x-ratelimit-limit-tokens",
    "x-ratelimit-remaining-requests",
    "x-ratelimit-remaining-tokens",
    "x-ratelimit-reset-requests",
    "x-ratelimit-reset-tokens",
}
_RATE_HEADER = re.compile(
    r"^x-codex-(?:[a-z0-9-]+-)?(?:primary|secondary)-(?:used-percent|window-minutes|reset-at)$"
)
_PERCENT = re.compile(r"^[0-9]{1,3}(?:\.[0-9]{1,4})?$")
log = logging.getLogger(__name__)


class GatewayError(Exception):
    """Deliberately static messages; never wrap provider exceptions/bodies."""

    def __init__(self, code: str, status: int = 503):
        self.code, self.status = code, status
        super().__init__(code)


def _text(value) -> str | None:
    if (
        isinstance(value, str)
        and value
        and value.isascii()
        and value == value.strip()
        and not any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        return value
    return None


def _claims(token: str) -> dict:
    try:
        parts = token.split(".")
        if len(parts) != 3 or not all(re.fullmatch(r"[A-Za-z0-9_-]+", part) for part in parts):
            raise ValueError
        payload = base64.b64decode(parts[1] + "=" * (-len(parts[1]) % 4), altchars=b"-_", validate=True)
        value = json.loads(payload)
        if not isinstance(value, dict):
            raise TypeError
        return value
    except (ValueError, TypeError, binascii.Error, UnicodeError):
        raise GatewayError("codex_auth_invalid") from None


@dataclass(repr=False)
class Credentials:
    document: dict = field(repr=False)
    access: str = field(repr=False)
    refresh: str | None = field(repr=False)
    account: str = field(repr=False)
    expires_at: float | None

    @property
    def fingerprint(self) -> bytes:
        identity = {
            name: self.document["tokens"].get(name)
            for name in ("access_token", "refresh_token", "id_token", "account_id")
        }
        return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).digest()

    def needs_refresh(self) -> bool:
        if self.expires_at is not None:
            return self.expires_at <= time.time() + 300
        try:
            refreshed = datetime.fromisoformat(self.document["last_refresh"])
            return time.time() - refreshed.timestamp() >= 8 * 86400
        except (KeyError, ValueError, TypeError, AttributeError):
            return True


class _Redactor:
    """Retain only an incomplete secret prefix, so SSE events stream immediately."""

    def __init__(self, secrets: set[bytes]):
        self.secrets, self.pending = secrets, b""

    def feed(self, data: bytes, *, final: bool = False) -> bytes:
        value = self.pending + data
        for secret in sorted(self.secrets, key=len, reverse=True):
            value = value.replace(secret, b"[REDACTED]")
        hold = 0
        if not final:
            for secret in self.secrets:
                for length in range(1, min(len(value), len(secret) - 1) + 1):
                    if value.endswith(secret[:length]):
                        hold = max(hold, length)
        self.pending = value[-hold:] if hold else b""
        return value[:-hold] if hold else value


class CodexGateway:
    def __init__(self, settings, *, upstream_transport: httpx.AsyncBaseTransport | None = None):
        self.settings = settings
        self._transport = upstream_transport
        self._client: httpx.AsyncClient | None = None
        self._runner: web.AppRunner | None = None
        self._key: bytes | None = None
        self._refresh_lock = asyncio.Lock()
        self._auth_tasks: set[asyncio.Task] = set()
        self._refresh_failure: tuple[bytes, float, GatewayError] | None = None
        self._secrets: set[bytes] = set()
        self.refreshes = 0
        self.ready = asyncio.Event()
        self.bound_port: int | None = None

    def _remember(self, value) -> None:
        if isinstance(value, str) and value:
            for variant in (value, json.dumps(value)[1:-1], quote(value, safe=""), quote_plus(value)):
                self._secrets.add(variant.encode())

    @staticmethod
    async def _close_response(response: httpx.Response) -> None:
        try:
            await response.aclose()
        except Exception:
            log.warning("Codex upstream response cleanup failed")

    def _load(self) -> Credentials:
        path = Path(self.settings.codex_auth_path)
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, "rb") as handle:
                raw = handle.read(_MAX_AUTH_BYTES + 1)
            if len(raw) > _MAX_AUTH_BYTES:
                raise ValueError
            document = json.loads(raw)
            if not isinstance(document, dict) or document.get("auth_mode") not in (None, "chatgpt"):
                raise ValueError
            tokens = document["tokens"]
            if not isinstance(tokens, dict):
                raise TypeError
            for name in ("access_token", "refresh_token", "id_token"):
                self._remember(tokens.get(name))
            self._remember(document.get("OPENAI_API_KEY"))
            access = _text(tokens.get("access_token"))
            if not access:
                raise ValueError
            claims = _claims(access)
            account = _text(tokens.get("account_id"))
            if not account:
                identity = _claims(_text(tokens.get("id_token")) or "")
                account = _text(identity.get("https://api.openai.com/auth", {}).get("chatgpt_account_id"))
            if not account:
                raise ValueError
            expiry = claims.get("exp")
            if expiry is not None and (type(expiry) not in (int, float) or not 0 < expiry < 1e12):
                raise ValueError
            refresh = tokens.get("refresh_token")
            if refresh not in (None, "") and not _text(refresh):
                raise ValueError
            return Credentials(document, access, refresh or None, account, expiry)
        except GatewayError:
            raise
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            raise GatewayError("codex_auth_invalid") from None

    async def preflight(self) -> None:
        """Validate the staged generation locally, without binding or networking."""
        key = _text(self.settings.codex_gateway_key)
        if not key:
            raise GatewayError("codex_gateway_key_missing")
        self._key = key.encode()
        self._remember(key)
        credentials = self._load()
        if credentials.needs_refresh() and not credentials.refresh:
            raise GatewayError("codex_login_required")

    async def start(self) -> None:
        if self._runner is not None:
            return
        await self.preflight()
        self._client = httpx.AsyncClient(
            transport=self._transport,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(connect=15, read=120, write=30, pool=30),
        )
        app = web.Application(client_max_size=32 * 1024 * 1024, handler_args={"handler_cancellation": True})
        app.router.add_post("/v1/responses", self._responses)
        self._runner = web.AppRunner(app, access_log=None, handler_cancellation=True, shutdown_timeout=5)
        try:
            await self._runner.setup()
            site = web.TCPSite(
                self._runner, self.settings.codex_gateway_host, self.settings.codex_gateway_port
            )
            await site.start()
            self.bound_port = self._runner.addresses[0][1]
            self.ready.set()
        except BaseException:
            await self.close()
            raise

    async def run(self, stop: asyncio.Event) -> None:
        await self.start()
        try:
            await stop.wait()
        finally:
            await self.close()

    async def close(self) -> None:
        self.ready.clear()
        runner, self._runner = self._runner, None
        try:
            if runner:
                await runner.cleanup()
        finally:
            if self._auth_tasks:
                await asyncio.gather(*tuple(self._auth_tasks), return_exceptions=True)
            self.bound_port = None
            client, self._client = self._client, None
            if client:
                try:
                    await client.aclose()
                except Exception:
                    raise GatewayError("codex_gateway_close_failed") from None

    def _persist(self, credentials: Credentials, update: dict) -> Credentials:
        current = self._load()
        if current.fingerprint != credentials.fingerprint:
            return current
        document = current.document
        document["tokens"].update(update)
        document["last_refresh"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        path = Path(self.settings.codex_auth_path)
        temporary = None
        try:
            fd, name = tempfile.mkstemp(prefix=".auth-", dir=path.parent)
            temporary = Path(name)
            with os.fdopen(fd, "w") as handle:
                os.fchmod(handle.fileno(), 0o600)
                json.dump(document, handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            return self._load()
        except OSError:
            raise GatewayError("codex_auth_write_failed") from None
        finally:
            if temporary:
                temporary.unlink(missing_ok=True)

    async def _credentials(self, rejected: Credentials | None = None) -> Credentials:
        task = asyncio.create_task(self._bounded_credentials(rejected))
        self._auth_tasks.add(task)
        task.add_done_callback(self._auth_done)
        return await asyncio.shield(task)

    async def _bounded_credentials(self, rejected: Credentials | None) -> Credentials:
        try:
            async with asyncio.timeout(45):
                return await self._credentials_locked(rejected)
        except TimeoutError:
            raise GatewayError("codex_refresh_timeout") from None

    def _auth_done(self, task: asyncio.Task) -> None:
        self._auth_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    async def _credentials_locked(self, rejected: Credentials | None = None) -> Credentials:
        async with self._refresh_lock:
            credentials = self._load()
            forced = rejected is not None and credentials.fingerprint == rejected.fingerprint
            if not forced and not credentials.needs_refresh():
                return credentials
            if not credentials.refresh:
                raise GatewayError("codex_login_required")
            if self._refresh_failure:
                fingerprint, failed_at, error = self._refresh_failure
                if fingerprint == credentials.fingerprint and time.monotonic() - failed_at < 1:
                    raise error
            try:
                assert self._client is not None

                request = httpx.Request(
                    "POST",
                    TOKEN_URL,
                    json={
                        "grant_type": "refresh_token",
                        "client_id": CLIENT_ID,
                        "refresh_token": credentials.refresh,
                        "refresh_reason": "unauthorized_recovery" if forced else "proactive_expiry",
                    },
                    extensions={"timeout": {"connect": 15, "read": 30, "write": 30, "pool": 30}},
                )
                response = await self._client.send(request, stream=True, follow_redirects=False)
                try:
                    if response.status_code != 200:
                        code = (
                            "codex_login_required"
                            if response.status_code in (400, 401, 403)
                            else "codex_refresh_failed"
                        )
                        raise GatewayError(code)
                    raw = bytearray()
                    async for chunk in response.aiter_bytes():
                        raw.extend(chunk)
                        if len(raw) > _MAX_AUTH_BYTES:
                            raise GatewayError("codex_refresh_invalid")
                    update = json.loads(raw)
                finally:
                    await self._close_response(response)
                if not isinstance(update, dict):
                    raise GatewayError("codex_refresh_invalid")
                for name in ("access_token", "refresh_token", "id_token"):
                    self._remember(update.get(name))
                access = _text(update.get("access_token"))
                if not access:
                    raise GatewayError("codex_refresh_invalid")
                claims = _claims(access)
                expiry = claims.get("exp")
                if expiry is not None and (
                    type(expiry) not in (int, float) or not time.time() < expiry < 1e12
                ):
                    raise GatewayError("codex_refresh_invalid")
                selected = {"access_token": access}
                for name in ("refresh_token", "id_token"):
                    if name in update:
                        if not _text(update[name]):
                            raise GatewayError("codex_refresh_invalid")
                        if name == "id_token":
                            _claims(update[name])
                        selected[name] = update[name]
                result = self._persist(credentials, selected)
                self._refresh_failure = None
                self.refreshes += 1
                log.info(
                    "Codex OAuth refresh succeeded: reason=%s refreshes=%d",
                    "unauthorized_recovery" if forced else "proactive_expiry",
                    self.refreshes,
                )
                return result
            except (httpx.HTTPError, ValueError, GatewayError) as exc:
                error = exc if isinstance(exc, GatewayError) else GatewayError("codex_refresh_failed")
                self._refresh_failure = (credentials.fingerprint, time.monotonic(), error)
                log.warning(
                    "Codex OAuth refresh failed: reason=%s code=%s",
                    "unauthorized_recovery" if forced else "proactive_expiry",
                    error.code,
                )
                raise error from None

    @staticmethod
    def _body(body: dict) -> dict:
        if body.get("model") != "gpt-6.1-sol":
            raise GatewayError("unsupported_model", 400)
        if type(body.get("stream", False)) is not bool:
            raise GatewayError("invalid_stream", 400)
        items = body.get("input")
        if isinstance(items, str):
            items = [{"role": "user", "content": items}]
        if not isinstance(items, list):
            raise GatewayError("invalid_input", 400)
        instructions = body.get("instructions", "")
        if not isinstance(instructions, str):
            raise GatewayError("invalid_instructions", 400)
        messages = []
        for item in items:
            if not isinstance(item, dict):
                raise GatewayError("invalid_input", 400)
            if item.get("role") in ("system", "developer"):
                content = item.get("content")
                if isinstance(content, list) and all(
                    isinstance(part, dict)
                    and part.get("type") in ("input_text", "text")
                    and isinstance(part.get("text"), str)
                    for part in content
                ):
                    content = "\n".join(part["text"] for part in content)
                if not isinstance(content, str):
                    raise GatewayError("invalid_instructions", 400)
                instructions = "\n\n".join(part for part in (instructions, content) if part)
            else:
                messages.append(item)
        result = dict(body, input=messages, instructions=instructions, store=False, stream=True)

        for name in ("max_output_tokens", "temperature", "prompt_cache_retention", "prompt_cache_options"):
            result.pop(name, None)
        return result

    def _headers(self, upstream: httpx.Response) -> dict[str, str]:
        result = {"Cache-Control": "no-store"}
        hop_headers = {token.strip().lower() for token in upstream.headers.get("connection", "").split(",")}
        for name, value in upstream.headers.items():
            if name not in hop_headers and (name in _RESPONSE_HEADERS or _RATE_HEADER.fullmatch(name)):
                result[name] = _Redactor(self._secrets).feed(value.encode(), final=True).decode()
        return result

    @staticmethod
    def _log_upstream(upstream: httpx.Response, started: float, attempts: int) -> None:
        """Status, header latency and Codex usage windows; never bodies, tokens or identifiers."""
        usage = []
        for name, value in upstream.headers.items():
            if name.endswith("-used-percent") and _RATE_HEADER.fullmatch(name) and _PERCENT.fullmatch(value):
                usage.append(name.removeprefix("x-codex-").removesuffix("-used-percent") + "=" + value)
        log.log(
            logging.WARNING if upstream.status_code >= 400 else logging.INFO,
            "Codex upstream response: status=%d latency_ms=%d attempts=%d used_percent=%s",
            upstream.status_code,
            round((time.monotonic() - started) * 1000),
            attempts,
            ",".join(sorted(usage)) or "-",
        )

    @staticmethod
    def _error(error: GatewayError) -> web.Response:
        return web.json_response(
            {"error": {"type": "codex_gateway_error", "code": error.code, "message": error.code}},
            status=error.status,
        )

    async def _responses(self, request: web.Request) -> web.StreamResponse:
        authorizations = [value for name, value in request.raw_headers if name.lower() == b"authorization"]
        supplied = authorizations[0] if len(authorizations) == 1 else b""
        expected = b"Bearer " + (self._key or b"")
        if not self._key or not hmac.compare_digest(supplied, expected):
            return self._error(GatewayError("unauthorized", 401))
        upstream = None
        downstream = None
        started = time.monotonic()
        try:
            try:
                original = await request.json()
                if not isinstance(original, dict):
                    raise TypeError
            except (ValueError, TypeError, UnicodeError):
                raise GatewayError("invalid_json", 400) from None
            body = self._body(original)
            credentials = await self._credentials()
            assert self._client is not None
            for attempt in range(2):
                headers = {
                    "Authorization": "Bearer " + credentials.access,
                    "ChatGPT-Account-ID": credentials.account,
                    "Content-Type": "application/json",
                    "Accept": "text/event-stream",
                    "User-Agent": "tank-codex-gateway/0.1",
                    "originator": "codex_cli_rs",
                }
                upstream = await self._client.send(
                    httpx.Request("POST", RESPONSES_URL, headers=headers, json=body),
                    stream=True,
                    follow_redirects=False,
                )
                if upstream.status_code == 401 and attempt == 0:
                    await self._close_response(upstream)
                    upstream = None
                    credentials = await self._credentials(rejected=credentials)
                    continue
                break
            assert upstream is not None
            self._log_upstream(upstream, started, attempt + 1)
            if 300 <= upstream.status_code < 400:
                raise GatewayError("codex_redirect_refused", 502)
            headers = self._headers(upstream)
            if upstream.is_success and not original.get("stream", False):
                return await self._collect(upstream, headers)
            downstream = web.StreamResponse(status=upstream.status_code, headers=headers)
            await downstream.prepare(request)
            redactor = _Redactor(self._secrets)
            async for chunk in upstream.aiter_bytes():
                safe = redactor.feed(chunk)
                if safe:
                    await downstream.write(safe)
            await downstream.write(redactor.feed(b"", final=True))
            await downstream.write_eof()
            return downstream
        except GatewayError as error:
            log.warning("Codex gateway request failed: code=%s status=%d", error.code, error.status)
            return self._error(error)
        except httpx.HTTPError as exc:
            if downstream is None:
                log.warning("Codex upstream unavailable: %s", type(exc).__name__)
                return self._error(GatewayError("codex_upstream_unavailable", 502))
            log.warning("Codex upstream stream interrupted: %s", type(exc).__name__)

            await downstream.write(
                b'event: error\ndata: {"type":"error","code":"codex_stream_interrupted","message":"Codex stream interrupted"}\n\n'
            )
            await downstream.write_eof()
            return downstream
        except (ConnectionError, asyncio.CancelledError):
            raise
        except Exception as exc:
            log.warning("Codex gateway failed: %s", type(exc).__name__)
            if downstream is not None and downstream.prepared:
                await downstream.write(
                    b'event: error\ndata: {"type":"error","code":"codex_stream_interrupted","message":"Codex stream interrupted"}\n\n'
                )
                await downstream.write_eof()
                return downstream
            return self._error(GatewayError("codex_gateway_failed", 502))
        finally:
            if upstream is not None:
                await self._close_response(upstream)

    async def _collect(self, upstream: httpx.Response, headers: dict) -> web.Response:
        """Codex streams; nonstream Responses callers receive the terminal object."""
        pending = ""
        encoding = None
        redactor = _Redactor(self._secrets)

        import codecs

        decoder = codecs.getincrementaldecoder("utf-8")()
        async for chunk in upstream.aiter_bytes():
            pending += decoder.decode(redactor.feed(chunk))
            if encoding is None and pending.lstrip():
                encoding = "json" if pending.lstrip().startswith("{") else "sse"
            if encoding == "json":
                continue
            pending = pending.replace("\r\n", "\n")
            while "\n\n" in pending:
                frame, pending = pending.split("\n\n", 1)
                data = "\n".join(
                    line[5:].lstrip(" ") for line in frame.splitlines() if line.startswith("data:")
                )
                if not data or data == "[DONE]":
                    continue
                try:
                    event = json.loads(data)
                except ValueError:
                    raise GatewayError("codex_stream_invalid", 502) from None
                if event.get("type") in ("response.completed", "response.incomplete", "response.failed"):
                    response = event.get("response")
                    if not isinstance(response, dict):
                        raise GatewayError("codex_stream_invalid", 502)
                    safe_headers = {k: v for k, v in headers.items() if k.lower() != "content-type"}
                    return web.json_response(response, headers=safe_headers)
                if event.get("type") == "error":
                    raise GatewayError("codex_stream_failed", 502)
        if encoding == "json":
            pending += decoder.decode(redactor.feed(b"", final=True), final=True)
            try:
                response = json.loads(pending)
            except ValueError:
                raise GatewayError("codex_response_invalid", 502) from None
            if not isinstance(response, dict):
                raise GatewayError("codex_response_invalid", 502)
            safe_headers = {k: v for k, v in headers.items() if k.lower() != "content-type"}
            return web.json_response(response, headers=safe_headers)
        raise GatewayError("codex_stream_incomplete", 502)
