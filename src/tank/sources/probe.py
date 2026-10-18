"""Explicit current-read smoke probes; never create source items or queue jobs."""

from __future__ import annotations

import asyncio
from urllib.parse import urlsplit

import httpx

from tank.util import iso

from . import parsers


async def probe_feeds(service, *, include_browser=False):
    host_locks = {}
    semaphore = asyncio.Semaphore(4)

    async def probe(source, index, feed):
        started = iso()
        sid = source["source_id"]
        result = {
            "source_id": sid,
            "feed_index": index,
            "url": feed["url"],
            "kind": feed["kind"],
            "started_at": started,
            "status": "UNAVAILABLE",
            "method": feed.get("method", "GET"),
            "item_count": None,
            "config_version": service.settings.config_version,
            "code_version": service.settings.code_version,
        }
        if feed.get("bootstrap_script_url"):
            result["bootstrap_script_url"] = feed["bootstrap_script_url"]
        if sid == "S29":
            if not getattr(service.settings, "x_enabled", True):
                result.update(status="DISABLED", reason="X_DISABLED_BY_OPERATOR")
            else:
                result["reason"] = "Authenticated private X list excluded from public HTTP smoke"
            result["url"] = "https://x.com/i/lists/{operator_configured_list_id}"
            result["finished_at"] = iso()
            return result
        if sid == "S19" and not include_browser:
            result["reason"] = "Rendered-browser probe not enabled"
            result["finished_at"] = iso()
            return result
        host = urlsplit(feed["url"]).netloc
        lock = host_locks.setdefault(host, asyncio.Lock())
        async with semaphore, lock:
            try:
                if feed["kind"] == "dtcc_search":
                    payload, state = await service.dtcc_payload(feed, "probe:" + sid, {})
                    result.update(
                        raw_ref=state["raw_ref"],
                        raw_sha256=state["raw_sha256"],
                        response_bytes=len(payload),
                        http_status=200,
                    )
                elif sid == "S19":
                    payload = await service.browser.html(feed["url"])
                else:
                    await service.acquire_budget(dict(feed, source_limits=source.get("limits", {})))
                    response = await service.http.request(
                        feed.get("method", "GET"),
                        feed["url"],
                        json=feed.get("body") if feed.get("method", "GET").upper() == "POST" else None,
                        headers={"User-Agent": service.settings.user_agent},
                        timeout=20,
                    )
                    result["http_status"] = response.status_code
                    result["response_content_type"] = response.headers.get("content-type")
                    # Raw evidence is retained for successful and failed server responses.
                    payload = response.content
                    ref, digest = service.raw(payload)
                    result.update(raw_ref=ref, raw_sha256=digest, response_bytes=len(payload))
                    response.raise_for_status()
                if "raw_ref" not in result:
                    ref, digest = service.raw(payload)
                    result.update(raw_ref=ref, raw_sha256=digest, response_bytes=len(payload))
                kind = feed["kind"]
                if kind in {"rss", "atom"}:
                    items = parsers.feeds(payload, sid, feed)
                elif kind in {"html", "rendered_html", "rendered"}:
                    items = parsers.article_links(payload, feed)
                elif kind == "instruments":
                    items = parsers.instruments(payload, feed)
                    if not items:
                        raise ValueError("Empty instrument list: cannot validate listing coverage")
                else:
                    items = parsers.json_items(payload, sid)
                result.update(
                    status="PASS" if items else "PASS_EMPTY",
                    item_count=len(items),
                    validation="Current configured endpoint and source-specific parser; no alternate discovery",
                )
            except httpx.HTTPStatusError as exc:
                result.update(status="HTTP_ERROR", reason=f"HTTP {exc.response.status_code}")
            except httpx.RequestError as exc:
                result.update(status="NETWORK_ERROR", reason=f"{type(exc).__name__}: {exc}")
            except Exception as exc:  # noqa: BLE001 — each probe must report failure independently
                result.update(
                    status="UNAVAILABLE" if sid == "S19" else "PARSE_ERROR",
                    reason=f"{type(exc).__name__}: {exc}",
                )
            finally:
                result["finished_at"] = iso()
                await asyncio.sleep(0.5)
        return result

    return await asyncio.gather(
        *(
            probe(source, i, feed)
            for source in service.sources.values()
            for i, feed in enumerate(source.get("feeds", []))
        )
    )
