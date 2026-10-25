"""Explicit current-read smoke probes; never create source items or queue jobs."""

import asyncio
from urllib.parse import urlsplit

import httpx

import tank.sources.parsers as parsers
from tank.sources.capture_fetch import capture_fetch, is_exchange_url
from tank.time_precision import collection_clock
from tank.util import clock_iso, sha256


async def probe_feeds(service):
    host_locks = {}
    semaphore = asyncio.Semaphore(4)

    async def probe(source, index, feed):
        started = clock_iso()
        sid = source.source_id
        result = {
            "source_id": sid,
            "feed_index": index,
            "url": feed.url,
            "kind": feed.kind,
            **collection_clock(started).fields("started_at"),
            "status": "UNAVAILABLE",
            "method": feed.method,
            "item_count": None,
            "config_version": service.versions.config_version,
            "code_version": service.versions.code_version,
        }
        if sid == "S29":
            result["reason"] = "X runs through a paid Apify actor run; excluded from the HTTP smoke"
            result.update(collection_clock().fields("finished_at"))
            return result
        host = urlsplit(feed.url).netloc
        lock = host_locks.setdefault(host, asyncio.Lock())
        async with semaphore, lock:
            try:
                post = feed.method.upper() == "POST"
                if is_exchange_url(feed.url):
                    await service.acquire_budget(feed.request_plan(source.limits))
                    response = await capture_fetch(
                        service.repo,
                        feed.method,
                        feed.url,
                        body=feed.body if post else None,
                        source_id=sid,
                    )
                else:
                    response = await service.transport.request(
                        feed.request_plan(source.limits),
                        feed.headers,
                        None,
                    )
                result["http_status"] = response.status_code
                result["response_content_type"] = response.headers.get("content-type")
                payload = response.content
                result.update(payload_sha256=sha256(payload), response_bytes=len(payload))
                response.raise_for_status()
                kind = feed.kind
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
                if getattr(items, "errors", []):
                    result.update(item_count=len(items), parser_entry_errors=items.errors)
                    raise ValueError("Publisher entries contain parser errors; valid siblings retained")
                result.update(
                    status="PASS" if items else "PASS_EMPTY",
                    item_count=len(items),
                    validation="Current configured endpoint and source-specific parser; no alternate discovery",
                )
            except httpx.HTTPStatusError as exc:
                result.update(status="HTTP_ERROR", reason=f"HTTP {exc.response.status_code}")
            except httpx.RequestError as exc:
                result.update(status="NETWORK_ERROR", reason=f"{type(exc).__name__}: {exc}")
            except Exception as exc:
                result.update(status="PARSE_ERROR", reason=f"{type(exc).__name__}: {exc}")
            finally:
                result.update(collection_clock().fields("finished_at"))
                await asyncio.sleep(0.5)
        return result

    return await asyncio.gather(
        *(
            probe(source, i, feed)
            for source in service.sources.values()
            for i, feed in enumerate(source.feeds)
        )
    )
