"""Dedicated read-only X sensor: operator session and bounded list capture (02 §3.7)."""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from urllib.parse import urlsplit

from tank.util import iso, sha256, utcnow


class XSensor:
    def __init__(self, settings, *, shared_browser=False):
        self.settings = settings
        self.shared_browser = shared_browser
        self.open_lock = asyncio.Lock()
        self.context = None
        self.driver = None
        self.lock = asyncio.Lock()
        self.pages = []
        self.scan_status = "COMPLETE"

    async def _open_context(self):
        if self.context is not None:
            return self.context
        from playwright.async_api import async_playwright

        profile = Path(self.settings.x_profile_dir)
        profile.mkdir(parents=True, exist_ok=True, mode=0o700)
        profile.chmod(0o700)
        self.driver = await async_playwright().start()
        headless = getattr(self.settings, "x_headless", True)
        self.context = await self.driver.chromium.launch_persistent_context(
            str(profile), headless=headless, args=["--headless=new"] if headless else []
        )
        return self.context

    async def browser_context(self):
        """Open the process without importing or using the operator session input."""
        async with self.open_lock:
            return await self._open_context()

    async def open(self):
        async with self.open_lock:
            await self._open_context()
            try:
                await self._bootstrap_storage_state(Path(self.settings.x_profile_dir))
            except Exception:  # noqa: BLE001 — secret-bearing browser errors must never escape
                # A bad X input must not terminate the shared public-page context.
                if not self.shared_browser:
                    await self.close()
                raise ValueError(
                    "X session bootstrap failed: invalid, expired, or unreadable storage state"
                ) from None

    async def _bootstrap_storage_state(self, profile):
        """Seed a persistent profile once from an operator-supplied Docker secret.

        Never save storage_state(), credentials, or their hashes to ingestion/raw
        data. The nonsecret marker records only that the one-time seed completed.
        Local storage is initialized on intercepted empty origin pages, so seeding
        itself makes no request to X or to any other external service.
        """
        marker = profile / ".tank-x-session-seeded.json"
        secret = Path(getattr(self.settings, "x_storage_state_path", "/run/secrets/x_session"))
        if marker.exists() or not secret.exists():
            return
        state = json.loads(secret.read_text())
        if not isinstance(state, dict) or not isinstance(state.get("cookies"), list):
            raise TypeError("Invalid storage state")

        def x_domain(domain):
            domain = str(domain).lstrip(".").lower()
            return domain in {"x.com", "twitter.com"} or domain.endswith((".x.com", ".twitter.com"))

        now = utcnow().timestamp()
        cookies = []
        for cookie in state["cookies"]:
            if not isinstance(cookie, dict):
                raise TypeError("Invalid cookie record")
            if not x_domain(cookie.get("domain", "")):
                continue
            expiry = float(cookie.get("expires", -1))
            if expiry != -1 and expiry <= now:
                continue
            if not isinstance(cookie.get("value"), str):
                raise TypeError("Invalid cookie value")
            cookies.append(cookie)
        if not any(cookie.get("name") == "auth_token" and cookie["value"] for cookie in cookies):
            raise ValueError("Missing or expired X auth cookie")
        origins = state.get("origins", [])
        if not isinstance(origins, list):
            raise TypeError("Invalid origin storage")
        validated_origins = []
        for origin in origins:
            if not isinstance(origin, dict):
                raise TypeError("Invalid origin record")
            url = origin.get("origin", "")
            parsed = urlsplit(url)
            if parsed.scheme != "https" or not x_domain(parsed.hostname or ""):
                continue
            entries = origin.get("localStorage", [])
            if not isinstance(entries, list) or any(
                not isinstance(entry, dict)
                or not isinstance(entry.get("name"), str)
                or not isinstance(entry.get("value"), str)
                for entry in entries
            ):
                raise TypeError("Invalid local storage")
            validated_origins.append((url, entries))
        # Validate the complete export before applying credentials to the profile.
        # Persistent contexts require explicit cookie and origin-storage imports.
        await self.context.add_cookies(cookies)
        for url, entries in validated_origins:
            page = await self.context.new_page()
            try:

                async def empty_origin(route):
                    await route.fulfill(status=200, content_type="text/html", body="<html></html>")

                await page.route("**/*", empty_origin)
                await page.goto(url, wait_until="domcontentloaded")
                await page.evaluate(
                    "entries => { for (const entry of entries) localStorage.setItem(entry.name, entry.value); }",
                    entries,
                )
            finally:
                await page.close()
        marker.write_text(json.dumps({"version": 1, "seeded_at": iso()}) + "\n")
        marker.chmod(0o600)

    async def close(self):
        if self.context:
            await self.context.close()
        if self.driver:
            await self.driver.stop()
        self.context = None
        self.driver = None

    async def capture_page(self, page, kind):
        payload = (await page.content()).encode()
        self.pages.append({"kind": kind, "url": page.url, "sha256": sha256(payload)})
        return payload

    async def posts(self, list_id, checkpoint=None, *, known_ids=None):
        if not list_id:
            raise ValueError("S29.list_id in sources.yaml is required")
        async with self.lock:
            await self.open()
            page = await self.context.new_page()
            result = {}
            reached = False
            self.pages = []
            self.scan_status = "COMPLETE"
            known_ids = set(known_ids or [])
            scan_started = asyncio.get_running_loop().time()
            try:
                await page.goto(
                    f"https://x.com/i/lists/{list_id}", wait_until="domcontentloaded", timeout=20000
                )
                if re.search(r"/(?:i/flow/login|login)(?:[/?#]|$)", page.url):
                    raise RuntimeError("X session unavailable or expired; operator session renewal required")
                await page.wait_for_selector("article", timeout=20000)
                stalled = 0
                for _ in range(25):
                    await self.capture_page(page, "X_LIST_HTML")
                    old = set(result)
                    articles = page.locator("article")
                    for idx in range(await articles.count()):
                        node = articles.nth(idx)
                        # The status permalink containing the outer time identifies the author;
                        # quote cards have their own links and must not replace that identity.
                        times = node.locator("time[datetime]")
                        if not await times.count():
                            continue
                        time = times.first
                        link = time.locator("xpath=ancestor::a[1]")
                        href = await link.get_attribute("href") or ""
                        match = re.search(r"/([^/]+)/status/(\d+)", href)
                        if not match:
                            continue
                        handle, status_id = match.groups()
                        if checkpoint and (status_id == checkpoint or status_id in known_ids):
                            reached = True
                            break
                        expand = node.locator('[data-testid="tweet-text-show-more-link"]')
                        if await expand.count():
                            await expand.first.click()
                            await page.wait_for_timeout(250)
                            await self.capture_page(page, "X_EXPANDED_LIST_HTML")
                        pieces = await node.locator('[data-testid="tweetText"]').all_text_contents()
                        own_pieces = await node.locator('[data-testid="tweetText"]').evaluate_all(
                            '(nodes)=>nodes.filter(n=>!n.closest("[data-testid=quoteTweet], [role=link]")).map(n=>n.innerText)'
                        )
                        links = await node.locator('a[href*="/status/"]').evaluate_all(
                            '(nodes)=>nodes.map(n=>n.getAttribute("href"))'
                        )
                        related = []
                        for target in links:
                            m = re.search(r"/([^/]+)/status/(\d+)", target or "")
                            if (
                                m
                                and m.group(2) != status_id
                                and m.group(2) not in [r["status_id"] for r in related]
                            ):
                                related.append({"author": m.group(1), "status_id": m.group(2)})
                        social = await node.locator('[data-testid="socialContext"]').all_text_contents()
                        reposter_links = await node.locator(
                            '[data-testid="socialContext"] a[href]'
                        ).evaluate_all('(nodes)=>nodes.map(n=>n.getAttribute("href"))')
                        reposter = next(
                            (
                                m.group(1)
                                for target in reposter_links
                                if (m := re.fullmatch(r"/([A-Za-z0-9_]+)/?", target or ""))
                            ),
                            None,
                        )
                        media = await node.locator('[data-testid="tweetPhoto"] img, video').evaluate_all(
                            '(nodes)=>nodes.map(n=>({type:n.tagName.toLowerCase(),alt:n.getAttribute("alt")||null,src:n.getAttribute("src")||null,poster:n.getAttribute("poster")||null}))'
                        )
                        result[status_id] = {
                            "external_id": status_id,
                            "observed_at": iso(),
                            "author": handle,
                            "title": "",
                            "text": own_pieces[0] if own_pieces else "",
                            "quoted": "\n".join(pieces[1:]) or None,
                            "quoted_status_ids": [r["status_id"] for r in related],
                            "related_authors": related,
                            "repost_context": " ".join(social) or None,
                            "reposted_by": reposter,
                            "monitored_author": reposter or handle,
                            "media": media,
                            "text_missing_reason": "MEDIA_ONLY_OR_EMPTY" if not own_pieces else None,
                            "url": f"https://x.com/{handle}/status/{status_id}",
                            "published_at": await time.get_attribute("datetime"),
                            "published_at_precision": "EXACT",
                            # The post's own DOM is its raw payload (02 §7.2).
                            "_payload": (await node.evaluate("n => n.outerHTML")).encode(),
                        }
                    if reached:
                        break
                    stalled = stalled + 1 if set(result) == old else 0
                    await page.mouse.wheel(0, 1200)
                    await page.wait_for_timeout(1200)
                    if stalled > 5:
                        # A stalled viewport cannot prove that unseen history is
                        # complete. Return new posts, retain the old checkpoint,
                        # and expose a gap instead of failing the whole batch.
                        if checkpoint and not reached:
                            self.scan_status = "GAP_LIMIT"
                        break
                    if asyncio.get_running_loop().time() - scan_started >= 30:
                        self.scan_status = "GAP_LIMIT"
                        break
                else:
                    self.scan_status = "GAP_LIMIT"
                # Fetch quote/repost status pages because list quote cards truncate long text.
                detail = await self.context.new_page()
                try:
                    for item in result.values():
                        full_quotes = []
                        for related in item["related_authors"]:
                            try:
                                await detail.goto(
                                    f"https://x.com/{related['author']}/status/{related['status_id']}",
                                    wait_until="domcontentloaded",
                                    timeout=5000,
                                )
                                await self.capture_page(detail, "X_QUOTED_STATUS_HTML")
                                await detail.wait_for_selector("article", timeout=5000)
                            except Exception:  # noqa: BLE001 — missing/deleted quote must not discard other posts
                                item.setdefault("quoted_unavailable", []).append(
                                    {"status_id": related["status_id"], "reason": "QUOTE_UNAVAILABLE"}
                                )
                                continue
                            candidates = detail.locator("article")
                            found = False
                            for index in range(await candidates.count()):
                                candidate = candidates.nth(index)
                                permalink = candidate.locator("time").first.locator("xpath=ancestor::a[1]")
                                if related["status_id"] not in (await permalink.get_attribute("href") or ""):
                                    continue
                                expansion = candidate.locator('[data-testid="tweet-text-show-more-link"]')
                                if await expansion.count():
                                    await expansion.first.click()
                                    await self.capture_page(detail, "X_EXPANDED_QUOTED_STATUS_HTML")
                                quote_text = await candidate.locator(
                                    '[data-testid="tweetText"]'
                                ).evaluate_all(
                                    '(nodes)=>nodes.filter(n=>!n.closest("[data-testid=quoteTweet], [role=link]")).map(n=>n.innerText)'
                                )
                                if quote_text:
                                    full_quotes.append(quote_text[0])
                                    item.setdefault("quoted_posts", []).append(
                                        {
                                            "status_id": related["status_id"],
                                            "author": related["author"],
                                            "text": quote_text[0],
                                        }
                                    )
                                else:
                                    item.setdefault("quoted_unavailable", []).append(
                                        {"status_id": related["status_id"], "reason": "MEDIA_ONLY_OR_EMPTY"}
                                    )
                                found = True
                                break
                            if not found:
                                item.setdefault("quoted_unavailable", []).append(
                                    {"status_id": related["status_id"], "reason": "QUOTE_NOT_IN_PAGE"}
                                )
                        if item["related_authors"]:
                            item["quoted_excerpt"] = item["quoted"]
                            item["quoted"] = "\n".join(full_quotes) or None
                            item["quoted_text_status"] = (
                                "PARTIAL_OR_UNAVAILABLE" if item.get("quoted_unavailable") else "FULL"
                            )
                finally:
                    await detail.close()
                return list(result.values()), reached
            finally:
                await page.close()

    async def run(self, stop, poll):
        """Own the X feed control loop; polling deadlines remain durable in the service."""
        while not stop.is_set():
            started = asyncio.get_running_loop().time()
            await poll()
            remaining = max(0, 15 - (asyncio.get_running_loop().time() - started))
            try:
                await asyncio.wait_for(stop.wait(), timeout=remaining)
            except TimeoutError:
                pass
