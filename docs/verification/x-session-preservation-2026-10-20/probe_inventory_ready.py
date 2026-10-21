"""One bounded read-only X probe from immutable operator JSON; never saves browser state."""

import asyncio
import json
import os
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

STATE = Path("/run/secrets/x_session")
OUTPUT = Path("/out/probe-inventory-ready-report.json")
REPORT = {
    "complete": False,
    "status": "NOT_STARTED",
    "authenticated": False,
    "challenge": False,
    "account_identity_readback": False,
    "list_inventory_readback": False,
    "list_found": False,
    "explicit_empty_inventory": False,
    "list_ui_ready": False,
    "blocked_request_operations": {},
    "member_query_requests": 0,
    "member_pagination_cap": 2,
    "member_query_count": 100,
    "exact_86_members_verified": False,
    "mutating_requests_blocked": 0,
    "login_attempted": False,
    "cookies_saved_or_overwritten": False,
    "list_mutations": 0,
}


def save():
    OUTPUT.write_text(json.dumps(REPORT, indent=2) + "\n")
    OUTPUT.chmod(0o600)


def expected_handles():
    text = Path("/prd/01_Tank_V0_PRD.md").read_text()
    section = text.split("### 4.3 X accounts (86)", 1)[1].split("### 4.4", 1)[0]
    handles = {h.lower() for h in re.findall(r"@([A-Za-z0-9_]+)", section)}
    if len(handles) != 86:
        raise ValueError("PRD_HANDLE_COUNT")
    return handles


async def guarded(page):
    path = urlsplit(page.url).path.lower()
    if any(
        value in path
        for value in (
            "/i/flow/",
            "/i/jf/onboarding",
            "/account/access",
            "/login",
            "/challenge",
            "/account/login_verification",
        )
    ):
        REPORT["challenge"] = not ("login" in path or "onboarding" in path)
        REPORT["status"] = "AUTH_CHECKPOINT" if REPORT["challenge"] else "AUTH_UNAVAILABLE"
        return False
    if await page.locator(
        'iframe[src*="arkoselabs"],iframe[src*="captcha"],[data-testid="ocfEnterTextTextInput"]'
    ).count():
        REPORT["challenge"] = True
        REPORT["status"] = "AUTH_CHECKPOINT"
        return False
    return True


async def run():
    from playwright.async_api import async_playwright

    expected = expected_handles()
    state = json.loads(STATE.read_text())
    if not isinstance(state, dict) or not isinstance(state.get("cookies"), list):
        raise ValueError("INVALID_STORAGE_STATE")
    REPORT["expected_handle_count"] = len(expected)
    REPORT["state_cookie_count"] = len(state["cookies"])
    REPORT["state_origins_count"] = len(state.get("origins", []))
    async with async_playwright() as driver:
        browser = await driver.chromium.launch(headless=True)
        try:
            context = await browser.new_context(storage_state=state)
            try:

                async def readonly(route):
                    request = route.request
                    host = (urlsplit(request.url).hostname or "").lower()
                    allowed_host = host in {"x.com", "twitter.com", "t.co"} or host.endswith(
                        (".x.com", ".twitter.com", ".twimg.com")
                    )
                    if request.method not in {"GET", "HEAD", "OPTIONS"}:
                        REPORT["mutating_requests_blocked"] += 1
                        operation = urlsplit(request.url).path.rstrip("/").rsplit("/", 1)[-1]
                        operation = (
                            operation if re.fullmatch(r"[A-Za-z_]{1,80}", operation) else "non_query_POST"
                        )
                        REPORT["blocked_request_operations"][operation] = (
                            REPORT["blocked_request_operations"].get(operation, 0) + 1
                        )
                        await route.abort()
                    elif request.url.startswith("https://") and allowed_host:
                        path = urlsplit(request.url).path
                        operation = path.rstrip("/").rsplit("/", 1)[-1]
                        if operation == "ListMembers" and request.method == "GET":
                            REPORT["member_query_requests"] += 1
                            if REPORT["member_query_requests"] > 2:
                                await route.abort()
                                return
                            parts = urlsplit(request.url)
                            pairs = parse_qsl(parts.query, keep_blank_values=True)
                            changed = []
                            for key, value in pairs:
                                if key == "variables":
                                    variables = json.loads(value)
                                    if str(variables.get("listId", "")) != str(REPORT.get("list_id", "")):
                                        await route.abort()
                                        return
                                    variables["count"] = 100
                                    REPORT["member_queries_sent"] = REPORT.get("member_queries_sent", 0) + 1
                                    value = json.dumps(variables, separators=(",", ":"))
                                changed.append((key, value))
                            await route.continue_(url=urlunsplit(parts._replace(query=urlencode(changed))))
                        else:
                            await route.continue_()
                    else:
                        await route.abort()

                await context.route("**/*", readonly)
                member_names = set()
                member_tasks = []

                async def member_response(response):
                    if (
                        response.request.method != "GET"
                        or urlsplit(response.url).path.rstrip("/").rsplit("/", 1)[-1] != "ListMembers"
                    ):
                        return
                    if response.status != 200:
                        REPORT["member_response_non200"] = True
                        return
                    data = await response.json()

                    # Only user-entries within this observed ListMembers response.
                    def walk(value):
                        if isinstance(value, dict):
                            if str(value.get("entryId", "")).startswith("user-"):
                                content = value.get("content", {}).get("itemContent", {})
                                user = content.get("user_results", {}).get("result", {})
                                name = user.get("legacy", {}).get("screen_name") or user.get("core", {}).get(
                                    "screen_name"
                                )
                                if isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9_]+", name):
                                    member_names.add(name.lower())
                            for child in value.values():
                                walk(child)
                        elif isinstance(value, list):
                            for child in value:
                                walk(child)

                    walk(data)
                    REPORT["member_response_records"] = len(member_names)

                def schedule_member_response(response):
                    if (
                        response.request.method == "GET"
                        and urlsplit(response.url).path.rstrip("/").rsplit("/", 1)[-1] == "ListMembers"
                    ):
                        task = asyncio.create_task(member_response(response))

                        def consume(task):
                            if task.cancelled():
                                return
                            if task.exception() is not None:
                                REPORT["member_response_error"] = True

                        task.add_done_callback(consume)
                        member_tasks.append(task)

                context.on("response", schedule_member_response)
                page = await context.new_page()
                REPORT["status"] = "NAVIGATING_LIST_INVENTORY"
                save()
                await page.goto("https://x.com/i/lists", wait_until="domcontentloaded", timeout=20000)
                if not await guarded(page):
                    return
                try:
                    await page.locator(
                        '[data-testid="AppTabBar_Profile_Link"],[data-testid="SideNav_AccountSwitcher_Button"]'
                    ).first.wait_for(timeout=12000)
                except Exception:
                    if not await guarded(page):
                        return
                    REPORT["status"] = "AUTH_IDENTITY_UNCONFIRMED"
                    return
                if not await guarded(page):
                    return
                profile = page.locator('[data-testid="AppTabBar_Profile_Link"]')
                profile_href = await profile.first.get_attribute("href") if await profile.count() else None
                match = re.fullmatch(r"/([A-Za-z0-9_]+)/?", profile_href or "")
                if not match:
                    REPORT["status"] = "AUTH_IDENTITY_UNCONFIRMED"
                    return
                REPORT["account_handle"] = match[1]
                REPORT["authenticated"] = True
                REPORT["account_identity_readback"] = True
                REPORT["status"] = "FINDING_OBSERVED_LISTS_NAVIGATION"
                nav = page.locator('[data-testid="AppTabBar_Lists_Link"]')
                if not await nav.count():
                    nav = page.get_by_role("link", name=re.compile(r"^(Lists|Списки)$"))
                if not await nav.count():
                    more = page.locator('[data-testid="AppTabBar_More_Menu"]')
                    if await more.count():
                        await more.first.click(timeout=3000)
                        nav = page.get_by_role("link", name=re.compile(r"^(Lists|Списки)$"))
                if not await nav.count():
                    REPORT["status"] = "LIST_NAVIGATION_UNAVAILABLE"
                    return
                destination = await nav.first.get_attribute("href")
                parsed = urlsplit(destination or "")
                if not (destination and destination.startswith("/") and not destination.startswith("//")):
                    REPORT["status"] = "LIST_NAVIGATION_NOT_RELATIVE"
                    return
                REPORT["observed_list_navigation_path"] = parsed.path
                if urlsplit(page.url).path != parsed.path:
                    await page.goto(
                        "https://x.com" + destination, wait_until="domcontentloaded", timeout=20000
                    )
                if not await guarded(page):
                    return
                REPORT["status"] = "WAITING_EXPLICIT_LIST_UI"
                try:
                    ready = await page.wait_for_function(
                        r"""() => {
                      const rows = [...document.querySelectorAll('a[href]')].filter(n => /\/i\/lists\/\d+/.test(n.getAttribute('href')||''));
                      if (rows.length) return 'list_rows';
                      const text = document.body.innerText;
                      const empty = /You haven.t created or followed any Lists|You haven.t created any Lists|У вас пока нет списков|Вы ещ. не создали списк/i.test(text);
                      return empty ? 'explicit_empty' : false;
                    }""",
                        timeout=12000,
                    )
                    ready_value = await ready.json_value()
                except Exception:
                    REPORT["visible_list_headings"] = [
                        (text or "")[:120]
                        for text in await page.locator("h1,h2,[role=heading]").all_text_contents()
                    ][:12]
                    REPORT["status"] = "LIST_UI_NOT_READY"
                    return
                REPORT["list_ui_ready"] = True
                REPORT["list_inventory_readback"] = True
                REPORT["explicit_empty_inventory"] = ready_value == "explicit_empty"
                if ready_value == "explicit_empty":
                    REPORT["status"] = "EXPLICIT_EMPTY_LIST_INVENTORY"
                    return
                links = await page.locator('a[href*="/i/lists/"]').evaluate_all(
                    '(nodes)=>nodes.map(n=>({href:n.getAttribute("href"),text:n.innerText}))'
                )
                lists = {}
                for row in links:
                    found = re.search(r"/i/lists/(\d+)(?:$|[/?#])", row.get("href") or "")
                    if found:
                        lists[found[1]] = {"id": found[1], "label": (row.get("text") or "").strip()[:240]}
                REPORT["lists"] = list(lists.values())
                REPORT["list_inventory_readback"] = True
                selected = [
                    row for row in lists.values() if re.search(r"(?i)(^|\W)tank-v0($|\W)", row["label"])
                ]
                if len(selected) != 1:
                    REPORT["status"] = (
                        "MATCHING_LIST_NOT_OBSERVED_BOUNDED_VIEW" if not selected else "LIST_AMBIGUOUS"
                    )
                    return
                chosen = selected[0]
                REPORT["list_found"] = True
                REPORT["list_id"] = chosen["id"]
                await page.goto(
                    "https://x.com/i/lists/" + chosen["id"], wait_until="domcontentloaded", timeout=20000
                )
                if not await guarded(page):
                    return
                await page.wait_for_timeout(1000)
                await page.screenshot(path="/out/list-detail-review.png")
                Path("/out/list-detail-review.png").chmod(0o600)
                REPORT["list_identity_private_screenshot"] = (
                    "/etc/tank/private/x-connection-20261020/list-detail-review.png"
                )
                members = page.locator('a[href$="/members"]')
                if not await members.count():
                    REPORT["status"] = "LIST_MEMBERS_LINK_UNAVAILABLE"
                    return
                members_href = await members.first.get_attribute("href")
                if members_href != "/i/lists/" + chosen["id"] + "/members":
                    REPORT["status"] = "LIST_MEMBERS_PATH_MISMATCH"
                    return
                REPORT["members_label"] = (await members.first.inner_text())[:80]
                await members.first.click(timeout=5000)
                if not await guarded(page):
                    return
                await page.locator('[data-testid="UserCell"]').first.wait_for(timeout=10000)
                observed = set()
                stable = 0
                for _ in range(12):
                    before = len(observed)
                    cells = await page.locator('[data-testid="UserCell"] a[href]').evaluate_all(
                        '(nodes)=>nodes.map(n=>n.getAttribute("href"))'
                    )
                    for href in cells:
                        user = re.fullmatch(r"/([A-Za-z0-9_]+)/?", href or "")
                        if user:
                            observed.add(user[1].lower())
                    stable = stable + 1 if len(observed) == before else 0
                    if stable >= 2:
                        break
                    await page.locator("[data-testid=UserCell]").last.scroll_into_view_if_needed(timeout=1000)
                    box = await page.locator("[data-testid=UserCell]").last.bounding_box()
                    if box:
                        await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                    await page.mouse.wheel(0, 1100)
                    await page.wait_for_timeout(500)
                    if not await guarded(page):
                        return
                if member_tasks:
                    await asyncio.wait_for(asyncio.gather(*member_tasks, return_exceptions=True), timeout=5)
                observed.update(member_names)
                REPORT["observed_member_handles"] = sorted(observed)
                REPORT["observed_member_count"] = len(observed)
                REPORT["missing_expected_handles"] = sorted(expected - observed)
                REPORT["extra_handles"] = sorted(observed - expected)
                REPORT["exact_86_members_verified"] = observed == expected
                REPORT["status"] = (
                    "READY_FOR_ROOT_REVIEW" if observed == expected else "LIST_MEMBERSHIP_REVIEW_REQUIRED"
                )
            finally:
                await context.close()
        finally:
            await browser.close()


async def main():
    try:
        await asyncio.wait_for(run(), timeout=90)
    except TimeoutError:
        REPORT["status"] = "BOUNDED_TIMEOUT"
    except Exception as error:
        REPORT["status"] = "PROBE_FAILED"
        REPORT["error_type"] = type(error).__name__
    finally:
        REPORT["complete"] = True
        save()
        print(
            json.dumps(
                {
                    key: REPORT[key]
                    for key in (
                        "complete",
                        "status",
                        "authenticated",
                        "challenge",
                        "account_identity_readback",
                        "list_inventory_readback",
                        "list_found",
                        "exact_86_members_verified",
                        "login_attempted",
                        "cookies_saved_or_overwritten",
                        "list_mutations",
                    )
                }
            ),
            flush=True,
        )


if __name__ == "__main__":
    os.umask(0o077)
    asyncio.run(main())
