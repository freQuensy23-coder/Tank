"""Read-only: find the operator X account's lists and their members. Changes nothing on X."""

import asyncio
import json
import re
import sys

from playwright.async_api import async_playwright

SECRET = "/etc/tank/secrets/x_session"
OUT = sys.argv[1]

LISTS_JS = """() => [...document.querySelectorAll('a[href^="/i/lists/"]')]
  .map(a => ({href: a.getAttribute('href'), text: a.innerText.replace(/\\s+/g, ' ').trim()}))
  .filter(x => /^\\/i\\/lists\\/\\d+$/.test(x.href))"""
MEMBERS_JS = """() => [...document.querySelectorAll('[data-testid="UserCell"] a[href^="/"]')]
  .map(a => a.getAttribute('href')).filter(h => /^\\/[A-Za-z0-9_]+$/.test(h)).map(h => h.slice(1))"""


async def scroll_collect(page, script, rounds=40):
    seen, stall = [], 0
    for _ in range(rounds):
        new = [f for f in await page.evaluate(script) if f not in seen]
        seen += new
        stall = 0 if new else stall + 1
        if stall >= 4:
            break
        await page.mouse.wheel(0, 1500)
        await page.wait_for_timeout(1200)
    return seen


async def main():
    report = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--headless=new"])
        context = await browser.new_context(storage_state=SECRET, locale="en-US")
        page = await context.new_page()
        await page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(4000)
        report["home_url"] = page.url
        if re.search(r"/(i/flow/login|login)", page.url):
            report["error"] = "SESSION_NOT_SIGNED_IN"
        else:
            link = page.locator('[data-testid="AppTabBar_Profile_Link"]')
            await link.wait_for(timeout=20000)
            handle = (await link.get_attribute("href")).strip("/")
            report["account"] = handle
            await page.goto(f"https://x.com/{handle}/lists", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)
            report["lists_page_url"] = page.url
            report["lists"] = await scroll_collect(page, LISTS_JS, rounds=10)
            report["lists_text"] = (await page.locator("body").inner_text())[:5000]
            await page.goto("https://x.com/i/lists", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)
            report["i_lists_url"] = page.url
            report["i_lists_text"] = (await page.locator("body").inner_text())[:5000]
            report["lists"] += [x for x in await page.evaluate(LISTS_JS) if x not in report["lists"]]
            report["members"] = {}
            for entry in report["lists"][:8]:
                list_id = entry["href"].rsplit("/", 1)[1]
                await page.goto(
                    f"https://x.com/i/lists/{list_id}/members", wait_until="domcontentloaded", timeout=30000
                )
                await page.wait_for_timeout(4000)
                report["members"][list_id] = await scroll_collect(page, MEMBERS_JS)
        await browser.close()
    with open(OUT, "w") as handle_out:
        json.dump(report, handle_out, indent=1)
    print(
        json.dumps(
            {
                "account": report.get("account"),
                "error": report.get("error"),
                "lists": report.get("lists"),
                "member_counts": {k: len(v) for k, v in report.get("members", {}).items()},
            }
        )
    )


asyncio.run(main())
