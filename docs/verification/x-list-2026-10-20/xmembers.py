"""Add the 86 handles to the private list tank-v0 (user-approved), then verify membership.

Only the list's own "Add" buttons are clicked: no follow, post or other change.
Already-present members are left alone. The session file is only read.
Usage: python xmembers.py HANDLES_FILE OUT_JSON [--verify-only]
"""

import asyncio
import json
import re
import sys

from playwright.async_api import async_playwright

SECRET = "/etc/tank/secrets/x_session"
HANDLES = [h.strip() for h in open(sys.argv[1]) if h.strip()]
OUT = sys.argv[2]
VERIFY_ONLY = "--verify-only" in sys.argv

CELL_JS = """(handle) => {
  const avatars = [...document.querySelectorAll('[role=dialog] [data-testid^="UserAvatar-Container-"]')];
  const avatar = avatars.find(n => n.getAttribute('data-testid').slice(21).toLowerCase() === handle.toLowerCase());
  if (!avatar) return 'NOT_FOUND';
  for (let node = avatar; node && node.getAttribute('role') !== 'dialog'; node = node.parentElement) {
    const buttons = [...node.querySelectorAll('button')].filter(b => ['Add', 'Remove'].includes(b.innerText.trim()));
    if (buttons.length === 1) {
      buttons[0].setAttribute('data-tank-target', '1');
      return buttons[0].innerText.trim();
    }
    if (buttons.length > 1) return 'AMBIGUOUS';
  }
  return 'NO_BUTTON';
}"""
MEMBERS_JS = """() => [...document.querySelectorAll('[role=dialog] [data-testid=UserCell] [data-testid^="UserAvatar-Container-"]')]
  .map(n => n.getAttribute('data-testid').slice(21))"""
COUNT_JS = """() => { const a = document.querySelector('a[href$="/members"]'); return a ? a.innerText.trim() : null; }"""


async def list_id(page):
    await page.goto("https://x.com/MargaretAlxgav/lists", wait_until="domcontentloaded", timeout=30000)
    await page.get_by_text("tank-v0", exact=True).last.wait_for(timeout=45000)
    await page.get_by_text("tank-v0", exact=True).last.click()
    await page.wait_for_url(re.compile(r"/i/lists/\d+"), timeout=20000)
    return re.search(r"/i/lists/(\d+)", page.url).group(1)


async def members(page, lid, report):
    await page.goto(f"https://x.com/i/lists/{lid}", wait_until="domcontentloaded", timeout=30000)
    await page.locator('a[href$="/members"]').first.wait_for(timeout=45000)
    report["members_counter"] = await page.evaluate(COUNT_JS)
    await page.goto(f"https://x.com/i/lists/{lid}/members", wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(6000)
    if "--debug" in sys.argv:
        await page.screenshot(path="debug-members.png")
    seen, stall = [], 0
    while stall < 5:
        new = [m for m in await page.evaluate(MEMBERS_JS) if m not in seen]
        seen += new
        stall = 0 if new else stall + 1
        await page.mouse.move(640, 500)
        await page.mouse.wheel(0, 1500)
        await page.wait_for_timeout(1500)
    return seen


async def main():
    report = {"actions": {}}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--headless=new"])
        context = await browser.new_context(
            storage_state=SECRET, locale="en-US", viewport={"width": 1280, "height": 1000}
        )
        page = await context.new_page()
        lid = await list_id(page)
        report["list_id"] = lid
        refuse = page.get_by_role("button", name="Refuse non-essential cookies")
        if await refuse.count():
            await refuse.first.click()
        if not VERIFY_ONLY:
            await page.goto(f"https://x.com/i/lists/{lid}", wait_until="domcontentloaded", timeout=30000)
            await page.get_by_text("Edit List", exact=True).first.click(timeout=45000)
            dialog = page.locator("[role=dialog]")
            report["private_checked"] = await dialog.locator('input[type="checkbox"]').first.is_checked(
                timeout=20000
            )
            await dialog.get_by_text("Manage members", exact=True).first.click(timeout=20000)
            search = page.locator('[role=dialog] input[placeholder="Search people"]').first
            if not await search.count():
                await page.get_by_role("tab", name="Suggested").click(timeout=20000)
            await search.wait_for(timeout=30000)
            for handle in HANDLES:
                state = "NOT_FOUND"
                for _ in range(2):
                    await search.fill("")
                    await search.fill(handle)
                    await page.wait_for_timeout(3000)
                    state = await page.evaluate(CELL_JS, handle)
                    if state != "NOT_FOUND":
                        break
                    if "--debug" in sys.argv:
                        await page.screenshot(path="debug-search.png")
                        print(
                            await page.evaluate(
                                "() => [...document.querySelectorAll('[role=dialog] [data-testid]')].map(n => n.getAttribute('data-testid')).slice(0, 40)"
                            )
                        )
                if state == "Add":
                    await page.locator("[data-tank-target='1']").first.click()
                    await page.wait_for_timeout(1500)
                    state = "ADDED"
                elif state == "Remove":
                    state = "ALREADY_MEMBER"
                await page.evaluate(
                    "() => document.querySelectorAll('[data-tank-target]').forEach(n => n.removeAttribute('data-tank-target'))"
                )
                report["actions"][handle] = state
                print(handle, state, flush=True)
        found = await members(page, lid, report)
        await browser.close()
    wanted = {h.lower() for h in HANDLES}
    have = {m.lower() for m in found}
    report.update(
        members=sorted(found),
        member_count=len(have),
        missing=sorted(wanted - have),
        unexpected=sorted(have - wanted),
        exact=have == wanted,
    )
    json.dump(report, open(OUT, "w"), indent=1)
    print(
        json.dumps(
            {
                k: report[k]
                for k in ("list_id", "members_counter", "member_count", "missing", "unexpected", "exact")
            }
        )
    )


asyncio.run(main())
