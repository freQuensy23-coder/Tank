"""Create the private X list "tank-v0" once (user-approved), then dump the add-members page UI.

Idempotent: if the account already owns a list named tank-v0, it is reused.
Changes nothing else on the account. The session file is only read.
"""

import asyncio
import json
import re
import sys

from playwright.async_api import async_playwright

SECRET = "/etc/tank/secrets/x_session"
OUT = sys.argv[1]
NAME = "tank-v0"

DUMP_JS = """() => [...document.querySelectorAll('input, [data-testid=UserCell] button, [data-testid=UserCell] [role=button]')]
  .slice(0, 40).map(n => ({tag: n.tagName, type: n.type || null, testid: n.getAttribute('data-testid'),
       aria: n.getAttribute('aria-label'), placeholder: n.getAttribute('placeholder'), text: (n.innerText || '').trim().slice(0, 40)}))"""


async def main():
    result = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--headless=new"])
        context = await browser.new_context(storage_state=SECRET, locale="en-US", viewport={"width": 1280, "height": 900})
        page = await context.new_page()
        await page.goto("https://x.com/MargaretAlxgav/lists", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(6000)
        body = await page.locator("body").inner_text()
        owned = body[body.rindex("Your Lists") :]
        if NAME in owned:
            await page.get_by_text(NAME, exact=True).last.click()
            await page.wait_for_url(re.compile(r"/i/lists/\d+"), timeout=20000)
            result["created"] = False
        else:
            await page.goto("https://x.com/i/lists/create", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)
            dialog = page.locator("[role=dialog]")
            await dialog.locator('input[name="name"]').fill(NAME)
            private = dialog.locator('input[type="checkbox"]')
            if not await private.is_checked():
                await private.check()
            assert await private.is_checked()
            await dialog.get_by_role("button", name="Next").click()
            await page.wait_for_url(re.compile(r"/i/lists/\d+"), timeout=30000)
            result["created"] = True
        result["url_after"] = page.url
        result["list_id"] = re.search(r"/i/lists/(\d+)", page.url).group(1)
        await page.wait_for_timeout(5000)
        result["ui"] = await page.evaluate(DUMP_JS)
        await page.screenshot(path=OUT + "/x-after-create.png")
        await browser.close()
    print(json.dumps(result, indent=0))


asyncio.run(main())
