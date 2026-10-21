"""One bounded read-only X probe from immutable operator JSON; never saves browser state."""
import asyncio
import json
import os
from pathlib import Path
import re
import signal
from urllib.parse import urlsplit

STATE = Path('/run/secrets/x_session')
OUTPUT = Path('/out/probe-report.json')
REPORT = {'complete': False, 'status': 'NOT_STARTED', 'authenticated': False, 'challenge': False,
          'account_identity_readback': False, 'list_inventory_readback': False,
          'list_found': False, 'exact_86_members_verified': False, 'mutating_requests_blocked': 0,
          'login_attempted': False, 'cookies_saved_or_overwritten': False, 'list_mutations': 0}


def save():
    OUTPUT.write_text(json.dumps(REPORT, indent=2) + '\n')
    OUTPUT.chmod(0o600)


def expected_handles():
    text = Path('/prd/01_Tank_V0_PRD.md').read_text()
    section = text.split('### 4.3 X accounts (86)', 1)[1].split('### 4.4', 1)[0]
    handles = {h.lower() for h in re.findall(r'@([A-Za-z0-9_]+)', section)}
    if len(handles) != 86:
        raise ValueError('PRD_HANDLE_COUNT')
    return handles


async def guarded(page):
    path = urlsplit(page.url).path.lower()
    if any(value in path for value in ('/i/flow/', '/i/jf/onboarding', '/account/access', '/login', '/challenge', '/account/login_verification')):
        REPORT['challenge'] = not ('login' in path or 'onboarding' in path)
        REPORT['status'] = 'AUTH_CHECKPOINT' if REPORT['challenge'] else 'AUTH_UNAVAILABLE'
        return False
    if await page.locator('iframe[src*="arkoselabs"],iframe[src*="captcha"],[data-testid="ocfEnterTextTextInput"]').count():
        REPORT['challenge'] = True
        REPORT['status'] = 'AUTH_CHECKPOINT'
        return False
    return True


async def run():
    from playwright.async_api import async_playwright
    expected = expected_handles()
    state = json.loads(STATE.read_text())
    if not isinstance(state, dict) or not isinstance(state.get('cookies'), list):
        raise ValueError('INVALID_STORAGE_STATE')
    REPORT['expected_handle_count'] = len(expected)
    REPORT['state_cookie_count'] = len(state['cookies'])
    REPORT['state_origins_count'] = len(state.get('origins', []))
    async with async_playwright() as driver:
        browser = await driver.chromium.launch(headless=True)
        try:
            context = await browser.new_context(storage_state=state)
            try:
                async def readonly(route):
                    request = route.request
                    host = (urlsplit(request.url).hostname or '').lower()
                    allowed_host = host in {'x.com', 'twitter.com', 't.co'} or host.endswith(('.x.com', '.twitter.com', '.twimg.com'))
                    if request.method not in {'GET', 'HEAD', 'OPTIONS'}:
                        REPORT['mutating_requests_blocked'] += 1
                        await route.abort()
                    elif request.url.startswith('https://') and allowed_host:
                        await route.continue_()
                    else:
                        await route.abort()
                await context.route('**/*', readonly)
                page = await context.new_page()
                REPORT['status'] = 'NAVIGATING_LIST_INVENTORY'
                save()
                await page.goto('https://x.com/i/lists', wait_until='domcontentloaded', timeout=20000)
                if not await guarded(page):
                    return
                try:
                    await page.locator('[data-testid="AppTabBar_Profile_Link"],[data-testid="SideNav_AccountSwitcher_Button"]').first.wait_for(timeout=12000)
                except Exception:
                    if not await guarded(page):
                        return
                    REPORT['status'] = 'AUTH_IDENTITY_UNCONFIRMED'
                    return
                if not await guarded(page):
                    return
                profile = page.locator('[data-testid="AppTabBar_Profile_Link"]')
                profile_href = await profile.first.get_attribute('href') if await profile.count() else None
                match = re.fullmatch(r'/([A-Za-z0-9_]+)/?', profile_href or '')
                if not match:
                    REPORT['status'] = 'AUTH_IDENTITY_UNCONFIRMED'
                    return
                REPORT['account_handle'] = match[1]
                REPORT['authenticated'] = True
                REPORT['account_identity_readback'] = True
                REPORT['status'] = 'READING_LIST_INVENTORY'
                await page.wait_for_timeout(1500)
                links = await page.locator('a[href*="/i/lists/"]').evaluate_all('(nodes)=>nodes.map(n=>({href:n.getAttribute("href"),text:n.innerText}))')
                lists = {}
                for row in links:
                    found = re.search(r'/i/lists/(\d+)(?:$|[/?#])', row.get('href') or '')
                    if found:
                        lists[found[1]] = {'id': found[1], 'label': (row.get('text') or '').strip()[:240]}
                REPORT['lists'] = list(lists.values())
                REPORT['list_inventory_readback'] = True
                selected = [row for row in lists.values() if re.search(r'(?i)(^|\W)tank-v0($|\W)', row['label'])]
                if len(selected) != 1:
                    REPORT['status'] = 'LIST_MISSING' if not selected else 'LIST_AMBIGUOUS'
                    return
                chosen = selected[0]
                REPORT['list_found'] = True
                REPORT['list_id'] = chosen['id']
                await page.goto('https://x.com/i/lists/' + chosen['id'], wait_until='domcontentloaded', timeout=20000)
                if not await guarded(page):
                    return
                await page.wait_for_timeout(1000)
                members = page.locator('a[href$="/members"]')
                if not await members.count():
                    REPORT['status'] = 'LIST_MEMBERS_LINK_UNAVAILABLE'
                    return
                members_href = await members.first.get_attribute('href')
                if members_href != '/i/lists/' + chosen['id'] + '/members':
                    REPORT['status'] = 'LIST_MEMBERS_PATH_MISMATCH'
                    return
                REPORT['members_label'] = (await members.first.inner_text())[:80]
                await members.first.click(timeout=5000)
                if not await guarded(page):
                    return
                await page.locator('[data-testid="UserCell"]').first.wait_for(timeout=10000)
                observed = set()
                stable = 0
                for _ in range(12):
                    before = len(observed)
                    cells = await page.locator('[data-testid="UserCell"] a[href]').evaluate_all('(nodes)=>nodes.map(n=>n.getAttribute("href"))')
                    for href in cells:
                        user = re.fullmatch(r'/([A-Za-z0-9_]+)/?', href or '')
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
                REPORT['observed_member_handles'] = sorted(observed)
                REPORT['observed_member_count'] = len(observed)
                REPORT['missing_expected_handles'] = sorted(expected - observed)
                REPORT['extra_handles'] = sorted(observed - expected)
                REPORT['exact_86_members_verified'] = observed == expected
                REPORT['status'] = 'READY_FOR_ROOT_REVIEW' if observed == expected else 'LIST_MEMBERSHIP_REVIEW_REQUIRED'
            finally:
                await context.close()
        finally:
            await browser.close()


async def main():
    try:
        await asyncio.wait_for(run(), timeout=90)
    except TimeoutError:
        REPORT['status'] = 'BOUNDED_TIMEOUT'
    except Exception as error:
        REPORT['status'] = 'PROBE_FAILED'
        REPORT['error_type'] = type(error).__name__
    finally:
        REPORT['complete'] = True
        save()
        print(json.dumps({key: REPORT[key] for key in ('complete', 'status', 'authenticated', 'challenge', 'account_identity_readback', 'list_inventory_readback', 'list_found', 'exact_86_members_verified', 'login_attempted', 'cookies_saved_or_overwritten', 'list_mutations')}), flush=True)


if __name__ == '__main__':
    os.umask(0o077)
    asyncio.run(main())
