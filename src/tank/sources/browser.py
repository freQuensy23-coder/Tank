"""Read-only persistent operator browser for X and rendered issuer pages."""
from __future__ import annotations
import asyncio
import re
from pathlib import Path

class BrowserReader:
    def __init__(self, settings):
        self.settings=settings; self.context=None; self.driver=None; self.lock=asyncio.Lock()

    async def open(self):
        if self.context is None:
            from playwright.async_api import async_playwright
            self.driver=await async_playwright().start()
            self.context=await self.driver.chromium.launch_persistent_context(str(self.settings.x_profile_dir),headless=True,args=['--headless=new'])

    async def close(self):
        if self.context: await self.context.close()
        if self.driver: await self.driver.stop()
        self.context=None; self.driver=None

    async def html(self,url):
        async with self.lock:
            await self.open(); page=await self.context.new_page()
            try:
                await page.goto(url,wait_until='networkidle',timeout=20000)
                return (await page.content()).encode()
            finally: await page.close()

    async def posts(self,list_id,checkpoint=None):
        if not list_id: raise ValueError('TANK_X_LIST_ID/private list configuration required')
        async with self.lock:
            await self.open(); page=await self.context.new_page(); result={}; reached=False
            try:
                await page.goto(f'https://x.com/i/lists/{list_id}',wait_until='domcontentloaded',timeout=20000)
                await page.wait_for_selector('article',timeout=20000)
                stalled=0
                for _ in range(1000):
                    old=set(result)
                    articles=page.locator('article')
                    for idx in range(await articles.count()):
                        node=articles.nth(idx)
                        # The status permalink containing the outer time identifies the author;
                        # quote cards have their own links and must not replace that identity.
                        times=node.locator('time[datetime]')
                        if not await times.count(): continue
                        time=times.first; link=time.locator('xpath=ancestor::a[1]')
                        href=await link.get_attribute('href') or ''
                        match=re.search(r'/([^/]+)/status/(\d+)',href)
                        if not match: continue
                        handle,status_id=match.groups()
                        if checkpoint and status_id==checkpoint: reached=True; break
                        expand=node.locator('[data-testid="tweet-text-show-more-link"]')
                        if await expand.count():
                            await expand.first.click()
                            await page.wait_for_timeout(250)
                        pieces=await node.locator('[data-testid="tweetText"]').all_text_contents()
                        links=await node.locator('a[href*="/status/"]').evaluate_all('(nodes)=>nodes.map(n=>n.getAttribute("href"))')
                        related=[]
                        for target in links:
                            m=re.search(r'/([^/]+)/status/(\d+)',target or '')
                            if m and m.group(2)!=status_id and m.group(2) not in [r['status_id'] for r in related]: related.append({'author':m.group(1),'status_id':m.group(2)})
                        social=await node.locator('[data-testid="socialContext"]').all_text_contents()
                        result[status_id]=dict(external_id=status_id,author=handle,title='',text=pieces[0] if pieces else '',quoted='\n'.join(pieces[1:]) or None,quoted_status_ids=[r['status_id'] for r in related],related_authors=related,repost_context=' '.join(social) or None,url=f'https://x.com/{handle}/status/{status_id}',published_at=await time.get_attribute('datetime'))
                    if reached: break
                    stalled=stalled+1 if set(result)==old else 0
                    await page.mouse.wheel(0,1200); await page.wait_for_timeout(1200)
                    if stalled>5:
                        # Do not advance checkpoint when pagination failed before old checkpoint.
                        if checkpoint: raise RuntimeError('X timeline pagination stalled before checkpoint')
                        break
                else: raise RuntimeError('X pagination limit reached before checkpoint')
                # Fetch quote/repost status pages because list quote cards truncate long text.
                detail=await self.context.new_page()
                try:
                    for item in result.values():
                        full_quotes=[]
                        for related in item['related_authors']:
                            await detail.goto(f"https://x.com/{related['author']}/status/{related['status_id']}",wait_until='domcontentloaded',timeout=20000)
                            await detail.wait_for_selector('article',timeout=20000)
                            candidates=detail.locator('article')
                            found=False
                            for index in range(await candidates.count()):
                                candidate=candidates.nth(index)
                                permalink=candidate.locator('time').first.locator('xpath=ancestor::a[1]')
                                if related['status_id'] not in (await permalink.get_attribute('href') or ''): continue
                                expansion=candidate.locator('[data-testid="tweet-text-show-more-link"]')
                                if await expansion.count(): await expansion.first.click()
                                full_quotes.append(await candidate.locator('[data-testid="tweetText"]').first.inner_text())
                                found=True; break
                            if not found: raise RuntimeError('Quoted status full text unavailable: '+related['status_id'])
                        if full_quotes: item['quoted']='\n'.join(full_quotes)
                finally: await detail.close()
                return list(result.values()),reached
            finally: await page.close()
