"""Unauthenticated public-page renderer for S19, isolated from the X session."""

from __future__ import annotations

import asyncio


class BrowserReader:
    def __init__(self, settings):
        self.settings = settings
        self.driver = None
        self.browser = None
        self.context = None
        self.lock = asyncio.Lock()

    async def open(self):
        if self.context is not None:
            return
        from playwright.async_api import async_playwright

        self.driver = await async_playwright().start()
        self.browser = await self.driver.chromium.launch(headless=True, args=["--headless=new"])
        self.context = await self.browser.new_context()

    async def html(self, url):
        if self.context is None:
            async with self.lock:
                await self.open()
        page = await self.context.new_page()
        try:
            await page.goto(url, wait_until="networkidle", timeout=20000)
            return (await page.content()).encode()
        finally:
            await page.close()

    async def close(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.driver:
            await self.driver.stop()
        self.context = None
        self.browser = None
        self.driver = None
