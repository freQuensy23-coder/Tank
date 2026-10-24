"""Apify adapter: read INPUT once and write one OUTPUT response."""

import asyncio

from apify import Actor
from direct_fetch import fetch_snapshot


async def main() -> None:
    async with Actor:
        response = await fetch_snapshot(await Actor.get_input())
        await Actor.set_value("OUTPUT", response)


if __name__ == "__main__":
    asyncio.run(main())
