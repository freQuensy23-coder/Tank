"""Lead E2E (20 Oct): one synthetic The Block RSS item through the production ingest path.

The feed document is parsed by the product's own RSS parser, its bytes stored by the
raw store, and the row ingested by SourceService.ingest, which queues FILTER_ITEM.
"""
import asyncio
import json
import uuid

from tank.settings import Settings, resolve_versions
from tank.sources import SourceService
from tank.sources.parsers import feeds
from tank.storage import PostgresRepository
from tank.util import iso, new_id

PUBLISHED = "Tue, 20 Oct 2026 22:35:00 +0000"
TITLE = "Japan's GPIF approves first bitcoin allocation, plans up to $8 billion in spot purchases"
URL = (
    "https://www.theblock.co/news/markets/2026-10-20-japans-gpif-approves-first-bitcoin-allocation-"
    "plans-up-to-8-billion-in-spot-purchases-421163"
)
BODY = (
    "<p>Japan's Government Pension Investment Fund (GPIF), the world's largest public pension fund with "
    "about ¥260 trillion ($1.7 trillion) in assets, has approved its first allocation to bitcoin, according "
    "to a statement published by the fund early Wednesday Tokyo time.</p>"
    "<p>The fund's investment committee approved a target allocation of up to 0.5% of assets, roughly "
    "$8 billion, to be bought as spot bitcoin through two external asset managers over six months starting "
    "in November. GPIF said a Japanese trust bank will provide custody and that the allocation will not use "
    "derivatives or exchange-traded products.</p>"
    "<p>The fund had studied bitcoin in a 2025 research program on alternative assets. A GPIF spokesperson "
    "confirmed the decision to The Block.</p>"
)
RSS = (
    '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0" '
    'xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:dc="http://purl.org/dc/elements/1.1/">'
    "<channel><title>The Block</title><link>https://www.theblock.co</link>"
    "<description>The Block features breaking Crypto news</description>"
    f"<item><title>{TITLE.replace('&', '&amp;')}</title><link>{URL}</link>"
    f'<guid isPermaLink="false">{uuid.uuid4()}</guid><pubDate>{PUBLISHED}</pubDate>'
    "<category>Markets</category><category>Bitcoin</category><category>News</category>"
    f"<description><![CDATA[{BODY}]]></description><content:encoded><![CDATA[{BODY}]]></content:encoded>"
    "</item></channel></rss>"
)


async def main():
    settings = resolve_versions(Settings())
    repo = PostgresRepository(settings.database_url)
    await repo.open()
    svc = SourceService(repo, settings)
    try:
        source = svc.sources["S34"]
        feed = source["feeds"][0]
        payload = RSS.encode()
        rows = feeds(payload, "S34", feed)
        assert len(rows) == 1 and not rows.errors, rows.errors
        row = dict(rows[0])
        row.update(
            feed=feed["url"],
            observed_at=iso(),
            item_id=new_id("item"),
            discovery_config_version=settings.config_version,
            discovery_code_version=settings.code_version,
        )
        row["raw_ref"], row["raw_sha256"] = svc.raw(
            payload, source_id="S34", item_id=row["item_id"], observed_at=row["observed_at"]
        )
        await svc.ingest(source, row)
        stored = await repo.get("ingest.source_items", row["item_id"])
        keys = ("item_id", "external_id", "published_at", "published_at_precision", "observed_at", "relevance")
        print(json.dumps({k: stored.get(k) for k in keys}, default=str))
    finally:
        if hasattr(svc, "close"):
            await svc.close()
        await repo.close()


asyncio.run(main())
