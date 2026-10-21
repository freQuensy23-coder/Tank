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

PUBLISHED = "Tue, 20 Oct 2026 22:36:00 +0000"
TITLE = "South Korea's FSC approves first spot bitcoin ETFs, clearing three funds to list on KRX"
URL = (
    "https://www.theblock.co/news/regulation/2026-10-20-south-korea-fsc-approves-first-spot-bitcoin-etfs-"
    "three-funds-to-list-on-krx-421165"
)
BODY = (
    "<p>South Korea's Financial Services Commission (FSC) approved the country's first spot bitcoin "
    "exchange-traded funds on Wednesday, clearing products from Samsung Asset Management, Mirae Asset "
    "Global Investments and KB Asset Management to list on the Korea Exchange (KRX), according to a notice "
    "published by the regulator.</p>"
    "<p>The approval follows amendments to the Capital Markets Act that took effect earlier this month and "
    "allow ETFs to hold bitcoin directly. Trading is expected to start on October 28, with creations settled "
    "in spot bitcoin held by local custodians. The FSC said retail investors will be able to buy the funds "
    "through ordinary brokerage accounts.</p>"
    "<p>An FSC official confirmed the approval to The Block.</p>"
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
