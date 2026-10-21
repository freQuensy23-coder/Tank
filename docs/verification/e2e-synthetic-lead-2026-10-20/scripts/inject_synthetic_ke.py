"""Lead E2E #10 (21 Oct, r118): one synthetic The Block RSS item through the production ingest path.

The feed document is parsed by the product's own RSS parser, its bytes stored by the
raw store, and the row ingested by SourceService.ingest, which queues FILTER_ITEM.
"""
import asyncio
import json
import sys
import uuid

from tank.settings import Settings, resolve_versions
from tank.sources import SourceService
from tank.sources.parsers import feeds
from tank.storage import PostgresRepository
from tank.util import iso, new_id

PUBLISHED = sys.argv[1]  # RFC 2822, e.g. "Tue, 20 Oct 2026 23:50:00 +0000"
TITLE = "Kenya's CMA approves first spot bitcoin ETFs for NSE listing; 700 BTC seed"
URL = (
    "https://www.theblock.co/news/regulation/2026-10-21-kenya-cma-approves-first-spot-bitcoin-etfs-"
    "nse-421533"
)
BODY = (
    "<p>Kenya's Capital Markets Authority (CMA) approved the country's first two spot bitcoin "
    "exchange-traded funds on Wednesday, clearing products from two local asset managers to list on the Nairobi "
    "Securities Exchange (NSE), according to a notice published by the regulator.</p>"
    "<p>The CMA had not listed crypto funds in its published regulatory sandbox plans, so the approval came "
    "earlier than market participants expected. The funds will hold bitcoin directly and begin trading on "
    "December 9. The issuers said they plan to seed the funds with about 700 BTC bought on the spot market "
    "before launch.</p>"
    "<p>A CMA spokesperson confirmed the approval to The Block.</p>"
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
