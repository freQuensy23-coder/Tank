"""Initialize only a caller-owned loopback ClickHouse test service."""

import asyncio
import ipaddress
import os
from urllib.parse import urlsplit

from tank.clickhouse_market import MARKET_TABLES, TRADES, ClickHouse, ddl


def test_url(value: str) -> str:
    parts = urlsplit(value)
    host = parts.hostname or ""
    try:
        loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = host == "localhost"
    if parts.scheme not in {"http", "https"} or not loopback or parts.path not in {"", "/"}:
        raise ValueError("CLICKHOUSE_TEST_URL must name a dedicated loopback HTTP service")
    if parts.query or parts.fragment:
        raise ValueError("CLICKHOUSE_TEST_URL must not contain query parameters or a fragment")
    return value


async def main() -> None:
    client = ClickHouse(test_url(os.environ["CLICKHOUSE_TEST_URL"]))
    try:
        version = (await client.rows("SELECT version() AS version"))[0]["version"]
        if not version.startswith("25.8."):
            raise RuntimeError("CI requires the pinned ClickHouse 25.8 release line")
        for statement in ddl():
            await client.execute(statement)
        expected = {*MARKET_TABLES, TRADES}
        actual = {
            f"mkt.{row['name']}"
            for row in await client.rows("SELECT name FROM system.tables WHERE database = 'mkt'")
        }
        if expected - actual:
            raise RuntimeError("The isolated ClickHouse service is missing required market tables")
        print(f"ClickHouse {version}: all {len(expected)} required market tables ready")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
