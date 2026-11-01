from datetime import timedelta

import pytest

from tank import clickhouse_market as chm
from tank.util import parse_time


@pytest.mark.parametrize("columnar", [False, True])
async def test_key_envelope_and_payload_use_one_transport_request(columnar):
    from test_market_truth_wide_columns import WideClient

    at = parse_time("2026-10-19T00:00:00Z")
    rows = [dict(instrument_id="A:perp:T", storage_key="K", ts=at.isoformat(), oi_native="1.000")]
    ch = WideClient(rows)
    query = chm.Query()
    query.where(f"ts >= {query.param(at, chm.DT)}")
    query.where(f"ts < {query.param(at + timedelta(hours=1), chm.DT)}")
    actual = await chm._select(ch, "mkt.oi_obs", query, "ts, storage_key", "LIMIT 1", columnar=columnar)
    expected = {name: [row[name] for row in rows] for name in rows[0]} if columnar else rows
    assert actual == expected
    assert len(ch.row_queries) + len(ch.column_queries) == 1
