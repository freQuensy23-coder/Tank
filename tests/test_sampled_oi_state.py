from datetime import timedelta

import pytest
from test_clickhouse_market import IIDS, T0, ch_row
from test_clickhouse_market import both as both

from tank import clickhouse_market as chm
from tank import market_storage as ms
from tank.market_schema import MARKET_COLUMNS
from tank.util import iso

pytestmark = pytest.mark.integration


async def test_sampled_oi_state_suppresses_unchanged_payload_and_refreshes_interior_corrections(both):
    memory, routed, put = both
    collection = "mkt.oi_obs"
    rows = [
        dict(
            instrument_id=iid,
            metric_key=metric,
            ts=iso(T0 + timedelta(minutes=minute)),
            ts_prec="EXACT",
            oi_native=str(value) + ".00",
            oi_usd=None,
            recorded_at=iso(T0 + timedelta(minutes=10)),
        )
        for iid, metric, minute, value in (
            (IIDS[0], "native", 0, 1),
            (IIDS[0], "native", 2, 2),
            (IIDS[0], "native", 5, 7),
            (IIDS[0], "native", 6, 3),
            (IIDS[0], "native", 10, 999),
            (IIDS[1], "native", 1, 4),
            (IIDS[0], "other", 1, 5),
        )
    ]
    rows[0].update(oi_usd="99.000", mark_price="19.00")
    rows[1]["ts"] = iso(T0 + timedelta(minutes=2, microseconds=1))
    await put(collection, rows)
    args = collection, "ts", ("metric_key",), IIDS, T0, T0 + timedelta(minutes=10), 300
    expected = await ms.sampled_market_history(memory, *args)
    names = [*MARKET_COLUMNS[collection], "storage_key", "_version"]
    kinds = [
        chm.TYPES[kind]
        if name in chm.MARKET_IDENTITIES[collection] or kind == "text[]"
        else f"Nullable({chm.TYPES[kind]})"
        for name, kind in MARKET_COLUMNS[collection].items()
    ] + ["String", "UInt64"]
    fields = ", ".join(f"`{name}`" for name in names)
    q = chm.Query()
    chm._ids(q, IIDS)
    q.where(f"`ts` >= {q.param(T0, chm.DT)}")
    q.where(f"`ts` < {q.param(T0 + timedelta(minutes=10), chm.DT)}")
    groups = "instrument_id, `metric_key`, intDiv(toUnixTimestamp(`ts`), 300)"
    sort_keys = [f"x.{names.index(name) + 1}" for name in ("instrument_id", "metric_key")]
    sort_keys.append(f"intDiv(toUnixTimestamp(x.{names.index('ts') + 1}), 300)")
    reference_query = (
        f"WITH arraySort(x -> tuple({', '.join(sort_keys)}), groupArray(sampled_record)) AS population, "
        "hex(SHA256(toString(population))) AS fingerprint "
        "SELECT fingerprint, population AS records FROM ("
        f"SELECT CAST(tuple({fields}) AS Tuple({', '.join(kinds)})) AS sampled_record "
        f"FROM {chm._table(collection)} WHERE {q.predicate()} "
        f"ORDER BY {groups}, `ts` DESC, storage_key DESC LIMIT 1 BY {groups})"
    )
    reference = await routed.clickhouse.rows(reference_query, q.params)

    def projected_reference(value):
        projected = []
        for row in value:
            projected.append(dict(row, records=[record[:-2] for record in row["records"]]))
        return projected

    actual_rows, selected, queries = routed.clickhouse.rows, [], []

    async def observed(query, params=None):
        result = await actual_rows(query, params)
        if "AS fingerprint" in query:
            selected.append(result)
            queries.append(query)
        return result

    routed.clickhouse.rows = observed
    cache = {}
    first = await chm.sampled_market_history(routed.clickhouse, *args, cache=cache)
    second = await chm.sampled_market_history(routed.clickhouse, *args, cache=cache)
    assert first == second == expected
    assert selected[0] == projected_reference(reference)
    assert "argMax(" in queries[0] and "LIMIT 1 BY" not in queries[0]
    assert len(first) == 4
    assert len(selected[-1]) == 1
    assert selected[-1][0]["records"] == []
    assert len(selected[-2][0]["records"]) == len(expected)
    previous_fingerprint = selected[-1][0]["fingerprint"]
    first[0]["gap_reason"] = "caller-local"
    assert await chm.sampled_market_history(routed.clickhouse, *args, cache=cache) == expected
    assert selected[-1][0]["records"] == []

    changed = dict(rows[1], oi_native="200.000", mark_price="17.00")
    await memory.put_market_batch(collection, [changed])
    prepared = ms._prepare_records(collection, [changed], None, MARKET_COLUMNS[collection], False)
    for key, record in prepared.items():
        await routed.clickhouse.insert(
            collection,
            [dict(ch_row(collection, record, key), storage_key=key + "-replacement", _version=2)],
        )
    refreshed = await chm.sampled_market_history(routed.clickhouse, *args, cache=cache)
    assert selected[-1] == projected_reference(await actual_rows(reference_query, q.params))
    assert refreshed == await ms.sampled_market_history(memory, *args)
    assert selected[-1][0]["fingerprint"] != previous_fingerprint
    assert len(selected[-1][0]["records"]) == len(refreshed)
    assert any(row["oi_native"] == "200.000" and row["mark_price"] == "17.00" for row in refreshed)
    assert any(row["oi_native"] == "2.00" for row in first)
    previous_fingerprint = selected[-1][0]["fingerprint"]
    for key, record in prepared.items():
        await routed.clickhouse.insert(
            collection,
            [dict(ch_row(collection, record, key), storage_key=key + "-replacement", _version=3)],
        )
    version_only = await chm.sampled_market_history(routed.clickhouse, *args, cache=cache)
    assert version_only == refreshed
    assert selected[-1] == projected_reference(await actual_rows(reference_query, q.params))
    assert selected[-1][0]["fingerprint"] != previous_fingerprint
    assert selected[-1][0]["records"]
    assert await chm.sampled_market_history(routed.clickhouse, *args, cache=cache) == refreshed
    assert selected[-1][0]["records"] == []
    empty = await chm.sampled_market_history(
        routed.clickhouse, collection, "ts", ("metric_key",), IIDS, T0, T0, 300, cache={}
    )
    assert empty == []
    assert selected[-1] == await actual_rows("SELECT hex(SHA256(toString([]))) AS fingerprint, [] AS records")
