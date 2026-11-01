from decimal import getcontext, localcontext

import pytest
from test_clickhouse_market import both as both

from tank import clickhouse_market as chm

pytestmark = pytest.mark.integration


async def test_real_sampled_binary_matches_json_types_quantum_and_timestamp_format(both):
    _, routed, _ = both
    client = routed.clickhouse
    kinds = ["String", "Nullable(String)", "Bool", "Int64", "UInt64", chm.DT, "Array(String)"]
    sql = (
        "SELECT repeat('A', 64) AS fingerprint, arrayMap(t -> tuple("
        "{money:String}, CAST(NULL, 'Nullable(String)'), CAST(1, 'Bool'), "
        "CAST('-9223372036854775808', 'Int64'), CAST('18446744073709551615', 'UInt64'), "
        "fromUnixTimestamp64Micro(t, 'UTC'), ['α', '', concat('a', char(0), 'b')]), "
        "[CAST(-1, 'Int64'), CAST(0, 'Int64'), CAST(1000001, 'Int64')]) AS records"
    )
    params = {"money": ("9" * 100 + ".00000000", "String")}
    queries = []
    post = client._post

    async def observed(query, parameters):
        queries.append(query)
        return await post(query, parameters)

    client._post = observed
    with localcontext() as context:
        context.prec = 9
        context.clear_flags()
        expected = await client.rows(sql, params)
        expected_flags = context.flags.copy()
        context.clear_flags()
        queries.clear()
        actual = await client.sampled_state_rows(sql, params, kinds)
        assert actual == expected
        assert queries == [sql + " FORMAT RowBinary"]
        assert context.flags == expected_flags
        assert getcontext().prec == 9
        assert type(actual[0]["records"][0][2]) is bool
        assert type(actual[0]["records"][0][3]) is int


async def test_real_sampled_binary_preserves_json_invalid_utf8_behavior(both):
    _, routed, _ = both
    client = routed.clickhouse
    sql = "SELECT repeat('A', 64) AS fingerprint, [tuple(unhex('FF'))] AS records"
    queries = []
    post = client._post

    async def observed(query, params):
        queries.append(query)
        return await post(query, params)

    client._post = observed
    expected_error = None
    try:
        expected = await client.rows(sql)
    except (UnicodeError, ValueError) as error:
        expected_error = type(error)
    queries.clear()
    if expected_error is not None:
        with pytest.raises(expected_error):
            await client.sampled_state_rows(sql, {}, ["String"])
    else:
        assert await client.sampled_state_rows(sql, {}, ["String"]) == expected
    assert queries == [sql + " FORMAT RowBinary", sql + " FORMAT JSONCompactEachRowWithNames"]


async def test_real_sampled_binary_preserves_empty_typed_population(both):
    _, routed, _ = both
    client = routed.clickhouse
    sql = "SELECT repeat('A', 64) AS fingerprint, CAST([], 'Array(Tuple(Nullable(String)))') AS records"
    assert await client.sampled_state_rows(sql, {}, ["Nullable(String)"]) == await client.rows(sql)
