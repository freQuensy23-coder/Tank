import os
from decimal import ROUND_DOWN, Decimal, localcontext

import pytest

from tank.analysis.truth_sql import arithmetic_query
from tank.clickhouse_market import ClickHouse

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    ("operation", "left", "right"),
    [
        ("add", "999999999999999999999999", "0.000000000000000000000001"),
        ("add", "-100000000000000000000000", "0.000000000000000000000001"),
        ("multiply", "999999999999999999999999", "999999999999999999999999"),
        ("multiply", "-0.25", "8"),
        ("divide", "2", "3"),
        ("divide", "-2", "3"),
        ("divide", "1", "0.000000000000000000000001"),
        ("funding_8h", "0.25", "3"),
        ("funding_8h", "-0.25", "3"),
        ("compare", "0.500000000000000000000001", "0.5"),
        ("compare", "-0.500000000000000000000001", "-0.5"),
    ],
)
async def test_native_sql_financial_arithmetic(operation, left, right, record_property):
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned native CI service"
    with localcontext() as context:
        context.prec = 100
        a, b = Decimal(left), Decimal(right)
        if operation == "add":
            expected = a + b
        elif operation == "multiply":
            expected = a * b
        elif operation == "funding_8h":
            expected = a * 8 / b
        elif operation == "divide":
            expected = a / b
        else:
            expected = Decimal((a > b) - (a < b))
        expected = expected.quantize(Decimal("1e-24"), rounding=ROUND_DOWN)
    query, params = arithmetic_query(operation, left, right)
    record_property("executed_sql", query)
    record_property("sql_parameters", str(params))
    client = ClickHouse(url)
    try:
        rows = await client.rows(query, params)
    finally:
        await client.close()
    assert len(rows) == 1
    assert Decimal(rows[0]["value"]) == expected


@pytest.mark.parametrize(
    ("operation", "left", "right"),
    [
        ("add", "1000000000000000000000000", "1"),
        ("add", "0.0000000000000000000000001", "1"),
        ("multiply", "NaN", "1"),
        ("add", "Infinity", "1"),
        ("divide", "1", "0"),
        ("funding_8h", "0.25", "0"),
    ],
)
async def test_native_sql_refuses_unrepresentable_financial_input(operation, left, right):
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned native CI service"
    query, params = arithmetic_query(operation, left, right)
    client = ClickHouse(url)
    try:
        with pytest.raises(RuntimeError, match="ClickHouse"):
            await client.rows(query, params)
    finally:
        await client.close()
