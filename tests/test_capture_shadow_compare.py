"""The tank-capture rollout gate: shadow flow bars compared with the live ones per minute."""

import importlib.util
import json
import os
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "shadow_compare", ROOT / "scripts" / "capture" / "shadow_compare.py"
)
shadow_compare = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(shadow_compare)


def bar(minute, *, iid="binance:SPOT:ABCUSDT", complete=True, venue="binance", market_type="SPOT", **fields):
    row = {
        "instrument_id": iid,
        "minute_start": "2026-10-21T10:" + minute + ":00Z",
        "venue": venue,
        "market_type": market_type,
        "gap_reason": None if complete else "STREAM_COVERAGE_UNPROVEN",
        "buy_qty": "1.5",
        "sell_qty": "0",
        "buy_usd": "15",
        "sell_usd": "0",
        "n_buy": 2,
        "n_sell": 0,
        "sum_sq_qty": "1.25",
        "size_buckets": {"B1": {"buy_qty": "1.5", "sell_qty": "0", "n": 2}},
        "complete": complete,
    }
    row.update(fields)
    return row


def test_numbers_and_size_buckets_compare_as_decimals():
    live = bar("00")
    shadow = bar(
        "00",
        buy_qty="1.50",
        sum_sq_qty="1.2500",
        size_buckets=json.dumps({"B1": {"buy_qty": "1.500", "sell_qty": "0E-8", "n": 2}}),
    )
    assert shadow_compare.differing_fields(live, shadow) == []
    assert shadow_compare.differing_fields(live, bar("00", n_sell=1)) == ["n_sell"]
    changed = {"B1": {"buy_qty": "1.5", "sell_qty": "0", "n": 3}}
    assert shadow_compare.differing_fields(live, bar("00", size_buckets=changed)) == ["size_buckets"]


def test_only_minutes_complete_in_both_schemas_fail_the_gate():
    live = [
        bar("00"),
        bar("01"),
        bar("02"),
        bar("03"),
        bar("00", iid="okx:SWAP:ABC-USDT-SWAP", venue="okx", market_type="PERP_LINEAR"),
    ]
    shadow = [
        bar("00"),
        bar("01", sum_sq_qty="1.26"),  # complete in both and different: the gate
        bar("02", complete=False, buy_qty="1"),  # still unsealed in the shadow writer
        bar("04"),  # complete in the shadow only
        bar("00", iid="okx:SWAP:ABC-USDT-SWAP", venue="okx", market_type="PERP_LINEAR"),
    ]
    order = shadow_compare.key_of
    report = shadow_compare.compare(sorted(live, key=order), sorted(shadow, key=order), show=5)
    binance = report["venues"]["binance/SPOT"]
    assert report["verdict"] == "FAIL"
    assert (binance["compared"], binance["equal"], binance["live_only"], binance["shadow_only"]) == (
        3,
        1,
        1,
        1,
    )
    assert (binance["complete_both"], binance["complete_live_only"], binance["complete_shadow_only"]) == (
        2,
        2,
        1,
    )
    assert binance["gate_failures"] == 1
    assert binance["mismatched"] == {
        "buy_qty": 1,
        "sell_qty": 0,
        "n_buy": 0,
        "n_sell": 0,
        "sum_sq_qty": 1,
        "size_buckets": 0,
        "complete": 1,
    }
    assert report["venues"]["okx/PERP_LINEAR"]["equal"] == 1
    assert report["total"]["compared"] == 4 and report["total"]["gate_failures"] == 1
    assert [(e["minute_start"], e["gate"], sorted(e["fields"])) for e in report["examples"]] == [
        ("2026-10-21T10:01:00Z", True, ["sum_sq_qty"]),
        ("2026-10-21T10:02:00Z", False, ["buy_qty", "complete"]),
    ]
    agreed = [row for row in shadow if row["minute_start"] != "2026-10-21T10:01:00Z"] + [bar("01")]
    assert shadow_compare.compare(sorted(live, key=order), sorted(agreed, key=order))["verdict"] == "PASS"


@pytest.fixture
async def database():
    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TANK_TEST_DATABASE_URL required; creates only an owned disposable database")
    import psycopg
    from psycopg import sql

    from tank.storage import PostgresRepository

    name = "tank_test_shadow_compare_" + uuid.uuid4().hex[:12]
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    target = urlunsplit(urlsplit(dsn)._replace(path="/" + name))
    repo = await PostgresRepository(target).open()
    try:
        await repo.migrate()
        yield repo, target
    finally:
        await repo.close()
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def test_compares_the_shadow_schema_with_live_bars_end_to_end(database, capsys):
    import psycopg
    from psycopg import sql
    from psycopg.types.json import Jsonb

    repo, url = database
    live = [bar("00"), bar("01"), bar("02", complete=False)]
    await repo.put_market_batch("mkt.flow_bars_1m", [dict(row, token_id="ABC") for row in live])
    shadow = [bar("00"), bar("01", n_buy=3), bar("02"), bar("03")]
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as connection:
        await connection.execute("CREATE SCHEMA mkt_shadow")
        await connection.execute(
            "CREATE TABLE mkt_shadow.flow_bars_1m (LIKE mkt.flow_bars_1m "
            "INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)"
        )
        for row in shadow:
            values = dict(row, storage_key=row["instrument_id"] + ":" + row["minute_start"])
            values["size_buckets"] = Jsonb(row["size_buckets"])
            await connection.execute(
                sql.SQL("INSERT INTO mkt_shadow.flow_bars_1m ({}) VALUES ({})").format(
                    sql.SQL(",").join(map(sql.Identifier, values)),
                    sql.SQL(",").join(sql.Placeholder() for _ in values),
                ),
                list(values.values()),
            )
    window = ["--since", "2026-10-21T10:00:00Z", "--until", "2026-10-21T11:00:00Z"]
    status = shadow_compare.main(["--database-url", url, *window, "--json"])
    report = json.loads(capsys.readouterr().out)
    assert status == 1 and report["verdict"] == "FAIL"
    summary = report["venues"]["binance/SPOT"]
    assert {k: summary[k] for k in ("compared", "equal", "shadow_only", "gate_failures")} == {
        "compared": 3,
        "equal": 1,
        "shadow_only": 1,
        "gate_failures": 1,
    }
    assert summary["complete_shadow_only"] == 2 and summary["mismatched"]["n_buy"] == 1
    assert report["examples"][0]["minute_start"] == "2026-10-21T10:01:00Z"
    # Outside the window nothing is compared, and the text report states its verdict.
    later = ["--since", "2026-10-21T11:00:00Z", "--until", "2026-10-21T12:00:00Z"]
    assert shadow_compare.main(["--database-url", url, *later]) == 0
    assert "verdict: PASS" in capsys.readouterr().out
    assert shadow_compare.main(["--database-url", url, *window, "--shadow-schema", "absent"]) == 2
