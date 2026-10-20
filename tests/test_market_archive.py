import hashlib
import io
import zipfile

import pytest

from tank.market.archive import ArchiveIntegrityError, RawArchive, verify_binance_zip
from tank.market.supply import unlock_schedule


def zipped(rows):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr("test.csv", rows)
    b = out.getvalue()
    return b, hashlib.sha256(b).hexdigest() + "  test.zip"


def test_raw_parquet_hash_and_roundtrip(tmp_path):
    archive = RawArchive(tmp_path)
    row = archive.write("binance:spot:BTC", "trade", {"q": "0.00000000000000001"})
    assert (
        RawArchive.read_verified(row["path"], row["sha256"])[0]["payload_json"]
        == '{"q":"0.00000000000000001"}'
    )
    with pytest.raises(ArchiveIntegrityError):
        RawArchive.read_verified(row["path"], "0" * 64)


def test_microsecond_archive_checks_checksum_units_and_gaps():
    b, c = zipped(
        "1767225600000000,1,1,1,1,1,1767225659999999,1,1,1,1\n1767225720000000,1,1,1,1,1,1767225779999999,1,1,1,1\n"
    )
    r = verify_binance_zip(b, c, filename="test.zip", kind="klines", expected_unit="US")
    assert not r["complete"] and len(r["gaps"]) == 2
    with pytest.raises(ArchiveIntegrityError):
        verify_binance_zip(b, c, filename="other.zip", kind="klines")
    with pytest.raises(ArchiveIntegrityError):
        verify_binance_zip(b, c, filename="test.zip", kind="klines", expected_unit="MS")
    with pytest.raises(ArchiveIntegrityError):
        verify_binance_zip(b + b"x", c, filename="test.zip", kind="klines")


def test_aggtrade_continuity_and_duplicate_ids():
    b, c = zipped(
        "1,1,2,3,4,1767225600000,false\n1,1,2,3,4,1767225600001,true\n3,1,2,3,4,1767225600002,false\n"
    )
    r = verify_binance_zip(b, c, filename="test.zip", kind="aggTrades")
    assert r["duplicates"] == [1] and r["gaps"] == [{"after": 1, "before": 3}]


def test_complete_daily_klines_and_metrics_row_counts():
    rows = "".join(f"{1767225600000 + i * 60000},1,1,1,1,1,1767225659999,1,1,1,1\n" for i in range(1440))
    b, c = zipped(rows)
    assert verify_binance_zip(b, c, filename="test.zip", kind="klines")["complete"]
    b, c = zipped(
        "create_time,symbol,sum_open_interest,sum_open_interest_value\n"
        + "\n".join(f"2026-01-01 {i // 12:02}:{(i % 12) * 5:02}:00,BTCUSDT,10,100" for i in range(288))
    )
    assert verify_binance_zip(b, c, filename="test.zip", kind="metrics")["complete"]


def test_unlock_delta_is_potential_supply_not_sale():
    data = {
        "documentedData": {
            "data": [
                {
                    "label": "team",
                    "data": [
                        {"timestamp": 1767225600, "unlocked": 100},
                        {"timestamp": 1767312000, "unlocked": 140},
                        {"timestamp": 1767398400, "unlocked": 140},
                    ],
                }
            ]
        }
    }
    r = unlock_schedule(data, now="2026-01-01T12:00:00Z")
    assert len(r) == 1 and r[0]["qty"] == "40" and r[0]["sale_inferred"] is False
    with pytest.raises(ValueError):
        unlock_schedule({"wrong": []})


def test_hourly_segments_seal_and_prefix_survives_later_frames(tmp_path):
    archive = RawArchive(tmp_path)
    first = archive.write("venue:spot:T", "trade", {"id": 1}, received_ts="2026-01-01T01:01:00Z")
    archive.write("venue:spot:T", "trade", {"id": 2}, received_ts="2026-01-01T01:59:00Z")
    assert (
        len(RawArchive.read_verified(first["path"], first["sha256"], byte_length=first["byte_length"])) == 1
    )
    sealed = archive.flush_expired("2026-01-01T02:00:00Z")
    assert len(sealed) == 1 and sealed[0]["status"] == "SEALED"
    assert sealed[0]["path"].endswith(".parquet")
    assert len(RawArchive.read_verified(sealed[0]["path"], sealed[0]["sha256"])) == 2
    assert archive.flush_all() == []


@pytest.mark.asyncio
async def test_archive_checksum_rejected_and_durable_hour_retry(tmp_path):
    from types import SimpleNamespace

    import httpx

    from tank.market.archive import fetch_archive
    from tank.storage import MemoryRepository
    from tank.util import parse_time

    payload, _ = zipped("1767225600000000,1,1,1,1,1,1767225659999999,1,1,1,1\n")
    downloads = []
    filename = "BTCUSDT-1m-2026-01-01.zip"
    checksum = hashlib.sha256(payload).hexdigest() + "  " + filename

    def response(request):
        if request.url.path.endswith(".CHECKSUM"):
            return httpx.Response(200, text=checksum)
        downloads.append(str(request.url))
        return httpx.Response(200, content=payload + b"corrupt")

    registry = {
        "instruments": [
            {
                "instrument_id": "binance:spot:BTCUSDT",
                "token_id": "bitcoin:BTC",
                "venue": "binance",
                "venue_symbol": "BTCUSDT",
                "market_type": "SPOT",
                "quote_asset": "USDT",
                "contract_multiplier": "1",
            }
        ]
    }
    settings = SimpleNamespace(
        data_dir=tmp_path,
        config_version="test",
        code_version="test",
        load_config=lambda kind: registry if kind == "registry" else {"archive_retry_s": 3600},
    )
    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        with pytest.raises(ArchiveIntegrityError):
            await fetch_archive(repo, settings, "BTCUSDT", "2026-01-01", http=client)
        retry = (await repo.list("qa.reviews"))[0]
        assert retry["label"] == "PENDING" and retry["attempts"] == 1
        assert (parse_time(retry["retry_at"]) - parse_time(retry["at"])).total_seconds() == 3600
        assert not await repo.list("mkt.raw_archives")
        assert len(downloads) == 1  # No immediate checksum retry.
        with pytest.raises(ArchiveIntegrityError):
            await fetch_archive(repo, settings, "BTCUSDT", "2026-01-01", http=client, _scheduled_retry=True)
        final = max(await repo.list("qa.reviews"), key=lambda row: row["at"])
        assert final["label"] == "FAILED" and final["attempts"] == 2
    assert len(downloads) == 2


def test_restart_seals_immutable_versions_without_replacing_previous_evidence(tmp_path):
    archive = RawArchive(tmp_path)
    archive.write("venue:spot:T", "trade", {"id": 1}, received_ts="2026-01-01T01:01:00Z")
    first = archive.flush_all()[0]
    restarted = RawArchive(tmp_path)
    restarted.write("venue:spot:T", "trade", {"id": 2}, received_ts="2026-01-01T01:20:00Z")
    second = restarted.flush_all()[0]
    assert first["path"] != second["path"] and first["archive_id"] != second["archive_id"]
    assert len(RawArchive.read_verified(first["path"], first["sha256"])) == 1
    assert len(RawArchive.read_verified(second["path"], second["sha256"])) == 2


def metrics_csv(indices):
    from datetime import UTC, datetime, timedelta

    day = datetime(2026, 1, 1, tzinfo=UTC)
    return "create_time,symbol,sum_open_interest,sum_open_interest_value\n" + "\n".join(
        f"{(day + timedelta(minutes=index * 5)).strftime('%Y-%m-%d %H:%M:%S')},BTCUSDT,{ordinal},100"
        for ordinal, index in enumerate(indices)
    )


def test_metrics_permuted_full_day_canonicalizes_only_memory_and_records_provenance():
    csv_text = metrics_csv([(i * 37) % 288 for i in range(288)])
    payload, checksum = zipped(csv_text)
    original = bytes(payload)
    result = verify_binance_zip(payload, checksum, filename="test.zip", kind="metrics")
    assert result["complete"] and result["gaps"] == result["duplicates"] == []
    assert result["order_changed"] and result["normalization"]["rows_preserved"] == 288
    assert result["normalization"]["original_csv_sha256"] == hashlib.sha256(csv_text.encode()).hexdigest()
    assert result["rows"][0][0] == "2026-01-01 00:00:00" and result["rows"][-1][0] == "2026-01-01 23:55:00"
    assert payload == original and hashlib.sha256(payload).hexdigest() == checksum.split()[0]


def test_metrics_missing_bucket_remains_incomplete_after_stable_sort():
    payload, checksum = zipped(metrics_csv([i for i in reversed(range(288)) if i != 123]))
    result = verify_binance_zip(payload, checksum, filename="test.zip", kind="metrics")
    assert not result["complete"] and result["order_changed"]
    assert {"after": 1767225600000 + 122 * 300000, "before": 1767225600000 + 124 * 300000} in result["gaps"]
    assert {"expected_rows": 288, "actual_rows": 287} in result["gaps"]
    assert result["duplicates"] == []


def test_metrics_separated_duplicate_is_detected_and_stable_values_are_not_dropped():
    indices = list(range(288))
    indices[200] = 0
    payload, checksum = zipped(metrics_csv(indices))
    result = verify_binance_zip(payload, checksum, filename="test.zip", kind="metrics")
    assert not result["complete"] and result["order_changed"]
    assert result["duplicates"] == [1767225600000]
    assert len(result["rows"]) == 288
    assert [row[2] for row in result["rows"][:2]] == ["0", "200"]
    assert {"after": 1767225600000 + 199 * 300000, "before": 1767225600000 + 201 * 300000} in result["gaps"]


def test_non_metrics_reordering_is_not_silently_normalized():
    payload, checksum = zipped("2,1,2,3,4,1767225600001,false\n1,1,2,3,4,1767225600000,true\n")
    result = verify_binance_zip(payload, checksum, filename="test.zip", kind="aggTrades")
    assert not result["complete"] and result["rows"][0][0] == "2"
    assert "order_changed" not in result and "normalization" not in result
