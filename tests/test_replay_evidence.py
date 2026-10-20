from types import SimpleNamespace

import pytest

from tank.market.archive import TradeArchive
from tank.replay_evidence import ReplayEvidenceError, seal_replay_evidence


def test_sealed_empty_run_binds_cutoff_and_rejects_reuse(tmp_path):
    inputs = SimpleNamespace(
        assembled_at="2099-01-01T00:00:00Z", cutoff="2026-10-18T12:00:00Z", config={}, _records=[]
    )
    result = seal_replay_evidence(inputs, "RPL-test", tmp_path / "RPL-test", input_sha256="a" * 64)
    assert result["manifest"]["files"] == []
    assert result["manifest"]["cutoff"] == inputs.cutoff
    assert result["manifest"]["cutoff_prec"] is None
    assert result["manifest"]["cutoff_prec_reason"]
    assert (tmp_path / "RPL-test.source-audit.json").exists()
    with pytest.raises(ReplayEvidenceError, match="immutable"):
        seal_replay_evidence(inputs, "RPL-test", tmp_path / "RPL-test", input_sha256="a" * 64)


def test_sealed_native_zero_trade_proof_survives(tmp_path):
    import hashlib

    import pyarrow as pa
    import pyarrow.parquet as pq

    original = tmp_path / "empty.parquet"
    pq.write_table(
        pa.table(
            {
                "instrument_id": pa.array([], type=pa.string()),
                "trade_id": pa.array([], type=pa.string()),
                "ts": pa.array([], type=pa.string()),
            }
        ),
        original,
    )
    inputs = SimpleNamespace(
        assembled_at="2099-01-01T00:00:00Z",
        cutoff="2026-10-18T12:00:17Z",
        config={},
        _records=[
            {
                "collection": "mkt.raw_archives",
                "record": {
                    "kind": "NORMALIZED_TRADES",
                    "instrument_id": "I",
                    "path": str(original),
                    "sha256": hashlib.sha256(original.read_bytes()).hexdigest(),
                    "integrity": {
                        "coverage": [
                            {
                                "start": "2026-10-18T12:00:00Z",
                                "end": "2026-10-18T12:01:00Z",
                                "complete": True,
                                "observed_at": "2026-10-18T11:59:00Z",
                            }
                        ]
                    },
                },
            }
        ],
    )
    result = seal_replay_evidence(inputs, "RPL-zero", tmp_path / "RPL-zero", input_sha256="b" * 64)
    entry = result["manifest"]["files"][0]
    assert entry["row_count"] == 0
    assert entry["coverage"][0]["end"] == inputs.cutoff
    assert TradeArchive.read_verified(result["rebuild_manifests"][0]["path"], entry["sha256"]) == []


def test_sealed_evidence_refuses_path_escape(tmp_path):
    inputs = SimpleNamespace(
        assembled_at="2099-01-01T00:00:00Z", cutoff="2026-10-18T12:00:00Z", config={}, _records=[]
    )
    with pytest.raises(ReplayEvidenceError, match="identity"):
        seal_replay_evidence(inputs, "../escape", tmp_path / "out", input_sha256="a" * 64)


@pytest.mark.asyncio
async def test_raw_projection_rebuilds_production_snapshot_without_future_trade(tmp_path):
    from datetime import timedelta
    from decimal import Decimal

    from tank.market import normalize
    from tank.replay import ReplayMarketReader
    from tank.util import parse_time

    instrument = {
        "instrument_id": "binance:spot:ABCUSD",
        "token_id": "test:ABC",
        "venue": "binance",
        "market_type": "SPOT",
        "quote_asset": "USD",
        "contract_multiplier": "1",
    }
    minute = parse_time("2026-10-17T12:00:00Z")
    raw = TradeArchive(tmp_path / "original")
    for tid, second in [(1, 10), (2, 50)]:
        raw.append(
            normalize.trade(
                instrument,
                {
                    "a": tid,
                    "p": str(tid),
                    "q": "1",
                    "m": False,
                    "T": int((minute + timedelta(seconds=second)).timestamp() * 1000),
                },
            )
        )
    raw.coverage(instrument["instrument_id"], minute, minute + timedelta(minutes=2), complete=True)
    archive = raw.flush(seal_before=minute + timedelta(hours=1))[0]
    cutoff = "2026-10-17T12:00:17Z"
    inputs = SimpleNamespace(
        assembled_at="2099-01-01T00:00:00Z",
        cutoff=cutoff,
        config={},
        registry={"instruments": [instrument]},
        _records=[{"collection": "mkt.raw_archives", "record": archive}],
    )
    inputs.records = lambda *args, **kwargs: [
        {
            "token_id": "test:ABC",
            "cutoff": cutoff,
            "flow_bars": [],
            "flow_segments": [],
            "reference_points": [],
        }
    ]
    sealed = seal_replay_evidence(
        inputs, "RPL-prefix", tmp_path / "sealed" / "RPL-prefix", input_sha256="c" * 64
    )
    entry = sealed["manifest"]["files"][0]
    assert entry["row_count"] == 1
    output = await ReplayMarketReader(inputs, sealed).snapshot("test:ABC", cutoff)
    assert all(
        parse_time(row["minute_start"]) + timedelta(minutes=1) <= parse_time(cutoff)
        for row in output["flow_bars"]
    )
    assert any(Decimal(segment["buy_qty"]) == 1 for segment in output["flow_segments"])
    assert output["replay_raw_provenance"]


def test_same_guest_directory_exposes_only_each_message_prefix(tmp_path):
    import json
    from datetime import timedelta

    from tank.market import normalize
    from tank.replay_evidence import ReplayEvidenceView
    from tank.time_precision import propagate_timestamp
    from tank.util import parse_time

    instrument = {
        "instrument_id": "binance:spot:ABCUSD",
        "token_id": "test:ABC",
        "venue": "binance",
        "market_type": "SPOT",
        "quote_asset": "USD",
        "contract_multiplier": "1",
    }
    start = parse_time("2026-10-17T12:00:00Z")
    archive = TradeArchive(tmp_path / "private-source")
    for tid, second in [(1, -10), (2, 30)]:
        archive.append(
            normalize.trade(
                instrument,
                {
                    "a": tid,
                    "p": "1",
                    "q": "1",
                    "m": False,
                    "T": int((start + timedelta(seconds=second)).timestamp() * 1000),
                },
            )
        )
    manifests = archive.flush(seal_before=start + timedelta(hours=2))
    inputs = SimpleNamespace(
        assembled_at="2099-01-01T00:00:00Z",
        cutoff="2026-10-17T12:01:00Z",
        config={},
        _records=[{"collection": "mkt.raw_archives", "record": row} for row in manifests],
    )
    visible = tmp_path / "guest" / "RPL-prefix-two"
    view = ReplayEvidenceView(
        inputs, "RPL-prefix-two", visible, tmp_path / "private-projections", input_sha256="d" * 64
    )

    def read_guest():
        manifest = json.loads((visible / "manifest.json").read_text())
        rows = []
        for entry in manifest["files"]:
            rows.extend(TradeArchive.read_verified(visible / entry["relative_path"], entry["sha256"]))
        assert {path.name for path in visible.iterdir()} == {
            "manifest.json",
            *[entry["relative_path"] for entry in manifest["files"]],
        }
        return rows

    first = view.advance(propagate_timestamp({"at": "2026-10-17T12:00:00Z"}, "at"))
    inode = visible.stat().st_ino
    assert len(read_guest()) == 1
    first_hash = first["manifest_sha256"]
    view.advance(propagate_timestamp({"at": inputs.cutoff}, "at"))
    assert visible.stat().st_ino == inode
    assert len(read_guest()) == 2
    assert view.capture["manifest_sha256"] != first_hash
    assert len(view.receipts) == 2


def test_as_of_raw_projection_waits_for_actual_receipt(tmp_path):
    import hashlib

    import pyarrow as pa
    import pyarrow.parquet as pq

    from tank.replay_evidence import ReplayEvidenceView
    from tank.time_precision import propagate_timestamp

    source = tmp_path / "source.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "instrument_id": "I",
                    "trade_id": "late",
                    "ts": "2026-10-18T11:59:50Z",
                    "received_ts": "2026-10-18T12:00:30Z",
                }
            ]
        ),
        source,
    )
    inputs = SimpleNamespace(
        cutoff="2026-10-18T12:01:00Z",
        assembled_at=None,
        config={},
        _records=[
            {
                "collection": "mkt.raw_archives",
                "available_at": "2026-10-18T12:00:30Z",
                "record": {
                    "kind": "NORMALIZED_TRADES",
                    "instrument_id": "I",
                    "path": str(source),
                    "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                },
            }
        ],
    )
    view = ReplayEvidenceView(
        inputs, "RPL-receipt", tmp_path / "guest", tmp_path / "private", input_sha256="e" * 64
    )
    early = view.advance(propagate_timestamp({"at": "2026-10-18T12:00:00Z"}, "at"))
    assert early["manifest"]["files"][0]["row_count"] == 0
    late = view.advance(propagate_timestamp({"at": inputs.cutoff}, "at"))
    assert late["manifest"]["files"][0]["row_count"] == 1
