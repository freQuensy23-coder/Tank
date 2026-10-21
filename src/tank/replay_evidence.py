"""Seal cutoff-bounded, run-only raw market evidence; never restore raw rows to SQL."""

import hashlib
import os
import re
from collections import defaultdict
from pathlib import Path

from tank.market.archive import TradeArchive
from tank.market_migration import read_verified_legacy_raw
from tank.time_precision import Timestamp, collection_clock, propagate_timestamp
from tank.util import canonical_json, parse_time, sha256


class ReplayEvidenceError(ValueError):
    pass


def _durable_file(path, content):
    with path.open("xb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())


def seal_replay_evidence(inputs, run_id, destination, *, input_sha256, cutoff=None):
    """Return a verified sealed manifest and native rebuild manifests.

    Legacy packet rows and migration exports are preserved as explicit Parquet
    derivations, not silently dropped or inserted into PostgreSQL. No coverage is
    invented for legacy rows. Guest files contain only trades strictly before the
    requested cutoff; original future-bearing files stay outside the sealed root.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    cutoff = cutoff or propagate_timestamp({"cutoff": inputs.cutoff}, "cutoff")
    if not isinstance(cutoff, Timestamp) or cutoff.value is None:
        raise ReplayEvidenceError("Replay evidence requires a declared cutoff timestamp")
    end = parse_time(cutoff.value)
    if end > parse_time(inputs.cutoff):
        raise ReplayEvidenceError("Evidence projection exceeds packet cutoff")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ReplayEvidenceError("Invalid replay run identity")
    candidate = Path(destination)
    if any(part.is_symlink() for part in (candidate, *candidate.parents)):
        raise ReplayEvidenceError("Replay evidence destination cannot traverse symlinks")
    root = candidate.resolve()
    if root.exists() and any(root.iterdir()):
        raise ReplayEvidenceError("Sealed replay destination must be new; prior artifacts remain immutable")
    root.mkdir(parents=True, exist_ok=True)
    groups, provenance, coverage = defaultdict(dict), defaultdict(list), defaultdict(list)
    host_audit = []

    def add(rows, source, proofs=()):
        source_iid = source.get("instrument_id")
        if not source_iid:
            raise ReplayEvidenceError("Captured evidence lacks instrument identity")
        groups[source_iid]
        if source not in provenance[source_iid]:
            provenance[source_iid].append(source)
        for row in rows:
            if not isinstance(row, dict) or not all(
                row.get(field) is not None for field in ("instrument_id", "trade_id", "ts")
            ):
                raise ReplayEvidenceError("Captured raw trade lacks its canonical identity or timestamp")
            iid = row["instrument_id"]
            if source not in provenance[iid]:
                provenance[iid].append(source)

            if parse_time(row["ts"]) >= end:
                continue
            receipts = [
                parse_time(row[field])
                for field in ("received_ts", "received_at", "collected_at", "available_at")
                if row.get(field)
            ]
            if source.get("available_at"):
                receipts.append(parse_time(source["available_at"]))
            if not receipts:
                raise ReplayEvidenceError("Raw replay trade lacks captured receipt/availability provenance")
            received = max(receipts)
            assembled = getattr(inputs, "assembled_at", None)
            historical = (
                received > parse_time(inputs.cutoff)
                and assembled is not None
                and received <= parse_time(assembled)
            )
            if received > end and not historical:
                continue
            identity = str(row["trade_id"])
            existing = groups[iid].get(identity)
            if existing is not None and canonical_json(existing) != canonical_json(row):
                raise ReplayEvidenceError("Conflicting normalized rows share a captured trade identity")
            groups[iid][identity] = row
        for proof in proofs:
            proof_receipt = proof.get("observed_at") or source.get("available_at")
            if not proof_receipt:
                continue
            received = parse_time(proof_receipt)
            assembled = getattr(inputs, "assembled_at", None)
            historical = (
                received > parse_time(inputs.cutoff)
                and assembled is not None
                and received <= parse_time(assembled)
            )
            if received > end and not historical:
                continue
            lo, hi = parse_time(proof["start"]), min(parse_time(proof["end"]), end)
            if lo >= hi:
                continue
            clipped = dict(proof)

            if parse_time(proof["end"]) > end:
                clipped.update(cutoff.fields("end"))
            coverage[source["instrument_id"]].append(clipped)

    native = [
        dict(row["record"], replay_available_at=row.get("available_at"))
        for row in inputs._records
        if row["collection"] == "mkt.raw_archives" and row["record"].get("kind") == "NORMALIZED_TRADES"
    ]
    native += inputs.config.get("replay_trade_archives", [])
    for manifest in native:
        prefix = manifest.get("byte_length") if manifest.get("checksum_scope") == "JOURNAL_PREFIX" else None
        rows = TradeArchive.read_verified(manifest["path"], manifest["sha256"], byte_length=prefix)
        source = {
            "source_ref": manifest["path"],
            "source_sha256": manifest["sha256"],
            "instrument_id": manifest["instrument_id"],
            "origin_kind": "NORMALIZED_TRADES",
            **(
                {"available_at": manifest["replay_available_at"]}
                if manifest.get("replay_available_at")
                else {}
            ),
        }
        add(rows, source, TradeArchive.validated_coverage(manifest, native))
        host_audit.append({"source_manifest": manifest, "verified_rows": len(rows)})

    legacy = [row for row in inputs._records if row["collection"] == "mkt.trades"]
    for row in legacy:
        record = row["record"]
        add(
            [record],
            {
                "source_ref": "packet:" + input_sha256 + "#" + row["key"],
                "source_sha256": sha256(row),
                "instrument_id": record.get("instrument_id"),
                "origin_kind": "LEGACY_PACKET_RAW_ROWS",
                **({"available_at": row["available_at"]} if row.get("available_at") else {}),
            },
        )
        host_audit.append({"legacy_record_key": row["key"], "record_sha256": sha256(row)})
    export = inputs.config.get("replay_legacy_raw_export")
    if export:
        for sql_row in read_verified_legacy_raw(export):
            record = sql_row.get("document")
            if not isinstance(record, dict):
                raise ReplayEvidenceError("Verified legacy SQL export lacks its original normalized document")
            add(
                [record],
                {
                    "source_ref": "legacy-sql:" + str(sql_row.get("storage_key")),
                    "source_sha256": sha256(sql_row),
                    "instrument_id": record.get("instrument_id"),
                    "origin_kind": "LEGACY_SQL_RAW_EVIDENCE_V1",
                },
            )
            host_audit.append(
                {"legacy_storage_key": sql_row.get("storage_key"), "row_sha256": sha256(sql_row)}
            )

    files, rebuild = [], []
    for iid in sorted(groups):
        rows = sorted(groups[iid].values(), key=lambda row: (row["ts"], str(row["trade_id"])))
        columns = sorted({key for row in rows for key in row})
        table = (
            pa.Table.from_pylist([{key: row.get(key) for key in columns} for row in rows])
            if rows
            else pa.table(
                {
                    "instrument_id": pa.array([], type=pa.string()),
                    "trade_id": pa.array([], type=pa.string()),
                    "ts": pa.array([], type=pa.string()),
                }
            )
        )
        temporary = root / "projection.staging.parquet"
        pq.write_table(table, temporary, compression="zstd")
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        relative = digest + ".parquet"
        path = root / relative
        temporary.replace(path)

        verified = TradeArchive.read_verified(path, digest)
        if canonical_json(verified) != canonical_json(rows):
            raise ReplayEvidenceError("Parquet projection changed normalized source values")
        sources = provenance[iid]
        entry = {
            "relative_path": relative,
            "sha256": digest,
            "byte_length": path.stat().st_size,
            "kind": "NORMALIZED_TRADES",
            "instrument_id": iid,
            "row_count": len(rows),
            "source_ref": sources[0]["source_ref"],
            "source_sha256": sources[0]["source_sha256"],
            "source_refs": sources,
            "guest_path": "/replay/evidence/" + relative,
            **cutoff.fields("projection_cutoff"),
            "coverage": coverage[iid],
        }
        files.append(entry)
        rebuild.append(
            {
                "archive_id": "REPLAY-" + digest,
                "path": str(path),
                "sha256": digest,
                "checksum_scope": "FILE",
                "kind": "NORMALIZED_TRADES",
                "instrument_id": iid,
                "row_count": len(rows),
                "byte_length": path.stat().st_size,
                "integrity": {"coverage": coverage[iid]},
                "replay_guest_path": entry["guest_path"],
                **cutoff.fields("projection_cutoff"),
            }
        )
    manifest = {
        "schema": "tank.replay-evidence.v1",
        "run_id": run_id,
        "input_sha256": input_sha256,
        **cutoff.fields("cutoff"),
        "guest_root": "/replay/evidence",
        "files": files,
        "identity_key": ["instrument_id", "trade_id"],
        "writer": {"library": "pyarrow", "version": pa.__version__, "compression": "zstd"},
        **collection_clock().fields("sealed_at"),
    }
    encoded = (canonical_json(manifest) + "\n").encode()
    _durable_file(root / "manifest.json", encoded)
    folder = os.open(root, os.O_RDONLY)
    try:
        os.fsync(folder)
    finally:
        os.close(folder)

    _durable_file(root.parent / (run_id + ".source-audit.json"), (canonical_json(host_audit) + "\n").encode())
    return {
        "manifest": manifest,
        "manifest_sha256": hashlib.sha256(encoded).hexdigest(),
        "sealed_root": str(root),
        "rebuild_manifests": rebuild,
    }


class ReplayEvidenceView:
    """Host-controlled monotonic prefix in one stable read-only guest mount.

    Immutable source projections/receipts stay outside the mounted child. New
    files contain only the selected prefix; publishing manifest last is the
    commit point. A concurrent controller validation can refuse a transition,
    but cannot observe a future prefix.
    """

    def __init__(self, inputs, run_id, destination, private_root, *, input_sha256):
        self.inputs, self.run_id = inputs, run_id
        self.destination, self.private_root = Path(destination), Path(private_root)
        self.input_sha256 = input_sha256
        self.current = None
        self.capture = {}
        self.receipts = []

    def advance(self, cutoff):
        import shutil

        if self.current and parse_time(cutoff.value) < parse_time(self.current.value):
            raise ReplayEvidenceError("Guest evidence prefix cannot move backwards")
        if self.current == cutoff:
            return self.capture
        generation = self.private_root / f"prefix-{len(self.receipts):06d}" / "view"
        sealed = seal_replay_evidence(
            self.inputs, self.run_id, generation, input_sha256=self.input_sha256, cutoff=cutoff
        )
        root = self.destination
        if root.is_symlink():
            raise ReplayEvidenceError("Guest prefix destination cannot be a symlink")
        root.mkdir(parents=True, exist_ok=True)
        expected = {entry["relative_path"] for entry in sealed["manifest"]["files"]}
        for entry in sealed["manifest"]["files"]:
            target = root / entry["relative_path"]
            if not target.exists():
                staging = root / (entry["relative_path"] + ".staging")
                shutil.copyfile(generation / entry["relative_path"], staging)
                with staging.open("rb") as handle:
                    os.fsync(handle.fileno())
                staging.replace(target)
        staging = root / "manifest.staging"
        _durable_file(staging, (generation / "manifest.json").read_bytes())
        staging.replace(root / "manifest.json")
        for path in root.iterdir():
            if path.name != "manifest.json" and path.name not in expected:
                path.unlink()
        descriptor = os.open(root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        for manifest in sealed["rebuild_manifests"]:
            manifest["path"] = str(root / Path(manifest["path"]).name)
        sealed["sealed_root"] = str(root)
        self.capture.clear()
        self.capture.update(sealed)
        self.current = cutoff
        self.receipts.append({"manifest_sha256": sealed["manifest_sha256"], **cutoff.fields("cutoff")})
        return self.capture
