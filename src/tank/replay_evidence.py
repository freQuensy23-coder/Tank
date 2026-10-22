"""Seal cutoff-bounded, run-only print evidence from the replay packet (mkt.trades rows)."""

import hashlib
import json
import os
import re
from collections import defaultdict
from pathlib import Path

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
    """Return a verified sealed manifest of the packet's prints before the cutoff.

    Each instrument's prints are one canonical JSON-lines file named by its sha256.
    Guest files contain only prints strictly before the requested cutoff and received
    by then (or historical receipts within the packet's assembly).
    """
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

    for row in inputs._records:
        if row["collection"] != "mkt.trades":
            continue
        record = row["record"]
        add(
            [record],
            {
                "source_ref": "packet:" + input_sha256 + "#" + row["key"],
                "source_sha256": sha256(row),
                "instrument_id": record.get("instrument_id"),
                "origin_kind": "PACKET_PRINTS",
                **({"available_at": row["available_at"]} if row.get("available_at") else {}),
            },
        )
        host_audit.append({"record_key": row["key"], "record_sha256": sha256(row)})

    files = []
    for iid in sorted(groups):
        rows = sorted(groups[iid].values(), key=lambda row: (row["ts"], str(row["trade_id"])))
        content = "".join(canonical_json(row) + "\n" for row in rows).encode()
        digest = hashlib.sha256(content).hexdigest()
        relative = digest + ".jsonl"
        path = root / relative
        if not path.exists():
            _durable_file(path, content)
        if [json.loads(line) for line in path.read_bytes().splitlines()] != rows:
            raise ReplayEvidenceError("Sealed print file changed normalized source values")
        sources = provenance[iid]
        files.append(
            {
                "relative_path": relative,
                "sha256": digest,
                "byte_length": len(content),
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
        )
    manifest = {
        "schema": "tank.replay-evidence.v1",
        "run_id": run_id,
        "input_sha256": input_sha256,
        **cutoff.fields("cutoff"),
        "guest_root": "/replay/evidence",
        "files": files,
        "identity_key": ["instrument_id", "trade_id"],
        "format": "jsonl",
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
        sealed["sealed_root"] = str(root)
        self.capture.clear()
        self.capture.update(sealed)
        self.current = cutoff
        self.receipts.append({"manifest_sha256": sealed["manifest_sha256"], **cutoff.fields("cutoff")})
        return self.capture
