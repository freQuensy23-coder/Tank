"""Seal cutoff-bounded immutable print evidence from validated packet projections."""

import hashlib
import os
import re
import shutil
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.replay_evidence_contracts import (
    EvidenceFile,
    EvidenceManifest,
    EvidenceSource,
    PrefixReceipt,
    SealedReplayEvidence,
)
from tank.time_precision import Timestamp, collection_clock, propagate_timestamp
from tank.util import canonical_json, parse_time, sha256

type BucketAnchor = Literal["START", "END", "UNSPECIFIED"]


class ReplayEvidenceError(ValueError):
    pass


class ReplayEvidenceInputs(Protocol):
    @property
    def cutoff(self) -> str: ...

    @property
    def assembled_at(self) -> str | None: ...

    @property
    def _records(self) -> Sequence[Mapping[str, JsonValue]]: ...


class CapturedPrint(DomainModel):
    instrument_id: str = Field(min_length=1)
    trade_id: str = Field(min_length=1)
    timestamp_spelling: str
    market_at: datetime
    received_at: datetime
    canonical_row: str


class PacketPrint(DomainModel):
    record_key: str
    record_sha256: str
    source: EvidenceSource
    print: CapturedPrint


def _text(row: Mapping[str, JsonValue], field: str) -> str:
    value = row[field]
    if not isinstance(value, str) or not value:
        raise ReplayEvidenceError(f"Replay print requires a nonempty {field}")
    return value


def _optional_text(row: Mapping[str, JsonValue], field: str) -> str | None:
    if field not in row or row[field] is None:
        return None
    return _text(row, field)


def _packet_prints(inputs: ReplayEvidenceInputs, input_sha256: str) -> list[PacketPrint]:
    """Decode only identity/availability semantics; preserve complete source bytes as evidence."""
    result = []
    for packet in inputs._records:
        if packet["collection"] != "mkt.trades":
            continue
        row = packet["record"]
        if not isinstance(row, dict):
            raise ReplayEvidenceError("Captured raw trade must be an object")
        instrument_id = _text(row, "instrument_id")
        raw_id = row["trade_id"]
        if isinstance(raw_id, bool) or not isinstance(raw_id, (str, int)) or str(raw_id) == "":
            raise ReplayEvidenceError("Captured raw trade lacks canonical trade identity")
        timestamp = _text(row, "ts")
        available = _optional_text(packet, "available_at")
        receipts = [
            parse_time(_text(row, field))
            for field in ("received_ts", "received_at", "collected_at", "available_at")
            if field in row and row[field] is not None
        ]
        if available is not None:
            receipts.append(parse_time(available))
        if not receipts:
            raise ReplayEvidenceError("Raw replay trade lacks captured receipt/availability provenance")
        key = _text(packet, "key")
        result.append(
            PacketPrint(
                record_key=key,
                record_sha256=sha256(dict(packet)),
                source=EvidenceSource(
                    source_ref="packet:" + input_sha256 + "#" + key,
                    source_sha256=sha256(dict(packet)),
                    instrument_id=instrument_id,
                    origin_kind="PACKET_PRINTS",
                    available_at=available,
                ),
                print=CapturedPrint(
                    instrument_id=instrument_id,
                    trade_id=str(raw_id),
                    timestamp_spelling=timestamp,
                    market_at=parse_time(timestamp),
                    received_at=max(receipts),
                    canonical_row=canonical_json(row),
                ),
            )
        )
    return result


def _bucket_anchor(timestamp: Timestamp) -> BucketAnchor | None:
    match timestamp.bucket_anchor:
        case None:
            return None
        case "START":
            return "START"
        case "END":
            return "END"
        case "UNSPECIFIED":
            return "UNSPECIFIED"
    raise ReplayEvidenceError("Unsupported timestamp bucket anchor")


def _value(timestamp: Timestamp) -> str:
    if timestamp.value is None:
        raise ReplayEvidenceError("Replay evidence requires a declared cutoff timestamp")
    return timestamp.value


def _durable_file(path: Path, content: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())


def seal_replay_evidence(
    inputs: ReplayEvidenceInputs,
    run_id: str,
    destination: str | Path,
    *,
    input_sha256: str,
    cutoff: Timestamp | None = None,
) -> SealedReplayEvidence:
    """Seal canonical source bytes, respecting market cutoff and captured receipt availability."""
    selected = propagate_timestamp({"cutoff": inputs.cutoff}, "cutoff") if cutoff is None else cutoff
    if not isinstance(selected, Timestamp):
        raise ReplayEvidenceError("Replay evidence requires a declared cutoff timestamp")
    end = parse_time(_value(selected))
    packet_cutoff = parse_time(inputs.cutoff)
    assembled = parse_time(inputs.assembled_at) if inputs.assembled_at is not None else None
    if end > packet_cutoff:
        raise ReplayEvidenceError("Evidence projection exceeds packet cutoff")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ReplayEvidenceError("Invalid replay run identity")
    candidate = Path(destination)
    if any(part.is_symlink() for part in (candidate, *candidate.parents)):
        raise ReplayEvidenceError("Replay evidence destination cannot traverse symlinks")
    root = candidate.resolve()
    if root.exists() and any(root.iterdir()):
        raise ReplayEvidenceError("Sealed replay destination must be new; prior artifacts remain immutable")
    packets = _packet_prints(inputs, input_sha256)
    root.mkdir(parents=True, exist_ok=True)
    groups: dict[str, dict[str, CapturedPrint]] = defaultdict(dict)
    provenance: dict[str, list[EvidenceSource]] = defaultdict(list)
    audit: list[JsonValue] = []
    for packet in packets:
        print_row = packet.print
        iid = print_row.instrument_id
        groups[iid]
        if packet.source not in provenance[iid]:
            provenance[iid].append(packet.source)
        audit.append({"record_key": packet.record_key, "record_sha256": packet.record_sha256})
        if print_row.market_at >= end:
            continue
        historical = (
            print_row.received_at > packet_cutoff
            and assembled is not None
            and print_row.received_at <= assembled
        )
        if print_row.received_at > end and not historical:
            continue
        existing = groups[iid].get(print_row.trade_id)
        if existing is not None and existing.canonical_row != print_row.canonical_row:
            raise ReplayEvidenceError("Conflicting normalized rows share a captured trade identity")
        groups[iid][print_row.trade_id] = print_row
    files = []
    for iid in sorted(groups):
        rows = sorted(groups[iid].values(), key=lambda row: (row.timestamp_spelling, row.trade_id))
        content = "".join(row.canonical_row + "\n" for row in rows).encode()
        digest = hashlib.sha256(content).hexdigest()
        relative = digest + ".jsonl"
        path = root / relative
        if not path.exists():
            _durable_file(path, content)
        if path.read_bytes() != content:
            raise ReplayEvidenceError("Sealed print file changed normalized source values")
        sources = provenance[iid]
        files.append(
            EvidenceFile(
                relative_path=relative,
                sha256=digest,
                byte_length=len(content),
                instrument_id=iid,
                row_count=len(rows),
                source_ref=sources[0].source_ref,
                source_sha256=sources[0].source_sha256,
                source_refs=sources,
                guest_path="/replay/evidence/" + relative,
                projection_cutoff=_value(selected),
                projection_cutoff_prec=selected.precision,
                projection_cutoff_prec_reason=selected.precision_reason,
                projection_cutoff_bucket_anchor=_bucket_anchor(selected),
            )
        )
    sealed = collection_clock()
    manifest = EvidenceManifest(
        run_id=run_id,
        input_sha256=input_sha256,
        cutoff=_value(selected),
        cutoff_prec=selected.precision,
        cutoff_prec_reason=selected.precision_reason,
        cutoff_bucket_anchor=_bucket_anchor(selected),
        files=files,
        sealed_at=_value(sealed),
        sealed_at_prec="COLLECTION_ONLY",
        sealed_at_prec_reason=sealed.precision_reason,
    )
    encoded = (canonical_json(manifest.to_wire()) + "\n").encode()
    _durable_file(root / "manifest.json", encoded)
    folder = os.open(root, os.O_RDONLY)
    try:
        os.fsync(folder)
    finally:
        os.close(folder)
    _durable_file(root.parent / (run_id + ".source-audit.json"), (canonical_json(audit) + "\n").encode())
    return SealedReplayEvidence(
        manifest=manifest, manifest_sha256=hashlib.sha256(encoded).hexdigest(), sealed_root=str(root)
    )


class ReplayEvidenceView:
    """Host-controlled monotonic prefixes; manifest publication is the guest commit point."""

    def __init__(
        self,
        inputs: ReplayEvidenceInputs,
        run_id: str,
        destination: str | Path,
        private_root: str | Path,
        *,
        input_sha256: str,
    ) -> None:
        self.inputs, self.run_id = inputs, run_id
        self.destination, self.private_root = Path(destination), Path(private_root)
        self.input_sha256 = input_sha256
        self.current: Timestamp | None = None
        self._capture: SealedReplayEvidence | None = None
        self.receipts: list[PrefixReceipt] = []

    @property
    def capture(self) -> SealedReplayEvidence:
        if self._capture is None:
            raise ReplayEvidenceError("Replay prefix has not been sealed")
        return self._capture

    def advance(self, cutoff: Timestamp) -> SealedReplayEvidence:
        if self.current is not None and parse_time(_value(cutoff)) < parse_time(_value(self.current)):
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
        expected = {entry.relative_path for entry in sealed.manifest.files}
        for entry in sealed.manifest.files:
            target = root / entry.relative_path
            if not target.exists():
                staging = root / (entry.relative_path + ".staging")
                shutil.copyfile(generation / entry.relative_path, staging)
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
        self._capture = SealedReplayEvidence(
            manifest=sealed.manifest, manifest_sha256=sealed.manifest_sha256, sealed_root=str(root)
        )
        self.current = cutoff
        self.receipts.append(
            PrefixReceipt(
                manifest_sha256=sealed.manifest_sha256,
                cutoff=_value(cutoff),
                cutoff_prec=cutoff.precision,
                cutoff_prec_reason=cutoff.precision_reason,
                cutoff_bucket_anchor=_bucket_anchor(cutoff),
            )
        )
        return self.capture
