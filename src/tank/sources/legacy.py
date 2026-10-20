"""Metadata-only legacy source boundary, proven from retained provider bytes."""

import gzip
import json
from copy import deepcopy
from pathlib import Path

import feedparser
import zstandard
from selectolax.parser import HTMLParser

from tank.time_precision import Timestamp, collection_clock
from tank.util import parse_time, sha256

from . import parsers

SOURCE_METADATA_CONTRACT_VERSION = 1
PUBLICATION_FIELDS = {
    "published_prec",
    "published_prec_reason",
    "published_at_precision",
    "published_at_precision_reason",
    "published_at_bucket_size_s",
    "published_at_reason",
    "published_at_missing_reason",
    "published_at_basis",
}

SOURCE_UPDATED_FIELDS = {
    "source_updated_at",
    "source_updated_at_prec",
    "source_updated_at_prec_reason",
    "source_updated_at_precision",
    "source_updated_at_precision_reason",
    "source_updated_at_bucket_size_s",
}


def needs_source_metadata_repair(record):
    receipt = record.get("source_metadata_repair", {})
    if (
        receipt.get("contract_version") == SOURCE_METADATA_CONTRACT_VERSION
        and receipt.get("outcome") == "VERIFIED"
    ):
        return False
    if receipt.get("outcome") == "UNVERIFIABLE":
        return True
    return record.get("published_at_precision") in {"SECOND", "DAY", "HOUR", "UNKNOWN"} or (
        record.get("published_prec") == "BUCKET" and not record.get("published_at_bucket_size_s")
    )


def normalize_legacy_source_record(record, payload, *, feed, raw_ref, raw_sha256, converted_at=None):
    """Return a copy; unproven values survive with explicit unknown precision.

    No legacy precision alias itself proves source semantics. This function never
    walks nested semantic records or reparses text into the returned document.
    """
    result = deepcopy(record)
    prior = result.get("source_metadata_repair", {})
    if (
        prior.get("contract_version") == SOURCE_METADATA_CONTRACT_VERSION
        and prior.get("outcome") == "VERIFIED"
    ):
        return result
    original_fields = {
        k: deepcopy(v) for k, v in record.items() if k in PUBLICATION_FIELDS | {"published_at"}
    }
    reason = None
    proof = None
    updated_misassigned = False
    raw_updated = None
    sid = record.get("source_id")
    try:
        declared_pairs = [
            (record.get("raw_ref"), record.get("raw_sha256")),
            (record.get("detail_raw_ref"), record.get("detail_raw_sha256")),
            (record.get("discovery_raw_ref"), record.get("discovery_raw_sha256")),
        ]
        if not raw_ref or not raw_sha256 or (raw_ref, raw_sha256) not in declared_pairs:
            raise ValueError("RAW_PROVENANCE_PAIR_NOT_DECLARED_BY_RECORD")
        if not isinstance(payload, bytes) or sha256(payload) != raw_sha256:
            raise ValueError("RAW_SHA_MISMATCH")
        if record.get("feed") != feed.get("url"):
            raise ValueError("CONFIGURED_FEED_IDENTITY_MISMATCH")
        data = None
        if sid == "S09":
            data = json.loads(payload)
        if sid == "S09" and isinstance(data.get("data"), dict) and data["data"].get("code"):
            detail = data["data"]
            rows = [{"external_id": detail["code"], **parsers.timestamp_fields(detail.get("publishDate"))}]
            contract = "BINANCE_CMS_DETAIL_PUBLISH_DATE"
        elif feed.get("kind") in {"rss", "atom"}:
            rows = parsers.feeds(payload, sid, feed)
            contract = "RSS_ATOM_ORIGINAL_PUBLISHED_FIELD"
        elif feed.get("kind") == "html":
            canonical = {
                node.attributes.get("href")
                for node in HTMLParser(payload).css('link[rel="canonical"]')
                if node.attributes.get("href")
            }
            if (
                len(canonical) != 1
                or next(iter(canonical)) != record.get("external_id")
                or record.get("url") != record.get("external_id")
            ):
                raise ValueError("CANONICAL_ARTICLE_IDENTITY_NOT_UNIQUELY_PROVEN")
            rows = [{"external_id": next(iter(canonical)), **parsers.article(payload, source_id=sid)}]
            contract = "EXPLICIT_CANONICAL_ARTICLE_PUBLICATION_FIELD"
        elif feed.get("kind") in {"json", "graphql", "dtcc_search"}:
            rows = parsers.json_items(payload, sid)
            contract = "CONFIGURED_SOURCE_JSON_PUBLICATION_FIELD"
        else:
            raise ValueError("UNSUPPORTED_PROVIDER_IDENTITY_PROOF")
        matched = [row for row in rows if str(row.get("external_id")) == str(record.get("external_id"))]
        if len(matched) != 1:
            raise ValueError("EXTERNAL_ID_NOT_UNIQUELY_PROVEN")
        proof = matched[0]
        if (
            sid == "S25"
            and feed.get("kind") == "atom"
            and record.get("published_at_precision") == "SECOND"
            and proof.get("published_at") is None
            and record.get("published_at") is not None
            and proof.get("source_updated_at") is not None
            and parse_time(proof["source_updated_at"]) == parse_time(record["published_at"])
        ):
            originals = [
                e
                for e in feedparser.parse(payload).entries
                if str(e.get("id") or e.get("guid") or e.get("link")) == str(record["external_id"])
            ]
            if len(originals) != 1 or originals[0].get("published") or not originals[0].get("updated"):
                raise ValueError("LEGACY_UPDATED_FIELD_ASSIGNMENT_NOT_PROVEN")
            raw_updated = originals[0]["updated"]
            updated_misassigned = True
            proof = {
                **proof,
                **Timestamp(None, "COLLECTION_ONLY", "SOURCE_DID_NOT_PROVIDE_TIME").fields("published_at"),
            }
            contract = "LEGACY_UPDATED_MISASSIGNED_AS_PUBLICATION"
        else:
            if proof.get("published_at") is None or record.get("published_at") is None:
                raise ValueError("ORIGINAL_PUBLICATION_NOT_PROVEN")
            if parse_time(proof["published_at"]) != parse_time(record["published_at"]):
                raise ValueError("PROVIDER_PUBLICATION_VALUE_MISMATCH")
            precision = proof.get("published_at_precision")
            if precision != "EXACT" and not str(precision).startswith("BUCKET("):
                raise ValueError("PROVIDER_PUBLICATION_PRECISION_UNVERIFIED")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        reason = str(exc)
    for key in PUBLICATION_FIELDS:
        result.pop(key, None)
    if reason is None:
        result.update({k: deepcopy(v) for k, v in proof.items() if k in PUBLICATION_FIELDS})
        result["published_at_missing_reason"] = "SOURCE_DID_NOT_PROVIDE_TIME" if updated_misassigned else None
        if updated_misassigned:
            for key in SOURCE_UPDATED_FIELDS:
                result.pop(key, None)
            result.update({k: deepcopy(v) for k, v in proof.items() if k in SOURCE_UPDATED_FIELDS})
    else:
        result.update(
            Timestamp(record.get("published_at"), None, "UNVERIFIED_SEMANTICS").fields("published_at")
        )
        result["published_at_missing_reason"] = "UNVERIFIED_SEMANTICS"
    # Only the proven S25 wrong-field assignment may remove a publication value.
    result["published_at"] = None if updated_misassigned else record.get("published_at")
    result["source_metadata_repair"] = {
        "contract_version": SOURCE_METADATA_CONTRACT_VERSION,
        "outcome": "UNVERIFIABLE" if reason else "VERIFIED",
        "reason": reason,
        "original_publication_fields": original_fields,
        "original_source_updated_fields": {
            k: deepcopy(v) for k, v in record.items() if k in SOURCE_UPDATED_FIELDS
        }
        if updated_misassigned
        else None,
        "proven_raw_updated_field": raw_updated,
        "original_record_sha256": sha256(record),
        "source_id": sid,
        "external_id": record.get("external_id"),
        "feed_url": feed.get("url"),
        "raw_ref": raw_ref,
        "raw_sha256": raw_sha256,
        "parser_sha256": sha256(Path(parsers.__file__).read_bytes()),
        "provider_contract": contract if reason is None else None,
        **collection_clock(converted_at).fields("converted_at"),
    }
    return result


def source_metadata_at(record, at):
    """Restore only original publication fields before metadata conversion."""
    result = deepcopy(record)
    receipt = result.get("source_metadata_repair")
    if receipt and parse_time(at) < parse_time(receipt["converted_at"]):
        for key in PUBLICATION_FIELDS | {"published_at"}:
            result.pop(key, None)
        result.update(deepcopy(receipt["original_publication_fields"]))
        if receipt.get("provider_contract") == "LEGACY_UPDATED_MISASSIGNED_AS_PUBLICATION":
            for key in SOURCE_UPDATED_FIELDS:
                result.pop(key, None)
            result.update(deepcopy(receipt.get("original_source_updated_fields") or {}))
        result.pop("source_metadata_repair", None)
    return result


def retained_source_payload(raw_ref):
    """Read original transport bytes without changing the retained artifact."""
    compressed = Path(raw_ref).read_bytes()
    if compressed.startswith(b"\x28\xb5\x2f\xfd"):
        return zstandard.ZstdDecompressor().decompress(compressed)
    if compressed.startswith(b"\x1f\x8b"):
        return gzip.decompress(compressed)
    return compressed


def select_source_metadata_raw(record, feed):
    """Select one declared proof representation; never search or fetch alternatives."""
    if record.get("source_id") == "S09" and record.get("detail_raw_ref") and record.get("detail_raw_sha256"):
        return record["detail_raw_ref"], record["detail_raw_sha256"]
    if feed.get("kind") != "html" and record.get("discovery_raw_ref") and record.get("discovery_raw_sha256"):
        return record["discovery_raw_ref"], record["discovery_raw_sha256"]
    return record.get("raw_ref"), record.get("raw_sha256")
