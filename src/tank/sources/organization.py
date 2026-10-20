"""Configured publisher authority with retained source/body identity proofs."""

import json
from copy import deepcopy
from pathlib import Path
from urllib.parse import urlparse

from selectolax.parser import HTMLParser

from tank.time_precision import collection_clock
from tank.util import sha256

from . import parsers
from .legacy import select_source_metadata_raw

ORGANIZATION_CONTRACT_VERSION = 2


def _retained_publisher_organization(source, item, feeds):
    """Issuing agency or repository publisher; never a contributor's employer."""
    if len(feeds) != 1:
        return None
    feed = feeds[0]
    endpoint = urlparse(str(feed.get("url") or ""))
    page = urlparse(str(item.get("url") or ""))
    if source.get("source_id") == "S03":
        if (
            feed.get("kind") != "json"
            or endpoint.scheme != "https"
            or endpoint.netloc not in {"www.federalregister.gov", "federalregister.gov"}
            or endpoint.path != "/api/v1/documents.json"
            or page.scheme != "https"
            or page.netloc not in {"www.federalregister.gov", "federalregister.gov"}
        ):
            return None
        agencies = item.get("agencies")
        if not isinstance(agencies, list) or not agencies:
            return None
        names = []
        for agency in agencies:
            if not isinstance(agency, dict):
                return None
            name = agency.get("name") or agency.get("raw_name")
            if not isinstance(name, str) or not name.strip():
                return None
            if name.strip().casefold() not in {value.casefold() for value in names}:
                names.append(name.strip())
        return "; ".join(names)
    if source.get("source_id") == "S25":
        parts = endpoint.path.strip("/").split("/")
        target = page.path.strip("/").split("/")
        if (
            feed.get("kind") != "atom"
            or feed.get("subtype") not in {"releases", "commits"}
            or endpoint.scheme != "https"
            or endpoint.netloc != "github.com"
            or endpoint.query
            or endpoint.fragment
            or page.scheme != "https"
            or page.netloc != "github.com"
            or len(parts) < 3
            or len(target) < 3
            or [part.casefold() for part in parts[:2]] != [part.casefold() for part in target[:2]]
            or parts[2] not in {"releases.atom", "commits"}
            or not parts[0]
            or not parts[1]
        ):
            return None
        return parts[0]
    return None


def resolve_author_organization(source, item, accounts=None):
    """The ingestion precedence; never infer authority from a display name."""
    if item.get("author_organization"):
        return item["author_organization"]
    feeds = [f for f in source.get("feeds", []) if f.get("url") == item.get("feed")]
    if len(feeds) > 1:
        raise ValueError("Ambiguous configured publisher feed")
    configured = (feeds[0].get("author_organization") if feeds else None) or source.get("author_organization")
    if configured:
        return configured
    publisher = _retained_publisher_organization(source, item, feeds)
    if publisher:
        return publisher
    if source.get("source_id") == "S29":
        account = (
            (item.get("monitored_author") or item.get("reposted_by") or item.get("author", ""))
            .lower()
            .lstrip("@")
        )
        return (accounts or {}).get(account, {}).get("entity_id")
    return None


def _body_hash(row):
    return sha256(
        " ".join((row.get("title", "") + " " + row.get("text", "") + " " + (row.get("quoted") or "")).split())
    )


def _verified_bytes(record, payloads, ref, digest):
    pairs = {
        (record.get(prefix + "raw_ref"), record.get(prefix + "raw_sha256"))
        for prefix in ("", "detail_", "discovery_", "article_")
    }
    if not ref or not digest or (ref, digest) not in pairs:
        raise ValueError("UNDECLARED_RAW_IDENTITY")
    payload = payloads.get(ref)
    if not isinstance(payload, bytes) or sha256(payload) != digest:
        raise ValueError("RAW_BODY_SHA_MISMATCH")
    return payload


def _canonical_article(payload, record):
    urls = {
        node.attributes.get("href")
        for node in HTMLParser(payload).css('link[rel="canonical"]')
        if node.attributes.get("href")
    }
    if len(urls) != 1 or next(iter(urls)) != record.get("url"):
        raise ValueError("ARTICLE_CANONICAL_IDENTITY_MISMATCH")
    return parsers.article(payload, source_id=record["source_id"])


def normalize_author_organization(record, *, sources, raw_payloads, normalized_at=None):
    """Return (record copy, external receipt); never alter text/clocks/other fields.

    Unknown authority or failed proof leaves the complete original untouched.
    Receipts stay outside SourceItem/model state so unchanged requests are reusable.
    """
    result = deepcopy(record)
    receipt = {
        "contract_version": ORGANIZATION_CONTRACT_VERSION,
        "original_record_sha256": sha256(record),
        "original_author_organization": {
            "present": "author_organization" in record,
            "value": record.get("author_organization"),
        },
        "source_id": record.get("source_id"),
        "external_id": record.get("external_id"),
        "parser_sha256": sha256(Path(parsers.__file__).read_bytes()),
        **collection_clock(normalized_at).fields("normalized_at"),
    }
    try:
        matches = [source for source in sources if source.get("source_id") == record.get("source_id")]
        if len(matches) != 1:
            raise ValueError("CONFIGURED_SOURCE_NOT_UNIQUE")
        source = matches[0]
        feeds = [feed for feed in source.get("feeds", []) if feed.get("url") == record.get("feed")]
        if len(feeds) != 1:
            raise ValueError("EXACT_CONFIGURED_FEED_NOT_UNIQUE")
        feed = feeds[0]
        receipt.update(source_config_sha256=sha256(source), feed_config_sha256=sha256(feed))
        if source["source_id"] == "S29":
            raise ValueError("PRIVATE_SOURCE_PROOF_NOT_IN_PUBLIC_SCOPE")
        ref, digest = select_source_metadata_raw(record, feed)
        payload = _verified_bytes(record, raw_payloads, ref, digest)
        if feed["kind"] in {"rss", "atom"}:
            rows = parsers.feeds(payload, record["source_id"], feed)
        elif feed["kind"] in {"json", "graphql", "dtcc_search"}:
            data = json.loads(payload)
            if (
                record["source_id"] == "S09"
                and isinstance(data.get("data"), dict)
                and data["data"].get("code")
            ):
                detail = data["data"]
                rows = [
                    {
                        "external_id": detail["code"],
                        "title": detail.get("title", ""),
                        "text": parsers.binance_body(detail.get("body", "")),
                        "url": record.get("url"),
                    }
                ]
            else:
                rows = parsers.json_items(payload, record["source_id"])
        elif feed["kind"] == "html":
            article = _canonical_article(payload, record)
            if record.get("external_id") != record.get("url"):
                raise ValueError("ARTICLE_EXTERNAL_IDENTITY_MISMATCH")
            rows = [{"external_id": record["external_id"], "url": record["url"], **article}]
        else:
            raise ValueError("UNSUPPORTED_SOURCE_IDENTITY_PROOF")
        identities = [row for row in rows if str(row.get("external_id")) == str(record.get("external_id"))]
        if len(identities) != 1:
            raise ValueError("SOURCE_ITEM_IDENTITY_NOT_UNIQUE")
        identity = identities[0]
        if _body_hash(record) != record.get("content_hash"):
            raise ValueError("SOURCE_CONTENT_HASH_MISMATCH")
        body_ref, body_digest = ref, digest
        if _body_hash(identity) != record["content_hash"]:
            # An enriched article is bound through the exact index link and its
            # own declared immutable raw pair, never through a title match.
            if identity.get("url") != record.get("url"):
                raise ValueError("DISCOVERY_ARTICLE_LINK_MISMATCH")
            body_ref, body_digest = (
                record.get("article_raw_ref") or record.get("raw_ref"),
                record.get("article_raw_sha256") or record.get("raw_sha256"),
            )
            article_payload = _verified_bytes(record, raw_payloads, body_ref, body_digest)
            article = _canonical_article(article_payload, record)
            if _body_hash(article) != record["content_hash"]:
                raise ValueError("RETAINED_ARTICLE_CONTENT_HASH_MISMATCH")
        organization_record = record
        if source["source_id"] in {"S03", "S25"}:
            if identity.get("url") != record.get("url"):
                raise ValueError("PUBLISHER_ITEM_URL_IDENTITY_MISMATCH")
            if source["source_id"] == "S03":
                if record.get("agencies") is not None and record["agencies"] != identity.get("agencies"):
                    raise ValueError("ISSUING_AGENCY_RAW_METADATA_MISMATCH")
                organization_record = {**record, "agencies": identity.get("agencies")}
            receipt["organization_basis"] = (
                "OFFICIAL_RAW_ISSUING_AGENCIES"
                if source["source_id"] == "S03"
                else "CONFIGURED_REPOSITORY_PUBLISHING_NAMESPACE_NOT_EMPLOYER"
            )
        organization = resolve_author_organization(source, organization_record)
        if organization and organization != record.get("author_organization"):
            result["author_organization"] = organization
        receipt.update(
            outcome="VERIFIED",
            reason=None,
            identity_raw_ref=ref,
            identity_raw_sha256=digest,
            body_raw_ref=body_ref,
            body_raw_sha256=body_digest,
            verified_content_hash=record["content_hash"],
            organization=organization,
        )
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        result = deepcopy(record)
        receipt.update(outcome="UNVERIFIABLE", reason=str(exc))
    receipt["changed_fields"] = ["author_organization"] if result != record else []
    receipt["normalized_record_sha256"] = sha256(result)
    return result, receipt
