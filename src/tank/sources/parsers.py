"""Source-specific wire parsers. No network side effects."""

import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from io import BytesIO
from math import isfinite
from typing import Self
from urllib.parse import urljoin, urlsplit

import feedparser
from pydantic import ConfigDict, Field, JsonValue, field_validator, model_validator
from selectolax.parser import HTMLParser

from tank.contracts import DomainModel
from tank.contracts.json import decode_unique_json
from tank.contracts.scalars import FalseFlag, OneInt, TrueFlag, ZeroInt
from tank.contracts.source_envelopes import bybit_announcement_success
from tank.contracts.source_errors import DigestEntryError, DocumentEntryError, EntryIdentity
from tank.contracts.source_items import DiscoveryEnvelope, discovered_item
from tank.contracts.source_parsing import ParsedDiscoveryBatch, PublisherEntryIdentity
from tank.contracts.sources import FeedParsingSpec
from tank.sources.instrument_adapters import parse_instrument_page
from tank.time_precision import Timestamp
from tank.util import EpochUnit, iso, parse_epoch, parse_utc_time, sha256


def text(html: str | bytes | None) -> str:
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    tree = HTMLParser(str(html or ""))
    for node in tree.css("script,style,nav,footer,header,noscript"):
        node.decompose()
    return " ".join(tree.text(separator=" ").split())


type PublicationInput = str | int | float | datetime | None


def published(value: PublicationInput, *, epoch_unit: EpochUnit | None = None) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, (int, float)):
            return iso(parse_epoch(value, unit=epoch_unit)) if epoch_unit is not None else None
        if isinstance(value, str):
            if re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]+)?", value):
                return iso(parse_epoch(value, unit=epoch_unit)) if epoch_unit is not None else None
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                return iso(parse_utc_time(value))
            try:
                instant = datetime.fromisoformat(value)
            except ValueError:
                instant = parsedate_to_datetime(value)
        elif isinstance(value, datetime):
            instant = value
        else:
            return None
        return iso(instant) if instant.tzinfo is not None else None
    except (ValueError, TypeError, OverflowError):
        return None


def preferred_publication(primary: PublicationInput, alternate: PublicationInput) -> PublicationInput:
    """Prefer the declared field while preserving a valid zero epoch value."""
    return primary if primary is not None else alternate


def timestamp_fields(
    value: PublicationInput,
    name: str = "published_at",
    *,
    date_only: bool = False,
    epoch_unit: EpochUnit | None = None,
) -> dict[str, str | int | None]:
    """Decode declared provider epoch units or calendar-day/offset-bearing publication semantics."""
    normalized = published(value, epoch_unit=epoch_unit)
    day = date_only or (isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is not None)
    precision = (
        "BUCKET(1d)"
        if normalized and day
        else "EXACT"
        if normalized
        else "COLLECTION_ONLY"
        if name == "published_at"
        else None
    )
    reason = None if normalized else "INCOMPLETE" if value is None else "UNVERIFIED_SEMANTICS"
    fields = Timestamp(normalized, precision, reason).fields(name)
    fields[name + "_precision"] = precision
    return fields


class ParsedRows(list):
    """Valid siblings plus explicit wire-entry errors; never fabricate identities."""

    def __init__(self, rows=(), *, wire_count=0):
        super().__init__(rows)
        self.errors = []
        self.wire_count = wire_count


ENTRY_IDENTITY_FIELDS = {
    "id": ("id", "guid", "document_number", "proposal_id"),
    "title": ("title", "name", "headline"),
    "link": ("link", "url", "html_url"),
    "excerpt": ("summary", "abstract", "description", "body", "content"),
}


def entry_identity(entry: JsonValue) -> PublisherEntryIdentity:
    """Keep the actual wire-entry digest separate from short presentation labels."""
    fields = (
        {key: value for key, value in entry.items() if not key.endswith("_parsed")}
        if isinstance(entry, dict)
        else entry
    )
    identity: dict[str, JsonValue] = {}
    if isinstance(fields, dict):
        for label, names in ENTRY_IDENTITY_FIELDS.items():
            for name in names:
                value = fields.get(name)
                if isinstance(value, (str, int)) and not isinstance(value, bool) and str(value).strip():
                    identity[label] = " ".join(str(value).split())[:300]
                    break
    return PublisherEntryIdentity(entry_sha256=sha256(fields), entry=EntryIdentity.model_validate(identity))


def _feed_spec(feed: FeedParsingSpec | Mapping[str, JsonValue]) -> FeedParsingSpec:
    """Project parser configuration at its external mapping boundary."""
    if isinstance(feed, FeedParsingSpec):
        return feed
    return FeedParsingSpec.model_validate(
        {field: feed[field] for field in FeedParsingSpec.model_fields if field in feed}
    )


def _feed_sdk_metadata(value: object) -> JsonValue:
    """Preserve SDK tuple metadata in the same JSON form used by existing entry digests."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not isfinite(value):
            raise ValueError("Feed SDK metadata contains a nonfinite number")
        return value
    if isinstance(value, (list, tuple)):
        return [_feed_sdk_metadata(child) for child in value]
    if isinstance(value, dict):
        record: dict[str, JsonValue] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError("Feed SDK metadata requires string keys")
            record[key] = _feed_sdk_metadata(child)
        return record
    raise ValueError("Feed SDK metadata contains an unsupported value")


class _FeedSdkDocument(DomainModel):
    model_config = ConfigDict(extra="ignore")

    version: str | None
    bozo: FalseFlag | TrueFlag | ZeroInt | OneInt
    bozo_exception: str | None = None
    entries: list[dict[str, JsonValue]]

    @model_validator(mode="after")
    def actual_malformed_diagnostic(self) -> Self:
        if self.bozo and self.bozo_exception is None:
            raise ValueError("Malformed feed SDK result lacks its actual exception diagnostic")
        return self

    @field_validator("bozo_exception", mode="before")
    @classmethod
    def actual_sdk_exception(cls, value: object) -> object:
        return str(value) if isinstance(value, BaseException) else value

    @field_validator("entries", mode="before")
    @classmethod
    def exclude_derived_sdk_time_structs(cls, value: object) -> object:
        if not isinstance(value, list):
            return value
        return [
            _feed_sdk_metadata({key: item for key, item in row.items() if not str(key).endswith("_parsed")})
            if isinstance(row, dict)
            else row
            for row in value
        ]


class _FeedContent(DomainModel):
    model_config = ConfigDict(extra="ignore")

    value: str


class _FeedTag(DomainModel):
    model_config = ConfigDict(extra="ignore")

    term: str | None = None


class _FeedEntry(DomainModel):
    model_config = ConfigDict(extra="ignore")

    id: str | None = None
    guid: str | None = None
    link: str = ""
    title: str = ""
    summary: str = ""
    content: list[_FeedContent] = Field(default_factory=list)
    published: str | None = None
    updated: str | None = None
    tags: list[_FeedTag] = Field(default_factory=list)


def feeds(
    payload: bytes | str, source_id: str, feed: FeedParsingSpec | Mapping[str, JsonValue]
) -> ParsedDiscoveryBatch:
    feed = _feed_spec(feed)
    parsed = _FeedSdkDocument.model_validate(
        feedparser.parse(BytesIO(payload.encode("utf-8") if isinstance(payload, str) else payload))
    )
    if not parsed.version:
        raise ValueError("Invalid feed: no RSS or Atom document")
    if parsed.bozo and not parsed.entries:
        raise ValueError(f"Invalid feed: {parsed.bozo_exception}")
    discoveries: list[DiscoveryEnvelope] = []
    errors: list[DigestEntryError | DocumentEntryError] = []
    if parsed.bozo:
        errors.append(DocumentEntryError(reason="MALFORMED_FEED_DOCUMENT", error=parsed.bozo_exception))
    for index, original in enumerate(parsed.entries):
        identity = entry_identity(original)
        try:
            entry = _FeedEntry.model_validate(original)
            key = entry.link if source_id == "S23" else entry.id or entry.guid or entry.link
            if not key:
                errors.append(
                    DigestEntryError(
                        entry_index=index,
                        reason="ENTRY_WITHOUT_ID_OR_URL",
                        entry_sha256=identity.entry_sha256,
                        entry=identity.entry,
                    )
                )
                continue
            body = "\n".join(content.value for content in entry.content) or entry.summary
            row: dict[str, JsonValue] = {
                "external_id": key,
                "title": text(entry.title),
                "text": text(body),
                "url": entry.link,
                **timestamp_fields(entry.published),
                **(timestamp_fields(entry.updated, "source_updated_at") if entry.updated else {}),
                "categories": [tag.term for tag in entry.tags],
                "subtype": feed.subtype or ("commits" if "/commits/" in feed.url else "releases"),
            }
            discoveries.append(discovered_item(source_id, row))
        except ValueError as exc:
            errors.append(
                DigestEntryError(
                    entry_index=index,
                    reason="INVALID_PUBLISHER_ENTRY",
                    error=str(exc),
                    entry_sha256=identity.entry_sha256,
                    entry=identity.entry,
                )
            )
    return ParsedDiscoveryBatch(discoveries=discoveries, errors=errors, wire_count=len(parsed.entries))


def publisher_cms_entries(payload):
    """Explicit Contentful entry identities, not guessed URL aliases."""
    script = HTMLParser(payload).css_first('script#__NEXT_DATA__[type="application/json"]')
    if script is None:
        return []
    entries = []

    def visit(value):
        if isinstance(value, dict):
            fields = value.get("fields", {})
            if isinstance(fields, dict):
                slug, title, content = (fields.get(k) for k in ("slug", "title", "content"))
                identity = value.get("sys", {}).get("id")
                if (
                    isinstance(identity, str)
                    and isinstance(slug, str)
                    and re.fullmatch(r"[A-Za-z0-9-]+", slug)
                    and isinstance(title, str)
                    and title.strip()
                    and isinstance(content, dict)
                    and content
                ):
                    entries.append(
                        {
                            "entry_id": identity,
                            "slug": slug,
                            "title": title,
                            "content_sha256": sha256(content),
                        }
                    )
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(json.loads(script.text()))
    return entries


def _dtcc_release_url(url):
    parts = urlsplit(url)
    return (
        parts.netloc == "www.dtcc.com"
        and bool(re.fullmatch(r"/press-releases/\d{4}/[A-Za-z0-9][A-Za-z0-9._~-]*", parts.path))
        and not parts.path.lower().endswith(
            (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".zip")
        )
    )


def article_links(payload, feed):
    feed = _feed_spec(feed)
    pattern = feed.article_pattern
    if not pattern:
        raise ValueError("HTML feed needs article_pattern")
    result = {}
    tree = HTMLParser(payload)
    for a in tree.css("a[href]"):
        url = urljoin(feed.url, a.attributes["href"])
        if urlsplit(url).netloc != urlsplit(feed.url).netloc:
            continue
        if urlsplit(feed.url).netloc == "www.dtcc.com" and not _dtcc_release_url(url):
            continue
        target = url if pattern.startswith("http") else urlsplit(url).path
        if re.search(pattern, target):
            result[url] = text(a.text())

    fields = feed.embedded_json_fields
    if fields:
        script = tree.css_first('script#__NEXT_DATA__[type="application/json"]')
        if script is None:
            raise ValueError("Configured __NEXT_DATA__ article metadata missing")

        def visit(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    if key in fields and isinstance(child, str):
                        url = urljoin(feed.url, child)
                        if urlsplit(url).netloc == urlsplit(feed.url).netloc:
                            target = url if pattern.startswith("http") else urlsplit(url).path
                            if re.search(pattern, target):
                                result[url] = text(value.get("title", ""))
                    visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)

        visit(json.loads(script.text()))
    if not result:
        raise ValueError("No article links matched; possible layout change")
    rows = [
        {"external_id": u, "url": u, "title": t, "text": "", "published_at": None} for u, t in result.items()
    ]
    if fields:
        data = json.loads(script.text())
        bindings = {}
        identities = publisher_cms_entries(payload)

        def bind(value):
            if isinstance(value, dict):
                entry = value.get("fields", {})
                if isinstance(entry, dict) and value.get("sys", {}).get("id"):
                    seo = entry.get("seo", {})
                    canonical = seo.get("fields", {}).get("canonicalUrl") if isinstance(seo, dict) else None
                    candidates = [e for e in identities if e["entry_id"] == value["sys"]["id"]]
                    if canonical in result and len(candidates) == 1:
                        bindings.setdefault(canonical, []).append(candidates[0])
                for child in value.values():
                    bind(child)
            elif isinstance(value, list):
                for child in value:
                    bind(child)

        bind(data)
        for row in rows:
            candidates = bindings.get(row["url"], [])
            if len(candidates) == 1:
                row["publisher_cms_identity"] = candidates[0]
                row["publisher_cms_slug_url"] = urljoin(
                    feed.url.rstrip("/") + "/", candidates[0]["slug"] + "/"
                )
    return rows


BYBIT_BOILERPLATE = re.compile(
    r"Bybit is committed to maintaining a secure and reliable trading environment for our community\.\s*"
)


def contentful_text(node):
    """Text of a Contentful rich-text document, one block per paragraph or heading."""
    if isinstance(node, dict):
        if node.get("nodeType") == "text":
            return node.get("value", "")
        parts = [contentful_text(child) for child in node.get("content", [])]
        joiner = " " if node.get("nodeType") in {"paragraph", "hyperlink", "list-item"} else "\n"
        return joiner.join(part for part in parts if part.strip())
    return ""


def article(payload, *, source_id=None):
    tree = HTMLParser(payload)
    title_node = tree.css_first("h1")
    body_text = None
    if source_id == "S01":
        authored = tree.css("main .field--name-body")
        if len(authored) != 1 or not text(authored[0].html):
            raise ValueError("SEC release requires one nonempty body field")
        body = authored[0]
    elif source_id == "S13":
        script = tree.css_first('script#__NEXT_DATA__[type="application/json"]')
        entry = (
            json.loads(script.text()).get("props", {}).get("pageProps", {}).get("article") if script else None
        )
        content = (entry or {}).get("fields", {}).get("content")
        body_text = " ".join(contentful_text(content).split()) if content else ""
        if not body_text:
            raise ValueError("Robinhood article requires its Contentful content")
        body = None
        if not title_node and (entry or {}).get("fields", {}).get("title"):
            title_node = HTMLParser("<h1>" + entry["fields"]["title"] + "</h1>").css_first("h1")
    elif source_id == "S08":
        authored = tree.css("main .content-item--richtextblock .rtf")
        if not authored or any(not text(node.html) or len(node.css(".rtf")) > 1 for node in authored):
            raise ValueError("DTCC release requires nonempty unambiguous authored rich-text blocks")
        body = HTMLParser("<div>" + "\n".join(node.html for node in authored) + "</div>").body
    elif source_id == "S28":
        authored = tree.css(".insight-rich-text.w-richtext")
        if len(authored) != 1 or not text(authored[0].html):
            raise ValueError("Anchorage article requires one nonempty authored body")
        body = authored[0]
    elif source_id == "S15":
        authored = tree.css("main .ArticleContent")
        if len(authored) != 1 or not text(authored[0].html):
            raise ValueError("Uniswap article requires one nonempty authored body")
        body = authored[0]
    elif source_id == "S07" and tree.css("article.node--type-mega_menu_panel"):
        authored = tree.css("main article[about]")
        headings = tree.css("main .uswds-page-title")
        if len(authored) != 1 or len(headings) != 1 or not text(authored[0].html):
            raise ValueError("Treasury release requires one authored article and page title")
        body, title_node = authored[0], headings[0]
    else:
        body = tree.css_first("article") or tree.css_first("main") or tree.body
    title = text(title_node.text()) if title_node else ""
    candidates = []

    def publication_nodes(value):
        if isinstance(value, list):
            for child in value:
                yield from publication_nodes(child)
        elif isinstance(value, dict):
            types = value.get("@type", [])
            types = [types] if isinstance(types, str) else types
            if set(types) & {"Article", "NewsArticle", "BlogPosting", "ReportageNewsArticle"} and published(
                value.get("datePublished")
            ):
                yield value

            if "@graph" in value:
                yield from publication_nodes(value["@graph"])

    for node in tree.css('script[type="application/ld+json"]'):
        try:
            candidates.extend(publication_nodes(json.loads(node.text())))
        except (ValueError, TypeError):
            continue
    matching = [node for node in candidates if text(node.get("headline", "")).casefold() == title.casefold()]
    selected = matching or candidates
    values = {str(node["datePublished"]) for node in selected}
    timestamp = next(iter(values)) if len(values) == 1 else None
    basis = "ARTICLE_JSON_LD_DATE_PUBLISHED" if timestamp else None
    if not timestamp and not values:
        meta = tree.css_first('meta[property="article:published_time"], meta[itemprop="datePublished"]')
        if meta:
            timestamp, basis = meta.attributes.get("content"), "ARTICLE_PUBLICATION_META"
        elif source_id == "S08":
            dates = {
                node.text(strip=True)
                for node in tree.css("main .page-detail-masthead__meta .paragraph-small")
            }
            if len(dates) == 1:
                try:
                    timestamp = (
                        datetime.strptime(next(iter(dates)), "%b %d, %Y")
                        .replace(tzinfo=UTC)
                        .date()
                        .isoformat()
                    )
                    basis = "DTCC_CALENDAR_MASTHEAD"
                except ValueError:
                    pass
        elif source_id == "S18":
            meta = tree.css_first('meta[name="publicationDate"]')
            if meta:
                timestamp, basis = meta.attributes.get("content"), "BLACKROCK_CALENDAR_PUBLICATION_META"
        else:
            times = (body or tree).css("time[datetime]")
            designated = tree.css('time[itemprop="datePublished"][datetime]')
            if source_id == "S07":
                designated += tree.css(".field--name-field-news-publication-date time[datetime]")
            declared_values = {node.attributes["datetime"] for node in designated}
            if len(declared_values) == 1:
                timestamp = next(iter(declared_values))
                basis = "DESIGNATED_PUBLICATION_TIME_ELEMENT"
            elif not designated and len(times) == 1:
                timestamp = times[0].attributes.get("datetime")
                basis = "ARTICLE_TIME_ELEMENT"
    fields = timestamp_fields(
        timestamp,
        date_only=basis == "BLACKROCK_CALENDAR_PUBLICATION_META",
        epoch_unit="ms" if basis == "BLACKROCK_CALENDAR_PUBLICATION_META" else None,
    )
    return {
        "title": title,
        "text": body_text if body_text is not None else text(body.html if body else payload),
        **fields,
        "published_at_basis": basis,
        "published_at_missing_reason": None
        if fields["published_at"]
        else "UNVERIFIED_SEMANTICS"
        if len(values) > 1
        else "INCOMPLETE",
    }


def json_items(payload, source_id):
    data = (
        decode_unique_json(payload)
        if source_id == "S11" and isinstance(payload, (str, bytes))
        else json.loads(payload)
        if isinstance(payload, (str, bytes))
        else payload
    )
    if source_id == "S03":
        rows = data["results"]

        def single(row):
            return dict(data, results=[row])
    elif source_id == "S09":
        if data.get("success") is False:
            raise ValueError("Binance CMS API returned failure")
        root = data["data"]
        rows = list(root.get("articles", []))
        for catalog in root.get("catalogs", []):
            rows.extend(catalog.get("articles", []))

        def single(row):
            return dict(data, data=dict(root, articles=[row], catalogs=[]))
    elif source_id == "S10":
        if str(data.get("code", "0")) != "0":
            raise ValueError("OKX API error: " + str(data.get("code")))
        rows = [row for page in data["data"] for row in page.get("details", [page])]

        def single(row):
            return dict(data, data=[{"details": [row]}])
    elif source_id == "S11":
        rows = bybit_announcement_success(data).result.list

        def single(row):
            return dict(data, result=dict(data["result"], list=[row]))
    elif source_id == "S19":
        rows = data["results"]["response"]["hits"]["hits"]

        def single(row):
            return {"results": {"response": {"hits": {"hits": [row]}}}}
    elif source_id == "S24":
        if data.get("errors"):
            raise ValueError("Snapshot GraphQL error: " + str(data["errors"]))
        rows = data["data"]["proposals"]

        def single(row):
            return dict(data, data=dict(data["data"], proposals=[row]))
    else:
        raise ValueError(f"No JSON parser for {source_id}")
    if not isinstance(rows, list):
        raise TypeError("Publisher entry collection is not a list")
    parsed = ParsedRows(wire_count=len(rows))
    for index, row in enumerate(rows):
        try:
            normalized = _json_items(single(row), source_id)
            for item in normalized:
                discovered_item(source_id, item)
                identity = item.get("external_id")
                if (
                    isinstance(identity, bool)
                    or not isinstance(identity, (str, int))
                    or not str(identity).strip()
                ):
                    raise ValueError("Publisher entry has no valid external identity")
            parsed.extend(normalized)
        except (KeyError, ValueError, TypeError) as exc:
            parsed.errors.append(
                {
                    "entry_index": index,
                    "reason": "INVALID_PUBLISHER_ENTRY",
                    "error": str(exc),
                    **entry_identity(row),
                }
            )
    return parsed


def _json_items(payload, source_id):
    data = (
        decode_unique_json(payload)
        if source_id == "S11" and isinstance(payload, (str, bytes))
        else json.loads(payload)
        if isinstance(payload, (str, bytes))
        else payload
    )
    if source_id == "S03":
        rows = data["results"]
        return [
            {
                "external_id": r["document_number"],
                "title": r["title"],
                "text": text(r.get("abstract") or r.get("excerpts", "")),
                "url": r["html_url"],
                **timestamp_fields(r.get("publication_date")),
                "document_type": r.get("type"),
                "pdf_url": r.get("pdf_url"),
                "agencies": r.get("agencies"),
            }
            for r in rows
        ]
    if source_id == "S09":
        if data.get("success") is False:
            raise ValueError("Binance CMS API returned failure")
        root = data["data"]
        rows = root.get("articles", []) if isinstance(root, dict) else []
        for cat in root.get("catalogs", []):
            rows += cat.get("articles", [])
        return [
            {
                "external_id": r["code"],
                "title": r["title"],
                "text": text(r.get("body", "")),
                "url": "https://www.binance.com/en/support/announcement/" + r["code"],
                **timestamp_fields(
                    preferred_publication(r.get("releaseDate"), r.get("publishDate")), epoch_unit="ms"
                ),
            }
            for r in rows
        ]
    if source_id == "S10":
        if str(data.get("code", "0")) != "0":
            raise ValueError("OKX API error: " + str(data.get("code")))
        rows = [detail for page in data["data"] for detail in page.get("details", [page])]
        return [
            {
                "external_id": r["url"] + "|" + str(r["pTime"]),
                "title": r["title"],
                "text": r.get("title", ""),
                "url": r["url"],
                **timestamp_fields(r["pTime"], epoch_unit="ms"),
                "announcement_type": r.get("annType"),
                **timestamp_fields(r.get("businessPTime"), "business_published_at", epoch_unit="ms"),
            }
            for r in rows
        ]
    if source_id == "S11":
        response = bybit_announcement_success(data)
        return [
            {
                "external_id": r["url"],
                "title": r["title"],
                "text": BYBIT_BOILERPLATE.sub("", text(r.get("description", ""))).strip() or r["title"],
                "url": r["url"],
                **timestamp_fields(
                    preferred_publication(r.get("publishTime"), r.get("dateTimestamp")), epoch_unit="ms"
                ),
                "tags": r.get("tags"),
                "announcement_type": r.get("type"),
            }
            for r in response.result.list
        ]
    if source_id == "S19":
        return [
            {
                "external_id": r["url"],
                "title": r["title"],
                "text": " ".join(re.sub(r"\bnull\b", " ", r["contentString"]).split()),
                "url": r["url"],
                **timestamp_fields(r["referenceDate"]),
            }
            for r in (hit["_source"] for hit in data["results"]["response"]["hits"]["hits"])
        ]
    if source_id == "S24":
        if data.get("errors"):
            raise ValueError("Snapshot GraphQL error: " + str(data["errors"]))
        return [
            {
                "external_id": r["id"] + "|" + r["state"],
                "proposal_id": r["id"],
                "state": r["state"],
                "title": r["title"],
                "text": r["body"]
                + "\nState: "
                + r["state"]
                + "\nScores: "
                + json.dumps(r["scores"])
                + "; total: "
                + str(r["scores_total"]),
                "url": "https://snapshot.org/#/" + r["space"]["id"] + "/proposal/" + r["id"],
                **timestamp_fields(r.get("created", r["start"]), epoch_unit="s"),
                **timestamp_fields(r["end"], "end", epoch_unit="s"),
                "scores": r["scores"],
                "scores_total": r["scores_total"],
            }
            for r in data["data"]["proposals"]
        ]
    raise ValueError(f"No JSON parser for {source_id}")


def instruments(
    payload: bytes | JsonValue, feed: FeedParsingSpec | Mapping[str, JsonValue]
) -> dict[str, JsonValue]:
    return parse_instrument_page(payload, _feed_spec(feed)).snapshot.storage_record()


def binance_body(value):
    """Binance CMS body is a JSON rich-text document, not HTML alone."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return text(value)
    if isinstance(value, list):
        return " ".join(filter(None, (binance_body(v) for v in value)))
    if isinstance(value, dict):
        own = value.get("text", "")
        own = text(own) if isinstance(own, str) else ""
        children = value.get("child", value.get("children", []))
        return " ".join(filter(None, [own, binance_body(children)]))
    return ""
