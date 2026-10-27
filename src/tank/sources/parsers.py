"""Source-specific wire parsers. No network side effects."""

import json
import re
from collections.abc import Iterator, Mapping
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
from tank.contracts.json import DuplicateJsonKeyError, JsonDocument, decode_unique_json
from tank.contracts.scalars import FalseFlag, OneInt, TrueFlag, ZeroInt
from tank.contracts.source_envelopes import (
    BinanceCmsCatalogReply,
    BinanceCmsStatus,
    FederalRegisterResults,
    OkxAnnouncementReply,
    OkxAnnouncementStatus,
    PressSearchReply,
    SnapshotProposalReply,
    SnapshotReportedErrors,
    bybit_announcement_success,
)
from tank.contracts.source_errors import DigestEntryError, DocumentEntryError, EntryIdentity
from tank.contracts.source_items import (
    ArticleDiscovery,
    BinanceCatalogListing,
    DiscoveredItem,
    DiscoveryEnvelope,
    DiscoveryObservation,
    ExchangeAnnouncementDiscovery,
    FederalRegisterDiscovery,
    GovernanceDiscovery,
    PublisherCMSIdentity,
    discovered_item,
)
from tank.contracts.source_parsing import (
    ArticleLinkListing,
    BinanceCmsEntry,
    BybitAnnouncementEntry,
    ContentfulDocument,
    ContentfulNode,
    ContentfulRichTextDocument,
    ContentfulTextNode,
    FederalRegisterEntry,
    OkxAnnouncementEntry,
    ParsedArticle,
    ParsedDiscoveryBatch,
    PressReleaseHit,
    PublisherEntryIdentity,
    S28PressRoomArticleBinding,
    SnapshotProposalEntry,
)
from tank.contracts.sources import FeedParsingSpec
from tank.sources.instrument_adapters import parse_instrument_page
from tank.time_precision import Timestamp, propagate_timestamp
from tank.util import EpochUnit, iso, parse_epoch, parse_utc_time, sha256


def text(html: str | bytes | None) -> str:
    if html is None:
        return ""
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    tree = HTMLParser(html)
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

    version: str | None = None
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


def _cms_entries(document: JsonValue) -> list[PublisherCMSIdentity]:
    entries: list[PublisherCMSIdentity] = []

    def visit(value: JsonValue) -> None:
        if isinstance(value, dict):
            fields = value.get("fields")
            system = value.get("sys")
            if isinstance(fields, dict) and isinstance(system, dict):
                identity = system.get("id")
                slug, title, content = (fields.get(key) for key in ("slug", "title", "content"))
                if (
                    isinstance(identity, str)
                    and identity.strip()
                    and isinstance(slug, str)
                    and re.fullmatch(r"[A-Za-z0-9-]+", slug)
                    and isinstance(title, str)
                    and title.strip()
                    and isinstance(content, dict)
                    and content
                ):
                    entries.append(
                        PublisherCMSIdentity(
                            entry_id=identity, slug=slug, title=title, content_sha256=sha256(content)
                        )
                    )
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(document)
    return entries


def publisher_cms_entries(payload: bytes | str) -> list[PublisherCMSIdentity]:
    """Return only actual complete identities from uniquely decoded CMS metadata."""
    script = HTMLParser(payload).css_first('script#__NEXT_DATA__[type="application/json"]')
    if script is None:
        return []
    return _cms_entries(decode_unique_json(script.text()))


def _dtcc_release_url(url: str) -> bool:
    parts = urlsplit(url)
    return (
        parts.netloc == "www.dtcc.com"
        and bool(re.fullmatch(r"/press-releases/\d{4}/[A-Za-z0-9][A-Za-z0-9._~-]*", parts.path))
        and not parts.path.lower().endswith(
            (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".zip")
        )
    )


def article_links(
    payload: bytes | str, feed: FeedParsingSpec | Mapping[str, JsonValue]
) -> list[ArticleLinkListing]:
    feed = _feed_spec(feed)
    pattern = feed.article_pattern
    if not pattern:
        raise ValueError("HTML feed needs article_pattern")
    result: dict[str, ArticleLinkListing] = {}
    tree = HTMLParser(payload)
    for anchor in tree.css("a[href]"):
        url = urljoin(feed.url, anchor.attributes["href"])
        if urlsplit(url).netloc != urlsplit(feed.url).netloc:
            continue
        if urlsplit(feed.url).netloc == "www.dtcc.com" and not _dtcc_release_url(url):
            continue
        target = url if pattern.startswith("http") else urlsplit(url).path
        if re.search(pattern, target):
            result[url] = ArticleLinkListing(external_id=url, url=url, title=text(anchor.text()))

    fields = feed.embedded_json_fields
    metadata: JsonValue = None
    if fields:
        script = tree.css_first('script#__NEXT_DATA__[type="application/json"]')
        if script is None:
            raise ValueError("Configured __NEXT_DATA__ article metadata missing")
        metadata = decode_unique_json(script.text())

        def visit_links(value: JsonValue) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    if key in fields and isinstance(child, str):
                        url = urljoin(feed.url, child)
                        if urlsplit(url).netloc == urlsplit(feed.url).netloc:
                            target = url if pattern.startswith("http") else urlsplit(url).path
                            if re.search(pattern, target):
                                raw_title = value.get("title")
                                title = text(raw_title) if isinstance(raw_title, str) else None
                                if title is not None or url not in result:
                                    result[url] = ArticleLinkListing(external_id=url, url=url, title=title)
                    visit_links(child)
            elif isinstance(value, list):
                for child in value:
                    visit_links(child)

        visit_links(metadata)
    if not result:
        raise ValueError("No article links matched; possible layout change")
    if fields:
        identities = _cms_entries(metadata)
        bindings: dict[str, list[PublisherCMSIdentity]] = {}

        def bind(value: JsonValue) -> None:
            if isinstance(value, dict):
                entry = value.get("fields")
                system = value.get("sys")
                if isinstance(entry, dict) and isinstance(system, dict):
                    seo = entry.get("seo")
                    seo_fields = seo.get("fields") if isinstance(seo, dict) else None
                    canonical = seo_fields.get("canonicalUrl") if isinstance(seo_fields, dict) else None
                    candidates = [
                        identity for identity in identities if identity.entry_id == system.get("id")
                    ]
                    if isinstance(canonical, str) and canonical in result and len(candidates) == 1:
                        bindings.setdefault(canonical, []).append(candidates[0])
                for child in value.values():
                    bind(child)
            elif isinstance(value, list):
                for child in value:
                    bind(child)

        bind(metadata)
        for url, listing in result.items():
            candidates = bindings.get(url, [])
            if len(candidates) == 1:
                result[url] = ArticleLinkListing(
                    external_id=listing.external_id,
                    url=listing.url,
                    title=listing.title,
                    publisher_cms_identity=candidates[0],
                    publisher_cms_slug_url=urljoin(feed.url.rstrip("/") + "/", candidates[0].slug + "/"),
                )
    return list(result.values())


BYBIT_BOILERPLATE = re.compile(
    r"Bybit is committed to maintaining a secure and reliable trading environment for our community\.\s*"
)


class ContentfulStructureError(ValueError):
    pass


def contentful_text(node: ContentfulNode | ContentfulRichTextDocument) -> str:
    """Text of a Contentful rich-text document, one block per paragraph or heading."""
    if isinstance(node, ContentfulTextNode):
        return node.value
    parts = [contentful_text(child) for child in node.content]
    joiner = " " if node.nodeType in {"paragraph", "hyperlink", "list-item"} else "\n"
    return joiner.join(part for part in parts if part.strip())


def _article_nodes(value: JsonValue) -> Iterator[dict[str, JsonValue]]:
    if isinstance(value, list):
        for child in value:
            yield from _article_nodes(child)
    elif isinstance(value, dict):
        types = value.get("@type")
        declared_types = [types] if isinstance(types, str) else types if isinstance(types, list) else []
        if any(
            kind in {"Article", "NewsArticle", "BlogPosting", "ReportageNewsArticle"}
            for kind in declared_types
            if isinstance(kind, str)
        ):
            yield value
        if "@graph" in value:
            yield from _article_nodes(value["@graph"])


def _article_urls(value: JsonValue) -> Iterator[str]:
    if isinstance(value, str):
        if value.strip() and not value.startswith("#"):
            yield value
    elif isinstance(value, list):
        for child in value:
            yield from _article_urls(child)
    elif isinstance(value, dict):
        for key in ("@id", "url"):
            if key in value:
                yield from _article_urls(value[key])


def _document_url(value: str, page_url: str) -> tuple[str, str | None, int | None, str, str]:
    resolved = urlsplit(urljoin(page_url, value))
    scheme = resolved.scheme.casefold()
    port = resolved.port
    if port is None:
        port = 443 if scheme == "https" else 80 if scheme == "http" else None
    return scheme, resolved.hostname, port, resolved.path or "/", resolved.query


def _article_owns_page(node: dict[str, JsonValue], title: str, page_url: str | None) -> bool:
    declared_urls = [
        url for key in ("url", "mainEntityOfPage") if key in node for url in _article_urls(node[key])
    ]
    if declared_urls:
        if page_url is None:
            return False
        try:
            actual = _document_url(page_url, page_url)
            return all(_document_url(url, page_url) == actual for url in declared_urls)
        except ValueError:
            return False
    headline = node.get("headline")
    return isinstance(headline, str) and bool(title) and text(headline).casefold() == title.casefold()


def _s28_article_binding(
    tree: HTMLParser,
    node: dict[str, JsonValue],
    title: str,
    page_url: str | None,
    response_url: str | None,
) -> S28PressRoomArticleBinding | None:
    canonical = tree.css('link[rel="canonical"]')
    headlines = tree.css("h1")
    headline = node.get("headline")
    urls = list(_article_urls(node.get("url")))
    main_entities = list(_article_urls(node.get("mainEntityOfPage")))
    if (
        node.get("@type") != "BlogPosting"
        or len(canonical) != 1
        or len(headlines) != 1
        or not isinstance(headline, str)
        or len(urls) != 1
        or len(main_entities) != 1
    ):
        return None
    declared_canonical = canonical[0].attributes.get("href")
    if not isinstance(declared_canonical, str):
        return None
    try:
        return S28PressRoomArticleBinding(
            kind="S28_PRESS_ROOM_INSIGHTS",
            canonical_url=declared_canonical,
            known_document_url=page_url,
            response_url=response_url,
            heading=title,
            headline=headline,
            article_url=urls[0],
            main_entity_url=main_entities[0],
        )
    except ValueError:
        return None


def article(
    payload: str | bytes,
    *,
    source_id: str | None = None,
    page_url: str | None = None,
    response_url: str | None = None,
) -> ParsedArticle:
    tree = HTMLParser(payload)
    title_node = tree.css_first("h1")
    reported_title = None
    body_text = None
    if source_id == "S01":
        authored = tree.css("main .field--name-body")
        if len(authored) != 1 or not text(authored[0].html):
            raise ValueError("SEC release requires one nonempty body field")
        body = authored[0]
    elif source_id == "S13":
        script = tree.css_first('script#__NEXT_DATA__[type="application/json"]')
        if script is None:
            raise ContentfulStructureError("Publisher article requires its Contentful document")
        try:
            entry = ContentfulDocument.model_validate(
                decode_unique_json(script.text())
            ).props.pageProps.article
        except ValueError:
            raise ContentfulStructureError(
                "Publisher article contains invalid Contentful document structure"
            ) from None
        body_text = " ".join(contentful_text(entry.fields.content).split())
        body = None
        if title_node is None:
            reported_title = entry.fields.title
    elif source_id == "S08":
        authored = tree.css("main .content-item--richtextblock .rtf")
        if not authored:
            raise ValueError("DTCC release requires nonempty unambiguous authored rich-text blocks")
        markup: list[str] = []
        for node in authored:
            block = node.html
            if block is None or not text(block) or len(node.css(".rtf")) > 1:
                raise ValueError("DTCC release requires nonempty unambiguous authored rich-text blocks")
            markup.append(block)
        body = HTMLParser("<div>" + "\n".join(markup) + "</div>").body
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
    title = text(title_node.text()) if title_node else text(reported_title)
    structured_articles: list[dict[str, JsonValue]] = []

    for node in tree.css('script[type="application/ld+json"]'):
        try:
            structured_articles.extend(_article_nodes(decode_unique_json(node.text())))
        except (ValueError, TypeError):
            continue
    candidates: list[dict[str, JsonValue]] = []
    for structured in structured_articles:
        declared_publication = structured.get("datePublished")
        if isinstance(declared_publication, str) and published(declared_publication) is not None:
            candidates.append(structured)
    observed_page = response_url if response_url is not None else page_url
    selected = [node for node in candidates if _article_owns_page(node, title, observed_page)]
    if source_id == "S28" and response_url is not None and page_url is not None and response_url != page_url:
        selected = []
    anchorage_binding = None
    postings = [node for node in structured_articles if node.get("@type") == "BlogPosting"]
    if source_id == "S28":
        canonical_links = tree.css('link[rel="canonical"]')
        for posting in postings:
            posting_urls = list(_article_urls(posting.get("url")))
            if canonical_links and (
                len(canonical_links) != 1
                or len(posting_urls) != 1
                or canonical_links[0].attributes.get("href") != posting_urls[0]
            ):
                selected = [candidate for candidate in selected if candidate is not posting]
    if source_id == "S28" and len(postings) == 1 and postings[0] in candidates:
        anchorage_binding = _s28_article_binding(tree, postings[0], title, page_url, response_url)
        if anchorage_binding is not None and postings[0] not in selected:
            selected.append(postings[0])
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
    publication = Timestamp.model_validate(
        {
            "value": fields["published_at"],
            "precision": fields["published_at_precision"],
            "precision_reason": fields["published_prec_reason"],
        }
    )
    if publication.value is None and (len(values) > 1 or (candidates and not selected)):
        publication = Timestamp(None, "COLLECTION_ONLY", "UNVERIFIED_SEMANTICS")
    return ParsedArticle(
        title=title,
        text=body_text if body_text is not None else text(body.html if body else payload),
        publication=publication,
        publication_basis=basis,
        publication_missing_reason=publication.precision_reason if publication.value is None else None,
        anchorage_binding=anchorage_binding,
    )


def _json_rows(data: dict[str, JsonValue], source_id: str) -> list[JsonValue]:
    if source_id == "S03":
        return FederalRegisterResults.model_validate(data).results
    if source_id == "S09":
        if not BinanceCmsStatus.model_validate(data).success:
            raise ValueError("Binance CMS API returned failure")
        response = BinanceCmsCatalogReply.model_validate(data)
        return [row for catalog in response.data.catalogs for row in catalog.articles]
    if source_id == "S10":
        status = OkxAnnouncementStatus.model_validate(data)
        if status.code != "0":
            raise ValueError("OKX API error: " + status.code)
        return [row for page in OkxAnnouncementReply.model_validate(data).data for row in page.details]
    if source_id == "S11":
        return bybit_announcement_success(data).result.list
    if source_id == "S19":
        return PressSearchReply.model_validate(data).results.response.hits.hits
    if source_id == "S24":
        if "errors" in data:
            reported = SnapshotReportedErrors.model_validate({"errors": data["errors"]})
            if reported.errors:
                raise ValueError("Snapshot GraphQL error: " + str(reported.errors))
        return SnapshotProposalReply.model_validate(data).data.proposals
    raise ValueError(f"No JSON parser for {source_id}")


def _json_discovery(source_id: str, original: JsonValue) -> DiscoveryEnvelope:
    material: DiscoveredItem
    if source_id == "S03":
        register = FederalRegisterEntry.model_validate(original)
        material = FederalRegisterDiscovery.model_validate(
            {
                "source_id": "S03",
                "external_id": register.document_number,
                "title": register.title,
                "text": text(register.abstract if register.abstract is not None else register.excerpts),
                "url": register.html_url,
                **timestamp_fields(register.publication_date),
                "document_type": register.type,
                "pdf_url": register.pdf_url,
                "agencies": register.agencies,
            }
        )
    elif source_id == "S09":
        catalog = BinanceCmsEntry.model_validate(original)
        listing = BinanceCatalogListing.model_validate(
            {
                "source_id": "S09",
                "external_id": catalog.code,
                "title": catalog.title,
                "url": "https://www.binance.com/en/support/announcement/" + catalog.code,
                "publication": propagate_timestamp(
                    timestamp_fields(
                        preferred_publication(catalog.releaseDate, catalog.publishDate), epoch_unit="ms"
                    ),
                    "published_at",
                ),
                **(
                    {"summary": text(catalog.body) if catalog.body is not None else None}
                    if "body" in catalog.model_fields_set
                    else {}
                ),
            }
        )
        return DiscoveryEnvelope(discovery=listing, observation=DiscoveryObservation(), payload=None)
    elif source_id == "S10":
        okx = OkxAnnouncementEntry.model_validate(original)
        material = ExchangeAnnouncementDiscovery.model_validate(
            {
                "source_id": "S10",
                "external_id": okx.url + "|" + okx.pTime,
                "title": okx.title,
                "text": okx.title,
                "url": okx.url,
                **timestamp_fields(okx.pTime, epoch_unit="ms"),
                "announcement_type": okx.annType,
                **timestamp_fields(okx.businessPTime, "business_published_at", epoch_unit="ms"),
            }
        )
    elif source_id == "S11":
        bybit = BybitAnnouncementEntry.model_validate(original)
        material = ExchangeAnnouncementDiscovery.model_validate(
            {
                "source_id": "S11",
                "external_id": bybit.url,
                "title": bybit.title,
                "text": BYBIT_BOILERPLATE.sub("", text(bybit.description)).strip() or bybit.title,
                "url": bybit.url,
                **timestamp_fields(
                    preferred_publication(bybit.publishTime, bybit.dateTimestamp), epoch_unit="ms"
                ),
                "tags": bybit.tags,
                "announcement_type": bybit.type,
            }
        )
    elif source_id == "S19":
        press = PressReleaseHit.model_validate(original).source
        material = ArticleDiscovery.model_validate(
            {
                "source_id": "S19",
                "external_id": press.url,
                "title": press.title,
                "text": " ".join(re.sub(r"\bnull\b", " ", press.contentString).split()),
                "url": press.url,
                **timestamp_fields(press.referenceDate),
            }
        )
    elif source_id == "S24":
        proposal = SnapshotProposalEntry.model_validate(original)
        publication = proposal.created if "created" in proposal.model_fields_set else proposal.start
        material = GovernanceDiscovery.model_validate(
            {
                "source_id": "S24",
                "external_id": proposal.id + "|" + proposal.state,
                "proposal_id": proposal.id,
                "state": proposal.state,
                "title": proposal.title,
                "text": proposal.body
                + "\nState: "
                + proposal.state
                + "\nScores: "
                + json.dumps(proposal.scores)
                + "; total: "
                + str(proposal.scores_total),
                "url": "https://snapshot.org/#/" + proposal.space.id + "/proposal/" + proposal.id,
                **timestamp_fields(publication, epoch_unit="s"),
                **timestamp_fields(proposal.end, "end", epoch_unit="s"),
                "scores": proposal.scores,
                "scores_total": proposal.scores_total,
            }
        )
    else:
        raise ValueError(f"No JSON parser for {source_id}")
    return DiscoveryEnvelope(discovery=material, observation=DiscoveryObservation(), payload=None)


def json_items(payload: bytes | str | Mapping[str, JsonValue], source_id: str) -> ParsedDiscoveryBatch:
    """Decode raw JSON once and retain actual admitted discoveries and entry failures."""
    data = (
        decode_unique_json(payload)
        if isinstance(payload, (bytes, str))
        else JsonDocument.validate_value(dict(payload))
    )
    if not isinstance(data, dict):
        raise ValueError("Publisher response must be a JSON object")
    originals = _json_rows(data, source_id)
    discoveries: list[DiscoveryEnvelope] = []
    errors: list[DigestEntryError | DocumentEntryError] = []
    for index, original in enumerate(originals):
        try:
            discoveries.append(_json_discovery(source_id, original))
        except (KeyError, ValueError, TypeError) as exc:
            identity = entry_identity(original)
            errors.append(
                DigestEntryError(
                    entry_index=index,
                    reason="INVALID_PUBLISHER_ENTRY",
                    error=str(exc),
                    entry_sha256=identity.entry_sha256,
                    entry=identity.entry,
                )
            )
    return ParsedDiscoveryBatch(discoveries=discoveries, errors=errors, wire_count=len(originals))


def instruments(
    payload: bytes | JsonValue, feed: FeedParsingSpec | Mapping[str, JsonValue]
) -> dict[str, JsonValue]:
    return parse_instrument_page(payload, _feed_spec(feed)).snapshot.storage_record()


def binance_body(value: JsonValue) -> str:
    """Binance CMS body is a JSON rich-text document, not HTML alone."""
    if isinstance(value, str):
        encoded = value
        try:
            value = decode_unique_json(value)
        except DuplicateJsonKeyError:
            raise
        except (ValueError, TypeError):
            return text(encoded)
    if isinstance(value, list):
        return " ".join(filter(None, (binance_body(v) for v in value)))
    if isinstance(value, dict):
        own = value.get("text", "")
        own = text(own) if isinstance(own, str) else ""
        children = value.get("child", value.get("children", []))
        return " ".join(filter(None, [own, binance_body(children)]))
    return ""
