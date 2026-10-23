"""Source-specific wire parsers. No network side effects."""

import json
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlsplit

import feedparser
from selectolax.parser import HTMLParser

from tank.time_precision import Timestamp
from tank.util import iso, sha256


def text(html):
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    tree = HTMLParser(str(html or ""))
    for node in tree.css("script,style,nav,footer,header,noscript"):
        node.decompose()
    return " ".join(tree.text(separator=" ").split())


def published(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, (int, float)) or re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", str(value)):
            return iso(value)
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value)):
            return iso(value)
        try:
            instant = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        except ValueError:
            instant = parsedate_to_datetime(str(value))

        return iso(instant) if instant.tzinfo is not None else None
    except (ValueError, TypeError, OverflowError):
        return None


def timestamp_fields(value, name="published_at", *, date_only=False):
    """Read a provider field's declared calendar day or timestamp semantics."""
    normalized = published(value)
    day = date_only or bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value or "")))
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


def feeds(payload, source_id, feed):
    parsed = feedparser.parse(payload)
    if not parsed.version:
        raise ValueError("Invalid feed: no RSS or Atom document")
    if parsed.bozo and not parsed.entries:
        raise ValueError(f"Invalid feed: {parsed.bozo_exception}")
    result = ParsedRows(wire_count=len(parsed.entries))
    if parsed.bozo:
        result.errors.append({"reason": "MALFORMED_FEED_DOCUMENT", "error": str(parsed.bozo_exception)})
    for index, e in enumerate(parsed.entries):
        link = e.get("link", "")
        body = "\n".join(c.get("value", "") for c in e.get("content", [])) or e.get("summary", "")
        key = link if source_id == "S23" else e.get("id") or e.get("guid") or link
        if not key:
            result.errors.append({"entry_index": index, "reason": "ENTRY_WITHOUT_ID_OR_URL"})
            continue
        result.append(
            {
                "external_id": key,
                "title": text(e.get("title", "")),
                "text": text(body),
                "url": link,
                **timestamp_fields(dict(e).get("published")),
                **(
                    timestamp_fields(dict(e)["updated"], "source_updated_at")
                    if dict(e).get("updated")
                    else {}
                ),
                "categories": [t.get("term") for t in e.get("tags", [])],
                "subtype": feed.get("subtype") or ("commits" if "/commits/" in feed["url"] else "releases"),
            }
        )
    return result


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
    pattern = feed.get("article_pattern") or feed.get("pattern")
    if not pattern:
        raise ValueError("HTML feed needs article_pattern")
    result = {}
    tree = HTMLParser(payload)
    for a in tree.css("a[href]"):
        url = urljoin(feed["url"], a.attributes["href"])
        if urlsplit(url).netloc != urlsplit(feed["url"]).netloc:
            continue
        if urlsplit(feed["url"]).netloc == "www.dtcc.com" and not _dtcc_release_url(url):
            continue
        target = url if pattern.startswith("http") else urlsplit(url).path
        if re.search(pattern, target):
            result[url] = text(a.text())

    fields = feed.get("embedded_json_fields", [])
    if fields:
        script = tree.css_first('script#__NEXT_DATA__[type="application/json"]')
        if script is None:
            raise ValueError("Configured __NEXT_DATA__ article metadata missing")

        def visit(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    if key in fields and isinstance(child, str):
                        url = urljoin(feed["url"], child)
                        if urlsplit(url).netloc == urlsplit(feed["url"]).netloc:
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
                    feed["url"].rstrip("/") + "/", candidates[0]["slug"] + "/"
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
                    timestamp = datetime.strptime(next(iter(dates)), "%b %d, %Y").date().isoformat()
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
    fields = timestamp_fields(timestamp, date_only=basis == "BLACKROCK_CALENDAR_PUBLICATION_META")
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
    data = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
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
        if data.get("retCode", 0) != 0:
            raise ValueError("Bybit API error: " + str(data.get("retCode")))
        rows = data["result"]["list"]

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
                {"entry_index": index, "reason": "INVALID_PUBLISHER_ENTRY", "error": str(exc)}
            )
    return parsed


def _json_items(payload, source_id):
    data = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
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
                **timestamp_fields(r.get("releaseDate") or r.get("publishDate")),
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
                **timestamp_fields(r["pTime"]),
                "announcement_type": r.get("annType"),
                **timestamp_fields(r.get("businessPTime"), "business_published_at"),
            }
            for r in rows
        ]
    if source_id == "S11":
        if data.get("retCode", 0) != 0:
            raise ValueError("Bybit API error: " + str(data.get("retCode")))
        return [
            {
                "external_id": r["url"],
                "title": r["title"],
                "text": BYBIT_BOILERPLATE.sub("", text(r.get("description", ""))).strip() or r["title"],
                "url": r["url"],
                **timestamp_fields(r.get("publishTime") or r.get("dateTimestamp")),
                "tags": r.get("tags"),
                "announcement_type": r.get("type"),
            }
            for r in data["result"]["list"]
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
                **timestamp_fields(r.get("created", r["start"])),
                **timestamp_fields(r["end"], "end"),
                "scores": r["scores"],
                "scores_total": r["scores_total"],
            }
            for r in data["data"]["proposals"]
        ]
    raise ValueError(f"No JSON parser for {source_id}")


def instruments(payload, feed):
    data = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
    venue = str(feed.get("venue", "")).lower()
    kind = feed.get("market_type", "")
    if venue == "binance":
        rows = data["symbols"]
        fields = ("symbol", "baseAsset", "quoteAsset", "status")
    elif venue == "coinbase":
        rows = data
        fields = ("id", "base_currency", "quote_currency", "status")
    elif venue == "kraken":
        if data.get("error"):
            raise ValueError(str(data["error"]))
        rows = [dict(r, symbol=k) for k, r in data["result"].items()]
        fields = ("symbol", "base", "quote", "status")
    elif venue == "okx":
        if str(data.get("code", "0")) != "0":
            raise ValueError(str(data))
        rows = data["data"]
        fields = ("instId", "baseCcy", "quoteCcy", "state")
    elif venue == "bybit":
        if data.get("retCode", 0) != 0:
            raise ValueError(str(data))
        rows = data["result"]["list"]
        fields = ("symbol", "baseCoin", "quoteCoin", "status")
    elif venue == "hyperliquid":
        rows = data["universe"]
        fields = ("name", "name", "quote", "status")
    else:
        raise ValueError("Unknown instrument venue " + venue)
    result = {}
    for r in rows:
        symbol = r[fields[0]]
        status = r.get(fields[3])
        if venue == "hyperliquid":
            status = "DELISTED" if r.get("isDelisted") else "TRADING"
        if venue == "coinbase" and r.get("trading_disabled"):
            status = "TRADING_DISABLED"
        result[symbol] = {
            "venue": venue,
            "market_type": kind,
            "symbol": symbol,
            "base": r.get(fields[1]),
            "quote": r.get(fields[2]),
            "status": status,
            "contract_type": r.get("contractType") or r.get("ctType"),
        }
        if venue == "coinbase":
            result[symbol]["status_flags"] = {
                field: r[field]
                for field in ("trading_disabled", "cancel_only", "post_only", "limit_only", "auction_mode")
                if field in r
            }
    return result


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
