"""Source-specific wire parsers. No network side effects."""

from __future__ import annotations

import json
import re
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlsplit

import feedparser
from selectolax.parser import HTMLParser

from tank.util import iso


def text(html):
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    tree = HTMLParser(str(html or ""))
    for node in tree.css("script,style,nav,footer,header,noscript"):
        node.decompose()
    return " ".join(tree.text(separator=" ").split())


def published(value):
    if value is None:
        return None
    try:
        return iso(value)
    except (ValueError, TypeError):
        try:
            return iso(parsedate_to_datetime(str(value)))
        except (ValueError, TypeError):
            return None


def feeds(payload, source_id, feed):
    parsed = feedparser.parse(payload)
    if not parsed.version:
        raise ValueError("Invalid feed: no RSS or Atom document")
    if parsed.bozo and not parsed.entries:
        raise ValueError(f"Invalid feed: {parsed.bozo_exception}")
    result = []
    for e in parsed.entries:
        link = e.get("link", "")
        body = "\n".join(c.get("value", "") for c in e.get("content", [])) or e.get("summary", "")
        key = link if source_id == "S23" else e.get("id") or e.get("guid") or link
        if not key:
            raise ValueError("Feed entry has neither id nor URL")
        result.append(
            {
                "external_id": key,
                "title": text(e.get("title", "")),
                "text": text(body),
                "url": link,
                "published_at": published(e.get("published") or e.get("updated")),
                "categories": [t.get("term") for t in e.get("tags", [])],
                "subtype": feed.get("subtype") or ("commits" if "/commits/" in feed["url"] else "releases"),
            }
        )
    return result


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
        target = url if pattern.startswith("http") else urlsplit(url).path
        if re.search(pattern, target):
            result[url] = text(a.text())
    # Explicit publisher schema: Robinhood delivers article cards through Next.js
    # Contentful canonicalUrl fields instead of anchors in its server HTML.
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
    return [
        {"external_id": u, "url": u, "title": t, "text": "", "published_at": None} for u, t in result.items()
    ]


def article(payload):
    tree = HTMLParser(payload)
    title = tree.css_first("h1")
    body = tree.css_first("article") or tree.css_first("main") or tree.body
    timestamp = tree.css_first("time[datetime]")
    return {
        "title": text(title.text()) if title else "",
        "text": text(body.html if body else payload),
        "published_at": published(timestamp.attributes["datetime"]) if timestamp else None,
    }


def json_items(payload, source_id):
    data = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
    if source_id == "S08":
        rows = []
        for row in data["results"]:
            url = row["clickUri"]
            if urlsplit(url).netloc != "www.dtcc.com" or not re.match(
                r"^/press-releases/\d{4}/[a-z0-9-]+$", urlsplit(url).path
            ):
                continue
            raw = row.get("raw", {})
            rows.append(
                {
                    "external_id": url,
                    "url": url,
                    "title": raw.get("news_full_name") or row["title"],
                    "text": row.get("excerpt", ""),
                    "published_at": published(raw.get("news_date")),
                    "published_at_precision": "DAY",
                }
            )
        if data["results"] and not rows:
            raise ValueError("DTCC search returned no release URLs")
        return rows
    if source_id == "S03":
        rows = data["results"]
        return [
            {
                "external_id": r["document_number"],
                "title": r["title"],
                "text": text(r.get("abstract") or r.get("excerpts", "")),
                "url": r["html_url"],
                "published_at": published(r.get("publication_date")),
                "published_at_precision": "DAY"
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(r.get("publication_date", "")))
                else "SECOND",
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
                "published_at": published(r.get("releaseDate") or r.get("publishDate")),
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
                "published_at": published(r["pTime"]),
                "announcement_type": r.get("annType"),
                "business_published_at": published(r.get("businessPTime")),
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
                "text": text(r.get("description", "")),
                "url": r["url"],
                "published_at": published(r.get("publishTime") or r.get("dateTimestamp")),
                "tags": r.get("tags"),
                "announcement_type": r.get("type"),
            }
            for r in data["result"]["list"]
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
                "published_at": published(r.get("created", r["start"])),
                "end": published(r["end"]),
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
        children = value.get("children", [])
        return " ".join(filter(None, [text(own), binance_body(children)]))
    return ""
