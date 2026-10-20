"""Configured publisher authority of a source item."""

from urllib.parse import urlparse


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
