"""Configured publisher authority of a source item."""

from collections.abc import Mapping
from urllib.parse import urlparse

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.contracts.source_items import IssuingAgency
from tank.contracts.sources import SourceAccount, SourceDefinition, SourceFeed


class PublisherContext(DomainModel):
    url: str
    feed: str
    author_organization: str | None = Field(default=None, min_length=1)
    monitored_author: str | None = None
    author: str | None = None
    agencies: list[IssuingAgency] | None = None


def _retained_publisher_organization(
    source: SourceDefinition, item: PublisherContext, feeds: list[SourceFeed]
) -> str | None:
    """Issuing agency or repository publisher; never a contributor's employer."""
    if len(feeds) != 1:
        return None
    feed = feeds[0]
    endpoint = urlparse(feed.url)
    page = urlparse(item.url)
    if source.source_id == "S03":
        if (
            feed.kind != "json"
            or endpoint.scheme != "https"
            or endpoint.netloc not in {"www.federalregister.gov", "federalregister.gov"}
            or endpoint.path != "/api/v1/documents.json"
            or page.scheme != "https"
            or page.netloc not in {"www.federalregister.gov", "federalregister.gov"}
        ):
            return None
        agencies = item.agencies
        if agencies is None or not agencies:
            return None
        names: list[str] = []
        for agency in agencies:
            name = agency.name if agency.name is not None and agency.name.strip() else agency.raw_name
            if name is None or not name.strip():
                return None
            if name.strip().casefold() not in {value.casefold() for value in names}:
                names.append(name.strip())
        return "; ".join(names)
    if source.source_id == "S25":
        parts = endpoint.path.strip("/").split("/")
        target = page.path.strip("/").split("/")
        if (
            feed.kind != "atom"
            or feed.subtype not in {"releases", "commits"}
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


def resolve_author_organization(
    source: SourceDefinition,
    item: Mapping[str, JsonValue],
    accounts: Mapping[str, SourceAccount],
) -> str | None:
    """Validate the captured-record projection before resolving declared publisher authority."""
    context = PublisherContext.model_validate(
        {
            name: item[name]
            for name in ("url", "feed", "author_organization", "monitored_author", "author", "agencies")
            if name in item
        }
    )
    return publisher_organization(source, context, accounts)


def publisher_organization(
    source: SourceDefinition,
    item: PublisherContext,
    accounts: Mapping[str, SourceAccount],
) -> str | None:
    """The ingestion precedence; never infer authority from a display name."""
    if item.author_organization is not None:
        return item.author_organization
    feeds = [feed for feed in source.feeds if feed.url == item.feed]
    if len(feeds) > 1:
        raise ValueError("Ambiguous configured publisher feed")
    configured = feeds[0].author_organization if feeds else None
    if configured is None:
        configured = source.author_organization
    if configured is not None:
        return configured
    publisher = _retained_publisher_organization(source, item, feeds)
    if publisher is not None:
        return publisher
    if source.source_id == "S29":
        author = item.monitored_author if item.monitored_author is not None else item.author
        if author is None:
            return None
        account = accounts.get(author.lower().lstrip("@"))
        return account.entity_id if account is not None else None
    return None
