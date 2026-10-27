"""Publisher parser results retain admitted discoveries before acquisition effects."""

from typing import Self

from pydantic import Field, model_validator

from tank.contracts import DomainModel
from tank.contracts.source_errors import DigestEntryError, DocumentEntryError, EntryIdentity
from tank.contracts.source_items import DiscoveryEnvelope, PublisherCMSIdentity


class ParsedDiscoveryBatch(DomainModel):
    discoveries: list[DiscoveryEnvelope]
    errors: list[DigestEntryError | DocumentEntryError]
    wire_count: int = Field(ge=0)


class ArticleLinkListing(DomainModel):
    """A discovered link before detail acquisition reports any body or publication clock."""

    external_id: str = Field(min_length=1)
    url: str = Field(min_length=1)
    title: str | None
    publisher_cms_identity: PublisherCMSIdentity | None = None
    publisher_cms_slug_url: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def actual_cms_binding(self) -> Self:
        if (self.publisher_cms_identity is None) != (self.publisher_cms_slug_url is None):
            raise ValueError("Article listing requires both observed CMS identity and its bound slug URL")
        return self


class PublisherEntryIdentity(DomainModel):
    entry_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    entry: EntryIdentity
