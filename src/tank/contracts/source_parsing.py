"""Publisher parser results retain admitted discoveries before acquisition effects."""

from typing import Annotated

from pydantic import BeforeValidator, ConfigDict, Field, JsonValue

from tank.contracts import DomainModel
from tank.contracts.scalars import require_number
from tank.contracts.source_errors import DigestEntryError, DocumentEntryError, EntryIdentity
from tank.contracts.source_items import (
    AnnouncementLabel,
    DiscoveryEnvelope,
    IssuingAgency,
)
from tank.contracts.source_items import (
    ArticleLinkListing as ArticleLinkListing,
)
from tank.time_precision import Timestamp


class ParsedArticle(DomainModel):
    title: str
    text: str
    publication: Timestamp
    publication_basis: str | None
    publication_missing_reason: str | None

    def storage_record(self) -> dict[str, str | int | None]:
        record = self.publication.fields("published_at")
        return {
            "title": self.title,
            "text": self.text,
            **record,
            "published_at_precision": self.publication.precision,
            "published_at_basis": self.publication_basis,
            "published_at_missing_reason": self.publication_missing_reason,
        }


class ContentfulArticleFields(DomainModel):
    model_config = ConfigDict(extra="ignore")

    title: str | None = None
    content: JsonValue


class ContentfulArticle(DomainModel):
    model_config = ConfigDict(extra="ignore")

    fields: ContentfulArticleFields


class ContentfulArticlePage(DomainModel):
    model_config = ConfigDict(extra="ignore")

    article: ContentfulArticle


class ContentfulPageProps(DomainModel):
    model_config = ConfigDict(extra="ignore")

    pageProps: ContentfulArticlePage


class ContentfulDocument(DomainModel):
    model_config = ConfigDict(extra="ignore")

    props: ContentfulPageProps


class ParsedDiscoveryBatch(DomainModel):
    discoveries: list[DiscoveryEnvelope]
    errors: list[DigestEntryError | DocumentEntryError]
    wire_count: int = Field(ge=0)


class PublisherEntryIdentity(DomainModel):
    entry_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    entry: EntryIdentity


type ReportedNumber = Annotated[int | float, BeforeValidator(require_number)]
type ReportedPublication = str | ReportedNumber | None


class FederalRegisterEntry(DomainModel):
    model_config = ConfigDict(extra="ignore")

    document_number: str = Field(min_length=1, pattern=r"\S")
    title: str
    html_url: str
    abstract: str | None = None
    excerpts: str | None = None
    publication_date: ReportedPublication = None
    type: str | None = None
    pdf_url: str | None = None
    agencies: list[IssuingAgency] | None = None


class BinanceCmsEntry(DomainModel):
    model_config = ConfigDict(extra="ignore")

    code: str = Field(min_length=1, pattern=r"\S")
    title: str
    body: str | None = None
    releaseDate: ReportedPublication = None
    publishDate: ReportedPublication = None


class OkxAnnouncementEntry(DomainModel):
    model_config = ConfigDict(extra="ignore")

    url: str = Field(min_length=1, pattern=r"\S")
    title: str
    pTime: str
    annType: int | str | None = None
    businessPTime: str | None = None


class BybitAnnouncementEntry(DomainModel):
    model_config = ConfigDict(extra="ignore")

    url: str = Field(min_length=1, pattern=r"\S")
    title: str
    description: str | None = None
    type: AnnouncementLabel | int | str | None = None
    tags: list[str | AnnouncementLabel] | None = None
    publishTime: ReportedPublication = None
    dateTimestamp: ReportedPublication = None


class PressReleaseEntry(DomainModel):
    model_config = ConfigDict(extra="ignore")

    url: str = Field(min_length=1, pattern=r"\S")
    title: str
    contentString: str
    referenceDate: ReportedPublication


class PressReleaseHit(DomainModel):
    model_config = ConfigDict(extra="ignore")

    source: PressReleaseEntry = Field(alias="_source")


class SnapshotProposalSpace(DomainModel):
    model_config = ConfigDict(extra="ignore")

    id: str = Field(min_length=1, pattern=r"\S")


class SnapshotProposalEntry(DomainModel):
    model_config = ConfigDict(extra="ignore")

    id: str = Field(min_length=1, pattern=r"\S")
    title: str
    body: str
    state: str = Field(min_length=1, pattern=r"\S")
    start: ReportedNumber
    end: ReportedNumber
    created: ReportedNumber | None = None
    scores: list[ReportedNumber]
    scores_total: ReportedNumber
    space: SnapshotProposalSpace


class BinanceArticleDetail(DomainModel):
    model_config = ConfigDict(extra="ignore")

    body: JsonValue
    publishDate: ReportedNumber | None = None


class BinanceArticleDetailReply(DomainModel):
    model_config = ConfigDict(extra="ignore")

    data: BinanceArticleDetail
