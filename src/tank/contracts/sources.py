"""Validated publisher configuration, independent of polling and presentation."""

import re
from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel

type SourceAuthorRole = Literal["ORGANIZATION", "FOUNDER_C_LEVEL", "PERSON", "OTHER"]


class SourceLimits(DomainModel):
    rate_per_min: int | None = Field(gt=0)
    budget_fraction: float = Field(gt=0, le=1, allow_inf_nan=False)
    unknown_limit_policy: str = Field(min_length=1)
    documented_rate_per_s: int | None = Field(default=None, gt=0)
    shared_host: str | None = None
    requires_contact_user_agent: bool = False


class EndpointVerification(DomainModel):
    checked_at: str
    status: str
    basis: str = Field(min_length=1)
    baseline_discrepancy: str | None = None


class FeedParsingSpec(DomainModel):
    url: str = Field(min_length=1)
    subtype: Literal["releases", "commits"] | None = None
    article_pattern: str | None = None
    embedded_json_fields: list[str] = Field(default_factory=list)
    venue: Literal["binance", "coinbase", "kraken", "okx", "bybit", "hyperliquid"] | None = None
    market_type: Literal["SPOT", "PERP_LINEAR"] | None = None


class HttpFetchPlan(DomainModel):
    url: str = Field(min_length=1)
    method: Literal["GET", "POST"] = "GET"
    headers: dict[str, str] = Field(default_factory=dict)
    body: dict[str, JsonValue] | None = None
    form: dict[str, str] | None = None
    source_limits: SourceLimits | None = None
    request_weight: float = Field(default=1, gt=0, allow_inf_nan=False)
    requests_per_minute: float | None = Field(default=None, gt=0, allow_inf_nan=False)


class SourceFeed(FeedParsingSpec):
    kind: Literal[
        "rss",
        "atom",
        "feed",
        "html",
        "rendered",
        "rendered_html",
        "html_diff",
        "json",
        "graphql",
        "instruments",
        "x",
    ]
    interval_s: float = Field(gt=0, allow_inf_nan=False)
    method: Literal["GET", "POST"] = "GET"
    author_organization: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    body: dict[str, JsonValue] | None = None
    form: dict[str, str] | None = None
    official_index_url: str | None = None
    request_weight: float = Field(default=1, gt=0, allow_inf_nan=False)
    requests_per_minute: float | None = Field(default=None, gt=0, allow_inf_nan=False)

    def request_plan(self, limits: SourceLimits) -> HttpFetchPlan:
        return HttpFetchPlan(
            url=self.url,
            method=self.method,
            headers=self.headers,
            body=self.body,
            form=self.form,
            source_limits=limits,
            request_weight=self.request_weight,
            requests_per_minute=self.requests_per_minute,
        )

    @model_validator(mode="after")
    def adapter_requirements(self) -> Self:
        parts = urlsplit(self.url)
        if parts.scheme != "https" or not parts.hostname:
            raise ValueError("Configured source feed requires an absolute HTTPS URL")
        if self.kind in {"html", "rendered", "rendered_html", "html_diff"}:
            if self.article_pattern is None:
                raise ValueError("HTML feed requires its article pattern")
            re.compile(self.article_pattern)
        if self.kind == "instruments" and (self.venue is None or self.market_type is None):
            raise ValueError("Instrument feed requires venue and market_type")
        if self.kind == "graphql" and (self.method != "POST" or self.body is None):
            raise ValueError("GraphQL feed requires POST and its request body")
        if self.method != "POST" and (self.body is not None or self.form is not None):
            raise ValueError("Configured request payload requires POST")
        if self.body is not None and self.form is not None:
            raise ValueError("Configured request cannot contain JSON and form payloads together")
        return self


class SourceDefinition(DomainModel):
    source_id: str = Field(pattern=r"^S(?:0[1-9]|[12][0-9]|3[0-4])$")
    name: str = Field(min_length=1)
    author_organization: str | None = None
    mode: Literal["REST"]
    interval_s: float = Field(gt=0, allow_inf_nan=False)
    feeds: list[SourceFeed]
    priority: Literal["CORE", "SECONDARY"]
    terms_notes: str = Field(min_length=1)
    limits: SourceLimits
    bucket: Literal[
        "EXCHANGE_BROKER",
        "GOVERNANCE",
        "HIGH_VALUE_SOCIAL",
        "INSTITUTIONAL_CUSTODY",
        "NEWS_RSS",
        "PROTOCOL_ISSUER",
        "REGULATOR_MARKET_STRUCTURE",
        "TECHNICAL_ARTIFACT",
    ]
    source_class: Literal["PRIMARY", "SECONDARY", "SOCIAL_LEAD", "TECHNICAL_EVIDENCE"]
    cost_class: Literal["FREE_OPEN", "PAID"]
    auth: Literal["NONE", "API_KEY"]
    adapter_status: Literal["NOT_TESTED", "PARTIAL"]
    adapter_status_basis: str = Field(min_length=1)
    detail_url: str | None = None
    endpoint_verification: EndpointVerification | None = None
    paid_gap_ref: str | None = None


class SourceConfiguration(DomainModel):
    version: str = Field(min_length=1)
    contact_email: str = Field(min_length=1)
    sources: list[SourceDefinition]

    @model_validator(mode="after")
    def unique_sources(self) -> Self:
        identities = [source.source_id for source in self.sources]
        if len(set(identities)) != len(identities):
            raise ValueError("Source configuration contains duplicate source IDs")
        return self
