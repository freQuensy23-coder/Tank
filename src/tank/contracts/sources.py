"""Validated publisher configuration, independent of polling and presentation."""

import re
from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.time_precision import propagate_timestamp

type SourceAuthorRole = Literal["ORGANIZATION", "FOUNDER_C_LEVEL", "PERSON", "OTHER"]


class SourceLimits(DomainModel):
    rate_per_min: int | None = Field(gt=0)
    budget_fraction: int | float = Field(gt=0, le=1, allow_inf_nan=False)
    unknown_limit_policy: str = Field(min_length=1)
    documented_rate_per_s: int | None = Field(default=None, gt=0)
    shared_host: str | None = None
    requires_contact_user_agent: bool = False


class EndpointVerification(DomainModel):
    checked_at: str
    status: str
    basis: str = Field(min_length=1)
    baseline_discrepancy: str | None = None
    checked_at_prec: str | None = None
    checked_at_precision: str | None = None
    checked_at_prec_reason: str | None = None
    checked_at_precision_reason: str | None = None
    checked_at_reason: str | None = None
    checked_at_bucket_size_s: int | float | str | None = None
    checked_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    @model_validator(mode="after")
    def declared_clock_facets(self) -> Self:
        if any(name.startswith("checked_at_") for name in self.model_fields_set):
            propagate_timestamp(self.model_dump(mode="json", exclude_unset=True), "checked_at")
        return self


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
    request_weight: int | float = Field(default=1, gt=0, allow_inf_nan=False)
    requests_per_minute: int | float | None = Field(default=None, gt=0, allow_inf_nan=False)


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
    interval_s: int | float = Field(gt=0, allow_inf_nan=False)
    method: Literal["GET", "POST"] = "GET"
    author_organization: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    body: dict[str, JsonValue] | None = None
    form: dict[str, str] | None = None
    official_index_url: str | None = None
    request_weight: int | float = Field(default=1, gt=0, allow_inf_nan=False)
    requests_per_minute: int | float | None = Field(default=None, gt=0, allow_inf_nan=False)

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
    interval_s: int | float = Field(gt=0, allow_inf_nan=False)
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


class SourceOfficialAuthority(DomainModel):
    entity_id: str = Field(min_length=1)
    role: str = Field(min_length=1)
    evidence_refs: list[str] = Field(min_length=1)
    authority_scope: str = Field(min_length=1)


class SourceAccount(DomainModel):
    handle: str = Field(min_length=1)
    role: SourceAuthorRole
    entity_id: str | None
    token_id: str | None
    official_authorities: list[SourceOfficialAuthority] | None = None


class SourceAccountRegistryView(DomainModel):
    x_accounts: list[SourceAccount]

    @model_validator(mode="after")
    def unique_handles(self) -> Self:
        handles = [account.handle.lower().lstrip("@") for account in self.x_accounts]
        if any(not handle for handle in handles) or len(handles) != len(set(handles)):
            raise ValueError("Source registry account handles must be distinct nonempty identities")
        return self


class SourceEndpointBudget(DomainModel):
    path: str = Field(min_length=1)
    documented_requests: int = Field(gt=0)
    period_s: int = Field(gt=0)
    tank_request_budget: int = Field(gt=0)
    scope: str = Field(min_length=1)


class SourceProviderBudget(DomainModel):
    provider: str = Field(min_length=1)
    basis: str | None = Field(default=None, min_length=1)
    documented_requests_per_s: int | None = Field(default=None, gt=0)
    documented_requests_per_day: int | None = Field(default=None, gt=0)
    documented_weight_per_min: int | None = Field(default=None, gt=0)
    tank_max_requests_per_s: int | float | None = Field(default=None, gt=0, allow_inf_nan=False)
    tank_max_requests_per_day: int | None = Field(default=None, gt=0)
    tank_max_weight_per_min: int | None = Field(default=None, gt=0)
    documented_requests: int | None = Field(default=None, gt=0)
    period_s: int | None = Field(default=None, gt=0)
    tank_request_budget: int | None = Field(default=None, gt=0)
    minimum_request_spacing_s: int | None = Field(default=None, ge=0)
    observed_header: str | None = None
    source_ref: str | None = None
    source_content_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    verified_at: str | None = None
    endpoint_limits: list[SourceEndpointBudget] | None = None
    request_weights: dict[str, int] | None = None
    response_size_extra_weights: dict[str, str] | None = None
    inference_limits: list[str] | None = None
    retry_delays_s: list[int] | None = None
    api_key_required: bool | None = None
    backend: str | None = None
    client: str | None = None
    cost_usd: int | None = Field(default=None, ge=0)
    scope: str | None = None
    tier: str | None = None

    @model_validator(mode="after")
    def documented_policy(self) -> Self:
        if self.basis is None and self.source_ref is None:
            raise ValueError("A provider budget requires declared policy or official source evidence")
        if self.request_weights is not None and any(weight <= 0 for weight in self.request_weights.values()):
            raise ValueError("Documented operation request weights must be positive")
        if self.retry_delays_s is not None and any(delay <= 0 for delay in self.retry_delays_s):
            raise ValueError("Provider retry delays must be positive")
        return self


class SourceApiLimitsConfiguration(DomainModel):
    version: str = Field(min_length=1)
    policy: str = Field(min_length=1)
    limits: list[SourceProviderBudget]

    @model_validator(mode="after")
    def distinct_providers(self) -> Self:
        providers = [rule.provider for rule in self.limits]
        if len(providers) != len(set(providers)):
            raise ValueError("API limit configuration requires distinct provider rules")
        return self
