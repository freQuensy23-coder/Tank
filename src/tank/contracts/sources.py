"""Validated publisher configuration, independent of polling and presentation."""

import re
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.time_precision import normalize_precision, propagate_timestamp
from tank.util import iso, parse_iso_time, parse_utc_time

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
        facets = (
            self.checked_at_prec,
            self.checked_at_precision,
            self.checked_at_prec_reason,
            self.checked_at_precision_reason,
            self.checked_at_reason,
            self.checked_at_bucket_size_s,
            self.checked_at_bucket_anchor,
        )
        if not any(value is not None for value in facets):
            return self
        declared = self.model_dump(mode="json", exclude_unset=True)
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", self.checked_at):
            precisions = [
                value for value in (self.checked_at_prec, self.checked_at_precision) if value is not None
            ]
            if not precisions:
                if self.checked_at_bucket_size_s is not None or self.checked_at_bucket_anchor is not None:
                    raise ValueError("Verification bucket metadata requires declared precision")
                return self
            size = (self.checked_at_bucket_size_s, "s") if self.checked_at_bucket_size_s is not None else None
            if any(normalize_precision(value, bucket_size=size) != "BUCKET(1d)" for value in precisions):
                raise ValueError("Calendar verification date requires a day-bucket declaration")
            declared["checked_at"] = iso(parse_utc_time(self.checked_at))
        else:
            parse_iso_time(self.checked_at)
        propagate_timestamp(declared, "checked_at")
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

    @model_validator(mode="after")
    def half_documented_capacity(self) -> Self:
        if 2 * self.tank_request_budget > self.documented_requests:
            raise ValueError("Endpoint budget exceeds half the documented capacity")
        return self


class SourceBudgetEvidence(DomainModel):
    basis: str | None = Field(default=None, min_length=1)
    source_ref: str | None = Field(default=None, min_length=1)
    source_content_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    verified_at: str | None = Field(default=None, min_length=1)
    inference_limits: list[str] | None = None

    @model_validator(mode="after")
    def declared_evidence(self) -> Self:
        if not (
            self.basis is not None
            and self.basis.strip()
            or self.source_ref is not None
            and self.source_ref.strip()
        ):
            raise ValueError("Provider budget requires declared policy or official source evidence")
        return self


class DocumentedRequestBudget(DomainModel):
    documented_requests_per_s: int = Field(gt=0)
    tank_max_requests_per_s: int | float = Field(gt=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def within_documented_request_capacity(self) -> Self:
        if 2 * self.tank_max_requests_per_s > self.documented_requests_per_s:
            raise ValueError("Provider budget exceeds half the documented capacity")
        return self


class DocumentedWeightBudget(DomainModel):
    documented_weight_per_min: int = Field(gt=0)
    tank_max_weight_per_min: int = Field(gt=0)

    @model_validator(mode="after")
    def within_documented_weight_capacity(self) -> Self:
        if 2 * self.tank_max_weight_per_min > self.documented_weight_per_min:
            raise ValueError("Provider budget exceeds half the documented capacity")
        return self


class SourceSecBudget(SourceBudgetEvidence, DocumentedRequestBudget):
    provider: Literal["sec.gov"]
    basis: str = Field(min_length=1)


class SourceUncapturedRpsBudget(SourceBudgetEvidence):
    provider: Literal["coinbase", "kraken"]
    basis: str = Field(min_length=1)
    tank_max_requests_per_s: int | float = Field(gt=0, allow_inf_nan=False)
    documented_requests_per_s: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def compare_only_declared_documentation(self) -> Self:
        if (
            self.documented_requests_per_s is not None
            and 2 * self.tank_max_requests_per_s > self.documented_requests_per_s
        ):
            raise ValueError("Provider budget exceeds half the documented capacity")
        return self


class SourceClientProtectionLimit(DomainModel):
    bucket: Literal["binance.usdm.funding", "binance.usdm.statistics"]
    paths: list[Literal["/fapi/v1/fundingRate", "/fapi/v1/fundingInfo", "/futures/data/openInterestHist"]] = (
        Field(min_length=1)
    )
    policy_kind: Literal["CLIENT_PROTECTION"]
    documentation_status: Literal["NOT_CAPTURED"]
    documented_requests: None
    period_s: int = Field(gt=0)
    tank_request_budget: int = Field(gt=0)
    scope: Literal["IP"]
    basis: str = Field(min_length=1)

    @model_validator(mode="after")
    def exact_operation_group(self) -> Self:
        expected = (
            {"/fapi/v1/fundingRate", "/fapi/v1/fundingInfo"}
            if self.bucket == "binance.usdm.funding"
            else {"/futures/data/openInterestHist"}
        )
        if set(self.paths) != expected or len(self.paths) != len(expected):
            raise ValueError("Client protection bucket must retain its exact operation group")
        return self


class SourceBinanceBudget(SourceBudgetEvidence, DocumentedWeightBudget):
    basis: str = Field(min_length=1)


class SourceBinanceSpotBudget(SourceBinanceBudget):
    provider: Literal["binance.spot"]
    observed_header: str | None = Field(default=None, min_length=1)


class SourceBinanceUsdmBudget(SourceBinanceBudget):
    provider: Literal["binance.usdm"]
    client_protection_limits: list[SourceClientProtectionLimit] = Field(min_length=2, max_length=2)

    @model_validator(mode="after")
    def required_operation_protection(self) -> Self:
        if {rule.bucket for rule in self.client_protection_limits} != {
            "binance.usdm.funding",
            "binance.usdm.statistics",
        }:
            raise ValueError("USD-M requires both existing operation-protection windows")
        return self


class SourceOkxBudget(SourceBudgetEvidence):
    provider: Literal["okx"]
    basis: str = Field(min_length=1)
    source_ref: str = Field(min_length=1)
    source_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    verified_at: str = Field(min_length=1)
    endpoint_limits: list[SourceEndpointBudget] = Field(min_length=1)


class SourceBybitBudget(SourceBudgetEvidence):
    provider: Literal["bybit"]
    basis: str = Field(min_length=1)
    source_ref: str = Field(min_length=1)
    verified_at: str = Field(min_length=1)
    documented_requests: int = Field(gt=0)
    period_s: int = Field(gt=0)
    tank_request_budget: int = Field(gt=0)
    scope: Literal["IP"]

    @model_validator(mode="after")
    def within_documented_period_capacity(self) -> Self:
        if 2 * self.tank_request_budget > self.documented_requests:
            raise ValueError("Provider budget exceeds half the documented capacity")
        return self


class SourceHyperliquidBudget(SourceBudgetEvidence, DocumentedWeightBudget):
    provider: Literal["hyperliquid"]
    basis: str = Field(min_length=1)
    source_ref: str = Field(min_length=1)
    verified_at: str = Field(min_length=1)
    scope: Literal["IP"]
    request_weights: dict[str, int] = Field(min_length=1)
    response_size_extra_weights: dict[str, str] = Field(min_length=1)

    @model_validator(mode="after")
    def usable_operation_weights(self) -> Self:
        if any(not operation.strip() or weight <= 0 for operation, weight in self.request_weights.items()):
            raise ValueError("Documented operation request weights must have names and positive values")
        if any(
            not operation.strip() or not rule.strip()
            for operation, rule in self.response_size_extra_weights.items()
        ):
            raise ValueError("Response size weight rules require operation names and declarations")
        return self


class SourceDuckDuckGoBudget(SourceBudgetEvidence):
    provider: Literal["duckduckgo"]
    basis: str = Field(min_length=1)
    client: str = Field(min_length=1)
    backend: Literal["duckduckgo"]
    minimum_request_spacing_s: int = Field(ge=0)
    retry_delays_s: list[Annotated[int, Field(gt=0)]] = Field(min_length=1)
    cost_usd: int = Field(ge=0, le=0)
    api_key_required: bool

    @model_validator(mode="after")
    def free_keyless_backend(self) -> Self:
        if self.api_key_required:
            raise ValueError("Configured free search backend cannot require an API key")
        return self


class SourceEtherscanBudget(SourceBudgetEvidence, DocumentedRequestBudget):
    provider: Literal["etherscan"]
    source_ref: str = Field(min_length=1)
    verified_at: str = Field(min_length=1)
    tier: Literal["FREE"]
    scope: Literal["API_KEY"]
    documented_requests_per_day: int = Field(gt=0)
    tank_max_requests_per_day: int = Field(gt=0)

    @model_validator(mode="after")
    def within_documented_daily_capacity(self) -> Self:
        if 2 * self.tank_max_requests_per_day > self.documented_requests_per_day:
            raise ValueError("Provider budget exceeds half the documented capacity")
        return self


class SourceClientProtectionBudget(SourceBudgetEvidence):
    provider: Literal["binance.www", "binance.vision"]
    host: Literal["www.binance.com", "data.binance.vision"]
    policy_kind: Literal["CLIENT_PROTECTION"]
    documentation_status: Literal["NOT_CAPTURED"]
    documented_requests: None
    basis: str = Field(min_length=1)
    scope: Literal["IP"]
    period_s: int = Field(gt=0)
    tank_request_budget: int = Field(gt=0)

    @model_validator(mode="after")
    def exact_public_host(self) -> Self:
        expected = "www.binance.com" if self.provider == "binance.www" else "data.binance.vision"
        if self.host != expected:
            raise ValueError("Client protection provider must match its declared public host")
        return self


type SourceProviderBudget = Annotated[
    SourceSecBudget
    | SourceUncapturedRpsBudget
    | SourceBinanceSpotBudget
    | SourceBinanceUsdmBudget
    | SourceOkxBudget
    | SourceBybitBudget
    | SourceHyperliquidBudget
    | SourceDuckDuckGoBudget
    | SourceEtherscanBudget
    | SourceClientProtectionBudget,
    Field(discriminator="provider"),
]


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


class FilingTokenLabel(DomainModel):
    name: str
    symbol: str = Field(min_length=1)


class FilingEntityLabel(DomainModel):
    kind: Literal["PROTOCOL", "OTHER", "EXCHANGE", "COMPANY", "REGULATOR"]
    names: list[str]
    aliases: list[str]
    name: str | None


class FilingRegistryDocument(DomainModel):
    """Only the two external JSON namespaces needed to project filing labels."""

    tokens: list[dict[str, JsonValue]]
    entities: list[dict[str, JsonValue]]


class SourceFilingRegistryView(DomainModel):
    tokens: list[FilingTokenLabel]
    entities: list[FilingEntityLabel]

    @classmethod
    def from_document(cls, document: dict[str, JsonValue]) -> Self:
        wire = FilingRegistryDocument.model_validate(
            {key: document[key] for key in ("tokens", "entities") if key in document}
        )
        token_labels = [
            FilingTokenLabel.model_validate({key: token[key] for key in ("name", "symbol") if key in token})
            for token in wire.tokens
        ]
        entity_labels = [
            FilingEntityLabel.model_validate(
                {
                    **{key: entity[key] for key in ("kind", "names", "aliases") if key in entity},
                    "name": entity.get("name"),
                }
            )
            for entity in wire.entities
        ]
        return cls(tokens=token_labels, entities=entity_labels)


class AccountProbeResponse(DomainModel):
    handle: str = Field(min_length=1)
    http_status: int = Field(ge=100, le=599)
    exists: None = None
    identity_status: Literal["REQUIRES_OWNER_EVIDENCE"] = "REQUIRES_OWNER_EVIDENCE"
    role: SourceAuthorRole


class AccountProbeFailure(DomainModel):
    handle: str = Field(min_length=1)
    exists: None = None
    error: str = Field(min_length=1)


type AccountProbeResult = AccountProbeResponse | AccountProbeFailure
