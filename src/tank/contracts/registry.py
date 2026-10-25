"""Closed captured configuration contracts for registry bootstrap and domain projections."""

import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, field_validator, model_validator

from tank.contracts import DomainModel
from tank.contracts.sources import SourceAccount, SourceAccountRegistryView
from tank.time_precision import BucketSize, propagate_timestamp, reconcile_precision


class CalendarReviewMetadata(DomainModel):
    precision: str | None
    precision_reason: str | None
    bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None

    @model_validator(mode="after")
    def declared_precision(self) -> Self:
        if self.precision is None:
            if self.precision_reason is None or not self.precision_reason.strip():
                raise ValueError("Unknown calendar review precision requires a reason")
        elif reconcile_precision([self.precision], field="calendar_review") != self.precision:
            raise ValueError("Calendar review precision requires canonical metadata")
        if self.bucket_anchor is not None and (
            self.precision is None or not self.precision.startswith("BUCKET(")
        ):
            raise ValueError("Calendar review bucket anchor requires bucket precision")
        return self

    def fields(self, field: str) -> dict[str, str | None]:
        result = {field + "_prec": self.precision, field + "_prec_reason": self.precision_reason}
        if self.bucket_anchor is not None:
            result[field + "_bucket_anchor"] = self.bucket_anchor
        return result


def calendar_review_metadata(record: dict[str, JsonValue], field: str) -> CalendarReviewMetadata | None:
    """Validate a calendar review label without converting it to a timezone-bearing occurrence."""
    raw = record[field]
    if not isinstance(raw, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw) is None:
        return None
    try:
        date.fromisoformat(raw)
    except ValueError:
        raise ValueError("Invalid calendar review date") from None
    declarations: list[str] = []
    reasons: list[str] = []
    for name in (field + "_prec", field + "_precision"):
        declaration = record.get(name)
        if declaration is not None:
            if not isinstance(declaration, str):
                raise ValueError("Calendar review precision requires text")
            declarations.append(declaration)
        reason = record.get(name + "_reason")
        if reason is not None:
            if not isinstance(reason, str) or not reason.strip():
                raise ValueError("Calendar review precision reason requires nonempty text")
            reasons.append(reason)
    reason = record.get(field + "_reason")
    if reason is not None:
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("Calendar review precision reason requires nonempty text")
        reasons.append(reason)
    size = record.get(field + "_bucket_size_s")
    bucket_size: BucketSize | None = None
    if size is not None:
        if isinstance(size, bool) or not isinstance(size, (str, int, float)):
            raise ValueError("Calendar review bucket size requires seconds")
        bucket_size = (size, "s")
    precision = reconcile_precision(declarations, field=field, bucket_size=bucket_size)
    anchor = record.get(field + "_bucket_anchor")
    if anchor is not None and (precision is None or not precision.startswith("BUCKET(")):
        raise ValueError("Calendar review bucket anchor requires bucket precision")
    return CalendarReviewMetadata.model_validate(
        {
            "precision": precision,
            "precision_reason": None if precision is not None else reasons[0] if reasons else "INCOMPLETE",
            "bucket_anchor": anchor,
        }
    )


def validate_review_clock(record: dict[str, JsonValue], field: str) -> None:
    if calendar_review_metadata(record, field) is None:
        propagate_timestamp(record, field)


type RegistryVenue = Literal["binance", "coinbase", "okx", "bybit", "kraken", "hyperliquid"]
type RegistryMarketType = Literal["SPOT", "PERP_LINEAR"]
type MetricSupport = Literal["SUPPORTED", "NOT_SUPPORTED"]
type RegistryLayer = Literal[
    "CUSTODY_COMPLIANCE",
    "INTEROPERABILITY",
    "ISSUER_PRODUCT",
    "LENDING_COLLATERAL",
    "LIQUIDITY_EXECUTION",
    "ORACLE_DATA",
    "SETTLEMENT_L1_L2",
    "VENUE_DEX",
]


class TokenContract(DomainModel):
    chain: str
    address: str
    chain_id: int | None
    decimals: int | None = Field(ge=0)
    chain_id_evidence: str | None = None


class FundamentalsMetrics(DomainModel):
    dailyFees: MetricSupport
    dailyRevenue: MetricSupport
    dailyHoldersRevenue: MetricSupport


class FundamentalsEvidence(DomainModel):
    catalog_url: str
    summary_id: str | None
    verified_at: str
    metrics: FundamentalsMetrics
    reason: str | None
    verified_at_prec: str | None = None
    verified_at_precision: str | None = None
    verified_at_prec_reason: str | None = None
    verified_at_precision_reason: str | None = None
    verified_at_reason: str | None = None
    verified_at_bucket_size_s: str | int | float | None = None
    verified_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    @model_validator(mode="after")
    def declared_clock(self) -> Self:
        validate_review_clock(
            {
                "verified_at": self.verified_at,
                "verified_at_prec": self.verified_at_prec,
                "verified_at_precision": self.verified_at_precision,
                "verified_at_prec_reason": self.verified_at_prec_reason,
                "verified_at_precision_reason": self.verified_at_precision_reason,
                "verified_at_reason": self.verified_at_reason,
                "verified_at_bucket_size_s": self.verified_at_bucket_size_s,
                "verified_at_bucket_anchor": self.verified_at_bucket_anchor,
            },
            "verified_at",
        )
        return self


class RegistryToken(DomainModel):
    token_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    name: str
    project: str
    entity_id: str
    identity: str
    contracts: list[TokenContract]
    native: bool
    coingecko_id: str
    unlock_slug: str | None
    supply_unavailable_reason: str | None
    official_accounts: list[str]
    sector: str
    defillama_protocol_slugs: list[str]
    defillama_protocol_slug: str | None
    fee_protocol_slug: str | None
    fundamentals_status: MetricSupport
    fundamentals_evidence: FundamentalsEvidence
    defillama_fee_scope: Literal[
        "NATIVE_CHAIN_FEES", "PROVIDER_PARENT_ROLLUP", "NOT_SUPPORTED", "EXACT_PROTOCOL"
    ]
    native_chain_id: int | None
    defillama_chain_name: str | None
    chain_identity_evidence: str | None
    fundamentals_inference_limits: list[str] | None = None
    supplementary_chain_fee_slug: str | None = None
    supplementary_chain_fee_warning: str | None = None


class RegistryInstrument(DomainModel):
    instrument_id: str = Field(min_length=1)
    token_id: str
    venue: RegistryVenue
    market_type: RegistryMarketType
    venue_symbol: str
    quote_asset: str
    active: bool
    contract_multiplier: Decimal | None
    tick_size: Decimal | None
    lot_size: Decimal | None
    funding_interval_h: int | None
    metadata_status: str
    resolved_at: str | None
    coverage_verified_date: str

    resolved_at_prec: str | None = None
    resolved_at_precision: str | None = None
    resolved_at_prec_reason: str | None = None
    resolved_at_precision_reason: str | None = None
    resolved_at_reason: str | None = None
    resolved_at_bucket_size_s: str | int | float | None = None
    resolved_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    @model_validator(mode="after")
    def declared_clock(self) -> Self:
        validate_review_clock(
            {
                "resolved_at": self.resolved_at,
                "resolved_at_prec": self.resolved_at_prec,
                "resolved_at_precision": self.resolved_at_precision,
                "resolved_at_prec_reason": self.resolved_at_prec_reason,
                "resolved_at_precision_reason": self.resolved_at_precision_reason,
                "resolved_at_reason": self.resolved_at_reason,
                "resolved_at_bucket_size_s": self.resolved_at_bucket_size_s,
                "resolved_at_bucket_anchor": self.resolved_at_bucket_anchor,
            },
            "resolved_at",
        )
        return self

    @field_validator("contract_multiplier", "tick_size", "lot_size", mode="before")
    @classmethod
    def declared_decimal(cls, value: Decimal | str | None) -> Decimal | None:
        if value is None or isinstance(value, Decimal):
            return value
        if not isinstance(value, str):
            raise ValueError("Instrument decimal metadata requires declared decimal text")
        try:
            return Decimal(value)
        except InvalidOperation:
            raise ValueError("Instrument decimal metadata is invalid") from None


class RegistryQualifiedAlias(DomainModel):
    alias: str = Field(min_length=1)
    requires_any: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def nonempty_terms(self) -> Self:
        if not self.alias.strip() or any(not term.strip() for term in self.requires_any):
            raise ValueError("Qualified aliases require nonempty terms")
        return self


class RegistryEntity(DomainModel):
    entity_id: str = Field(min_length=1)
    kind: Literal["PROTOCOL", "OTHER", "EXCHANGE", "COMPANY", "REGULATOR"]
    names: list[str]
    aliases: list[str]
    tokens: list[str]
    official_sources: list[str]
    layers_served: list[RegistryLayer]
    competes_with: list[str]
    name: str | None = None
    chains: list[str] | None = None
    qualified_aliases: list[RegistryQualifiedAlias] | None = None
    authority_evidence: list[str] | None = None
    identity_basis: str | None = None
    identity_evidence: list[str] | None = None
    identity_reviewed_at: str | None = None
    source_mapping_basis: str | None = None

    identity_reviewed_at_prec: str | None = None
    identity_reviewed_at_precision: str | None = None
    identity_reviewed_at_prec_reason: str | None = None
    identity_reviewed_at_precision_reason: str | None = None
    identity_reviewed_at_reason: str | None = None
    identity_reviewed_at_bucket_size_s: str | int | float | None = None
    identity_reviewed_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    @model_validator(mode="after")
    def declared_clock(self) -> Self:
        validate_review_clock(
            {
                "identity_reviewed_at": self.identity_reviewed_at,
                "identity_reviewed_at_prec": self.identity_reviewed_at_prec,
                "identity_reviewed_at_precision": self.identity_reviewed_at_precision,
                "identity_reviewed_at_prec_reason": self.identity_reviewed_at_prec_reason,
                "identity_reviewed_at_precision_reason": self.identity_reviewed_at_precision_reason,
                "identity_reviewed_at_reason": self.identity_reviewed_at_reason,
                "identity_reviewed_at_bucket_size_s": self.identity_reviewed_at_bucket_size_s,
                "identity_reviewed_at_bucket_anchor": self.identity_reviewed_at_bucket_anchor,
            },
            "identity_reviewed_at",
        )
        return self

    @model_validator(mode="after")
    def nonempty_aliases(self) -> Self:
        if any(not alias.strip() for alias in self.aliases):
            raise ValueError("Entity aliases require nonempty text")
        return self


class RegistrySectorBasket(DomainModel):
    sector: str = Field(min_length=1)
    weighting: str
    constituents: list[str]
    basket_id: str | None = Field(default=None, min_length=1)


class LendingPool(DomainModel):
    pool_id: str
    project: str
    chain: str
    underlying_tokens: list[str]


class LendingPoolCatalog(DomainModel):
    source_url: str
    captured_at: str
    source_sha256: str
    pools: list[LendingPool]


def distinct(values: list[str], namespace: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"Duplicate configured {namespace} identity")


class RegistryConfiguration(DomainModel):
    version: str
    tokens: list[RegistryToken]
    instruments: list[RegistryInstrument]
    entities: list[RegistryEntity]
    sector_baskets: list[RegistrySectorBasket]
    x_accounts: list[SourceAccount]
    lending_pool_catalog: LendingPoolCatalog

    @model_validator(mode="after")
    def unique_identities(self) -> Self:
        distinct([token.token_id for token in self.tokens], "token")
        distinct([instrument.instrument_id for instrument in self.instruments], "instrument")
        distinct([entity.entity_id for entity in self.entities], "entity")
        SourceAccountRegistryView(x_accounts=self.x_accounts)
        return self


class RegistryDependency(DomainModel):
    entity_id: str
    name: str
    token_id: str | None
    layers_served: list[RegistryLayer]
    description: str
    competes_with: list[str]
    state: str
    evidence_required_for_promotion: bool
    edge_id: str | None = Field(default=None, min_length=1)


class DependencyConfiguration(DomainModel):
    version: str
    edges: list[RegistryDependency]


class MetricSemantics(DomainModel):
    metric_key: str = Field(min_length=1)
    meaning: str
    venue: RegistryVenue
    market_type: RegistryMarketType
    endpoint_or_stream: str
    unit: Literal["BASE_QTY", "CONTRACTS", "RATE_PER_INTERVAL", "PRICE"]
    contract_multiplier_ref: str | None
    contract_multiplier_rule: str | None
    sidedness: Literal["N_A", "SINGLE_SIDED", "UNKNOWN"]
    side_semantics: Literal["BUYER_IS_MAKER_FLAG", "MAKER_SIDE", "TAKER_SIDE", "N_A"]
    trade_aggregation: Literal["PER_TAKER_ORDER_SAME_PRICE", "PER_FILL", "N_A"]
    scope: str
    calc_method: str
    critical: bool
    doc_ref: str
    aggregation_class: str
    inference_limits: list[str]
    price_contaminated: bool
    requires_native_normalization: bool
    source_basis: str
    source_basis_date: str
    timestamp_unit: Literal["MS", "US", "S"]
    timestamp_precision: Literal["EXACT", "BUCKET", "COLLECTION_ONLY"]
    timestamp_semantics: Literal["EXCHANGE_EVENT_TIME", "BUCKET_END", "COLLECTION_TIME", "SETTLEMENT_TIME"]


class AggressorSemantics(MetricSemantics):
    metric_family: Literal["aggressor"]
    field: str
    dedup_key: str
    evidence: str
    units_time: str


class DepthSemantics(MetricSemantics):
    metric_family: Literal["depth"]
    current_doc_ref: str | None = None
    endpoint_change_evidence: str | None = None


class FundingSemantics(MetricSemantics):
    metric_family: Literal["funding"]
    estimate: str
    evidence: str
    interval: str
    settled: str


class LiquidationSemantics(MetricSemantics):
    metric_family: Literal["liquidations"]
    coverage: str


class OpenInterestSemantics(MetricSemantics):
    metric_family: Literal["oi"]
    evidence: str
    excluded: str
    field: str
    units_sidedness: str
    publish_lag_basis: str | None = None
    publish_lag_s: int | None = Field(default=None, ge=0)
    source_interval_s: int | None = Field(default=None, ge=0)
    timestamp_semantics_detail: str | None = None
    timestamp_semantics_reason: str | None = None


class PriceSemantics(MetricSemantics):
    metric_family: Literal["price"]


type RegisteredMetric = Annotated[
    AggressorSemantics
    | DepthSemantics
    | FundingSemantics
    | LiquidationSemantics
    | OpenInterestSemantics
    | PriceSemantics,
    Field(discriminator="metric_family"),
]


class SemanticsConfiguration(DomainModel):
    version: str
    metrics: list[RegisteredMetric]

    @model_validator(mode="after")
    def unique_metrics(self) -> Self:
        distinct([metric.metric_key for metric in self.metrics], "metric")
        return self


class RegistryWalletList(DomainModel):
    venue: RegistryVenue
    repository: str
    path: str
    commit: str
    expected_evm_addresses: int
    addresses: list[str]
    status: str
    verified_count: int
    chain: str | None = None


class ExchangeAddressesConfiguration(DomainModel):
    version: str
    wallet_lists: list[RegistryWalletList]
    unavailable: dict[RegistryVenue, str]
    setup_required: str
