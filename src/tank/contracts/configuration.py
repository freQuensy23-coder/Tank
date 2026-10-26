"""Required versioned operational configuration contracts."""

from pydantic import Field

from tank.contracts import DomainModel


class FreshnessHours(DomainModel):
    FRESH: int = Field(gt=0)
    NORMAL: int = Field(gt=0)
    MEDIUM: int = Field(gt=0)
    POOR: int = Field(gt=0)
    OLD: int = Field(gt=0)


class Thresholds(DomainModel):
    version: str
    filter_cutoff: float = Field(ge=0, le=1)
    routing_cutoff: float = Field(ge=0, le=1)
    filter_model: str
    calibration_required: list[str]
    tank_agent_statement_timeout_ms: int | None
    tank_agent_connection_limit: int | None
    normal_range_quantiles: list[float]
    significant_flow_sigma: float
    freshness_hours: FreshnessHours
    min_spot_venues: int
    large_ticket_usd: int
    size_buckets_usd: list[int]
    oi_sync_tolerance_s: int
    book_interval_s: int
    impact_tiers_usd: list[int]
    depth_band_pct: float
    refill_window_s: int
    resilience_window_min: int
    absorption_min_bursts: int
    history_min_days: int
    beta_blume_weight: float = Field(ge=0, le=1)
    event_close_days: int
    call_expiry_days: int
    session_keep_days: int
    update_rate_limit_s: int
    heartbeat_utc: str
    escalated_interval_s: int
    escalation_expiry_s: int
    hot_token_cap: int
    base_kline_interval_s: int
    base_oi_funding_interval_s: int
    hot_oi_funding_interval_s: int
    base_retention_days: int
    trades_retention_days: int
    jobs_retention_days: int
    backup_snapshots_kept: int = Field(gt=0)
    book_summary_retention_hours: int
    hot_ticker_interval_s: int
    quiet_socket_s: int
    archive_retry_s: int
    routing_window_days: int
    market_context_rule: str


class DeliveryConfiguration(DomainModel):
    alerts_chat_id: str
    ops_chat_id: str
    parse_mode: str
    max_attempts: int = Field(gt=0)
    secrets: str
