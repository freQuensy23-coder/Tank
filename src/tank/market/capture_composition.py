"""Composition of capture application policy and its persistence adapter."""

from decimal import Decimal

from tank.market.capture_contracts import CapturePolicy
from tank.market.capture_lifecycle import CaptureLifecycle
from tank.market.capture_ports import CaptureClock
from tank.market.capture_repository import CaptureDocumentStore, CaptureRepository
from tank.settings import Settings


def build_capture_lifecycle(
    store: CaptureDocumentStore,
    settings: Settings,
    known_tokens: frozenset[str],
    clock: CaptureClock,
) -> CaptureLifecycle:
    policy = CapturePolicy(
        thresholds=settings.thresholds(),
        replay=settings.replay,
        request_budget_s=Decimal("30"),
        request_grace_s=Decimal("2"),
        hot_ttl_hours=72,
        config_version=settings.config_version,
        code_version=settings.code_version,
    )
    repository = CaptureRepository(
        store,
        config_version=policy.config_version,
        code_version=policy.code_version,
        secrets=settings.configured_secrets,
    )
    return CaptureLifecycle(repository, policy, clock, known_tokens)
