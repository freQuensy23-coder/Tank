"""Shared numerical supply refresh policy without capture or HTTP orchestration."""

from tank.market.capture_contracts import SupplyKind


def supply_cadences(hot: bool) -> dict[SupplyKind, int]:
    """The implemented §5.6 refresh periods, shared by polling and snapshots."""
    return {
        "EXCHANGE_INVENTORY": 900 if hot else 21600,
        "WALLET_FLOWS": 900 if hot else 21600,
        "UNLOCKS": 86400,
        "LENDING": 1800,
        "CHAIN_LIQUIDITY": 21600,
        "FUNDAMENTALS": 21600,
    }
