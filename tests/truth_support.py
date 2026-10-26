"""Explicit captured policy for authored Truth fixtures."""

from copy import deepcopy
from datetime import datetime
from functools import lru_cache

from pydantic import JsonValue

from tank.analysis.condition_graph_contracts import ConditionInterval, ConditionTopVenueSignHistory
from tank.settings import Settings


@lru_cache(maxsize=1)
def _captured_truth_configuration() -> dict[str, JsonValue]:
    return Settings().load_config("thresholds")


def fixture_truth_config(**overrides: JsonValue) -> dict[str, JsonValue]:
    return deepcopy(_captured_truth_configuration()) | overrides


def unselected_spot_sign_history(
    token_id: str, start: datetime, cutoff: datetime
) -> ConditionTopVenueSignHistory:
    """Authored enrichment fixtures explicitly select no top-venue direction history."""
    return ConditionTopVenueSignHistory(
        token_id=token_id,
        interval=ConditionInterval(window="EVENT", start=start, cutoff=cutoff),
        selected_venues=(),
        windows=(),
    )
