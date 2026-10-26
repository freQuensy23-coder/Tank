"""Explicit captured policy for authored Truth fixtures."""

from copy import deepcopy
from functools import lru_cache

from pydantic import JsonValue

from tank.settings import Settings


@lru_cache(maxsize=1)
def _captured_truth_configuration() -> dict[str, JsonValue]:
    return Settings().load_config("thresholds")


def fixture_truth_config(**overrides: JsonValue) -> dict[str, JsonValue]:
    return deepcopy(_captured_truth_configuration()) | overrides
