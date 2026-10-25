"""Actual immutable configured membership for card component fixtures."""

from functools import lru_cache

from tank.analysis.card_codec import card_registry_from_record
from tank.analysis.card_contracts import CardRegistryMembership
from tank.settings import Settings


@lru_cache(maxsize=1)
def fixture_registry_membership() -> CardRegistryMembership:
    return card_registry_from_record(Settings().load_config("registry"))
