"""Registry identities fail closed while preserving qualified aliases."""

import pytest
from pydantic import ValidationError

from tank.analysis.identity import IdentityRegistry, resolve_entity, resolve_token, token_identities


def registry():
    return IdentityRegistry.from_wire(
        {
            "version": "fixture",
            "entities": [
                {
                    "entity_id": "plasma",
                    "names": ["Plasma"],
                    "qualified_aliases": [{"alias": "Plasma", "requires_any": ["XPL"]}],
                    "capture_settings": {"not_an_identity_field": True},
                }
            ],
            "tokens": [{"token_id": "plasma:XPL", "symbol": "XPL", "entity_id": "plasma"}],
        }
    )


def test_qualified_alias_requires_whole_word_qualifier():
    domain = registry()
    assert resolve_entity(domain, "Plasma") == "UNRESOLVED:Plasma"
    assert resolve_entity(domain, "Plasma", context="EXPLORER") == "UNRESOLVED:Plasma"
    assert resolve_entity(domain, "plasma", context="XPL launched") == "plasma"
    assert resolve_token(domain, "Plasma", context="XPL launched") == "plasma:XPL"
    assert resolve_token(domain, "$xpl") == "plasma:XPL"


def test_domain_rejects_malformed_identities_and_empty_qualifier_rules():
    for source in (
        {"entities": [{"names": ["Plasma"]}]},
        {"tokens": [{"token_id": 7}]},
        {
            "entities": [
                {"entity_id": "plasma", "qualified_aliases": [{"alias": "Plasma", "requires_any": []}]}
            ]
        },
    ):
        with pytest.raises(ValidationError):
            IdentityRegistry.from_wire(source)


def test_known_token_names_require_id_and_external_names_remain_explicit():
    domain = registry()
    with pytest.raises(ValueError, match="pass its token_id"):
        token_identities(domain, [], ["XPL"])
    assert token_identities(domain, ["plasma:XPL"], ["Outside", "Outside"]) == [
        "plasma:XPL",
        "UNRESOLVED:Outside",
    ]
    assert resolve_token(domain, "external:ABC") == "external:ABC"
