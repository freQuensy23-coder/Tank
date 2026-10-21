import pytest

from tank.market.supply import unlock_schedule


def test_unlock_delta_is_potential_supply_not_sale():
    data = {
        "documentedData": {
            "data": [
                {
                    "label": "team",
                    "data": [
                        {"timestamp": 1767225600, "unlocked": 100},
                        {"timestamp": 1767312000, "unlocked": 140},
                        {"timestamp": 1767398400, "unlocked": 140},
                    ],
                }
            ]
        }
    }
    r = unlock_schedule(data, now="2026-01-01T12:00:00Z")
    assert len(r) == 1 and r[0]["qty"] == "40" and r[0]["sale_inferred"] is False
    with pytest.raises(ValueError):
        unlock_schedule({"wrong": []})
