from test_model_context import context as context

from tank.agents import context as boundary


async def test_nested_truth_projection_has_one_recursive_final_guard(context, monkeypatch):
    repo, settings, session, _ = context
    payload = {"metric": {"value": "100.00", "window": "5m"}}
    expected = {"metric": {"value": "100.00", "window": "5m"}}
    for _ in range(12):
        payload = {"block": payload, "raw_sha256": "f" * 64}
        expected = {"block": expected}
    visits = []
    original = boundary.clean_value

    def observed(value):
        visits.append(value)
        return original(value)

    monkeypatch.setattr(boundary, "clean_value", observed)
    projected = await boundary.ModelContext(repo, settings, session).project(payload)
    assert projected == expected
    assert len(visits) <= 32


async def test_single_projection_guard_preserves_literals_and_removes_private_references(context):
    repo, settings, session, item = context
    reference = item["item_id"]
    url = f"https://example.org/{reference}"
    payload = {
        "windows": [
            {
                "value": "100.00",
                "note": f"See {reference} now",
                "quote": reference,
                "url": url,
                "raw_ref": "/private/source",
            }
        ],
        "config_version": "hidden",
    }
    expected = {
        "windows": [
            {
                "value": "100.00",
                "note": "See [internal reference omitted] now",
                "quote": reference,
                "url": url,
            }
        ]
    }
    model = boundary.ModelContext(repo, settings, session)
    assert await model.project(payload) == expected
    assert await model.result("get_market_truth", payload) == expected
    assert payload["config_version"] == "hidden"
