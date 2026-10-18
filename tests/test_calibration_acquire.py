import json
from types import SimpleNamespace

import httpx
import pytest

from tank.calibration import CalibrationError
from tank.calibration_acquire import acquire, digest


def inputs(tmp_path):
    rows = [
        {
            "item_id": str(n),
            "source_id": "S30",
            "content_hash": "content",
            "raw_sha256": "raw",
            "source": "Outlet",
            "author": "Outlet",
            "text": "Ethereum fact",
            "observed_at": f"2026-10-18T12:00:0{n}Z",
        }
        for n in range(3)
    ]
    raw = tmp_path / "raw.jsonl"
    raw.write_text("\n".join(map(json.dumps, rows)))
    labels = tmp_path / "labels.json"
    labels.write_text(
        json.dumps(
            {
                "dataset_id": "actual-test",
                "status": "FROZEN",
                "labeler": "independent-test",
                "labeled_at": "2026-10-18",
                "split_method": "group-disjoint",
                "candidate_set_description": "observed",
                "items": [
                    {
                        "item_id": str(n),
                        "source_id": "S30",
                        "content_hash": "content",
                        "raw_sha256": "raw",
                        "known_to_us_at": f"2026-10-18T12:00:0{n}Z",
                        "raw_index": n,
                        "relevant": True,
                        "label_group": "g",
                        "split": "calibration",
                        "cross_source_repeat": False,
                        "is_repeat": n > 0,
                    }
                    for n in range(3)
                ],
                "events": [
                    {
                        "event_id": "e",
                        "label_group": "g",
                        "representative_item_id": "0",
                        "statement": "Ethereum fact",
                        "entities": [{"entity_id": "ethereum", "role": "SUBJECT"}],
                        "tokens_mentioned": [],
                    }
                ],
            }
        )
    )
    return raw, labels


async def test_acquisition_frozen_labels_actual_sdk_and_restart_cache(tmp_path, monkeypatch):
    raw, labels = inputs(tmp_path)
    settings = SimpleNamespace(
        jev_provider="typesafe",
        typesafe_api_key="FAKE",
        config_version="test",
        code_version="code",
        replay=False,
        load_config=lambda _: {"entities": [{"entity_id": "ethereum", "name": "Ethereum"}]},
    )
    calls = []

    def transport(request):
        body = json.loads(request.content)
        calls.append(body)
        name = next(iter(body["questions"]))
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13.0",
                "answers": {name: {"type": "noul", "noul": 0.99}},
                "usage": {"input_tokens": 1},
            },
        )

    monkeypatch.setattr("tank.calibration_acquire.write_report", lambda path, _: json.loads(path.read_text()))
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        result = await acquire(raw, labels, tmp_path / "out", digest(labels), settings=settings, http=http)
        assert len(calls) == 5
        assert [r["expected_event_id"] for r in result["items"]] == [None, "e", "e"]
        assert (
            result["items"][1]["candidates"][0]["state"]["event"]["known_to_us_at"] == "2026-10-18T12:00:00Z"
        )
        again = await acquire(raw, labels, tmp_path / "out", digest(labels), settings=settings, http=http)
        assert len(calls) == 5 and result == again
    assert len(list((tmp_path / "out/usage").glob("*.json"))) == 5


async def test_acquisition_refuses_mutated_labels_before_call(tmp_path):
    raw, labels = inputs(tmp_path)
    with pytest.raises(CalibrationError, match="SHA256"):
        await acquire(raw, labels, tmp_path / "out", "wrong")


async def test_acquisition_refuses_group_split_leak_before_call(tmp_path):
    raw, labels = inputs(tmp_path)
    data = json.loads(labels.read_text())
    data["items"][1]["split"] = "heldout"
    labels.write_text(json.dumps(data))
    with pytest.raises(CalibrationError, match="leakage"):
        await acquire(raw, labels, tmp_path / "out", digest(labels))


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", "S31"),
        ("content_hash", "changed"),
        ("raw_sha256", "changed"),
        ("known_to_us_at", "2026-10-19T00:00:00Z"),
        ("raw_index", 2),
    ],
)
async def test_acquisition_binds_each_frozen_label_to_actual_capture(tmp_path, field, value):
    raw, labels = inputs(tmp_path)
    data = json.loads(labels.read_text())
    data["items"][0][field] = value
    labels.write_text(json.dumps(data))
    with pytest.raises(CalibrationError, match="mismatch|match captured"):
        await acquire(raw, labels, tmp_path / "out", digest(labels))


async def test_exclusions_and_frozen_nonrepeat_not_inferred_from_group(tmp_path, monkeypatch):
    raw, labels = inputs(tmp_path)
    data = json.loads(labels.read_text())
    data["items"][1]["ambiguity_excluded"] = True
    data["items"][2]["is_repeat"] = False
    labels.write_text(json.dumps(data))
    settings = SimpleNamespace(
        jev_provider="typesafe",
        typesafe_api_key="FAKE",
        config_version="test",
        code_version="code",
        replay=False,
        load_config=lambda _: {"entities": [{"entity_id": "ethereum", "name": "Ethereum"}]},
    )
    monkeypatch.setattr("tank.calibration_acquire.write_report", lambda path, _: json.loads(path.read_text()))

    def transport(request):
        name = next(iter(json.loads(request.content)["questions"]))
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13.0",
                "answers": {name: {"type": "noul", "noul": 0.99}},
                "usage": {"input_tokens": 1},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        result = await acquire(raw, labels, tmp_path / "out", digest(labels), settings=settings, http=http)
    assert [r["item_id"] for r in result["items"]] == ["0", "2"]
    assert result["items"][1]["candidates"] and result["items"][1]["expected_event_id"] is None
    assert len(result["exclusions"]) == 1


async def test_candidate_keys_cannot_expand_production_event_identity_universe(tmp_path):
    raw, labels = inputs(tmp_path)
    data = json.loads(labels.read_text())
    data["events"][0]["candidate_identity_keys"] = ["ethereum", "invented-extra"]
    labels.write_text(json.dumps(data))
    with pytest.raises(CalibrationError, match="production event"):
        await acquire(raw, labels, tmp_path / "out", digest(labels))
