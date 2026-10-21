"""Late-receipt unlock calendars in replay (lead rulings 2026-10-20, F-02 and F-04).

An unlock row captured after the cutoff enters as HISTORICAL_RECONSTRUCTION only when
the schedule holding the tranche was known by the cutoff (schedule_as_of, end of its
declared bucket, <= cutoff and <= the replay clock) and every receipt <= assembled_at.
The tranche date may be before or after the cutoff. Calendars captured before the
cutoff keep their actual availability; source evidence never gets this exception.
"""

import json

import pytest

from tank.replay import ReplayError, load_inputs
from tank.settings import Settings

CUTOFF = "2026-09-22T18:53:00Z"
RECEIVED = "2026-10-20T03:07:08Z"


def unlock(
    date,
    *,
    as_of="2026-09-01T00:00:00Z",
    as_of_prec="BUCKET(1d)",
    received=RECEIVED,
    mode="HISTORICAL_RECONSTRUCTION",
):
    record = {
        "token_id": "asset-k",
        "date": date,
        "date_prec": "EXACT",
        "label": "Core Contributors",
        "qty": "10625000",
        "kind": "POTENTIAL_SUPPLY",
        "sale_inferred": False,
        "observed_at": received,
        "observed_at_prec": "COLLECTION_ONLY",
        "received_ts": received,
        "received_ts_prec": "COLLECTION_ONLY",
    }
    if as_of is not None:
        record.update(schedule_as_of=as_of, schedule_as_of_prec=as_of_prec)
    row = {"collection": "mkt.unlocks", "key": "asset-k:" + date, "available_at": received, "record": record}
    if mode:
        row["evidence_mode"] = mode
    return row


def write(tmp_path, rows, assembled="2026-10-20T17:54:49Z"):
    packet = {
        "cutoff": CUTOFF,
        "config_version": "test",
        "code_version": "test",
        "registry": {},
        "config": {},
        "records": rows,
        "tool_results": [],
    }
    if assembled:
        packet["assembled_at"] = assembled
    path = tmp_path / "input.json"
    path.write_text(json.dumps(packet))
    return path


def settings():
    return Settings(config_version="test", code_version="test", replay=True, _env_file=None)


@pytest.mark.parametrize("tranche", ["2026-09-19T19:04:31Z", "2026-10-05T00:00:00Z"])
def test_late_unlock_known_schedule_is_visible_for_past_and_upcoming_tranches(tmp_path, tranche):
    inputs = load_inputs(write(tmp_path, [unlock(tranche)]), settings())
    assert [row["date"] for row in inputs.records("mkt.unlocks")] == [tranche]
    assert inputs.records("mkt.unlocks", cutoff="2026-09-01T12:00:00Z") == []
    assert len(inputs.records("mkt.unlocks", cutoff="2026-09-02T00:00:00Z")) == 1
    assert inputs.evidence_mode == "HISTORICAL_RECONSTRUCTION"


@pytest.mark.parametrize(
    ("row", "assembled", "message"),
    [
        (unlock("2026-10-05T00:00:00Z", as_of="2026-09-23T00:00:00Z"), "2026-10-20T17:54:49Z", "post-cutoff"),
        (unlock("2026-10-05T00:00:00Z", as_of=None), "2026-10-20T17:54:49Z", "schedule_as_of"),
        (unlock("2026-10-05T00:00:00Z", as_of_prec="COLLECTION_ONLY"), "2026-10-20T17:54:49Z", "precision"),
        (unlock("2026-09-19T19:04:31Z"), "2026-10-19T00:00:00Z", "assembled_at"),
        (unlock("2026-09-19T19:04:31Z"), None, "assembled_at"),
        (unlock("2026-09-19T19:04:31Z", mode=None), "2026-10-20T17:54:49Z", "HISTORICAL_RECONSTRUCTION"),
    ],
)
def test_late_unlock_is_refused_unless_strict(tmp_path, row, assembled, message):
    with pytest.raises(ReplayError, match=message):
        load_inputs(write(tmp_path, [row], assembled), settings())


def test_calendar_captured_before_cutoff_keeps_actual_availability(tmp_path):
    early = unlock("2026-12-01T00:00:00Z", as_of=None, received="2026-09-20T00:00:00Z", mode=None)
    inputs = load_inputs(write(tmp_path, [early], assembled=None), settings())
    assert [row["date"] for row in inputs.records("mkt.unlocks")] == ["2026-12-01T00:00:00Z"]
    assert inputs.records("mkt.unlocks", cutoff="2026-09-19T00:00:00Z") == []
