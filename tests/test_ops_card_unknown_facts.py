"""Unreported presentation facts stay distinct from recorded zero and empty sets."""

import pytest

from tank.contracts.delivery import (
    OpsAlertView,
    OpsAssessmentView,
    OpsCardSnapshot,
    OpsDeliveryView,
    OpsEventView,
    OpsMapView,
    OpsProgressNotice,
    OpsSourceItem,
)
from tank.ops_cards import render


def presentation(
    *, deliveries: list[OpsDeliveryView], maps: list[OpsMapView], alert: OpsAlertView | None
) -> OpsCardSnapshot:
    completed = OpsProgressNotice(notice_id="n", stage="AGENT_COMPLETED", created_at="2026-10-27T00:00:00Z")
    return OpsCardSnapshot(
        item=OpsSourceItem(item_id="i", title="Actual source"),
        history=[completed],
        stages={"AGENT_COMPLETED": completed},
        event=OpsEventView(statement="Actual event"),
        assessment=OpsAssessmentView.model_validate({"class": "MAJOR"}),
        maps=maps,
        truths=[],
        decisions=[],
        alert=alert,
        deliveries=deliveries,
        prior_receipt=None,
        observations=[],
    )


@pytest.mark.parametrize("attempt", [None, 0, 5])
def test_failed_delivery_does_not_display_unknown_attempt_as_zero(attempt: int | None) -> None:
    card = render(
        presentation(
            deliveries=[OpsDeliveryView(status="FAILED", terminal=True, attempt=attempt)],
            maps=[],
            alert=OpsAlertView(alert_id="a", version=1, generated_at="2026-10-27T00:00:00Z"),
        )
    )
    assert card.code == "ALERT_FAILED"
    if attempt is None:
        assert "число попыток не полностью записано" in card.reason
        assert "попыток: 0" not in card.reason
    else:
        assert f"попыток: {attempt}" in card.reason


@pytest.mark.parametrize("version,tokens", [(None, None), (1, None), (1, [])])
def test_analysis_map_unknown_is_not_an_empty_completed_universe(
    version: int | None, tokens: list[str] | None
) -> None:
    card = render(
        presentation(
            deliveries=[],
            maps=[OpsMapView(map_version=version, analysis_set=tokens)],
            alert=None,
        )
    )
    assert card.code == "INCOMPLETE"
    if version is None:
        assert "версия карты не записана" in card.reason
        assert "0/0" not in card.text
    elif tokens is None:
        assert "состав токенов не записан" in card.reason
        assert "0/0" not in card.text
    else:
        assert "данные: 0/0 токенов; решения: 0/0" in card.text
        assert "решение о публикации" in card.reason
