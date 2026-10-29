import pytest
from pydantic import ValidationError

from tank.delivery_jobs import AlertDeliveryJob, NoticeDeliveryJob, ledger_filters, parse_delivery_job


def test_alert_payload_keeps_identity_and_default_channel():
    job = parse_delivery_job({"alert_id": "a", "version": 2})
    assert job == AlertDeliveryJob(alert_id="a", version=2, channel="telegram:alerts")
    assert job.subject == "a:2"
    assert ledger_filters(job) == {"channel": "telegram:alerts", "alert_id": "a", "version": 2}
    assert list(ledger_filters(job)) == ["channel", "alert_id", "version"]


def test_notice_payload_keeps_identity_and_default_channel():
    job = parse_delivery_job({"notice_id": "n"})
    assert job == NoticeDeliveryJob(notice_id="n", channel="telegram:ops")
    assert job.subject == "n"
    assert ledger_filters(job) == {"channel": "telegram:ops", "notice_id": "n"}


def test_queued_channel_and_unknown_keys_are_kept_readable():
    proof = {"version": 1, "chat_id": "2"}
    job = parse_delivery_job({"notice_id": "n", "channel": "telegram:alerts", "_diagnostic_recovery": proof})
    assert isinstance(job, NoticeDeliveryJob)
    assert job.channel == "telegram:alerts"


def test_empty_notice_id_selects_alert_delivery_as_before():
    job = parse_delivery_job({"notice_id": "", "alert_id": "a", "version": 1, "channel": "telegram:alerts"})
    assert isinstance(job, AlertDeliveryJob)
    assert job.subject == "a:1"


def test_identity_values_are_not_coerced():
    with pytest.raises(ValidationError):
        parse_delivery_job({"alert_id": "a", "version": "1"})
