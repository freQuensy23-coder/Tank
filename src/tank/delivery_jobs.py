"""Typed payloads of durable ``DELIVER`` queue jobs.

Queued rows are parsed without coercion and unknown keys, such as the
``_diagnostic_recovery`` proof, are ignored, so every payload the producers
have written stays readable. A missing ``channel`` takes the subject's default
channel, as the delivery code always did.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict


class AlertDeliveryJob(BaseModel):
    """Delivery of one alert card version."""

    model_config = ConfigDict(frozen=True, strict=True, extra="ignore")

    alert_id: str
    version: int
    channel: str = "telegram:alerts"

    @property
    def subject(self) -> str:
        return f"{self.alert_id}:{self.version}"

    @property
    def subject_filters(self) -> dict[str, Any]:
        return {"alert_id": self.alert_id, "version": self.version}


class NoticeDeliveryJob(BaseModel):
    """Delivery of one Ops notice."""

    model_config = ConfigDict(frozen=True, strict=True, extra="ignore")

    notice_id: str
    channel: str = "telegram:ops"

    @property
    def subject(self) -> str:
        return self.notice_id

    @property
    def subject_filters(self) -> dict[str, Any]:
        return {"notice_id": self.notice_id}


type DeliveryJob = AlertDeliveryJob | NoticeDeliveryJob


def parse_delivery_job(payload: dict[str, Any]) -> DeliveryJob:
    """Return the typed job; a truthy ``notice_id`` selects a notice delivery."""
    if payload.get("notice_id"):
        return NoticeDeliveryJob.model_validate(payload)
    return AlertDeliveryJob.model_validate(payload)


def ledger_filters(job: DeliveryJob) -> dict[str, Any]:
    """Filters that select the job's rows in ``out.deliveries``."""
    return {"channel": job.channel, **job.subject_filters}
