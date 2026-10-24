"""Explicit delivery destinations for tests invoking the application job port."""

from collections.abc import Mapping

from pydantic import JsonValue

from tank.runtime_contracts import AlertDeliveryJob, DeliveryJob, NoticeDeliveryJob, parse_delivery_job


def delivery_job(payload: DeliveryJob | Mapping[str, JsonValue]) -> DeliveryJob:
    if isinstance(payload, (AlertDeliveryJob, NoticeDeliveryJob)):
        return payload
    channel = "telegram:ops" if "notice_id" in payload else "telegram:alerts"
    return parse_delivery_job({"channel": channel, **payload})
