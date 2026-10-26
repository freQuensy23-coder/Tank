"""Actual expiry queues the committed closed call consumed by Session admission."""

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from runtime_public_fixtures import public_runtime
from test_event_tools import complete_event_fixture
from test_storage import repo as repo

from tank.analysis.watch_contracts import WatchCall
from tank.runtime_contracts import CallExpiredMessage, parse_agent_message
from tank.storage import StorageBackend
from tank.util import canonical_json, iso, utcnow


async def test_actual_expiry_queue_consumes_same_closed_snapshot_as_persisted_call(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, _):
        now = utcnow()
        opened = iso(now - timedelta(days=runtime.scheduler.thresholds.call_expiry_days))
        call = WatchCall(
            call_id="call-expiry-snapshot",
            event_id="event-expiry-snapshot",
            token_id="T",
            opened_by_alert="alert-expiry-snapshot",
            opened_by_version=1,
            opened_at=opened,
            opened_at_prec="EXACT",
            opened_at_prec_reason=None,
            alert_generated_at=opened,
            alert_generated_at_prec="EXACT",
            alert_generated_at_prec_reason=None,
            entry_ref_price=Decimal("10"),
            invalidation_price=Decimal("8"),
            status="OPEN",
        )
        await repo.put(
            "evt.events",
            call.event_id,
            complete_event_fixture(call.event_id, statement="Authored expiry control"),
        )
        await repo.put("ana.calls", call.call_id, call.model_dump(mode="json"))
        await runtime.scheduler.expire_calls(now)
        stored = await repo.get("ana.calls", call.call_id)
        assert stored is not None and stored["status"] == "CLOSED" and stored["closed_by"] == "EXPIRED"
        jobs = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
        assert len(jobs) == 1
        payload = jobs[0]["payload"]
        assert isinstance(payload, dict)
        assert canonical_json(payload["call"]) == canonical_json(stored)
        admitted = parse_agent_message(payload)
        assert isinstance(admitted, CallExpiredMessage)
        assert admitted.call.closed_at == stored["closed_at"]
        assert admitted.call.call_id == call.call_id and admitted.call.token_id == call.token_id
        assert admitted.call.status == "CLOSED" and admitted.call.closed_by == "EXPIRED"
