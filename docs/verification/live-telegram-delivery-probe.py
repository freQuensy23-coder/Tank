"""Three operator-approved test messages through real DeliveryService and isolated PG."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

import httpx
import psycopg

from tank.delivery import DeliveryService
from tank.logging import configure_logging, redact_text
from tank.settings import Settings, resolve_versions
from tank.storage import PostgresRepository
from tank.util import iso, new_id

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "data/live-integration-2026-10-18/telegram-state.json"
REPORT = ROOT / "docs/verification/live-telegram-delivery-2026-10-18.json"
DB_NAME = "tank_live_delivery_20261018"
DB_URL = "postgresql://a.mametyev@127.0.0.1:55436/" + DB_NAME
TEXTS = [
    "[ТЕСТ] Tank: проверка доставки служебных сообщений",
    "[ТЕСТ] Tank: проверка доставки. Это не торговый сигнал",
    "[ТЕСТ] Tank: проверка связи обновления с исходным сообщением",
]


async def main(restart):
    settings = resolve_versions(Settings(_env_file=ROOT / ".env", data_dir=STATE.parent))
    configure_logging(settings)
    async with await psycopg.AsyncConnection.connect(
        "postgresql://a.mametyev@127.0.0.1:55436/postgres", autocommit=True
    ) as admin:
        row = await (await admin.execute("SELECT 1 FROM pg_database WHERE datname=%s", (DB_NAME,))).fetchone()
        if not row:
            if restart:
                raise RuntimeError("Original isolated probe database missing")
            await admin.execute(psycopg.sql.SQL("CREATE DATABASE {}").format(psycopg.sql.Identifier(DB_NAME)))
    repo = PostgresRepository(DB_URL)
    await repo.open()
    try:
        await repo.migrate()
        if STATE.exists():
            state = json.loads(STATE.read_text())
        else:
            if restart:
                raise RuntimeError("Original probe state missing")
            state = {
                "event_id": new_id("event"),
                "alert_id": new_id("alert"),
                "notice_id": "live-integration-20261018",
            }
            STATE.parent.mkdir(parents=True, exist_ok=True)
            STATE.write_text(json.dumps(state))
        event_id, alert_id, notice_id = state["event_id"], state["alert_id"], state["notice_id"]
        payloads = [
            {"notice_id": notice_id},
            {"alert_id": alert_id, "version": 1},
            {"alert_id": alert_id, "version": 2},
        ]
        report = (
            json.loads(REPORT.read_text())
            if REPORT.exists()
            else {
                "started_at": iso(),
                "scope": "Explicitly authorized synthetic messages; real DeliveryService, PostgreSQL and Telegram",
                "database": DB_NAME,
                "responses": [],
                "texts": TEXTS,
                "config_version": settings.config_version,
                "code_version": settings.code_version,
            }
        )
        if not restart and not await repo.get("evt.events", event_id):
            now = iso()
            await repo.put(
                "evt.events",
                event_id,
                {
                    "event_id": event_id,
                    "statement": "Operator-authorized delivery test; no market event",
                    "theme_tags": ["INTEGRATION_TEST"],
                    "public_at": now,
                    "public_at_prec": "COLLECTION_ONLY",
                    "public_at_basis": "OPERATOR_TEST",
                    "known_to_us_at": now,
                    "freshness": "FRESH",
                    "event_state": "RUMOR",
                    "state_verified": False,
                    "certainty": "LOW",
                    "timing_confidence": "LOW",
                    "best_source_class": "TECHNICAL_EVIDENCE",
                    "lifecycle": "ANALYZING",
                },
            )
            await repo.put(
                "out.ops_notices",
                notice_id,
                {
                    "notice_id": notice_id,
                    "kind": "HEARTBEAT",
                    "text": TEXTS[0],
                    "created_at": now,
                    "created_by": "operator-authorized-integration-test",
                },
                immutable=True,
            )
            bundle = {"scope": "Operator-authorized delivery test", "event_id": event_id, "texts": TEXTS}
            bundle_id = hashlib.sha256(json.dumps(bundle, sort_keys=True).encode()).hexdigest()
            await repo.put(
                "out.evidence_bundles",
                bundle_id,
                {"bundle_id": bundle_id, "cutoff": now, **bundle},
                immutable=True,
            )
            for version, kind in ((1, "PARTIAL"), (2, "FULL")):
                await repo.put(
                    "out.alerts",
                    f"{alert_id}:{version}",
                    {
                        "alert_id": alert_id,
                        "version": version,
                        "kind": kind,
                        "event_id": event_id,
                        "generated_at": now,
                        "card_text": TEXTS[version],
                        "card_struct": {"operator_test": True},
                        "decisions": [],
                        "evidence_bundle_id": bundle_id,
                        "linter": {"status": "NOT_APPLICABLE_OPERATOR_TEST"},
                        "config_version": settings.config_version,
                        "code_version": settings.code_version,
                    },
                    immutable=True,
                )
        requests = []

        async def guard_request(request):
            if restart:
                raise RuntimeError("Unexpected send attempt after process restart")
            body = json.loads(request.content)
            if request.url.host != "api.telegram.org" or not request.url.path.endswith("/sendMessage"):
                raise RuntimeError("Unexpected endpoint")
            if body.get("text") not in TEXTS or str(body.get("chat_id")) not in {
                settings.telegram_ops_chat,
                settings.telegram_alerts_chat,
            }:
                raise RuntimeError("Unexpected message or destination")
            if len(requests) >= 3:
                raise RuntimeError("Three approved sends already attempted")
            requests.append({k: body[k] for k in ("chat_id", "text", "reply_parameters") if k in body})

        async def capture_response(response):
            await response.aread()
            body = response.json()
            result = body.get("result", {})
            record = {
                "http_status": response.status_code,
                "ok": body.get("ok"),
                "chat_id": result.get("chat", {}).get("id"),
                "message_id": result.get("message_id"),
                "reply_to_message_id": result.get("reply_to_message", {}).get("message_id"),
            }
            report["responses"].append(record)
            REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

        async with httpx.AsyncClient(
            timeout=8,
            trust_env=False,
            follow_redirects=False,
            event_hooks={"request": [guard_request], "response": [capture_response]},
        ) as http:
            service = DeliveryService(repo, settings, http=http)
            for payload in payloads:
                await service.deliver(payload)
        deliveries = [row async for row in repo.iter_all("out.deliveries")]
        sent = [row for row in deliveries if row["status"] == "SENT"]
        if len(sent) != 3 or any(row["status"] == "FAILED" for row in deliveries):
            raise RuntimeError("Expected exactly three successful ledger entries")
        if restart:
            report["restart_idempotency"] = {
                "status": "PASS",
                "new_http_requests": len(requests),
                "sent_ledger_rows": len(sent),
            }
        else:
            responses = report["responses"]
            report["threading_verified"] = responses[2]["reply_to_message_id"] == responses[1]["message_id"]
            if not report["threading_verified"]:
                raise RuntimeError("Telegram did not confirm the expected reply relation")
        report["status"] = "PASS"
        report["checked_at"] = iso()
        report["delivery_ledger"] = deliveries
        REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
        print(
            json.dumps(
                {
                    "report": str(REPORT),
                    "status": report["status"],
                    "responses": report["responses"],
                    "restart_idempotency": report.get("restart_idempotency"),
                },
                ensure_ascii=False,
            )
        )
    finally:
        await repo.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["send-approved-test-messages", "verify-restart"])
    args = parser.parse_args()
    try:
        asyncio.run(main(args.mode == "verify-restart"))
    except Exception as error:
        settings = Settings(_env_file=ROOT / ".env")
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "error_type": type(error).__name__,
                    "reason": redact_text(str(error), (settings.telegram_token or "",)),
                }
            )
        )
        raise SystemExit(1) from None
