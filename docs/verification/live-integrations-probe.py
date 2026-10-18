"""Explicit authenticated, read-only probes. Never imported by pytest or Tank."""

import asyncio
import hashlib
import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
from dotenv import dotenv_values

from tank.logging import configure_logging, redact_text
from tank.market.service import MarketService
from tank.market.supply import CHAIN_IDS, SupplyCapture
from tank.settings import Settings
from tank.storage import MemoryRepository

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "docs/verification/live-integrations-2026-10-18.json"


async def main():
    env = dotenv_values(ROOT / ".env", interpolate=False)
    settings = Settings(_env_file=ROOT / ".env", data_dir=ROOT / "data/live-integration-2026-10-18")
    configure_logging(settings)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    secrets = tuple(
        value
        for key, value in env.items()
        if value and any(word in key.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD", "ETHERSCAN"))
    )
    report = {
        "started_at": datetime.now(UTC).isoformat(),
        "scope": "Authenticated read-only provider probes; no model generation or external messages",
        "telegram": {},
        "etherscan": {},
        "openrouter": {},
        "unconfigured": {
            "typesafe_api_key": not bool(settings.typesafe_api_key),
            "codex_auth_explicitly_configured_in_env": not bool(env.get("TANK_CODEX_AUTH_PATH")),
            "x_session_explicitly_configured_in_env": not bool(env.get("TANK_X_STORAGE_STATE_PATH")),
        },
        "documentation": {
            "telegram": "https://core.telegram.org/bots/api",
            "etherscan": "https://docs.etherscan.io/api-reference/endpoint/tokenbalance",
            "openrouter": "https://openrouter.ai/docs/api/api-reference/api-keys/get-current-api-key",
        },
    }

    def save():
        rendered = json.dumps(report, ensure_ascii=False, indent=2, default=str)
        rendered = redact_text(rendered, secrets)
        REPORT.write_text(rendered + "\n")

    async def guard_response(response):
        body = await response.aread()
        if any(secret.encode() in body for secret in secrets):
            raise RuntimeError("Provider response reflected a configured secret; body discarded")

    async with httpx.AsyncClient(
        timeout=20,
        follow_redirects=False,
        trust_env=False,
        event_hooks={"response": [guard_response]},
    ) as http:

        async def telegram():
            result = report["telegram"]
            if not settings.telegram_token:
                result["status"] = "UNCONFIGURED"
                return

            async def call(method, payload):
                started = time.monotonic()
                response = await http.post(
                    "https://api.telegram.org/bot" + settings.telegram_token + "/" + method,
                    json=payload,
                )
                body = response.json()
                check = {
                    "method": method,
                    "http_status": response.status_code,
                    "ok": body.get("ok") is True,
                    "duration_ms": round((time.monotonic() - started) * 1000),
                }
                if not check["ok"]:
                    check["description"] = redact_text(str(body.get("description")), secrets)
                result.setdefault("checks", []).append(check)
                if not check["ok"]:
                    raise RuntimeError("Telegram returned failure; sanitized result recorded")
                return body["result"]

            bot = await call("getMe", {})
            result["bot"] = {key: bot.get(key) for key in ("id", "username", "is_bot")}
            result["chats"] = []
            for name, chat_id in (
                ("Tank Alerts", settings.telegram_alerts_chat),
                ("Tank Ops", settings.telegram_ops_chat),
            ):
                chat = await call("getChat", {"chat_id": chat_id})
                member = await call("getChatMember", {"chat_id": chat_id, "user_id": bot["id"]})
                result["chats"].append(
                    {
                        "configured_name": name,
                        "id": chat["id"],
                        "title": chat.get("title"),
                        "type": chat["type"],
                        "bot_status": member["status"],
                        "rights": {k: v for k, v in member.items() if k.startswith("can_")},
                    }
                )
            result["status"] = "AUTH_AND_CHAT_ACCESS_VERIFIED"
            result["delivery"] = "NOT_SENT"

        async def etherscan():
            result = report["etherscan"]
            if not settings.etherscan_api_key:
                result["status"] = "UNCONFIGURED"
                return
            repo = MemoryRepository()
            market = MarketService(repo, settings, http=http)
            capture = SupplyCapture(market)
            token = market.tokens["uniswap:UNI"]
            contract = token["contracts"][0]
            chainid = contract.get("chainid") or contract.get("chain_id") or CHAIN_IDS[contract["chain"]]
            wallet = settings.load_config("exchange_addresses")["wallet_lists"][0]["addresses"][0]
            result.update(token_id=token["token_id"], chain_id=chainid, checks=[])
            try:
                for name, params in [
                    (
                        "decimals",
                        dict(
                            module="proxy",
                            action="eth_call",
                            to=contract["address"],
                            data="0x313ce567",
                            tag="latest",
                        ),
                    ),
                    (
                        "balance",
                        dict(
                            module="account",
                            action="tokenbalance",
                            contractaddress=contract["address"],
                            address=wallet,
                            tag="latest",
                        ),
                    ),
                    (
                        "transfers",
                        dict(
                            module="account",
                            action="tokentx",
                            contractaddress=contract["address"],
                            address=wallet,
                            page=1,
                            offset=2,
                            sort="desc",
                        ),
                    ),
                ]:
                    started = time.monotonic()
                    try:
                        value = await capture.etherscan(token, chainid, **params)
                        if name == "decimals":
                            shape_ok = (
                                isinstance(value, str)
                                and value.startswith("0x")
                                and 0 <= int(value, 16) <= 255
                            )
                            details = {"decimals": int(value, 16)} if shape_ok else {}
                        elif name == "balance":
                            shape_ok = isinstance(value, str) and value.isdecimal()
                            details = {"raw_integer": value} if shape_ok else {}
                        else:
                            shape_ok = isinstance(value, list) and all(
                                "hash" in x and "tokenDecimal" in x for x in value
                            )
                            details = {"rows": len(value)} if isinstance(value, list) else {}
                        result["checks"].append(
                            {
                                "name": name,
                                "status": "PASS" if shape_ok else "SCHEMA_FAILURE",
                                "duration_ms": round((time.monotonic() - started) * 1000),
                                **details,
                            }
                        )
                    except Exception as exc:
                        result["checks"].append(
                            {
                                "name": name,
                                "status": "FAIL",
                                "error_type": type(exc).__name__,
                                "reason": redact_text(str(exc), secrets),
                            }
                        )
                    save()
                result["status"] = (
                    "PASS" if all(x["status"] == "PASS" for x in result["checks"]) else "PARTIAL_OR_FAILED"
                )
            finally:
                await market.close()
                archives = [x async for x in repo.iter_all("mkt.raw_archives")]
                result["raw_archive_records"] = len(archives)
                result["application_path"] = "SupplyCapture.etherscan -> MarketService._request -> RawArchive"

        async def openrouter():
            result = report["openrouter"]
            key = env.get("OPENROUTER_API_KEY")
            if not key:
                result["status"] = "UNCONFIGURED"
                return
            response = await http.get(
                "https://openrouter.ai/api/v1/key", headers={"Authorization": "Bearer " + key}
            )
            body = response.json()
            result["http_status"] = response.status_code
            result["status"] = (
                "KEY_VALID"
                if response.status_code == 200 and isinstance(body.get("data"), dict)
                else "AUTH_FAILED"
            )
            result["used_by_current_tank_design"] = False
            result["model_generation_requests"] = 0
            if result["status"] != "KEY_VALID":
                result["error"] = redact_text(
                    str(body.get("error", {}).get("message", "Unrecognized response")), secrets
                )

        async def run(name, operation):
            try:
                await operation()
            except Exception as exc:
                report[name].update(
                    status="PROBE_ERROR", error_type=type(exc).__name__, reason=redact_text(str(exc), secrets)
                )
            save()

        await asyncio.gather(
            run("telegram", telegram), run("etherscan", etherscan), run("openrouter", openrouter)
        )
    report["completed_at"] = datetime.now(UTC).isoformat()
    report["probe_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    save()
    print(
        json.dumps(
            {
                "report": str(REPORT),
                "results": {k: report[k] for k in ("telegram", "etherscan", "openrouter", "unconfigured")},
            },
            ensure_ascii=False,
            default=str,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
