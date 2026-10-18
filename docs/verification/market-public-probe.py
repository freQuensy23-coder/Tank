import asyncio
import hashlib
import json
import tempfile
from pathlib import Path

from tank.market import normalize as norm
from tank.market.service import MarketService
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import canonical_json, iso


async def main():
    root = Path.cwd()
    settings = Settings(
        _env_file=None,
        config_dir=root / "config",
        data_dir=Path(tempfile.mkdtemp(prefix="tank-public-registry-")),
        replay=True,
        etherscan_api_key="",
        tavily_api_key="",
        typesafe_api_key="",
        openma_api_key="",
        telegram_token="",
    )
    service = MarketService(MemoryRepository(), settings)
    original = service._request

    async def request(method, url, **kw):
        # Sequential per path workers and existing shared host gates; extra delay halves normal market polling cap.
        await asyncio.sleep(1 if "kraken" in url else 0.21 if "coinbase" in url else 0.08)
        return await original(method, url, **kw)

    service._request = request
    evidence = {
        "started_at": iso(),
        "env_file_loaded": False,
        "auth_credentials_used": False,
        "scope": "181 configured instruments; public metadata, recent REST trade and kline normalizers; derivatives current samples",
        "rows": [],
    }

    async def path_worker(instruments):
        for inst in instruments:
            row = {
                "instrument_id": inst["instrument_id"],
                "token_id": inst["token_id"],
                "path": norm.venue_key(inst),
                "checks": {},
            }
            for name, action in [
                ("metadata", lambda inst=inst: service.discover(inst)),
                ("rest_semantic_self_test", lambda inst=inst: service.self_test(inst["instrument_id"])),
            ]:
                try:
                    result = await action()
                    row["checks"][name] = {
                        "status": (
                            "PASS"
                            if name != "rest_semantic_self_test"
                            or all(r.get("status") == "PASS" for r in result)
                            else "FAIL"
                        ),
                        "result": result,
                    }
                except Exception as exc:  # noqa: BLE001 - preserve every public probe failure in evidence
                    row["checks"][name] = {
                        "status": "FAIL",
                        "error_class": type(exc).__name__,
                        "reason": str(exc)[:300],
                    }
            row["metadata"] = {
                key: inst.get(key)
                for key in [
                    "active",
                    "contract_multiplier",
                    "tick_size",
                    "lot_size",
                    "funding_interval_h",
                    "metadata_status",
                ]
            }
            try:
                raw = await service.adapter(inst).klines()
                normalized = [norm.kline(inst, r) for r in raw]
                row["checks"]["klines"] = {
                    "status": "PASS",
                    "rows": len(normalized),
                    "raw_sha256": hashlib.sha256(canonical_json(raw).encode()).hexdigest(),
                }
            except Exception as exc:  # noqa: BLE001 - preserve every public probe failure in evidence
                row["checks"]["klines"] = {
                    "status": "FAIL",
                    "error_class": type(exc).__name__,
                    "reason": str(exc)[:300],
                }
            if inst["market_type"] != "SPOT":
                try:
                    oi, _current, _settled, interval, mark = await service.adapter(inst).derivatives()
                    normalized = [norm.oi(inst, r, mark=mark) for r in oi]
                    row["checks"]["derivatives"] = {
                        "status": "PASS",
                        "oi_rows": len(normalized),
                        "interval_h": str(interval),
                    }
                except Exception as exc:  # noqa: BLE001 - preserve every public probe failure in evidence
                    row["checks"]["derivatives"] = {
                        "status": "FAIL",
                        "error_class": type(exc).__name__,
                        "reason": str(exc)[:300],
                    }
            evidence["rows"].append(row)
            output = root / "docs/verification/market-registry-public-smoke.json"
            output.write_text(json.dumps(evidence, indent=2, default=str))
            print(len(evidence["rows"]), inst["instrument_id"], flush=True)

    groups = {}
    for inst in service.instruments.values():
        groups.setdefault(norm.venue_key(inst), []).append(inst)
    await asyncio.gather(*(path_worker(group) for group in groups.values()))
    await service.coverage()
    evidence.update(
        completed_at=iso(),
        instrument_count=len(evidence["rows"]),
        token_count=len({r["token_id"] for r in evidence["rows"]}),
    )
    evidence["summary"] = {
        status: sum(c["status"] == status for r in evidence["rows"] for c in r["checks"].values())
        for status in ["PASS", "FAIL"]
    }
    (root / "docs/verification/market-registry-public-smoke.json").write_text(
        json.dumps(evidence, indent=2, default=str)
    )
    await service.close()


asyncio.run(main())
