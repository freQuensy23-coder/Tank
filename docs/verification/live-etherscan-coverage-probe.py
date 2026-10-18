"""Explicit authenticated samples of configured contracts, not complete inventory coverage."""

import argparse
import asyncio
import json
from copy import deepcopy
from pathlib import Path

import httpx

from tank.logging import configure_logging, redact_text
from tank.market.service import MarketService
from tank.market.supply import SupplyCapture
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "docs/verification/live-etherscan-coverage-2026-10-18.json"


class ScopedSettings(Settings):
    def load_config(self, name):
        config = super().load_config(name)
        if name.removesuffix(".yaml") == "exchange_addresses":
            config = deepcopy(config)
            config["wallet_lists"] = config["wallet_lists"][:1]
            config["wallet_lists"][0]["addresses"] = config["wallet_lists"][0]["addresses"][:1]
        return config


async def main(inventory_only):
    settings = ScopedSettings(_env_file=ROOT / ".env", data_dir=ROOT / "data/live-integration-2026-10-18")
    configure_logging(settings)
    report = (
        json.loads(REPORT.read_text())
        if inventory_only and REPORT.exists()
        else {
            "started_at": iso(),
            "contract_checks": [],
            "scope": "All configured EVM contract decimals; inventory samples only one configured Binance wallet",
        }
    )
    report["inventory_checks"] = []

    def save():
        rendered = redact_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str), (settings.etherscan_api_key,)
        )
        REPORT.write_text(rendered + "\n")

    repo = MemoryRepository()
    async with httpx.AsyncClient(timeout=20, trust_env=False, follow_redirects=False) as http:
        market = MarketService(repo, settings, http=http)
        capture = SupplyCapture(market)
        try:
            if not inventory_only:
                for token in market.tokens.values():
                    for contract in token.get("contracts", []):
                        item = {
                            "token_id": token["token_id"],
                            "chain": contract["chain"],
                            "chain_id": contract.get("chain_id"),
                        }
                        if not item["chain_id"]:
                            item.update(status="NOT_APPLICABLE_NON_EVM", reason="No configured EVM chain ID")
                        else:
                            try:
                                value = await capture.etherscan(
                                    token,
                                    item["chain_id"],
                                    module="proxy",
                                    action="eth_call",
                                    to=contract["address"],
                                    data="0x313ce567",
                                    tag="latest",
                                )
                                if not isinstance(value, str) or not value.startswith("0x") or value == "0x":
                                    raise ValueError("Missing ERC20 decimals result")
                                decimals = int(value, 16)
                                if not 0 <= decimals <= 255:
                                    raise ValueError("Invalid ERC20 decimals result")
                                item.update(status="PASS", decimals=decimals)
                            except Exception as exc:
                                item.update(
                                    status="UNAVAILABLE",
                                    error_type=type(exc).__name__,
                                    reason=redact_text(str(exc), (settings.etherscan_api_key,)),
                                )
                        report["contract_checks"].append(item)
                        save()
                token = market.tokens["uniswap:UNI"]
                rows = await capture.etherscan(
                    token,
                    1,
                    module="account",
                    action="tokentx",
                    contractaddress=token["contracts"][0]["address"],
                    page=1,
                    offset=2,
                    sort="desc",
                )
                report["contract_transfer_sample"] = {
                    "rows": len(rows) if isinstance(rows, list) else None,
                    "required_fields_present": isinstance(rows, list)
                    and bool(rows)
                    and all(
                        all(
                            key in row for key in ("hash", "from", "to", "timeStamp", "value", "tokenDecimal")
                        )
                        for row in rows
                    ),
                }
                save()
            for token_id in ("uniswap:UNI", "layerzero:ZRO", "world:WLD"):
                try:
                    result = await capture.inventory(deepcopy(market.tokens[token_id]))
                    report["inventory_checks"].append(
                        {
                            "token_id": token_id,
                            "status": "RETURNED",
                            "available": result["available"],
                            "balances": result["balances"],
                            "coverage_errors": result["coverage_errors"],
                        }
                    )
                except Exception as exc:
                    report["inventory_checks"].append(
                        {
                            "token_id": token_id,
                            "status": "FAILED",
                            "error_type": type(exc).__name__,
                            "reason": redact_text(str(exc), (settings.etherscan_api_key,)),
                        }
                    )
                save()
        finally:
            await market.close()
    report["completed_at"] = iso()
    report["unavailable_is_not_zero"] = True
    save()
    print(json.dumps(report, ensure_ascii=False, default=str))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory-only", action="store_true")
    args = parser.parse_args()
    try:
        asyncio.run(main(args.inventory_only))
    except Exception as exc:
        print(json.dumps({"status": "PROBE_ABORTED", "error_type": type(exc).__name__}))
        raise SystemExit(1) from None
