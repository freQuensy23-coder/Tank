"""Supply observations retain partial coverage and avoid interpreting transfers as sales."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal as D

from tank.time_precision import collection_clock, propagate_timestamp
from tank.util import iso, parse_time, sha256, utcnow

from .normalize import SemanticError, precise

CHAIN_IDS = {
    "Ethereum": 1,
    "BNB Chain": 56,
    "Base": 8453,
    "Arbitrum": 42161,
    "Optimism": 10,
    "Polygon": 137,
    "Avalanche": 43114,
}
# Registry chain name -> DefiLlama stablecoincharts chain name, where they differ.
DEFILLAMA_CHAINS = {"BNB Chain": "BSC"}


def supply_cadences(hot):
    """The implemented §5.6 refresh periods, shared by polling and snapshots."""
    return {
        "EXCHANGE_INVENTORY": 900 if hot else 21600,
        "WALLET_FLOWS": 900 if hot else 21600,
        "UNLOCKS": 86400,
        "LENDING": 1800,
        "CHAIN_LIQUIDITY": 21600,
        "FUNDAMENTALS": 21600,
    }


@precise
def unlock_schedule(payload, *, now=None):
    now = parse_time(now or iso())
    end = now + timedelta(days=14)
    result = []
    documented = payload.get("documentedData", {}).get("data")
    if not isinstance(documented, list):
        raise SemanticError("DefiLlama documented emissions schema absent")
    for bucket in documented:
        previous = None
        for point in sorted(bucket["data"], key=lambda r: r["timestamp"]):
            amount = D(str(point["unlocked"]))
            ts = parse_time(point["timestamp"])
            if previous is not None and now <= ts <= end and amount > previous:
                result.append(
                    {
                        **propagate_timestamp(point, "timestamp").fields("date"),
                        "qty": str(amount - previous),
                        "label": bucket["label"],
                        "kind": "POTENTIAL_SUPPLY",
                        "sale_inferred": False,
                    }
                )
            previous = amount
    return result


class SupplyCapture:
    def __init__(self, market):
        self.market = market
        self.repo = market.repo
        self.settings = market.settings
        # /lendBorrow exposes stable UUIDs and measurements, but no project or
        # chain. Resolve those identities from the versioned registry, never a
        # second runtime feed whose availability could erase lending coverage.
        catalog = self.settings.load_config("registry").get("lending_pool_catalog", {})
        self.lending_pool_catalog = catalog
        self.lending_pool_identities = {row["pool_id"]: row for row in catalog.get("pools", [])}

    def instrument(self, token):
        return {"instrument_id": "supply:" + token["token_id"], "token_id": token["token_id"]}

    async def record(self, token, kind, **fields):
        if self.settings.etherscan_api_key:
            fields = {
                k: (v.replace(self.settings.etherscan_api_key, "[REDACTED]") if isinstance(v, str) else v)
                for k, v in fields.items()
            }
        observed = iso()
        row = dict(
            token_id=token["token_id"],
            **collection_clock(observed).fields("ts"),
            **collection_clock(observed).fields("received_ts"),
            kind=kind,
            **fields,
        )
        await self.market._put("mkt.supply_obs", token["token_id"] + ":" + kind + ":" + row["ts"], row)
        return row

    async def request(self, token, kind, url, **kwargs):
        raw = await self.market._request("GET", url, **kwargs)
        await self.market._raw(self.instrument(token), kind, raw)
        return raw

    async def poll(self, hot):
        state = getattr(self.market, "_supply_refresh", {})
        self.market._supply_refresh = state
        now = utcnow()
        for token in self.market.tokens.values():
            token_id = token["token_id"]
            periods = supply_cadences(token_id in hot)
            for kind, seconds, call in [
                ("unlocks", periods["UNLOCKS"], self.unlocks),
                ("inventory", periods["EXCHANGE_INVENTORY"], self.inventory),
                ("lending", periods["LENDING"], self.lending),
                ("chain_liquidity", periods["CHAIN_LIQUIDITY"], self.chain_liquidity),
                ("fundamentals", periods["FUNDAMENTALS"], self.fundamentals),
            ]:
                if (token_id, kind) in state and (now - state[token_id, kind]).total_seconds() < seconds:
                    continue
                try:
                    await call(token)
                except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                    await self.record(token, kind.upper(), available=False, reason=str(exc))
                state[token_id, kind] = now

    async def unlocks(self, token):
        slug = token.get("unlock_slug")
        if not slug:
            return await self.record(token, "UNLOCKS", available=False, reason="NO_PUBLISHED_DATASET")
        raw = await self.request(token, "unlocks", "https://defillama-datasets.llama.fi/emissions/" + slug)
        schedule = unlock_schedule(raw)
        for point in schedule:
            row = dict(token_id=token["token_id"], observed_at=iso(), **point)
            await self.market._put(
                "mkt.unlocks", token["token_id"] + ":" + point["date"] + ":" + point["label"], row
            )
        return await self.record(
            token,
            "UNLOCKS",
            available=True,
            unlocks=schedule,
            inference_limits=["POTENTIAL_SUPPLY_IS_NOT_A_SALE"],
        )

    async def etherscan(self, token, chainid, **params):
        if not self.settings.etherscan_api_key:
            raise SemanticError("ETHERSCAN_API_KEY_UNCONFIGURED")
        raw = await self.market._request(
            "GET",
            "https://api.etherscan.io/v2/api",
            params={"chainid": chainid, "apikey": self.settings.etherscan_api_key, **params},
        )
        # Archive payload contains no API credentials (response only).
        await self.market._raw(self.instrument(token), "etherscan_" + params.get("action", ""), raw)
        if raw.get("status") == "0" and raw.get("message") != "No transactions found":
            raise SemanticError("Etherscan request failed: " + str(raw.get("result")))
        if "error" in raw:
            raise SemanticError("Etherscan proxy error")
        return raw["result"]

    @precise
    async def inventory(self, token):
        contracts = token.get("contracts", [])
        if not contracts:
            return await self.record(
                token, "EXCHANGE_INVENTORY", available=False, reason="NOT_SUPPORTED_NATIVE_OR_NON_EVM"
            )
        wallets = self.settings.load_config("exchange_addresses")["wallet_lists"]
        balances = []
        transfers = []
        errors = []
        transfer_queries_succeeded = 0
        for contract in contracts:
            chainid = contract.get("chain_id") or contract.get("chainid") or CHAIN_IDS.get(contract["chain"])
            if chainid is None:
                errors.append({"chain": contract["chain"], "reason": "CHAIN_NOT_SUPPORTED"})
                continue
            decimals = contract.get("decimals")
            if decimals is None:
                # ERC-20 decimals() selector. No inference from symbol or popular defaults.
                try:
                    value = await self.etherscan(
                        token,
                        chainid,
                        module="proxy",
                        action="eth_call",
                        to=contract["address"],
                        data="0x313ce567",
                        tag="latest",
                    )
                    if not isinstance(value, str) or not value.startswith("0x") or value == "0x":
                        raise SemanticError("ERC20 decimals unavailable")
                    decimals = int(value, 16)
                    if not 0 <= decimals <= 255:
                        raise SemanticError("invalid ERC20 decimals")
                except Exception as exc:  # noqa: BLE001 - one unavailable chain must not discard other contracts
                    errors.append(
                        {
                            "chain": contract["chain"],
                            "chainid": chainid,
                            "contract": contract["address"],
                            "operation": "decimals",
                            "reason": str(exc),
                        }
                    )
                    continue
                contract["decimals"] = decimals
            for group in wallets:
                total = D(0)
                observed = 0
                all_addresses = {a.lower() for a in group["addresses"]}
                for wallet in group["addresses"]:
                    try:
                        raw = await self.etherscan(
                            token,
                            chainid,
                            module="account",
                            action="tokenbalance",
                            contractaddress=contract["address"],
                            address=wallet,
                            tag="latest",
                        )
                        total += D(raw) / (D(10) ** decimals)
                        observed += 1
                        # Hash/tx index dedup; pages sorted descending, stop after 24h.
                        for page in range(1, 101):
                            rows = await self.etherscan(
                                token,
                                chainid,
                                module="account",
                                action="tokentx",
                                contractaddress=contract["address"],
                                address=wallet,
                                page=page,
                                offset=100,
                                sort="desc",
                            )
                            if not isinstance(rows, list):
                                raise SemanticError("unexpected tokentx payload")
                            transfer_queries_succeeded += 1
                            for row in rows:
                                if parse_time(row["timeStamp"]) < utcnow() - timedelta(days=1):
                                    continue
                                internal = (
                                    row["from"].lower() in all_addresses
                                    and row["to"].lower() in all_addresses
                                )
                                # Unlabeled origin is ambiguous. Never force accumulation or sale.
                                transfers.append(
                                    {
                                        "chainid": chainid,
                                        "tx_hash": row["hash"],
                                        "log_index": row.get("logIndex"),
                                        "from": row["from"],
                                        "to": row["to"],
                                        "ts": iso(row["timeStamp"]),
                                        "ts_prec": "EXACT",
                                        "qty_native": str(
                                            D(row["value"]) / (D(10) ** int(row["tokenDecimal"]))
                                        ),
                                        "classification": "CEX_CUSTODY_MIGRATION"
                                        if internal
                                        else "AMBIGUOUS",
                                        "exchange": group["venue"],
                                        "reaches_exchange": row["to"].lower() in all_addresses,
                                        "directional_inference": None,
                                    }
                                )
                            if len(rows) < 100 or any(
                                parse_time(r["timeStamp"]) < utcnow() - timedelta(days=1) for r in rows
                            ):
                                break
                    except Exception as exc:  # noqa: BLE001 - isolate venue failures; persist explicit capture gaps
                        errors.append({"wallet": wallet, "chainid": chainid, "reason": str(exc)})
                balances.append(
                    {
                        "venue": group["venue"],
                        "chainid": chainid,
                        "contract": contract["address"],
                        "qty_native": str(total) if observed else None,
                        "observed_wallets": observed,
                        "expected_wallets": len(group["addresses"]),
                        "complete": observed == len(group["addresses"]),
                        "wallet_list_commit": group["commit"],
                    }
                )
        unique = {
            sha256({k: r.get(k) for k in ("chainid", "tx_hash", "log_index", "from", "to", "qty_native")}): r
            for r in transfers
        }
        await self.record(
            token,
            "WALLET_FLOWS",
            available=transfer_queries_succeeded > 0,
            flows=list(unique.values()),
            coverage_errors=errors,
            transfer_queries_succeeded=transfer_queries_succeeded,
            reason=None if transfer_queries_succeeded else "SOURCE_FAILED" if errors else "INCOMPLETE",
            classification_required=True,
        )
        return await self.record(
            token,
            "EXCHANGE_INVENTORY",
            available=any(b["qty_native"] is not None for b in balances),
            balances=balances,
            coverage_errors=errors,
            unsupported_venues=["coinbase", "bybit", "kraken"],
        )

    @precise
    async def lending(self, token):
        now = utcnow()
        cached = getattr(self.market, "_lend_cache", None)
        if cached is None or (now - cached[0]).total_seconds() >= 1800:
            borrow = await self.request(token, "lending_borrow", "https://yields.llama.fi/lendBorrow")
            if not isinstance(borrow, list):
                raise SemanticError("DefiLlama lendBorrow schema absent")
            self.market._lend_cache = (now, borrow)
        _, borrow = self.market._lend_cache
        contracts = {(c["chain"], c["address"].lower()) for c in token.get("contracts", [])}
        addresses = {address for _, address in contracts}
        matched = []
        unmapped = []
        for pool in borrow:
            underlying = {a.lower() for a in pool.get("underlyingTokens") or []}
            if not addresses.intersection(underlying):
                continue
            metadata = self.lending_pool_identities.get(pool["pool"])
            if metadata is None:
                unmapped.append(pool["pool"])
                continue
            project = metadata.get("project", "")
            if project not in {
                "aave",
                "aave-v2",
                "aave-v3",
                "aave-v4",
                "morpho",
                "morpho-aave",
                "morpho-aavev3",
                "morpho-blue",
                "euler",
                "euler-v2",
            }:
                continue
            registered = set(metadata.get("underlying_tokens", []))
            if not any((metadata["chain"], address) in contracts for address in underlying & registered):
                continue
            supply = pool.get("totalSupplyUsd")
            debt = pool.get("totalBorrowUsd")
            utilization = (
                D(str(debt)) / D(str(supply))
                if supply is not None and debt is not None and D(str(supply)) > 0
                else None
            )
            matched.append(
                {
                    "pool_id": pool["pool"],
                    "project": project,
                    "chain": metadata.get("chain"),
                    "utilization": utilization,
                    "borrow_apy": pool.get("apyBaseBorrow"),
                    "total_supply_usd": supply,
                    "total_borrow_usd": debt,
                }
            )
        return await self.record(
            token,
            "LENDING",
            available=bool(matched),
            pools=matched,
            identity_evidence={
                "match_method": "REGISTERED_POOL_UUID_CHAIN_UNDERLYING_CONTRACT",
                "unmapped_pool_ids": sorted(unmapped),
                "pool_identity_source": {
                    key: self.lending_pool_catalog.get(key)
                    for key in ("source_url", "captured_at", "source_sha256")
                },
            },
            reason=None if matched else "NO_MATCHED_LENDING_POOLS",
        )

    @precise
    async def chain_liquidity(self, token):
        chains = (
            token.get("settlement_chains")
            or ([token["defillama_chain_name"]] if token.get("defillama_chain_name") else None)
            or token.get("chains")
            or sorted({c["chain"] for c in token.get("contracts", [])})
        )
        if not chains:
            return await self.record(
                token, "CHAIN_LIQUIDITY", available=False, reason="SETTLEMENT_CHAIN_UNCONFIGURED"
            )
        for chain in chains:
            try:
                data = await self.request(
                    token,
                    "chain_liquidity",
                    "https://stablecoins.llama.fi/stablecoincharts/" + DEFILLAMA_CHAINS.get(chain, chain),
                )
                if not isinstance(data, list):
                    raise SemanticError("stablecoin chart schema mismatch")
            except Exception as exc:  # noqa: BLE001 - one chain's gap must not erase the other chains
                await self.record(token, "CHAIN_LIQUIDITY", available=False, chain=chain, reason=str(exc))
                continue
            values = []
            for row in data:
                circulating = row.get("totalCirculatingUSD")
                if not isinstance(circulating, dict):
                    continue
                values.append(
                    {
                        **propagate_timestamp(row, "date").fields("ts"),
                        "supply_usd": str(sum((D(str(v)) for v in circulating.values()), D(0))),
                    }
                )
            await self.record(token, "CHAIN_LIQUIDITY", available=bool(values), chain=chain, series=values)

    @precise
    async def fundamentals(self, token):
        if token.get("fundamentals_status") == "NOT_SUPPORTED":
            return await self.record(
                token,
                "FUNDAMENTALS",
                available=False,
                reason="NO_SUPPORTED_PUBLIC_FEES_ADAPTER",
                identity_evidence=token.get("fundamentals_evidence"),
                inference_limits=["UNSUPPORTED_IS_NOT_ZERO_FEES"],
            )
        # Slugs come only from the versioned registry; HLD §4.8 lists no catalog endpoint.
        slugs = token.get("defillama_protocol_slugs") or (
            [token["defillama_protocol_slug"]] if token.get("defillama_protocol_slug") else None
        )
        evidence = token.get("fundamentals_evidence")
        if not slugs:
            return await self.record(
                token,
                "FUNDAMENTALS",
                available=False,
                reason="NO_REGISTERED_DEFILLAMA_PROTOCOL_SLUG",
                identity_evidence=evidence,
            )
        protocols = {}
        errors = []
        for slug in slugs:
            metrics = {}
            for kind in ("dailyFees", "dailyRevenue", "dailyHoldersRevenue"):
                if token.get("fundamentals_evidence", {}).get("metrics", {}).get(kind) == "NOT_SUPPORTED":
                    metrics[kind] = {
                        "available": False,
                        "reason": "PROVIDER_METRIC_NOT_SUPPORTED",
                        "evidence": token["fundamentals_evidence"],
                    }
                    continue
                try:
                    raw = await self.request(
                        token,
                        "fundamentals",
                        "https://api.llama.fi/summary/fees/" + slug,
                        params={"dataType": kind},
                    )
                    metrics[kind] = {k: raw.get(k) for k in ("total24h", "total30d", "totalDataChart")}
                    metrics[kind].update(
                        available=raw.get("total24h") is not None
                        or raw.get("total30d") is not None
                        or bool(raw.get("totalDataChart")),
                        source_url="https://api.llama.fi/summary/fees/" + slug + "?dataType=" + kind,
                    )
                except Exception as exc:  # noqa: BLE001 - preserve available fundamentals when one metric is unsupported
                    metrics[kind] = {"available": False, "reason": str(exc)}
                    errors.append({"slug": slug, "metric": kind, "reason": str(exc)})
            protocols[slug] = metrics
        aggregate = {}
        for kind in ("dailyFees", "dailyRevenue", "dailyHoldersRevenue"):
            metrics = [p[kind] for p in protocols.values()]
            available = [m for m in metrics if m.get("available")]
            totals = {}
            for field in ("total24h", "total30d"):
                values = [D(str(m[field])) for m in available if m.get(field) is not None]
                totals[field] = str(sum(values, D(0))) if values else None
            chart = {}
            for metric in available:
                for stamp, value in metric.get("totalDataChart") or []:
                    if value is not None:
                        chart[stamp] = chart.get(stamp, D(0)) + D(str(value))
            aggregate[kind] = dict(
                available=bool(available),
                complete=len(available) == len(metrics),
                totalDataChart=[[ts, str(v)] for ts, v in sorted(chart.items())],
                **totals,
            )
        return await self.record(
            token,
            "FUNDAMENTALS",
            available=any(m["available"] for m in aggregate.values()),
            metrics=aggregate,
            protocols=protocols,
            identity_evidence=evidence,
            inference_limits=token.get(
                "fundamentals_inference_limits", ["PROVIDER_REVENUE_IS_NOT_PROOF_OF_TOKEN_CAPTURE"]
            ),
            fee_scope=token.get("defillama_fee_scope"),
            coverage_errors=errors,
        )
