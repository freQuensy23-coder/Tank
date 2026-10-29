"""Supply observations retain partial coverage and avoid interpreting transfers as sales."""

import logging
import math
import time
from datetime import datetime, timedelta
from decimal import Decimal as D

from tank.market.normalize import SemanticError, precise
from tank.time_precision import collection_clock, propagate_timestamp
from tank.util import iso, parse_time, sha256, utcnow

CHAIN_IDS = {
    "Ethereum": 1,
    "BNB Chain": 56,
    "Base": 8453,
    "Arbitrum": 42161,
    "Optimism": 10,
    "Polygon": 137,
    "Avalanche": 43114,
}

DEFILLAMA_CHAINS = {"BNB Chain": "BSC"}


REFRESH_KINDS = {
    "unlocks": "UNLOCKS",
    "inventory": "EXCHANGE_INVENTORY",
    "lending": "LENDING",
    "chain_liquidity": "CHAIN_LIQUIDITY",
    "fundamentals": "FUNDAMENTALS",
}


SUPPLY_LOOP_SLEEP_S = 60

ETHERSCAN_HOST = "api.etherscan.io"
ETHERSCAN_PACING_WINDOW_S = 3600
ETHERSCAN_DAY_S = 86400
ETHERSCAN_BUDGET_EXHAUSTED = "ETHERSCAN_BUDGET_EXHAUSTED"
ETHERSCAN_DAILY_LIMIT_REACHED = "ETHERSCAN_DAILY_LIMIT_REACHED"

log = logging.getLogger(__name__)


class EtherscanBudgetDeferred(Exception):
    """An Etherscan call the budget cannot admit now. The refresh waits for the budget and
    records nothing, so the last observation stays the latest one."""

    def __init__(self, reason, retry_s):
        super().__init__(reason)
        self.reason = reason
        self.retry_s = retry_s


class EtherscanBudget:
    """03 §5.6: the Etherscan `tank_max_requests_per_day` of config/api_limits.yaml, enforced.
    A call is admitted only when it fits both a rolling 24 h bucket of that size and an hourly
    pacing bucket of a 24th of it, kept in the shared request ledger (so a restart does not
    forget the day), and the day's calls are spread over the day instead of being spent by
    the morning. A refresh reserves its calls before it starts; once a reservation is refused
    no other refresh of the same pass reserves, so the most overdue refresh gets the next
    budget. Etherscan's own "Max daily rate limit" stops every call until 00:00 UTC, when
    Etherscan resets the day. Without a configured daily limit nothing is enforced."""

    def __init__(self, settings):
        try:
            limits = settings.load_config("api_limits").get("limits", [])
        except FileNotFoundError:
            limits = []
        rule = next((r for r in limits if r.get("provider") == "etherscan"), {})
        self.per_day = rule.get("tank_max_requests_per_day")
        self.prepaid = 0
        self.refused = None
        self.blocked_until = None

    def open_pass(self):
        self.refused = None

    def _blocked(self):
        if self.blocked_until is None:
            return None
        remaining = (self.blocked_until - utcnow()).total_seconds()
        if remaining > 0:
            return EtherscanBudgetDeferred(ETHERSCAN_DAILY_LIMIT_REACHED, remaining)
        self.blocked_until = None
        return None

    def provider_limit_reached(self, now=None):
        now = now or utcnow()
        self.blocked_until = datetime.combine(
            now.date() + timedelta(days=1), datetime.min.time(), tzinfo=now.tzinfo
        )
        return EtherscanBudgetDeferred(
            ETHERSCAN_DAILY_LIMIT_REACHED, (self.blocked_until - now).total_seconds()
        )

    async def reserve(self, calls):
        """Charge `calls` upcoming calls to the budget, or raise EtherscanBudgetDeferred."""
        from tank.sources.rate_limits import shared_request_budgets

        blocked = self._blocked()
        if blocked is not None:
            raise blocked
        needed = calls - self.prepaid
        if needed <= 0:
            return
        if self.per_day is not None:
            if self.refused is not None:
                raise self.refused
            for window_s, capacity in (
                (ETHERSCAN_PACING_WINDOW_S, self.per_day * ETHERSCAN_PACING_WINDOW_S / ETHERSCAN_DAY_S),
                (ETHERSCAN_DAY_S, self.per_day),
            ):
                wait = await shared_request_budgets.try_acquire(
                    ETHERSCAN_HOST, capacity, window_s, weight=min(needed, capacity), host=ETHERSCAN_HOST
                )
                if wait > 0:
                    self.refused = EtherscanBudgetDeferred(ETHERSCAN_BUDGET_EXHAUSTED, wait)
                    raise self.refused
        self.prepaid += needed

    async def spend(self):
        """Admit one call: from the reserved calls, otherwise by reserving it."""
        blocked = self._blocked()
        if blocked is not None:
            raise blocked
        if self.prepaid <= 0:
            await self.reserve(1)
        self.prepaid -= 1


def refresh_due(age_s, cadence_s, cycle_s):
    """03 §5.10: a kind must never be older than its §5.6 cadence. A pass over all tokens takes
    minutes and the loop sleeps between passes, so refreshing only at age >= cadence left every
    kind stale for a few minutes per period. Refresh at 0.9 x cadence, or earlier by two full
    poll cycles (longest pass + loop sleep), whichever comes first."""
    return age_s >= max(0.0, min(0.9 * cadence_s, cadence_s - 2 * cycle_s))


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

        catalog = self.settings.load_config("registry").get("lending_pool_catalog", {})
        self.lending_pool_catalog = catalog
        self.lending_pool_identities = {row["pool_id"]: row for row in catalog.get("pools", [])}
        self.budget = EtherscanBudget(self.settings)
        self.deferred_reasons = set()

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
        return await self.market._request("GET", url, **kwargs)

    async def resume_refresh_state(self, state, now):
        """Restore the last refresh of each step from stored observations, so a restart
        resumes the §5.6 cadences instead of restarting the cycle at the first token."""
        for token_id in self.market.tokens:
            for kind, observation in REFRESH_KINDS.items():
                rows = await self.repo.list_latest(
                    "mkt.supply_obs",
                    "ts",
                    filters={"token_id": token_id, "kind": observation},
                    limit=1,
                    start=iso(now - timedelta(seconds=max(supply_cadences(False).values()))),
                )
                if rows:
                    state[token_id, kind] = parse_time(rows[0]["ts"])

    async def poll(self, hot):
        state = getattr(self.market, "_supply_refresh", {})
        self.market._supply_refresh = state
        started = time.monotonic()
        cycle = getattr(self.market, "_supply_longest_pass_s", 0.0) + SUPPLY_LOOP_SLEEP_S
        now = utcnow()
        if not getattr(self.market, "_supply_refresh_resumed", False):
            await self.resume_refresh_state(state, now)
            self.market._supply_refresh_resumed = True
        self.budget.open_pass()
        deferred = {}
        for token in sorted(
            self.market.tokens.values(),
            key=lambda token: self.inventory_overdue(state, token["token_id"], hot, now),
            reverse=True,
        ):
            token_id = token["token_id"]
            periods = supply_cadences(token_id in hot)
            for kind, seconds, call in [
                ("unlocks", periods["UNLOCKS"], self.unlocks),
                ("inventory", periods["EXCHANGE_INVENTORY"], self.inventory),
                ("lending", periods["LENDING"], self.lending),
                ("chain_liquidity", periods["CHAIN_LIQUIDITY"], self.chain_liquidity),
                ("fundamentals", periods["FUNDAMENTALS"], self.fundamentals),
            ]:
                if (token_id, kind) in state and not refresh_due(
                    (now - state[token_id, kind]).total_seconds(), seconds, cycle
                ):
                    continue
                try:
                    await call(token)
                except EtherscanBudgetDeferred as exc:
                    deferred[token_id] = exc
                    continue
                except Exception as exc:
                    await self.record(token, kind.upper(), available=False, reason=str(exc))
                state[token_id, kind] = now
        self.report_budget(deferred)
        self.market._supply_longest_pass_s = max(
            getattr(self.market, "_supply_longest_pass_s", 0.0), time.monotonic() - started
        )

    @staticmethod
    def inventory_overdue(state, token_id, hot, now):
        """Age of the token's exchange inventory and wallet flows in units of its own cadence.
        Passes refresh the most overdue first, so when the Etherscan budget cannot pay for
        every due refresh a hot token (15 min cadence) is served 24 times as often as a base
        token (6 h), and no token waits forever."""
        if (token_id, "inventory") not in state:
            return math.inf
        cadence = supply_cadences(token_id in hot)["EXCHANGE_INVENTORY"]
        return (now - state[token_id, "inventory"]).total_seconds() / cadence

    def report_budget(self, deferred):
        """Log the Etherscan budget state when deferral starts, changes reason or ends."""
        reasons = {exc.reason for exc in deferred.values()}
        if reasons and reasons != self.deferred_reasons:
            log.warning(
                "Etherscan budget %s (%s requests/day): exchange inventory and wallet flows of %s "
                "deferred, budget frees in %.0f s",
                "/".join(sorted(reasons)),
                self.budget.per_day,
                ", ".join(sorted(deferred)),
                min(exc.retry_s for exc in deferred.values()),
            )
        elif not reasons and self.deferred_reasons:
            log.info("Etherscan budget: deferred exchange inventory and wallet flow refreshes resumed")
        self.deferred_reasons = reasons

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
        await self.budget.spend()
        raw = await self.market._request(
            "GET",
            "https://api.etherscan.io/v2/api",
            params={"chainid": chainid, "apikey": self.settings.etherscan_api_key, **params},
        )
        if raw.get("status") == "0" and "Max daily rate limit" in str(raw.get("result")):
            raise self.budget.provider_limit_reached()
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
        ready = []
        for contract in contracts:
            chainid = contract.get("chain_id") or contract.get("chainid") or CHAIN_IDS.get(contract["chain"])
            if chainid is None:
                errors.append({"chain": contract["chain"], "reason": "CHAIN_NOT_SUPPORTED"})
                continue
            decimals = contract.get("decimals")
            if decimals is None:
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
                except EtherscanBudgetDeferred:
                    raise
                except Exception as exc:
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
            ready.append((contract, chainid, decimals))
        await self.budget.reserve(2 * sum(len(group["addresses"]) for group in wallets) * len(ready))
        for contract, chainid, decimals in ready:
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
                    except EtherscanBudgetDeferred as exc:
                        if exc.reason == ETHERSCAN_DAILY_LIMIT_REACHED:
                            raise
                        errors.append({"wallet": wallet, "chainid": chainid, "reason": exc.reason})
                    except Exception as exc:
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
            except Exception as exc:
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
                except Exception as exc:
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
