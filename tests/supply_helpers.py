"""Records of the supply statements (analysis/sql/supply_inventory.sql, supply_lending.sql and supply_chains.sql)
for tests of the rules that read them, made from the supply rows a test writes down.

A test that describes a token with `supply` rows gets the records the statements return for them: per window the
24-hour change of the exchange inventory with its history, the utilization change of the lending pools and the
stablecoin supply of the chains. The statements themselves are tested against a real PostgreSQL server in
tests/test_truth_supply_sql.py."""

from datetime import timedelta
from decimal import Decimal

from tank.analysis import truth, truth_sql
from tank.util import parse_time

DAY = timedelta(days=1)


def _number(value):
    return None if value is None else Decimal(str(value))


def _visible(row, assembled):
    for key in ("received_ts", "recorded_at"):
        if assembled and row.get(key) and parse_time(row[key]) > assembled:
            return False
    return True


def _asof(rows, target, maxage):
    found = [
        r for r in rows if truth._time(r) <= target and (target - truth._time(r)).total_seconds() <= maxage
    ]
    return max(found, key=truth._time) if found else None


def _balances(row):
    if row is None:
        return {}
    return {
        (b.get("venue"), b.get("chainid"), b.get("contract"), b.get("wallet_list_commit")): b
        for b in row.get("balances") or []
        if b.get("complete") is True and _number(b.get("qty_native")) is not None
    }


def _total(row, keys):
    held = _balances(row)
    if not keys or not all(k in held for k in keys):
        return None
    return sum((_number(held[k]["qty_native"]) for k in keys), Decimal(0))


@truth.precise
def inventory_records(rows, end, starts, lookback):
    inventories = sorted(
        (
            r
            for r in rows
            if r.get("kind", r.get("type")) in ("INVENTORY", "EXCHANGE_INVENTORY")
            and end - lookback <= truth._time(r) <= end
        ),
        key=truth._time,
    )
    current = inventories[-1] if inventories else None
    previous = _asof(inventories, end - DAY, 21600)
    now = _balances(current)
    old = _balances(previous) if previous and previous.get("available") is not False else {}
    cohort = sorted(set(now) & set(old), key=str)
    cohort_set = set(cohort)
    per = [(key, _number(now[key]["qty_native"]), _number(old[key]["qty_native"])) for key in cohort]
    base = sum((o for _, _, o in per), Decimal(0)) if per else None
    total = sum((n for _, n, _ in per), Decimal(0)) if per else None
    change = total - base if per else None
    history = []
    for r in inventories:
        n = _total(r, cohort_set)
        past = _asof(inventories, truth._time(r) - DAY, 21600)
        o = _total(past, cohort_set) if past else None
        if n is not None and o:
            history.append((truth._time(r), float((n - o) / o)))
    found = []
    for name, start in starts.items():
        samples = [x for at, x in history if at < start]
        found.append(
            truth_sql.SupplyInventory(
                window=name,
                current_at=truth._time(current) if current else None,
                previous_at=truth._time(previous) if previous else None,
                first_at=truth._time(inventories[0]) if inventories else None,
                observed_balances=len(current.get("balances") or []) if current else None,
                complete_balances=len(now),
                comparable_balances=len(cohort),
                base=base,
                total=total,
                change=change,
                pct=float(change / base) if per and base else None,
                samples=len(samples),
                normal_low=truth.percentile(samples, 0.025) if samples else None,
                normal_high=truth.percentile(samples, 0.975) if samples else None,
                venues=tuple(k[0] for k, _, _ in per),
                chainids=tuple(k[1] for k, _, _ in per),
                contracts=tuple(k[2] for k, _, _ in per),
                qty_native=tuple(n for _, n, _ in per),
                chg_native=tuple(n - o for _, n, o in per),
                chg_pct=tuple(float((n - o) / o) if o else None for _, n, o in per),
            )
        )
    return found


@truth.precise
def pool_records(rows, end):
    lending = sorted((r for r in rows if r.get("kind", r.get("type")) == "LENDING"), key=truth._time)
    latest = lending[-1] if lending else None
    prior = _asof([r for r in lending if r.get("available") is not False], end - DAY, 1800)
    earlier = {p["pool_id"]: p for p in (prior.get("pools") or [])} if prior else {}
    found = {}
    for pool in (latest.get("pools") or []) if latest else []:
        a, b = (
            truth.number(pool.get("utilization")),
            truth.number(earlier.get(pool["pool_id"], {}).get("utilization")),
        )
        found[pool["pool_id"]] = truth_sql.SupplyPool(
            pool_id=pool["pool_id"], chg_24h=a - b if None not in (a, b) else None
        )
    return list(found.values())


@truth.precise
def chain_records(rows, end):
    latest = {}
    for r in sorted(
        (r for r in rows if r.get("kind", r.get("type")) in ("CHAIN_LIQUIDITY", "STABLECOIN_CHAIN")),
        key=truth._time,
    ):
        latest[r.get("chain")] = r
    found = []
    for chain, r in latest.items():
        if r.get("available") is False:
            found.append(
                truth_sql.SupplyChain(
                    chain=chain,
                    observed_at=truth._time(r),
                    available=False,
                    reason=r.get("reason", "INCOMPLETE"),
                    supply_usd=None,
                    supply_chg_24h=None,
                    supply_chg_7d=None,
                )
            )
            continue
        series = sorted(
            (p for p in r.get("series") or [] if parse_time(p["ts"]) <= end),
            key=lambda p: parse_time(p["ts"]),
        )

        def point(target, series=series):
            fresh = [
                p
                for p in series
                if parse_time(p["ts"]) <= target and (target - parse_time(p["ts"])).total_seconds() <= 86400
            ]
            return _number(fresh[-1].get("supply_usd")) if fresh else None

        n, p1, p7 = point(end), point(end - DAY), point(end - 7 * DAY)
        found.append(
            truth_sql.SupplyChain(
                chain=chain,
                observed_at=truth._time(r),
                available=r.get("available"),
                reason=r.get("reason"),
                supply_usd=n,
                supply_chg_24h=n - p1 if None not in (n, p1) else None,
                supply_chg_7d=n - p7 if None not in (n, p7) else None,
            )
        )
    return sorted(found, key=lambda c: (c.observed_at, c.chain or ""))


def supply_evidence(snapshot, *, end=None, windows=None, config=None):
    """The records of the supply statements for the `supply` rows of a snapshot, at its cutoff (or `end`), in these
    windows (name to seconds, by default those of an assessment)."""
    end = end or parse_time(snapshot["cutoff"])
    assembled = parse_time(snapshot["assembled_at"]) if snapshot.get("assembled_at") else None
    if windows is None:
        anchor = snapshot.get("anchor")
        anchor_at = anchor.get("ts") if isinstance(anchor, dict) else anchor
        start = parse_time(anchor_at) if anchor_at else end - timedelta(hours=4)
        windows = dict(truth.WINDOWS, EVENT=(end - start).total_seconds())
    lookback = timedelta(days=int((config or {}).get("historical_lookback_days", 90)))
    rows = [r for r in snapshot.get("supply", []) if truth._time(r) is not None and truth._time(r) <= end]
    rows = [r for r in rows if _visible(r, assembled)]
    starts = {name: end - timedelta(seconds=seconds) for name, seconds in windows.items()}
    return truth_sql.SupplyEvidence(
        inventory=tuple(inventory_records(rows, end, starts, lookback)),
        pools=tuple(pool_records(rows, end)),
        chains=tuple(chain_records(rows, end)),
    )


def with_supply(snapshot, **kwargs):
    """The snapshot with the supply evidence of its rows as `supply_sql`."""
    return dict(snapshot, supply_sql=supply_evidence(snapshot, **kwargs))


def window_supply(snapshot, start, end, *, name="W4H"):
    """The snapshot with the supply evidence of the one window [start, end] named `name`, which `truth.supply_truth`
    reads as the snapshot's `_primary` window."""
    windows = {name: (end - start).total_seconds()}
    return dict(snapshot, supply_sql=supply_evidence(snapshot, end=end, windows=windows), _primary=name)
