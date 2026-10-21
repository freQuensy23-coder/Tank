"""V0-A: 34 sources and both market-data paths unattended for 72 h, < 1% failed reads per source (01 §10).

Read-only. Sources: every feed keeps cumulative counters in ops.source_checkpoints["S##:N"]
(`reads` = one feed poll after its retries, `detail_reads` = article/detail fetches; tank-sources,
same counters as `python -m tank.sources.read_report`). The window is the difference of a snapshot
taken at its start and one taken at its end; counters survive restarts.
  A source PASSES with >= 1 read, failed/(ok+failed) < 1% and its feeds polled >= 95% of their cadence.
Market BASE path (REST, 03 §5.1): 1m klines per instrument, OI and funding every 300 s on perps.
  Failed reads = expected polls without a stored live row (source_archive IS NULL) in the window.
Market HOT path (WebSocket): streaming capture gaps while a token is hot, as a share of hot time.

Usage:
  python3 v0a_health.py --snapshot $RUN/start.json                       # at the window start
  python3 v0a_health.py --start-snapshot $RUN/start.json --end-snapshot $RUN/end.json --out $RUN/report
"""

import argparse
import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import iso, parse, query, seconds, utcnow, write_json

LIMIT = 0.01
HOT_GAP_PREFIXES = ("STREAM", "RECONNECT", "TRADE_INTEGRITY", "ORDER_BOOK_STREAM")

KLINES = """
SELECT instrument_id, count(DISTINCT open_time) AS minutes
  FROM mkt.klines_1m
 WHERE open_time >= :'since' AND open_time < :'until' AND source_archive IS NULL
 GROUP BY instrument_id
"""
OI = """
SELECT instrument_id, count(*) AS n, count(DISTINCT date_trunc('minute', ts)) AS distinct_minutes
  FROM mkt.oi_obs
 WHERE ts >= :'since' AND ts < :'until' AND source_archive IS NULL
 GROUP BY instrument_id
"""
FUNDING = """
SELECT instrument_id, count(DISTINCT date_trunc('minute', as_of)) AS polls
  FROM mkt.funding_obs
 WHERE as_of >= :'since' AND as_of < :'until' AND source_archive IS NULL
 GROUP BY instrument_id
"""
INSTRUMENTS = """
SELECT document->>'instrument_id' AS instrument_id, document->>'token_id' AS token_id,
       document->>'venue' AS venue, document->>'market_type' AS market_type
  FROM reg.instruments WHERE coalesce((document->>'active')::boolean, true)
"""
HOT = """
SELECT token_id, instrument_id, since, expires_at FROM mkt.hot_tokens
 WHERE since < :'until' AND (expires_at IS NULL OR expires_at > :'since')
"""
GAPS = """
SELECT instrument_id, token_id, kind, reason, start, "end" FROM mkt.capture_gaps
 WHERE start < :'until' AND coalesce("end", now()) > :'since'
"""


SNAPSHOT = """
SELECT storage_key AS key, document->'reads' AS reads, document->'detail_reads' AS detail_reads,
       document->>'status' AS status, left(coalesce(document->>'error', ''), 300) AS error
  FROM ops.source_checkpoints WHERE storage_key ~ '^S[0-9]{2}:[0-9]+$'
"""


def take_snapshot():
    """Same shape as `python -m tank.sources.read_report --save` (cumulative per-feed read counters)."""
    feeds = {}
    for r in query(SNAPSHOT):
        feeds[r["key"]] = {
            name: dict(r[name] or {"ok": 0, "failed": 0}) for name in ("reads", "detail_reads")
        }
        feeds[r["key"]].update(status=r["status"], error=r["error"])
    return {"taken_at": iso(utcnow()), "feeds": feeds}


def feed_intervals(path):
    """Configured poll interval per feed key S##:N (feed interval, else source interval, else 60 s)."""
    import yaml

    out = {}
    for source in yaml.safe_load(open(path))["sources"]:
        for i, feed in enumerate(source.get("feeds", [])):
            out[f"{source['source_id']}:{i}"] = int(feed.get("interval_s", source.get("interval_s", 60)))
    return out


def sources(start, end, intervals, window_s):
    expected_ids = sorted({k.split(":")[0] for k in intervals})
    rows = {
        sid: {
            "source_id": sid,
            "polls_ok": 0,
            "polls_failed": 0,
            "detail_ok": 0,
            "detail_failed": 0,
            "polls_expected": 0,
            "degraded_feeds": [],
        }
        for sid in expected_ids
    }
    for key, interval in intervals.items():
        rows[key.split(":")[0]]["polls_expected"] += int(window_s // interval)
    for key, now in end["feeds"].items():
        sid = key.split(":")[0]
        row = rows.setdefault(
            sid,
            {
                "source_id": sid,
                "polls_ok": 0,
                "polls_failed": 0,
                "detail_ok": 0,
                "detail_failed": 0,
                "polls_expected": 0,
                "degraded_feeds": [],
            },
        )
        before = start["feeds"].get(key, {})
        for name, prefix in (("reads", "polls"), ("detail_reads", "detail")):
            for field in ("ok", "failed"):
                row[f"{prefix}_{field}"] += now[name][field] - before.get(name, {}).get(field, 0)
        if now.get("status") == "DEGRADED":
            row["degraded_feeds"].append({"feed": key, "error": now.get("error")})
    out = []
    for row in rows.values():
        reads = row["polls_ok"] + row["polls_failed"] + row["detail_ok"] + row["detail_failed"]
        failed = row["polls_failed"] + row["detail_failed"]
        polls = row["polls_ok"] + row["polls_failed"]
        row.update(
            reads=reads,
            failures=failed,
            failed_ratio=failed / reads if reads else None,
            poll_coverage=polls / row["polls_expected"] if row["polls_expected"] else None,
        )
        continuous = row["poll_coverage"] is not None and row["poll_coverage"] >= 0.95
        row["status"] = "PASS" if reads and failed / reads < LIMIT and continuous else "FAIL"
        out.append(row)
    return out


def rest_reads(path, since, until):
    """Sum the market service's per-minute "rest reads: <host>=<ok>/<failed>" lines (counters reset each line)."""
    import re as _re

    t0, t1 = parse(since), parse(until)
    hosts = defaultdict(lambda: {"ok": 0, "failed": 0})
    lines = 0
    for line in open(path):
        m = _re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?).*rest reads:(.*)$", line)
        if not m:
            continue
        at = parse(m.group(1) if m.group(1).endswith("Z") else m.group(1) + "Z")
        if not (t0 <= at < t1):
            continue
        lines += 1
        for host, ok, failed in _re.findall(r"(\S+)=(\d+)/(\d+)", m.group(2)):
            hosts[host]["ok"] += int(ok)
            hosts[host]["failed"] += int(failed)
    rows = []
    for host, c in sorted(hosts.items()):
        total = c["ok"] + c["failed"]
        ratio = c["failed"] / total if total else None
        rows.append(
            {
                "host": host,
                **c,
                "failed_ratio": ratio,
                "status": "PASS" if total and ratio < LIMIT else "FAIL",
            }
        )
    return {
        "log_lines": lines,
        "expected_lines": int(seconds(until, since) // 60),
        "hosts": rows,
        "status": "PASS" if rows and all(r["status"] == "PASS" for r in rows) else "FAIL",
    }


def market(since, until):
    p = {"since": since, "until": until}
    window_s = seconds(until, since)
    inst = query(INSTRUMENTS)
    kl = {r["instrument_id"]: r["minutes"] for r in query(KLINES, p)}
    oi = {r["instrument_id"]: r["distinct_minutes"] for r in query(OI, p)}
    fu = {r["instrument_id"]: r["polls"] for r in query(FUNDING, p)}
    base = []
    for i in inst:
        iid = i["instrument_id"]
        exp_k = int(window_s // 60)
        row = {
            **i,
            "klines_expected": exp_k,
            "klines_live": kl.get(iid, 0),
            "klines_failed_ratio": 1 - kl.get(iid, 0) / exp_k if exp_k else None,
        }
        if i["market_type"] != "SPOT":
            exp_p = int(window_s // 300)
            row.update(
                oi_expected=exp_p,
                oi_live=oi.get(iid, 0),
                oi_failed_ratio=max(0.0, 1 - oi.get(iid, 0) / exp_p) if exp_p else None,
                funding_expected=exp_p,
                funding_live=fu.get(iid, 0),
                funding_failed_ratio=max(0.0, 1 - fu.get(iid, 0) / exp_p) if exp_p else None,
            )
        ratios = [v for k, v in row.items() if k.endswith("_failed_ratio") and v is not None]
        row["status"] = "PASS" if ratios and max(ratios) < LIMIT else "FAIL"
        base.append(row)
    hot = query(HOT, p)
    gaps = query(GAPS, p)
    t0, t1 = parse(since), parse(until)
    hot_s = defaultdict(float)
    for h in hot:
        a = max(parse(h["since"]), t0)
        b = min(parse(h["expires_at"]) if h["expires_at"] else t1, t1)
        hot_s[h["token_id"]] += max(0.0, (b - a).total_seconds())
    gap_s = defaultdict(float)
    for g in gaps:
        a = max(parse(g["start"]), t0)
        b = min(parse(g["end"]) if g["end"] else t1, t1)
        gap_s[(g["token_id"], g["instrument_id"], g["kind"])] += max(0.0, (b - a).total_seconds())
    hot_rows = []
    for (tok, iid, kind), s in sorted(gap_s.items()):
        exposure = hot_s.get(tok)
        if not exposure or not kind or not kind.startswith(HOT_GAP_PREFIXES):
            continue
        hot_rows.append(
            {
                "token_id": tok,
                "instrument_id": iid,
                "kind": kind,
                "gap_s": s,
                "hot_s": exposure,
                "gap_share": s / exposure if exposure else None,
            }
        )
    hot_fail = [r for r in hot_rows if r["gap_share"] is not None and r["gap_share"] >= LIMIT]
    return {
        "base": {
            "instruments": len(base),
            "failing": [r for r in base if r["status"] == "FAIL"],
            "rows": base,
            "status": "PASS" if base and all(r["status"] == "PASS" for r in base) else "FAIL",
        },
        "hot": {
            "hot_tokens_in_window": len(hot_s),
            "hot_seconds": dict(hot_s),
            "gap_rows": hot_rows,
            "status": ("PASS" if not hot_fail else "FAIL") if hot_s else "NOT_EXERCISED",
        },
    }


def render(res):
    out = [
        f"# V0-A health {res['window']['since']} → {res['window']['until']}",
        "",
        f"Sources: {res['sources_pass']}/{res['sources_expected']} pass (< 1% failed reads incl. detail fetches; polled >= 95% of cadence).",
        "",
        "| Source | reads | failed | failed % | polls / expected | degraded feeds | status |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for r in res["sources"]:
        pct = "—" if r["failed_ratio"] is None else f"{100 * r['failed_ratio']:.2f}"
        cov = (
            "—"
            if r["poll_coverage"] is None
            else f"{r['polls_ok'] + r['polls_failed']}/{r['polls_expected']}"
        )
        out.append(
            f"| {r['source_id']} | {r['reads']} | {r['failures']} | {pct} | {cov} | {len(r['degraded_feeds'])} | {r['status']} |"
        )
    b, h = res["market"]["base"], res["market"]["hot"]
    out += [
        "",
        f"Market BASE (REST): {b['instruments'] - len(b['failing'])}/{b['instruments']} instruments < 1% missing polls → {b['status']}",
        f"Market HOT (WebSocket): {h['hot_tokens_in_window']} hot tokens; gap rows {len(h['gap_rows'])} → {h['status']}",
        "",
        f"Overall V0-A source/market part: {res['status']}",
    ]
    if b["failing"]:
        out += ["", "Failing BASE instruments (first 30):", ""]
        for r in b["failing"][:30]:
            out.append(
                f"- {r['instrument_id']}: klines {r['klines_live']}/{r['klines_expected']}"
                + (
                    f", OI {r.get('oi_live')}/{r.get('oi_expected')}, funding {r.get('funding_live')}/{r.get('funding_expected')}"
                    if "oi_live" in r
                    else ""
                )
            )
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", help="write a counter snapshot to this path and exit (window start)")
    ap.add_argument("--start-snapshot", help="snapshot taken at the window start")
    ap.add_argument("--end-snapshot", help="write the end snapshot here (taken now)")
    ap.add_argument("--sources-yaml", default="/srv/tank/config/sources.yaml")
    ap.add_argument(
        "--rest-log", help="captured market log lines (`rest reads: host=ok/failed`) for the window"
    )
    ap.add_argument("--out")
    a = ap.parse_args()
    if a.snapshot:
        write_json(a.snapshot, take_snapshot())
        print(a.snapshot)
        sys.exit(0)
    start = json.load(open(a.start_snapshot))
    end = take_snapshot()
    if a.end_snapshot:
        write_json(a.end_snapshot, end)
    a.since, a.until = start["taken_at"], end["taken_at"]
    intervals = feed_intervals(a.sources_yaml)
    expected = sorted({k.split(":")[0] for k in intervals})
    src = sources(start, end, intervals, seconds(a.until, a.since))
    mk = market(a.since, a.until)
    if a.rest_log:
        mk["base"]["rest_reads"] = rest_reads(a.rest_log, a.since, a.until)
        mk["base"]["row_coverage_status"] = mk["base"]["status"]
        mk["base"]["status"] = mk["base"]["rest_reads"]["status"]
    passing = sum(1 for r in src if r["status"] == "PASS")
    res = {
        "window": {"since": a.since, "until": a.until, "hours": seconds(a.until, a.since) / 3600},
        "computed_at": iso(utcnow()),
        "sources_expected": len(expected),
        "sources_pass": passing,
        "sources": src,
        "market": mk,
        "status": "PASS"
        if len(expected) == 34
        and passing == 34
        and mk["base"]["status"] == "PASS"
        and mk["hot"]["status"] in ("PASS",)
        and seconds(a.until, a.since) >= 72 * 3600
        else "FAIL",
    }
    md = render(res)
    if a.out:
        write_json(os.path.join(a.out, "v0a-health.json"), res)
        open(os.path.join(a.out, "v0a-health.md"), "w").write(md)
    print(md)
