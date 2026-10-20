"""V0-A: 34 sources and both market-data paths unattended for 72 h, < 1% failed reads per source (01 §10).

Read-only. Inputs: the JSONL ledger written by source_sampler.py for the window, plus market rows.

Sources: failed-read ratio = FAILURE / ATTEMPT per source over the window (baseline rows ignored).
  A source PASSES with >= 1 attempt, ratio < 1% and no silence longer than 3x its slowest feed interval.
Market BASE path (REST, 03 §5.1): 1m klines per instrument, OI and funding every 300 s on perps.
  Failed reads = expected polls without a stored live row (source_archive IS NULL, received in window).
Market HOT path (WebSocket): capture gaps recorded while a token is hot, as a share of hot time.

Usage: python3 v0a_health.py --reads FILE --since T --until T [--sources-yaml /srv/tank/config/sources.yaml] --out DIR
"""
import argparse
import json
import os
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import iso, parse, query, seconds, utcnow, write_json  # noqa: E402

LIMIT = 0.01
# Gap kinds written by the streaming (HOT) path; history/backfill/FX gaps belong to other paths.
HOT_GAP_PREFIXES = ("STREAM", "RECONNECT", "TRADE_INTEGRITY", "BOOK")

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


def source_ids(path):
    ids = set()
    try:
        with open(path) as fh:
            ids = set(re.findall(r"source_id:\s*['\"]?(S\d{2})", fh.read()))
    except OSError:
        pass
    return sorted(ids)


def intervals(path):
    """Slowest configured interval per source (seconds), from sources.yaml without a YAML dependency."""
    out, current = defaultdict(lambda: 60), None
    try:
        for line in open(path):
            m = re.search(r"source_id:\s*['\"]?(S\d{2})", line)
            if m:
                current = m.group(1)
            m = re.search(r"interval_s:\s*(\d+)", line)
            if m and current:
                out[current] = max(out[current] if current in out else 0, int(m.group(1)))
    except OSError:
        pass
    return out


def sources(reads, since, until, expected, slowest):
    t0, t1 = parse(since), parse(until)
    att, fail, errors, times = defaultdict(int), defaultdict(int), defaultdict(list), defaultdict(list)
    sampler = {"starts": 0, "sample_errors": 0, "first": None, "last": None}
    for line in open(reads):
        r = json.loads(line)
        k = r["kind"]
        if k == "SAMPLER_START":
            sampler["starts"] += 1
        if k == "SAMPLE_ERROR":
            sampler["sample_errors"] += 1
        if k not in ("ATTEMPT", "FAILURE") or r.get("baseline"):
            continue
        at = parse(r["at"])
        if not (t0 <= at < t1):
            continue
        sid = r.get("source_id") or r["key"].split(":")[0]
        sampler["first"] = min(filter(None, [sampler["first"], r["at"]]))
        sampler["last"] = max(filter(None, [sampler["last"], r["at"]]))
        if k == "ATTEMPT":
            att[sid] += 1
            times[sid].append(at)
        else:
            fail[sid] += 1
            if len(errors[sid]) < 5:
                errors[sid].append({"at": r["at"], "key": r["key"], "error": r.get("error")})
    rows = []
    for sid in sorted(set(expected) | set(att) | set(fail)):
        ts = sorted(times[sid])
        edges = [t0] + ts + [t1]
        silence = max((b - a).total_seconds() for a, b in zip(edges, edges[1:])) if ts else (t1 - t0).total_seconds()
        ratio = fail[sid] / att[sid] if att[sid] else None
        ok = bool(att[sid]) and ratio < LIMIT and silence <= 3 * max(slowest.get(sid, 60), 60) + 60
        rows.append({"source_id": sid, "attempts": att[sid], "failures": fail[sid], "failed_ratio": ratio,
                     "longest_silence_s": silence, "slowest_interval_s": slowest.get(sid), "status": "PASS" if ok else "FAIL",
                     "sample_failures": errors[sid]})
    return rows, sampler


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
        row = {**i, "klines_expected": exp_k, "klines_live": kl.get(iid, 0),
               "klines_failed_ratio": 1 - kl.get(iid, 0) / exp_k if exp_k else None}
        if i["market_type"] != "SPOT":
            exp_p = int(window_s // 300)
            row.update(oi_expected=exp_p, oi_live=oi.get(iid, 0), oi_failed_ratio=max(0.0, 1 - oi.get(iid, 0) / exp_p) if exp_p else None,
                       funding_expected=exp_p, funding_live=fu.get(iid, 0),
                       funding_failed_ratio=max(0.0, 1 - fu.get(iid, 0) / exp_p) if exp_p else None)
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
            continue  # base-path or history gaps are measured by the BASE poll counts
        hot_rows.append({"token_id": tok, "instrument_id": iid, "kind": kind, "gap_s": s, "hot_s": exposure,
                         "gap_share": s / exposure if exposure else None})
    hot_fail = [r for r in hot_rows if r["gap_share"] is not None and r["gap_share"] >= LIMIT]
    return {
        "base": {"instruments": len(base), "failing": [r for r in base if r["status"] == "FAIL"], "rows": base,
                 "status": "PASS" if base and all(r["status"] == "PASS" for r in base) else "FAIL"},
        "hot": {"hot_tokens_in_window": len(hot_s), "hot_seconds": dict(hot_s), "gap_rows": hot_rows,
                "status": ("PASS" if not hot_fail else "FAIL") if hot_s else "NOT_EXERCISED"},
    }


def render(res):
    out = [f"# V0-A health {res['window']['since']} → {res['window']['until']}", "",
           f"Sources: {res['sources_pass']}/{res['sources_expected']} pass (< 1% failed reads, no long silence).", "",
           "| Source | attempts | failures | failed % | longest silence s | status |", "|---|---:|---:|---:|---:|---|"]
    for r in res["sources"]:
        pct = "—" if r["failed_ratio"] is None else f"{100 * r['failed_ratio']:.2f}"
        out.append(f"| {r['source_id']} | {r['attempts']} | {r['failures']} | {pct} | {r['longest_silence_s']:.0f} | {r['status']} |")
    b, h = res["market"]["base"], res["market"]["hot"]
    out += ["", f"Market BASE (REST): {b['instruments'] - len(b['failing'])}/{b['instruments']} instruments < 1% missing polls → {b['status']}",
            f"Market HOT (WebSocket): {h['hot_tokens_in_window']} hot tokens; gap rows {len(h['gap_rows'])} → {h['status']}", "",
            f"Overall V0-A source/market part: {res['status']}"]
    if b["failing"]:
        out += ["", "Failing BASE instruments (first 30):", ""]
        for r in b["failing"][:30]:
            out.append(f"- {r['instrument_id']}: klines {r['klines_live']}/{r['klines_expected']}"
                       + (f", OI {r.get('oi_live')}/{r.get('oi_expected')}, funding {r.get('funding_live')}/{r.get('funding_expected')}" if "oi_live" in r else ""))
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--reads", required=True)
    ap.add_argument("--since", required=True)
    ap.add_argument("--until", required=True)
    ap.add_argument("--sources-yaml", default="/srv/tank/config/sources.yaml")
    ap.add_argument("--out")
    a = ap.parse_args()
    expected = source_ids(a.sources_yaml)
    src, sampler = sources(a.reads, a.since, a.until, expected, intervals(a.sources_yaml))
    mk = market(a.since, a.until)
    passing = sum(1 for r in src if r["status"] == "PASS")
    res = {"window": {"since": a.since, "until": a.until, "hours": seconds(a.until, a.since) / 3600},
           "computed_at": iso(utcnow()), "sources_expected": len(expected), "sources_pass": passing,
           "sources": src, "sampler": sampler, "market": mk,
           "status": "PASS" if len(expected) == 34 and passing == 34 and mk["base"]["status"] == "PASS"
           and mk["hot"]["status"] in ("PASS",) and seconds(a.until, a.since) >= 72 * 3600 else "FAIL"}
    md = render(res)
    if a.out:
        write_json(os.path.join(a.out, "v0a-health.json"), res)
        open(os.path.join(a.out, "v0a-health.md"), "w").write(md)
    print(md)
