"""Lead: independent synchronized-OI check for a card window, from a CSV export of mkt.oi_obs.

Re-implements 03 §5.4 (venues observed within 60 s of both window bounds, native sums) and
03 §0 (middle 95% of same-length past changes, each over the venues observed at both of its
bounds; pairing tolerance max(60 s, half the venue's median cadence)) without product code.
Usage: independent_oi.py btc_oi.csv <window start ISO> <window end ISO>
"""

import csv
import sys
from bisect import bisect_left
from datetime import UTC, datetime, timedelta


def parse(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)


def pct(values, p):
    s = sorted(values)
    k = (len(s) - 1) * p
    i = int(k)
    return s[i] + (s[min(i + 1, len(s) - 1)] - s[i]) * (k - i)


start = datetime.fromisoformat(sys.argv[2].replace("Z", "+00:00"))
end = datetime.fromisoformat(sys.argv[3].replace("Z", "+00:00"))
# Optional 4th arg: recent start. Before it the history is the base tier, the last point per
# (instrument, metric_key, 300 s bucket), as the snapshot reads it (03 §17 "stored base tier").
recent = datetime.fromisoformat(sys.argv[4].replace("Z", "+00:00")) if len(sys.argv) > 4 else None
venues, sampled = {}, {}
with open(sys.argv[1]) as f:
    for r in csv.DictReader(f):
        if r["ts_bucket_anchor"] == "UNSPECIFIED" or not r["oi_native"]:
            continue
        # A synchronized composite sums only rows flagged synchronized_eligible (aligned buckets).
        if r.get("synchronized_eligible") == "f":
            continue
        t, v = parse(r["ts"]), r["instrument_id"].split(":")[0]
        if recent is not None and t < recent:
            key = (v, r.get("metric_key"), int(t.timestamp()) // 300)
            if key not in sampled or t >= sampled[key][0]:
                sampled[key] = (t, float(r["oi_native"]))
            continue
        venues.setdefault(v, []).append((t, float(r["oi_native"])))
for (v, _, _), point in sampled.items():
    venues.setdefault(v, []).append(point)
# Point in time: nothing observed after the cutoff may be used (03 §5.4 eligibility, _time(r) <= end).
venues = {v: [(t, n) for t, n in obs if t <= end] for v, obs in venues.items()}
window = end - start
series = {}
for v, obs in venues.items():
    obs.sort()
    series[v] = ([t for t, _ in obs], [n for _, n in obs])


def near(v, t, tol):
    times, vals = series[v]
    i = bisect_left(times, t)
    best = min(
        ((abs((times[j] - t).total_seconds()), j) for j in (i - 1, i) if 0 <= j < len(times)), default=None
    )
    return vals[best[1]] if best and best[0] <= tol else None


cohort = [v for v in sorted(series) if v != "hyperliquid" and near(v, start, 60) and near(v, end, 60)]
n0 = sum(near(v, start, 60) for v in cohort)
n1 = sum(near(v, end, 60) for v in cohort)
print("cohort", cohort, "n0", round(n0, 4), "n1", round(n1, 4), "dn", f"{n1 / n0 - 1:+.4%}")
for v in cohort:
    print(
        "  ",
        v,
        near(v, start, 60),
        "->",
        near(v, end, 60),
        f"{near(v, end, 60) / near(v, start, 60) - 1:+.4%}",
    )

floor = start - timedelta(days=90)
hist, tol = {}, {}
for v in cohort:
    times, vals = series[v]
    lo, hi = bisect_left(times, floor), bisect_left(times, start)
    hist[v] = (times[lo:hi], vals[lo:hi])
    gaps = sorted(
        (b - a).total_seconds() for a, b in zip(times[lo:hi], times[lo + 1 : hi], strict=False) if b > a
    )
    tol[v] = max(60.0, gaps[len(gaps) // 2] / 2 if gaps else 0)
saved = series
series = hist
changes, ends = [], []
for t in sorted({t for times, _ in hist.values() for t in times}):
    a = b = 0.0
    k = 0
    for v in cohort:
        old, new = near(v, t - window, tol[v]), near(v, t, tol[v])
        if old and new is not None:
            a, b, k = a + old, b + new, k + 1
    if k:
        changes.append(b / a - 1)
        ends.append(t)
span = (ends[-1] - ends[0] + window).total_seconds() / 86400 if ends else 0
print("history windows", len(changes), "span_days", round(span, 2), "tolerance_s", tol)
if span >= 7:
    lo, hi = pct(changes, 0.025), pct(changes, 0.975)
    dn = n1 / n0 - 1
    print(
        "native normal range",
        f"[{lo:+.4%}, {hi:+.4%}]",
        "state",
        "ABOVE" if dn > hi else "BELOW" if dn < lo else "WITHIN",
    )
else:
    print("native normal range n/a (< 7 days)")
