"""Lead: independent BTC relative check from binance 1m closes (benchmark: r_raw vs its own history).

r(t) = close(bar ending t) / close(bar ending t - L) - 1 for every minute t in the 90 days before
the window start with both bars present; normal range = 2.5/97.5% (linear), pctile = share <= value.
Usage: independent_relative.py closes.csv <window start> <cutoff> <window seconds>
"""

import csv
import sys
from datetime import UTC, datetime, timedelta


def parse(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def pct(values, p):
    s = sorted(values)
    k = (len(s) - 1) * p
    i = int(k)
    return s[i] + (s[min(i + 1, len(s) - 1)] - s[i]) * (k - i)


closes = {}
with open(sys.argv[1]) as f:
    for r in csv.DictReader(f):
        closes[parse(r["t"]) + timedelta(minutes=1)] = float(r["c"])  # keyed by bar end
start, cutoff = parse(sys.argv[2]), parse(sys.argv[3])
length = timedelta(seconds=int(sys.argv[4]))
value = closes[cutoff] / closes[start] - 1
floor = start - timedelta(days=90)
hist = [
    closes[t] / closes[t - length] - 1
    for t in closes
    if floor <= t - length and t <= start and (t - length) in closes
]
print(f"EVENT r_raw {value:+.6%}  history n={len(hist)}")
print(
    f"normal range [{pct(hist, 0.025):+.4%}, {pct(hist, 0.975):+.4%}]  pctile {sum(x <= value for x in hist) / len(hist):.4f}"
)
q75 = pct(hist, 0.75)
print(f"upper quartile {q75:+.4%}")
day = timedelta(days=1)
btc24 = closes[cutoff] / closes[cutoff - day] - 1
hist24 = [
    closes[t] / closes[t - day] - 1
    for t in closes
    if floor <= t - day and t <= cutoff and (t - day) in closes
]
print(
    f"BTC 24h {btc24:+.4%}  24h normal range [{pct(hist24, 0.025):+.2%}, {pct(hist24, 0.975):+.2%}] n={len(hist24)}"
)
# The market-context baseline (03 §5.7): hourly points t on the hour, t < window start, 90 days.
hourly = [t for t in closes if t.minute == 0 and floor <= t < start]
hist24h = [closes[t] / closes[t - day] - 1 for t in hourly if (t - day) in closes]
print(f"hourly 24h normal range [{pct(hist24h, 0.025):+.2%}, {pct(hist24h, 0.975):+.2%}] n={len(hist24h)}")
