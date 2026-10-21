"""Test the rule alternative for the failing fixture (F-16 context) and re-check the F-11 snapshot."""

import datetime as dt
import json
import math
import os
import sys
import urllib.request

DATA = sys.argv[1]
OUT = sys.argv[2]
UTC = dt.UTC


def T(s):
    return int(dt.datetime.fromisoformat(s).replace(tzinfo=UTC).timestamp() * 1000)


def iso(ms):
    return dt.datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d %H:%M")


def get(u):
    return json.loads(
        urllib.request.urlopen(
            urllib.request.Request(u, headers={"User-Agent": "tank-fixture-check/1.0"}), timeout=30
        ).read()
    )


def kl(name):
    return {
        r[0]: {"o": float(r[1]), "c": float(r[4]), "qv": float(r[7])}
        for r in json.load(open(os.path.join(DATA, name)))
    }


H, D90 = 3600_000, 90 * 86400_000
R = {}
# ---- F-16: is 15 Sep a record low of BTC's 24h return, or a record high of its 24h realized volatility? ----
btc = kl("spot_1h_BTCUSDT.json")
day0, cut = T("2026-09-15T00:00:00"), T("2026-09-16T00:00:00")
r = btc[cut - H]["c"] / btc[day0]["o"] - 1
hist = [btc[t - H]["c"] / btc[t - 25 * H]["c"] - 1 for t in range(day0 - D90 + 25 * H, day0 + 1, H)]


def rv(e):
    lr = [math.log(btc[t]["c"] / btc[t - H]["c"]) for t in range(e - 24 * H, e, H)]
    m = sum(lr) / 24
    return math.sqrt(sum((x - m) ** 2 for x in lr) / 23)


rvs = [rv(t) for t in range(day0 - D90 + 26 * H, day0, H)]
R["F-16"] = dict(
    btc_24h=r,
    hist_min=min(hist),
    hist_min_at=None,
    rv=rv(cut - H),
    rv_max=max(rvs),
    stress_if_record=(r < min(hist) or rv(cut - H) > max(rvs)),
)
# ---- F-11: synchronized change from REST 5m (Binance) + Bybit, at the Addendum's frozen times ----
a, z = T("2026-10-16T08:30:00"), T("2026-10-16T12:30:00")
bn = {
    r["timestamp"]: float(r["sumOpenInterest"])
    for r in get(
        f"https://fapi.binance.com/futures/data/openInterestHist?symbol=WLDUSDT&period=5m&startTime={a - 600_000}&endTime={z + 600_000}&limit=100"
    )
}
by = {
    int(r["timestamp"]): float(r["singleOpenInterest"])
    for r in json.load(open(os.path.join(DATA, "bybit_oi_5m_WLDUSDT.json")))
}
R["F-11"] = dict(
    binance_rest=bn[z] / bn[a] - 1,
    bybit=by[z] / by[a] - 1,
    sync_rest=(bn[z] + by[z]) / (bn[a] + by[a]) - 1,
    binance_rest_at=(bn[a], bn[z]),
    bybit_at=(by[a], by[z]),
)
json.dump(R, open(OUT, "w"), indent=1, default=str)
for k, v in R.items():
    print(k, json.dumps(v, default=str)[:1200])
    print()
