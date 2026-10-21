"""Recompute the market fixtures of TDD §11.3 under the history-based rules (§0, §5.4, §5.7, §9.2)."""

import csv
import datetime as dt
import io
import json
import math
import os
import sys
import zipfile

DATA = sys.argv[1]
OUT = sys.argv[2]
UTC = dt.UTC


def T(s):
    return int(dt.datetime.fromisoformat(s).replace(tzinfo=UTC).timestamp() * 1000)


def iso(ms):
    return dt.datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d %H:%M")


H, M5, M15, D90 = 3600_000, 300_000, 900_000, 90 * 86400_000


def kl(name):
    rows = json.load(open(os.path.join(DATA, name)))
    return {r[0]: {"o": float(r[1]), "c": float(r[4]), "qv": float(r[7])} for r in rows}


def pct(xs, p):
    s = sorted(xs)
    k = (len(s) - 1) * p / 100
    f = math.floor(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def beta(tok, btc, end):
    ts = [t for t in sorted(tok) if end - D90 <= t < end and t - H in tok and t in btc and t - H in btc]
    y = [math.log(tok[t]["c"] / tok[t - H]["c"]) for t in ts]
    x = [math.log(btc[t]["c"] / btc[t - H]["c"]) for t in ts]
    mx, my = sum(x) / len(x), sum(y) / len(y)
    b = sum((a - mx) * (c - my) for a, c in zip(x, y, strict=False)) / sum((a - mx) ** 2 for a in x)
    return (2 / 3) * b + 1 / 3, b, len(ts)


def ret_series(tok, btc, b, step, length, start, end):
    """BTC-adjusted simple returns over `length` ms, windows ending every `step` ms in [start, end)."""
    out = []
    for t in range(start - start % step, end, step):
        a, z = t - length, t  # value at open of candle t = close of candle t-step
        if (a - step) in tok and (z - step) in tok and (a - step) in btc and (z - step) in btc:
            r = tok[z - step]["c"] / tok[a - step]["c"] - 1
            rb = btc[z - step]["c"] / btc[a - step]["c"] - 1
            out.append(r - b * rb)
    return out


def close_at(k, t, step):  # price at time t = close of the candle that ends at t
    return k[t - step]["c"]


def metrics(sym):
    d = {}
    for f in sorted(os.listdir(os.path.join(DATA, "metrics"))):
        if not f.startswith(sym + "-"):
            continue
        with zipfile.ZipFile(os.path.join(DATA, "metrics", f)) as z:
            for name in z.namelist():
                for row in csv.DictReader(io.TextIOWrapper(z.open(name), encoding="utf-8")):
                    t = int(
                        dt.datetime.fromisoformat(row["create_time"]).replace(tzinfo=UTC).timestamp() * 1000
                    )
                    try:
                        oi, val = float(row["sum_open_interest"]), float(row["sum_open_interest_value"])
                    except ValueError:
                        continue
                    if oi > 0 and val > 0:
                        d[t] = (oi, val)  # rows with zero or empty OI are archive gaps, not data
    return d


def chg(series, t0, t1):
    return series[t1] / series[t0] - 1


def window_changes(series, length, start, end):
    return [
        series[t] / series[t - length] - 1
        for t in sorted(series)
        if start <= t - length and t < end and (t - length) in series
    ]


def decomp(n0, n1, p0, p1):
    pos, mk = (n1 - n0) * p0, n0 * (p1 - p0)
    it = (n1 - n0) * (p1 - p0)
    return pos, mk, it, abs(pos) / (abs(pos) + abs(mk) + abs(it))


R = {}
btc1h = kl("spot_1h_BTCUSDT.json")

# ---------------- F-16: BTC context on 15 Sep 2026 ----------------
cut = T("2026-09-16T00:00:00")
day0 = T("2026-09-15T00:00:00")
r_day = btc1h[cut - H]["c"] / btc1h[day0]["o"] - 1
hist = [
    btc1h[t - H]["c"] / btc1h[t - 25 * H]["c"] - 1
    for t in range(day0 - D90 + 25 * H, day0 + 1, H)
    if (t - 25 * H) in btc1h and (t - H) in btc1h
]


def rv(t_end):
    lr = [math.log(btc1h[t]["c"] / btc1h[t - H]["c"]) for t in range(t_end - 24 * H, t_end, H)]
    m = sum(lr) / len(lr)
    return math.sqrt(sum((x - m) ** 2 for x in lr) / (len(lr) - 1))


rv_now = rv(cut - H)
rvs = [rv(t) for t in range(day0 - D90 + 26 * H, day0, H)]
R["F-16"] = dict(
    btc_24h=r_day,
    q25=pct(hist, 25),
    p2_5=pct(hist, 2.5),
    rv24=rv_now,
    rv_p97_5=pct(rvs, 97.5),
    n=len(hist),
    context=(
        "STRESS"
        if (r_day < pct(hist, 2.5) or rv_now > pct(rvs, 97.5))
        else "HEADWIND"
        if r_day < pct(hist, 25)
        else "NEUTRAL"
    ),
)

# ---------------- F-08: QNT repricing on 24 Sep 2026 ----------------
qnt1h = kl("spot_1h_QNTUSDT.json")
q15 = kl("spot_15m_QNTUSDT_0924.json")
b15 = kl("spot_15m_BTCUSDT_0924.json")
cut = T("2026-09-24T20:15:00")
bq, braw, nb = beta(qnt1h, btc1h, T("2026-09-24T00:00:00"))
h24 = ret_series(qnt1h, btc1h, bq, H, 24 * H, cut - D90 + 24 * H, T("2026-09-24T00:00:00"))
p_cut, pb_cut = (
    q15[T("2026-09-24T20:00:00")]["c"],
    b15[T("2026-09-24T20:00:00")]["c"],
)  # close of the 20:00–20:15 bar
lows = sorted(q15.items(), key=lambda kv: kv[1]["c"])
anchors = {"day open 00:00": T("2026-09-24T00:00:00")}
lo_t = min((t for t in q15 if t < T("2026-09-24T20:00:00")), key=lambda t: q15[t]["c"])
anchors["lowest 15m close before the call (" + iso(lo_t + M15) + ")"] = lo_t + M15
f08 = []
for name, a in anchors.items():
    pa = q15[a]["o"] if a in q15 else q15[a - M15]["c"]
    pba = b15[a]["o"] if a in b15 else b15[a - M15]["c"]
    r = p_cut / pa - 1
    rb = pb_cut / pba - 1
    L = cut - a
    hl = ret_series(
        qnt1h, btc1h, bq, H, max(H, round(L / H) * H), cut - D90 + 24 * H, T("2026-09-24T00:00:00")
    )
    f08.append(
        dict(
            anchor=name,
            hours=L / H,
            r=r,
            r_btc=rb,
            adj=r - bq * rb,
            len_q75=pct(hl, 75),
            len_p97_5=pct(hl, 97.5),
        )
    )
R["F-08"] = dict(
    beta=bq,
    beta_raw=braw,
    n_beta=nb,
    h24_p97_5=pct(h24, 97.5),
    h24_p2_5=pct(h24, 2.5),
    n24=len(h24),
    windows=f08,
)


# ---------------- OI fixtures: F-11 (WLD), F-12, F-14 (QNT), F-13 (ONDO) ----------------
def oi_case(sym, w0, w1, extra=None):
    m = metrics(sym)
    n = {t: v[0] for t, v in m.items()}
    usd = {t: v[1] for t, v in m.items()}
    a, z = T(w0), T(w1)
    L = z - a
    hn = window_changes(n, L, a - D90, a)
    hu = window_changes(usd, L, a - D90, a)
    p0, p1 = usd[a] / n[a], usd[z] / n[z]
    pos, mk, it, ps = decomp(n[a], n[z], p0, p1)
    dn, du = chg(n, a, z), chg(usd, a, z)
    nb = dn < pct(hn, 2.5) or dn > pct(hn, 97.5)
    ub = du < pct(hu, 2.5) or du > pct(hu, 97.5)
    cls = "REAL_NATIVE" if (nb and ps > 0.5) else "PRICE_MARKUP" if (not nb and ub) else "MIXED"
    return dict(
        window=f"{w0} → {w1}",
        dn=dn,
        du=du,
        dp=p1 / p0 - 1,
        n_p2_5=pct(hn, 2.5),
        n_p97_5=pct(hn, 97.5),
        u_p2_5=pct(hu, 2.5),
        u_p97_5=pct(hu, 97.5),
        pos_share=ps,
        native_beyond=nb,
        usd_beyond=ub,
        oi_class=cls,
        n_hist=len(hn),
    )


R["F-12"] = oi_case("QNTUSDT", "2026-09-26T19:00:00", "2026-09-26T23:00:00")
R["F-13"] = oi_case("ONDOUSDT", "2026-09-24T12:00:00", "2026-09-24T16:00:00")
R["F-14"] = oi_case("QNTUSDT", "2026-09-27T19:00:00", "2026-09-27T23:00:00")

# F-11: synchronized Binance + Bybit (single-sided) native OI
wm = metrics("WLDUSDT")
by = json.load(open(os.path.join(DATA, "bybit_oi_5m_WLDUSDT.json")))
byn = {int(r["timestamp"]): float(r.get("singleOpenInterest") or float(r["openInterest"]) / 2) for r in by}
syn = {t: wm[t][0] + byn[t] for t in wm if t in byn}
a, z = T("2026-10-16T08:30:00"), T("2026-10-16T12:30:00")
L = z - a
hs = window_changes(syn, L, a - D90, a)
p0, p1 = wm[a][1] / wm[a][0], wm[z][1] / wm[z][0]
pos, mk, it, ps = decomp(syn[a], syn[z], p0, p1)
ds = chg(syn, a, z)
beyond = ds > pct(hs, 97.5) or ds < pct(hs, 2.5)
hb = window_changes({t: v[0] for t, v in wm.items()}, L, a - D90, a)
R["F-11"] = dict(
    window="2026-10-16 08:30 → 12:30",
    sync_dn=ds,
    binance_dn=chg({t: v[0] for t, v in wm.items()}, a, z),
    bybit_dn=chg(byn, a, z),
    dp=p1 / p0 - 1,
    p2_5=pct(hs, 2.5),
    p97_5=pct(hs, 97.5),
    bin_p97_5=pct(hb, 97.5),
    pos_share=ps,
    native_beyond=beyond,
    oi_class=(
        "REAL_NATIVE"
        if beyond and ps > 0.5
        else "MIXED"
        if beyond
        else "MIXED (native change within its normal range)"
    ),
    n_hist=len(hs),
)
# incremental perp vs spot volume (Binance only, hourly bars 08:00–12:00 vs 04:00–08:00) for the PERP_LED_MARKUP alternative
ws, wp = kl("spot_1h_WLDUSDT.json"), kl("perp_1h_WLDUSDT.json")


def vol(k, a, z):
    return sum(k[t]["qv"] for t in range(a, z, H))


a2, z2 = T("2026-10-16T08:00:00"), T("2026-10-16T12:00:00")
dsp = max(0, vol(ws, a2, z2) - vol(ws, a2 - 4 * H, a2))
dpp = max(0, vol(wp, a2, z2) - vol(wp, a2 - 4 * H, a2))
R["F-11"]["perp_share_incremental_binance"] = (dpp / (dpp + dsp)) if (dpp + dsp) else None
R["F-11"]["perp_vs_spot_volume_binance_4h"] = vol(wp, a2, z2) / vol(ws, a2, z2)

# price checks for F-13 / F-14 (BTC-adjusted 4h return against its normal range)
for key, sym, w0 in [("F-13", "ONDOUSDT", "2026-09-24T12:00:00"), ("F-14", "QNTUSDT", "2026-09-27T19:00:00")]:
    k = kl(f"spot_1h_{sym}.json")
    a = T(w0)
    z = a + 4 * H
    b, _, _ = beta(k, btc1h, a)
    hist4 = ret_series(k, btc1h, b, H, 4 * H, a - D90 + 4 * H, a)
    r = k[z - H]["c"] / k[a - H]["c"] - 1
    rb = btc1h[z - H]["c"] / btc1h[a - H]["c"] - 1
    R[key].update(
        price_4h=r,
        price_4h_adj=r - b * rb,
        price_p97_5=pct(hist4, 97.5),
        price_beyond=(r - b * rb) > pct(hist4, 97.5),
    )
    if key == "F-13":
        sp, pp = kl("spot_1h_ONDOUSDT.json"), kl("perp_1h_ONDOUSDT.json")
        dsp = max(0, vol(sp, a, z) - vol(sp, a - 4 * H, a))
        dpp = max(0, vol(pp, a, z) - vol(pp, a - 4 * H, a))
        R[key]["perp_share_incremental_binance"] = dpp / (dpp + dsp) if (dpp + dsp) else None
fr = json.load(open(os.path.join(DATA, "funding_QNTUSDT.json")))
R["F-14"]["funding"] = [
    (iso(f["fundingTime"]), float(f["fundingRate"]))
    for f in fr
    if T("2026-09-27T00:00:00") <= f["fundingTime"] <= T("2026-09-28T00:00:00")
]

json.dump(R, open(OUT, "w"), indent=1, default=str)
for k, v in R.items():
    print(k, json.dumps(v, default=lambda x: round(x, 5) if isinstance(x, float) else str(x))[:900])
    print()
