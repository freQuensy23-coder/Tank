"""Explain differences between fixture numbers and the archive: compare openInterestHist (REST) with archive metrics, print price bars."""

import csv
import datetime as dt
import io
import json
import os
import sys
import urllib.request
import zipfile

DATA = sys.argv[1]
UTC = dt.UTC


def T(s):
    return int(dt.datetime.fromisoformat(s).replace(tzinfo=UTC).timestamp() * 1000)


def iso(ms):
    return dt.datetime.fromtimestamp(ms / 1000, UTC).strftime("%m-%d %H:%M")


def get(u):
    return json.loads(
        urllib.request.urlopen(
            urllib.request.Request(u, headers={"User-Agent": "tank-fixture-check/1.0"}), timeout=30
        ).read()
    )


def arch(sym, day):
    out = {}
    with zipfile.ZipFile(os.path.join(DATA, "metrics", f"{sym}-{day}.zip")) as z:
        for row in csv.DictReader(io.TextIOWrapper(z.open(z.namelist()[0]), encoding="utf-8")):
            out[int(dt.datetime.fromisoformat(row["create_time"]).replace(tzinfo=UTC).timestamp() * 1000)] = (
                float(row["sum_open_interest"] or 0)
            )
    return out


for sym, day, a, z in [
    ("ONDOUSDT", "2026-09-24", "2026-09-24T12:00:00", "2026-09-24T16:00:00"),
    ("QNTUSDT", "2026-09-26", "2026-09-26T19:00:00", "2026-09-26T23:00:00"),
    ("QNTUSDT", "2026-09-27", "2026-09-27T19:00:00", "2026-09-27T23:00:00"),
]:
    rest = {
        r["timestamp"]: float(r["sumOpenInterest"])
        for r in get(
            f"https://fapi.binance.com/futures/data/openInterestHist?symbol={sym}&period=1h&startTime={T(a) - 3 * 3600_000}&endTime={T(z) + 3 * 3600_000}&limit=50"
        )
    }
    ar = arch(sym, day)
    print(sym, day)
    for t in sorted(rest):
        print(
            "  ",
            iso(t),
            "REST 1h",
            round(rest[t]),
            "| archive",
            round(ar.get(t, float("nan"))) if t in ar else "—",
        )
    if T(a) in rest and T(z) in rest:
        print("   REST change", a[11:16], "→", z[11:16], f"{rest[T(z)] / rest[T(a)] - 1:+.2%}")
    if T(a) in ar and T(z) in ar:
        print("   archive change", f"{ar[T(z)] / ar[T(a)] - 1:+.2%}")
k = {r[0]: r for r in json.load(open(os.path.join(DATA, "spot_1h_QNTUSDT.json")))}
p = {r[0]: r for r in json.load(open(os.path.join(DATA, "perp_1h_QNTUSDT.json")))}
print("QNT 27 Sep 1h bars (open, close) spot | perp")
for t in range(T("2026-09-27T17:00:00"), T("2026-09-28T01:00:00"), 3600_000):
    print("  ", iso(t), k[t][1], k[t][4], "|", p[t][1], p[t][4])
