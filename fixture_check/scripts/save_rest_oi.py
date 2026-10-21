"""Store Binance openInterestHist values for the fixture windows before the 30-day REST retention drops them."""

import datetime as dt
import json
import sys
import urllib.request

OUT = sys.argv[1]
UTC = dt.UTC


def T(s):
    return int(dt.datetime.fromisoformat(s).replace(tzinfo=UTC).timestamp() * 1000)


def get(u):
    return json.loads(
        urllib.request.urlopen(
            urllib.request.Request(u, headers={"User-Agent": "tank-fixture-check/1.0"}), timeout=30
        ).read()
    )


cases = [
    ("F-13", "ONDOUSDT", "2026-09-24T08:00:00", "2026-09-24T20:00:00"),
    ("F-12", "QNTUSDT", "2026-09-26T15:00:00", "2026-09-27T03:00:00"),
    ("F-14", "QNTUSDT", "2026-09-27T15:00:00", "2026-09-28T03:00:00"),
    ("F-11", "WLDUSDT", "2026-10-16T07:00:00", "2026-10-16T14:00:00"),
]
saved = {}
for fid, sym, a, z in cases:
    for period in ["5m", "1h"]:
        rows, t = [], T(a)
        while t < T(z):
            page = get(
                f"https://fapi.binance.com/futures/data/openInterestHist?symbol={sym}&period={period}&startTime={t}&endTime={T(z)}&limit=500"
            )
            if not page:
                break
            rows += page
            t = page[-1]["timestamp"] + 1
            if len(page) < 500:
                break
        saved[f"{fid}:{sym}:{period}"] = rows
        print(fid, sym, period, len(rows))
json.dump(
    dict(
        fetched_at=dt.datetime.now(UTC).isoformat(),
        source="https://fapi.binance.com/futures/data/openInterestHist",
        data=saved,
    ),
    open(OUT, "w"),
)
