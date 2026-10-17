"""Download the public market history the fixture check needs into fixture_check/data (no API keys)."""
import json, os, sys, time, urllib.request, datetime as dt
OUT = sys.argv[1]
os.makedirs(OUT, exist_ok=True)
UA = {'User-Agent': 'tank-fixture-check/1.0'}

def get(url, binary=False, tries=4):
    for k in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30) as r:
                b = r.read(); return b if binary else json.loads(b)
        except Exception as e:
            if k == tries - 1: raise
            time.sleep(1.5 * (k + 1))

def ms(s): return int(dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc).timestamp() * 1000)

def klines(base, path, sym, interval, start, end, fname):
    f = os.path.join(OUT, fname)
    if os.path.exists(f): return
    rows, t = [], ms(start); e = ms(end)
    while t < e:
        page = get(f'{base}{path}?symbol={sym}&interval={interval}&startTime={t}&endTime={e}&limit=1000')
        if not page: break
        rows += page; t = page[-1][0] + 1
        if len(page) < 1000: break
    json.dump(rows, open(f, 'w')); print(fname, len(rows))

def bybit_oi(sym, start, end, fname):
    f = os.path.join(OUT, fname)
    if os.path.exists(f): return
    rows, e = [], ms(end); s = ms(start)
    while e > s:
        r = get(f'https://api.bybit.com/v5/market/open-interest?category=linear&symbol={sym}&intervalTime=5min&startTime={s}&endTime={e}&limit=200')
        lst = r['result']['list']
        if not lst: break
        rows += lst; e = int(lst[-1]['timestamp']) - 1
        if len(lst) < 200: break
    json.dump(rows, open(f, 'w')); print(fname, len(rows))

def metrics(sym, start, end):
    d = dt.date.fromisoformat(start); last = dt.date.fromisoformat(end)
    os.makedirs(os.path.join(OUT, 'metrics'), exist_ok=True)
    n = 0
    while d <= last:
        f = os.path.join(OUT, 'metrics', f'{sym}-{d}.zip')
        if not os.path.exists(f):
            try:
                b = get(f'https://data.binance.vision/data/futures/um/daily/metrics/{sym}/{sym}-metrics-{d}.zip', binary=True)
                open(f, 'wb').write(b); n += 1
            except Exception as ex:
                print('missing', sym, d, ex)
        d += dt.timedelta(days=1)
    print('metrics', sym, 'new files', n)

def funding(sym, start, end, fname):
    f = os.path.join(OUT, fname)
    if os.path.exists(f): return
    r = get(f'https://fapi.binance.com/fapi/v1/fundingRate?symbol={sym}&startTime={ms(start)}&endTime={ms(end)}&limit=1000')
    json.dump(r, open(f, 'w')); print(fname, len(r))

SPOT, FUT = 'https://api.binance.com', 'https://fapi.binance.com'
H0 = '2026-06-10T00:00:00'                       # 90 days before the earliest cutoff (15 Sep) plus margin
for s in ['BTCUSDT', 'QNTUSDT', 'WLDUSDT', 'ONDOUSDT']:
    klines(SPOT, '/api/v3/klines', s, '1h', H0, '2026-10-17T00:00:00', f'spot_1h_{s}.json')
for s in ['QNTUSDT', 'WLDUSDT', 'ONDOUSDT']:
    klines(FUT, '/fapi/v1/klines', s, '1h', H0, '2026-10-17T00:00:00', f'perp_1h_{s}.json')
klines(SPOT, '/api/v3/klines', 'QNTUSDT', '15m', '2026-09-24T00:00:00', '2026-09-25T00:00:00', 'spot_15m_QNTUSDT_0924.json')
klines(SPOT, '/api/v3/klines', 'BTCUSDT', '15m', '2026-09-24T00:00:00', '2026-09-25T00:00:00', 'spot_15m_BTCUSDT_0924.json')
metrics('WLDUSDT', '2026-07-15', '2026-10-16')
metrics('QNTUSDT', '2026-06-25', '2026-09-28')
metrics('ONDOUSDT', '2026-06-24', '2026-09-24')
bybit_oi('WLDUSDT', '2026-07-15T00:00:00', '2026-10-16T13:00:00', 'bybit_oi_5m_WLDUSDT.json')
funding('QNTUSDT', '2026-09-20T00:00:00', '2026-09-29T00:00:00', 'funding_QNTUSDT.json')
